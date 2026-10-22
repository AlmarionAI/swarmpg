package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These pins are operator configuration, deliberately outside PostgreSQL's
// restore history. Reconnecting never learns or accepts a new incarnation.
type recoveryConfig struct {
	SystemID   string   `json:"system_id"`
	Timeline   int64    `json:"timeline"`
	Generation int64    `json:"generation"`
	uncertain  sync.Map // *pgconn.PgConn -> true, retired with its connection
	committing sync.Map // *pgconn.PgConn -> true, only while COMMIT is executing
}

func recoveryConfigFromEnv() (*recoveryConfig, error) {
	values := []string{os.Getenv("RECOVERY_SYSTEM_ID"), os.Getenv("RECOVERY_TIMELINE"), os.Getenv("RECOVERY_GENERATION")}
	if values[0] == "" && values[1] == "" && values[2] == "" {
		return nil, nil
	}
	system, e1 := strconv.ParseUint(values[0], 10, 64)
	timeline, e2 := strconv.ParseInt(values[1], 10, 64)
	generation, e3 := strconv.ParseInt(values[2], 10, 64)
	if e1 != nil || system == 0 || strconv.FormatUint(system, 10) != values[0] || e2 != nil || timeline < 1 || timeline > 4294967295 || e3 != nil || generation < 1 || generation > 2147483647 {
		return nil, errors.New("RECOVERY_SYSTEM_ID, RECOVERY_TIMELINE and RECOVERY_GENERATION must all be canonical positive integers in range")
	}
	if strconv.FormatInt(timeline, 10) != values[1] || strconv.FormatInt(generation, 10) != values[2] {
		return nil, errors.New("recovery timeline and generation must be canonical positive integers")
	}
	return &recoveryConfig{SystemID: values[0], Timeline: timeline, Generation: generation}, nil
}

func commitUnconfirmed() *apiError {
	return &apiError{Status: 503, Code: "commit_unconfirmed", Message: "commit durability was not confirmed; the operation may have committed", Retryable: true,
		Hint: "Resolve the outcome using the same durable task/outbox request identity. Do not blindly repeat arbitrary SQL."}
}

func (c *recoveryConfig) observeNotice(conn *pgconn.PgConn, notice *pgconn.Notice) {
	// PostgreSQL's query-cancel path in SyncRepWaitForLSN emits a generic 01000
	// WARNING, then returns a successful COMMIT even though replication did not
	// finish. Treat every warning during COMMIT as uncertain, without relying on
	// localized message text or assuming cancellation has SQLSTATE 57014.
	severity := notice.SeverityUnlocalized
	if severity == "" {
		severity = notice.Severity
	}
	_, committing := c.committing.Load(conn)
	if severity == "WARNING" && committing {
		c.uncertain.Store(conn, true)
	}
}

func (c *recoveryConfig) configurePool(cfg *pgxpool.Config) {
	if c == nil {
		return
	}
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "remote_apply"
	cfg.ConnConfig.RuntimeParams["application_name"] = "swarmpg-recovery"
	cfg.ConnConfig.OnNotice = c.observeNotice
	cfg.AfterRelease = func(conn *pgx.Conn) bool { _, bad := c.uncertain.Load(conn.PgConn()); return !bad }
	cfg.BeforeClose = func(conn *pgx.Conn) { c.uncertain.Delete(conn.PgConn()) }
}

func (c *recoveryConfig) check(ctx context.Context, tx pgx.Tx) error {
	if c == nil {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT swarmpg.recovery_check($1,$2,$3)`, c.SystemID, c.Timeline, c.Generation)
	return err
}

func (c *recoveryConfig) commit(ctx context.Context, tx pgx.Tx) error {
	if c == nil {
		return tx.Commit(ctx)
	}
	conn := tx.Conn().PgConn()
	c.committing.Store(conn, true)
	defer c.committing.Delete(conn)
	err := tx.Commit(ctx)
	_, uncertain := c.uncertain.Load(conn)
	if err != nil || uncertain {
		markAdmissionRuntimeUncertain(ctx)
		return commitUnconfirmed()
	}
	return nil
}

// Successful reads/replays also pass a transactional WAL barrier in
// recovery_check. A read-only replay must not acknowledge a receipt that was
// locally committed while an earlier synchronous commit wait was cancelled.
func recoveryTransaction(ctx context.Context, pool *pgxpool.Pool, c *recoveryConfig, fn func(context.Context, pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	if c != nil {
		if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit = remote_apply; SET LOCAL statement_timeout = 9000; SET LOCAL lock_timeout = 1000"); err != nil {
			return err
		}
		if err = c.check(ctx, tx); err != nil {
			return err
		}
	}
	if err = fn(ctx, tx); err != nil {
		return err
	}
	return c.commit(ctx, tx)
}

func (g *Gateway) adminRow(ctx context.Context, query string, args []any, dest ...any) error {
	if g.recovery == nil {
		return g.admin.QueryRow(ctx, query, args...).Scan(dest...)
	}
	return recoveryTransaction(ctx, g.admin, g.recovery, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, query, args...).Scan(dest...)
	})
}

func (g *Gateway) adminExec(ctx context.Context, query string, args ...any) error {
	if g.recovery == nil {
		_, err := g.admin.Exec(ctx, query, args...)
		return err
	}
	return recoveryTransaction(ctx, g.admin, g.recovery, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, query, args...)
		return err
	})
}

// Derive credentials only; preserve host fallback lists, TLS and session
// selection options. An explicit runtime URL is preferred in production.
func runtimeDatabaseURL(admin string) (string, error) {
	u, err := url.Parse(admin)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" {
		return "", fmt.Errorf("RUNTIME_DATABASE_URL is required when DATABASE_URL is not a PostgreSQL URI")
	}
	u.User = url.User("agent_runtime")
	q := u.Query()
	q.Del("user")
	q.Del("password")
	q.Del("passfile")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (g *Gateway) recoveryReady(ctx context.Context) error {
	if g.recovery == nil {
		return nil
	}
	return recoveryTransaction(ctx, g.quota, g.recovery, func(context.Context, pgx.Tx) error { return nil })
}

func (g *Gateway) validateRecoveryStartup(ctx context.Context) error {
	var installed bool
	if err := g.admin.QueryRow(ctx, `SELECT to_regclass('swarmpg.recovery_control') IS NOT NULL`).Scan(&installed); err != nil {
		return err
	}
	if !installed {
		if g.recovery != nil {
			return errors.New("apply the recovery schema before enabling recovery pins")
		}
		return nil
	}
	var enabled bool
	if err := g.admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM swarmpg.recovery_control)`).Scan(&enabled); err != nil {
		return err
	}
	if enabled && g.recovery == nil {
		return errors.New("database recovery is enabled; explicit RECOVERY_SYSTEM_ID/TIMELINE/GENERATION pins are required")
	}
	return nil
}
