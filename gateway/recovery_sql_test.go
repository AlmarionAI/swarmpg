package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The replicated topology, actual promotions, partitions and PITR are exercised
// by scripts/recovery-e2e.py. These checks run with every ordinary stock
// and kernel integration suite and leave no recovery configuration behind.
func TestRecoverySQLGuardAndFencesIntegration(t *testing.T) {
	dsn := os.Getenv("SWARMPG_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SWARMPG_TEST_DATABASE_URL to a disposable initialized database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var enabled bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM swarmpg.recovery_control)`).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Skip("this test requires legacy mode; it never changes an activated cluster")
	}
	must := func(query string, args ...any) {
		t.Helper()
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	wantError := func(code, query string, args ...any) {
		t.Helper()
		must("SAVEPOINT expected_error")
		_, err := tx.Exec(ctx, query, args...)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != code {
			t.Fatalf("want SQLSTATE %s; got %v", code, err)
		}
		must("ROLLBACK TO SAVEPOINT expected_error")
		must("RELEASE SAVEPOINT expected_error")
	}
	fence := func(previous, want int64) {
		t.Helper()
		var got int64
		if err := tx.QueryRow(ctx, `SELECT swarmpg._next_fence($1)`, previous).Scan(&got); err != nil || got != want {
			t.Fatalf("fence(%d) = %d, %v; want %d", previous, got, err, want)
		}
	}

	// Existing installations preserve legacy fences until recovery is enabled.
	must(`SELECT swarmpg._recovery_guard()`)
	fence(0, 1)
	wantError("SR503", `SELECT swarmpg.recovery_check('missing',1,1)`)
	must(`INSERT INTO swarmpg.recovery_control(system_identifier,timeline,generation,state,reason)
		SELECT (swarmpg._recovery_identity()->>'system_identifier'),
		(swarmpg._recovery_identity()->>'timeline')::bigint,1,'quarantined','transactional fixture'`)
	fence(0, 4294967297)
	fence(4294967297, 4294967298)
	wantError("SR409", `SELECT swarmpg._next_fence(8589934591)`)
	wantError("SR409", `SELECT swarmpg._next_fence(8589934592)`)
	wantError("SR503", `SELECT swarmpg._recovery_guard()`)
	wantError("SR503", `SELECT swarmpg.recovery_check('wrong-cluster',1,1)`)
	wantError("SR503", `SELECT swarmpg.outbox_claim(gen_random_uuid(),'test',gen_random_uuid(),gen_random_uuid(),1000)`)
	wantError("SR503", `SELECT swarmpg.admission_sweep(1)`)
	wantError("SR503", `SELECT swarmpg.task_recover(gen_random_uuid(),'must be blocked')`)
	must(`UPDATE swarmpg.recovery_control SET generation=2`)
	fence(4294967399, 8589934593)
	must(`UPDATE swarmpg.recovery_control SET generation=2147483647`)
	fence(0, 9223372032559808513)
	fence(9223372036854775806, 9223372036854775807)
	wantError("SR409", `SELECT swarmpg._next_fence(9223372036854775807)`)

	// No runtime role can activate, reconcile, inspect global state or generate
	// arbitrary fences. Only the narrow pin check is callable by the gateway.
	for _, signature := range []string{
		"swarmpg.recovery_activate(text,bigint,bigint,text,text)",
		"swarmpg.recovery_resume(text,bigint,bigint,text)",
		"swarmpg.recovery_resolve_outbox(uuid,uuid,text,text)",
		"swarmpg.recovery_status()", "swarmpg._recovery_guard()", "swarmpg._next_fence(bigint)",
	} {
		var allowed bool
		if err := tx.QueryRow(ctx, `SELECT has_function_privilege('agent_runtime',$1,'EXECUTE')`, signature).Scan(&allowed); err != nil || allowed {
			t.Fatalf("runtime privilege %s = %v, %v", signature, allowed, err)
		}
	}
	var allowed bool
	if err := tx.QueryRow(ctx, `SELECT has_function_privilege('agent_runtime','swarmpg.recovery_check(text,bigint,bigint)','EXECUTE')`).Scan(&allowed); err != nil || !allowed {
		t.Fatalf("runtime pin check = %v, %v", allowed, err)
	}
}

func TestRecoverySQLReadOnlyWALBarrierIntegration(t *testing.T) {
	dsn := os.Getenv("SWARMPG_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SWARMPG_TEST_DATABASE_URL to a disposable initialized database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var lsn string
	if err := tx.QueryRow(ctx, `SELECT pg_logical_emit_message(true,'swarmpg.recovery','')::text`).Scan(&lsn); err != nil || lsn == "" {
		t.Fatalf("read-only transaction barrier: %q, %v", lsn, err)
	}
}
