package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Exercise SQL functions directly under the runtime role, independently of
// REST validation and cached agent metadata. Each call is its own transaction.
func taskSQLCall(t *testing.T, f *sharedMemoryFixture, who int, sql string, args ...any) (map[string]any, error) {
	t.Helper()
	var raw []byte
	err := f.g.withConn(context.Background(), f.agents[who], func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, sql, args...).Scan(&raw)
	})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("SQL returned invalid JSON: %s: %v", raw, err)
	}
	return out, nil
}

func taskSQLState(t *testing.T, err error, state string) {
	t.Helper()
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != state {
		t.Fatalf("want SQLSTATE %s, got %v", state, err)
	}
}

func TestTaskSQLValidationIntegration(t *testing.T) {
	f := coordinationFixture(t)
	uuid := f.agents[0].ID
	input := fmt.Sprintf(`{"agent_id":%q,"key":"k","version":1}`, uuid)
	for _, tc := range []struct {
		name, deps, inputs string
	}{
		{"null dependencies", "NULL", "'[]'"},
		{"null dependency element", "ARRAY[NULL]::uuid[]", "'[]'"},
		{"multidimensional dependencies", "ARRAY[[gen_random_uuid()],[gen_random_uuid()]]", "'[]'"},
		{"duplicate dependencies", "ARRAY[$2::uuid,$2::uuid]", "'[]'"},
		{"too many dependencies", "ARRAY(SELECT gen_random_uuid() FROM generate_series(1,65))", "'[]'"},
		{"SQL null inputs", "'{}'", "NULL"},
		{"JSON null inputs", "'{}'", "'null'"},
		{"object inputs", "'{}'", "'{}'"},
		{"scalar inputs", "'{}'", "'1'"},
		{"null input entry", "'{}'", "'[null]'"},
		{"missing input fields", "'{}'", "'[{}]'"},
		{"unknown input fields", "'{}'", fmt.Sprintf("'[%s]'", input[:len(input)-1]+`,"extra":true}`)},
		{"malformed input UUID", "'{}'", `'[{"agent_id":"invalid","key":"k","version":1}]'`},
		{"noninteger input version", "'{}'", fmt.Sprintf(`'[{"agent_id":%q,"key":"k","version":1.5}]'`, uuid)},
		{"string input version", "'{}'", fmt.Sprintf(`'[{"agent_id":%q,"key":"k","version":"1"}]'`, uuid)},
		{"overflow input version", "'{}'", fmt.Sprintf(`'[{"agent_id":%q,"key":"k","version":9223372036854775808}]'`, uuid)},
		{"null input key", "'{}'", fmt.Sprintf(`'[{"agent_id":%q,"key":null,"version":1}]'`, uuid)},
		{"duplicate input references", "'{}'", fmt.Sprintf("'[%s,%s]'", input, input)},
		{"too many inputs", "'{}'", "(SELECT jsonb_agg(jsonb_build_object('agent_id',$2::text,'key',i::text,'version',1)) FROM generate_series(1,65) i)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Referencing $2 in an always-true predicate keeps the parameter count
			// constant for the cases that embed the fixture identity.
			sql := fmt.Sprintf("SELECT swarmpg.task_enqueue('raw',$1,'{}',%s,%s) WHERE $2::uuid IS NOT NULL", tc.deps, tc.inputs)
			_, err := taskSQLCall(t, f, 0, sql, tc.name, uuid)
			taskSQLState(t, err, "PT400")
		})
	}
	var count int
	if err := f.g.admin.QueryRow(context.Background(), "SELECT count(*) FROM swarmpg.tasks WHERE org_id=$1", f.agents[0].OrgID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid SQL requests left task rows: count=%d error=%v", count, err)
	}
}

func TestTaskSQLIdempotencyAndDependenciesIntegration(t *testing.T) {
	f := coordinationFixture(t)
	sql := "SELECT swarmpg.task_enqueue('raw','stable-key',$1::jsonb)"
	first, err := taskSQLCall(t, f, 0, sql, `{"n":1}`)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := taskSQLCall(t, f, 0, sql, `{"n":1}`)
	if err != nil {
		t.Fatal(err)
	}
	firstTask, replayTask := first["task"].(map[string]any), replay["task"].(map[string]any)
	if first["replayed"] != false || replay["replayed"] != true || firstTask["id"] != replayTask["id"] || replayTask["revision"] != float64(1) {
		t.Fatalf("SQL enqueue replay changed the task: first=%+v replay=%+v", first, replay)
	}
	_, err = taskSQLCall(t, f, 0, sql, `{"n":2}`)
	taskSQLState(t, err, "PT409")
	_, err = taskSQLCall(t, f, 1, sql, `{"n":1}`)
	taskSQLState(t, err, "PT409")
	foreign, err := taskSQLCall(t, f, 4, "SELECT swarmpg.task_enqueue('raw','foreign','{}')")
	if err != nil {
		t.Fatal(err)
	}
	foreignID := foreign["task"].(map[string]any)["id"]
	_, err = taskSQLCall(t, f, 0, "SELECT swarmpg.task_enqueue('raw','bad-dependency','{}',ARRAY[$1::uuid])", foreignID)
	taskSQLState(t, err, "PT404")
	var count, history int
	if err := f.g.admin.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM swarmpg.tasks WHERE org_id=$1),
		(SELECT count(*) FROM swarmpg.task_history WHERE org_id=$1)`, f.agents[0].OrgID).Scan(&count, &history); err != nil || count != 1 || history != 1 {
		t.Fatalf("replay/conflict/dependency refusal changed durable state: tasks=%d history=%d err=%v", count, history, err)
	}
}

func TestTaskSQLRegistryAuthorizationIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx := context.Background()
	for i, change := range []string{"disabled=true", "token_expires_at=clock_timestamp()-interval '1 second'", "read_only=true"} {
		who := i + 1
		if _, err := f.g.admin.Exec(ctx, "UPDATE swarmpg.agents SET "+change+" WHERE id=$1", f.agents[who].ID); err != nil {
			t.Fatal(err)
		}
		// withConn still receives the original enabled/writable Agent object.
		// The authoritative SQL registry check must independently reject it.
		for _, sql := range []string{
			"SELECT swarmpg.task_enqueue('raw','unauthorized','{}')",
			"SELECT swarmpg.task_claim('raw',gen_random_uuid(),1000)",
		} {
			_, err := taskSQLCall(t, f, who, sql)
			taskSQLState(t, err, "42501")
		}
	}
	var requests int
	if err := f.g.admin.QueryRow(ctx, "SELECT count(*) FROM swarmpg.task_claim_requests WHERE org_id=$1", f.agents[0].OrgID).Scan(&requests); err != nil || requests != 0 {
		t.Fatalf("unauthorized claim reserved a receipt: count=%d err=%v", requests, err)
	}
}

func TestTaskSQLPrivateHelpersIntegration(t *testing.T) {
	f := coordinationFixture(t)
	for _, sql := range []string{
		"SELECT swarmpg._task_authorize(false)",
		"SELECT swarmpg._task_authorize_locked(true)",
		"SELECT swarmpg._task_response('{}')",
		"SELECT swarmpg._task_json(NULL::swarmpg.tasks)",
		"SELECT swarmpg._task_event(NULL::swarmpg.tasks,'forged',NULL)",
		"SELECT swarmpg._task_lease(1000)",
		"SELECT swarmpg._task_inputs('[]')",
		"SELECT swarmpg._task_check_inputs('[]')",
		"SELECT swarmpg._task_require_claim(NULL::swarmpg.tasks,1)",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := taskSQLCall(t, f, 0, sql)
			taskSQLState(t, err, "42501")
		})
	}
}

func TestTaskSQLAuthorizationAfterLockWaitIntegration(t *testing.T) {
	for _, scenario := range []string{"expiry", "disabled", "read_only"} {
		t.Run(scenario, func(t *testing.T) {
			f := coordinationFixture(t)
			ctx := context.Background()
			task := coordinationEnqueue(t, f, "auth-wait", nil, nil)
			claim := coordinationClaim(t, f, 1, 30000)
			fence, _ := strconv.ParseInt(claim["fence"].(string), 10, 64)
			if scenario == "expiry" {
				if _, err := f.g.admin.Exec(ctx, "UPDATE swarmpg.agents SET token_expires_at=clock_timestamp()+interval '500 milliseconds' WHERE id=$1", f.agents[1].ID); err != nil {
					t.Fatal(err)
				}
			}
			blocker, err := f.g.admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(ctx)
			if _, err := blocker.Exec(ctx, "SELECT 1 FROM swarmpg.tasks WHERE id=$1 FOR UPDATE", task["id"]); err != nil {
				t.Fatal(err)
			}
			pid := make(chan int, 1)
			done := make(chan error, 1)
			go func() {
				done <- f.g.withConn(ctx, f.agents[1], func(ctx context.Context, tx pgx.Tx) error {
					var backend int
					if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&backend); err != nil {
						return err
					}
					pid <- backend
					var raw []byte
					return tx.QueryRow(ctx, "SELECT swarmpg.task_complete($1,$2,'42')", task["id"], fence).Scan(&raw)
				})
			}()
			var backend int
			select {
			case backend = <-pid:
			case err := <-done:
				t.Fatalf("completion stopped before task lock: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("completion did not acquire a runtime connection")
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				var blocked bool
				if err := f.g.admin.QueryRow(ctx, "SELECT cardinality(pg_blocking_pids($1)) > 0", backend).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("completion never waited for the locked task")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if scenario == "expiry" {
				for {
					var expired bool
					if err := f.g.admin.QueryRow(ctx, "SELECT token_expires_at <= clock_timestamp() FROM swarmpg.agents WHERE id=$1", f.agents[1].ID).Scan(&expired); err != nil {
						t.Fatal(err)
					}
					if expired {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("test token did not expire")
					}
					time.Sleep(5 * time.Millisecond)
				}
			} else {
				changeCtx, cancel := context.WithTimeout(ctx, time.Second)
				_, err := f.g.admin.Exec(changeCtx, "UPDATE swarmpg.agents SET "+scenario+"=true WHERE id=$1", f.agents[1].ID)
				cancel()
				if err != nil {
					t.Fatalf("revocation must remain possible while task is locked: %v", err)
				}
			}
			if err := blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				taskSQLState(t, err, "42501")
			case <-time.After(2 * time.Second):
				t.Fatal("completion did not finish after unlocking")
			}
			var state string
			var completions int
			if err := f.g.admin.QueryRow(ctx, `SELECT state,
				(SELECT count(*) FROM swarmpg.task_history WHERE task_id=t.id AND event='completed')
				FROM swarmpg.tasks t WHERE id=$1`, task["id"]).Scan(&state, &completions); err != nil || state != "claimed" || completions != 0 {
				t.Fatalf("revoked completion committed: state=%s completions=%d err=%v", state, completions, err)
			}
		})
	}
}

func TestTaskSQLReceiptBudgetBeforeCommitIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx := context.Background()
	for _, scenario := range []string{"bytes", "rows"} {
		t.Run(scenario, func(t *testing.T) {
			rows, bytes := 1000, 10
			if scenario == "rows" {
				rows, bytes = 0, 1<<20
			}
			if _, err := f.g.admin.Exec(ctx, "UPDATE swarmpg.agents SET max_rows=$2,max_result_bytes=$3 WHERE id=$1", f.agents[0].ID, rows, bytes); err != nil {
				t.Fatal(err)
			}
			// Keep the gateway's cached budgets generous: the SQL function must
			// consult the registry before returning any mutating receipt.
			_, err := f.g.query(ctx, f.agents[0], QueryRequest{SQL: "SELECT swarmpg.task_enqueue('raw','budget','{}')"})
			if err == nil || err.Status != 413 || err.Code != "result_limit_exceeded" {
				t.Fatalf("raw query bypassed registry receipt budget: %+v", err)
			}
			var count int
			if err := f.g.admin.QueryRow(ctx, "SELECT count(*) FROM swarmpg.tasks WHERE org_id=$1", f.agents[0].OrgID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("oversized receipt left an enqueued task: count=%d err=%v", count, err)
			}
		})
	}
}

func TestTaskSQLReadOnlyReadersIntegration(t *testing.T) {
	f := coordinationFixture(t)
	task := coordinationEnqueue(t, f, "readonly-read", nil, nil)
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.agents SET read_only=true WHERE id=$1", f.agents[1].ID); err != nil {
		t.Fatal(err)
	}
	f.agents[1].ReadOnly = true // withConn now starts an actual read-only transaction.
	for _, sql := range []string{"SELECT swarmpg.task_get($1)", "SELECT swarmpg.task_history($1)"} {
		if _, err := taskSQLCall(t, f, 1, sql, task["id"]); err != nil {
			t.Fatalf("read-only task inspection failed: %v", err)
		}
	}
}

func TestTaskSQLRegistryRevocationSerializesWithCommitIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready := make(chan error, 1)
	commit := make(chan struct{}, 1)
	done := make(chan error, 1)
	defer func() { commit <- struct{}{} }()
	go func() {
		done <- f.g.withConn(ctx, f.agents[0], func(ctx context.Context, tx pgx.Tx) error {
			var raw []byte
			err := tx.QueryRow(ctx, "SELECT swarmpg.task_enqueue('raw','serialize','{}')").Scan(&raw)
			ready <- err
			if err != nil {
				return err
			}
			select {
			case <-commit:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	admin, err := f.g.admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Release()
	var pid int
	if err := admin.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	revoked := make(chan error, 1)
	go func() {
		_, err := admin.Exec(ctx, "UPDATE swarmpg.agents SET disabled=true WHERE id=$1", f.agents[0].ID)
		revoked <- err
	}()
	for {
		var blocked bool
		if err := f.g.admin.QueryRow(ctx, "SELECT cardinality(pg_blocking_pids($1)) > 0", pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-revoked:
			t.Fatalf("registry revocation did not wait for admitted mutation to commit: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	commit <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	var count int
	if err := f.g.admin.QueryRow(ctx, "SELECT count(*) FROM swarmpg.tasks WHERE org_id=$1 AND task_key='serialize'", f.agents[0].OrgID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("admitted mutation did not commit before revocation: count=%d err=%v", count, err)
	}
	_, err = taskSQLCall(t, f, 0, "SELECT swarmpg.task_claim('raw',gen_random_uuid())")
	taskSQLState(t, err, "42501")
}

func TestTaskSQLMutatingReceiptRollbackIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx := context.Background()
	task := coordinationEnqueue(t, f, "receipt-rollback", nil, nil)
	setBudget := func(bytes int) {
		t.Helper()
		if _, err := f.g.admin.Exec(ctx, "UPDATE swarmpg.agents SET max_result_bytes=$2 WHERE id=$1", f.agents[1].ID, bytes); err != nil {
			t.Fatal(err)
		}
	}
	queryTooLarge := func(sql string, args ...any) {
		t.Helper()
		q := QueryRequest{SQL: sql}
		for _, arg := range args {
			raw, err := json.Marshal(arg)
			if err != nil {
				t.Fatal(err)
			}
			q.Params = append(q.Params, raw)
		}
		_, err := f.g.query(ctx, f.agents[1], q)
		if err == nil || err.Status != 413 || err.Code != "result_limit_exceeded" {
			t.Fatalf("raw task mutation bypassed receipt budget: %+v", err)
		}
	}
	setBudget(10)
	queryTooLarge("SELECT swarmpg.task_claim('test',gen_random_uuid(),30000)")
	var state string
	var fence int64
	var receipts, completions int
	if err := f.g.admin.QueryRow(ctx, `SELECT state,fence,
		(SELECT count(*) FROM swarmpg.task_claim_requests WHERE org_id=t.org_id)
		FROM swarmpg.tasks t WHERE id=$1`, task["id"]).Scan(&state, &fence, &receipts); err != nil || state != "ready" || fence != 0 || receipts != 0 {
		t.Fatalf("oversized claim committed: state=%s fence=%d receipts=%d err=%v", state, fence, receipts, err)
	}
	setBudget(1 << 20)
	claim := coordinationClaim(t, f, 1, 30000)
	setBudget(10)
	queryTooLarge("SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42')", task["id"], claim["fence"])
	if err := f.g.admin.QueryRow(ctx, `SELECT state,
		(SELECT count(*) FROM swarmpg.task_history WHERE task_id=t.id AND event='completed')
		FROM swarmpg.tasks t WHERE id=$1`, task["id"]).Scan(&state, &completions); err != nil || state != "claimed" || completions != 0 {
		t.Fatalf("oversized completion committed: state=%s completions=%d err=%v", state, completions, err)
	}
	setBudget(1 << 20)
	if _, err := taskSQLCall(t, f, 1, "SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42')", task["id"], claim["fence"]); err != nil {
		t.Fatalf("completion could not retry after rejected receipt: %v", err)
	}
}
