package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const outboxEffects = `[{"key":"notify","destination":"webhook","payload":{"value":42}}]`

func outboxSQLAdmin(f *sharedMemoryFixture, sql string, args ...any) (map[string]any, error) {
	var raw []byte
	if err := f.g.admin.QueryRow(context.Background(), sql, args...).Scan(&raw); err != nil {
		return nil, err
	}
	var out map[string]any
	err := json.Unmarshal(raw, &out)
	return out, err
}

func outboxSQLMustAdmin(t *testing.T, f *sharedMemoryFixture, sql string, args ...any) map[string]any {
	t.Helper()
	out, err := outboxSQLAdmin(f, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func outboxSQLTask(t *testing.T, f *sharedMemoryFixture, effects string) (map[string]any, map[string]any) {
	t.Helper()
	task := coordinationEnqueue(t, f, "outbox", nil, nil)
	claim := coordinationClaim(t, f, 1, 30000)
	if effects == "" {
		return task, claim
	}
	out, err := taskSQLCall(t, f, 1, "SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42',$3::jsonb)", task["id"], claim["fence"], effects)
	if err != nil {
		t.Fatal(err)
	}
	return out["task"].(map[string]any), claim
}

func outboxSQLClaim(t *testing.T, f *sharedMemoryFixture, lease int) map[string]any {
	t.Helper()
	return outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_claim($1,'webhook',$2,gen_random_uuid(),$3)", f.agents[0].OrgID, f.agents[1].ID, lease)
}

func outboxSQLNoIntents(t *testing.T, f *sharedMemoryFixture, task map[string]any) {
	t.Helper()
	var state string
	var messages, history int
	err := f.g.admin.QueryRow(context.Background(), `SELECT state,
		(SELECT count(*) FROM swarmpg.outbox_messages WHERE task_id=t.id),
		(SELECT count(*) FROM swarmpg.task_history WHERE task_id=t.id AND event='completed')
		FROM swarmpg.tasks t WHERE id=$1`, task["id"]).Scan(&state, &messages, &history)
	if err != nil || state != "claimed" || messages != 0 || history != 0 {
		t.Fatalf("failed completion persisted partial state: state=%s messages=%d completed=%d err=%v", state, messages, history, err)
	}
}

func TestOutboxSQLCanonicalCompletionIntegration(t *testing.T) {
	f := coordinationFixture(t)
	effects := `[{"key":"z","destination":"webhook","payload":null},{"key":"a","destination":"webhook","payload":{"n":1}}]`
	task, claim := outboxSQLTask(t, f, effects)
	canonical := `[{"payload":{"n":1},"destination":"webhook","key":"a"},{"key":"z","payload":null,"destination":"webhook"}]`
	if task["completion_effects"].([]any)[0].(map[string]any)["key"] != "a" {
		t.Fatalf("effect set is not canonically ordered: %+v", task)
	}
	replay, err := taskSQLCall(t, f, 1, "SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42',$3::jsonb)", task["id"], claim["fence"], canonical)
	if err != nil || replay["replayed"] != true || !reflect.DeepEqual(task, replay["task"]) {
		t.Fatalf("canonical retry changed completion receipt: %+v %v", replay, err)
	}
	for _, sql := range []string{
		"SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42')",
		"SELECT swarmpg.task_complete($1::uuid,$2::bigint,'43','[]')",
		"SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42','[]')",
	} {
		_, err := taskSQLCall(t, f, 1, sql, task["id"], claim["fence"])
		taskSQLState(t, err, "PT409")
	}
	visible, err := taskSQLCall(t, f, 2, "SELECT swarmpg.task_outbox($1)", task["id"])
	if err != nil || len(visible["messages"].([]any)) != 2 {
		t.Fatalf("same-org agent cannot see exactly two intents: %+v %v", visible, err)
	}
	m := outboxSQLClaim(t, f, 30000)["message"].(map[string]any)
	wantKey := "swarmpg:" + f.agents[0].OrgID + ":" + m["id"].(string)
	if m["idempotency_key"] != wantKey || m["fence"] != "1" || m["max_attempts"] != float64(10) {
		t.Fatalf("unexpected stable delivery identity: %+v", m)
	}
	outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_ack($1,$2,$3,$4::bigint,'{\"status\":200}')", f.agents[0].OrgID, m["id"], f.agents[1].ID, m["fence"])
	replay, err = taskSQLCall(t, f, 1, "SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42',$3::jsonb)", task["id"], claim["fence"], canonical)
	if err != nil || !reflect.DeepEqual(task, replay["task"]) {
		t.Fatalf("delivery mutated immutable completion receipt: %+v %v", replay, err)
	}
	var messages, transitions int
	if err := f.g.admin.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM swarmpg.outbox_messages WHERE task_id=$1),
		(SELECT count(*) FROM swarmpg.task_history WHERE task_id=$1 AND event='completed')`, task["id"]).Scan(&messages, &transitions); err != nil || messages != 2 || transitions != 1 {
		t.Fatalf("duplicate completion created duplicate intents/history: %d %d %v", messages, transitions, err)
	}
}

func TestOutboxSQLValidationAndBudgetRollbackIntegration(t *testing.T) {
	f := coordinationFixture(t)
	task, claim := outboxSQLTask(t, f, "")
	largePayload, _ := json.Marshal(strings.Repeat("x", 65536))
	totalPayload, _ := json.Marshal(strings.Repeat("x", 60000))
	oversizeTotal := []string{}
	for i := 0; i < 5; i++ {
		oversizeTotal = append(oversizeTotal, fmt.Sprintf(`{"key":"k%d","destination":"webhook","payload":%s}`, i, totalPayload))
	}
	for _, effects := range []any{
		nil, `null`, `{}`, `[null]`, `[{}]`,
		`[{"key":"x","destination":"webhook"}]`,
		`[{"key":"x","destination":"webhook","payload":1,"extra":2}]`,
		`[{"key":null,"destination":"webhook","payload":1}]`,
		`[{"key":"x","destination":"https://receiver","payload":1}]`,
		`[{"key":"é","destination":"webhook","payload":1}]`,
		`[{"key":"x\n","destination":"webhook","payload":1}]`,
		`[{"key":"x","destination":"webhook","payload":1},{"key":"x","destination":"other","payload":2}]`,
		fmt.Sprintf(`[{"key":"x","destination":"webhook","payload":%s}]`, largePayload),
		"[" + strings.Join(oversizeTotal, ",") + "]",
	} {
		_, err := taskSQLCall(t, f, 1, "SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42',$3::jsonb)", task["id"], claim["fence"], effects)
		taskSQLState(t, err, "PT400")
	}
	_, err := taskSQLCall(t, f, 1, `SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42',
		(SELECT jsonb_agg(jsonb_build_object('key',i::text,'destination','webhook','payload',null)) FROM generate_series(1,33) i))`, task["id"], claim["fence"])
	taskSQLState(t, err, "PT400")
	outboxSQLNoIntents(t, f, task)
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.agents SET max_result_bytes=10 WHERE id=$1", f.agents[1].ID); err != nil {
		t.Fatal(err)
	}
	_, err = taskSQLCall(t, f, 1, "SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42',$3::jsonb)", task["id"], claim["fence"], outboxEffects)
	taskSQLState(t, err, "PT413")
	outboxSQLNoIntents(t, f, task)
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.agents SET max_result_bytes=1048576 WHERE id=$1", f.agents[1].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := taskSQLCall(t, f, 1, "SELECT swarmpg.task_complete($1::uuid,$2::bigint,'42',$3::jsonb)", task["id"], claim["fence"], outboxEffects); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxSQLAuthorityIntegration(t *testing.T) {
	f := coordinationFixture(t)
	task, _ := outboxSQLTask(t, f, outboxEffects)
	for _, sql := range []string{
		"SELECT swarmpg._task_effects('[]')",
		"SELECT swarmpg._outbox_json(NULL::swarmpg.outbox_messages)",
		"SELECT swarmpg._outbox_event(NULL::swarmpg.outbox_messages,'forged',NULL)",
		"SELECT swarmpg._outbox_require_claim(NULL::swarmpg.outbox_messages,NULL,1)",
		"SELECT swarmpg.outbox_claim(NULL,'webhook',NULL,NULL)",
		"SELECT swarmpg.outbox_get(NULL,NULL)",
		"SELECT swarmpg.outbox_renew(NULL,NULL,NULL,1,1000)",
		"SELECT swarmpg.outbox_ack(NULL,NULL,NULL,1,'{}')",
		"SELECT swarmpg.outbox_nack(NULL,NULL,NULL,1,'error',0,false)",
		"SELECT swarmpg.outbox_retry(NULL,NULL,'retry')",
		"DELETE FROM swarmpg.outbox_messages RETURNING '{}'::jsonb",
		"UPDATE swarmpg.outbox_history SET event='forged' RETURNING '{}'::jsonb",
		"SELECT response FROM swarmpg.outbox_claim_requests",
	} {
		_, err := taskSQLCall(t, f, 1, sql)
		taskSQLState(t, err, "42501")
	}
	_, err := taskSQLCall(t, f, 4, "SELECT swarmpg.task_outbox($1)", task["id"])
	taskSQLState(t, err, "PT404")
	for _, table := range []string{"outbox_messages", "outbox_history"} {
		out, err := taskSQLCall(t, f, 4, "SELECT jsonb_build_object('n',count(*)) FROM swarmpg."+table)
		if err != nil || out["n"] != float64(0) {
			t.Fatalf("cross-org outbox read: %+v %v", out, err)
		}
	}
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.agents SET read_only=true WHERE id=$1", f.agents[2].ID); err != nil {
		t.Fatal(err)
	}
	f.agents[2].ReadOnly = true
	if _, err := taskSQLCall(t, f, 2, "SELECT swarmpg.task_outbox($1)", task["id"]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.agents SET max_result_bytes=10 WHERE id=$1", f.agents[2].ID); err != nil {
		t.Fatal(err)
	}
	_, err = taskSQLCall(t, f, 2, "SELECT swarmpg.task_outbox($1)", task["id"])
	taskSQLState(t, err, "PT413")
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.agents SET disabled=true WHERE id=$1", f.agents[2].ID); err != nil {
		t.Fatal(err)
	}
	_, err = taskSQLCall(t, f, 2, "SELECT swarmpg.task_outbox($1)", task["id"])
	taskSQLState(t, err, "42501")
}

func TestOutboxSQLClaimsAttemptsAndAckIntegration(t *testing.T) {
	f := coordinationFixture(t)
	outboxSQLTask(t, f, outboxEffects)
	org, worker := f.agents[0].OrgID, f.agents[1].ID
	request, _ := auditID()
	first := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_claim($1,'webhook',$2,$3,30000)", org, worker, request)
	m := first["message"].(map[string]any)
	replay := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_claim($1,'webhook',$2,$3,30000)", org, worker, request)
	if replay["replayed"] != true || !reflect.DeepEqual(m, replay["message"]) {
		t.Fatalf("claim did not replay original message: %+v", replay)
	}
	_, err := outboxSQLAdmin(f, "SELECT swarmpg.outbox_claim($1,'other',$2,$3,30000)", org, worker, request)
	taskSQLState(t, err, "PT409")
	_, err = outboxSQLAdmin(f, "SELECT swarmpg.outbox_get($1,$2)", f.agents[4].OrgID, m["id"])
	taskSQLState(t, err, "PT404")
	_, err = outboxSQLAdmin(f, "SELECT swarmpg.outbox_ack($1,$2,$3,1,'{}')", org, m["id"], f.agents[2].ID)
	taskSQLState(t, err, "PT410")
	_, err = outboxSQLAdmin(f, "SELECT swarmpg.outbox_ack($1,$2,$3,1,to_jsonb(repeat('x',16384)))", org, m["id"], worker)
	taskSQLState(t, err, "PT400")
	renewed := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_renew($1,$2,$3,1,100)", org, m["id"], worker)
	if renewed["lease_until"].(string) < m["lease_until"].(string) {
		t.Fatal("renew shortened lease")
	}
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.outbox_messages SET max_attempts=2 WHERE id=$1", m["id"]); err != nil {
		t.Fatal(err)
	}
	nacked := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_nack($1,$2,$3,1,'temporary',60000,false)", org, m["id"], worker)
	if nacked["state"] != "pending" || outboxSQLClaim(t, f, 100)["message"] != nil {
		t.Fatal("retry delay was ignored")
	}
	emptyID, _ := auditID()
	empty := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_claim($1,'webhook',$2,$3,100)", org, worker, emptyID)
	if empty["message"] != nil {
		t.Fatal("delayed message was claimed")
	}
	if _, err := f.g.admin.Exec(context.Background(), "UPDATE swarmpg.outbox_messages SET available_at=clock_timestamp() WHERE id=$1", m["id"]); err != nil {
		t.Fatal(err)
	}
	if replay := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_claim($1,'webhook',$2,$3,100)", org, worker, emptyID); replay["message"] != nil || replay["replayed"] != true {
		t.Fatal("empty claim retry silently claimed newly available work")
	}
	second := outboxSQLClaim(t, f, 100)["message"].(map[string]any)
	if second["id"] != m["id"] || second["idempotency_key"] != m["idempotency_key"] || second["fence"] != "2" {
		t.Fatalf("retry changed identity or reused fence: %+v", second)
	}
	time.Sleep(130 * time.Millisecond)
	_, err = outboxSQLAdmin(f, "SELECT swarmpg.outbox_renew($1,$2,$3,2,1000)", org, m["id"], worker)
	taskSQLState(t, err, "PT411")
	if outboxSQLClaim(t, f, 100)["message"] != nil {
		t.Fatal("exhausted expired delivery received another attempt")
	}
	failed := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_get($1,$2)", org, m["id"])
	if failed["state"] != "failed" || failed["attempts"] != float64(2) {
		t.Fatalf("expired final attempt stuck delivering: %+v", failed)
	}
	retried := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_retry($1,$2,'operator retry')", org, m["id"])
	if retried["attempts"] != float64(0) || retried["total_attempts"] != float64(2) || retried["idempotency_key"] != m["idempotency_key"] {
		t.Fatalf("admin retry lost lifetime counters/identity: %+v", retried)
	}
	last := outboxSQLClaim(t, f, 30000)["message"].(map[string]any)
	ack := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_ack($1,$2,$3,$4::bigint,'null')", org, m["id"], worker, last["fence"])
	if ack["replayed"] != false || ack["message"].(map[string]any)["state"] != "delivered" {
		t.Fatal("valid final acknowledgement failed")
	}
	ackReplay := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_ack($1,$2,$3,$4::bigint,'null')", org, m["id"], worker, last["fence"])
	if ackReplay["replayed"] != true || !reflect.DeepEqual(ack["message"], ackReplay["message"]) {
		t.Fatal("acknowledgement did not replay exactly")
	}
	_, err = outboxSQLAdmin(f, "SELECT swarmpg.outbox_ack($1,$2,$3,$4::bigint,'{}')", org, m["id"], worker, last["fence"])
	taskSQLState(t, err, "PT409")
	_, err = outboxSQLAdmin(f, "SELECT swarmpg.outbox_retry($1,$2,'cannot rewrite delivered')", org, m["id"])
	taskSQLState(t, err, "PT409")
	_, err = outboxSQLAdmin(f, "SELECT swarmpg.outbox_ack($1,$2,$3,1,'null')", org, m["id"], worker)
	taskSQLState(t, err, "PT410")
}

func TestOutboxSQLClaimAndAckRacesIntegration(t *testing.T) {
	f := coordinationFixture(t)
	outboxSQLTask(t, f, outboxEffects)
	request, _ := auditID()
	results := make(chan map[string]any, 2)
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			out, err := outboxSQLAdmin(f, "SELECT swarmpg.outbox_claim($1,'webhook',$2,$3,30000)", f.agents[0].OrgID, f.agents[1].ID, request)
			results <- out
			errors <- err
		}()
	}
	first, second := <-results, <-results
	for i := 0; i < 2; i++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(first["message"], second["message"]) || first["replayed"] == second["replayed"] {
		t.Fatalf("same-request race created different deliveries: %+v %+v", first, second)
	}
	m := first["message"].(map[string]any)
	for i := 0; i < 2; i++ {
		go func() {
			out, err := outboxSQLAdmin(f, "SELECT swarmpg.outbox_ack($1,$2,$3,1,'{\"status\":200}')", f.agents[0].OrgID, m["id"], f.agents[1].ID)
			results <- out
			errors <- err
		}()
	}
	first, second = <-results, <-results
	for i := 0; i < 2; i++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if first["replayed"] == second["replayed"] {
		t.Fatal("ack race did not produce one transition and one replay")
	}
	var deliveries int
	if err := f.g.admin.QueryRow(context.Background(), "SELECT count(*) FROM swarmpg.outbox_history WHERE message_id=$1 AND event='delivered'", m["id"]).Scan(&deliveries); err != nil || deliveries != 1 {
		t.Fatalf("ack race created duplicate transitions: %d %v", deliveries, err)
	}
}

func TestOutboxSQLLeaseAfterLockWaitIntegration(t *testing.T) {
	f := coordinationFixture(t)
	outboxSQLTask(t, f, outboxEffects)
	m := outboxSQLClaim(t, f, 200)["message"].(map[string]any)
	ctx := context.Background()
	blocker, err := f.g.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err := blocker.Exec(ctx, "SELECT 1 FROM swarmpg.outbox_messages WHERE id=$1 FOR UPDATE", m["id"]); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := outboxSQLAdmin(f, "SELECT swarmpg.outbox_ack($1,$2,$3,1,'{}')", f.agents[0].OrgID, m["id"], f.agents[1].ID)
		done <- err
	}()
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("ack did not wait for locked delivery: %v", err)
	default:
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	taskSQLState(t, <-done, "PT411")
	saved := outboxSQLMustAdmin(t, f, "SELECT swarmpg.outbox_get($1,$2)", f.agents[0].OrgID, m["id"])
	if saved["state"] != "delivering" || saved["revision"] != m["revision"] {
		t.Fatal("expired acknowledgement modified delivery")
	}
}

func TestOutboxSQLCompletionInputRaceIntegration(t *testing.T) {
	for _, scenario := range []string{"changed-input", "expired-lease"} {
		t.Run(scenario, func(t *testing.T) {
			f := coordinationFixture(t)
			ctx := context.Background()
			if _, err := f.g.memoryPut(ctx, f.agents[0], "input", json.RawMessage(`1`), true, nil); err != nil {
				t.Fatal(err)
			}
			task := coordinationEnqueue(t, f, scenario, nil, []map[string]any{{"agent_id": f.agents[0].ID, "key": "input", "version": 1}})
			lease := 30000
			if scenario == "expired-lease" {
				lease = 200
			}
			claim := coordinationClaim(t, f, 1, lease)
			fence, _ := strconv.ParseInt(claim["fence"].(string), 10, 64)
			blocker, err := f.g.admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(ctx)
			if scenario == "changed-input" {
				_, err = blocker.Exec(ctx, "UPDATE swarmpg.memory SET value='2' WHERE agent_id=$1 AND key='input'", f.agents[0].ID)
			} else {
				_, err = blocker.Exec(ctx, "SELECT 1 FROM swarmpg.memory WHERE agent_id=$1 AND key='input' FOR UPDATE", f.agents[0].ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- f.g.withConn(ctx, f.agents[1], func(ctx context.Context, tx pgx.Tx) error {
					var raw []byte
					return tx.QueryRow(ctx, "SELECT swarmpg.task_complete($1,$2,'42',$3::jsonb)", task["id"], fence, outboxEffects).Scan(&raw)
				})
			}()
			time.Sleep(300 * time.Millisecond)
			select {
			case err := <-done:
				t.Fatalf("completion did not wait for locked input: %v", err)
			default:
			}
			if err := blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			state := "PT412"
			if scenario == "expired-lease" {
				state = "PT411"
			}
			taskSQLState(t, <-done, state)
			outboxSQLNoIntents(t, f, task)
		})
	}
}
