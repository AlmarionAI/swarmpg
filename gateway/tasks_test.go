package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Use the production HTTP handlers and runtime role against the opt-in,
// disposable integration database. All data belongs to generated fixture orgs.
func coordinationFixture(t *testing.T) *sharedMemoryFixture {
	t.Helper()
	f := newSharedMemoryFixture(t)
	// These races exercise database ownership, independently of per-agent
	// HTTP rate/concurrency admission (covered by the fleet workload).
	for i, a := range f.agents {
		f.g.cache.Store(fmt.Sprintf("%x", hashToken(f.tokens[i])), &cachedAgent{a: a, at: time.Now()})
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"task_claim_requests", "task_history", "tasks"} {
			if _, err := f.g.admin.Exec(ctx, "DELETE FROM swarmpg."+table+" WHERE org_id=ANY($1::uuid[])", []string{f.agents[0].OrgID, f.agents[4].OrgID}); err != nil {
				t.Errorf("cleanup %s: %v", table, err)
			}
		}
	})
	return f
}

func coordinationCall(f *sharedMemoryFixture, who int, method, path string, body any) (int, map[string]any) {
	b, _ := json.Marshal(body)
	r := httptest.NewRequest(method, path, bytes.NewReader(b))
	if who >= 0 {
		r.Header.Set("Authorization", "Bearer "+f.tokens[who])
	} else {
		r.Header.Set("X-Admin-Key", f.g.adminKey)
	}
	w := httptest.NewRecorder()
	f.g.routes().ServeHTTP(w, r)
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		out = map[string]any{"invalid_response": w.Body.String()}
	}
	return w.Code, out
}

func coordinationOK(t *testing.T, f *sharedMemoryFixture, who int, method, path string, body any) map[string]any {
	t.Helper()
	status, out := coordinationCall(f, who, method, path, body)
	if status != http.StatusOK {
		t.Fatalf("%s %s: HTTP %d: %+v", method, path, status, out)
	}
	return out
}

func coordinationError(t *testing.T, f *sharedMemoryFixture, who int, method, path string, body any, status int, code string) {
	t.Helper()
	gotStatus, out := coordinationCall(f, who, method, path, body)
	e, _ := out["error"].(map[string]any)
	if gotStatus != status || e["code"] != code {
		t.Fatalf("%s %s: want %d/%s, got %d %+v", method, path, status, code, gotStatus, out)
	}
}

func coordinationEnqueue(t *testing.T, f *sharedMemoryFixture, key string, deps []string, inputs []map[string]any) map[string]any {
	t.Helper()
	if deps == nil {
		deps = []string{}
	}
	if inputs == nil {
		inputs = []map[string]any{}
	}
	out := coordinationOK(t, f, 0, "POST", "/v1/tasks", map[string]any{"queue": "test", "key": key, "payload": map[string]any{"n": 1}, "dependencies": deps, "inputs": inputs})
	return out["task"].(map[string]any)
}

func coordinationClaim(t *testing.T, f *sharedMemoryFixture, who, lease int) map[string]any {
	t.Helper()
	id, err := auditID()
	if err != nil {
		t.Fatal(err)
	}
	out := coordinationOK(t, f, who, "POST", "/v1/tasks/claim", map[string]any{"queue": "test", "request_id": id, "lease_ms": lease})
	if out["task"] == nil {
		return nil
	}
	return out["task"].(map[string]any)
}

func coordinationPath(task map[string]any, action string) string {
	path := "/v1/tasks/" + task["id"].(string)
	if action != "" {
		path += "/" + action
	}
	return path
}

func TestTaskLifecycleAndAtomicReplayIntegration(t *testing.T) {
	f := coordinationFixture(t)
	first := coordinationEnqueue(t, f, "first", nil, nil)
	child := coordinationEnqueue(t, f, "child", []string{first["id"].(string)}, nil)
	claim := coordinationClaim(t, f, 1, 30000)
	if claim == nil || claim["id"] != first["id"] || claim["fence"] != "1" {
		t.Fatalf("wrong first claim: %+v", claim)
	}
	if blocked := coordinationClaim(t, f, 2, 30000); blocked != nil {
		t.Fatalf("dependency claimed before completion: %+v", blocked)
	}
	complete := map[string]any{"fence": claim["fence"], "result": map[string]any{"answer": 42}}
	out := coordinationOK(t, f, 1, "POST", coordinationPath(first, "complete"), complete)
	if out["replayed"] != false || out["task"].(map[string]any)["state"] != "completed" {
		t.Fatalf("invalid completed result: %+v", out)
	}
	replay := coordinationOK(t, f, 1, "POST", coordinationPath(first, "complete"), complete)
	if replay["replayed"] != true {
		t.Fatalf("completion retry not replayed: %+v", replay)
	}
	coordinationError(t, f, 1, "POST", coordinationPath(first, "complete"), map[string]any{"fence": claim["fence"], "result": 43}, 409, "task_conflict")
	coordinationError(t, f, 2, "POST", coordinationPath(first, "complete"), complete, 409, "stale_claim")
	next := coordinationClaim(t, f, 2, 30000)
	if next == nil || next["id"] != child["id"] {
		t.Fatalf("completed dependency did not unlock child: %+v", next)
	}
	history := coordinationOK(t, f, 0, "GET", coordinationPath(first, "history"), nil)["events"].([]any)
	completed := 0
	for _, e := range history {
		if e.(map[string]any)["event"] == "completed" {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("expected one completion history event, got %+v", history)
	}
	coordinationError(t, f, 0, "POST", coordinationPath(first, "cancel"), map[string]any{}, 409, "task_conflict")
}

func TestTaskConcurrentClaimsAndRequestReplayIntegration(t *testing.T) {
	f := coordinationFixture(t)
	task := coordinationEnqueue(t, f, "race", nil, nil)
	const contenders = 24
	var wg sync.WaitGroup
	type response struct {
		status int
		out    map[string]any
	}
	results := make(chan response, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(who int) {
			defer wg.Done()
			id, _ := auditID()
			code, out := coordinationCall(f, who, "POST", "/v1/tasks/claim", map[string]any{"queue": "test", "request_id": id, "lease_ms": 30000})
			results <- response{code, out}
		}(i % 4)
	}
	wg.Wait()
	close(results)
	winners := 0
	for r := range results {
		if r.status != 200 {
			t.Fatalf("claim race: %d %+v", r.status, r.out)
		}
		if r.out["task"] != nil {
			winners++
			if r.out["task"].(map[string]any)["id"] != task["id"] {
				t.Fatal("wrong task")
			}
		}
	}
	if winners != 1 {
		t.Fatalf("competing claims acquired %d owners", winners)
	}
	// A duplicate request must not consume a second task, including when the
	// original reply was lost and both copies arrive concurrently.
	second := coordinationEnqueue(t, f, "second", nil, nil)
	coordinationEnqueue(t, f, "third", nil, nil)
	id, _ := auditID()
	request := map[string]any{"queue": "test", "request_id": id, "lease_ms": 30000}
	results = make(chan response, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, out := coordinationCall(f, 0, "POST", "/v1/tasks/claim", request)
			results <- response{code, out}
		}()
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r.status != 200 || r.out["task"] == nil || r.out["task"].(map[string]any)["id"] != second["id"] {
			t.Fatalf("duplicate claim consumed another task: %d %+v", r.status, r.out)
		}
	}
	request["lease_ms"] = 20000
	coordinationError(t, f, 0, "POST", "/v1/tasks/claim", request, 409, "task_conflict")
}

func TestTaskLeaseExpiryFencingAndRecoveryIntegration(t *testing.T) {
	f := coordinationFixture(t)
	f.g.adminKey = "task-test-admin"
	task := coordinationEnqueue(t, f, "expiry", nil, nil)
	claim := coordinationClaim(t, f, 1, 100)
	time.Sleep(150 * time.Millisecond)
	for _, action := range []string{"renew", "complete"} {
		body := map[string]any{"fence": claim["fence"]}
		if action == "renew" {
			body["lease_ms"] = 1000
		} else {
			body["result"] = 1
		}
		coordinationError(t, f, 1, "POST", coordinationPath(task, action), body, 409, "lease_expired")
	}
	newClaim := coordinationClaim(t, f, 2, 30000)
	if newClaim == nil || newClaim["id"] != task["id"] || newClaim["fence"] != "2" {
		t.Fatalf("expired claim not reclaimed with fresh fence: %+v", newClaim)
	}
	for _, action := range []string{"renew", "release", "complete"} {
		body := map[string]any{"fence": claim["fence"]}
		if action == "renew" {
			body["lease_ms"] = 1000
		}
		if action == "complete" {
			body["result"] = 1
		}
		coordinationError(t, f, 1, "POST", coordinationPath(task, action), body, 409, "stale_claim")
	}
	renewed := coordinationOK(t, f, 2, "POST", coordinationPath(task, "renew"), map[string]any{"fence": newClaim["fence"], "lease_ms": 30000})
	if renewed["fence"] != newClaim["fence"] {
		t.Fatal("renew changed fencing token")
	}
	coordinationOK(t, f, -1, "POST", "/v1/admin/tasks/"+task["id"].(string)+"/recover", map[string]any{"reason": "operator recovery"})
	coordinationError(t, f, 2, "POST", coordinationPath(task, "complete"), map[string]any{"fence": newClaim["fence"], "result": 1}, 409, "stale_claim")
	recovered := coordinationClaim(t, f, 3, 30000)
	oldFence, _ := strconv.ParseInt(newClaim["fence"].(string), 10, 64)
	newFence, _ := strconv.ParseInt(recovered["fence"].(string), 10, 64)
	if newFence <= oldFence {
		t.Fatal("recovery reused fence")
	}
	coordinationOK(t, f, 3, "POST", coordinationPath(task, "release"), map[string]any{"fence": recovered["fence"]})
	coordinationError(t, f, 1, "POST", coordinationPath(task, "cancel"), map[string]any{}, 403, "42501")
	cancelled := coordinationOK(t, f, 0, "POST", coordinationPath(task, "cancel"), map[string]any{})
	if cancelled["state"] != "cancelled" || coordinationClaim(t, f, 2, 1000) != nil {
		t.Fatal("cancelled task remained eligible")
	}
}

func TestTaskInputGuardLocksAndRollsBackIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx := context.Background()
	for _, scenario := range []string{"changed", "expiry-during-lock"} {
		t.Run(scenario, func(t *testing.T) {
			key := "input-" + scenario
			if _, err := f.g.memoryPut(ctx, f.agents[0], key, json.RawMessage(`{"n":1}`), true, nil); err != nil {
				t.Fatal(err)
			}
			task := coordinationEnqueue(t, f, scenario, nil, []map[string]any{{"agent_id": f.agents[0].ID, "key": key, "version": 1}})
			lease := 30000
			if scenario == "expiry-during-lock" {
				lease = 200
			}
			claim := coordinationClaim(t, f, 1, lease)
			tx, err := f.g.admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if scenario == "changed" {
				_, err = tx.Exec(ctx, `UPDATE swarmpg.memory SET value='{"n":2}' WHERE agent_id=$1 AND key=$2`, f.agents[0].ID, key)
			} else {
				_, err = tx.Exec(ctx, `SELECT 1 FROM swarmpg.memory WHERE agent_id=$1 AND key=$2 FOR UPDATE`, f.agents[0].ID, key)
			}
			if err != nil {
				t.Fatal(err)
			}
			type answer struct {
				status int
				out    map[string]any
			}
			done := make(chan answer, 1)
			go func() {
				code, out := coordinationCall(f, 1, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": 42})
				done <- answer{code, out}
			}()
			wait := 50 * time.Millisecond
			if scenario == "expiry-during-lock" {
				wait = 300 * time.Millisecond
			}
			time.Sleep(wait)
			select {
			case got := <-done:
				t.Fatalf("completion did not lock its input: %+v", got)
			default:
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			got := <-done
			want := "inputs_changed"
			if scenario == "expiry-during-lock" {
				want = "lease_expired"
			}
			e, _ := got.out["error"].(map[string]any)
			if got.status != 409 || e["code"] != want {
				t.Fatalf("input/lease guard: %d %+v, want %s", got.status, got.out, want)
			}
			saved := coordinationOK(t, f, 0, "GET", coordinationPath(task, ""), nil)
			if saved["state"] != "claimed" || saved["result"] != nil {
				t.Fatalf("failed completion partially persisted: %+v", saved)
			}
			coordinationOK(t, f, 0, "POST", coordinationPath(task, "cancel"), map[string]any{})
		})
	}
}

func TestTaskAuthorityAndSQLBypassIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx := context.Background()
	task := coordinationEnqueue(t, f, "authority", nil, nil)
	coordinationOK(t, f, 1, "GET", coordinationPath(task, ""), nil)
	for _, suffix := range []string{"", "history"} {
		coordinationError(t, f, 4, "GET", coordinationPath(task, suffix), nil, 404, "not_found")
	}
	coordinationError(t, f, 4, "POST", coordinationPath(task, "cancel"), map[string]any{}, 404, "not_found")
	for _, sql := range []string{
		"UPDATE swarmpg.tasks SET state='completed'",
		"DELETE FROM swarmpg.task_history",
		"INSERT INTO swarmpg.task_claim_requests DEFAULT VALUES",
		fmt.Sprintf("SELECT swarmpg.task_recover('%s','spoofed')", task["id"]),
	} {
		_, err := f.g.query(ctx, f.agents[1], QueryRequest{SQL: sql})
		if err == nil || err.Code != "42501" {
			t.Fatalf("direct SQL authority bypass %s: %+v", sql, err)
		}
	}
	rows, err := f.g.query(ctx, f.agents[4], QueryRequest{SQL: "SELECT * FROM swarmpg.tasks"})
	if err != nil || rows.RowCount != 0 {
		t.Fatalf("cross-org SQL read: %+v %+v", rows, err)
	}
	if _, err := f.g.admin.Exec(ctx, `UPDATE swarmpg.agents SET read_only=true WHERE id=$1`, f.agents[2].ID); err != nil {
		t.Fatal(err)
	}
	// Bypass cached REST read_only metadata: the SQL function must still reject.
	_, err = f.g.query(ctx, f.agents[2], QueryRequest{SQL: "SELECT swarmpg.task_claim('test',gen_random_uuid(),1000)"})
	if err == nil || err.Code != "42501" {
		t.Fatalf("read-only registry bypass: %+v", err)
	}
	if _, err := f.g.memoryPut(ctx, f.agents[3], "private", json.RawMessage(`1`), false, nil); err != nil {
		t.Fatal(err)
	}
	coordinationError(t, f, 0, "POST", "/v1/tasks", map[string]any{"queue": "test", "key": "private-input", "payload": nil, "inputs": []any{map[string]any{"agent_id": f.agents[3].ID, "key": "private", "version": 1}}}, 404, "not_found")
}

func TestTaskResponseBudgetRollsBackAndFenceIsExactIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx := context.Background()
	// Cache a deliberately tight response budget without changing server-global limits.
	f.agents[0].MaxResultBytes = 10
	f.g.cache.Store(fmt.Sprintf("%x", hashToken(f.tokens[0])), &cachedAgent{a: f.agents[0], at: time.Now()})
	coordinationError(t, f, 0, "POST", "/v1/tasks", map[string]any{"queue": "test", "key": "too-large", "payload": 1}, 413, "result_limit_exceeded")
	var count int
	if err := f.g.admin.QueryRow(ctx, `SELECT count(*) FROM swarmpg.tasks WHERE org_id=$1`, f.agents[0].OrgID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("oversized mutation receipt committed: %d %v", count, err)
	}
	f.agents[0].MaxResultBytes = 1 << 20
	task := coordinationEnqueue(t, f, "precise-fence", nil, nil)
	if _, err := f.g.admin.Exec(ctx, `UPDATE swarmpg.tasks SET fence=9007199254740991 WHERE id=$1`, task["id"]); err != nil {
		t.Fatal(err)
	}
	claim := coordinationClaim(t, f, 1, 30000)
	if claim["fence"] != "9007199254740992" {
		t.Fatalf("fencing token lost integer precision: %+v", claim)
	}
	coordinationOK(t, f, 1, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": nil})
}

func TestTaskMCPAndKernelBudgetIntegration(t *testing.T) {
	f := coordinationFixture(t)
	ctx := context.Background()
	call := func(name, args string) map[string]any {
		t.Helper()
		tool, ok := mcpToolsByName[name]
		if !ok {
			t.Fatalf("missing MCP tool %s", name)
		}
		out := tool.run(f.g, ctx, f.agents[0], json.RawMessage(args))
		if out["isError"] == true {
			t.Fatalf("MCP %s: %+v", name, out)
		}
		b, _ := json.Marshal(out["structuredContent"])
		var parsed map[string]any
		json.Unmarshal(b, &parsed)
		return parsed
	}
	task := call("task_enqueue", `{"queue":"test","key":"mcp","payload":{"n":1}}`)["task"].(map[string]any)
	id, _ := auditID()
	claim := call("task_claim", fmt.Sprintf(`{"queue":"test","request_id":%q}`, id))["task"].(map[string]any)
	out := call("task_complete", fmt.Sprintf(`{"id":%q,"fence":%q,"result":42}`, task["id"], claim["fence"]))
	if out["task"].(map[string]any)["state"] != "completed" {
		t.Fatalf("MCP completion: %+v", out)
	}
	if !f.g.sealed {
		return
	}
	pending := coordinationEnqueue(t, f, "budget", nil, nil)
	tight := *f.agents[1]
	tight.MaxRows = 2
	err := f.g.withConn(ctx, &tight, func(ctx context.Context, tx pgx.Tx) error {
		var out []byte
		return tx.QueryRow(ctx, `SELECT swarmpg.task_claim('test',gen_random_uuid(),30000)`).Scan(&out)
	})
	if err == nil || classifyPg(err).Code != "53400" {
		t.Fatalf("coordination escaped cumulative kernel budget: %v", err)
	}
	saved := coordinationOK(t, f, 0, "GET", coordinationPath(pending, ""), nil)
	if saved["state"] != "ready" || saved["fence"] != "0" {
		t.Fatalf("budget failure left a partial claim: %+v", saved)
	}
}
