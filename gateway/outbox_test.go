package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestOutboxNormalizedJSONLimitsIntegration(t *testing.T) {
	f := coordinationFixture(t)
	f.g.adminKey = "outbox-test-admin"
	task := coordinationEnqueue(t, f, "json-limits", nil, nil)
	claim := coordinationClaim(t, f, 0, 30000)
	tooLarge := []map[string]any{{"key": "large", "destination": "mail", "payload": strings.Repeat("x", 64<<10)}}
	totalLarge := make([]map[string]any, 5)
	for i := range totalLarge {
		totalLarge[i] = map[string]any{"key": fmt.Sprintf("effect-%d", i), "destination": "mail", "payload": strings.Repeat("x", 60<<10)}
	}
	for _, effects := range [][]map[string]any{tooLarge, totalLarge} {
		coordinationError(t, f, 0, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": nil, "effects": effects}, 400, "bad_request")
		args, _ := json.Marshal(map[string]any{"id": task["id"], "fence": claim["fence"], "result": nil, "effects": effects})
		out := toolTaskComplete(f.g, context.Background(), f.agents[0], args)
		if out["isError"] != true || out["structuredContent"].(map[string]any)["error"].(map[string]any)["code"] != "bad_request" {
			t.Fatalf("MCP JSONB limit not enforced: %+v", out)
		}
	}
	if got := coordinationOK(t, f, 0, "GET", coordinationPath(task, "outbox"), nil)["messages"].([]any); len(got) != 0 {
		t.Fatalf("oversized completion persisted effects: %+v", got)
	}
	// 66,002 wire bytes become 11,002 JSONB bytes. Both completion payloads
	// and acknowledgment responses must use the database's normalized size.
	escaped := json.RawMessage(`"` + strings.Repeat(`\u0061`, 11000) + `"`)
	coordinationOK(t, f, 0, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": nil, "effects": []map[string]any{{"key": "escaped", "destination": "mail", "payload": escaped}}})
	worker, _ := auditID()
	request, _ := auditID()
	message := coordinationOK(t, f, -1, "POST", "/v1/admin/outbox/claim", map[string]any{"org_id": f.agents[0].OrgID, "destination": "mail", "worker_id": worker, "request_id": request})["message"].(map[string]any)
	path := "/v1/admin/outbox/" + message["id"].(string)
	ack := map[string]any{"org_id": f.agents[0].OrgID, "worker_id": worker, "fence": message["fence"], "response": strings.Repeat("x", 16<<10)}
	coordinationError(t, f, -1, "POST", path+"/ack", ack, 400, "bad_request")
	if got := coordinationOK(t, f, -1, "GET", path+"?org_id="+f.agents[0].OrgID, nil); got["state"] != "delivering" {
		t.Fatalf("oversized ack committed: %+v", got)
	}
	ack["response"] = escaped
	if got := coordinationOK(t, f, -1, "POST", path+"/ack", ack)["message"].(map[string]any); got["state"] != "delivered" || got["delivery_response"] != strings.Repeat("a", 11000) {
		t.Fatal("normalized acknowledgment was not committed intact")
	}
}

func outboxMCP(t *testing.T, f *sharedMemoryFixture, who int, name string, args any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	out := mcpToolsByName[name].run(f.g, context.Background(), f.agents[who], raw)
	if out["isError"] == true {
		t.Fatalf("MCP %s: %+v", name, out)
	}
	raw, err = json.Marshal(out["structuredContent"])
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestOutboxCompletionRESTMCPReplayAndVisibilityIntegration(t *testing.T) {
	f := coordinationFixture(t)
	task := coordinationEnqueue(t, f, "effects", nil, nil)
	claim := coordinationClaim(t, f, 1, 30000)
	effects := []map[string]any{
		{"key": "z-null", "destination": "mail", "payload": nil},
		{"key": "a-precise", "destination": "mail", "payload": json.RawMessage(`{"n":9007199254740993}`)},
	}
	completion := coordinationOK(t, f, 1, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": nil, "effects": effects})
	canonical := completion["task"].(map[string]any)["completion_effects"].([]any)
	if len(canonical) != 2 || canonical[0].(map[string]any)["key"] != "a-precise" || completion["replayed"] != false {
		t.Fatalf("effects not normalized in completion: %+v", completion)
	}
	replay := outboxMCP(t, f, 1, "task_complete", map[string]any{"id": task["id"], "fence": claim["fence"], "result": nil, "effects": []map[string]any{effects[1], effects[0]}})
	if replay["replayed"] != true || !reflect.DeepEqual(replay["task"], completion["task"]) {
		t.Fatalf("REST/MCP completion replay differs: %+v", replay)
	}
	rest := coordinationOK(t, f, 2, "GET", coordinationPath(task, "outbox"), nil)
	mcp := outboxMCP(t, f, 2, "task_outbox", map[string]any{"id": task["id"]})
	if !reflect.DeepEqual(rest, mcp) {
		t.Fatalf("outbox transports differ: REST=%+v MCP=%+v", rest, mcp)
	}
	messages := rest["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("completion replay duplicated or lost effects: %+v", messages)
	}
	for _, item := range messages {
		message := item.(map[string]any)
		if message["state"] != "pending" || message["task_id"] != task["id"] || message["idempotency_key"] != "swarmpg:"+f.agents[0].OrgID+":"+message["id"].(string) {
			t.Fatalf("invalid intent: %+v", message)
		}
	}
	// Check precision before decoding to Go's generic JSON numbers.
	raw, apiErr := f.g.taskOutbox(context.Background(), f.agents[2], task["id"].(string))
	if apiErr != nil || !strings.Contains(string(raw), "9007199254740993") {
		t.Fatalf("intent payload lost precision: %s %v", raw, apiErr)
	}
	coordinationError(t, f, 4, "GET", coordinationPath(task, "outbox"), nil, 404, "not_found")
	foreign := mcpToolsByName["task_outbox"].run(f.g, context.Background(), f.agents[4], json.RawMessage(`{"id":"`+task["id"].(string)+`"}`))
	if foreign["isError"] != true {
		t.Fatalf("foreign MCP read exposed intents: %+v", foreign)
	}
	coordinationError(t, f, 1, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": nil, "effects": []any{}}, 409, "task_conflict")

	// Exercise initial completion through MCP as well as replay through MCP.
	second := coordinationEnqueue(t, f, "mcp-effects", nil, nil)
	secondClaim := coordinationClaim(t, f, 3, 30000)
	outboxMCP(t, f, 3, "task_complete", map[string]any{"id": second["id"], "fence": secondClaim["fence"], "result": true, "effects": effects[:1]})
	if got := coordinationOK(t, f, 0, "GET", coordinationPath(second, "outbox"), nil)["messages"].([]any); len(got) != 1 {
		t.Fatalf("MCP completion did not persist one effect: %+v", got)
	}
}

func TestOutboxCompletionResponseBudgetRollsBackIntegration(t *testing.T) {
	f := coordinationFixture(t)
	task := coordinationEnqueue(t, f, "budget-effects", nil, nil)
	claim := coordinationClaim(t, f, 1, 30000)
	effects := []map[string]any{{"key": "notice", "destination": "mail", "payload": strings.Repeat("x", 2048)}}
	// The response failure must roll back the task, result, history and intents.
	f.agents[1].MaxResultBytes = 128
	coordinationError(t, f, 1, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": "answer", "effects": effects}, 413, "result_limit_exceeded")
	saved := coordinationOK(t, f, 0, "GET", coordinationPath(task, ""), nil)
	if saved["state"] != "claimed" || saved["result"] != nil {
		t.Fatalf("failed receipt committed result/state: %+v", saved)
	}
	if messages := coordinationOK(t, f, 0, "GET", coordinationPath(task, "outbox"), nil)["messages"].([]any); len(messages) != 0 {
		t.Fatalf("failed receipt committed effects: %+v", messages)
	}
	for _, event := range coordinationOK(t, f, 0, "GET", coordinationPath(task, "history"), nil)["events"].([]any) {
		if event.(map[string]any)["event"] == "completed" {
			t.Fatal("failed receipt committed completion history")
		}
	}
	f.agents[1].MaxResultBytes = 1 << 20
	coordinationOK(t, f, 1, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": "answer", "effects": effects})
	f.agents[2].MaxResultBytes = 128
	coordinationError(t, f, 2, "GET", coordinationPath(task, "outbox"), nil, 413, "result_limit_exceeded")
}

func TestOutboxAdminLifecycleReplayAndRequeueIntegration(t *testing.T) {
	f := coordinationFixture(t)
	f.g.adminKey = "outbox-test-admin"
	task := coordinationEnqueue(t, f, "delivery", nil, nil)
	claim := coordinationClaim(t, f, 0, 30000)
	coordinationOK(t, f, 0, "POST", coordinationPath(task, "complete"), map[string]any{"fence": claim["fence"], "result": true, "effects": []map[string]any{{"key": "first", "destination": "mail", "payload": nil}, {"key": "second", "destination": "mail", "payload": 2}}})
	worker, _ := auditID()
	request, _ := auditID()
	claimBody := map[string]any{"org_id": f.agents[0].OrgID, "destination": "mail", "worker_id": worker, "request_id": request}
	claimed := coordinationOK(t, f, -1, "POST", "/v1/admin/outbox/claim", claimBody)
	message := claimed["message"].(map[string]any)
	path := "/v1/admin/outbox/" + message["id"].(string)
	if claimed["replayed"] != false || message["fence"] != "1" || message["attempts"] != float64(1) {
		t.Fatalf("invalid delivery claim: %+v", claimed)
	}
	replay := coordinationOK(t, f, -1, "POST", "/v1/admin/outbox/claim", claimBody)
	if replay["replayed"] != true || !reflect.DeepEqual(replay["message"], message) {
		t.Fatalf("claim replay consumed another intent: %+v", replay)
	}
	claimBody["destination"] = "changed"
	coordinationError(t, f, -1, "POST", "/v1/admin/outbox/claim", claimBody, 409, "task_conflict")
	claimBody["destination"] = "mail"
	// Consume the other intent while the first is still leased, so subsequent
	// retry assertions cannot depend on scheduler ordering between two messages.
	request, _ = auditID()
	claimBody["request_id"] = request
	second := coordinationOK(t, f, -1, "POST", "/v1/admin/outbox/claim", claimBody)["message"].(map[string]any)
	if second["id"] == message["id"] {
		t.Fatal("fresh claim returned the still-leased first intent")
	}
	coordinationOK(t, f, -1, "POST", "/v1/admin/outbox/"+second["id"].(string)+"/ack", map[string]any{"org_id": f.agents[0].OrgID, "worker_id": worker, "fence": second["fence"], "response": map[string]any{"status": 200}})
	owner := map[string]any{"org_id": f.agents[0].OrgID, "worker_id": worker, "fence": message["fence"]}
	renew := map[string]any{"org_id": owner["org_id"], "worker_id": worker, "fence": owner["fence"], "lease_ms": 30000}
	if got := coordinationOK(t, f, -1, "POST", path+"/renew", renew); got["fence"] != owner["fence"] {
		t.Fatalf("renew changed fence: %+v", got)
	}
	coordinationError(t, f, -1, "GET", path+"?org_id="+f.agents[4].OrgID, nil, 404, "not_found")
	coordinationError(t, f, 0, "POST", path+"/ack", owner, 401, "unauthorized")
	wrong := map[string]any{"org_id": owner["org_id"], "worker_id": f.agents[3].ID, "fence": owner["fence"], "response": nil}
	coordinationError(t, f, -1, "POST", path+"/ack", wrong, 409, "stale_claim")
	// Explicit false and zero must reach SQL, returning pending rather than failed.
	nack := map[string]any{"org_id": owner["org_id"], "worker_id": worker, "fence": owner["fence"], "error": "temporary", "retry_ms": 0, "permanent": false}
	if got := coordinationOK(t, f, -1, "POST", path+"/nack", nack); got["state"] != "pending" {
		t.Fatalf("nonpermanent nack: %+v", got)
	}
	request, _ = auditID()
	claimBody["request_id"] = request
	message = coordinationOK(t, f, -1, "POST", "/v1/admin/outbox/claim", claimBody)["message"].(map[string]any)
	if path != "/v1/admin/outbox/"+message["id"].(string) {
		t.Fatalf("retry claimed the wrong intent: %+v", message)
	}
	ack := map[string]any{"org_id": owner["org_id"], "worker_id": worker, "fence": owner["fence"], "response": nil}
	coordinationError(t, f, -1, "POST", path+"/ack", ack, 409, "stale_claim")
	// Permanently fail and explicitly requeue, preserving immutable identity.
	nack["fence"], nack["permanent"] = message["fence"], true
	failed := coordinationOK(t, f, -1, "POST", path+"/nack", nack)
	if failed["state"] != "failed" {
		t.Fatalf("permanent nack not failed: %+v", failed)
	}
	requeued := coordinationOK(t, f, -1, "POST", path+"/retry", map[string]any{"org_id": owner["org_id"], "reason": "operator approved retry"})
	beforeFence, _ := strconv.ParseInt(message["fence"].(string), 10, 64)
	afterFence, _ := strconv.ParseInt(requeued["fence"].(string), 10, 64)
	if requeued["id"] != message["id"] || requeued["idempotency_key"] != message["idempotency_key"] || requeued["attempts"] != float64(0) || requeued["total_attempts"] != message["total_attempts"] || afterFence <= beforeFence {
		t.Fatalf("retry lost identity/counters/fence: %+v", requeued)
	}
	request, _ = auditID()
	claimBody["request_id"] = request
	message = coordinationOK(t, f, -1, "POST", "/v1/admin/outbox/claim", claimBody)["message"].(map[string]any)
	ack["fence"] = message["fence"]
	acked := coordinationOK(t, f, -1, "POST", path+"/ack", ack)
	if acked["replayed"] != false || acked["message"].(map[string]any)["state"] != "delivered" {
		t.Fatalf("null-response acknowledgment failed: %+v", acked)
	}
	if replay := coordinationOK(t, f, -1, "POST", path+"/ack", ack); replay["replayed"] != true {
		t.Fatalf("ack not replayed: %+v", replay)
	}
	ack["response"] = map[string]any{"status": 201}
	coordinationError(t, f, -1, "POST", path+"/ack", ack, 409, "task_conflict")
	coordinationError(t, f, -1, "POST", path+"/retry", map[string]any{"org_id": owner["org_id"], "reason": "cannot redeliver"}, 409, "task_conflict")
	if saved := coordinationOK(t, f, -1, "GET", path+"?org_id="+f.agents[0].OrgID, nil); saved["state"] != "delivered" || saved["idempotency_key"] != message["idempotency_key"] {
		t.Fatalf("final delivery missing: %+v", saved)
	}
}
