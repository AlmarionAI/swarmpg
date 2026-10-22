package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const taskAPITestID = "00000000-0000-0000-0000-000000000001"

func taskAPITestGateway() (*Gateway, *Agent, string) {
	// No database pools: these calls must fail at validation, never execute SQL.
	g := &Gateway{adminKey: "task-test-admin"}
	a := &Agent{ID: taskAPITestID, OrgID: "00000000-0000-0000-0000-000000000002", MaxRows: 100}
	token := "task-validation-test"
	g.cache.Store(hex.EncodeToString(hashToken(token)), &cachedAgent{a: a, at: time.Now()})
	return g, a, token
}

func taskRESTError(t *testing.T, g *Gateway, token, method, path, body string) string {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	g.routes().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("%s %s: want 400, got %d %s", method, path, w.Code, w.Body.String())
	}
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response.Error.Code
}

func TestTaskArgumentDecoderRejectsMalformedBodies(t *testing.T) {
	g, _, token := taskAPITestGateway()
	for _, body := range []string{"", "null", "[]", `"text"`, "1", "{", "{} {}", "{} trailing", `{"unknown":true}`} {
		t.Run(strconv.Quote(body), func(t *testing.T) {
			if code := taskRESTError(t, g, token, "POST", "/v1/tasks/"+taskAPITestID+"/cancel", body); code != "bad_request" {
				t.Fatalf("REST returned %s", code)
			}
			var in struct{}
			if err := decodeTaskArgs([]byte(body), &in); err == nil || err.Code != "bad_request" {
				t.Fatalf("MCP argument decoder accepted %q: %v", body, err)
			}
		})
	}
}

func TestTaskInvalidArgumentsHaveRESTMCPParity(t *testing.T) {
	g, a, token := taskAPITestGateway()
	for _, tc := range []struct{ name, method, path, body, tool, args string }{
		{"enqueue requires payload", "POST", "/v1/tasks", `{"queue":"q","key":"k"}`, "task_enqueue", `{"queue":"q","key":"k"}`},
		{"enqueue requires queue", "POST", "/v1/tasks", `{"key":"k","payload":null}`, "task_enqueue", `{"key":"k","payload":null}`},
		{"enqueue requires key", "POST", "/v1/tasks", `{"queue":"q","payload":null}`, "task_enqueue", `{"queue":"q","payload":null}`},
		{"enqueue invalid dependency", "POST", "/v1/tasks", `{"queue":"q","key":"k","payload":{},"dependencies":["invalid"]}`, "task_enqueue", `{"queue":"q","key":"k","payload":{},"dependencies":["invalid"]}`},
		{"enqueue invalid input version", "POST", "/v1/tasks", `{"queue":"q","key":"k","payload":{},"inputs":[{"agent_id":"` + taskAPITestID + `","key":"k","version":0}]}`, "task_enqueue", `{"queue":"q","key":"k","payload":{},"inputs":[{"agent_id":"` + taskAPITestID + `","key":"k","version":0}]}`},
		{"enqueue unknown nested field", "POST", "/v1/tasks", `{"queue":"q","key":"k","payload":{},"inputs":[{"agent_id":"` + taskAPITestID + `","key":"k","version":1,"ignored":true}]}`, "task_enqueue", `{"queue":"q","key":"k","payload":{},"inputs":[{"agent_id":"` + taskAPITestID + `","key":"k","version":1,"ignored":true}]}`},
		{"get invalid UUID", "GET", "/v1/tasks/bad-id", "", "task_get", `{"id":"bad-id"}`},
		{"claim requires request UUID", "POST", "/v1/tasks/claim", `{"queue":"q"}`, "task_claim", `{"queue":"q"}`},
		{"claim too short lease", "POST", "/v1/tasks/claim", `{"queue":"q","request_id":"` + taskAPITestID + `","lease_ms":99}`, "task_claim", `{"queue":"q","request_id":"` + taskAPITestID + `","lease_ms":99}`},
		{"claim too long lease", "POST", "/v1/tasks/claim", `{"queue":"q","request_id":"` + taskAPITestID + `","lease_ms":300001}`, "task_claim", `{"queue":"q","request_id":"` + taskAPITestID + `","lease_ms":300001}`},
		{"renew numeric fence", "POST", "/v1/tasks/" + taskAPITestID + "/renew", `{"fence":9007199254740993}`, "task_renew", `{"id":"` + taskAPITestID + `","fence":9007199254740993}`},
		{"release noncanonical fence", "POST", "/v1/tasks/" + taskAPITestID + "/release", `{"fence":"01"}`, "task_release", `{"id":"` + taskAPITestID + `","fence":"01"}`},
		{"complete requires result", "POST", "/v1/tasks/" + taskAPITestID + "/complete", `{"fence":"1"}`, "task_complete", `{"id":"` + taskAPITestID + `","fence":"1"}`},
		{"complete overflowing fence", "POST", "/v1/tasks/" + taskAPITestID + "/complete", `{"fence":"9223372036854775808","result":null}`, "task_complete", `{"id":"` + taskAPITestID + `","fence":"9223372036854775808","result":null}`},
		{"cancel rejects extras", "POST", "/v1/tasks/" + taskAPITestID + "/cancel", `{"force":true}`, "task_cancel", `{"id":"` + taskAPITestID + `","force":true}`},
		{"history rejects zero limit", "GET", "/v1/tasks/" + taskAPITestID + "/history?limit=0", "", "task_history", `{"id":"` + taskAPITestID + `","limit":0}`},
		{"history rejects excessive limit", "GET", "/v1/tasks/" + taskAPITestID + "/history?limit=1001", "", "task_history", `{"id":"` + taskAPITestID + `","limit":1001}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := taskRESTError(t, g, token, tc.method, tc.path, tc.body); code != "bad_request" {
				t.Fatalf("REST code=%s", code)
			}
			tool := mcpToolsByName[tc.tool]
			out := tool.run(g, context.Background(), a, json.RawMessage(tc.args))
			if out["isError"] != true {
				t.Fatalf("MCP accepted invalid arguments: %#v", out)
			}
			body := out["structuredContent"].(map[string]any)["error"].(map[string]any)
			if body["code"] != "bad_request" {
				t.Fatalf("MCP code=%v", body["code"])
			}
		})
	}
}

func TestTaskFencesPreserveInt64Precision(t *testing.T) {
	for _, value := range []string{"1", "9007199254740993", "9223372036854775807"} {
		n, err := taskFence(value)
		if err != nil || strconv.FormatInt(n, 10) != value {
			t.Fatalf("fence %q lost precision: %d %v", value, n, err)
		}
	}
	for _, value := range []string{"", "0", "-1", "+1", "01", " 1", "1 ", "1.0", "1e3", "9223372036854775808"} {
		if _, err := taskFence(value); err == nil {
			t.Fatalf("accepted invalid fence %q", value)
		}
	}
}

func TestTaskValidationBounds(t *testing.T) {
	for _, ms := range []int{100, 30000, 300000} {
		got, err := taskLease(&ms)
		if err != nil || got != ms {
			t.Fatalf("valid lease %d: %d %v", ms, got, err)
		}
	}
	if ms, err := taskLease(nil); err != nil || ms != 30000 {
		t.Fatalf("default lease: %d %v", ms, err)
	}
	for _, id := range []string{taskAPITestID, "00112233AABBCCDDEEFF001122334455"} {
		if err := taskUUID(id, "id"); err != nil {
			t.Fatalf("valid UUID refused: %v", err)
		}
	}
	for _, id := range []string{"", "00000000x0000x0000x0000x000000000001", strings.Repeat("g", 32)} {
		if err := taskUUID(id, "id"); err == nil {
			t.Fatalf("invalid UUID accepted: %q", id)
		}
	}
	if err := taskJSON(json.RawMessage("null"), "result"); err != nil {
		t.Fatalf("explicit JSON null refused: %v", err)
	}
	for _, value := range []json.RawMessage{nil, json.RawMessage("invalid"), json.RawMessage(`"` + strings.Repeat("x", taskJSONLimit) + `"`)} {
		if err := taskJSON(value, "result"); err == nil {
			t.Fatal("invalid or oversized result accepted")
		}
	}
	g, a, _ := taskAPITestGateway()
	for _, in := range []taskEnqueueRequest{
		{Queue: strings.Repeat("q", 129), Key: "k", Payload: json.RawMessage(`{}`)},
		{Queue: "q", Key: strings.Repeat("k", 257), Payload: json.RawMessage(`{}`)},
		{Queue: "q\x00", Key: "k", Payload: json.RawMessage(`{}`)},
		{Queue: "q", Key: "k", Payload: json.RawMessage(`{}`), Dependencies: make([]string, 65)},
		{Queue: "q", Key: "k", Payload: json.RawMessage(`{}`), Inputs: make([]taskInput, 65)},
		{Queue: "q", Key: "k", Payload: json.RawMessage(`{}`), Inputs: []taskInput{{AgentID: taskAPITestID, Key: strings.Repeat("k", 257), Version: 1}}},
	} {
		if _, err := g.taskEnqueue(context.Background(), a, in); err == nil || err.Code != "bad_request" {
			t.Fatalf("invalid enqueue reached database: %v", err)
		}
	}
	a.MaxRows = 0
	if _, err := g.taskGet(context.Background(), a, taskAPITestID); err == nil || err.Code != "result_limit_exceeded" {
		t.Fatalf("zero result budget reached database: %v", err)
	}
}

func TestTaskRecoveryIsAdminOnlyAndNotAMCPTool(t *testing.T) {
	g, _, token := taskAPITestGateway()
	for _, key := range []string{"", "wrong", "task-test-admin"} {
		r := httptest.NewRequest("POST", "/v1/admin/tasks/"+taskAPITestID+"/recover", strings.NewReader(`{"reason":""}`))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Admin-Key", key)
		w := httptest.NewRecorder()
		g.routes().ServeHTTP(w, r)
		want := http.StatusUnauthorized
		if key == "task-test-admin" {
			want = http.StatusBadRequest // authenticated but invalid reason
		}
		if w.Code != want {
			t.Fatalf("admin key %q: want %d got %d %s", key, want, w.Code, w.Body.String())
		}
	}
	if _, ok := mcpToolsByName["task_recover"]; ok {
		t.Fatal("admin recovery must not be exposed as an agent MCP tool")
	}
}
