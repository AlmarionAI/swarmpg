package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestOutboxInvalidEffectsHaveRESTMCPParity(t *testing.T) {
	g, a, token := taskAPITestGateway()
	for name, effects := range map[string]string{
		"null array":            `null`,
		"object":                `{}`,
		"null member":           `[null]`,
		"missing key":           `[{"destination":"mail","payload":null}]`,
		"missing destination":   `[{"key":"a","payload":null}]`,
		"missing payload":       `[{"key":"a","destination":"mail"}]`,
		"unknown field":         `[{"key":"a","destination":"mail","payload":null,"url":"https://example.com"}]`,
		"invalid key":           `[{"key":"a/b","destination":"mail","payload":null}]`,
		"URL destination":       `[{"key":"a","destination":"https://example.com","payload":null}]`,
		"non ASCII destination": `[{"key":"a","destination":"máil","payload":null}]`,
		"duplicate keys":        `[{"key":"a","destination":"one","payload":1},{"key":"a","destination":"two","payload":2}]`,
		"too many":              `[` + strings.Repeat(`{"key":"a","destination":"d","payload":null},`, 32) + `{"key":"z","destination":"d","payload":null}]`,
	} {
		t.Run(name, func(t *testing.T) {
			body := `{"fence":"1","result":null,"effects":` + effects + `}`
			taskRESTError(t, g, token, "POST", "/v1/tasks/"+taskAPITestID+"/complete", body)
			args := `{"id":"` + taskAPITestID + `",` + body[1:]
			out := toolTaskComplete(g, context.Background(), a, json.RawMessage(args))
			if out["isError"] != true || out["structuredContent"].(map[string]any)["error"].(map[string]any)["code"] != "bad_request" {
				t.Fatalf("MCP accepted invalid effects: %+v", out)
			}
		})
	}
	taskRESTError(t, g, token, "GET", "/v1/tasks/bad-id/outbox", "")
	taskRESTError(t, g, token, "POST", "/v1/tasks/"+taskAPITestID+"/complete", `{"fence":"1","result":null,"effects":[{"key":"a","destination":"mail","payload":"`+strings.Repeat("x", maxRequestBytes)+`"}]}`)
	for _, args := range []string{`{}`, `{"id":null}`, `{"id":"bad-id"}`, `{"id":"` + taskAPITestID + `","unknown":true}`} {
		if out := toolTaskOutbox(g, context.Background(), a, json.RawMessage(args)); out["isError"] != true {
			t.Fatalf("task_outbox accepted %s: %+v", args, out)
		}
	}
}

func TestOutboxEffectsValidationPreservesJSONAndBounds(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`[]`), json.RawMessage(`[{"key":"a._-9","destination":"mail","payload":null}]`), json.RawMessage(`[{"key":"a","destination":"mail","payload":{"precise":9007199254740993}}]`)} {
		got, err := taskEffects(raw)
		if err != nil || !json.Valid(got) {
			t.Fatalf("valid effects %s: %s %v", raw, got, err)
		}
		if strings.Contains(string(raw), "9007199254740993") && !strings.Contains(string(got), "9007199254740993") {
			t.Fatal("payload number lost precision")
		}
	}
	effects := make([]taskEffect, 32)
	for i := range effects {
		effects[i] = taskEffect{Key: fmt.Sprintf("effect-%d", i), Destination: "mail", Payload: json.RawMessage(`null`)}
	}
	raw, _ := json.Marshal(effects)
	if _, err := taskEffects(raw); err != nil {
		t.Fatalf("32 effects refused: %v", err)
	}
	// Normalized JSONB size is enforced in SQL; escaped wire strings may be
	// larger than the limit while their stored representation fits comfortably.
	escaped := json.RawMessage(`[{"key":"a","destination":"mail","payload":"` + strings.Repeat(`\u0061`, 11000) + `"}]`)
	if _, err := taskEffects(escaped); err != nil {
		t.Fatalf("gateway incorrectly used wire bytes as JSONB size: %v", err)
	}
	for _, name := range []string{"", "-first", "white space", strings.Repeat("a", 129), "ü"} {
		if err := outboxName(name, "name"); err == nil {
			t.Fatalf("accepted invalid outbox name %q", name)
		}
	}
	if err := outboxName(strings.Repeat("a", 128), "name"); err != nil {
		t.Fatalf("128-character name refused: %v", err)
	}
}

func outboxAdminValidation(t *testing.T, g *Gateway, method, path, body string, want int) {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("X-Admin-Key", g.adminKey)
	w := httptest.NewRecorder()
	g.routes().ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s %s body=%s: want %d got %d %s", method, path, body, want, w.Code, w.Body.String())
	}
}

func TestOutboxAdminRequiredFieldsRejectMissingNullAndMalformed(t *testing.T) {
	g, _, _ := taskAPITestGateway()
	owner := map[string]any{"org_id": taskAPITestID, "worker_id": taskAPITestID, "fence": "9007199254740993"}
	for _, tc := range []struct {
		action string
		fields map[string]any
	}{
		{"claim", map[string]any{"org_id": taskAPITestID, "destination": "mail", "worker_id": taskAPITestID, "request_id": taskAPITestID}},
		{"renew", map[string]any{"lease_ms": 1000}},
		{"ack", map[string]any{"response": nil}},
		{"nack", map[string]any{"error": "temporary", "retry_ms": 0, "permanent": false}},
		{"retry", map[string]any{"org_id": taskAPITestID, "reason": "operator retry"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			base := make(map[string]any)
			if tc.action == "renew" || tc.action == "ack" || tc.action == "nack" {
				for k, v := range owner {
					base[k] = v
				}
			}
			for k, v := range tc.fields {
				base[k] = v
			}
			path := "/v1/admin/outbox/" + taskAPITestID + "/" + tc.action
			if tc.action == "claim" {
				path = "/v1/admin/outbox/claim"
			}
			for field := range base {
				for _, mutation := range []string{"missing", "null", "wrong type"} {
					if field == "response" && mutation != "missing" {
						continue
					} // Any JSON, including null, is valid.
					t.Run(field+"/"+mutation, func(t *testing.T) {
						body := make(map[string]any)
						for k, v := range base {
							body[k] = v
						}
						switch mutation {
						case "missing":
							delete(body, field)
						case "null":
							body[field] = nil
						case "wrong type":
							body[field] = []any{}
						}
						raw, _ := json.Marshal(body)
						outboxAdminValidation(t, g, "POST", path, string(raw), 400)
					})
				}
			}
			base["unknown"] = true
			raw, _ := json.Marshal(base)
			outboxAdminValidation(t, g, "POST", path, string(raw), 400)
			for _, body := range []string{"", "null", "[]", "{} {}"} {
				outboxAdminValidation(t, g, "POST", path, body, 400)
			}
		})
	}
}

func TestOutboxAdminScalarBoundsAndExactFences(t *testing.T) {
	g, _, _ := taskAPITestGateway()
	for _, value := range []string{`null`, `"1000"`, `1.5`, `99`, `300001`} {
		body := `{"org_id":"` + taskAPITestID + `","worker_id":"` + taskAPITestID + `","request_id":"` + taskAPITestID + `","destination":"mail","lease_ms":` + value + `}`
		outboxAdminValidation(t, g, "POST", "/v1/admin/outbox/claim", body, 400)
	}
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`100`), json.RawMessage(`300000`)} {
		if _, err := outboxClaimLease(raw); err != nil {
			t.Fatalf("valid claim lease refused: %s %v", raw, err)
		}
	}
	for _, fence := range []string{"9007199254740993", "9223372036854775807"} {
		n, err := outboxOwner(taskAPITestID, outboxOwnerRequest{OrgID: taskAPITestID, WorkerID: taskAPITestID, Fence: fence})
		if err != nil || strconv.FormatInt(n, 10) != fence {
			t.Fatalf("fence lost precision: %d %v", n, err)
		}
	}
	owner := `"org_id":"` + taskAPITestID + `","worker_id":"` + taskAPITestID + `"`
	for _, fence := range []string{`9007199254740993`, `"01"`, `"9223372036854775808"`} {
		outboxAdminValidation(t, g, "POST", "/v1/admin/outbox/"+taskAPITestID+"/ack", `{`+owner+`,"fence":`+fence+`,"response":null}`, 400)
	}
	for _, retry := range []string{`-1`, `3600001`, `"0"`, `0.5`} {
		outboxAdminValidation(t, g, "POST", "/v1/admin/outbox/"+taskAPITestID+"/nack", `{`+owner+`,"fence":"1","error":"x","permanent":false,"retry_ms":`+retry+`}`, 400)
	}
	var nack outboxNackRequest
	if err := decodeTaskArgs([]byte(`{`+owner+`,"fence":"1","error":"temporary","retry_ms":0,"permanent":false}`), &nack); err != nil || nack.RetryMS == nil || *nack.RetryMS != 0 || nack.Permanent == nil || *nack.Permanent {
		t.Fatalf("valid false/zero lost in decoder: %+v %v", nack, err)
	}
	for _, query := range []string{"", "?org_id=", "?org_id=bad", "?org_id=" + taskAPITestID + "&org_id=" + taskAPITestID, "?org_id=" + taskAPITestID + "&unknown=1"} {
		outboxAdminValidation(t, g, "GET", "/v1/admin/outbox/"+taskAPITestID+query, "", 400)
	}
}

func TestOutboxDispatcherRoutesRequireAdminAndHaveNoMCPTools(t *testing.T) {
	g, _, token := taskAPITestGateway()
	paths := []string{"/v1/admin/outbox/claim"}
	for _, action := range []string{"renew", "ack", "nack", "retry"} {
		paths = append(paths, "/v1/admin/outbox/"+taskAPITestID+"/"+action)
	}
	paths = append(paths, "/v1/admin/outbox/"+taskAPITestID+"?org_id="+taskAPITestID)
	for i, path := range paths {
		method := "POST"
		if i == len(paths)-1 {
			method = "GET"
		}
		for _, key := range []string{"", "wrong"} {
			r := httptest.NewRequest(method, path, strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer "+token)
			r.Header.Set("X-Admin-Key", key)
			w := httptest.NewRecorder()
			g.routes().ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("agent reached dispatcher %s: %d %s", path, w.Code, w.Body.String())
			}
		}
	}
	for _, name := range []string{"outbox_claim", "outbox_renew", "outbox_ack", "outbox_nack", "outbox_retry", "outbox_get"} {
		if _, ok := mcpToolsByName[name]; ok {
			t.Fatalf("dispatcher exposed as MCP tool: %s", name)
		}
	}
	if _, ok := mcpToolsByName["task_outbox"]; !ok || len(mcpToolsByName) != 17 {
		t.Fatalf("expected 17 MCP tools including task_outbox, got %d", len(mcpToolsByName))
	}
}
