package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// rpcReply is a decoded /mcp response.
type rpcReply struct {
	Status int
	Header http.Header
	Raw    string
	ID     json.RawMessage `json:"id"`
	Result map[string]any  `json:"result"`
	Error  *struct {
		Code    int            `json:"code"`
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	} `json:"error"`
}

// serve sends one message to serveMCP as an admitted agent. No test here
// reaches the database: they stop at transport validation or argument checks.
func serve(t *testing.T, body string, headers map[string]string) rpcReply {
	t.Helper()
	g := &Gateway{mcpOrigins: map[string]bool{}}
	r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	g.serveMCP(w, r, &Agent{ID: "00000000-0000-0000-0000-000000000001", MaxRows: 10})
	rep := rpcReply{Status: w.Code, Header: w.Header(), Raw: w.Body.String()}
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &rep); err != nil {
			t.Fatalf("response is not JSON: %v: %s", err, w.Body.String())
		}
	}
	return rep
}

// modern builds a 2026-07-28 request with its _meta and mirrored headers.
func modern(id int, method string, params map[string]any) (string, map[string]string) {
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{metaProtocolVersion: mcpModern, metaClientCapabilities: map[string]any{}}
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	h := map[string]string{"MCP-Protocol-Version": mcpModern, "Mcp-Method": method}
	if name, ok := params["name"].(string); ok {
		h["Mcp-Name"] = name
	}
	return string(b), h
}

func wantError(t *testing.T, rep rpcReply, status, code int) {
	t.Helper()
	if rep.Status != status || rep.Error == nil || rep.Error.Code != code {
		t.Fatalf("want HTTP %d with error %d, got HTTP %d: %s", status, code, rep.Status, rep.Raw)
	}
}

func toolNames(t *testing.T, rep rpcReply) []string {
	t.Helper()
	tools, _ := rep.Result["tools"].([]any)
	var names []string
	for _, tool := range tools {
		names = append(names, tool.(map[string]any)["name"].(string))
	}
	return names
}

func TestLegacyInitializeNegotiatesWithoutSession(t *testing.T) {
	for requested, want := range map[string]string{"2025-06-18": "2025-06-18", "2025-03-26": "2025-03-26", "1999-01-01": mcpLegacyLatest} {
		rep := serve(t, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"`+requested+`","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`, nil)
		if rep.Status != 200 || rep.Result["protocolVersion"] != want {
			t.Fatalf("initialize %s: want %s, got %d %s", requested, want, rep.Status, rep.Raw)
		}
		if info, _ := rep.Result["serverInfo"].(map[string]any); info["name"] != "swarmpg" {
			t.Fatalf("serverInfo missing: %s", rep.Raw)
		}
		if rep.Header.Get("Mcp-Session-Id") != "" {
			t.Fatal("a stateless server must not mint a session id")
		}
	}
}

func TestNotificationIsAccepted(t *testing.T) {
	rep := serve(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, nil)
	if rep.Status != http.StatusAccepted || rep.Raw != "" {
		t.Fatalf("want 202 with no body, got %d %q", rep.Status, rep.Raw)
	}
}

func TestLegacyToolsList(t *testing.T) {
	rep := serve(t, `{"jsonrpc":"2.0","id":"a","method":"tools/list"}`, map[string]string{"MCP-Protocol-Version": "2025-06-18"})
	got := strings.Join(toolNames(t, rep), ",")
	if got != "schema,query,memory_get,memory_put,memory_delete,memory_history,memory_undo,events_append,task_enqueue,task_get,task_claim,task_renew,task_release,task_complete,task_cancel,task_history" {
		t.Fatalf("tools: %s", got)
	}
	if _, ok := rep.Result["resultType"]; ok {
		t.Fatal("legacy results carry no resultType")
	}
	if string(rep.ID) != `"a"` {
		t.Fatalf("id not echoed: %s", rep.ID)
	}
}

func TestLegacyPing(t *testing.T) {
	rep := serve(t, `{"jsonrpc":"2.0","id":2,"method":"ping"}`, nil)
	if rep.Status != 200 || rep.Error != nil {
		t.Fatalf("ping: %s", rep.Raw)
	}
}

func TestModernDiscover(t *testing.T) {
	body, h := modern(1, "server/discover", nil)
	rep := serve(t, body, h)
	if rep.Status != 200 || rep.Result["resultType"] != "complete" || rep.Result["cacheScope"] != "public" {
		t.Fatalf("discover: %s", rep.Raw)
	}
	if v, _ := rep.Result["supportedVersions"].([]any); len(v) != len(mcpVersions) || v[0] != mcpModern {
		t.Fatalf("supportedVersions: %s", rep.Raw)
	}
	if _, ok := rep.Result["ttlMs"]; !ok {
		t.Fatal("ttlMs missing")
	}
	meta, _ := rep.Result["_meta"].(map[string]any)
	if info, _ := meta[metaServerInfo].(map[string]any); info["name"] != "swarmpg" {
		t.Fatalf("serverInfo missing from _meta: %s", rep.Raw)
	}
}

func TestModernToolsList(t *testing.T) {
	body, h := modern(1, "tools/list", nil)
	rep := serve(t, body, h)
	if rep.Result["resultType"] != "complete" || rep.Result["cacheScope"] != "public" || rep.Result["ttlMs"] == nil {
		t.Fatalf("tools/list: %s", rep.Raw)
	}
	if len(toolNames(t, rep)) != len(mcpTools) {
		t.Fatalf("tools: %s", rep.Raw)
	}
}

func TestModernRequestValidation(t *testing.T) {
	// _meta without a protocol version, though the header names 2026-07-28
	rep := serve(t, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`,
		map[string]string{"MCP-Protocol-Version": mcpModern, "Mcp-Method": "tools/list"})
	wantError(t, rep, 400, rpcInvalidParams)

	// unsupported version in _meta
	body, h := modern(1, "tools/list", nil)
	body = strings.Replace(body, mcpModern, "2099-01-01", 1)
	h["MCP-Protocol-Version"] = "2099-01-01"
	rep = serve(t, body, h)
	wantError(t, rep, 400, rpcUnsupportedVersion)
	if rep.Error.Data["requested"] != "2099-01-01" || rep.Error.Data["supported"] == nil {
		t.Fatalf("UnsupportedProtocolVersion data: %s", rep.Raw)
	}

	// missing MCP-Protocol-Version header
	body, h = modern(1, "tools/list", nil)
	delete(h, "MCP-Protocol-Version")
	wantError(t, serve(t, body, h), 400, rpcHeaderMismatch)

	// Mcp-Method that does not match the body
	body, h = modern(1, "tools/list", nil)
	h["Mcp-Method"] = "tools/call"
	wantError(t, serve(t, body, h), 400, rpcHeaderMismatch)

	// tools/call without Mcp-Name, and with a mismatched one
	body, h = modern(1, "tools/call", map[string]any{"name": "schema", "arguments": map[string]any{}})
	delete(h, "Mcp-Name")
	wantError(t, serve(t, body, h), 400, rpcHeaderMismatch)
	h["Mcp-Name"] = "query"
	wantError(t, serve(t, body, h), 400, rpcHeaderMismatch)

	// a base64-encoded Mcp-Name passes header validation and reaches the tool
	body, h = modern(1, "tools/call", map[string]any{"name": "query", "arguments": map[string]any{"sql": "  "}})
	h["Mcp-Name"] = "=?base64?" + base64.StdEncoding.EncodeToString([]byte("query")) + "?="
	rep = serve(t, body, h)
	if rep.Status != 200 || rep.Result["isError"] != true || rep.Result["resultType"] != "complete" {
		t.Fatalf("base64 Mcp-Name: %s", rep.Raw)
	}

	// ping was removed in 2026-07-28
	body, h = modern(1, "ping", nil)
	wantError(t, serve(t, body, h), 404, rpcMethodNotFound)
}

func TestEnvelopeErrors(t *testing.T) {
	wantError(t, serve(t, `[{"jsonrpc":"2.0","id":1,"method":"ping"}]`, nil), 400, rpcInvalidRequest)
	wantError(t, serve(t, `{"jsonrpc":"2.0","id":1,`, nil), 400, rpcParseError)
	wantError(t, serve(t, `{"id":1,"method":"ping"}`, nil), 400, rpcInvalidRequest)
	wantError(t, serve(t, `{"jsonrpc":"2.0","id":null,"method":"ping"}`, nil), 400, rpcInvalidRequest)
	wantError(t, serve(t, `{"jsonrpc":"2.0","id":1,"result":{}}`, nil), 400, rpcInvalidRequest)
	wantError(t, serve(t, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, map[string]string{"MCP-Protocol-Version": "2024-01-01"}), 400, rpcUnsupportedVersion)
	wantError(t, serve(t, `{"jsonrpc":"2.0","id":1,"method":"resources/list"}`, nil), 200, rpcMethodNotFound)
}

func TestToolCallErrors(t *testing.T) {
	call := func(name, args string) rpcReply {
		return serve(t, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"`+name+`","arguments":`+args+`}}`, nil)
	}
	// an unknown tool is a protocol error
	wantError(t, call("drop_everything", `{}`), 200, rpcInvalidParams)

	// bad arguments are tool errors the model can read and fix
	for _, tc := range []struct{ name, args string }{
		{"query", `{"sql":"   "}`},
		{"memory_get", `{}`},
		{"memory_get", `{"key":"k","version":1,"as_of":"2026-01-01T00:00:00Z"}`},
		{"memory_put", `{"key":"k"}`},
		{"memory_put", `{"key":"k","value":1,"expected_version":-1}`},
		{"memory_delete", `{"key":"k","expected_version":0}`},
		{"memory_history", `{"key":""}`},
		{"memory_undo", `{"key":"k","to_version":"one"}`},
		{"events_append", `{"payload":{}}`},
		{"schema", `{"verbose":true}`},
	} {
		name, args := tc.name, tc.args
		rep := call(name, args)
		if rep.Status != 200 || rep.Error != nil || rep.Result["isError"] != true {
			t.Fatalf("%s %s: want an isError result, got %s", name, args, rep.Raw)
		}
		sc, _ := rep.Result["structuredContent"].(map[string]any)
		if e, _ := sc["error"].(map[string]any); e["code"] == nil || e["message"] == nil {
			t.Fatalf("%s: error lacks code/message: %s", name, rep.Raw)
		}
	}

	// a statement the guard refuses never reaches the database
	rep := call("query", `{"sql":"SET app.agent_id = 'x'"}`)
	if rep.Result["isError"] != true || !strings.Contains(rep.Raw, "not permitted") {
		t.Fatalf("guarded SET: %s", rep.Raw)
	}
}

func TestOriginAndMethod(t *testing.T) {
	g := &Gateway{mcpOrigins: map[string]bool{"https://app.example": true}}
	if !g.originAllowed("") || !g.originAllowed("https://app.example") || g.originAllowed("https://evil.example") || g.originAllowed("null") {
		t.Fatal("origin allowlist")
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{}`))
	r.Header.Set("Origin", "https://evil.example")
	g.handleMCP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("foreign origin: want 403, got %d", w.Code)
	}

	for _, m := range []string{http.MethodGet, http.MethodDelete} {
		w = httptest.NewRecorder()
		g.handleMCP(w, httptest.NewRequest(m, "/mcp", nil))
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
			t.Fatalf("%s: want 405 with Allow: POST, got %d", m, w.Code)
		}
	}
}

func TestToolSchemasAreObjects(t *testing.T) {
	for _, d := range mcpToolDefs {
		if d.InputSchema["type"] != "object" || d.InputSchema["additionalProperties"] != false {
			t.Fatalf("%s: inputSchema must be a closed object", d.Name)
		}
		if d.Description == "" || d.Annotations["readOnlyHint"] == nil {
			t.Fatalf("%s: description and annotations required", d.Name)
		}
	}
}
