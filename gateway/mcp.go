// MCP endpoint: the gateway's agent tools (query, schema, memory, events) over
// the Model Context Protocol's Streamable HTTP transport, at POST /mcp.
//
// Stateless and JSON-only: every POST carries one JSON-RPC message and gets one
// JSON response, or 202 for a notification. There are no sessions and no SSE
// streams. Both protocol eras are served:
//
//   - 2026-07-28: no handshake. Each request carries its protocol version and
//     client capabilities in params._meta, mirrored in the MCP-Protocol-Version,
//     Mcp-Method and (for tools/call) Mcp-Name headers; server/discover
//     describes the server.
//   - 2025-11-25, 2025-06-18, 2025-03-26: initialize negotiates the version, and
//     later requests name it in MCP-Protocol-Version (absent means 2025-03-26).
//
// Every request is authenticated with the agent's bearer token and runs under
// the same identity, budgets and limits as the REST API: the tools call the
// same code paths.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	mcpModern        = "2026-07-28"
	mcpLegacyLatest  = "2025-11-25"
	mcpLegacyDefault = "2025-03-26" // a legacy request without MCP-Protocol-Version
	mcpCacheTTLMs    = 300000       // tools/list and server/discover change only on deploy

	metaProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	metaClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
	metaServerInfo         = "io.modelcontextprotocol/serverInfo"

	rpcParseError         = -32700
	rpcInvalidRequest     = -32600
	rpcMethodNotFound     = -32601
	rpcInvalidParams      = -32602
	rpcServerError        = -32000 // authentication and admission failures
	rpcHeaderMismatch     = -32020
	rpcUnsupportedVersion = -32022
)

// mcpVersions lists every protocol version served, newest first.
var mcpVersions = []string{mcpModern, "2025-11-25", "2025-06-18", "2025-03-26"}

var mcpServerInfo = map[string]string{"name": "swarmpg", "title": "SwarmPG", "version": "0.1.0"}

const mcpInstructions = "SwarmPG gives you a private, database-enforced slice of a shared Postgres. " +
	"Your identity, org and budgets come from your token and cannot be changed from SQL. " +
	"You have versioned key/value JSON memory (optionally shared with your org) with compare-and-set " +
	"writes, history and undo, JSON documents in named " +
	"collections, an append-only, hash-chained event journal, and org-shared task queues with leases, " +
	"fencing tokens and atomic completion. Call schema first, then use query " +
	"for SQL with $1, $2 parameters; errors return a SQLSTATE code, hint and position so you can fix " +
	"the statement and retry."

// mcpToolDef is a tool as listed by tools/list.
type mcpToolDef struct {
	Name        string         `json:"name"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations"`
}

type mcpToolFunc func(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any

type mcpTool struct {
	def mcpToolDef
	run mcpToolFunc
}

func object(props map[string]any, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func annotations(title string, readOnly, destructive, idempotent bool) map[string]any {
	return map[string]any{"title": title, "readOnlyHint": readOnly, "destructiveHint": destructive,
		"idempotentHint": idempotent, "openWorldHint": false}
}

// mcpTools is in the order tools/list returns them.
var mcpTools = append([]mcpTool{
	{mcpToolDef{
		Name:  "schema",
		Title: "Describe your tables",
		Description: "Describe the tables you can use and the rules that apply to your SQL. " +
			"Call this before writing queries.",
		InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		Annotations: annotations("Describe your tables", true, false, true),
	}, toolSchema},
	{mcpToolDef{
		Name:  "query",
		Title: "Run SQL",
		Description: "Run one SQL statement against your data and return the rows. Put values in params " +
			"and refer to them as $1, $2, ... in the statement. Results are capped by your row and byte " +
			"budgets (truncated is true when rows were cut) and over-budget queries are refused before " +
			"they run. SET, transaction control, DDL, COPY and cursors are rejected. On error the result " +
			"carries the SQLSTATE code, message, hint and position so you can repair the statement.",
		InputSchema: object(map[string]any{
			"sql":    map[string]any{"type": "string", "description": "One SQL statement."},
			"params": map[string]any{"type": "array", "items": map[string]any{}, "description": "Values for $1, $2, ..."},
		}, "sql"),
		Annotations: annotations("Run SQL", false, true, false),
	}, toolQuery},
	{mcpToolDef{
		Name:  "memory_get",
		Title: "Read a memory key",
		Description: "Read one of your memory keys with its version. If you have no such key, an org-shared key " +
			"of the same name is returned (own is false), ordered by version descending then agent_id ascending. " +
			"The result identifies its producer with agent_id. Pass from to select one visible live producer, " +
			"or list_shared to list every org-shared producer. Pass version, or as_of (an RFC 3339 time), to read " +
			"your own key as it was then. from, list_shared and historical selectors cannot be combined.",
		InputSchema: object(map[string]any{
			"key":         map[string]any{"type": "string", "description": "The memory key."},
			"version":     map[string]any{"type": "integer", "minimum": 1, "description": "Read this version from your history."},
			"as_of":       map[string]any{"type": "string", "format": "date-time", "description": "Read your value as it was at this time."},
			"from":        map[string]any{"type": "string", "format": "uuid", "description": "Read the live key from this agent; other agents' keys must be shared with your org."},
			"list_shared": map[string]any{"type": "boolean", "description": "Return {key, memories} with all org-shared live producers, ordered by version descending then agent_id ascending. Errors if the complete list exceeds your result budget."},
		}, "key"),
		Annotations: annotations("Read a memory key", true, false, true),
	}, toolMemoryGet},
	{mcpToolDef{
		Name:  "memory_put",
		Title: "Write a memory key",
		Description: "Create or replace one of your memory keys with any JSON value; returns the new version. " +
			"Pass expected_version for a safe read-modify-write: 0 creates the key only if it does not exist, " +
			"N writes only if the key is still at version N. Otherwise nothing is written and you get " +
			"version_conflict with current_version: read the key again and retry. Set shared to let agents " +
			"in your org read it.",
		InputSchema: object(map[string]any{
			"key":              map[string]any{"type": "string", "description": "The memory key."},
			"value":            map[string]any{"description": "Any JSON value."},
			"shared":           map[string]any{"type": "boolean", "description": "Readable by your org. Default false."},
			"expected_version": map[string]any{"type": "integer", "minimum": 0, "description": "Write only if the key is at this version (0: absent)."},
		}, "key", "value"),
		Annotations: annotations("Write a memory key", false, true, false),
	}, toolMemoryPut},
	{mcpToolDef{
		Name:        "memory_delete",
		Title:       "Delete a memory key",
		Description: "Delete one of your memory keys. It stays in your history, so memory_undo can bring it back. Pass expected_version to delete only that version.",
		InputSchema: object(map[string]any{
			"key":              map[string]any{"type": "string", "description": "The memory key."},
			"expected_version": map[string]any{"type": "integer", "minimum": 1, "description": "Delete only if the key is at this version."},
		}, "key"),
		Annotations: annotations("Delete a memory key", false, true, false),
	}, toolMemoryDelete},
	{mcpToolDef{
		Name:        "memory_history",
		Title:       "List a key's versions",
		Description: "List the versions of one of your memory keys, newest first, with each value, when it was written and whether it was a delete.",
		InputSchema: object(map[string]any{
			"key":   map[string]any{"type": "string", "description": "The memory key."},
			"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 1000, "description": "How many versions. Default 50."},
		}, "key"),
		Annotations: annotations("List a key's versions", true, false, true),
	}, toolMemoryHistory},
	{mcpToolDef{
		Name:  "memory_undo",
		Title: "Undo a memory change",
		Description: "Undo the latest change to one of your memory keys, or restore it to to_version. The restored " +
			"state is written as a new version, so an undo can itself be undone. Restoring a state where the " +
			"key did not exist deletes it. expected_version guards it like memory_put.",
		InputSchema: object(map[string]any{
			"key":              map[string]any{"type": "string", "description": "The memory key."},
			"to_version":       map[string]any{"type": "integer", "minimum": 1, "description": "Restore this version instead of the previous one."},
			"expected_version": map[string]any{"type": "integer", "minimum": 0, "description": "Undo only if the key is at this version (0: absent)."},
		}, "key"),
		Annotations: annotations("Undo a memory change", false, true, false),
	}, toolMemoryUndo},
	{mcpToolDef{
		Name:  "events_append",
		Title: "Append to your journal",
		Description: "Append an event to your append-only, hash-chained journal. Pass an idempotency_key to " +
			"make retries safe: a repeat with the same key returns the original event instead of a new one.",
		InputSchema: object(map[string]any{
			"kind":            map[string]any{"type": "string", "description": "Event type, e.g. tool_call."},
			"payload":         map[string]any{"type": "object", "description": "Event data. Default {}."},
			"idempotency_key": map[string]any{"type": "string", "description": "Optional key that makes a retry return the first result."},
		}, "kind"),
		Annotations: annotations("Append to your journal", false, false, false),
	}, toolEventsAppend},
}, taskMCPTools...)

var mcpToolDefs, mcpToolsByName = func() ([]mcpToolDef, map[string]mcpTool) {
	defs := make([]mcpToolDef, len(mcpTools))
	byName := make(map[string]mcpTool, len(mcpTools))
	for i, t := range mcpTools {
		defs[i] = t.def
		byName[t.def.Name] = t
	}
	return defs, byName
}()

// ---- tools

// decodeArgs decodes tool arguments strictly, so a misspelled field is
// reported to the model instead of being ignored.
func decodeArgs(args json.RawMessage, v any) *apiError {
	dec := json.NewDecoder(bytes.NewReader(args))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errBadReq("invalid arguments: " + err.Error())
	}
	return nil
}

func toolSchema(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct{}
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	s, aerr := g.describe(ctx, a)
	if aerr != nil {
		return toolError(aerr)
	}
	return toolText(s, map[string]any{"schema": s})
}

func toolQuery(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in QueryRequest
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	resp, aerr := g.query(ctx, a, in)
	if aerr != nil {
		g.stats.errors.Add(1)
		return toolError(aerr)
	}
	return toolJSON(resp)
}

func toolMemoryGet(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		Key        string     `json:"key"`
		Version    *int64     `json:"version"`
		AsOf       *time.Time `json:"as_of"`
		From       *string    `json:"from"`
		ListShared bool       `json:"list_shared"`
	}
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	if aerr := validateMemoryRead(in.Key, in.Version, in.AsOf, in.From, in.ListShared); aerr != nil {
		return toolError(aerr)
	}
	if in.ListShared {
		out, aerr := g.memoryShared(ctx, a, in.Key)
		if aerr != nil {
			return toolError(aerr)
		}
		return toolJSON(out)
	}
	m, aerr := g.memoryGet(ctx, a, in.Key, in.Version, in.AsOf, in.From)
	if aerr != nil {
		return toolError(aerr)
	}
	return toolJSON(m)
}

func toolMemoryPut(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		Key             string          `json:"key"`
		Value           json.RawMessage `json:"value"`
		Shared          bool            `json:"shared"`
		ExpectedVersion *int64          `json:"expected_version"`
	}
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	version, aerr := g.memoryPut(ctx, a, in.Key, in.Value, in.Shared, in.ExpectedVersion)
	if aerr != nil {
		return toolError(aerr)
	}
	return toolJSON(map[string]any{"ok": true, "key": in.Key, "version": version, "shared": in.Shared})
}

func toolMemoryDelete(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		Key             string `json:"key"`
		ExpectedVersion *int64 `json:"expected_version"`
	}
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	version, aerr := g.memoryDelete(ctx, a, in.Key, in.ExpectedVersion)
	if aerr != nil {
		return toolError(aerr)
	}
	return toolJSON(map[string]any{"ok": true, "key": in.Key, "version": version})
}

func toolMemoryHistory(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		Key   string `json:"key"`
		Limit int    `json:"limit"`
	}
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	versions, aerr := g.memoryHistory(ctx, a, in.Key, in.Limit)
	if aerr != nil {
		return toolError(aerr)
	}
	return toolJSON(map[string]any{"key": in.Key, "versions": versions})
}

func toolMemoryUndo(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		Key             string `json:"key"`
		ToVersion       *int64 `json:"to_version"`
		ExpectedVersion *int64 `json:"expected_version"`
	}
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	out, aerr := g.memoryUndo(ctx, a, in.Key, in.ToVersion, in.ExpectedVersion)
	if aerr != nil {
		return toolError(aerr)
	}
	return toolJSON(out)
}

func toolEventsAppend(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		Kind           string          `json:"kind"`
		Payload        json.RawMessage `json:"payload"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	if aerr := decodeArgs(args, &in); aerr != nil {
		return toolError(aerr)
	}
	out, replay, aerr := g.appendEvent(ctx, a, in.Kind, in.Payload, in.IdempotencyKey)
	if aerr != nil {
		g.stats.errors.Add(1)
		return toolError(aerr)
	}
	var ev map[string]any
	if err := json.Unmarshal(out, &ev); err != nil {
		return toolError(&apiError{Code: "internal", Message: "stored event could not be decoded"})
	}
	ev["replayed"] = replay
	return toolJSON(ev)
}

// toolJSON returns v as structuredContent and, for clients that only read
// text, the same JSON as a text block.
func toolJSON(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return toolError(&apiError{Code: "encode_error", Message: "result could not be encoded"})
	}
	return toolText(string(b), v)
}

func toolText(text string, structured any) map[string]any {
	return map[string]any{
		"content":           []any{map[string]any{"type": "text", "text": text}},
		"structuredContent": structured,
		"isError":           false,
	}
}

// toolError reports a failed call as a tool result, not a protocol error, so
// the model sees the SQLSTATE, hint and position and can repair its request.
func toolError(e *apiError) map[string]any {
	body := map[string]any{"error": errorBody(e)}
	b, _ := json.Marshal(body)
	return map[string]any{
		"content":           []any{map[string]any{"type": "text", "text": string(b)}},
		"structuredContent": body,
		"isError":           true,
	}
}

// ---- transport

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"` // only a response has these, and clients
	Error   json.RawMessage `json:"error"`  // must not send responses to this server
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
	status  int    // HTTP status the error is sent with
}

// mcpCall is one JSON-RPC message that passed transport validation.
type mcpCall struct {
	id       json.RawMessage
	method   string
	params   json.RawMessage
	toolName string
	modern   bool // 2026-07-28 semantics
}

func (g *Gateway) originAllowed(origin string) bool {
	return origin == "" || g.mcpOrigins[origin]
}

// handleMCP checks the Origin (DNS-rebinding defense), the method and the
// agent's token, then serves the JSON-RPC message.
func (g *Gateway) handleMCP(w http.ResponseWriter, r *http.Request) {
	if !g.originAllowed(r.Header.Get("Origin")) {
		writeRPCError(w, nil, &rpcError{Code: rpcInvalidRequest, Message: "origin not allowed (set MCP_ALLOWED_ORIGINS)", status: http.StatusForbidden})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeRPCError(w, nil, &rpcError{Code: rpcInvalidRequest, Message: "POST only: this endpoint keeps no sessions and opens no streams", status: http.StatusMethodNotAllowed})
		return
	}
	a, authErr := g.authenticate(r.Context(), r)
	if authErr != nil {
		g.mcpAdmissionError(w, authErr.(*apiError))
		return
	}
	// Classification needs the actual payload. Bound body reads separately so
	// slow or malformed MCP messages cannot allocate unbounded memory before
	// the execution queues apply, and reject revoked cached identities early.
	if a.Disabled || a.Expired {
		g.mcpAdmissionError(w, errForbidden("agent disabled or token expired"))
		return
	}
	releaseIngress, ingressErr := g.acquireMCPIngress(a)
	if ingressErr != nil {
		g.mcpAdmissionError(w, ingressErr)
		return
	}
	c, rerr := func() (*mcpCall, *rpcError) {
		defer releaseIngress()
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
		if err != nil {
			return &mcpCall{}, &rpcError{Code: rpcParseError, Message: "request body unreadable or larger than 1 MiB", status: 400}
		}
		return parseMCP(r.Header, body)
	}()
	if rerr != nil {
		writeRPCError(w, c.id, rerr)
		return
	}
	class := admissionGeneral
	if c.method == "tools/call" && (c.toolName == "task_renew" || c.toolName == "task_release" || c.toolName == "task_complete") {
		class = admissionControl
	}
	a, release, aerr := g.admitAgent(r, a, class)
	setAdmissionTiming(w, r)
	if aerr != nil {
		g.mcpAdmissionError(w, aerr)
		return
	}
	defer release()
	g.serveMCPCall(w, r, a, c)
}

func (g *Gateway) acquireMCPIngress(a *Agent) (func(), *apiError) {
	g.mcpIngressOnce.Do(func() { g.mcpIngress = make(chan struct{}, 64) })
	// One identity cannot occupy all body-read slots even when its execution
	// concurrency is configured as unlimited. This is a separate local bound.
	ingressAgent := *a
	ingressAgent.MaxConcurrency = 4
	if a.MaxConcurrency > 0 {
		ingressAgent.MaxConcurrency = min(a.MaxConcurrency, 4)
	}
	releaseAgent, ok := g.acquireSlot(&ingressAgent, admissionClass("mcp_ingress"))
	if !ok {
		return nil, admissionRejected("mcp_ingress_busy", 100)
	}
	select {
	case g.mcpIngress <- struct{}{}:
		return func() { <-g.mcpIngress; releaseAgent() }, nil
	default:
		releaseAgent()
		return nil, admissionRejected("mcp_ingress_busy", 100)
	}
}

func (g *Gateway) mcpAdmissionError(w http.ResponseWriter, err *apiError) {
	if err.Status == 401 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="swarmpg"`)
	}
	admissionHTTPHeaders(w, err)
	data := map[string]any{"code": err.Code, "retryable": err.Retryable}
	for k, v := range err.Extra {
		data[k] = v
	}
	writeRPCError(w, nil, &rpcError{Code: rpcServerError, Message: err.Message, Data: data, status: err.Status})
}

// serveMCP permits protocol unit tests to exercise an already admitted call.
func (g *Gateway) serveMCP(w http.ResponseWriter, r *http.Request, a *Agent) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		writeRPCError(w, nil, &rpcError{Code: rpcParseError, Message: "request body unreadable or larger than 1 MiB", status: 400})
		return
	}
	c, rerr := parseMCP(r.Header, body)
	if rerr != nil {
		writeRPCError(w, c.id, rerr)
		return
	}
	g.serveMCPCall(w, r, a, c)
}
func (g *Gateway) serveMCPCall(w http.ResponseWriter, r *http.Request, a *Agent, c *mcpCall) {
	if len(c.id) == 0 { // a notification: accepted, nothing to answer
		w.WriteHeader(http.StatusAccepted)
		return
	}
	result, rerr := g.dispatchMCP(r.Context(), a, c)
	if rerr != nil {
		writeRPCError(w, c.id, rerr)
		return
	}
	writeRPC(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": c.id, "result": result})
}

// parseMCP validates the JSON-RPC envelope and picks the protocol era. For a
// 2026-07-28 request it checks the per-request _meta and the mirrored headers.
func parseMCP(h http.Header, body []byte) (*mcpCall, *rpcError) {
	c := &mcpCall{}
	if t := bytes.TrimLeft(body, " \t\r\n"); len(t) > 0 && t[0] == '[' {
		return c, &rpcError{Code: rpcInvalidRequest, Message: "batches are not supported: send one JSON-RPC message per POST", status: http.StatusBadRequest}
	}
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return c, &rpcError{Code: rpcParseError, Message: "invalid JSON: " + err.Error(), status: http.StatusBadRequest}
	}
	c.id, c.method, c.params = req.ID, req.Method, req.Params
	if string(c.id) == "null" {
		c.id = nil
		return c, &rpcError{Code: rpcInvalidRequest, Message: "id must be a string or a number", status: http.StatusBadRequest}
	}
	if req.Result != nil || req.Error != nil {
		return c, &rpcError{Code: rpcInvalidRequest, Message: "this server sends no requests, so it accepts no responses", status: http.StatusBadRequest}
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		return c, &rpcError{Code: rpcInvalidRequest, Message: `expected a JSON-RPC 2.0 message: "jsonrpc": "2.0" and a method`, status: http.StatusBadRequest}
	}
	if len(c.id) == 0 {
		return c, nil // notifications need no version checks: nothing is answered
	}
	var p struct {
		Meta map[string]json.RawMessage `json:"_meta"`
		Name string                     `json:"name"`
	}
	if len(c.params) > 0 && string(c.params) != "null" {
		if err := json.Unmarshal(c.params, &p); err != nil {
			return c, &rpcError{Code: rpcInvalidParams, Message: "params must be an object", status: http.StatusBadRequest}
		}
	}
	c.toolName = p.Name
	header := h.Get("MCP-Protocol-Version")
	_, hasMetaVersion := p.Meta[metaProtocolVersion]
	switch {
	case c.method == "initialize":
		return c, nil // legacy: the version is negotiated in the result
	case header == mcpModern || hasMetaVersion:
		c.modern = true
		return c, checkModern(h, c, p.Meta)
	case header == "":
		return c, nil // a 2025-03-26 client sends no version header
	case isLegacyVersion(header):
		return c, nil
	default:
		return c, unsupportedVersion(header)
	}
}

func checkModern(h http.Header, c *mcpCall, meta map[string]json.RawMessage) *rpcError {
	var version string
	if raw, ok := meta[metaProtocolVersion]; !ok || json.Unmarshal(raw, &version) != nil || version == "" {
		return &rpcError{Code: rpcInvalidParams, Message: "params._meta must carry \"" + metaProtocolVersion + "\"", status: http.StatusBadRequest}
	}
	if version != mcpModern {
		return unsupportedVersion(version)
	}
	if _, ok := meta[metaClientCapabilities]; !ok {
		return &rpcError{Code: rpcInvalidParams, Message: "params._meta must carry \"" + metaClientCapabilities + "\"", status: http.StatusBadRequest}
	}
	if err := headerMatches(h, "MCP-Protocol-Version", version); err != nil {
		return err
	}
	if err := headerMatches(h, "Mcp-Method", c.method); err != nil {
		return err
	}
	if c.method == "tools/call" {
		return headerMatches(h, "Mcp-Name", c.toolName)
	}
	return nil
}

// headerMatches requires a mirrored header to be present and equal to the body
// value. Non-ASCII values arrive as =?base64?...?=.
func headerMatches(h http.Header, name, want string) *rpcError {
	got := h.Get(name)
	if got == "" {
		return &rpcError{Code: rpcHeaderMismatch, Message: fmt.Sprintf("Header mismatch: required header %s is missing", name), status: http.StatusBadRequest}
	}
	if v, ok := strings.CutPrefix(got, "=?base64?"); ok && strings.HasSuffix(v, "?=") {
		dec, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(v, "?="))
		if err != nil {
			return &rpcError{Code: rpcHeaderMismatch, Message: fmt.Sprintf("Header mismatch: %s header is not valid base64", name), status: http.StatusBadRequest}
		}
		got = string(dec)
	}
	if got != want {
		return &rpcError{Code: rpcHeaderMismatch, Message: fmt.Sprintf("Header mismatch: %s header value '%s' does not match body value '%s'", name, got, want), status: http.StatusBadRequest}
	}
	return nil
}

func isLegacyVersion(v string) bool {
	for _, s := range mcpVersions[1:] {
		if v == s {
			return true
		}
	}
	return false
}

func unsupportedVersion(requested string) *rpcError {
	return &rpcError{Code: rpcUnsupportedVersion, Message: "Unsupported protocol version",
		Data: map[string]any{"supported": mcpVersions, "requested": requested}, status: http.StatusBadRequest}
}

// dispatchMCP runs one request. initialize and ping exist only for legacy
// clients; server/discover only for 2026-07-28.
func (g *Gateway) dispatchMCP(ctx context.Context, a *Agent, c *mcpCall) (map[string]any, *rpcError) {
	capabilities := map[string]any{"tools": map[string]any{"listChanged": false}}
	switch {
	case !c.modern && c.method == "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(c.params, &p)
		version := mcpLegacyLatest
		if isLegacyVersion(p.ProtocolVersion) {
			version = p.ProtocolVersion
		}
		return map[string]any{"protocolVersion": version, "capabilities": capabilities,
			"serverInfo": mcpServerInfo, "instructions": mcpInstructions}, nil
	case !c.modern && c.method == "ping":
		return map[string]any{}, nil
	case c.modern && c.method == "server/discover":
		return c.complete(map[string]any{"supportedVersions": mcpVersions, "capabilities": capabilities,
			"instructions": mcpInstructions, "ttlMs": mcpCacheTTLMs, "cacheScope": "public"}), nil
	case c.method == "tools/list":
		var p struct {
			Cursor string `json:"cursor"`
		}
		_ = json.Unmarshal(c.params, &p)
		if p.Cursor != "" {
			return nil, c.invalidParams("unknown cursor: every tool is returned in one page")
		}
		result := map[string]any{"tools": mcpToolDefs}
		if c.modern {
			// The same tools for every agent, so any cache may share the list.
			result["ttlMs"], result["cacheScope"] = mcpCacheTTLMs, "public"
		}
		return c.complete(result), nil
	case c.method == "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(c.params, &p); err != nil || p.Name == "" {
			return nil, c.invalidParams("tools/call needs params.name")
		}
		tool, ok := mcpToolsByName[p.Name]
		if !ok {
			return nil, c.invalidParams("Unknown tool: " + p.Name)
		}
		args := p.Arguments
		if len(args) == 0 || string(args) == "null" {
			args = json.RawMessage("{}")
		}
		return c.complete(tool.run(g, ctx, a, args)), nil
	}
	status := http.StatusOK
	if c.modern {
		status = http.StatusNotFound
	}
	return nil, &rpcError{Code: rpcMethodNotFound, Message: "Method not found: " + c.method, status: status}
}

// complete adds what a 2026-07-28 result must carry.
func (c *mcpCall) complete(result map[string]any) map[string]any {
	if c.modern {
		result["resultType"] = "complete"
		result["_meta"] = map[string]any{metaServerInfo: mcpServerInfo}
	}
	return result
}

func (c *mcpCall) invalidParams(msg string) *rpcError {
	status := http.StatusOK
	if c.modern {
		status = http.StatusBadRequest
	}
	return &rpcError{Code: rpcInvalidParams, Message: msg, status: status}
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, e *rpcError) {
	msg := map[string]any{"jsonrpc": "2.0", "id": nil, "error": e}
	if len(id) > 0 {
		msg["id"] = id
	}
	writeRPC(w, e.status, msg)
}

func writeRPC(w http.ResponseWriter, status int, msg map[string]any) {
	buf, err := json.Marshal(msg)
	if err != nil {
		status, buf = http.StatusInternalServerError, []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"response could not be encoded"}}`)
	}
	if status == 0 {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}
