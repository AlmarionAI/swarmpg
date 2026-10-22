// Task coordination uses short database transactions. The SQL functions own the
// authorization, lease clock, fencing and completion invariants; REST and MCP
// share these adapters and never update task tables directly.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const taskJSONLimit = 64 << 10

type taskInput struct {
	AgentID string `json:"agent_id"`
	Key     string `json:"key"`
	Version int64  `json:"version"`
}

type taskEnqueueRequest struct {
	Queue        string          `json:"queue"`
	Key          string          `json:"key"`
	Payload      json.RawMessage `json:"payload"`
	Dependencies []string        `json:"dependencies,omitempty"`
	Inputs       []taskInput     `json:"inputs,omitempty"`
}

type taskClaimRequest struct {
	Queue     string `json:"queue"`
	RequestID string `json:"request_id"`
	LeaseMS   *int   `json:"lease_ms,omitempty"`
}

type taskFenceRequest struct {
	Fence string `json:"fence"`
}

type taskRenewRequest struct {
	Fence   string `json:"fence"`
	LeaseMS *int   `json:"lease_ms,omitempty"`
}

type taskCompleteRequest struct {
	Fence   string          `json:"fence"`
	Result  json.RawMessage `json:"result"`
	Effects json.RawMessage `json:"effects,omitempty"`
}

// Both transports reject unknown fields and extra JSON values. In particular,
// null is not an empty request object, while payload/result may explicitly be null.
func decodeTaskArgs(data []byte, out any) *apiError {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return errBadReq("task arguments must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return errBadReq("invalid task arguments: " + err.Error())
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errBadReq("task arguments must contain exactly one JSON object")
	}
	return nil
}

func readTaskJSON(r *http.Request, out any) *apiError {
	r.Body = http.MaxBytesReader(nil, r.Body, maxRequestBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return errBadReq(err.Error())
	}
	return decodeTaskArgs(data, out)
}

func taskUUID(value, name string) *apiError {
	if len(value) != 32 && (len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-') {
		return errBadReq(name + " must be a UUID")
	}
	var id pgtype.UUID
	if err := id.Scan(value); err != nil || !id.Valid {
		return errBadReq(name + " must be a UUID")
	}
	return nil
}

func taskText(value, name string, max int) *apiError {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) || utf8.RuneCountInString(value) > max {
		return errBadReq(name + " must contain 1 to " + strconv.Itoa(max) + " characters without NUL")
	}
	return nil
}

func taskFence(value string) (int64, *apiError) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != value {
		return 0, errBadReq("fence must be a positive decimal int64 string returned by task_claim")
	}
	return n, nil
}

func taskLease(value *int) (int, *apiError) {
	ms := 30000
	if value != nil {
		ms = *value
	}
	if ms < 100 || ms > 300000 {
		return 0, errBadReq("lease_ms must be between 100 and 300000")
	}
	return ms, nil
}

func taskJSON(value json.RawMessage, name string) *apiError {
	if len(value) == 0 || !json.Valid(value) {
		return errBadReq(name + " is required and must be JSON")
	}
	if len(value) > taskJSONLimit {
		return errBadReq(name + " must be at most 64 KiB")
	}
	return nil
}

func taskResultLimit() *apiError {
	return &apiError{Status: http.StatusRequestEntityTooLarge, Code: "result_limit_exceeded",
		Message: "complete task response exceeds the agent's result budget; operation was not committed",
		Hint:    "Raise the result budget before retrying; task responses are never truncated."}
}

// Check the complete JSON result before withConn commits. Returning a truncated
// claim/completion receipt would make a successful transition unsafe to consume.
func (g *Gateway) taskCall(ctx context.Context, a *Agent, sql string, args ...any) (json.RawMessage, *apiError) {
	if a.MaxRows < 1 {
		return nil, taskResultLimit()
	}
	var out json.RawMessage
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, sql, args...).Scan(&out); err != nil {
			return err
		}
		encoded, err := json.Marshal(out)
		if err != nil {
			return err
		}
		if a.MaxResultBytes > 0 && int64(len(encoded)) > a.MaxResultBytes {
			return taskResultLimit()
		}
		out = encoded
		return nil
	})
	if err != nil {
		return nil, classifyPg(err)
	}
	return out, nil
}

func (g *Gateway) taskEnqueue(ctx context.Context, a *Agent, in taskEnqueueRequest) (json.RawMessage, *apiError) {
	if err := taskText(in.Queue, "queue", 128); err != nil {
		return nil, err
	}
	if err := taskText(in.Key, "key", 256); err != nil {
		return nil, err
	}
	if err := taskJSON(in.Payload, "payload"); err != nil {
		return nil, err
	}
	if len(in.Dependencies) > 64 || len(in.Inputs) > 64 {
		return nil, errBadReq("at most 64 dependencies and 64 inputs are allowed")
	}
	for _, id := range in.Dependencies {
		if err := taskUUID(id, "dependency"); err != nil {
			return nil, err
		}
	}
	for _, input := range in.Inputs {
		if err := taskUUID(input.AgentID, "input agent_id"); err != nil {
			return nil, err
		}
		if err := taskText(input.Key, "input key", 256); err != nil {
			return nil, err
		}
		if input.Version <= 0 {
			return nil, errBadReq("input version must be positive")
		}
	}
	if in.Dependencies == nil {
		in.Dependencies = []string{}
	}
	if in.Inputs == nil {
		in.Inputs = []taskInput{}
	}
	inputs, err := json.Marshal(in.Inputs)
	if err != nil {
		return nil, errBadReq(err.Error())
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_enqueue($1, $2, $3::jsonb, $4::uuid[], $5::jsonb)`,
		in.Queue, in.Key, string(in.Payload), in.Dependencies, string(inputs))
}

func (g *Gateway) taskGet(ctx context.Context, a *Agent, id string) (json.RawMessage, *apiError) {
	if err := taskUUID(id, "task id"); err != nil {
		return nil, err
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_get($1::uuid)`, id)
}

func (g *Gateway) taskClaim(ctx context.Context, a *Agent, in taskClaimRequest) (json.RawMessage, *apiError) {
	if err := taskText(in.Queue, "queue", 128); err != nil {
		return nil, err
	}
	if err := taskUUID(in.RequestID, "request_id"); err != nil {
		return nil, err
	}
	ms, err := taskLease(in.LeaseMS)
	if err != nil {
		return nil, err
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_claim($1, $2::uuid, $3)`, in.Queue, in.RequestID, ms)
}

func (g *Gateway) taskRenew(ctx context.Context, a *Agent, id string, in taskRenewRequest) (json.RawMessage, *apiError) {
	if err := taskUUID(id, "task id"); err != nil {
		return nil, err
	}
	fence, err := taskFence(in.Fence)
	if err != nil {
		return nil, err
	}
	ms, err := taskLease(in.LeaseMS)
	if err != nil {
		return nil, err
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_renew($1::uuid, $2::bigint, $3)`, id, fence, ms)
}

func (g *Gateway) taskRelease(ctx context.Context, a *Agent, id, value string) (json.RawMessage, *apiError) {
	if err := taskUUID(id, "task id"); err != nil {
		return nil, err
	}
	fence, err := taskFence(value)
	if err != nil {
		return nil, err
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_release($1::uuid, $2::bigint)`, id, fence)
}

func (g *Gateway) taskComplete(ctx context.Context, a *Agent, id string, in taskCompleteRequest) (json.RawMessage, *apiError) {
	if err := taskUUID(id, "task id"); err != nil {
		return nil, err
	}
	fence, err := taskFence(in.Fence)
	if err != nil {
		return nil, err
	}
	if err := taskJSON(in.Result, "result"); err != nil {
		return nil, err
	}
	effects, effectsErr := taskEffects(in.Effects)
	if effectsErr != nil {
		return nil, effectsErr
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_complete($1::uuid, $2::bigint, $3::jsonb, $4::jsonb)`, id, fence, string(in.Result), string(effects))
}

func (g *Gateway) taskCancel(ctx context.Context, a *Agent, id string) (json.RawMessage, *apiError) {
	if err := taskUUID(id, "task id"); err != nil {
		return nil, err
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_cancel($1::uuid)`, id)
}

func (g *Gateway) taskHistory(ctx context.Context, a *Agent, id string, limit *int) (json.RawMessage, *apiError) {
	if err := taskUUID(id, "task id"); err != nil {
		return nil, err
	}
	n := 50
	if limit != nil {
		n = *limit
	}
	if n < 1 || n > 1000 {
		return nil, errBadReq("limit must be between 1 and 1000")
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_history($1::uuid, $2)`, id, n)
}

func (g *Gateway) taskRoutes(mux *http.ServeMux) {
	respond := func(w http.ResponseWriter, out json.RawMessage, err *apiError) {
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, out)
	}
	mux.HandleFunc("POST /v1/tasks", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in taskEnqueueRequest
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		out, err := g.taskEnqueue(r.Context(), a, in)
		respond(w, out, err)
	}))
	mux.HandleFunc("POST /v1/tasks/claim", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in taskClaimRequest
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		out, err := g.taskClaim(r.Context(), a, in)
		respond(w, out, err)
	}))
	mux.HandleFunc("GET /v1/tasks/{id}", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		out, err := g.taskGet(r.Context(), a, r.PathValue("id"))
		respond(w, out, err)
	}))
	mux.HandleFunc("POST /v1/tasks/{id}/renew", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in taskRenewRequest
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		out, err := g.taskRenew(r.Context(), a, r.PathValue("id"), in)
		respond(w, out, err)
	}))
	mux.HandleFunc("POST /v1/tasks/{id}/release", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in taskFenceRequest
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		out, err := g.taskRelease(r.Context(), a, r.PathValue("id"), in.Fence)
		respond(w, out, err)
	}))
	mux.HandleFunc("POST /v1/tasks/{id}/complete", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in taskCompleteRequest
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		out, err := g.taskComplete(r.Context(), a, r.PathValue("id"), in)
		respond(w, out, err)
	}))
	mux.HandleFunc("POST /v1/tasks/{id}/cancel", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in struct{}
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		out, err := g.taskCancel(r.Context(), a, r.PathValue("id"))
		respond(w, out, err)
	}))
	mux.HandleFunc("GET /v1/tasks/{id}/history", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var limit *int
		if r.URL.Query().Has("limit") {
			n, err := strconv.Atoi(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, errBadReq("limit must be an integer"))
				return
			}
			limit = &n
		}
		out, err := g.taskHistory(r.Context(), a, r.PathValue("id"), limit)
		respond(w, out, err)
	}))
	mux.HandleFunc("POST /v1/admin/tasks/{id}/recover", g.withAdmin(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Reason string `json:"reason"`
		}
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		if err := taskUUID(r.PathValue("id"), "task id"); err != nil {
			writeErr(w, err)
			return
		}
		if err := taskText(in.Reason, "reason", 1000); err != nil {
			writeErr(w, err)
			return
		}
		var out json.RawMessage
		if err := g.admin.QueryRow(r.Context(), `SELECT swarmpg.task_recover($1::uuid, $2)`, r.PathValue("id"), in.Reason).Scan(&out); err != nil {
			writeErr(w, classifyPg(err))
			return
		}
		respond(w, out, nil)
	}))
}
