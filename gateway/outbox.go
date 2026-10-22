package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"regexp"
)

const outboxNamePattern = `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`

var outboxNameRE = regexp.MustCompile(outboxNamePattern)

type taskEffect struct {
	Key         string          `json:"key"`
	Destination string          `json:"destination"`
	Payload     json.RawMessage `json:"payload"`
}

func outboxName(value, name string) *apiError {
	if !outboxNameRE.MatchString(value) {
		return errBadReq(name + " must match " + outboxNamePattern)
	}
	return nil
}

// SQL enforces normalized JSONB sizes and canonical ordering. Wire JSON can be
// larger (escaped strings) or smaller (expanded numbers) than JSONB text, so the
// adapter only validates and compacts it without converting numbers to float64.
func outboxJSON(value json.RawMessage, name string) (json.RawMessage, *apiError) {
	var compact bytes.Buffer
	if len(value) == 0 || json.Compact(&compact, value) != nil {
		return nil, errBadReq(name + " is required and must be JSON")
	}
	return compact.Bytes(), nil
}

func taskEffects(raw json.RawMessage) (json.RawMessage, *apiError) {
	if len(raw) == 0 {
		return json.RawMessage(`[]`), nil
	}
	compact, err := outboxJSON(raw, "effects")
	if err != nil {
		return nil, err
	}
	if compact[0] != '[' {
		return nil, errBadReq("effects must be an array")
	}
	var effects []taskEffect
	dec := json.NewDecoder(bytes.NewReader(compact))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&effects); err != nil {
		return nil, errBadReq("invalid effects: " + err.Error())
	}
	if len(effects) > 32 {
		return nil, errBadReq("at most 32 effects are allowed")
	}
	seen := make(map[string]bool, len(effects))
	for _, effect := range effects {
		if err := outboxName(effect.Key, "effect key"); err != nil {
			return nil, err
		}
		if seen[effect.Key] {
			return nil, errBadReq("effect keys must be unique within a task")
		}
		seen[effect.Key] = true
		if err := outboxName(effect.Destination, "effect destination"); err != nil {
			return nil, err
		}
		if _, err := outboxJSON(effect.Payload, "effect payload"); err != nil {
			return nil, err
		}
	}
	return compact, nil
}

func (g *Gateway) taskOutbox(ctx context.Context, a *Agent, id string) (json.RawMessage, *apiError) {
	if err := taskUUID(id, "task id"); err != nil {
		return nil, err
	}
	return g.taskCall(ctx, a, `SELECT swarmpg.task_outbox($1::uuid)`, id)
}

type outboxClaimRequest struct {
	OrgID       string          `json:"org_id"`
	Destination string          `json:"destination"`
	WorkerID    string          `json:"worker_id"`
	RequestID   string          `json:"request_id"`
	LeaseMS     json.RawMessage `json:"lease_ms,omitempty"`
}

type outboxOwnerRequest struct {
	OrgID    string `json:"org_id"`
	WorkerID string `json:"worker_id"`
	Fence    string `json:"fence"`
}

type outboxRenewRequest struct {
	outboxOwnerRequest
	LeaseMS *int `json:"lease_ms"`
}

type outboxAckRequest struct {
	outboxOwnerRequest
	Response json.RawMessage `json:"response"`
}

type outboxNackRequest struct {
	outboxOwnerRequest
	Error     string `json:"error"`
	RetryMS   *int   `json:"retry_ms"`
	Permanent *bool  `json:"permanent"`
}

type outboxRetryRequest struct {
	OrgID  string `json:"org_id"`
	Reason string `json:"reason"`
}

func outboxIdentity(org, id string) *apiError {
	if err := taskUUID(org, "org_id"); err != nil {
		return err
	}
	return taskUUID(id, "outbox id")
}

func outboxOwner(id string, in outboxOwnerRequest) (int64, *apiError) {
	if err := outboxIdentity(in.OrgID, id); err != nil {
		return 0, err
	}
	if err := taskUUID(in.WorkerID, "worker_id"); err != nil {
		return 0, err
	}
	return taskFence(in.Fence)
}

func outboxClaimLease(raw json.RawMessage) (int, *apiError) {
	if len(raw) == 0 {
		return taskLease(nil)
	}
	var lease *int
	if err := json.Unmarshal(raw, &lease); err != nil || lease == nil {
		return 0, errBadReq("lease_ms must be an integer")
	}
	return taskLease(lease)
}

// These methods are reachable only through withAdmin. SQL owns all durable
// claim receipts, fencing and state changes; an adapter never updates tables.
func (g *Gateway) outboxCall(ctx context.Context, query string, args ...any) (json.RawMessage, *apiError) {
	var out json.RawMessage
	if err := g.admin.QueryRow(ctx, query, args...).Scan(&out); err != nil {
		return nil, classifyPg(err)
	}
	return out, nil
}

func (g *Gateway) outboxClaim(ctx context.Context, _ string, in outboxClaimRequest) (json.RawMessage, *apiError) {
	for _, field := range []struct{ value, name string }{{in.OrgID, "org_id"}, {in.WorkerID, "worker_id"}, {in.RequestID, "request_id"}} {
		if err := taskUUID(field.value, field.name); err != nil {
			return nil, err
		}
	}
	if err := outboxName(in.Destination, "destination"); err != nil {
		return nil, err
	}
	lease, err := outboxClaimLease(in.LeaseMS)
	if err != nil {
		return nil, err
	}
	return g.outboxCall(ctx, `SELECT swarmpg.outbox_claim($1::uuid, $2, $3::uuid, $4::uuid, $5)`, in.OrgID, in.Destination, in.WorkerID, in.RequestID, lease)
}

func (g *Gateway) outboxRenew(ctx context.Context, id string, in outboxRenewRequest) (json.RawMessage, *apiError) {
	fence, err := outboxOwner(id, in.outboxOwnerRequest)
	if err != nil {
		return nil, err
	}
	if in.LeaseMS == nil {
		return nil, errBadReq("lease_ms is required and must be an integer")
	}
	lease, err := taskLease(in.LeaseMS)
	if err != nil {
		return nil, err
	}
	return g.outboxCall(ctx, `SELECT swarmpg.outbox_renew($1::uuid, $2::uuid, $3::uuid, $4::bigint, $5)`, in.OrgID, id, in.WorkerID, fence, lease)
}

func (g *Gateway) outboxAck(ctx context.Context, id string, in outboxAckRequest) (json.RawMessage, *apiError) {
	fence, err := outboxOwner(id, in.outboxOwnerRequest)
	if err != nil {
		return nil, err
	}
	response, err := outboxJSON(in.Response, "response")
	if err != nil {
		return nil, err
	}
	return g.outboxCall(ctx, `SELECT swarmpg.outbox_ack($1::uuid, $2::uuid, $3::uuid, $4::bigint, $5::jsonb)`, in.OrgID, id, in.WorkerID, fence, string(response))
}

func (g *Gateway) outboxNack(ctx context.Context, id string, in outboxNackRequest) (json.RawMessage, *apiError) {
	fence, err := outboxOwner(id, in.outboxOwnerRequest)
	if err != nil {
		return nil, err
	}
	if err := taskText(in.Error, "error", 1000); err != nil {
		return nil, err
	}
	if in.RetryMS == nil || *in.RetryMS < 0 || *in.RetryMS > 3600000 {
		return nil, errBadReq("retry_ms is required and must be between 0 and 3600000")
	}
	if in.Permanent == nil {
		return nil, errBadReq("permanent is required and must be a boolean")
	}
	return g.outboxCall(ctx, `SELECT swarmpg.outbox_nack($1::uuid, $2::uuid, $3::uuid, $4::bigint, $5, $6, $7)`, in.OrgID, id, in.WorkerID, fence, in.Error, *in.RetryMS, *in.Permanent)
}

func (g *Gateway) outboxRetry(ctx context.Context, id string, in outboxRetryRequest) (json.RawMessage, *apiError) {
	if err := outboxIdentity(in.OrgID, id); err != nil {
		return nil, err
	}
	if err := taskText(in.Reason, "reason", 1000); err != nil {
		return nil, err
	}
	return g.outboxCall(ctx, `SELECT swarmpg.outbox_retry($1::uuid, $2::uuid, $3)`, in.OrgID, id, in.Reason)
}

func outboxRespond(w http.ResponseWriter, out json.RawMessage, err *apiError) {
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func outboxAdminHandler[T any](g *Gateway, call func(context.Context, string, T) (json.RawMessage, *apiError)) http.HandlerFunc {
	return g.withAdmin(func(w http.ResponseWriter, r *http.Request) {
		var in T
		if err := readTaskJSON(r, &in); err != nil {
			writeErr(w, err)
			return
		}
		out, err := call(r.Context(), r.PathValue("id"), in)
		outboxRespond(w, out, err)
	})
}

func (g *Gateway) outboxRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/tasks/{id}/outbox", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		out, err := g.taskOutbox(r.Context(), a, r.PathValue("id"))
		outboxRespond(w, out, err)
	}))
	mux.HandleFunc("POST /v1/admin/outbox/claim", outboxAdminHandler(g, g.outboxClaim))
	mux.HandleFunc("POST /v1/admin/outbox/{id}/renew", outboxAdminHandler(g, g.outboxRenew))
	mux.HandleFunc("POST /v1/admin/outbox/{id}/ack", outboxAdminHandler(g, g.outboxAck))
	mux.HandleFunc("POST /v1/admin/outbox/{id}/nack", outboxAdminHandler(g, g.outboxNack))
	mux.HandleFunc("POST /v1/admin/outbox/{id}/retry", outboxAdminHandler(g, g.outboxRetry))
	mux.HandleFunc("GET /v1/admin/outbox/{id}", g.withAdmin(func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		orgs := values["org_id"]
		if len(values) != 1 || len(orgs) != 1 {
			writeErr(w, errBadReq("exactly one org_id query parameter is required"))
			return
		}
		if err := outboxIdentity(orgs[0], r.PathValue("id")); err != nil {
			writeErr(w, err)
			return
		}
		out, err := g.outboxCall(r.Context(), `SELECT swarmpg.outbox_get($1::uuid, $2::uuid)`, orgs[0], r.PathValue("id"))
		outboxRespond(w, out, err)
	}))
}
