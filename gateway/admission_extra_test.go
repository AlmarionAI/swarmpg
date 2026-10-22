package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestAdmissionRuntimeAuthorityIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	org, agent := f.agents[0].OrgID, f.agents[0].ID
	queries := []struct {
		name, sql string
		args      []any
	}{
		{"policies", `SELECT * FROM swarmpg.admission_policies`, nil},
		{"buckets", `SELECT * FROM swarmpg.admission_buckets`, nil},
		{"permits", `SELECT * FROM swarmpg.admission_permits`, nil},
		{"read policy", `SELECT swarmpg.admission_policy_get($1::uuid)`, []any{org}},
		{"initialize policy", `SELECT swarmpg._admission_policy($1::uuid)`, []any{org}},
		{"change policy", `SELECT swarmpg.admission_policy_set($1::uuid,'{}'::jsonb)`, []any{org}},
		{"inspect status", `SELECT swarmpg.admission_status($1::uuid)`, []any{org}},
		{"forge permit", `SELECT swarmpg.admission_acquire($1::uuid,$2::uuid,$3::uuid,$4::uuid,'control',1000)`, []any{org, agent, admissionID(t), admissionID(t)}},
		{"release permit", `SELECT swarmpg.admission_release($1::uuid,$2::uuid,$3::uuid)`, []any{org, admissionID(t), admissionID(t)}},
		{"sweep permits", `SELECT swarmpg.admission_sweep(1000)`, nil},
	}
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			err := f.g.withConn(context.Background(), f.agents[0], func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, q.sql, q.args...)
				return err
			})
			var pgerr *pgconn.PgError
			if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
				t.Fatalf("runtime role must be denied before function validation; got %v", err)
			}
		})
	}
}

func TestAdmissionPolicyHTTPAuthorityAndValidationIntegration(t *testing.T) {
	f := coordinationFixture(t)
	f.g.adminKey = "fixture-admission-admin"
	path := "/v1/orgs/" + f.agents[0].OrgID + "/admission"
	policy := map[string]any{"general_rps": 100, "general_burst": 200, "general_concurrency": 3, "control_rps": 50, "control_burst": 100, "control_concurrency": 2}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		coordinationError(t, f, 0, method, path, policy, http.StatusUnauthorized, "unauthorized")
		// An absent bearer token and an incorrect admin key are also denied.
		for _, key := range []string{"", "wrong-admin"} {
			r := httptest.NewRequest(method, path, strings.NewReader(`{}`))
			r.Header.Set("X-Admin-Key", key)
			w := httptest.NewRecorder()
			f.g.routes().ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("%s with admin key %q: HTTP %d", method, key, w.Code)
			}
		}
	}
	before := coordinationOK(t, f, -1, http.MethodPut, path, policy)
	invalid := []map[string]any{nil, {}, {"general_rps": 100}}
	for _, value := range []any{0, -1, 1.5, "10", nil, 1000001} {
		copy := make(map[string]any, len(policy))
		for k, v := range policy {
			copy[k] = v
		}
		copy["general_rps"] = value
		invalid = append(invalid, copy)
	}
	extra := make(map[string]any, len(policy)+1)
	for k, v := range policy {
		extra[k] = v
	}
	extra["unrecognized"] = 1
	invalid = append(invalid, extra)
	for _, body := range invalid {
		coordinationError(t, f, -1, http.MethodPut, path, body, http.StatusBadRequest, "bad_request")
		// SQL administrators get the same strict validation and atomic rejection.
		raw, _ := json.Marshal(body)
		_, err := f.g.admin.Exec(context.Background(), `SELECT swarmpg.admission_policy_set($1::uuid,$2::jsonb)`, f.agents[0].OrgID, string(raw))
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "PT400" {
			t.Fatalf("SQL accepted invalid policy %s: %v", raw, err)
		}
	}
	after := coordinationOK(t, f, -1, http.MethodGet, path, nil)["policy"].(map[string]any)
	beforeJSON, _ := json.Marshal(before)
	afterJSON, _ := json.Marshal(after)
	if string(beforeJSON) != string(afterJSON) {
		t.Fatalf("rejected updates changed policy: before %s after %s", beforeJSON, afterJSON)
	}
}

func TestAdmissionExecutionDeadlineBoundsUnlimitedAgentIntegration(t *testing.T) {
	f := coordinationFixture(t)
	cfg := defaultAdmissionConfig()
	cfg.Execution = 200 * time.Millisecond
	controller, err := newAdmissionController(&postgresAdmissionStore{pool: f.g.admin, gate: make(chan struct{}, 64)}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.close()
	f.g.admission = controller
	f.agents[0].StatementTimeoutMS = 0
	// Inspect the actual server timeout through the same public handler. A
	// zero agent timeout must not remove the admission execution bound.
	out := coordinationOK(t, f, 0, http.MethodPost, "/v1/query", map[string]any{"sql": `SELECT current_setting('statement_timeout')::interval <= interval '200 milliseconds', current_setting('statement_timeout') <> '0'`})
	rows := out["rows"].([]any)
	row := rows[0].([]any)
	if row[0] != true || row[1] != true {
		t.Fatalf("unbounded server statement timeout: %+v", out)
	}
	start := time.Now()
	status, out := coordinationCall(f, 0, http.MethodPost, "/v1/query", map[string]any{"sql": `SELECT pg_sleep(2)`})
	body, _ := out["error"].(map[string]any)
	if (body["code"] != "57014" && body["code"] != "statement_timeout") || (status != 400 && status != 504) {
		t.Fatalf("slow query not canceled: HTTP %d %+v", status, out)
	}
	if time.Since(start) > time.Second {
		t.Fatal("query outlived admission execution bound")
	}
	// Cancellation can race server confirmation. An idle, successfully reset
	// backend permits immediate release; an uncertain backend must retain its
	// shared reservation until the original bounded lease expires.
	var retained int
	var bounded bool
	ttl := (cfg.Execution + admissionMargin).Milliseconds()
	if err := f.g.admin.QueryRow(context.Background(), `SELECT count(*),coalesce(bool_and(
		agent_id=$2::uuid AND gateway_id=$3::uuid AND class='general' AND ttl_ms=$4
		AND expires_at=created_at+ttl_ms*interval '1 millisecond'
		AND expires_at<=clock_timestamp()+$4*interval '1 millisecond'),true)
		FROM swarmpg.admission_permits WHERE org_id=$1::uuid`,
		f.agents[0].OrgID, f.agents[0].ID, controller.gatewayID, ttl).Scan(&retained, &bounded); err != nil {
		t.Fatal(err)
	}
	if retained > 1 || !bounded || int64(retained) != controller.retainedUncertain.Load() {
		t.Fatalf("unsafe cancellation reservation: retained=%d uncertain=%d bounded=%v", retained, controller.retainedUncertain.Load(), bounded)
	}
	if controller.scheduler.snapshot()["general"].(map[string]int)["active"] != 0 {
		t.Fatal("cancellation leaked local capacity")
	}
	coordinationOK(t, f, 0, http.MethodPost, "/v1/query", map[string]any{"sql": `SELECT 1`})
}

type failingReleaseStore struct{ fakeAdmissionStore }

func (s *failingReleaseStore) release(context.Context, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases++
	return errors.New("release unavailable")
}

func TestAdmissionReleaseFailureDoesNotLeakLocalCapacity(t *testing.T) {
	s := &failingReleaseStore{fakeAdmissionStore: fakeAdmissionStore{decision: admissionDecision{Admitted: true, LeaseMS: 20000}}}
	c, err := newAdmissionController(s, defaultAdmissionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	ctx, release, _, failure := c.enter(context.Background(), &Agent{ID: "agent", OrgID: "org"}, admissionGeneral)
	if failure != nil {
		t.Fatal(failure)
	}
	release()
	release()
	if ctx.Err() == nil || c.releaseFailures.Load() != 1 || s.releases != 1 {
		t.Fatal("release failure was not bounded and observed")
	}
	if c.scheduler.snapshot()["general"].(map[string]int)["active"] != 0 {
		t.Fatal("release failure leaked local capacity")
	}
}
