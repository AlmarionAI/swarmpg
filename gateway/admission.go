package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const admissionMargin = 10 * time.Second
const admissionDBTimeout = 2 * time.Second

var errAdmissionBusy = errors.New("admission database capacity exhausted")

type admissionConfig struct {
	General, Control, OrgQueue, TotalQueue int
	Wait, Execution                        time.Duration
}

func defaultAdmissionConfig() admissionConfig {
	return admissionConfig{General: 24, Control: 8, OrgQueue: 128, TotalQueue: 1024, Wait: 2 * time.Second, Execution: 10 * time.Second}
}

func admissionConfigFromEnv() (admissionConfig, error) {
	c := defaultAdmissionConfig()
	for _, setting := range []struct {
		name     string
		value    *int
		min, max int
	}{
		{"ADMISSION_GENERAL_CAPACITY", &c.General, 1, 31}, {"ADMISSION_CONTROL_CAPACITY", &c.Control, 1, 31},
		{"ADMISSION_ORG_QUEUE", &c.OrgQueue, 1, 10000}, {"ADMISSION_TOTAL_QUEUE", &c.TotalQueue, 2, 100000},
	} {
		n, err := strconv.Atoi(env(setting.name, strconv.Itoa(*setting.value)))
		if err != nil || n < setting.min || n > setting.max {
			return c, fmt.Errorf("invalid %s", setting.name)
		}
		*setting.value = n
	}
	for _, setting := range []struct {
		name     string
		value    *time.Duration
		min, max int
	}{
		{"ADMISSION_WAIT_MS", &c.Wait, 1, 10000}, {"ADMISSION_EXECUTION_MS", &c.Execution, 100, 60000},
	} {
		n, err := strconv.Atoi(env(setting.name, strconv.Itoa(int(setting.value.Milliseconds()))))
		if err != nil || n < setting.min || n > setting.max {
			return c, fmt.Errorf("invalid %s", setting.name)
		}
		*setting.value = time.Duration(n) * time.Millisecond
	}
	if c.General+c.Control > 32 || c.OrgQueue > c.TotalQueue {
		return c, errors.New("admission capacities exceed runtime pool or total queue")
	}
	return c, nil
}

type admissionDecision struct {
	Admitted     bool   `json:"admitted"`
	Reason       string `json:"reason"`
	RetryAfterMS int    `json:"retry_after_ms"`
	LeaseMS      int    `json:"lease_ms"`
	Replayed     bool   `json:"replayed"`
}

// A lost runtime connection is not proof that its backend stopped executing.
// Retain the lease through its conservative expiry unless session cleanup was
// confirmed. Socket close and a best-effort cancel packet are insufficient.
type admissionRuntimeKey struct{}
type admissionRuntimeState struct{ uncertain atomic.Bool }

func markAdmissionRuntimeUncertain(ctx context.Context) {
	if state, ok := ctx.Value(admissionRuntimeKey{}).(*admissionRuntimeState); ok {
		state.uncertain.Store(true)
	}
}

type admissionStore interface {
	acquire(context.Context, string, string, string, string, admissionClass, int) (admissionDecision, error)
	release(context.Context, string, string, string) error
	sweep(context.Context) error
}

type postgresAdmissionStore struct {
	pool     *pgxpool.Pool
	gate     chan struct{}
	recovery *recoveryConfig
}

func (s *postgresAdmissionStore) call(ctx context.Context, query string, args ...any) (json.RawMessage, error) {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	default:
		return nil, errAdmissionBusy
	}
	ctx, cancel := context.WithTimeout(ctx, admissionDBTimeout)
	defer cancel()
	var raw json.RawMessage
	if s.recovery == nil {
		// Preserve the single-statement transaction in legacy mode: the server
		// releases the shared bucket lock before returning the receipt. Recovery
		// mode needs an explicit COMMIT to observe replication warnings.
		err := s.pool.QueryRow(ctx, query, args...).Scan(&raw)
		return raw, err
	}
	err := recoveryTransaction(ctx, s.pool, s.recovery, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, query, args...).Scan(&raw)
	})
	return raw, err
}

func (s *postgresAdmissionStore) acquire(ctx context.Context, org, agent, gateway, request string, class admissionClass, ttl int) (admissionDecision, error) {
	raw, err := s.call(ctx, `SELECT swarmpg.admission_acquire($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5,$6)`, org, agent, gateway, request, string(class), ttl)
	var d admissionDecision
	if err == nil {
		err = json.Unmarshal(raw, &d)
	}
	if err == nil && ((!d.Admitted && d.Reason != "org_rate_limited" && d.Reason != "org_concurrency_limited") || (d.Admitted && d.LeaseMS <= 0)) {
		err = errors.New("invalid admission receipt")
	}
	return d, err
}

func (s *postgresAdmissionStore) release(ctx context.Context, org, gateway, request string) error {
	_, err := s.call(ctx, `SELECT to_jsonb(swarmpg.admission_release($1::uuid,$2::uuid,$3::uuid))`, org, gateway, request)
	return err
}

func (s *postgresAdmissionStore) sweep(ctx context.Context) error {
	_, err := s.call(ctx, `SELECT to_jsonb(swarmpg.admission_sweep(1000))`)
	return err
}

type admissionController struct {
	cfg                                            admissionConfig
	gatewayID                                      string
	store                                          admissionStore
	scheduler                                      *admissionScheduler
	mu                                             sync.Mutex
	admitted                                       map[admissionClass]int64
	rejected                                       map[string]int64
	waitCount, waitNS, executionCount, executionNS atomic.Int64
	releaseFailures                                atomic.Int64
	retainedUncertain                              atomic.Int64
	cancel                                         context.CancelFunc
	done                                           chan struct{}
}

func newAdmissionController(store admissionStore, cfg admissionConfig) (*admissionController, error) {
	id, err := auditID()
	if err != nil {
		return nil, err
	}
	c := &admissionController{cfg: cfg, gatewayID: id, store: store, scheduler: newAdmissionScheduler(cfg.General, cfg.Control, cfg.OrgQueue, cfg.TotalQueue), admitted: map[admissionClass]int64{}, rejected: map[string]int64{}, done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepCtx, stop := context.WithTimeout(ctx, admissionDBTimeout)
				_ = store.sweep(sweepCtx)
				stop()
			}
		}
	}()
	return c, nil
}

func admissionUnavailable() *apiError {
	return &apiError{Status: 503, Code: "admission_unavailable", Message: "organization admission service unavailable; request was not executed", Retryable: true}
}
func admissionRejected(reason string, retryMS int) *apiError {
	return &apiError{Status: 429, Code: reason, Message: "request exceeds admission capacity", Retryable: true, Extra: map[string]any{"retry_after_ms": max(retryMS, 1)}}
}

func (c *admissionController) reject(err *apiError) *apiError {
	c.mu.Lock()
	c.rejected[err.Code]++
	c.mu.Unlock()
	return err
}

func (c *admissionController) cleanup(org, request string) {
	ctx, cancel := context.WithTimeout(context.Background(), admissionDBTimeout)
	defer cancel()
	if err := c.store.release(ctx, org, c.gatewayID, request); err != nil {
		c.releaseFailures.Add(1)
	}
}

func (c *admissionController) enter(parent context.Context, a *Agent, class admissionClass) (context.Context, func(), time.Duration, *apiError) {
	start := time.Now()
	queueCtx, cancelQueue := context.WithTimeout(parent, c.cfg.Wait)
	localRelease, err := c.scheduler.acquire(queueCtx, a.OrgID, class)
	cancelQueue()
	if err != nil {
		c.waitCount.Add(1)
		c.waitNS.Add(int64(time.Since(start)))
		return parent, nil, time.Since(start), c.reject(err)
	}
	request, idErr := auditID()
	if idErr != nil {
		localRelease()
		return parent, nil, time.Since(start), c.reject(admissionUnavailable())
	}
	acquireStart := time.Now()
	acquireCtx, cancelAcquire := context.WithTimeout(parent, admissionDBTimeout)
	ttl := c.cfg.Execution + admissionMargin
	decision, dbErr := c.store.acquire(acquireCtx, a.OrgID, a.ID, c.gatewayID, request, class, int(ttl.Milliseconds()))
	cancelAcquire()
	waited := time.Since(start)
	c.waitCount.Add(1)
	c.waitNS.Add(int64(waited))
	if dbErr != nil {
		// A timeout can hide a committed grant. Never change its UUID or execute
		// work; independent cleanup is best effort and expiry is the fallback.
		c.cleanup(a.OrgID, request)
		localRelease()
		if errors.Is(dbErr, errAdmissionBusy) {
			return parent, nil, waited, c.reject(admissionRejected("admission_busy", 100))
		}
		var pe *pgconn.PgError
		if errors.As(dbErr, &pe) && pe.Code == "42501" {
			return parent, nil, waited, c.reject(errForbidden("agent disabled, expired, or outside organization"))
		}
		return parent, nil, waited, c.reject(admissionUnavailable())
	}
	if !decision.Admitted {
		localRelease()
		return parent, nil, waited, c.reject(admissionRejected(decision.Reason, decision.RetryAfterMS))
	}
	// Subtract the entire acquisition RTT, even though SQL already returns
	// remaining time. This is conservative and never compares wall clocks.
	remaining := time.Duration(decision.LeaseMS)*time.Millisecond - time.Since(acquireStart) - admissionMargin
	budget := min(c.cfg.Execution, remaining)
	if budget <= 0 || parent.Err() != nil {
		c.cleanup(a.OrgID, request)
		localRelease()
		return parent, nil, waited, c.reject(admissionUnavailable())
	}
	runtimeState := &admissionRuntimeState{}
	ctx, cancel := context.WithTimeout(context.WithValue(parent, admissionRuntimeKey{}, runtimeState), budget)
	c.mu.Lock()
	c.admitted[class]++
	c.mu.Unlock()
	executionStart := time.Now()
	var once sync.Once
	release := func() {
		once.Do(func() {
			cancel()
			c.executionCount.Add(1)
			c.executionNS.Add(int64(time.Since(executionStart)))
			if runtimeState.uncertain.Load() {
				c.retainedUncertain.Add(1)
			} else {
				c.cleanup(a.OrgID, request)
			}
			localRelease()
		})
	}
	return ctx, release, waited, nil
}

func (c *admissionController) close() { c.scheduler.close(); c.cancel(); <-c.done }

func (c *admissionController) status() map[string]any {
	out := c.scheduler.snapshot()
	c.mu.Lock()
	admitted := map[string]int64{"general": c.admitted[admissionGeneral], "control": c.admitted[admissionControl]}
	rejected := map[string]int64{}
	for k, v := range c.rejected {
		rejected[k] = v
	}
	c.mu.Unlock()
	out["enabled"] = true
	out["gateway_id"] = c.gatewayID
	out["execution_ms"] = c.cfg.Execution.Milliseconds()
	out["permit_ttl_ms"] = (c.cfg.Execution + admissionMargin).Milliseconds()
	out["wait_ms"] = c.cfg.Wait.Milliseconds()
	out["capacities"] = map[string]int{"general": c.cfg.General, "control": c.cfg.Control, "org_queue": c.cfg.OrgQueue, "total_queue": c.cfg.TotalQueue}
	out["admitted"] = admitted
	out["rejected"] = rejected
	out["wait_count"] = c.waitCount.Load()
	out["wait_ms_total"] = float64(c.waitNS.Load()) / 1e6
	out["execution_count"] = c.executionCount.Load()
	out["execution_ms_total"] = float64(c.executionNS.Load()) / 1e6
	out["release_failures"] = c.releaseFailures.Load()
	out["retained_uncertain_permits"] = c.retainedUncertain.Load()
	return out
}

func admissionHTTPHeaders(w http.ResponseWriter, err *apiError) {
	if err.Status == 429 {
		seconds := 1
		if ms, ok := err.Extra["retry_after_ms"].(int); ok {
			seconds = max(1, (ms+999)/1000)
		}
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
}

func restAdmissionClass(r *http.Request) admissionClass {
	switch r.Pattern {
	case "POST /v1/tasks/{id}/renew", "POST /v1/tasks/{id}/release", "POST /v1/tasks/{id}/complete":
		return admissionControl
	}
	return admissionGeneral
}

func (g *Gateway) admissionStatus() map[string]any {
	if g.admission == nil {
		return map[string]any{"enabled": false}
	}
	return g.admission.status()
}

func validateAdmissionPolicy(raw json.RawMessage) *apiError {
	var policy map[string]json.RawMessage
	if err := decodeTaskArgs(raw, &policy); err != nil {
		return err
	}
	fields := map[string]int{"general_rps": 1000000, "general_burst": 1000000, "general_concurrency": 10000, "control_rps": 1000000, "control_burst": 1000000, "control_concurrency": 10000}
	if len(policy) != len(fields) {
		return errBadReq("admission policy requires exactly six class limit fields")
	}
	for key, limit := range fields {
		var value *int
		if err := json.Unmarshal(policy[key], &value); err != nil || value == nil || *value < 1 || *value > limit {
			return errBadReq("invalid admission policy field: " + key)
		}
	}
	return nil
}

func (g *Gateway) admissionRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if g.quota == nil || g.admission == nil {
			writeErr(w, admissionUnavailable())
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), admissionDBTimeout)
		defer cancel()
		// A reachable database with an older schema must not be advertised as
		// ready. Catalog checks do not consume an admission permit or create an
		// organization. Runtime dependency failures still fail closed per call.
		var installed bool
		err := g.quota.QueryRow(ctx, `SELECT
			to_regclass('swarmpg.admission_policies') IS NOT NULL AND
			to_regclass('swarmpg.admission_buckets') IS NOT NULL AND
			to_regclass('swarmpg.admission_permits') IS NOT NULL AND
			to_regprocedure('swarmpg._admission_policy(uuid)') IS NOT NULL AND
			to_regprocedure('swarmpg._admission_policy_json(swarmpg.admission_policies)') IS NOT NULL AND
			to_regprocedure('swarmpg.admission_policy_get(uuid)') IS NOT NULL AND
			to_regprocedure('swarmpg.admission_policy_set(uuid,jsonb)') IS NOT NULL AND
			to_regprocedure('swarmpg.admission_acquire(uuid,uuid,uuid,uuid,text,integer)') IS NOT NULL AND
			to_regprocedure('swarmpg.admission_release(uuid,uuid,uuid)') IS NOT NULL AND
			to_regprocedure('swarmpg.admission_sweep(integer)') IS NOT NULL AND
			to_regprocedure('swarmpg.admission_status(uuid)') IS NOT NULL`).Scan(&installed)
		if err != nil || !installed {
			writeErr(w, admissionUnavailable())
			return
		}
		if err := g.recoveryReady(ctx); err != nil {
			writeErr(w, classifyPg(err))
			return
		}
		writeJSON(w, 200, map[string]bool{"ready": true})
	})
	policyCall := func(w http.ResponseWriter, r *http.Request, raw json.RawMessage) {
		if err := taskUUID(r.PathValue("id"), "org id"); err != nil {
			writeErr(w, err)
			return
		}
		if g.admission == nil {
			writeErr(w, admissionUnavailable())
			return
		}
		store, ok := g.admission.store.(*postgresAdmissionStore)
		if !ok {
			writeErr(w, admissionUnavailable())
			return
		}
		query := `SELECT swarmpg.admission_status($1::uuid)`
		args := []any{r.PathValue("id")}
		if raw != nil {
			query = `SELECT swarmpg.admission_policy_set($1::uuid,$2::jsonb)`
			args = append(args, string(raw))
		}
		out, err := store.call(r.Context(), query, args...)
		if err != nil {
			writeErr(w, classifyPg(err))
			return
		}
		writeJSON(w, 200, out)
	}
	mux.HandleFunc("GET /v1/orgs/{id}/admission", g.withAdmin(func(w http.ResponseWriter, r *http.Request) { policyCall(w, r, nil) }))
	mux.HandleFunc("PUT /v1/orgs/{id}/admission", g.withAdmin(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		if err := readTaskJSON(r, &raw); err != nil {
			writeErr(w, err)
			return
		}
		if err := validateAdmissionPolicy(raw); err != nil {
			writeErr(w, err)
			return
		}
		policyCall(w, r, raw)
	}))
}
