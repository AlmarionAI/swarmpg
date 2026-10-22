// Reproducible collaborative task workload. Run through coordination-e2e.sh for
// owned, disposable database and gateway crash/restart injection.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	Gateway, AdminKey, Output, FaultControl string
	Workers, InFlight, LeaseMS              int
	Seed                                    int64
	Duration                                time.Duration
}

type agent struct {
	ID, Token string
	Tenant    int
}

type input struct {
	AgentID string `json:"agent_id"`
	Key     string `json:"key"`
	Version int64  `json:"version"`
}

type payload struct {
	Add     int `json:"add"`
	Ordinal int `json:"ordinal"`
	Wave    int `json:"wave"`
}

type result struct {
	Value int `json:"value"`
}

type task struct {
	ID           string          `json:"id"`
	OrgID        string          `json:"org_id"`
	Queue        string          `json:"queue"`
	Key          string          `json:"task_key"`
	Payload      payload         `json:"payload"`
	Dependencies []string        `json:"dependencies"`
	Inputs       []input         `json:"inputs"`
	State        string          `json:"state"`
	Owner        string          `json:"owner_agent_id"`
	Fence        string          `json:"fence"`
	Attempts     int             `json:"attempts"`
	Result       json.RawMessage `json:"result"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	LeaseUntil   *time.Time      `json:"lease_until"`
}

type receipt struct {
	Task     *task `json:"task"`
	Replayed bool  `json:"replayed"`
}

type expectedTask struct {
	Task   task
	Value  int
	Tenant int
}

type event struct {
	At       time.Time `json:"at"`
	Kind     string    `json:"kind"`
	TaskID   string    `json:"task_id,omitempty"`
	AgentID  string    `json:"agent_id,omitempty"`
	Fence    string    `json:"fence,omitempty"`
	Detail   string    `json:"detail,omitempty"`
	Duration float64   `json:"duration_ms,omitempty"`
}

type fault struct {
	Kind                string    `json:"kind"`
	StartedAt           time.Time `json:"started_at"`
	ServiceReadyAt      time.Time `json:"service_ready_at"`
	FirstCompletionAt   time.Time `json:"first_completion_at"`
	RecoveryMS          float64   `json:"recovery_ms"`
	InterruptedRequests int64     `json:"interrupted_requests"`
}

type tenantReport struct {
	Name                string             `json:"name"`
	OrgID               string             `json:"org_id"`
	Workers             int                `json:"workers"`
	Participants        int                `json:"participants"`
	CompletingWorkers   int                `json:"completing_workers"`
	Created             int                `json:"created"`
	Completed           int                `json:"completed"`
	Verified            int                `json:"verified"`
	CompletionsPerSec   float64            `json:"completions_per_second"`
	CompletionLatencyMS map[string]float64 `json:"completion_latency_ms"`
	QueueDelayMS        map[string]float64 `json:"queue_delay_ms"`
	HTTPOutcomes        map[string]int64   `json:"http_outcomes"`
}

type report struct {
	Passed                   bool              `json:"passed"`
	Seed                     int64             `json:"seed"`
	SourceCommit             string            `json:"source_commit"`
	SourceTreeSHA256         string            `json:"source_tree_sha256"`
	Environment              map[string]string `json:"environment"`
	Settings                 map[string]any    `json:"settings"`
	StartedAt                time.Time         `json:"started_at"`
	FinishedAt               time.Time         `json:"finished_at"`
	ProductionSeconds        float64           `json:"production_seconds"`
	ElapsedSeconds           float64           `json:"elapsed_seconds"`
	Waves                    int               `json:"waves"`
	Requests                 int64             `json:"requests"`
	RequestRetries           int64             `json:"request_retries"`
	RetryAmplification       float64           `json:"retry_amplification"`
	ClaimAttempts            int64             `json:"claim_attempts"`
	EmptyClaims              int64             `json:"empty_claims"`
	LeaseRenewalAttempts     int64             `json:"lease_renewal_attempts"`
	LeaseRenewals            int64             `json:"lease_renewals"`
	LeaseRenewalDenials      int64             `json:"lease_renewal_denials"`
	CompletionAttempts       int64             `json:"completion_attempts"`
	CompletionReplays        int64             `json:"completion_replays"`
	DiscardedAcknowledgments int64             `json:"discarded_acknowledgments"`
	PausedWorkers            int64             `json:"paused_workers"`
	ReclaimedTasks           int               `json:"reclaimed_tasks"`
	StaleDenials             int64             `json:"stale_denials"`
	ChangedInputDenials      int64             `json:"changed_input_denials"`
	NoisyRejections          int64             `json:"noisy_rejections"`
	LostCompletions          int               `json:"lost_completions"`
	DuplicateCompletions     int               `json:"duplicate_completions"`
	StaleOverwrites          int               `json:"stale_overwrites"`
	AttributionFailures      int               `json:"attribution_failures"`
	DependencyFailures       int               `json:"dependency_failures"`
	HistoryRows              int               `json:"history_rows"`
	HTTPOutcomes             map[string]int64  `json:"http_outcomes"`
	Tenants                  []tenantReport    `json:"tenants"`
	Faults                   []fault           `json:"faults"`
	Events                   []event           `json:"events"`
	Errors                   []string          `json:"errors"`
}

type run struct {
	cfg          config
	client       *http.Client
	sem          chan struct{}
	claimSem     chan struct{}
	sequence     atomic.Uint64
	faultUntil   atomic.Int64
	mu           sync.Mutex
	report       report
	creators     []agent
	workers      []agent
	workerIDs    map[string]int
	expected     []expectedTask
	completed    map[string]bool
	participants map[string]bool
	completers   map[string]bool
	latencies    [2][]float64
	queueDelays  [2][]float64
}

type apiResponse struct {
	Status int
	Code   string
	Body   []byte
}

func (r *run) note(kind string, t *task, a agent, detail string) {
	e := event{At: time.Now().UTC(), Kind: kind, AgentID: a.ID, Detail: detail}
	if t != nil {
		e.TaskID, e.Fence = t.ID, t.Fence
	}
	r.mu.Lock()
	r.report.Events = append(r.report.Events, e)
	r.mu.Unlock()
}

func (r *run) fail(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	// Keep diagnostics bounded if a broken implementation fails every worker.
	if len(r.report.Errors) < 100 {
		r.report.Errors = append(r.report.Errors, err.Error())
	}
	r.mu.Unlock()
}

func (r *run) uuid(label string) string {
	n := r.sequence.Add(1)
	h := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%d", r.cfg.Seed, label, n)))
	h[6], h[8] = (h[6]&0x0f)|0x40, (h[8]&0x3f)|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Claim waiters queue outside the shared HTTP budget. Reserving capacity for
// lease holders prevents idle polls from starving renewals and completions.
func (r *run) admitRequest(ctx context.Context, claim bool) (func(), error) {
	if claim {
		select {
		case r.claimSem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	select {
	case r.sem <- struct{}{}:
		return func() {
			<-r.sem
			if claim {
				<-r.claimSem
			}
		}, nil
	case <-ctx.Done():
		if claim {
			<-r.claimSem
		}
		return nil, ctx.Err()
	}
}

const requestRetryWindow = 30 * time.Second

// Keep the production retry deadline longer than the default admission permit
// lease (20s), so a gateway crash can safely retain uncertain reservations until
// expiry. Timing is injectable only in internal tests of the real HTTP path.
type requestRetryPolicy struct {
	window, rateDelay, backoff time.Duration
}

// Retries always reuse the exact request body, including claim request IDs.
// 5xx/transport failures are acceptable only in a recorded service fault window.
func (r *run) call(ctx context.Context, a agent, method, path string, body any, admin, retry bool) (apiResponse, error) {
	return r.callWithRetryPolicy(ctx, a, method, path, body, admin, retry,
		requestRetryPolicy{requestRetryWindow, time.Second, 50 * time.Millisecond})
}

func (r *run) callWithRetryPolicy(ctx context.Context, a agent, method, path string, body any, admin, retry bool, policy requestRetryPolicy) (outcome apiResponse, callErr error) {
	started := time.Now()
	var firstDispatch time.Time
	var initialQueue time.Duration
	attempts, lastStatus, lastCode := 0, 0, ""
	defer func() {
		if !errors.Is(callErr, context.DeadlineExceeded) {
			return
		}
		retryElapsed := time.Duration(0)
		first := "none"
		if firstDispatch.IsZero() {
			initialQueue = time.Since(started)
		} else {
			first = firstDispatch.UTC().Format(time.RFC3339Nano)
			retryElapsed = time.Since(firstDispatch)
		}
		callErr = fmt.Errorf("request deadline on %s %s: started=%s first_dispatch=%s attempts=%d initial_queue_ms=%.3f retry_elapsed_ms=%.3f last_http_status=%d last_http_code=%s: %w",
			method, path, started.UTC().Format(time.RFC3339Nano), first, attempts,
			float64(initialQueue.Microseconds())/1000, float64(retryElapsed.Microseconds())/1000, lastStatus, lastCode, callErr)
	}()
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return apiResponse{}, err
		}
	}
	for attempt := 0; ; attempt++ {
		isClaim := method == "POST" && path == "/v1/tasks/claim"
		release, err := r.admitRequest(ctx, isClaim)
		if err != nil {
			return apiResponse{}, err
		}
		// A cancelled waiter can win a simultaneously available semaphore.
		// It must not be counted as a participating HTTP attempt.
		if err := ctx.Err(); err != nil {
			release()
			return apiResponse{}, err
		}
		if attempt == 0 {
			firstDispatch = time.Now()
			initialQueue = firstDispatch.Sub(started)
			if retry {
				// Initial client queueing has not sent any operation and remains
				// bounded by the caller. Start one retry deadline on first dispatch;
				// subsequent queue waits, HTTP attempts and backoff all consume it.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, policy.window)
				defer cancel()
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, r.cfg.Gateway+path, bytes.NewReader(encoded))
		if err != nil {
			release()
			return apiResponse{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		if admin {
			req.Header.Set("X-Admin-Key", r.cfg.AdminKey)
		} else {
			req.Header.Set("Authorization", "Bearer "+a.Token)
		}
		if isClaim && attempt == 0 {
			r.mu.Lock()
			r.report.ClaimAttempts++
			r.participants[a.ID] = true
			r.mu.Unlock()
		}
		attempts++
		resp, transportErr := r.client.Do(req)
		out := apiResponse{}
		if transportErr == nil {
			out.Status = resp.StatusCode
			out.Body, err = io.ReadAll(io.LimitReader(resp.Body, 5<<20))
			resp.Body.Close()
			if err != nil {
				transportErr = err
			}
			var envelope struct {
				Error struct{ Code string } `json:"error"`
			}
			_ = json.Unmarshal(out.Body, &envelope)
			out.Code = envelope.Error.Code
			lastStatus, lastCode = out.Status, out.Code
		}
		release()
		key := strconv.Itoa(out.Status)
		if out.Code != "" {
			key += ":" + out.Code
		}
		r.mu.Lock()
		r.report.Requests++
		r.report.HTTPOutcomes[key]++
		if !admin && a.Token != "" && a.Tenant < len(r.report.Tenants) {
			if r.report.Tenants[a.Tenant].HTTPOutcomes == nil {
				r.report.Tenants[a.Tenant].HTTPOutcomes = map[string]int64{}
			}
			r.report.Tenants[a.Tenant].HTTPOutcomes[key]++
		}
		r.mu.Unlock()
		// Restart races can leave a retried receipt or renewal waiting on a
		// transaction until lock_timeout. Retry that exact operation only in
		// the recorded fault window; ordinary lock timeouts remain failures.
		faultLockTimeout := out.Code == "55P03" && time.Now().UnixNano() <= r.faultUntil.Load()
		transient := transportErr != nil || out.Status >= 500 || databaseDisconnect(out.Code) || faultLockTimeout
		if transient && ctx.Err() == nil && time.Now().UnixNano() <= r.faultUntil.Load() {
			r.mu.Lock()
			if n := len(r.report.Faults); n > 0 {
				r.report.Faults[n-1].InterruptedRequests++
			}
			r.mu.Unlock()
		}
		if transient && ctx.Err() == nil && time.Now().UnixNano() > r.faultUntil.Load() {
			return out, fmt.Errorf("unexpected service failure on %s %s: HTTP %d code=%s transport=%v", method, path, out.Status, out.Code, transportErr)
		}
		if retry && (transient || out.Status == 429 || out.Code == "40001" || out.Code == "40P01") {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			r.mu.Lock()
			r.report.RequestRetries++
			r.mu.Unlock()
			delay := time.Duration(attempt+1) * policy.backoff
			if out.Status == 429 {
				delay = policy.rateDelay
			}
			if !sleep(ctx, delay) {
				return out, ctx.Err()
			}
			continue
		}
		return out, transportErr
	}
}

func databaseDisconnect(code string) bool {
	return strings.HasPrefix(code, "08") || code == "57P01" || code == "57P02" || code == "57P03"
}

func decodeOK[T any](out apiResponse, err error) (T, error) {
	var v T
	if err != nil {
		return v, err
	}
	if out.Status < 200 || out.Status >= 300 {
		return v, fmt.Errorf("HTTP %d (%s): %.1000s", out.Status, out.Code, out.Body)
	}
	if err := json.Unmarshal(out.Body, &v); err != nil {
		return v, fmt.Errorf("invalid successful response: %w", err)
	}
	return v, nil
}

func (r *run) setup(ctx context.Context) error {
	names := []string{"quiet", "noisy"}
	r.report.Tenants = make([]tenantReport, 2)
	for i, name := range names {
		org, err := decodeOK[struct{ ID string }](r.call(ctx, agent{}, "POST", "/v1/orgs", map[string]any{"name": "coord-" + name}, true, true))
		if err != nil {
			return err
		}
		r.report.Tenants[i] = tenantReport{Name: name, OrgID: org.ID}
		a, err := r.createAgent(ctx, i, "producer", 0, 128)
		if err != nil {
			return err
		}
		r.creators = append(r.creators, a)
		if _, err := decodeOK[map[string]any](r.call(ctx, a, "PUT", "/v1/memory/coord-input", map[string]any{"value": result{Value: 7 + i}, "shared": true}, false, true)); err != nil {
			return err
		}
	}
	// Agent provisioning is concurrent but index ordering remains reproducible.
	r.workers = make([]agent, r.cfg.Workers)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	for i := range r.workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tenant := 0
			if i%5 == 0 {
				tenant = 1
			}
			a, err := r.createAgent(ctx, tenant, fmt.Sprintf("worker-%d", i), 0, 4)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && first == nil {
				first = err
			}
			r.workers[i] = a
			r.workerIDs[a.ID] = tenant
			r.report.Tenants[tenant].Workers++
		}(i)
	}
	wg.Wait()
	return first
}

func (r *run) createAgent(ctx context.Context, tenant int, name string, rps, concurrent int) (agent, error) {
	a, err := decodeOK[agent](r.call(ctx, agent{}, "POST", "/v1/agents", map[string]any{
		"org_id": r.report.Tenants[tenant].OrgID, "name": name, "rps_limit": rps,
		"max_concurrency": concurrent, "max_rows": 1000, "max_cost": 1000000, "statement_timeout_ms": 10000,
	}, true, true))
	a.Tenant = tenant
	return a, err
}

func (r *run) enqueue(ctx context.Context, a agent, queue, key string, p payload, deps []string, inputs []input) (*task, error) {
	if deps == nil {
		deps = []string{}
	}
	out, err := decodeOK[receipt](r.call(ctx, a, "POST", "/v1/tasks", map[string]any{
		"queue": queue, "key": key, "payload": p, "dependencies": deps, "inputs": inputs,
	}, false, true))
	if err == nil && out.Task == nil {
		err = errors.New("enqueue returned no task")
	}
	return out.Task, err
}

func (r *run) claim(ctx context.Context, a agent, queue string, lease int) (receipt, error) {
	return decodeOK[receipt](r.call(ctx, a, "POST", "/v1/tasks/claim", map[string]any{
		"queue": queue, "request_id": r.uuid("claim"), "lease_ms": lease,
	}, false, true))
}

func (r *run) complete(ctx context.Context, a agent, t *task, v result) (receipt, apiResponse, error) {
	r.mu.Lock()
	r.report.CompletionAttempts++
	r.mu.Unlock()
	out, err := r.call(ctx, a, "POST", "/v1/tasks/"+t.ID+"/complete", map[string]any{"fence": t.Fence, "result": v}, false, true)
	if err != nil || out.Status != 200 {
		return receipt{}, out, err
	}
	res, err := decodeOK[receipt](out, nil)
	if res.Replayed {
		r.mu.Lock()
		r.report.CompletionReplays++
		r.mu.Unlock()
	}
	return res, out, err
}

func requireCode(out apiResponse, err error, status int, code string) error {
	if err != nil {
		return err
	}
	if out.Status != status || out.Code != code {
		return fmt.Errorf("expected HTTP %d %s, got HTTP %d %s: %.1000s", status, code, out.Status, out.Code, out.Body)
	}
	return nil
}

// Deterministic adverse transitions before sustained contention. Every probe
// has real state and checks the precise denial, never merely any error.
func (r *run) probes(ctx context.Context) error {
	a, b := r.workers[1], r.workers[2] // both quiet org, different identities
	creator := r.creators[0]
	inputs := []input{{AgentID: creator.ID, Key: "coord-input", Version: 1}}
	t, err := r.enqueue(ctx, creator, "probe-fence", "lease", payload{}, nil, inputs)
	if err != nil {
		return err
	}
	c, err := r.claim(ctx, a, "probe-fence", 300)
	if err != nil || c.Task == nil || c.Task.ID != t.ID {
		return fmt.Errorf("first lease claim: %v %+v", err, c)
	}
	old := c.Task
	if _, err := decodeOK[task](r.call(ctx, a, "POST", "/v1/tasks/"+t.ID+"/renew", map[string]any{"fence": old.Fence, "lease_ms": 500}, false, true)); err != nil {
		return err
	}
	r.note("renewed_then_paused_worker", old, a, "No heartbeat while paused past database lease expiry")
	if !sleep(ctx, 650*time.Millisecond) {
		return ctx.Err()
	}
	_, out, err := r.complete(ctx, a, old, result{7})
	if err := requireCode(out, err, 409, "lease_expired"); err != nil {
		return err
	}
	c, err = r.claim(ctx, b, "probe-fence", r.cfg.LeaseMS)
	if err != nil || c.Task == nil || c.Task.ID != t.ID || !fenceGreater(c.Task.Fence, old.Fence) {
		return fmt.Errorf("reclaim did not advance fence: %v %+v", err, c)
	}
	_, out, err = r.complete(ctx, a, old, result{-1})
	if err := requireCode(out, err, 409, "stale_claim"); err != nil {
		return err
	}
	r.mu.Lock()
	r.report.StaleDenials += 2
	r.mu.Unlock()
	r.note("stale_completion_rejected", old, a, "Reclaimed task fence="+c.Task.Fence)
	completed, out, err := r.complete(ctx, b, c.Task, result{7})
	if err != nil || out.Status != 200 || completed.Task == nil || completed.Task.State != "completed" {
		return fmt.Errorf("reclaimed completion failed: %v HTTP%d %.1000s", err, out.Status, out.Body)
	}
	replayed, out, err := r.complete(ctx, b, c.Task, result{7})
	if err != nil || out.Status != 200 || !replayed.Replayed {
		return fmt.Errorf("completion replay failed: %v HTTP%d %.1000s", err, out.Status, out.Body)
	}
	_, out, err = r.complete(ctx, b, c.Task, result{8})
	if err := requireCode(out, err, 409, "task_conflict"); err != nil {
		return err
	}
	// Shared input edits must prevent atomic completion without changing the task.
	if _, err := decodeOK[map[string]any](r.call(ctx, creator, "PUT", "/v1/memory/coord-changing", map[string]any{"value": result{1}, "shared": true}, false, true)); err != nil {
		return err
	}
	t, err = r.enqueue(ctx, creator, "probe-input", "changed", payload{}, nil, []input{{creator.ID, "coord-changing", 1}})
	if err != nil {
		return err
	}
	c, err = r.claim(ctx, a, "probe-input", r.cfg.LeaseMS)
	if err != nil || c.Task == nil {
		return fmt.Errorf("input probe claim: %v", err)
	}
	if _, err := decodeOK[map[string]any](r.call(ctx, creator, "PUT", "/v1/memory/coord-changing", map[string]any{"value": result{2}, "shared": true}, false, true)); err != nil {
		return err
	}
	_, out, err = r.complete(ctx, a, c.Task, result{1})
	if err := requireCode(out, err, 409, "inputs_changed"); err != nil {
		return err
	}
	current, err := decodeOK[task](r.call(ctx, creator, "GET", "/v1/tasks/"+t.ID, nil, false, true))
	if err != nil || current.State != "claimed" || string(current.Result) != "null" {
		return fmt.Errorf("input conflict partially completed task: %v %+v", err, current)
	}
	r.mu.Lock()
	r.report.ChangedInputDenials++
	r.mu.Unlock()
	r.note("changed_input_rejected", c.Task, a, "Declared producer key changed after claim; task still claimed with null result")
	if _, err := decodeOK[task](r.call(ctx, creator, "POST", "/v1/tasks/"+t.ID+"/cancel", map[string]any{}, false, true)); err != nil {
		return err
	}
	// Same current lease can be released, but the released fence cannot be used.
	t, err = r.enqueue(ctx, creator, "probe-release", "release", payload{}, nil, inputs)
	if err != nil {
		return err
	}
	c, err = r.claim(ctx, a, "probe-release", r.cfg.LeaseMS)
	if err != nil || c.Task == nil {
		return fmt.Errorf("release probe claim: %v", err)
	}
	old = c.Task
	if _, err := decodeOK[task](r.call(ctx, a, "POST", "/v1/tasks/"+t.ID+"/release", map[string]any{"fence": old.Fence}, false, true)); err != nil {
		return err
	}
	c, err = r.claim(ctx, b, "probe-release", r.cfg.LeaseMS)
	if err != nil || c.Task == nil || !fenceGreater(c.Task.Fence, old.Fence) {
		return fmt.Errorf("release/reclaim fence: %v", err)
	}
	out, err = r.call(ctx, a, "POST", "/v1/tasks/"+t.ID+"/renew", map[string]any{"fence": old.Fence, "lease_ms": 1000}, false, true)
	if err := requireCode(out, err, 409, "stale_claim"); err != nil {
		return err
	}
	_, out, err = r.complete(ctx, b, c.Task, result{7})
	if _, err := decodeOK[receipt](out, err); err != nil {
		return err
	}
	out, err = r.call(ctx, r.workers[0], "GET", "/v1/tasks/"+t.ID, nil, false, true)
	if err := requireCode(out, err, 404, "not_found"); err != nil {
		return err
	}
	r.note("cross_org_read_rejected", t, r.workers[0], "Foreign task ID returns not_found")
	return nil
}

func fenceGreater(a, b string) bool {
	x, e1 := strconv.ParseInt(a, 10, 64)
	y, e2 := strconv.ParseInt(b, 10, 64)
	return e1 == nil && e2 == nil && x > y
}

func (r *run) calculate(ctx context.Context, a agent, t *task) (result, error) {
	v := t.Payload.Add
	for _, in := range t.Inputs {
		memory, err := decodeOK[struct {
			AgentID string `json:"agent_id"`
			Version int64  `json:"version"`
			Value   result `json:"value"`
		}](r.call(ctx, a, "GET", "/v1/memory/"+url.PathEscape(in.Key)+"?from="+url.QueryEscape(in.AgentID), nil, false, true))
		if err != nil || memory.AgentID != in.AgentID || memory.Version != in.Version {
			return result{}, fmt.Errorf("producer-selected input mismatch: %v %+v", err, memory)
		}
		v += memory.Value.Value
	}
	for _, id := range t.Dependencies {
		dep, err := decodeOK[task](r.call(ctx, a, "GET", "/v1/tasks/"+id, nil, false, true))
		if err != nil || dep.State != "completed" {
			if err == nil {
				r.mu.Lock()
				r.report.DependencyFailures++
				r.mu.Unlock()
			}
			return result{}, fmt.Errorf("claimed task has unfinished dependency: %v %+v", err, dep)
		}
		var previous result
		if err := json.Unmarshal(dep.Result, &previous); err != nil {
			return result{}, err
		}
		v += previous.Value
	}
	return result{v}, nil
}

func (r *run) observedCompletion(a agent, claimed *task, completed *task) {
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.completed[completed.ID] {
		return
	}
	r.completed[completed.ID] = true
	r.completers[a.ID] = true
	r.latencies[a.Tenant] = append(r.latencies[a.Tenant], float64(now.Sub(completed.CreatedAt).Microseconds())/1000)
	r.queueDelays[a.Tenant] = append(r.queueDelays[a.Tenant], float64(claimed.UpdatedAt.Sub(claimed.CreatedAt).Microseconds())/1000)
	for i := range r.report.Faults {
		f := &r.report.Faults[i]
		if !f.ServiceReadyAt.IsZero() && f.FirstCompletionAt.IsZero() && now.After(f.ServiceReadyAt) {
			f.FirstCompletionAt = now
			f.RecoveryMS = float64(now.Sub(f.StartedAt).Microseconds()) / 1000
		}
	}
}

func (r *run) worker(ctx context.Context, a agent, index int) {
	random := rand.New(rand.NewSource(r.cfg.Seed ^ int64(index)<<32))
	emptyPolls := 0
	for ctx.Err() == nil {
		c, err := r.claim(ctx, a, "work", r.cfg.LeaseMS)
		if err != nil {
			if ctx.Err() == nil {
				r.fail(fmt.Errorf("worker %d claim: %w", index, err))
			}
			if !sleep(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		if c.Task == nil {
			emptyPolls++
			r.mu.Lock()
			r.report.EmptyClaims++
			r.mu.Unlock()
			if !sleep(ctx, emptyPollDelay(emptyPolls, random)) {
				return
			}
			continue
		}
		emptyPolls = 0
		r.processTask(ctx, a, c.Task)
	}
}

// Empty claims are durable idempotency receipts, not free reads. Back off each
// idle worker independently so poll traffic leaves capacity for useful work.
func emptyPollDelay(streak int, random *rand.Rand) time.Duration {
	ceiling := min(2000, 200<<min(max(streak-1, 0), 4))
	return time.Duration(100+random.Intn(ceiling-99)) * time.Millisecond
}

// Healthy workers renew during dependency reads and computation. The returned
// stop joins the heartbeat before completion, preventing renew/complete races.
// Deliberately paused workers never start a heartbeat.
func (r *run) maintainLease(parent context.Context, a agent, t *task) (context.Context, func() bool) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	stopping := make(chan struct{})
	var stopOnce sync.Once
	var lost atomic.Bool
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			select {
			case <-stopping:
				return
			default:
			}
			r.mu.Lock()
			r.report.LeaseRenewalAttempts++
			r.mu.Unlock()
			out, err := r.call(ctx, a, "POST", "/v1/tasks/"+t.ID+"/renew", map[string]any{"fence": t.Fence, "lease_ms": r.cfg.LeaseMS}, false, true)
			if ctx.Err() != nil {
				return
			}
			if err == nil && out.Status == 409 && (out.Code == "stale_claim" || out.Code == "lease_expired") {
				r.mu.Lock()
				r.report.LeaseRenewalDenials++
				r.mu.Unlock()
				r.note("lease_renewal_rejected", t, a, out.Code)
			} else {
				renewed, decodeErr := decodeOK[task](out, err)
				if decodeErr == nil && renewed.Fence == t.Fence && renewed.Owner == a.ID && renewed.State == "claimed" {
					r.mu.Lock()
					r.report.LeaseRenewals++
					r.mu.Unlock()
					timer := time.NewTimer(time.Duration(r.cfg.LeaseMS) * time.Millisecond / 3)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-stopping:
						timer.Stop()
						return
					case <-timer.C:
					}
					continue
				}
				r.fail(fmt.Errorf("lease renewal failed for %s: %v HTTP%d %.1000s", t.ID, decodeErr, out.Status, out.Body))
			}
			lost.Store(true)
			cancel()
			return
		}
	}()
	return ctx, func() bool {
		// Finish any renewal already sent, then complete under its fresh lease.
		// Parent cancellation still interrupts an unavailable service promptly.
		stopOnce.Do(func() { close(stopping) })
		<-done
		cancel()
		return lost.Load()
	}
}

func (r *run) processTask(ctx context.Context, a agent, t *task) {
	paused := t.Payload.Ordinal%29 == 0 && t.Attempts == 1
	taskCtx := ctx
	stopLease := func() bool { return false }
	if !paused {
		taskCtx, stopLease = r.maintainLease(ctx, a, t)
		defer stopLease()
	}
	v, err := r.calculate(taskCtx, a, t)
	if err != nil {
		if taskCtx.Err() == nil {
			r.fail(err)
		}
		return
	}
	// These worker goroutines stop all work/heartbeats past the lease and
	// resume with the old fence. Other workers must reclaim their tasks.
	if paused {
		r.mu.Lock()
		r.report.PausedWorkers++
		r.mu.Unlock()
		r.note("worker_paused", t, a, "No requests or heartbeat from this worker during pause")
		if !sleep(ctx, time.Duration(r.cfg.LeaseMS+250)*time.Millisecond) {
			return
		}
	} else if !sleep(taskCtx, time.Duration(20+t.Payload.Ordinal%60)*time.Millisecond) {
		return
	}
	if stopLease() {
		return
	}
	// Lose selected committed acknowledgments at the client boundary. Do
	// not interpret the first result; retry the identical completion.
	if t.Payload.Ordinal%17 == 0 && t.Attempts == 1 {
		_, discardedErr := r.call(ctx, a, "POST", "/v1/tasks/"+t.ID+"/complete", map[string]any{"fence": t.Fence, "result": v}, false, true)
		if discardedErr != nil && ctx.Err() == nil {
			r.fail(discardedErr)
		}
		r.mu.Lock()
		r.report.DiscardedAcknowledgments++
		r.report.CompletionAttempts++
		r.mu.Unlock()
		r.note("completion_acknowledgment_discarded", t, a, "Client deliberately discarded the response before deciding the execution outcome")
	}
	completed, out, err := r.complete(ctx, a, t, v)
	if err != nil {
		if ctx.Err() == nil {
			r.fail(err)
		}
		return
	}
	if out.Status == 409 && (out.Code == "stale_claim" || out.Code == "lease_expired") {
		r.mu.Lock()
		r.report.StaleDenials++
		r.mu.Unlock()
		r.note("late_completion_rejected", t, a, out.Code)
		return
	}
	if paused && out.Status == 200 {
		r.mu.Lock()
		r.report.StaleOverwrites++
		r.mu.Unlock()
		r.fail(fmt.Errorf("paused worker completed task %s after its lease expired", t.ID))
		return
	}
	if out.Status != 200 || completed.Task == nil || completed.Task.State != "completed" {
		r.fail(fmt.Errorf("worker completion HTTP %d: %.1000s", out.Status, out.Body))
		return
	}
	if t.Payload.Ordinal%11 == 0 {
		replayed, out, err := r.complete(ctx, a, t, v)
		if err != nil || out.Status != 200 || !replayed.Replayed {
			r.fail(fmt.Errorf("duplicate completion did not replay: %v HTTP%d %.1000s", err, out.Status, out.Body))
		}
	}
	// The drain may cancel workers immediately after the last observed
	// completion. Finish its replay assertion before marking it observed.
	r.observedCompletion(a, t, completed.Task)
}

func (r *run) produceWave(ctx context.Context, wave int) error {
	chains := r.cfg.Workers / 100
	if chains < 2 {
		chains = 2
	}
	var wg sync.WaitGroup
	errs := make(chan error, chains*2)
	for tenant := 0; tenant < 2; tenant++ {
		for chain := 0; chain < chains; chain++ {
			wg.Add(1)
			go func(tenant, chain int) {
				defer wg.Done()
				creator := r.creators[tenant]
				inputs := []input{{creator.ID, "coord-input", 1}}
				var nodes [4]*task
				var values [4]int
				for step := 0; step < 4; step++ {
					ordinal := wave*chains*8 + tenant*chains*4 + chain*4 + step + 1
					p := payload{Add: int(r.cfg.Seed%97) + ordinal%13, Ordinal: ordinal, Wave: wave}
					deps := []string{}
					value := 7 + tenant + p.Add
					if step == 2 {
						deps = []string{nodes[0].ID, nodes[1].ID}
						value += values[0] + values[1]
					} else if step == 3 {
						deps = []string{nodes[2].ID}
						value += values[2]
					}
					t, err := r.enqueue(ctx, creator, "work", fmt.Sprintf("wave-%d-chain-%d-step-%d", wave, chain, step), p, deps, inputs)
					if err != nil {
						errs <- err
						return
					}
					nodes[step], values[step] = t, value
					r.mu.Lock()
					r.expected = append(r.expected, expectedTask{Task: *t, Value: value, Tenant: tenant})
					r.report.Tenants[tenant].Created++
					r.mu.Unlock()
				}
			}(tenant, chain)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func (r *run) serviceFault(ctx context.Context, kind string) {
	started := time.Now().UTC()
	r.faultUntil.Store(time.Now().Add(20 * time.Second).UnixNano())
	r.mu.Lock()
	i := len(r.report.Faults)
	r.report.Faults = append(r.report.Faults, fault{Kind: kind, StartedAt: started})
	r.mu.Unlock()
	r.note("service_fault_started", nil, agent{}, kind)
	cmd := exec.CommandContext(ctx, r.cfg.FaultControl, kind)
	output, err := cmd.CombinedOutput()
	if err != nil {
		r.fail(fmt.Errorf("%s fault command: %w: %.1000s", kind, err, output))
		return
	}
	ready := time.Now().UTC()
	r.mu.Lock()
	r.report.Faults[i].ServiceReadyAt = ready
	r.mu.Unlock()
	r.note("service_restarted", nil, agent{}, kind)
}

func (r *run) noisy(ctx context.Context, a agent) {
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				out, err := r.call(ctx, a, "POST", "/v1/query", map[string]any{"sql": "SELECT count(*) FROM generate_series(1,20000)"}, false, false)
				if out.Status == 429 {
					r.mu.Lock()
					r.report.NoisyRejections++
					r.mu.Unlock()
				} else if ctx.Err() == nil && err != nil && time.Now().UnixNano() > r.faultUntil.Load() {
					r.fail(err)
				} else if databaseDisconnect(out.Code) && time.Now().UnixNano() <= r.faultUntil.Load() {
					// PostgreSQL can reject a query while its owned instance is
					// restarting. Only these disconnect codes are expected here.
				} else if out.Status >= 400 && out.Status < 500 && out.Status != 429 {
					r.fail(fmt.Errorf("unexpected noisy rejection: HTTP%d %s", out.Status, out.Code))
				}
				if !sleep(ctx, 50*time.Millisecond) {
					return
				}
			}
		}()
	}
	wg.Wait()
}

func quantiles(values []float64) map[string]float64 {
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	out := map[string]float64{"p50": 0, "p95": 0, "p99": 0}
	if len(copyValues) == 0 {
		return out
	}
	for name, percent := range map[string]int{"p50": 50, "p95": 95, "p99": 99} {
		index := (len(copyValues)*percent+99)/100 - 1
		out[name] = copyValues[index]
	}
	return out
}

// validateTask is independently unit-tested with deliberately corrupt results.
func validateTask(want expectedTask, got task, workers map[string]int) error {
	if got.ID != want.Task.ID || got.State != "completed" {
		return fmt.Errorf("lost completion: task %s state=%s", want.Task.ID, got.State)
	}
	var actual result
	if err := json.Unmarshal(got.Result, &actual); err != nil || actual.Value != want.Value {
		return fmt.Errorf("incorrect result: task %s want=%d got=%s", got.ID, want.Value, got.Result)
	}
	if tenant, ok := workers[got.Owner]; !ok || tenant != want.Tenant || got.OrgID != want.Task.OrgID {
		return fmt.Errorf("invalid attribution: task %s owner=%s org=%s", got.ID, got.Owner, got.OrgID)
	}
	if n, err := strconv.ParseInt(got.Fence, 10, 64); err != nil || n < 1 || got.Attempts < 1 {
		return fmt.Errorf("invalid claim metadata: task %s", got.ID)
	}
	wantDeps, gotDeps := append([]string(nil), want.Task.Dependencies...), append([]string(nil), got.Dependencies...)
	sort.Strings(wantDeps)
	sort.Strings(gotDeps)
	if !reflect.DeepEqual(wantDeps, gotDeps) || !reflect.DeepEqual(want.Task.Inputs, got.Inputs) || got.Payload != want.Task.Payload {
		return fmt.Errorf("task dependencies, inputs or payload changed: %s", got.ID)
	}
	return nil
}

// History fields are intentionally decoded independently from the task result;
// a correct final row cannot conceal duplicate committed completion transitions.
type historyEvent struct {
	Revision   int64      `json:"revision"`
	Event      string     `json:"event"`
	AgentID    string     `json:"actor_agent_id"`
	Owner      string     `json:"owner_agent_id"`
	Fence      string     `json:"fence"`
	At         time.Time  `json:"at"`
	LeaseUntil *time.Time `json:"lease_until"`
}

func validateHistory(t task, events []historyEvent) (int, error) {
	completions := 0
	seenRevision := map[int64]bool{}
	sorted := append([]historyEvent(nil), events...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Revision < sorted[j].Revision })
	var lastFence int64
	var owner string
	var leaseUntil *time.Time
	for _, e := range sorted {
		if e.Revision < 1 || seenRevision[e.Revision] {
			return completions, fmt.Errorf("invalid or duplicate task history revision for %s", t.ID)
		}
		seenRevision[e.Revision] = true
		fence, err := strconv.ParseInt(e.Fence, 10, 64)
		if err != nil || fence < lastFence {
			return completions, fmt.Errorf("nonmonotonic task fence for %s", t.ID)
		}
		if e.Event == "claimed" {
			if fence <= lastFence || e.Owner == "" || e.AgentID != e.Owner {
				return completions, fmt.Errorf("invalid claim attribution/fence for %s", t.ID)
			}
			if owner != "" && leaseUntil != nil && e.At.Before(*leaseUntil) {
				return completions, fmt.Errorf("overlapping live task owners for %s", t.ID)
			}
			owner, leaseUntil = e.Owner, e.LeaseUntil
		} else if e.Event == "renewed" {
			if owner == "" || e.AgentID != owner || fence != lastFence {
				return completions, fmt.Errorf("invalid task renewal for %s", t.ID)
			}
			leaseUntil = e.LeaseUntil
		} else if e.Event == "released" || e.Event == "cancelled" || e.Event == "recovered" {
			owner, leaseUntil = "", nil
		}
		lastFence = fence
		if e.Event == "completed" {
			completions++
			if e.AgentID != t.Owner || e.Fence != t.Fence {
				return completions, fmt.Errorf("completion history attribution/fence mismatch for %s", t.ID)
			}
		}
	}
	if completions != 1 {
		return completions, fmt.Errorf("task %s has %d committed completion transitions", t.ID, completions)
	}
	return completions, nil
}

func (r *run) verify(ctx context.Context) {
	var wg sync.WaitGroup
	jobs := make(chan expectedTask)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for expected := range jobs {
				a := r.creators[expected.Tenant]
				got, err := decodeOK[task](r.call(ctx, a, "GET", "/v1/tasks/"+expected.Task.ID, nil, false, true))
				if err != nil {
					r.fail(err)
					continue
				}
				err = validateTask(expected, got, r.workerIDs)
				r.mu.Lock()
				if got.State != "completed" {
					r.report.LostCompletions++
				}
				if got.Attempts > 1 {
					r.report.ReclaimedTasks++
				}
				if err != nil {
					if strings.Contains(err.Error(), "attribution") {
						r.report.AttributionFailures++
					} else if strings.Contains(err.Error(), "incorrect result") {
						r.report.StaleOverwrites++
					}
				}
				r.mu.Unlock()
				if err != nil {
					r.fail(err)
					continue
				}
				history, err := decodeOK[struct {
					Events []historyEvent `json:"events"`
				}](r.call(ctx, a, "GET", "/v1/tasks/"+got.ID+"/history?limit=1000", nil, false, true))
				if err != nil {
					r.fail(err)
					continue
				}
				count, err := validateHistory(got, history.Events)
				r.mu.Lock()
				r.report.HistoryRows += len(history.Events)
				if count > 1 {
					r.report.DuplicateCompletions += count - 1
				}
				if err == nil {
					r.report.Tenants[expected.Tenant].Verified++
					r.report.Tenants[expected.Tenant].Completed++
				}
				r.mu.Unlock()
				r.fail(err)
			}
		}()
	}
	for _, e := range r.expected {
		jobs <- e
	}
	close(jobs)
	wg.Wait()
}

func (r *run) execute(ctx context.Context) error {
	if err := r.setup(ctx); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	if err := r.probes(ctx); err != nil {
		return fmt.Errorf("failure probes: %w", err)
	}
	bully, err := r.createAgent(ctx, 1, "noisy-query-flood", 5, 2)
	if err != nil {
		return err
	}
	r.report.StartedAt = time.Now().UTC()
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	var workers sync.WaitGroup
	for i, a := range r.workers {
		workers.Add(1)
		go func(i int, a agent) { defer workers.Done(); r.worker(workCtx, a, i) }(i, a)
	}
	workers.Add(1)
	go func() { defer workers.Done(); r.noisy(workCtx, bully) }()
	var faults sync.WaitGroup
	faults.Add(1)
	go func() {
		defer faults.Done()
		if sleep(workCtx, r.cfg.Duration/3) {
			r.serviceFault(workCtx, "gateway")
		}
		until := time.Until(r.report.StartedAt.Add(r.cfg.Duration * 2 / 3))
		if until < 0 {
			until = 0
		}
		if sleep(workCtx, until) {
			r.serviceFault(workCtx, "database")
		}
	}()
	for wave := 0; time.Since(r.report.StartedAt) < r.cfg.Duration; wave++ {
		if err := r.produceWave(ctx, wave); err != nil {
			r.fail(fmt.Errorf("wave %d: %w", wave, err))
			break
		}
		r.report.Waves++
		if !sleep(ctx, 250*time.Millisecond) {
			break
		}
	}
	r.report.ProductionSeconds = time.Since(r.report.StartedAt).Seconds()
	faults.Wait()
	deadline := time.Now().Add(max(45*time.Second, time.Duration(r.cfg.LeaseMS)*time.Millisecond+10*time.Second))
	for time.Now().Before(deadline) && ctx.Err() == nil {
		r.mu.Lock()
		done := len(r.completed) >= len(r.expected)
		r.mu.Unlock()
		if done {
			break
		}
		sleep(ctx, 100*time.Millisecond)
	}
	stop()
	workers.Wait()
	r.report.FinishedAt = time.Now().UTC()
	r.report.ElapsedSeconds = r.report.FinishedAt.Sub(r.report.StartedAt).Seconds()
	// Final verification runs after both owned services recovered and all
	// workers stopped, so it measures persisted state, not in-memory success.
	r.verify(ctx)
	for id := range r.participants {
		if tenant, ok := r.workerIDs[id]; ok {
			r.report.Tenants[tenant].Participants++
		}
	}
	for id := range r.completers {
		if tenant, ok := r.workerIDs[id]; ok {
			r.report.Tenants[tenant].CompletingWorkers++
		}
	}
	for i := range r.report.Tenants {
		t := &r.report.Tenants[i]
		t.CompletionLatencyMS = quantiles(r.latencies[i])
		t.QueueDelayMS = quantiles(r.queueDelays[i])
		t.CompletionsPerSec = float64(t.Completed) / r.report.ElapsedSeconds
		if t.Created != t.Verified || t.Participants != t.Workers {
			r.fail(fmt.Errorf("tenant %s: created=%d verified=%d participants=%d workers=%d", t.Name, t.Created, t.Verified, t.Participants, t.Workers))
		}
	}
	if r.report.Waves < 2 || r.report.ReclaimedTasks == 0 || r.report.NoisyRejections == 0 || r.report.CompletionReplays == 0 || r.report.DiscardedAcknowledgments == 0 {
		r.fail(errors.New("workload did not exercise waves, reclaim, noisy admission, completion replay and lost acknowledgments"))
	}
	if len(r.report.Faults) != 2 {
		r.fail(errors.New("both gateway and database restart faults must execute"))
	}
	for _, f := range r.report.Faults {
		if f.ServiceReadyAt.IsZero() || f.FirstCompletionAt.IsZero() || f.InterruptedRequests == 0 {
			r.fail(fmt.Errorf("%s fault lacks observed post-recovery completion", f.Kind))
		}
	}
	r.report.RetryAmplification = float64(r.report.Requests) / float64(max(1, r.report.Requests-r.report.RequestRetries))
	return nil
}

func main() {
	var cfg config
	flag.StringVar(&cfg.Gateway, "gateway", "http://127.0.0.1:8080", "gateway URL")
	flag.StringVar(&cfg.AdminKey, "admin-key", "dev-admin-key", "development admin key")
	flag.StringVar(&cfg.Output, "output", "coordination-report.json", "JSON evidence report")
	flag.StringVar(&cfg.FaultControl, "fault-control", "", "owned service controller executable; called with gateway or database")
	flag.IntVar(&cfg.Workers, "workers", 100, "worker identities (100–1000)")
	flag.IntVar(&cfg.InFlight, "in-flight", 128, "maximum concurrent HTTP requests")
	flag.IntVar(&cfg.LeaseMS, "lease-ms", 3000, "normal worker lease in milliseconds")
	flag.Int64Var(&cfg.Seed, "seed", 20261022, "reproducible workload seed")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "duration of continuous graph production, excluding setup/drain")
	flag.Parse()
	if cfg.Workers < 100 || cfg.Workers > 1000 || cfg.InFlight < 1 || cfg.InFlight > 1000 || cfg.Duration < 6*time.Second || cfg.LeaseMS < 500 || cfg.LeaseMS > 300000 || cfg.FaultControl == "" {
		fmt.Fprintln(os.Stderr, "require 100–1000 workers, 1–1000 in-flight, duration >=6s, lease 500–300000ms, and an owned fault controller")
		os.Exit(2)
	}
	claimLimit := min(16, max(1, cfg.InFlight/2))
	r := &run{cfg: cfg, sem: make(chan struct{}, cfg.InFlight), claimSem: make(chan struct{}, claimLimit), workerIDs: map[string]int{}, completed: map[string]bool{}, participants: map[string]bool{}, completers: map[string]bool{},
		client: &http.Client{Transport: &http.Transport{MaxIdleConns: cfg.InFlight * 2, MaxIdleConnsPerHost: cfg.InFlight, MaxConnsPerHost: cfg.InFlight}, Timeout: 10 * time.Second},
		report: report{Seed: cfg.Seed, SourceCommit: os.Getenv("COORD_SOURCE_COMMIT"), SourceTreeSHA256: os.Getenv("COORD_SOURCE_TREE_SHA256"),
			Environment:  map[string]string{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpu_count": strconv.Itoa(runtime.NumCPU()), "gomaxprocs": strconv.Itoa(runtime.GOMAXPROCS(0)), "postgres": os.Getenv("COORD_DB_VERSION"), "kernel": os.Getenv("KERNEL"), "audit_mode": "best-effort", "database_mode": os.Getenv("COORD_DB_MODE")},
			Settings:     map[string]any{"workers": cfg.Workers, "in_flight": cfg.InFlight, "claim_in_flight": claimLimit, "lease_ms": cfg.LeaseMS, "duration": cfg.Duration.String(), "runtime_pool": 32, "empty_poll_backoff_cap_ms": 2000, "request_retry_window_ms": requestRetryWindow.Milliseconds(), "request_retry_window_starts": "first_dispatch", "lease_renew_interval_ms": cfg.LeaseMS / 3, "quiet_worker_share": 0.8, "noisy_query_rps_limit": 5, "noisy_query_concurrency_limit": 2, "external_effects": false},
			HTTPOutcomes: map[string]int64{}, Events: []event{}, Errors: []string{}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration+time.Duration(cfg.LeaseMS)*time.Millisecond+3*time.Minute)
	defer cancel()
	r.fail(r.execute(ctx))
	r.report.Passed = len(r.report.Errors) == 0
	data, err := json.MarshalIndent(r.report, "", "  ")
	if err == nil {
		err = os.WriteFile(cfg.Output, append(data, '\n'), 0600)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("coordination: passed=%v workers=%d waves=%d tasks=%d recovered=%d duplicate_completions=%d lost=%d errors=%d report=%s\n",
		r.report.Passed, cfg.Workers, r.report.Waves, len(r.expected), r.report.ReclaimedTasks, r.report.DuplicateCompletions, r.report.LostCompletions, len(r.report.Errors), cfg.Output)
	for _, e := range r.report.Errors {
		fmt.Fprintln(os.Stderr, e)
	}
	if !r.report.Passed {
		os.Exit(1)
	}
}
