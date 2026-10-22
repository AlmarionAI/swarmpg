// The admission workload only runs through its owned-service wrapper. It is a
// bounded correctness and isolation experiment, not a throughput benchmark.
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
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	Gateways                                  []string
	AdminKey, Output, Control, PSQL, Database string
	Workers                                   int
	Duration                                  time.Duration
	Seed                                      int64
	P99MS                                     float64
}
type agent struct {
	ID, Token string
	Org       int
}
type policy struct {
	GeneralRPS         int `json:"general_rps"`
	GeneralBurst       int `json:"general_burst"`
	GeneralConcurrency int `json:"general_concurrency"`
	ControlRPS         int `json:"control_rps"`
	ControlBurst       int `json:"control_burst"`
	ControlConcurrency int `json:"control_concurrency"`
}
type counts struct {
	General int64 `json:"general"`
	Control int64 `json:"control"`
}
type status struct {
	Granted counts `json:"granted"`
	Policy  policy `json:"policy"`
	Active  struct {
		General int `json:"general"`
		Control int `json:"control"`
	} `json:"active"`
}
type latency struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}
type classReport struct {
	Requests                  int            `json:"requests"`
	Success                   int            `json:"success"`
	Rejected                  map[string]int `json:"rejected"`
	Latency                   latency        `json:"http_latency"`
	AdmissionWait             latency        `json:"server_admission_wait"`
	samples, admissionSamples []float64
}
type tenantReport struct {
	GrantedGeneral    int64       `json:"granted_general"`
	GrantedControl    int64       `json:"granted_control"`
	Name              string      `json:"name"`
	ID                string      `json:"org_id"`
	Workers           int         `json:"workers"`
	Participants      int         `json:"participating_workers"`
	CompletingWorkers int         `json:"completing_workers"`
	Policy            policy      `json:"policy"`
	General           classReport `json:"general"`
	Control           classReport `json:"control"`
	Created           int         `json:"created"`
	Completed         int         `json:"completed"`
	Verified          int         `json:"verified"`
	Renewed           int         `json:"renewed"`
	Expired           int         `json:"expired_leases"`
	CompletionLatency latency     `json:"completion_latency"`
	Progress          []int       `json:"completions_per_10s"`
	MaxGeneral        int         `json:"max_sampled_general_active"`
	MaxControl        int         `json:"max_sampled_control_active"`
	completions       []float64
}
type probe struct {
	Name     string         `json:"name"`
	Passed   bool           `json:"passed"`
	Evidence map[string]any `json:"evidence"`
	Error    string         `json:"error,omitempty"`
}
type sample struct {
	At      time.Time `json:"at"`
	OrgID   string    `json:"org_id"`
	General int       `json:"general"`
	Control int       `json:"control"`
}
type report struct {
	Passed           bool              `json:"passed"`
	SourceCommit     string            `json:"source_commit"`
	SourceTree       string            `json:"source_tree_sha256"`
	Environment      map[string]string `json:"environment"`
	Settings         map[string]any    `json:"settings"`
	Started          time.Time         `json:"started_at"`
	Finished         time.Time         `json:"finished_at"`
	SustainedSeconds float64           `json:"sustained_seconds"`
	Tenants          []tenantReport    `json:"tenants"`
	Probes           []probe           `json:"probes"`
	Samples          []sample          `json:"shared_permit_samples"`
	GatewaysBefore   []json.RawMessage `json:"gateways_before"`
	GatewaysAfter    []json.RawMessage `json:"gateways_after"`
	Faults           []map[string]any  `json:"faults"`
	DBBefore         json.RawMessage   `json:"database_before"`
	DBAfter          json.RawMessage   `json:"database_after"`
	Lost             int               `json:"lost_results"`
	Duplicates       int               `json:"duplicate_results"`
	Corrupt          int               `json:"corrupt_results"`
	StaleDenials     int               `json:"stale_denials"`
	RetryCount       int               `json:"request_retries"`
	Errors           []string          `json:"errors"`
}
type response struct {
	Status int
	Code   string
	Body   []byte
	Err    error
}
type task struct {
	ID      string `json:"id"`
	OrgID   string `json:"org_id"`
	State   string `json:"state"`
	Key     string `json:"task_key"`
	Owner   string `json:"owner_agent_id"`
	Fence   string `json:"fence"`
	Payload struct {
		A int `json:"a"`
		B int `json:"b"`
	} `json:"payload"`
	Result struct {
		Value int `json:"value"`
	} `json:"result"`
	Created time.Time `json:"created_at"`
}
type receipt struct {
	Task     *task `json:"task"`
	Replayed bool  `json:"replayed"`
}
type expected struct {
	ID, OrgID, Key string
	Value          int
}
type run struct {
	cfg          config
	client       *http.Client
	seq          atomic.Uint64
	mu           sync.Mutex
	report       report
	workers      []agent
	producers    []agent
	global       chan struct{}
	general      [3]chan struct{}
	claims       [3]chan struct{}
	control      [3]chan struct{}
	participants map[string]bool
	completers   map[string]bool
	expected     map[string]expected
	faultUntil   atomic.Int64
	started      time.Time
}

func newRun(c config) *run {
	r := &run{cfg: c, client: &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 128, MaxConnsPerHost: 192, IdleConnTimeout: 30 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, global: make(chan struct{}, 384), participants: map[string]bool{}, completers: map[string]bool{}, expected: map[string]expected{}}
	for i := 0; i < 3; i++ {
		n := 48
		if i == 0 {
			n = 96
		}
		r.general[i] = make(chan struct{}, n)
		r.claims[i] = make(chan struct{}, 8)
		r.control[i] = make(chan struct{}, 32)
	}
	r.report = report{SourceCommit: os.Getenv("ADMISSION_SOURCE_COMMIT"), SourceTree: os.Getenv("ADMISSION_SOURCE_TREE_SHA256"), Environment: map[string]string{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpu_count": strconv.Itoa(runtime.NumCPU()), "gomaxprocs": strconv.Itoa(runtime.GOMAXPROCS(0)), "database": os.Getenv("ADMISSION_DB_VERSION"), "db_mode": os.Getenv("ADMISSION_DB_MODE")}, Settings: map[string]any{"seed": c.Seed, "workers": c.Workers, "gateways": c.Gateways, "duration_seconds": c.Duration.Seconds(), "global_http_limit": 384, "noisy_general_http_limit": 96, "quiet_general_http_limit_each": 48, "claim_http_limit_each": 8, "control_http_limit_each": 32, "runtime_pool_per_gateway": 32, "auth_pool_per_gateway": 4, "admission_pool_per_gateway": 4, "task_lease_ms": 3000, "task_heartbeat_ms": 750, "task_work_ms": 1200, "quiet_completion_p99_limit_ms": c.P99MS, "progress_window_seconds": 10, "execution_ms": os.Getenv("ADMISSION_EXECUTION_MS"), "audit_mode": "best-effort", "fairness_scope": "shared organization ceilings and local gateway scheduling; bounded client backpressure"}}
	return r
}
func (r *run) uuid() string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", r.cfg.Seed, r.seq.Add(1))))
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
func take(ctx context.Context, ch chan struct{}) bool {
	select {
	case ch <- struct{}{}:
		if ctx.Err() != nil {
			<-ch
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}
func (r *run) fail(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.report.Errors) < 100 {
		r.report.Errors = append(r.report.Errors, fmt.Sprintf(format, args...))
	}
}

// Request bodies and caller-selected request IDs are encoded once before retries.
func (r *run) call(ctx context.Context, a agent, replica int, method, path string, body any, class string, retry bool) response {
	data, err := json.Marshal(body)
	if err != nil {
		return response{Err: err}
	}
	for attempt := 0; ; attempt++ {
		if class != "" && a.Token != "" {
			gate := r.general[a.Org]
			if class == "control" {
				gate = r.control[a.Org]
			}
			if !take(ctx, gate) {
				return response{Err: ctx.Err()}
			}
			deferfunc := func() { <-gate }
			out := r.once(ctx, a, replica, method, path, data, class)
			deferfunc()
			if !r.retry(ctx, out, retry, attempt) {
				return out
			}
		} else {
			out := r.once(ctx, a, replica, method, path, data, class)
			if !r.retry(ctx, out, retry, attempt) {
				return out
			}
		}
		r.mu.Lock()
		r.report.RetryCount++
		r.mu.Unlock()
		if !sleep(ctx, time.Duration(35+attempt*30)*time.Millisecond) {
			return response{Err: ctx.Err()}
		}
	}
}
func (r *run) retry(ctx context.Context, out response, enabled bool, attempt int) bool {
	if !enabled || attempt >= 30 || ctx.Err() != nil {
		return false
	}
	if out.Status == 429 {
		return true
	}
	if time.Now().UnixNano() < r.faultUntil.Load() && (out.Err != nil || out.Status == 503 || out.Status == 502) {
		return true
	}
	return out.Code == "40001" || out.Code == "40P01"
}
func (r *run) once(ctx context.Context, a agent, replica int, method, path string, data []byte, class string) response {
	if !take(ctx, r.global) {
		return response{Err: ctx.Err()}
	}
	defer func() { <-r.global }()
	req, err := http.NewRequestWithContext(ctx, method, r.cfg.Gateways[replica%len(r.cfg.Gateways)]+path, bytes.NewReader(data))
	if err != nil {
		return response{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	if a.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.Token)
	} else {
		req.Header.Set("X-Admin-Key", r.cfg.AdminKey)
	}
	if path == "/mcp" {
		req.Header.Set("MCP-Protocol-Version", "2025-11-25")
		req.Header.Set("Accept", "application/json, text/event-stream")
	}
	if class != "" && a.ID != "" {
		r.mu.Lock()
		r.participants[a.ID] = true
		r.mu.Unlock()
	}
	start := time.Now()
	res, err := r.client.Do(req)
	out := response{Err: err}
	admissionWait := -1.0
	if err == nil {
		defer res.Body.Close()
		out.Status = res.StatusCode
		out.Body, out.Err = io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
		if len(out.Body) > 2<<20 {
			out.Err = errors.New("oversized response")
		}
		var wire struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(out.Body, &wire)
		out.Code = wire.Error.Code
		if v := res.Header.Get("X-SwarmPG-Admission-Wait-Ms"); v != "" {
			admissionWait, _ = strconv.ParseFloat(v, 64)
		}
	}
	if class != "" && len(r.report.Tenants) > a.Org {
		r.mu.Lock()
		c := &r.report.Tenants[a.Org].General
		if class == "control" {
			c = &r.report.Tenants[a.Org].Control
		}
		c.Requests++
		c.samples = append(c.samples, float64(time.Since(start).Microseconds())/1000)
		if admissionWait >= 0 {
			c.admissionSamples = append(c.admissionSamples, admissionWait)
		}
		if out.Status >= 200 && out.Status < 300 {
			c.Success++
		} else {
			key := fmt.Sprintf("%d:%s", out.Status, out.Code)
			if out.Err != nil {
				key = "transport"
			}
			c.Rejected[key]++
		}
		r.mu.Unlock()
	}
	return out
}
func check(out response) error {
	if out.Err != nil {
		return out.Err
	}
	if out.Status < 200 || out.Status >= 300 {
		return fmt.Errorf("HTTP%d %s %.400s", out.Status, out.Code, out.Body)
	}
	return nil
}
func decode[T any](out response) (T, error) {
	var v T
	if err := check(out); err != nil {
		return v, err
	}
	err := json.Unmarshal(out.Body, &v)
	return v, err
}
func (r *run) sql(ctx context.Context, query string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.cfg.PSQL, r.cfg.Database, "-X", "-tA", "-v", "ON_ERROR_STOP=1", "-c", query)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("SQL verification: %w: %.1000s", err, out)
	}
	return bytes.TrimSpace(out), nil
}
func (r *run) owned(ctx context.Context, action string) error {
	out, err := exec.CommandContext(ctx, r.cfg.Control, action).CombinedOutput()
	if err != nil {
		return fmt.Errorf("owned service %s: %w: %.1000s", action, err, out)
	}
	return nil
}
func (r *run) setPolicy(ctx context.Context, org string, p policy) error {
	return check(r.call(ctx, agent{}, 0, "PUT", "/v1/orgs/"+org+"/admission", p, "", false))
}
func (r *run) status(ctx context.Context, org string) (status, error) {
	return decode[status](r.call(ctx, agent{}, 0, "GET", "/v1/orgs/"+org+"/admission", nil, "", false))
}
func (r *run) createAgent(ctx context.Context, org int, name string) (agent, error) {
	a, err := decode[agent](r.call(ctx, agent{}, org%3, "POST", "/v1/agents", map[string]any{"org_id": r.report.Tenants[org].ID, "name": name, "rps_limit": 0, "max_concurrency": 8, "statement_timeout_ms": 2500, "max_rows": 1000, "max_cost": 1000000}, "", false))
	a.Org = org
	return a, err
}
func (r *run) setup(ctx context.Context) error {
	for i, name := range []string{"noisy", "quiet-a", "quiet-b"} {
		o, err := decode[struct{ ID string }](r.call(ctx, agent{}, i, "POST", "/v1/orgs", map[string]string{"name": "admission-" + name}, "", false))
		if err != nil {
			return err
		}
		p := policy{40, 80, 4, 100, 200, 4}
		if i > 0 {
			p = policy{200, 400, 12, 100, 200, 6}
		}
		r.report.Tenants = append(r.report.Tenants, tenantReport{Name: name, ID: o.ID, Policy: p, General: classReport{Rejected: map[string]int{}}, Control: classReport{Rejected: map[string]int{}}, Progress: make([]int, int(r.cfg.Duration.Seconds()/10)+1)})
		a, err := r.createAgent(ctx, i, "producer")
		if err != nil {
			return err
		}
		r.producers = append(r.producers, a)
		if err := r.setPolicy(ctx, o.ID, p); err != nil {
			return err
		}
	}
	r.workers = make([]agent, r.cfg.Workers)
	var wg sync.WaitGroup
	gate := make(chan struct{}, 16)
	var first error
	var mu sync.Mutex
	for i := range r.workers {
		if !take(ctx, gate) {
			return ctx.Err()
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-gate }()
			org := 0
			if i%10 >= 7 {
				org = 1 + i%2
			}
			a, err := r.createAgent(ctx, org, fmt.Sprintf("worker-%d", i))
			mu.Lock()
			defer mu.Unlock()
			if err != nil && first == nil {
				first = err
			}
			r.workers[i] = a
			r.report.Tenants[org].Workers++
		}(i)
	}
	wg.Wait()
	return first
}
func summarize(values []float64) latency {
	if len(values) == 0 {
		return latency{}
	}
	sort.Float64s(values)
	at := func(p float64) float64 { i := int(float64(len(values)-1) * p); return values[i] }
	return latency{len(values), at(.5), at(.95), at(.99), values[len(values)-1]}
}
func (r *run) finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.Finished = time.Now()
	for _, a := range r.workers {
		if r.participants[a.ID] {
			r.report.Tenants[a.Org].Participants++
		}
		if r.completers[a.ID] {
			r.report.Tenants[a.Org].CompletingWorkers++
		}
	}
	for i := range r.report.Tenants {
		t := &r.report.Tenants[i]
		t.General.Latency = summarize(t.General.samples)
		t.Control.Latency = summarize(t.Control.samples)
		t.General.AdmissionWait = summarize(t.General.admissionSamples)
		t.Control.AdmissionWait = summarize(t.Control.admissionSamples)
		t.CompletionLatency = summarize(t.completions)
	}
	r.report.Errors = append(r.report.Errors, validate(r.report, r.cfg)...)
	r.report.Passed = len(r.report.Errors) == 0
}
func validate(rep report, c config) []string {
	var errs []string
	add := func(s string) { errs = append(errs, s) }
	if len(rep.Tenants) != 3 {
		return []string{"expected three sustained organizations"}
	}
	if rep.SustainedSeconds < c.Duration.Seconds()-.1 {
		add("sustained duration not reached")
	}
	for _, p := range rep.Probes {
		if !p.Passed {
			add("probe failed: " + p.Name + ": " + p.Error)
		}
	}
	if len(rep.Probes) < 6 {
		add("required deterministic probes missing")
	}
	n := 0
	for i, t := range rep.Tenants {
		n += t.Participants
		if t.Participants != t.Workers {
			add(t.Name + ": not every worker dispatched a request")
		}
		if t.MaxGeneral > t.Policy.GeneralConcurrency || t.MaxControl > t.Policy.ControlConcurrency {
			add(t.Name + ": aggregate concurrency ceiling exceeded")
		}
		if i > 0 {
			if t.Created == 0 || t.Verified != t.Created || t.Completed != t.Created {
				add(t.Name + ": task results missing")
			}
			if t.Renewed == 0 || t.CompletingWorkers == 0 {
				add(t.Name + ": no useful leased work")
			}
			if t.CompletionLatency.P99 > c.P99MS {
				add(t.Name + ": completion p99 exceeds stated bound")
			}
			for w := 1; w < int(c.Duration.Seconds()/10); w++ {
				if w >= len(t.Progress) || t.Progress[w] == 0 {
					add(fmt.Sprintf("%s: no completion in sustained window %d", t.Name, w))
				}
			}
		}
	}
	if n != c.Workers {
		add("participating worker total mismatch")
	}
	noisy := 0
	for code, v := range rep.Tenants[0].General.Rejected {
		if strings.HasPrefix(code, "429:org_") {
			noisy += v
		}
	}
	if noisy == 0 {
		add("no organization quota saturation observed")
	}
	if rep.Lost != 0 || rep.Duplicates != 0 || rep.Corrupt != 0 {
		add("persisted result correctness violated")
	}
	if len(rep.Samples) == 0 {
		add("no shared permit samples")
	}
	for _, fault := range rep.Faults {
		if fault["passed"] != true {
			add("gateway crash did not recover")
		}
	}
	if len(rep.Faults) == 0 {
		add("no gateway crash evidence")
	}
	return errs
}
func main() {
	var c config
	var gateways string
	flag.StringVar(&gateways, "gateways", "", "comma-separated three owned gateways")
	flag.StringVar(&c.AdminKey, "admin-key", os.Getenv("ADMIN_KEY"), "admin key")
	flag.StringVar(&c.Output, "output", "report.json", "retained JSON report")
	flag.StringVar(&c.Control, "fault-control", "", "wrapper-owned lifecycle controller")
	flag.StringVar(&c.PSQL, "psql", "psql", "verification psql binary")
	flag.StringVar(&c.Database, "database", os.Getenv("DATABASE_URL"), "owned database URL")
	flag.IntVar(&c.Workers, "workers", 2000, "real worker identities (minimum2000)")
	flag.DurationVar(&c.Duration, "duration", 120*time.Second, "sustained production duration (minimum20s)")
	flag.Int64Var(&c.Seed, "seed", 20261022, "reproducible UUID/work seed")
	flag.Float64Var(&c.P99MS, "quiet-p99-ms", 10000, "predeclared quiet completion p99 limit")
	flag.Parse()
	c.Gateways = strings.Split(gateways, ",")
	if len(c.Gateways) != 3 || c.Workers < 2000 || c.Duration < 20*time.Second || c.Control == "" || c.Database == "" || c.AdminKey == "" || c.P99MS <= 0 {
		fmt.Fprintln(os.Stderr, "three owned gateways,2000+workers,20s+duration,database,admin-key,fault-control and positive p99 required")
		os.Exit(2)
	}
	r := newRun(c)
	ctx, cancel := context.WithTimeout(context.Background(), c.Duration+5*time.Minute)
	defer cancel()
	r.report.Started = time.Now()
	if err := r.setup(ctx); err != nil {
		r.fail("setup: %v", err)
	} else if err := r.probes(ctx); err != nil {
		r.fail("probes: %v", err)
	} else {
		r.execute(ctx)
	}
	r.finish()
	data, _ := json.MarshalIndent(r.report, "", "  ")
	if err := os.WriteFile(c.Output, append(data, '\n'), 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("admission passed=%v workers=%d duration=%.2fs errors=%v report=%s\n", r.report.Passed, c.Workers, r.report.SustainedSeconds, r.report.Errors, c.Output)
	if !r.report.Passed {
		os.Exit(1)
	}
}
