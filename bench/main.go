// Benchmark: N agents each issue R requests of the SwarmPG workload mix, with C
// agents in flight at once, against one of two targets:
//
//	gateway    the SwarmPG HTTP API, with stock Postgres or the kernel behind it
//	pgbouncer  PgBouncer in transaction mode in front of Postgres with the same
//	           schema and row-level security. Every agent holds its own
//	           connection and sets its identity with SET LOCAL, the common way
//	           to give agents SQL access.
//
// Both targets run the same statements against the same tables, through a pool
// of the same size, so the difference measured is the SwarmPG frontend and the
// guarantees it adds. `bench report` renders result files as a Markdown table.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	target   = flag.String("target", "gateway", "gateway or pgbouncer")
	label    = flag.String("label", "", "name of this configuration in the report")
	gateway  = flag.String("gateway", "http://gateway:8080", "gateway URL (target gateway)")
	adminKey = flag.String("admin-key", "dev-admin-key", "gateway admin key (target gateway)")
	bouncer  = flag.String("pgbouncer", "postgres://agent_runtime@pgbouncer:6432/swarmpg?sslmode=disable", "agent connection string (target pgbouncer)")
	pgAdmin  = flag.String("pg", "", "admin connection to Postgres, to count agent backends")
	nAgents  = flag.Int("agents", 10000, "agents")
	nReq     = flag.Int("requests", 10, "requests per agent")
	conc     = flag.Int("concurrency", 1000, "agents in flight at once")
	out      = flag.String("out", "", "write the result as JSON to this file")
)

// Result is one benchmark run.
type Result struct {
	Label        string  `json:"label"`
	Target       string  `json:"target"`
	Agents       int     `json:"agents"`
	Requests     int     `json:"requests_per_agent"`
	Concurrency  int     `json:"concurrency"`
	Total        int     `json:"total"`
	OK           int64   `json:"ok"`
	Errors       int64   `json:"errors"`
	SetupSeconds float64 `json:"setup_seconds"` // provisioning (gateway) or connecting (pgbouncer)
	Seconds      float64 `json:"seconds"`
	RPS          float64 `json:"requests_per_second"`
	P50ms        float64 `json:"p50_ms"`
	P95ms        float64 `json:"p95_ms"`
	P99ms        float64 `json:"p99_ms"`
	MaxMs        float64 `json:"max_ms"`
	PeakBackends int64   `json:"peak_agent_backends"`
	ClientConns  int     `json:"agent_connections"`
	Probes       []Probe `json:"probes,omitempty"`
	Environment  string  `json:"environment,omitempty"`
}

// Probe is one isolation check run against the target after the load phase.
type Probe struct {
	Name    string `json:"name"`
	Blocked bool   `json:"blocked"`
	Detail  string `json:"detail"`
}

// agentClient issues one request of the workload mix for one agent.
type agentClient interface {
	do(ctx context.Context, r int) error
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "report" {
		report(os.Args[2:])
		return
	}
	flag.Parse()
	ctx := context.Background()
	res := Result{Label: *label, Target: *target, Agents: *nAgents, Requests: *nReq, Concurrency: *conc,
		Total: *nAgents * *nReq, Environment: os.Getenv("BENCH_ENV")}

	t0 := time.Now()
	var agents []agentClient
	var err error
	switch *target {
	case "gateway":
		agents, err = provisionGateway(ctx)
		res.ClientConns = *conc // the HTTP keep-alive pool is sized to the agents in flight
	case "pgbouncer":
		agents, err = connectBouncer(ctx)
		res.ClientConns = *nAgents
	default:
		err = fmt.Errorf("unknown target %q", *target)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
	res.SetupSeconds = time.Since(t0).Seconds()
	fmt.Printf("%s: %d agents ready in %.1fs\n", *label, len(agents), res.SetupSeconds)

	stop := make(chan struct{})
	peak := sampleBackends(stop)
	lat := make([]time.Duration, 0, res.Total)
	var mu sync.Mutex
	var ok, failed atomic.Int64
	var firstErr atomic.Value
	sem := make(chan struct{}, *conc)
	var wg sync.WaitGroup
	t0 = time.Now()
	for _, a := range agents {
		wg.Add(1)
		sem <- struct{}{}
		go func(a agentClient) {
			defer wg.Done()
			defer func() { <-sem }()
			local := make([]time.Duration, 0, *nReq)
			for r := 0; r < *nReq; r++ {
				start := time.Now()
				if err := a.do(ctx, r); err != nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
				} else {
					ok.Add(1)
				}
				local = append(local, time.Since(start))
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(a)
	}
	wg.Wait()
	res.Seconds = time.Since(t0).Seconds()
	close(stop)
	res.PeakBackends = <-peak

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(q float64) float64 { return float64(lat[int(float64(len(lat)-1)*q)].Microseconds()) / 1000 }
	res.OK, res.Errors = ok.Load(), failed.Load()
	res.RPS = float64(len(lat)) / res.Seconds
	res.P50ms, res.P95ms, res.P99ms, res.MaxMs = pct(.5), pct(.95), pct(.99), pct(1)
	if e := firstErr.Load(); e != nil {
		fmt.Println("first error:", e)
	}

	if *target == "pgbouncer" {
		res.Probes = probeBouncer(ctx, agents)
	}
	printResult(res)
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if res.Errors > 0 {
		os.Exit(1)
	}
}

// ---- gateway target

var httpClient *http.Client

type gatewayAgent struct{ id, token string }

func gwCall(method, path, token string, body any, admin bool) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	}
	req, _ := http.NewRequest(method, *gateway+path, rd)
	req.Header.Set("Content-Type", "application/json")
	if admin {
		req.Header.Set("X-Admin-Key", *adminKey)
	} else {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func provisionGateway(ctx context.Context) ([]agentClient, error) {
	httpClient = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
		MaxIdleConns: *conc, MaxIdleConnsPerHost: *conc, IdleConnTimeout: 90 * time.Second}}
	if err := retry(func() error {
		code, _, err := gwCall("GET", "/healthz", "", nil, false)
		if err == nil && code != 200 {
			err = fmt.Errorf("healthz: HTTP %d", code)
		}
		return err
	}); err != nil {
		return nil, err
	}
	code, b, err := gwCall("POST", "/v1/orgs", "", map[string]string{"name": "bench"}, true)
	if err != nil || code != 201 {
		return nil, fmt.Errorf("create org: %d %s %v", code, b, err)
	}
	var org struct{ ID string }
	_ = json.Unmarshal(b, &org)
	agents := make([]agentClient, *nAgents)
	errs := make(chan error, 1)
	sem := make(chan struct{}, 64)
	var wg sync.WaitGroup
	for i := range agents {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			code, b, err := gwCall("POST", "/v1/agents", "", map[string]any{"org_id": org.ID, "name": fmt.Sprintf("bench-%d", i), "rps_limit": 0}, true)
			if err != nil || code != 201 {
				select {
				case errs <- fmt.Errorf("create agent: %d %s %v", code, b, err):
				default:
				}
				return
			}
			var a struct{ ID, Token string }
			_ = json.Unmarshal(b, &a)
			agents[i] = &gatewayAgent{id: a.ID, token: a.Token}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
	}
	return agents, nil
}

func (a *gatewayAgent) do(ctx context.Context, r int) error {
	var code int
	var b []byte
	var err error
	switch r % 4 {
	case 0:
		code, b, err = gwCall("PUT", fmt.Sprintf("/v1/memory/plan-%d", r), a.token, map[string]any{"value": map[string]any{"step": r}}, false)
	case 1:
		code, b, err = gwCall("POST", "/v1/query", a.token, map[string]any{"sql": recentMemorySQL}, false)
	case 2:
		code, b, err = gwCall("POST", "/v1/events", a.token, map[string]any{"kind": "tool_call", "payload": map[string]any{"tool": "search", "i": r}}, false)
	case 3:
		code, b, err = gwCall("POST", "/v1/query", a.token, map[string]any{"sql": countEventsSQL, "params": []any{"tool_call"}}, false)
	}
	if err != nil {
		return err
	}
	if code != 200 && code != 201 {
		return fmt.Errorf("HTTP %d: %s", code, b)
	}
	return nil
}

// ---- pgbouncer target

const (
	recentMemorySQL = "SELECT key, value FROM swarmpg.memory ORDER BY updated_at DESC LIMIT 5"
	countEventsSQL  = "SELECT count(*) FROM swarmpg.events WHERE kind = $1"
)

type bouncerAgent struct {
	conn       *pgx.Conn
	id, org    string
	setupLocal string
}

func connectBouncer(ctx context.Context) ([]agentClient, error) {
	if err := retry(func() error {
		c, err := pgx.Connect(ctx, *bouncer)
		if err == nil {
			c.Close(ctx)
		}
		return err
	}); err != nil {
		return nil, err
	}
	org := randomUUID()
	agents := make([]agentClient, *nAgents)
	errs := make(chan error, 1)
	sem := make(chan struct{}, 128)
	var wg sync.WaitGroup
	for i := range agents {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			conn, err := pgx.Connect(ctx, *bouncer)
			if err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
			id := randomUUID()
			// The same per-transaction settings the gateway applies.
			setup := fmt.Sprintf("SET LOCAL app.agent_id = '%s'; SET LOCAL app.org_id = '%s'; "+
				"SET LOCAL statement_timeout = 5000; SET LOCAL idle_in_transaction_session_timeout = 2000; "+
				"SET LOCAL lock_timeout = 1000", id, org)
			agents[i] = &bouncerAgent{conn: conn, id: id, org: org, setupLocal: setup}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
	}
	return agents, nil
}

func (a *bouncerAgent) do(ctx context.Context, r int) error {
	return pgx.BeginFunc(ctx, a.conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, a.setupLocal); err != nil {
			return err
		}
		var err error
		switch r % 4 {
		case 0:
			_, err = tx.Exec(ctx, "SELECT swarmpg.memory_put($1, $2::jsonb)", fmt.Sprintf("plan-%d", r), fmt.Sprintf(`{"step":%d}`, r))
		case 1:
			err = drain(tx.Query(ctx, recentMemorySQL))
		case 2:
			_, err = tx.Exec(ctx, "INSERT INTO swarmpg.events(agent_id, org_id, kind, payload) VALUES(swarmpg.current_agent(), swarmpg.current_org(), $1, $2)",
				"tool_call", fmt.Sprintf(`{"tool":"search","i":%d}`, r))
		case 3:
			err = drain(tx.Query(ctx, countEventsSQL, "tool_call"))
		}
		return err
	})
}

func drain(rows pgx.Rows, err error) error {
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	rows.Close()
	return rows.Err()
}

// probeBouncer checks, after the load phase, what an agent holding its own
// connection can do. These are the attacks the gateway and kernel refuse.
func probeBouncer(ctx context.Context, agents []agentClient) []Probe {
	a, b := agents[0].(*bouncerAgent), agents[1].(*bouncerAgent)
	var probes []Probe
	// Agent b's rows exist from the load phase; agent a reads them by changing
	// its own identity, one SET away.
	var n int
	err := pgx.BeginFunc(ctx, a.conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL app.agent_id = '%s'; SET LOCAL app.org_id = '%s'", b.id, b.org)); err != nil {
			return err
		}
		return tx.QueryRow(ctx, "SELECT count(*) FROM swarmpg.memory WHERE agent_id = $1", b.id).Scan(&n)
	})
	probes = append(probes, Probe{Name: "read another agent's memory by changing identity (SET LOCAL)", Blocked: err != nil || n == 0,
		Detail: fmt.Sprintf("rows=%d err=%v", n, err)})

	// A runaway query runs until the statement timeout, holding a server
	// connection the whole time; there is no cost or row budget.
	start := time.Now()
	err = pgx.BeginFunc(ctx, a.conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, a.setupLocal); err != nil {
			return err
		}
		return drain(tx.Query(ctx, "SELECT count(*) FROM generate_series(1, 100000) x, generate_series(1, 100000) y"))
	})
	held := time.Since(start)
	probes = append(probes, Probe{Name: "runaway query is refused before it runs", Blocked: err != nil && held < time.Second,
		Detail: fmt.Sprintf("held a server connection for %s, then: %v", held.Round(10*time.Millisecond), err)})

	// A session-level SET survives the transaction and stays on the pooled
	// server connection for whichever client gets it next.
	err = pgx.BeginFunc(ctx, a.conn, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("SET app.agent_id = '%s'", b.id))
		return err
	})
	probes = append(probes, Probe{Name: "session-level SET of the identity is refused", Blocked: err != nil,
		Detail: fmt.Sprintf("err=%v (a SET without LOCAL persists on the pooled server connection)", err)})
	return probes
}

// ---- shared

// retry waits up to 30s for a just-started service.
func retry(f func() error) error {
	var err error
	for i := 0; i < 150; i++ {
		if err = f(); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return err
}

func sampleBackends(stop chan struct{}) chan int64 {
	peak := make(chan int64, 1)
	if *pgAdmin == "" {
		peak <- 0
		return peak
	}
	go func() {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, *pgAdmin)
		if err != nil {
			peak <- 0
			return
		}
		defer conn.Close(ctx)
		var max int64
		for {
			select {
			case <-stop:
				peak <- max
				return
			case <-time.After(100 * time.Millisecond):
				var n int64
				if conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE usename = 'agent_runtime'").Scan(&n) == nil && n > max {
					max = n
				}
			}
		}
	}()
	return peak
}

func randomUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func printResult(r Result) {
	fmt.Printf("%s: %d/%d ok, %d errors, %.0f req/s over %.1fs; p50 %.1fms p95 %.1fms p99 %.1fms max %.0fms; peak agent backends %d for %d agent connections\n",
		r.Label, r.OK, r.Total, r.Errors, r.RPS, r.Seconds, r.P50ms, r.P95ms, r.P99ms, r.MaxMs, r.PeakBackends, r.ClientConns)
	for _, p := range r.Probes {
		mark := "NOT BLOCKED"
		if p.Blocked {
			mark = "blocked"
		}
		fmt.Printf("  probe: %-62s %s (%s)\n", p.Name, mark, p.Detail)
	}
}

// report renders result files as a Markdown table, in the order given.
func report(files []string) {
	var rs []Result
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil {
			fmt.Fprintln(os.Stderr, f, err)
			os.Exit(1)
		}
		rs = append(rs, r)
	}
	fmt.Println("| Setup | Agents | Agent connections | Peak Postgres backends | Requests | Errors | Req/s | p50 ms | p95 ms | p99 ms |")
	fmt.Println("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	for _, r := range rs {
		fmt.Printf("| %s | %d | %d | %d | %d | %d | %.0f | %.1f | %.1f | %.1f |\n",
			r.Label, r.Agents, r.ClientConns, r.PeakBackends, r.Total, r.Errors, r.RPS, r.P50ms, r.P95ms, r.P99ms)
	}
}
