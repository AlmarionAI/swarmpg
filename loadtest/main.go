// Simulates N concurrent agents through the gateway, then runs isolation probes.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	base     = flag.String("gateway", "http://127.0.0.1:8080", "gateway URL")
	adminKey = flag.String("admin-key", "dev-admin-key", "admin key")
	nAgents  = flag.Int("agents", 2000, "concurrent agents")
	nReq     = flag.Int("requests", 10, "requests per agent")
	conc     = flag.Int("concurrency", 500, "max in-flight agents")
	pgURL    = flag.String("pg", os.Getenv("DATABASE_URL"), "postgres URL for pg_stat_activity")
)

var client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1000, MaxConnsPerHost: 0}, Timeout: 30 * time.Second}

func call(method, path, token string, body any, admin bool) (int, []byte) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, *base+path, rd)
	req.Header.Set("Content-Type", "application/json")
	if admin {
		req.Header.Set("X-Admin-Key", *adminKey)
	} else {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, []byte(err.Error())
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

type agent struct{ ID, Token string }

func createAgent(org string, i int) agent {
	code, b := call("POST", "/v1/agents", "", map[string]any{"org_id": org, "name": fmt.Sprintf("agent-%d", i), "rps_limit": 50}, true)
	if code != 201 {
		fmt.Println("create agent failed:", string(b))
		os.Exit(1)
	}
	var a agent
	_ = json.Unmarshal(b, &a)
	return a
}

func main() {
	flag.Parse()
	code, b := call("POST", "/v1/orgs", "", map[string]string{"name": "loadtest"}, true)
	if code != 201 {
		fmt.Println("org failed:", string(b))
		os.Exit(1)
	}
	var org struct{ ID string }
	_ = json.Unmarshal(b, &org)

	fmt.Printf("provisioning %d agents...\n", *nAgents)
	agents := make([]agent, *nAgents)
	t0 := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	for i := range agents {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) { defer wg.Done(); agents[i] = createAgent(org.ID, i); <-sem }(i)
	}
	wg.Wait()
	fmt.Printf("provisioned in %s\n", time.Since(t0).Round(time.Millisecond))

	// ---- load phase
	var lat []time.Duration
	var mu sync.Mutex
	var ok, fail, limited atomic.Int64
	sem = make(chan struct{}, *conc)
	maxConns := atomic.Int64{}
	stop := make(chan struct{})
	go func() { // sample backend connection count while load runs
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
				if n := backendConns(); n > maxConns.Load() {
					maxConns.Store(n)
				}
			}
		}
	}()
	t0 = time.Now()
	for i, a := range agents {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, a agent) {
			defer wg.Done()
			defer func() { <-sem }()
			for r := 0; r < *nReq; r++ {
				var code int
				var body []byte
				start := time.Now()
				switch r % 4 {
				case 0:
					code, body = call("PUT", fmt.Sprintf("/v1/memory/plan-%d", r), a.Token, map[string]any{"value": map[string]any{"step": r, "agent": i}}, false)
				case 1:
					code, body = call("POST", "/v1/query", a.Token, map[string]any{"sql": "SELECT key, value FROM swarmpg.memory ORDER BY updated_at DESC LIMIT 5"}, false)
				case 2:
					code, body = call("POST", "/v1/events", a.Token, map[string]any{"kind": "tool_call", "payload": map[string]any{"tool": "search", "i": r}}, false)
				case 3:
					code, body = call("POST", "/v1/query", a.Token, map[string]any{"sql": "SELECT count(*) FROM swarmpg.events WHERE kind = $1", "params": []any{"tool_call"}}, false)
				}
				d := time.Since(start)
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
				switch {
				case code == 200 || code == 201:
					ok.Add(1)
				case code == 429:
					limited.Add(1)
				default:
					fail.Add(1)
					if fail.Load() < 5 {
						fmt.Println("  error:", code, string(body))
					}
				}
			}
		}(i, a)
	}
	wg.Wait()
	close(stop)
	el := time.Since(t0)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p := func(q float64) time.Duration { return lat[int(float64(len(lat)-1)*q)] }
	fmt.Printf("\n== load: %d agents x %d requests, %d in flight ==\n", *nAgents, *nReq, *conc)
	fmt.Printf("ok=%d rate_limited=%d failed=%d\n", ok.Load(), limited.Load(), fail.Load())
	fmt.Printf("throughput: %.0f req/s over %s\n", float64(len(lat))/el.Seconds(), el.Round(time.Millisecond))
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", p(.5).Round(100*time.Microsecond), p(.95).Round(100*time.Microsecond), p(.99).Round(100*time.Microsecond), p(1).Round(100*time.Microsecond))
	fmt.Printf("peak postgres backends used by agents: %d (for %d agents)\n", maxConns.Load(), *nAgents)

	// ---- isolation probes: agent 0 attacks agent 1
	fmt.Println("\n== isolation probes (agent 0 vs agent 1) ==")
	A, B := agents[0], agents[1]
	// First, agent B writes a private memory row so cross-agent reads have a
	// concrete target to fail to find.
	_, _ = call("PUT", "/v1/memory/secret", B.Token, map[string]any{"value": map[string]any{"pw": "B-private"}}, false)

	var probePass, probeFail int
	// want: "reject" means the probe MUST be refused (error or 0 rows);
	//       "allow"  means it MUST succeed. isExfil marks reads that must return 0 rows.
	probe := func(name, want string, token, sql string, params ...any) {
		code, body := call("POST", "/v1/query", token, map[string]any{"sql": sql, "params": params}, false)
		var r struct {
			RowCount int            `json:"row_count"`
			Rows     [][]any        `json:"rows"`
			Error    map[string]any `json:"error"`
		}
		_ = json.Unmarshal(body, &r)
		rejected := r.Error != nil || code >= 400
		var ok bool
		var res string
		switch want {
		case "reject":
			ok = rejected
			if rejected {
				res = fmt.Sprintf("rejected (%v)", r.Error["code"])
			} else {
				res = fmt.Sprintf("LEAKED rows=%d", r.RowCount)
			}
		case "zero": // read that must return 0 rows (RLS isolation)
			ok = !rejected && r.RowCount == 0
			res = fmt.Sprintf("rows=%d", r.RowCount)
		case "allow":
			ok = !rejected
			if rejected {
				res = fmt.Sprintf("WRONGLY REJECTED (%v)", r.Error["code"])
			} else {
				res = fmt.Sprintf("ok rows=%d", r.RowCount)
			}
		}
		mark := "PASS"
		if ok {
			probePass++
		} else {
			mark = "FAIL"
			probeFail++
		}
		fmt.Printf("  [%s] %-50s -> %s\n", mark, name, res)
	}
	// Identity cannot be spoofed by any SQL route.
	probe("spoof identity via SET", "reject", A.Token, fmt.Sprintf("SET app.agent_id = '%s'", B.ID))
	probe("spoof identity via set_config()", "reject", A.Token, "SELECT set_config('app.agent_id', $1, true)", B.ID)
	probe("escalate via SET ROLE", "reject", A.Token, "SET ROLE postgres")
	probe("escalate via RESET ROLE", "reject", A.Token, "RESET ROLE")
	probe("anonymous code via DO", "reject", A.Token, "DO $$ BEGIN PERFORM 1; END $$")
	probe("multi-statement smuggling", "reject", A.Token, "SELECT 1; SET app.agent_id = 'x'")
	probe("obfuscated SET (nested comment)", "reject", A.Token, "/* /* */ */ SET app.agent_id = 'x'")
	probe("obfuscated SET (tab)", "reject", A.Token, "SET\tapp.agent_id = 'x'")
	// Cross-agent data isolation (RLS).
	probe("read peer memory by id", "zero", A.Token, "SELECT * FROM swarmpg.memory WHERE agent_id = $1", B.ID)
	probe("read all non-self memory", "zero", A.Token, "SELECT * FROM swarmpg.memory WHERE agent_id <> swarmpg.current_agent()")
	// No object creation / DDL, so nothing can be owned and leaked across the pool.
	probe("create scratch table (DDL)", "reject", A.Token, "CREATE TABLE scratch.notes(id int)")
	probe("CREATE TABLE AS to exfil own rows", "reject", A.Token, "CREATE TABLE public.leak AS SELECT * FROM swarmpg.memory")
	probe("create function to hide SET", "reject", A.Token, "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'")
	probe("create temp table (pool leak)", "reject", A.Token, "CREATE TEMP TABLE t(x int)")
	probe("declare WITH HOLD cursor (pool leak)", "reject", A.Token, "DECLARE c CURSOR WITH HOLD FOR SELECT 1")
	// No fleet-wide denial of service through the shared role.
	probe("terminate peer backends", "reject", A.Token, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = current_user")
	probe("cancel peer backends", "reject", A.Token, "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE usename = current_user")
	probe("read peer SQL text (pg_stat_activity)", "reject", A.Token, "SELECT query FROM pg_stat_activity WHERE usename = current_user")
	probe("read peer SQL text (backend function)", "reject", A.Token, "SELECT pg_stat_get_backend_activity(s) FROM pg_stat_get_backend_idset() s")
	probe("session advisory lock", "reject", A.Token, "SELECT pg_advisory_lock(1)")
	probe("large object spam", "reject", A.Token, "SELECT lo_create(0)")
	// Control-plane confidentiality and log integrity.
	probe("read agent registry (token hashes)", "reject", A.Token, "SELECT token_hash FROM swarmpg.agents")
	probe("read server audit log", "reject", A.Token, "SELECT * FROM swarmpg.audit")
	probe("tamper with event log (DELETE)", "reject", A.Token, "DELETE FROM swarmpg.events")
	probe("truncate event log", "reject", A.Token, "TRUNCATE swarmpg.events")
	// Legitimate operations still work.
	probe("read own memory", "allow", A.Token, "SELECT count(*) FROM swarmpg.memory")
	probe("write own document", "allow", A.Token, "INSERT INTO swarmpg.documents(agent_id,org_id,collection,body) VALUES(swarmpg.current_agent(),swarmpg.current_org(),'c',jsonb_build_object('ok',true))")

	// ---- kernel probes: raw SQL as agent_runtime with the gateway bypassed
	// entirely. Only the SwarmPG kernel can pass these, so they run only there.
	if *pgURL != "" && psqlOut(*pgURL, "SELECT EXISTS (SELECT 1 FROM pg_settings WHERE name = 'agent_id')") == "t" {
		fmt.Println("\n== kernel probes (gateway bypassed: raw SQL as agent_runtime) ==")
		rt := runtimeURL(*pgURL)
		kprobe := func(name, want string, stmts ...string) {
			out, err := psqlAs(rt, stmts...)
			var ok bool
			switch want {
			case "zero":
				ok = err == nil && out == "0"
			default: // the error message the kernel must raise
				ok = err != nil && strings.Contains(out, want)
			}
			mark := "PASS"
			if ok {
				probePass++
			} else {
				mark = "FAIL"
				probeFail++
			}
			fmt.Printf("  [%s] %-50s -> %s\n", mark, name, firstLine(out))
		}
		kprobe("unsealed query refused", "no agent principal established",
			"SELECT count(*) FROM swarmpg.memory")
		kprobe("re-seal to another agent refused", "is sealed",
			"BEGIN", fmt.Sprintf("SET LOCAL agent_id = '%s'", A.ID), fmt.Sprintf("SET LOCAL agent_id = '%s'", B.ID), "COMMIT")
		kprobe("RESET of a sealed identity refused", "is sealed",
			"BEGIN", fmt.Sprintf("SET LOCAL agent_id = '%s'", A.ID), "RESET agent_id", "COMMIT")
		kprobe("SET app.agent_id cannot read peer rows", "zero",
			"BEGIN", fmt.Sprintf("SET LOCAL agent_id = '%s'", A.ID), fmt.Sprintf("SET LOCAL app.agent_id = '%s'", B.ID),
			fmt.Sprintf("SELECT count(*) FROM swarmpg.memory WHERE agent_id = '%s'", B.ID), "COMMIT")
	}

	fmt.Printf("\nisolation probes: %d passed, %d failed\n", probePass, probeFail)
	if probeFail > 0 {
		os.Exit(1)
	}
}

// runtimeURL is the admin URL with the user swapped for agent_runtime.
func runtimeURL(admin string) string {
	u, err := url.Parse(admin)
	if err != nil {
		return admin
	}
	u.User = url.User("agent_runtime")
	return u.String()
}

// psqlAs runs each statement as its own -c in one session and returns the
// combined output; with -q only query results and errors are printed.
func psqlAs(dsn string, stmts ...string) (string, error) {
	args := []string{dsn, "-v", "ON_ERROR_STOP=1", "-tAq"}
	for _, s := range stmts {
		args = append(args, "-c", s)
	}
	out, err := exec.Command("psql", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func psqlOut(dsn, sql string) string {
	out, _ := psqlAs(dsn, sql)
	return out
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func backendConns() int64 {
	if *pgURL == "" {
		return 0
	}
	out, err := exec.Command("psql", *pgURL, "-tAc", "SELECT count(*) FROM pg_stat_activity WHERE usename='agent_runtime'").Output()
	if err != nil {
		return 0
	}
	var n int64
	fmt.Sscan(strings.TrimSpace(string(out)), &n)
	return n
}
