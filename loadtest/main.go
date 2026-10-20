// Simulates N concurrent agents through the gateway, then runs isolation probes.
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

	principalSecret = flag.String("principal-secret", os.Getenv("PRINCIPAL_SECRET"), "agent_principal_secret, to sign principals in the kernel probes")
)

// retry429 makes the probes honour Retry-After like a well-behaved agent. The
// load phase leaves it off, so rate limiting there is counted, not hidden.
var retry429 atomic.Bool

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
	resp, err := doWithRetry(req)
	if err != nil {
		return 0, []byte(err.Error())
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// doWithRetry sends req, and while retry429 is on, retries a 429 up to three
// times after the Retry-After delay.
func doWithRetry(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		req.Body.Close()
	}
	for attempt := 0; ; attempt++ {
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusTooManyRequests || !retry429.Load() || attempt == 3 {
			return resp, err
		}
		resp.Body.Close()
		time.Sleep(time.Second)
	}
}

type agent struct{ ID, Token string }

func createAgent(org string, i int) agent {
	return createAgentWith(org, fmt.Sprintf("agent-%d", i), nil)
}

// createAgentWith mints an agent with rps_limit 50 plus any extra budget fields.
func createAgentWith(org, name string, extra map[string]any) agent {
	body := map[string]any{"org_id": org, "name": name, "rps_limit": 50}
	for k, v := range extra {
		body[k] = v
	}
	code, b := call("POST", "/v1/agents", "", body, true)
	if code != 201 {
		fmt.Println("create agent failed:", string(b))
		os.Exit(1)
	}
	var a agent
	_ = json.Unmarshal(b, &a)
	return a
}

// decode parses a JSON object response; anything else decodes to an empty map.
func decode(b []byte) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m == nil {
		m = map[string]any{}
	}
	return m
}

// mustOK stops the run when a probe's setup write fails, so a probe can never
// pass vacuously against data that was never written.
func mustOK(code int, body []byte) {
	if code != 200 && code != 201 {
		fmt.Println("probe setup failed:", code, string(body))
		os.Exit(1)
	}
}

func createOrg(name string) string {
	code, b := call("POST", "/v1/orgs", "", map[string]string{"name": name}, true)
	if code != 201 {
		fmt.Println("org failed:", string(b))
		os.Exit(1)
	}
	var org struct{ ID string }
	_ = json.Unmarshal(b, &org)
	return org.ID
}

// principal is the kernel's "agent=<id>,org=<id>" principal, signed the way
// the gateway signs it when a principal secret is configured.
func principal(agentID, orgID string) string {
	p := "agent=" + agentID + ",org=" + orgID
	if *principalSecret != "" {
		m := hmac.New(sha256.New, []byte(*principalSecret))
		m.Write([]byte(p))
		p += ",sig=" + hex.EncodeToString(m.Sum(nil))
	}
	return p
}

func main() {
	flag.Parse()
	org := struct{ ID string }{createOrg("loadtest")}

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
	retry429.Store(true)
	fmt.Println("\n== isolation probes (agent 0 vs agent 1) ==")
	A, B := agents[0], agents[1]
	// First, agent B writes a private memory row so cross-agent reads have a
	// concrete target to fail to find.
	mustOK(call("PUT", "/v1/memory/secret", B.Token, map[string]any{"value": map[string]any{"pw": "B-private"}}, false))

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
		wantCode := ""
		if code, found := strings.CutPrefix(want, "reject:"); found {
			want, wantCode = "reject", code
		}
		switch want {
		case "reject":
			ok = rejected && (wantCode == "" || r.Error["code"] == wantCode)
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

	// ---- MCP: the same agents through the /mcp endpoint
	fmt.Println("\n== MCP probes (/mcp, agent tokens) ==")
	check := func(name string, ok bool, res string) {
		mark := "PASS"
		if ok {
			probePass++
		} else {
			mark = "FAIL"
			probeFail++
		}
		fmt.Printf("  [%s] %-50s -> %s\n", mark, name, res)
	}
	code, initMsg := mcpRPC(A.Token, false, "initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "loadtest", "version": "1"}}, nil)
	initRes, _ := initMsg["result"].(map[string]any)
	check("legacy initialize negotiates 2025-06-18", code == 200 && initRes["protocolVersion"] == "2025-06-18", fmt.Sprintf("%d %v", code, initRes["protocolVersion"]))
	code, disc := mcpRPC(A.Token, true, "server/discover", nil, nil)
	discRes, _ := disc["result"].(map[string]any)
	check("2026-07-28 server/discover", code == 200 && discRes["resultType"] == "complete", fmt.Sprintf("%d %v", code, discRes["resultType"]))
	code, list := mcpRPC(A.Token, true, "tools/list", nil, nil)
	listRes, _ := list["result"].(map[string]any)
	tools, _ := listRes["tools"].([]any)
	check("tools/list returns the 8 tools", code == 200 && len(tools) == 8, fmt.Sprintf("%d tools", len(tools)))
	code, _ = mcpRPC("", true, "tools/list", nil, nil)
	check("no token refused", code == 401, fmt.Sprint(code))
	code, _ = mcpRPC("spg_not_a_real_token", true, "tools/list", nil, nil)
	check("invalid token refused", code == 401, fmt.Sprint(code))
	code, _ = mcpRPC(A.Token, true, "tools/list", nil, map[string]string{"Origin": "https://evil.example"})
	check("foreign Origin refused (DNS rebinding)", code == 403, fmt.Sprint(code))

	tool := func(token, name string, args map[string]any) (bool, map[string]any, string) {
		_, msg := mcpRPC(token, true, "tools/call", map[string]any{"name": name, "arguments": args}, nil)
		res, _ := msg["result"].(map[string]any)
		sc, _ := res["structuredContent"].(map[string]any)
		if res == nil { // a protocol or HTTP error: show it rather than nil fields
			sc = map[string]any{"error": msg["error"]}
		}
		text := ""
		if content, _ := res["content"].([]any); len(content) > 0 {
			text, _ = content[0].(map[string]any)["text"].(string)
		}
		return res["isError"] == true, sc, text
	}
	isErr, _, text := tool(A.Token, "schema", map[string]any{})
	check("schema tool describes the tables", !isErr && strings.Contains(text, "swarmpg.memory"), firstLine(text))
	isErr, _, _ = tool(A.Token, "memory_put", map[string]any{"key": "mcp-plan", "value": map[string]any{"step": 7}})
	_, got, _ := tool(A.Token, "memory_get", map[string]any{"key": "mcp-plan"})
	value, _ := json.Marshal(got["value"])
	check("memory_put then memory_get round-trips", !isErr && string(value) == `{"step":7}`, string(value))
	isErr, sc, _ := tool(A.Token, "memory_get", map[string]any{"key": "secret"})
	errBody, _ := sc["error"].(map[string]any)
	check("memory_get cannot read a peer's private key", isErr && errBody["code"] == "not_found", fmt.Sprint(errBody["code"]))
	isErr, sc, _ = tool(A.Token, "query", map[string]any{"sql": "SELECT * FROM swarmpg.memory WHERE agent_id = $1", "params": []any{B.ID}})
	check("query cannot read a peer's rows", !isErr && sc["row_count"] == float64(0), fmt.Sprintf("rows=%v", sc["row_count"]))
	isErr, sc, _ = tool(A.Token, "query", map[string]any{"sql": fmt.Sprintf("SET app.agent_id = '%s'", B.ID)})
	errBody, _ = sc["error"].(map[string]any)
	check("query cannot change identity", isErr, fmt.Sprintf("isError (%v)", errBody["code"]))
	key := fmt.Sprintf("mcp-%d", time.Now().UnixNano())
	_, first, _ := tool(A.Token, "events_append", map[string]any{"kind": "mcp_probe", "idempotency_key": key})
	_, again, _ := tool(A.Token, "events_append", map[string]any{"kind": "mcp_probe", "idempotency_key": key})
	check("events_append retry with the same key is replayed", first["id"] != nil && first["id"] == again["id"] && again["replayed"] == true,
		fmt.Sprintf("id %v then %v, replayed=%v", first["id"], again["id"], again["replayed"]))

	// ---- memory versions: compare-and-set, history, reads of the past, undo
	fmt.Println("\n== memory version probes (REST and MCP) ==")
	k := fmt.Sprintf("cas-%d", time.Now().UnixNano())
	mem := "/v1/memory/" + k
	put := func(token string, n int, expected any) (int, map[string]any) {
		body := map[string]any{"value": map[string]any{"n": n}}
		if expected != nil {
			body["expected_version"] = expected
		}
		code, b := call("PUT", mem, token, body, false)
		return code, decode(b)
	}
	get := func(token, query string) (int, map[string]any) {
		code, b := call("GET", mem+query, token, nil, false)
		return code, decode(b)
	}
	n := func(m map[string]any) any {
		v, _ := m["value"].(map[string]any)
		return v["n"]
	}
	errOf := func(m map[string]any) map[string]any {
		e, _ := m["error"].(map[string]any)
		return e
	}
	code, r := put(A.Token, 1, 0)
	check("create-only write (expected_version 0)", code == 200 && r["version"] == float64(1), fmt.Sprintf("%d v%v", code, r["version"]))
	code, r = put(A.Token, 1, 0)
	check("second create-only write conflicts", code == 409 && errOf(r)["code"] == "version_conflict" && errOf(r)["current_version"] == float64(1),
		fmt.Sprintf("%d %v current=%v", code, errOf(r)["code"], errOf(r)["current_version"]))
	code, r = put(A.Token, 2, 1)
	check("write at the current version", code == 200 && r["version"] == float64(2), fmt.Sprintf("%d v%v", code, r["version"]))
	code, r = put(A.Token, 99, 1)
	_, now := get(A.Token, "")
	check("stale write conflicts and writes nothing", code == 409 && errOf(r)["current_version"] == float64(2) && n(now) == float64(2),
		fmt.Sprintf("%d current=%v value n=%v", code, errOf(r)["current_version"], n(now)))

	E := createAgentWith(org.ID, "racer", map[string]any{"max_concurrency": 32, "rps_limit": 500})
	_, _ = call("PUT", mem, E.Token, map[string]any{"value": map[string]any{"n": 0}}, false)
	var wins, conflicts, other atomic.Int64
	var race sync.WaitGroup
	for i := 0; i < 20; i++ {
		race.Add(1)
		go func(i int) {
			defer race.Done()
			switch c, _ := call("PUT", mem, E.Token, map[string]any{"value": map[string]any{"n": i}, "expected_version": 1}, false); c {
			case 200:
				wins.Add(1)
			case 409:
				conflicts.Add(1)
			default:
				other.Add(1)
			}
		}(i)
	}
	race.Wait()
	check("20 concurrent writers at one version: one wins", wins.Load() == 1 && conflicts.Load() == 19 && other.Load() == 0,
		fmt.Sprintf("won=%d conflicted=%d other=%d", wins.Load(), conflicts.Load(), other.Load()))

	code, r = get(A.Token, "?version=1")
	check("read an earlier version", code == 200 && n(r) == float64(1), fmt.Sprintf("%d n=%v", code, n(r)))
	_, hb := call("GET", mem+"/history", A.Token, nil, false)
	versions, _ := decode(hb)["versions"].([]any)
	check("history lists every version", len(versions) == 2, fmt.Sprintf("%d versions", len(versions)))
	if len(versions) == 2 {
		v1 := versions[1].(map[string]any)["changed_at"].(string)
		t1, _ := time.Parse(time.RFC3339Nano, v1)
		code, r = get(A.Token, "?as_of="+url.QueryEscape(v1))
		before, _ := get(A.Token, "?as_of="+url.QueryEscape(t1.Add(-time.Microsecond).Format(time.RFC3339Nano)))
		check("as_of reads the value at that time", code == 200 && n(r) == float64(1) && before == 404,
			fmt.Sprintf("at v1: n=%v; just before: %d", n(r), before))
	}
	code, ub := call("POST", mem+"/undo", A.Token, nil, false)
	u := decode(ub)
	_, now = get(A.Token, "")
	check("undo restores the previous value as a new version", code == 200 && u["restored_version"] == float64(1) && u["version"] == float64(3) && n(now) == float64(1),
		fmt.Sprintf("%d v%v from v%v, n=%v", code, u["version"], u["restored_version"], n(now)))
	code, _ = call("DELETE", mem+"?expected_version=3", A.Token, nil, false)
	gone, _ := get(A.Token, "")
	_, _ = call("POST", mem+"/undo", A.Token, nil, false)
	back, now := get(A.Token, "")
	check("a delete can be undone", code == 200 && gone == 404 && back == 200 && n(now) == float64(1),
		fmt.Sprintf("delete %d, then %d, after undo %d n=%v", code, gone, back, n(now)))
	probe("raw UPDATE cannot set a version", "allow", A.Token, "UPDATE swarmpg.memory SET version = 1 WHERE key = $1", k)
	_, now = get(A.Token, "")
	check("... the version still moves forward", now["version"] == float64(6), fmt.Sprintf("v%v", now["version"]))
	probe("rewrite history (UPDATE)", "reject", A.Token, "UPDATE swarmpg.memory_history SET value = '{}'::jsonb")
	probe("erase history (DELETE)", "reject", A.Token, "DELETE FROM swarmpg.memory_history")
	probe("forge history (INSERT)", "reject", A.Token, "INSERT INTO swarmpg.memory_history(agent_id, org_id, key, version) VALUES ($1, $2, 'x', 1)", A.ID, org.ID)
	code, _ = call("GET", mem+"/history", B.Token, nil, false)
	check("a peer cannot read your key's history", code == 404, fmt.Sprint(code))
	probe("a peer cannot read your history rows", "zero", B.Token, "SELECT * FROM swarmpg.memory_history WHERE agent_id = $1", A.ID)

	isErr, sc, _ = tool(A.Token, "memory_put", map[string]any{"key": k, "value": map[string]any{"n": 7}, "expected_version": 2})
	errBody, _ = sc["error"].(map[string]any)
	check("MCP memory_put with a stale version conflicts", isErr && errBody["code"] == "version_conflict" && errBody["current_version"] == float64(6),
		fmt.Sprintf("%v current=%v", errBody["code"], errBody["current_version"]))
	isErr, sc, _ = tool(A.Token, "memory_history", map[string]any{"key": k})
	hv, _ := sc["versions"].([]any)
	check("MCP memory_history", !isErr && len(hv) == 6, fmt.Sprintf("%d versions", len(hv)))
	isErr, sc, _ = tool(A.Token, "memory_get", map[string]any{"key": k, "version": 2})
	v2, _ := sc["value"].(map[string]any)
	check("MCP memory_get at a version", !isErr && v2["n"] == float64(2), fmt.Sprintf("n=%v", v2["n"]))
	isErr, sc, _ = tool(A.Token, "memory_undo", map[string]any{"key": k, "to_version": 2, "expected_version": 6})
	_, now = get(A.Token, "")
	check("MCP memory_undo to a version", !isErr && sc["version"] == float64(7) && n(now) == float64(2), fmt.Sprintf("v%v n=%v", sc["version"], n(now)))

	// ---- kernel probes: raw SQL as agent_runtime with the gateway bypassed
	// entirely, then budget probes through the gateway. Only the SwarmPG
	// kernel can pass these, so they run only there.
	if *pgURL != "" && psqlOut(*pgURL, "SELECT EXISTS (SELECT 1 FROM pg_settings WHERE name = 'agent_principal')") == "t" {
		// A second org with an org-shared memory row, for the cross-org probe.
		org2 := createOrg("loadtest-other")
		D := createAgentWith(org2, "other-org", nil)
		mustOK(call("PUT", "/v1/memory/shared-plan", D.Token, map[string]any{"value": map[string]any{"org": "other"}, "shared": true}, false))

		fmt.Println("\n== kernel probes (gateway bypassed: raw SQL as agent_runtime) ==")
		rt := runtimeURL(*pgURL)
		seal := func(agentID string) string {
			return fmt.Sprintf("SET LOCAL agent_principal = '%s'", principal(agentID, org.ID))
		}
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
		kprobe("half-set principal refused (org missing)", "invalid agent principal",
			"BEGIN", fmt.Sprintf("SET LOCAL agent_principal = 'agent=%s'", A.ID), "COMMIT")
		kprobe("re-seal to another agent refused", "is sealed",
			"BEGIN", seal(A.ID), seal(B.ID), "COMMIT")
		kprobe("RESET of a sealed principal refused", "is sealed",
			"BEGIN", seal(A.ID), "RESET agent_principal", "COMMIT")
		kprobe("SET app.agent_id cannot read peer rows", "zero",
			"BEGIN", seal(A.ID), fmt.Sprintf("SET LOCAL app.agent_id = '%s'", B.ID),
			fmt.Sprintf("SELECT count(*) FROM swarmpg.memory WHERE agent_id = '%s'", B.ID), "COMMIT")
		kprobe("SET app.org_id cannot read another org's rows", "zero",
			"BEGIN", seal(A.ID), fmt.Sprintf("SET LOCAL app.org_id = '%s'", org2),
			fmt.Sprintf("SELECT count(*) FROM swarmpg.memory WHERE org_id = '%s'", org2), "COMMIT")
		if *principalSecret != "" {
			kprobe("forge an unsigned principal", "not signed",
				"BEGIN", fmt.Sprintf("SET LOCAL agent_principal = 'agent=%s,org=%s'", B.ID, org.ID), "COMMIT")
			kprobe("forge a principal with a bad signature", "invalid agent principal signature",
				"BEGIN", fmt.Sprintf("SET LOCAL agent_principal = 'agent=%s,org=%s,sig=%s'", B.ID, org.ID, strings.Repeat("0", 64)), "COMMIT")
		} else {
			fmt.Println("  [SKIP] forged principals: PRINCIPAL_SECRET is not set, so principals are unsigned")
		}
		kprobe("raise the row budget refused", "can only be lowered",
			"BEGIN", seal(A.ID), "SET LOCAL agent_max_rows = 0", "COMMIT")
		kprobe("raise the cost budget refused", "can only be lowered",
			"BEGIN", seal(A.ID), "SET LOCAL agent_max_cost = 0", "COMMIT")
		kprobe("change the role's budget defaults refused", "permission denied",
			"ALTER ROLE agent_runtime SET agent_max_rows = 0")

		fmt.Println("\n== kernel budget probes (through the gateway: max_rows=5, max_cost=1000) ==")
		C := createAgentWith(org.ID, "budgeted", map[string]any{"max_rows": 5, "max_cost": 1000})
		runaway := "SELECT count(*) FROM generate_series(1, 100000) a, generate_series(1, 100000) b"
		probe("write over the row budget refused", "reject:53400", C.Token,
			"INSERT INTO swarmpg.documents(agent_id, org_id, collection, body) SELECT swarmpg.current_agent(), swarmpg.current_org(), 'bulk', '{}' FROM generate_series(1, 10)")
		probe("refused write left nothing behind", "zero", C.Token,
			"SELECT 1 FROM swarmpg.documents WHERE collection = 'bulk'")
		probe("runaway query refused before it runs", "reject:53400", C.Token, runaway)
		probe("EXPLAIN ANALYZE cannot dodge the cost budget", "reject:53400", C.Token, "EXPLAIN ANALYZE "+runaway)
		probe("query_to_xml cannot dodge the cost budget", "reject:53400", C.Token, "SELECT query_to_xml($1, false, false, '')", runaway)
		probe("EXPLAIN still shows the estimate", "allow", C.Token, "EXPLAIN "+runaway)
		_, msg := mcpRPC(C.Token, true, "tools/call", map[string]any{"name": "query", "arguments": map[string]any{"sql": runaway}}, nil)
		res, _ := msg["result"].(map[string]any)
		sc, _ := res["structuredContent"].(map[string]any)
		errBody, _ := sc["error"].(map[string]any)
		ok := res["isError"] == true && errBody["code"] == "53400"
		if ok {
			probePass++
		} else {
			probeFail++
		}
		fmt.Printf("  [%s] %-50s -> %v\n", map[bool]string{true: "PASS", false: "FAIL"}[ok], "MCP query tool is held to the same budget", errBody["code"])
	}

	fmt.Printf("\nisolation probes: %d passed, %d failed\n", probePass, probeFail)
	if probeFail > 0 {
		os.Exit(1)
	}
}

// mcpRPC sends one JSON-RPC request to /mcp, as a 2026-07-28 client (modern:
// version and capabilities in _meta, mirrored in headers) or a 2025-06-18 one,
// and returns the HTTP status and the decoded reply.
func mcpRPC(token string, modern bool, method string, params map[string]any, extra map[string]string) (int, map[string]any) {
	if params == nil {
		params = map[string]any{}
	}
	h := map[string]string{"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
	if modern {
		params["_meta"] = map[string]any{
			"io.modelcontextprotocol/protocolVersion":    "2026-07-28",
			"io.modelcontextprotocol/clientCapabilities": map[string]any{},
			"io.modelcontextprotocol/clientInfo":         map[string]any{"name": "loadtest", "version": "1"},
		}
		h["MCP-Protocol-Version"], h["Mcp-Method"] = "2026-07-28", method
		if name, ok := params["name"].(string); ok {
			h["Mcp-Name"] = name
		}
	} else if method != "initialize" {
		h["MCP-Protocol-Version"] = "2025-06-18"
	}
	for k, v := range extra {
		h[k] = v
	}
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, _ := http.NewRequest("POST", *base+"/mcp", bytes.NewReader(b))
	for k, v := range h {
		req.Header.Set(k, v)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := doWithRetry(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
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
