package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMemoryReadSelectorsRejectInvalidCombinations(t *testing.T) {
	// A nil database proves every rejected request stops before query execution.
	g := &Gateway{}
	a := &Agent{ID: "00000000-0000-0000-0000-000000000001"}
	token := "selector-test-token"
	g.cache.Store(hex.EncodeToString(hashToken(token)), &cachedAgent{a: a, at: time.Now()})
	for _, query := range []string{
		"from=", "from=not-a-uuid", "from=00000000x0000x0000x0000x000000000001",
		"from=00000000-0000-0000-0000-000000000001&version=1",
		"from=00000000-0000-0000-0000-000000000001&as_of=2026-01-01T00:00:00Z",
		"version=1&as_of=2026-01-01T00:00:00Z", "version=0", "version=-1",
	} {
		t.Run("REST/"+query, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/memory/k?"+query, nil)
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			g.routes().ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", w.Code, w.Body.String())
			}
		})
	}
	for _, query := range []string{"from=", "version=1", "as_of=2026-01-01T00:00:00Z"} {
		t.Run("REST shared/"+query, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/memory/k/shared?"+query, nil)
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			g.routes().ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", w.Code, w.Body.String())
			}
		})
	}
	for _, args := range []string{
		`{"key":"k","from":""}`, `{"key":"k","from":"invalid"}`,
		`{"key":"k","from":"00000000-0000-0000-0000-000000000001","version":1}`,
		`{"key":"k","from":"00000000-0000-0000-0000-000000000001","as_of":"2026-01-01T00:00:00Z"}`,
		`{"key":"k","list_shared":true,"from":"00000000-0000-0000-0000-000000000001"}`,
		`{"key":"k","list_shared":true,"version":1}`,
		`{"key":"k","list_shared":true,"as_of":"2026-01-01T00:00:00Z"}`,
		`{"key":"","list_shared":true}`,
	} {
		t.Run("MCP/"+args, func(t *testing.T) {
			out := toolMemoryGet(g, context.Background(), a, json.RawMessage(args))
			if out["isError"] != true {
				t.Fatalf("want tool error, got %#v", out)
			}
			errorBody := out["structuredContent"].(map[string]any)["error"].(map[string]any)
			if errorBody["code"] != "bad_request" {
				t.Fatalf("want bad_request, got %#v", errorBody)
			}
		})
	}
}

type sharedMemoryFixture struct {
	g      *Gateway
	agents []*Agent
	tokens []string
}

// These tests opt in to an already-initialized disposable database. They never
// install schema, alter roles, or remove data outside their generated org IDs.
func newSharedMemoryFixture(t *testing.T) *sharedMemoryFixture {
	t.Helper()
	dsn := os.Getenv("SWARMPG_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SWARMPG_TEST_DATABASE_URL to a disposable admin database with sql/schema.sql installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var kernel bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_settings WHERE name = 'agent_principal')`).Scan(&kernel); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 2
	if kernel {
		// Startup defaults survive withConn's RESET ALL, just as login-role
		// defaults do in the production pool. All settings are connection-local.
		cfg.ConnConfig.RuntimeParams["agent_require_principal"] = "on"
		cfg.ConnConfig.RuntimeParams["agent_max_rows"] = "100001"
		cfg.ConnConfig.RuntimeParams["agent_max_cost"] = "1e9"
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET SESSION AUTHORIZATION agent_runtime")
		return err
	}
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	f := &sharedMemoryFixture{g: &Gateway{admin: admin, runtime: runtime, sealed: kernel,
		ceilRows: 100001, ceilCost: 1e9, principalSecret: []byte(os.Getenv("SWARMPG_TEST_PRINCIPAL_SECRET"))}}
	acfg := defaultAdmissionConfig()
	controller, err := newAdmissionController(&postgresAdmissionStore{pool: admin, gate: make(chan struct{}, 64)}, acfg)
	if err != nil {
		t.Fatal(err)
	}
	f.g.admission = controller
	f.g.quota = admin
	t.Cleanup(controller.close)
	var orgs []string
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"memory", "memory_history", "agents"} {
			if _, err := admin.Exec(ctx, "DELETE FROM swarmpg."+table+" WHERE org_id = ANY($1::uuid[])", orgs); err != nil {
				t.Errorf("cleanup %s: %v", table, err)
			}
		}
		if _, err := admin.Exec(ctx, "DELETE FROM swarmpg.orgs WHERE id = ANY($1::uuid[])", orgs); err != nil {
			t.Errorf("cleanup orgs: %v", err)
		}
	})
	for i := 0; i < 2; i++ {
		var id string
		if err := admin.QueryRow(ctx, "INSERT INTO swarmpg.orgs(name) VALUES('shared-memory-test') RETURNING id::text").Scan(&id); err != nil {
			t.Fatal(err)
		}
		orgs = append(orgs, id)
	}
	for i := 0; i < 5; i++ {
		org := orgs[0]
		if i == 4 {
			org = orgs[1]
		}
		token := newToken()
		a := &Agent{OrgID: org, MaxRows: 100, MaxResultBytes: 1 << 20, StatementTimeoutMS: 5000, MaxCost: 1e6}
		if err := admin.QueryRow(ctx, `INSERT INTO swarmpg.agents(org_id, name, token_hash)
			VALUES($1, 'shared-memory-test', $2) RETURNING id::text`, org, hashToken(token)).Scan(&a.ID); err != nil {
			t.Fatal(err)
		}
		f.agents, f.tokens = append(f.agents, a), append(f.tokens, token)
	}
	return f
}

func TestSharedMemoryProducersIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	g, ctx := f.g, context.Background()
	reader, first, second, private, foreign := f.agents[0], f.agents[1], f.agents[2], f.agents[3], f.agents[4]
	key := "same-key"
	put := func(a *Agent, value string, shared bool) {
		t.Helper()
		if _, err := g.memoryPut(ctx, a, key, json.RawMessage(value), shared, nil); err != nil {
			t.Fatalf("put %s: %+v", a.ID, err)
		}
	}
	get := func(a *Agent, from *string, version *int64, asOf *time.Time) *memoryState {
		t.Helper()
		m, err := g.memoryGet(ctx, a, key, version, asOf, from)
		if err != nil {
			t.Fatalf("get: %+v", err)
		}
		if m.ChangedAt == nil || m.ChangedAt.IsZero() {
			t.Fatal("missing changed_at")
		}
		return m
	}
	put(first, `"first-v1"`, true)
	put(second, `"second-v1"`, true)
	put(second, `"second-v2"`, true)
	put(private, `"private"`, false)
	put(foreign, `"foreign"`, true)
	if m := get(reader, nil, nil, nil); m.AgentID != second.ID || m.Version != 2 || m.Own {
		t.Fatalf("highest-version fallback: %+v", m)
	}
	put(first, `"first-v2"`, true)
	peers := []string{first.ID, second.ID}
	sort.Strings(peers)
	for i := 0; i < 3; i++ {
		if m := get(reader, nil, nil, nil); m.AgentID != peers[0] {
			t.Fatalf("stable UUID tie-break: %+v", m)
		}
	}
	one := int64(1)
	if _, err := g.memoryGet(ctx, reader, key, &one, nil, nil); err == nil || err.Status != 404 {
		t.Fatalf("shared history must stay private: %+v", err)
	}
	put(reader, `"own-v1"`, false)
	if m := get(reader, nil, nil, nil); !m.Own || m.AgentID != reader.ID || m.Version != 1 {
		t.Fatalf("own value must win over higher peer versions: %+v", m)
	}
	if m := get(reader, &second.ID, nil, nil); m.Own || m.AgentID != second.ID {
		t.Fatalf("explicit producer must override own fallback: %+v", m)
	}
	for _, from := range []string{strings.ToUpper(second.ID), strings.ReplaceAll(second.ID, "-", "")} {
		if m := get(reader, &from, nil, nil); m.AgentID != second.ID {
			t.Fatalf("producer UUID must return canonical provenance: %+v", m)
		}
	}
	if m := get(reader, &reader.ID, nil, nil); !m.Own || m.AgentID != reader.ID {
		t.Fatalf("own private value remains explicitly addressable: %+v", m)
	}
	for _, from := range []string{private.ID, foreign.ID, "00000000-0000-0000-0000-000000000000"} {
		if _, err := g.memoryGet(ctx, reader, key, nil, nil, &from); err == nil || err.Status != 404 {
			t.Fatalf("invisible producer %s must return 404: %+v", from, err)
		}
	}
	historical := get(reader, nil, &one, nil)
	if historical.AgentID != reader.ID || !historical.Own || string(historical.Value) != `"own-v1"` {
		t.Fatalf("historical provenance: %+v", historical)
	}
	if m := get(reader, nil, nil, historical.ChangedAt); m.AgentID != reader.ID || m.Version != 1 {
		t.Fatalf("as-of provenance: %+v", m)
	}
	shared, err := g.memoryShared(ctx, reader, key)
	if err != nil || len(shared.Memories) != 2 {
		t.Fatalf("shared listing: %+v, %+v", shared, err)
	}
	for i, m := range shared.Memories {
		if m.AgentID != peers[i] || m.Version != 2 || !m.Shared || m.Own || m.ChangedAt == nil {
			t.Fatalf("shared producer %d: %+v", i, m)
		}
	}
	// Lists do not quietly omit producers when an agent's budget is too small.
	limited := *reader
	limited.MaxRows = 2
	if out, err := g.memoryShared(ctx, &limited, key); err != nil || len(out.Memories) != 2 {
		t.Fatalf("exact row budget should succeed: %+v, %+v", out, err)
	}
	limited.MaxRows = 1
	if _, err := g.memoryShared(ctx, &limited, key); err == nil || err.Code != "result_limit_exceeded" {
		t.Fatalf("row budget: %+v", err)
	}
	limited = *reader
	limited.MaxResultBytes = 1
	if _, err := g.memoryShared(ctx, &limited, key); err == nil || err.Code != "result_limit_exceeded" {
		t.Fatalf("byte budget: %+v", err)
	}
	empty, err := g.memoryShared(ctx, reader, "absent-key")
	if err != nil || empty.Memories == nil || len(empty.Memories) != 0 {
		t.Fatalf("empty list: %+v, %+v", empty, err)
	}
	// Exercise the public REST route, including a producer UUID query selector.
	for _, path := range []string{
		"/v1/memory/" + key + "?from=" + url.QueryEscape(second.ID),
		"/v1/memory/" + key + "/shared",
	} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer "+f.tokens[0])
		w := httptest.NewRecorder()
		g.routes().ServeHTTP(w, r)
		if w.Code != 200 || !strings.Contains(w.Body.String(), second.ID) || !strings.Contains(w.Body.String(), "changed_at") {
			t.Fatalf("REST %s: %d %s", path, w.Code, w.Body.String())
		}
	}
	// The eighth-tool API remains intact: both operations use memory_get.
	for _, args := range []string{
		fmt.Sprintf(`{"key":%q,"from":%q}`, key, second.ID),
		fmt.Sprintf(`{"key":%q,"list_shared":true}`, key),
	} {
		out := toolMemoryGet(g, ctx, reader, json.RawMessage(args))
		if out["isError"] != false {
			t.Fatalf("MCP: %+v", out)
		}
		encoded, err := json.Marshal(out["structuredContent"])
		if err != nil || !strings.Contains(string(encoded), second.ID) || !strings.Contains(string(encoded), "changed_at") {
			t.Fatalf("MCP provenance: %s, %v", encoded, err)
		}
	}
	put(reader, `"own-shared"`, true)
	shared, err = g.memoryShared(ctx, reader, key)
	if err != nil || len(shared.Memories) != 3 {
		t.Fatalf("own shared producer must appear: %+v, %+v", shared, err)
	}
	// Sharing can be revoked; it must disappear from both selector and list.
	put(second, `"second-private"`, false)
	if _, err := g.memoryGet(ctx, reader, key, nil, nil, &second.ID); err == nil || err.Status != 404 {
		t.Fatalf("unshared producer: %+v", err)
	}
	shared, err = g.memoryShared(ctx, reader, key)
	if err != nil || len(shared.Memories) != 2 {
		t.Fatalf("listing after unshare: %+v, %+v", shared, err)
	}
}
