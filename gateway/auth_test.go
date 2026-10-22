package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func checkAuthenticationResponse(t *testing.T, g *Gateway, transport, token string, status int, code string) {
	t.Helper()
	path, body := "/v1/tasks/claim", `{"queue":"auth-test","request_id":"00000000-0000-0000-0000-000000000001"}`
	if transport == "mcp" {
		path, body = "/mcp", `{"jsonrpc":"2.0","id":1,"method":"ping"}`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	g.routes().ServeHTTP(w, r)
	var out struct {
		Error struct {
			Code      any            `json:"code"`
			Retryable bool           `json:"retryable"`
			Data      map[string]any `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid authentication response: %v: %s", err, w.Body.String())
	}
	gotCode := out.Error.Code
	if transport == "mcp" {
		gotCode = out.Error.Data["code"]
		if out.Error.Code != float64(rpcServerError) {
			t.Fatalf("expected MCP admission error, got %s", w.Body.String())
		}
		if challenged := w.Header().Get("WWW-Authenticate") != ""; challenged != (status == http.StatusUnauthorized) {
			t.Fatalf("unexpected bearer challenge for HTTP %d: %v", w.Code, w.Header())
		}
	} else if status == http.StatusServiceUnavailable && !out.Error.Retryable {
		t.Fatalf("authentication outage is not retryable: %s", w.Body.String())
	}
	if w.Code != status || gotCode != code {
		t.Fatalf("want HTTP %d/%s, got HTTP %d: %s", status, code, w.Code, w.Body.String())
	}
}

func TestAuthenticationOutageIsNotInvalidCredentials(t *testing.T) {
	// Fail before dialing so the normal registry QueryRow/Scan path sees an
	// unavailable database without requiring or disrupting a running service.
	cfg, err := pgxpool.ParseConfig("postgres://postgres@127.0.0.1:1/auth_test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int64
	cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		attempts.Add(1)
		return errors.New("test registry unavailable: private database detail")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	g := &Gateway{admin: pool}
	for _, transport := range []string{"rest", "mcp"} {
		t.Run(transport, func(t *testing.T) {
			before := attempts.Load()
			checkAuthenticationResponse(t, g, transport, "", http.StatusUnauthorized, "unauthorized")
			if attempts.Load() != before {
				t.Fatal("missing credentials reached the unavailable registry")
			}
			checkAuthenticationResponse(t, g, transport, "uncached-token", http.StatusServiceUnavailable, "auth_unavailable")
			if attempts.Load() == before {
				t.Fatal("outage test did not exercise a registry lookup")
			}
		})
	}
}

func TestUnknownAuthenticationTokenRemainsUnauthorizedIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	// A successful registry query with no matching credential must retain its
	// authentication error, rather than being mistaken for database downtime.
	for _, transport := range []string{"rest", "mcp"} {
		t.Run(transport, func(t *testing.T) {
			checkAuthenticationResponse(t, f.g, transport, newToken(), http.StatusUnauthorized, "unauthorized")
		})
	}
}
