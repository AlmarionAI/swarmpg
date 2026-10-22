package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQueryProbeRequiresIntendedDenial(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		status     int
		body       string
		pass       bool
	}{
		{"budget denial", "reject:53400", 400, `{"error":{"code":"53400"}}`, true},
		{"privilege denial", "reject:42501", 403, `{"error":{"code":"42501"}}`, true},
		{"guard denial", "reject:bad_request", 400, `{"error":{"code":"bad_request"}}`, true},
		{"wrong denial", "reject:53400", 400, `{"error":{"code":"42601"}}`, false},
		{"server failure with matching code", "reject:53400", 500, `{"error":{"code":"53400"}}`, false},
		{"wrong HTTP status", "reject:42501", 400, `{"error":{"code":"42501"}}`, false},
		{"success carrying error", "reject:53400", 200, `{"error":{"code":"53400"}}`, false},
		{"unqualified reject", "reject", 400, `{"error":{"code":"42501"}}`, false},
		{"empty rejection code", "reject:", 400, `{"error":{"code":""}}`, false},
		{"visible peer row", "zero", 200, `{"row_count":1,"rows":[[1]]}`, false},
		{"no peer rows", "zero", 200, `{"row_count":0,"rows":[]}`, true},
		{"legitimate write", "allow", 200, `{"row_count":0,"rows":[]}`, true},
		{"malformed success", "allow", 200, `not json`, false},
		{"empty success", "zero", 200, `{}`, false},
		{"transport failure", "allow", 0, `connection reset`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, detail := assessQueryProbe(tc.want, tc.status, []byte(tc.body)); got != tc.pass {
				t.Fatalf("pass=%v, want %v: %s", got, tc.pass, detail)
			}
		})
	}
}

func TestServerErrorsFailRunThroughRESTAndMCP(t *testing.T) {
	oldBase, oldClient := *base, client
	oldFailures, oldRetry := httpFailures.Load(), retry429.Load()
	t.Cleanup(func() {
		*base, client = oldBase, oldClient
		httpFailures.Store(oldFailures)
		retry429.Store(oldRetry)
	})
	retry429.Store(false)
	for _, status := range []int{400, 500, 503, 504} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"code":"53400"}}`))
		}))
		*base, client = server.URL, server.Client()
		for _, protocol := range []string{"REST", "MCP"} {
			httpFailures.Store(0)
			if protocol == "REST" {
				call("POST", "/v1/query", "test", nil, false)
			} else {
				mcpRPC("test", true, "tools/list", nil, nil)
			}
			if got, want := runFailed(0, 0), status >= 500; got != want {
				t.Errorf("%s HTTP %d: runFailed=%v, want %v", protocol, status, got, want)
			}
		}
		server.Close()
	}
}

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connection reset")
}

func TestTransportAndLoadFailuresFailRun(t *testing.T) {
	oldClient, oldFailures := client, httpFailures.Load()
	t.Cleanup(func() { client = oldClient; httpFailures.Store(oldFailures) })
	client = &http.Client{Transport: failingTransport{}}
	httpFailures.Store(0)
	req, _ := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	if _, err := doWithRetry(req); err == nil {
		t.Fatal("expected a transport error")
	}
	if !runFailed(0, 0) {
		t.Fatal("ignored transport failure must still fail the run")
	}
	httpFailures.Store(0)
	if !runFailed(1, 0) || !runFailed(0, 1) || runFailed(0, 0) {
		t.Fatal("run must fail for load or probe failures and pass with neither")
	}
}
