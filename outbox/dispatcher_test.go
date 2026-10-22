package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testOrg = "11111111-1111-4111-8111-111111111111"
const testWorker = "22222222-2222-4222-8222-222222222222"

func testMessage() message {
	m := message{ID: "33333333-3333-4333-8333-333333333333", OrgID: testOrg, TaskID: "44444444-4444-4444-8444-444444444444", Key: "notify", Destination: "receiver", Payload: json.RawMessage(`{"z":9007199254740993,"a":null}`), State: "delivering", WorkerID: testWorker, Fence: "1", Attempts: 1, MaxAttempts: 10}
	m.IdempotencyKey = "swarmpg:" + m.OrgID + ":" + m.ID
	return m
}

func testConfig(gateway, endpoint string) config {
	return config{Gateway: gateway, Endpoint: endpoint, OrgID: testOrg, Destination: "receiver", WorkerID: testWorker, AdminKey: "admin-secret", TargetAuthorization: "Bearer receiver-secret", LeaseMS: 300, Concurrency: 1, PollInterval: time.Millisecond, RequestTimeout: time.Second}
}

type gatewayFixture struct {
	mu          sync.Mutex
	m           message
	acks        []string
	nacks       []map[string]any
	renews      int
	loseAck     bool
	loseLeaseAt int
}

func (f *gatewayFixture) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Admin-Key") != "admin-secret" || r.Header.Get("Authorization") != "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	data, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(data, &body)
	if body["org_id"] != testOrg || body["worker_id"] != testWorker || body["fence"] != f.m.Fence {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/renew"):
		f.renews++
		if f.loseLeaseAt > 0 && f.renews >= f.loseLeaseAt {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"code":"stale_claim"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(f.m)
	case strings.HasSuffix(r.URL.Path, "/ack"):
		f.acks = append(f.acks, string(data))
		f.m.State = "delivered"
		if f.loseAck && len(f.acks) == 1 {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(receipt{Message: &f.m, Replayed: len(f.acks) > 1})
	case strings.HasSuffix(r.URL.Path, "/nack"):
		f.nacks = append(f.nacks, body)
		f.m.State = "pending"
		if body["permanent"] == true || f.m.Attempts >= f.m.MaxAttempts {
			f.m.State = "failed"
		}
		_ = json.NewEncoder(w).Encode(f.m)
	default:
		http.NotFound(w, r)
	}
}

func makeFixture(t *testing.T, target http.HandlerFunc) (*dispatcher, *gatewayFixture) {
	t.Helper()
	f := &gatewayFixture{m: testMessage()}
	gateway := httptest.NewServer(http.HandlerFunc(f.serve))
	receiver := httptest.NewServer(target)
	t.Cleanup(gateway.Close)
	t.Cleanup(receiver.Close)
	d, err := newDispatcher(testConfig(gateway.URL, receiver.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.gateway.CloseIdleConnections)
	t.Cleanup(d.target.CloseIdleConnections)
	return d, f
}

func TestLostGatewayAckRetriesIdenticalDispositionWithoutResendingEffect(t *testing.T) {
	var sends atomic.Int32
	d, f := makeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		sends.Add(1)
		if r.Header.Get("X-Admin-Key") != "" || r.Header.Get("Authorization") != "Bearer receiver-secret" || r.Header.Get("Idempotency-Key") != testMessage().IdempotencyKey || r.Header.Get("X-SwarmPG-Delivery-Fence") != "1" {
			t.Error("wrong delivery headers or leaked admin credential")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("receiver-secret-response"))
	})
	f.loseAck = true
	if err := d.deliver(context.Background(), testMessage()); err != nil {
		t.Fatal(err)
	}
	if sends.Load() != 1 || len(f.acks) != 2 || f.acks[0] != f.acks[1] || strings.Contains(f.acks[0], "secret") {
		t.Fatalf("uncertain ack changed identity or resent effect: sends=%d acks=%v", sends.Load(), f.acks)
	}
	var ack map[string]any
	_ = json.Unmarshal([]byte(f.acks[0]), &ack)
	response := ack["response"].(map[string]any)
	if len(response) != 1 || response["status"] != float64(201) {
		t.Fatalf("persisted arbitrary receiver response: %v", response)
	}
}

func TestLostReceiverResponseRedeliversStableEnvelopeAndKey(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	effects := map[string]string{}
	d, f := makeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		key := r.Header.Get("Idempotency-Key")
		mu.Lock()
		defer mu.Unlock()
		bodies = append(bodies, string(data))
		if old, ok := effects[key]; ok && old != string(data) {
			t.Error("same idempotency key changed body")
		}
		effects[key] = string(data) // Atomic receiver-side dedup/effect model.
		if len(bodies) == 1 {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	if err := d.deliver(context.Background(), testMessage()); err != nil {
		t.Fatal(err)
	}
	if len(f.nacks) != 1 || f.nacks[0]["permanent"] != false || f.m.State != "pending" || len(f.acks) != 0 {
		t.Fatalf("uncertain receiver outcome incorrectly acknowledged: %+v", f)
	}
	f.m.State, f.m.Fence, f.m.Attempts = "delivering", "2", 2
	// Gateway payload formatting may differ on the subsequent receipt.
	f.m.Payload = json.RawMessage(`{ "a": null, "z": 9007199254740993 }`)
	if err := d.deliver(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if len(effects) != 1 || len(bodies) != 2 || bodies[0] != bodies[1] || len(f.acks) != 1 {
		t.Fatalf("delivery identity was not stable: effects=%v bodies=%v", effects, bodies)
	}
	var envelope map[string]json.RawMessage
	_ = json.Unmarshal([]byte(bodies[0]), &envelope)
	if len(envelope) != 7 || envelope["idempotency_key"] == nil || !strings.Contains(string(envelope["payload"]), "9007199254740993") {
		t.Fatalf("wrong envelope or lost integer precision: %s", bodies[0])
	}
}

func TestTransientRecoveryAndPermanentFailures(t *testing.T) {
	for _, status := range []int{408, 425, 429, 500, 503, 400, 401, 403, 404, 409, 302} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			d, f := makeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(status)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
			})
			if err := d.deliver(context.Background(), testMessage()); err != nil {
				t.Fatal(err)
			}
			transient := status == 408 || status == 425 || status == 429 || status >= 500
			if len(f.nacks) != 1 || f.nacks[0]["permanent"] != !transient || f.nacks[0]["retry_ms"] != float64(1000) {
				t.Fatalf("wrong classification: %v", f.nacks)
			}
			if transient {
				f.m.State, f.m.Fence, f.m.Attempts = "delivering", "2", 2
				if err := d.deliver(context.Background(), f.m); err != nil || len(f.acks) != 1 {
					t.Fatalf("retry did not recover: %v", err)
				}
			} else if f.m.State != "failed" {
				t.Fatal("permanent failure did not fail delivery")
			}
		})
	}
	if retryDelay(1) != 1000 || retryDelay(10) != 60000 || retryDelay(1000) != 60000 {
		t.Fatal("retry delay is not bounded")
	}
	d, f := makeFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	f.m.Attempts = 10
	if err := d.deliver(context.Background(), f.m); err != nil || f.m.State != "failed" || f.nacks[0]["retry_ms"] != float64(60000) {
		t.Fatalf("exhausted database retry cycle did not stop: state=%s err=%v", f.m.State, err)
	}
}

func TestLeaseLossCancelsReceiverAndNeverDisposesStaleClaim(t *testing.T) {
	cancelled := make(chan struct{})
	d, f := makeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		close(cancelled)
	})
	f.loseLeaseAt = 2
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.deliver(ctx, testMessage()); !ownershipLost(err) {
		t.Fatalf("expected lease loss: %v", err)
	}
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("lease loss did not cancel external request")
	}
	if len(f.acks) != 0 || len(f.nacks) != 0 {
		t.Fatal("stale dispatcher disposed the message")
	}
}

func TestExpiredReplayedClaimSendsNothing(t *testing.T) {
	var sends atomic.Int32
	d, f := makeFixture(t, func(w http.ResponseWriter, r *http.Request) { sends.Add(1) })
	f.loseLeaseAt = 1
	if err := d.deliver(context.Background(), testMessage()); !ownershipLost(err) || sends.Load() != 0 {
		t.Fatalf("stale receipt caused external delivery: err=%v sends=%d", err, sends.Load())
	}
}

func TestRedirectsCannotForwardCredentials(t *testing.T) {
	var leaks atomic.Int32
	trap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaks.Add(1) }))
	defer trap.Close()
	d, f := makeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, trap.URL, http.StatusTemporaryRedirect)
	})
	if err := d.deliver(context.Background(), testMessage()); err != nil || leaks.Load() != 0 || f.m.State != "failed" {
		t.Fatalf("receiver redirect followed: %v", err)
	}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, trap.URL, http.StatusTemporaryRedirect)
	}))
	defer gateway.Close()
	d.cfg.Gateway = gateway.URL
	err := d.gatewayCall(context.Background(), "/claim", []byte(`{}`), &receipt{})
	var failure *gatewayError
	if !errors.As(err, &failure) || failure.status != 307 || failure.retryable || leaks.Load() != 0 {
		t.Fatalf("gateway redirect followed or retried: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type endlessBody struct {
	read   int
	closed bool
}

type interruptedBody struct{}

func (interruptedBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (interruptedBody) Close() error             { return nil }

func (b *endlessBody) Read(p []byte) (int, error) { b.read += len(p); return len(p), nil }
func (b *endlessBody) Close() error               { b.closed = true; return nil }

func TestReceiverBodyIsBoundedAndTransportErrorsAreRedacted(t *testing.T) {
	d, err := newDispatcher(testConfig("http://localhost:1", "http://localhost:2/?secret=query-token"), nil)
	if err != nil {
		t.Fatal(err)
	}
	body := &endlessBody{}
	d.target.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})
	got := d.send(context.Background(), testMessage(), []byte(`{}`))
	if got.status != 200 || body.read != maxTargetBody+1 || !body.closed {
		t.Fatalf("receiver body was unbounded or not closed: %+v %+v", got, body)
	}
	d.target.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 403, Header: make(http.Header), Body: interruptedBody{}}, nil
	})
	if got := d.send(context.Background(), testMessage(), nil); !got.permanent || got.status != 403 {
		t.Fatalf("known permanent status became transient after body interruption: %+v", got)
	}
	d.target.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("receiver-secret query-token") })
	got = d.send(context.Background(), testMessage(), nil)
	if strings.Contains(got.errorText, "secret") || strings.Contains(got.errorText, "token") || got.permanent {
		t.Fatalf("transport error leaked data or became permanent: %+v", got)
	}
}

func TestUnknownClaimResponseKeepsRequestIdentityAcrossRetryBatches(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	resolved := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(data))
		n := len(bodies)
		mu.Unlock()
		if n <= gatewayTries {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":"unavailable"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"message":null,"replayed":true}`))
		if n == gatewayTries+1 {
			close(resolved)
		}
	}))
	defer server.Close()
	d, err := newDispatcher(testConfig(server.URL, "http://localhost:2"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.run(ctx) }()
	select {
	case <-resolved:
	case <-ctx.Done():
		t.Fatal("uncertain claim never retried across a retry batch")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, body := range bodies[:gatewayTries+1] {
		if body != bodies[0] {
			t.Fatal("uncertain claim advanced request identity")
		}
	}
}

func TestConfigurationAndProtocolValidation(t *testing.T) {
	for _, endpoint := range []string{"http://example.com", "https://user:password@example.com", "https://example.com/#fragment", "file:///tmp/receiver", "http://0.0.0.0/"} {
		cfg := testConfig("http://localhost:1", endpoint)
		if _, err := newDispatcher(cfg, nil); err == nil || strings.Contains(err.Error(), "password") {
			t.Fatalf("unsafe endpoint accepted or credentials exposed: %s %v", endpoint, err)
		}
	}
	d, _ := newDispatcher(testConfig("http://localhost:1", "https://example.com"), nil)
	for _, mutate := range []func(*message){
		func(m *message) { m.OrgID = testWorker },
		func(m *message) { m.Destination = "https://attacker.example" },
		func(m *message) { m.IdempotencyKey += "changed" },
		func(m *message) { m.MaxAttempts = 11 },
		func(m *message) { m.Payload = json.RawMessage(`null false`) },
	} {
		m := testMessage()
		mutate(&m)
		if _, err := d.envelope(m); err == nil {
			t.Fatal("inconsistent claim accepted")
		}
	}
	for _, raw := range []string{`{}`, `{"message":null}`, `{"message":null,"replayed":null}`} {
		if json.Unmarshal([]byte(raw), &receipt{}) == nil {
			t.Fatalf("ambiguous receipt accepted: %s", raw)
		}
	}
	if id, err := randomUUID(); err != nil || !validUUID(id) {
		t.Fatalf("invalid generated identity: %q %v", id, err)
	}
}

func TestUppercaseConfiguredUUIDsDeliverCanonicalDatabaseClaim(t *testing.T) {
	m := testMessage()
	m.OrgID = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	m.WorkerID = "fedcbafe-dcba-4fed-8cba-fedcbafedcba"
	m.IdempotencyKey = "swarmpg:" + m.OrgID + ":" + m.ID
	var sends atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sends.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["org_id"] != m.OrgID || body["worker_id"] != m.WorkerID {
			t.Error("gateway mutation did not use canonical UUIDs")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/renew") {
			_ = json.NewEncoder(w).Encode(m)
		} else if strings.HasSuffix(r.URL.Path, "/ack") {
			delivered := m
			delivered.State = "delivered"
			_ = json.NewEncoder(w).Encode(receipt{Message: &delivered})
		} else {
			http.NotFound(w, r)
		}
	}))
	defer gateway.Close()
	cfg := testConfig(gateway.URL, receiver.URL)
	cfg.OrgID, cfg.WorkerID = strings.ToUpper(m.OrgID), strings.ToUpper(m.WorkerID)
	d, err := newDispatcher(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.deliver(context.Background(), m); err != nil || sends.Load() != 1 {
		t.Fatalf("uppercase operator UUIDs rejected canonical database claim: sends=%d err=%v", sends.Load(), err)
	}
}
