package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestIdlePollingBacksOffWithReproducibleJitter(t *testing.T) {
	a, b := rand.New(rand.NewSource(42)), rand.New(rand.NewSource(42))
	var highStreakVaried bool
	var previous time.Duration
	for i := 1; i <= 30; i++ {
		delay := emptyPollDelay(i, a)
		if delay != emptyPollDelay(i, b) || delay < 100*time.Millisecond || delay > 2*time.Second {
			t.Fatalf("invalid or nondeterministic delay: %s", delay)
		}
		if i == 1 && delay > 200*time.Millisecond {
			t.Fatal("first empty claim used maximum backoff")
		}
		if i > 6 && delay != previous {
			highStreakVaried = true
		}
		previous = delay
	}
	if !highStreakVaried || emptyPollDelay(1, a) > 200*time.Millisecond {
		t.Fatal("backoff did not jitter at cap or reset after work")
	}
}

func workerTestRun(server *httptest.Server, leaseMS int) *run {
	return &run{
		cfg: config{Gateway: server.URL, LeaseMS: leaseMS}, client: server.Client(), sem: make(chan struct{}, 8), claimSem: make(chan struct{}, 4),
		completed: map[string]bool{}, completers: map[string]bool{}, participants: map[string]bool{},
		report: report{HTTPOutcomes: map[string]int64{}, Tenants: []tenantReport{{}}},
	}
}

func TestLockTimeoutRetriesExactBodyOnlyDuringFaultWindow(t *testing.T) {
	for _, operation := range []struct {
		name, path string
		body       map[string]any
	}{
		{"claim", "/v1/tasks/claim", map[string]any{"request_id": "fixed-request", "queue": "work", "lease_ms": 3000}},
		{"complete", "/v1/tasks/task/complete", map[string]any{"fence": "9007199254740993", "result": result{42}}},
	} {
		for _, window := range []string{"active", "absent", "expired"} {
			t.Run(operation.name+"/"+window, func(t *testing.T) {
				var mu sync.Mutex
				var requests []string
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					body, _ := io.ReadAll(req.Body)
					mu.Lock()
					requests = append(requests, string(body))
					n := len(requests)
					mu.Unlock()
					if n == 1 {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = w.Write([]byte(`{"error":{"code":"55P03"}}`))
					} else {
						_, _ = w.Write([]byte(`{}`))
					}
				}))
				defer server.Close()
				r := workerTestRun(server, 3000)
				if window == "active" {
					r.faultUntil.Store(time.Now().Add(time.Second).UnixNano())
					r.report.Faults = []fault{{Kind: "database", StartedAt: time.Now()}}
				} else if window == "expired" {
					r.faultUntil.Store(time.Now().Add(-time.Second).UnixNano())
				}
				out, err := r.call(context.Background(), agent{ID: "worker"}, "POST", operation.path, operation.body, false, true)
				mu.Lock()
				defer mu.Unlock()
				if window == "active" {
					if err != nil || out.Status != 200 || len(requests) != 2 || requests[0] != requests[1] || r.report.RequestRetries != 1 {
						t.Fatalf("fault retry changed request or failed: response=%+v error=%v requests=%v", out, err, requests)
					}
				} else {
					_, decodeErr := decodeOK[map[string]any](out, err)
					if decodeErr == nil || out.Status != 400 || len(requests) != 1 || r.report.RequestRetries != 0 {
						t.Fatalf("ordinary lock timeout was masked: response=%+v error=%v requests=%v", out, err, requests)
					}
				}
			})
		}
	}
}

func TestSaturatedClaimAdmissionLeavesCapacityForLeaseHolders(t *testing.T) {
	const claimLimit = 16
	entered := make(chan struct{}, 1000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v1/tasks/claim" {
			entered <- struct{}{}
			<-req.Context().Done()
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	r := workerTestRun(server, 3000)
	r.sem, r.claimSem = make(chan struct{}, 128), make(chan struct{}, claimLimit)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = r.call(ctx, agent{ID: fmt.Sprint(i)}, "POST", "/v1/tasks/claim", nil, false, false)
		}(i)
	}
	for i := 0; i < claimLimit; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("claim admission did not reach its cap")
		}
	}
	for _, operation := range []string{"renew", "complete"} {
		out, err := r.call(ctx, agent{}, "POST", "/v1/tasks/task/"+operation, nil, false, false)
		if err != nil || out.Status != 200 {
			t.Fatalf("%s starved behind idle claimers: HTTP%d %v", operation, out.Status, err)
		}
	}
	r.mu.Lock()
	participants := len(r.participants)
	r.mu.Unlock()
	if participants != claimLimit || len(r.sem) != claimLimit || len(entered) != 0 {
		t.Fatalf("waiting claimers consumed shared capacity or counted as participants: participants=%d in-flight=%d extra-admitted=%d", participants, len(r.sem), len(entered))
	}
	cancel()
	wg.Wait()
	if len(r.sem) != 0 || len(r.claimSem) != 0 {
		t.Fatal("cancelled claims leaked admission capacity")
	}
	if len(r.participants) != claimLimit {
		t.Fatal("cancelled waiters were counted as participating workers")
	}
}

func TestHeartbeatStopFinishesInflightRenewal(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		close(entered)
		select {
		case <-release:
			_ = json.NewEncoder(w).Encode(task{ID: "task", Owner: "worker", Fence: "1", State: "claimed"})
		case <-req.Context().Done():
		}
	}))
	defer server.Close()
	r := workerTestRun(server, 3000)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, stop := r.maintainLease(ctx, agent{ID: "worker"}, &task{ID: "task", Owner: "worker", Fence: "1"})
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("heartbeat did not start")
	}
	stopped := make(chan bool, 1)
	go func() { stopped <- stop() }()
	select {
	case <-stopped:
		t.Fatal("stop cancelled a renewal already in flight")
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case lost := <-stopped:
		if lost || r.report.LeaseRenewals != 1 || len(r.report.Errors) != 0 {
			t.Fatalf("graceful heartbeat stop failed: lost=%v report=%+v", lost, r.report)
		}
	case <-ctx.Done():
		t.Fatal("heartbeat stop did not join after renewal")
	}
}

func TestHealthyWorkerRenewsAcrossSlowInputRead(t *testing.T) {
	const leaseMS = 150
	var mu sync.Mutex
	expires := time.Now().Add(leaseMS * time.Millisecond)
	claimed := task{ID: "task", OrgID: "org", Owner: "worker", Fence: "1", State: "claimed", Attempts: 1,
		Payload: payload{Add: 2, Ordinal: 1}, Inputs: []input{{AgentID: "producer", Key: "slow", Version: 1}}, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	var completed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v1/memory/slow" {
			time.Sleep(350 * time.Millisecond) // Longer than two original leases.
			_ = json.NewEncoder(w).Encode(map[string]any{"agent_id": "producer", "version": 1, "value": result{7}})
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch req.URL.Path {
		case "/v1/tasks/task/renew":
			if completed || time.Now().After(expires) {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":{"code":"lease_expired"}}`))
				return
			}
			expires = time.Now().Add(leaseMS * time.Millisecond)
			renewed := claimed
			renewed.LeaseUntil = &expires
			_ = json.NewEncoder(w).Encode(renewed)
		case "/v1/tasks/task/complete":
			if time.Now().After(expires) {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":{"code":"lease_expired"}}`))
				return
			}
			completed = true
			finished := claimed
			finished.State, finished.Result = "completed", json.RawMessage(`{"value":9}`)
			_ = json.NewEncoder(w).Encode(receipt{Task: &finished})
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	r := workerTestRun(server, leaseMS)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r.processTask(ctx, agent{ID: "worker", Token: "token"}, &claimed)
	if !r.completed["task"] || r.report.LeaseRenewals < 3 || len(r.report.Errors) != 0 {
		t.Fatalf("healthy slow task did not keep its lease: completed=%v report=%+v", r.completed, r.report)
	}
	before := r.report.LeaseRenewalAttempts
	time.Sleep(100 * time.Millisecond)
	if r.report.LeaseRenewalAttempts != before {
		t.Fatal("heartbeat continued after completion")
	}
}

func TestPausedWorkerDoesNotRenewAndStaleLeaseStopsHealthyWork(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"lease_expired"}}`))
	}))
	defer server.Close()
	r := workerTestRun(server, 100)
	a := agent{ID: "worker", Token: "token"}
	claimed := task{ID: "task", Owner: "worker", Fence: "1", State: "claimed", Attempts: 1, Payload: payload{Ordinal: 29}}
	r.processTask(context.Background(), a, &claimed)
	if r.report.PausedWorkers != 1 || r.report.LeaseRenewalAttempts != 0 || r.report.StaleDenials != 1 || len(r.report.Errors) != 0 {
		t.Fatalf("paused-worker fault was masked: %+v", r.report)
	}
	ctx, stop := r.maintainLease(context.Background(), a, &claimed)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("lease loss did not cancel task work")
	}
	if !stop() || r.report.LeaseRenewalDenials != 1 || len(r.report.Errors) != 0 {
		t.Fatalf("lost lease did not stop work cleanly: %+v", r.report)
	}
}

func TestValidateTaskRejectsCorruptResults(t *testing.T) {
	want := expectedTask{Task: task{ID: "task", OrgID: "org"}, Value: 42, Tenant: 0}
	good := task{ID: "task", OrgID: "org", State: "completed", Owner: "worker", Fence: "2", Attempts: 2, Result: json.RawMessage(`{"value":42}`)}
	workers := map[string]int{"worker": 0, "foreign": 1}
	if err := validateTask(want, good, workers); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*task)
	}{
		{"lost work", func(v *task) { v.State = "claimed" }},
		{"stale overwrite", func(v *task) { v.Result = json.RawMessage(`{"value":41}`) }},
		{"foreign attribution", func(v *task) { v.Owner = "foreign" }},
		{"unknown attribution", func(v *task) { v.Owner = "unknown" }},
		{"foreign organization", func(v *task) { v.OrgID = "other" }},
		{"invalid fence", func(v *task) { v.Fence = "0" }},
		{"missing claim", func(v *task) { v.Attempts = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := good
			tc.edit(&bad)
			if err := validateTask(want, bad, workers); err == nil {
				t.Fatal("corrupt persisted state was accepted")
			}
		})
	}
}

func TestValidateHistoryRejectsDuplicateCompletion(t *testing.T) {
	task := task{ID: "task", Owner: "worker", Fence: "2"}
	good := []historyEvent{{Revision: 1, Event: "enqueued", Fence: "0"}, {Revision: 2, Event: "claimed", AgentID: "worker", Owner: "worker", Fence: "2"}, {Revision: 3, Event: "completed", AgentID: "worker", Owner: "worker", Fence: "2"}}
	if _, err := validateHistory(task, good); err != nil {
		t.Fatal(err)
	}
	duplicate := append(append([]historyEvent(nil), good...), historyEvent{Revision: 4, Event: "completed", AgentID: "worker", Fence: "2"})
	if _, err := validateHistory(task, duplicate); err == nil {
		t.Fatal("duplicate committed completion accepted")
	}
	if _, err := validateHistory(task, good[:2]); err == nil {
		t.Fatal("missing completion history accepted")
	}
	bad := append([]historyEvent(nil), good...)
	bad[2].Fence = "1"
	if _, err := validateHistory(task, bad); err == nil {
		t.Fatal("stale fence completion history accepted")
	}
}

func TestValidateHistoryRejectsOverlappingOwners(t *testing.T) {
	now := time.Now()
	expires := now.Add(time.Minute)
	events := []historyEvent{
		{Revision: 1, Event: "enqueued", Fence: "0"},
		{Revision: 2, Event: "claimed", AgentID: "a", Owner: "a", Fence: "1", At: now, LeaseUntil: &expires},
		{Revision: 3, Event: "claimed", AgentID: "b", Owner: "b", Fence: "2", At: now.Add(time.Second), LeaseUntil: &expires},
		{Revision: 4, Event: "completed", AgentID: "b", Owner: "b", Fence: "2"},
	}
	if _, err := validateHistory(task{ID: "task", Owner: "b", Fence: "2"}, events); err == nil {
		t.Fatal("overlapping current claims accepted")
	}
}

func TestQuantilesAndFencePrecision(t *testing.T) {
	got := quantiles([]float64{10, 1, 4, 2, 3})
	if got["p50"] != 3 || got["p95"] != 10 || got["p99"] != 10 {
		t.Fatalf("wrong nearest-rank quantiles: %+v", got)
	}
	if !fenceGreater("9007199254740993", "9007199254740992") || fenceGreater("2", "2") || fenceGreater("bad", "1") {
		t.Fatal("fence comparison lost integer precision or accepted invalid data")
	}
}

func TestDatabaseDisconnectCodesAreNarrow(t *testing.T) {
	for _, code := range []string{"08006", "08001", "57P01", "57P02", "57P03"} {
		if !databaseDisconnect(code) {
			t.Fatalf("disconnect %s was not classified", code)
		}
	}
	for _, code := range []string{"", "unauthorized", "bad_request", "42501", "40001", "57P04"} {
		if databaseDisconnect(code) {
			t.Fatalf("unrelated failure %s was masked", code)
		}
	}
}
