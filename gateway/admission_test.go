package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeAdmissionStore struct {
	mu       sync.Mutex
	calls    int
	releases int
	class    admissionClass
	decision admissionDecision
	err      error
}

func (s *fakeAdmissionStore) acquire(_ context.Context, _, _, _, _ string, c admissionClass, _ int) (admissionDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.class = c
	return s.decision, s.err
}
func (s *fakeAdmissionStore) release(context.Context, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases++
	return nil
}
func (s *fakeAdmissionStore) sweep(context.Context) error { return nil }

func waitQueue(t *testing.T, s *admissionScheduler, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := s.queued
		s.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("queue did not reach expected size")
}
func TestAdmissionRoundRobinAndReservedControl(t *testing.T) {
	s := newAdmissionScheduler(1, 1, 8, 16)
	defer s.close()
	held, e := s.acquire(context.Background(), "noisy", admissionGeneral)
	if e != nil {
		t.Fatal(e)
	}
	type grant struct {
		org     string
		release func()
	}
	grants := make(chan grant, 3)
	enqueue := func(org string, n int) {
		go func() {
			release, e := s.acquire(context.Background(), org, admissionGeneral)
			if e == nil {
				grants <- grant{org, release}
			}
		}()
		waitQueue(t, s, n)
	}
	enqueue("noisy", 1)
	enqueue("noisy", 2)
	enqueue("quiet", 3)
	control, e := s.acquire(context.Background(), "quiet", admissionControl)
	if e != nil {
		t.Fatal("general work consumed reserved control", e)
	}
	control()
	held()
	first := <-grants
	if first.org != "noisy" {
		t.Fatal("unexpected queue order")
	}
	first.release()
	second := <-grants
	if second.org != "quiet" {
		t.Fatal("one org monopolized consecutive queued grants")
	}
	second.release()
	(<-grants).release()
}
func TestAdmissionQueueBoundsCancellationAndClose(t *testing.T) {
	s := newAdmissionScheduler(1, 1, 1, 4)
	held, _ := s.acquire(context.Background(), "a", admissionGeneral)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *apiError, 1)
	go func() { _, e := s.acquire(ctx, "a", admissionGeneral); done <- e }()
	waitQueue(t, s, 1)
	if _, e := s.acquire(context.Background(), "a", admissionGeneral); e == nil || e.Code != "local_queue_full" {
		t.Fatal("org queue unbounded", e)
	}
	cancel()
	if e := <-done; e == nil {
		t.Fatal("cancelled waiter admitted")
	}
	waitQueue(t, s, 0)
	go func() { _, e := s.acquire(context.Background(), "b", admissionGeneral); done <- e }()
	waitQueue(t, s, 1)
	s.close()
	if e := <-done; e == nil || e.Status != 503 {
		t.Fatal("shutdown waiter not refused", e)
	}
	held()
	held()
}
func TestAdmissionUncertainGrantFailsClosedAndReleases(t *testing.T) {
	store := &fakeAdmissionStore{err: errors.New("reply lost after grant")}
	c, e := newAdmissionController(store, defaultAdmissionConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer c.close()
	_, release, _, aerr := c.enter(context.Background(), &Agent{ID: "a", OrgID: "o"}, admissionGeneral)
	if release != nil || aerr == nil || aerr.Code != "admission_unavailable" || store.calls != 1 || store.releases != 1 {
		t.Fatalf("uncertain grant not contained: %+v %+v", aerr, store)
	}
	if c.scheduler.snapshot()["general"].(map[string]int)["active"] != 0 {
		t.Fatal("local slot leaked")
	}
}
func TestAdmissionDeadlineAndReleaseOnce(t *testing.T) {
	cfg := defaultAdmissionConfig()
	cfg.Execution = 30 * time.Millisecond
	store := &fakeAdmissionStore{decision: admissionDecision{Admitted: true, LeaseMS: 10030}}
	c, _ := newAdmissionController(store, cfg)
	defer c.close()
	ctx, release, _, e := c.enter(context.Background(), &Agent{ID: "a", OrgID: "o"}, admissionControl)
	if e != nil {
		t.Fatal(e)
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > cfg.Execution {
		t.Fatal("unbounded request")
	}
	<-ctx.Done()
	release()
	release()
	if store.releases != 1 {
		t.Fatal("release not idempotent")
	}
}
func TestAdmissionLocalAgentPolicyUpdatesAndLanes(t *testing.T) {
	g := &Gateway{}
	a := &Agent{ID: "a", RPS: 1, MaxConcurrency: 1}
	if !g.allow(a, admissionGeneral) || g.allow(a, admissionGeneral) {
		t.Fatal("initial token policy")
	}
	if !g.allow(a, admissionControl) {
		t.Fatal("general exhausted control tokens")
	}
	a.RPS = 100
	g.allow(a, admissionGeneral)
	v, _ := g.buckets.Load("a:general")
	if v.(*bucket).rate != 100 {
		t.Fatal("live rate not applied")
	}
	release, ok := g.acquireSlot(a, admissionGeneral)
	if !ok {
		t.Fatal("firstslot")
	}
	defer release()
	if _, ok = g.acquireSlot(a, admissionGeneral); ok {
		t.Fatal("concurrency bypass")
	}
	a.MaxConcurrency = 2
	second, ok := g.acquireSlot(a, admissionGeneral)
	if !ok {
		t.Fatal("live capacity not applied")
	}
	second()
	control, ok := g.acquireSlot(a, admissionControl)
	if !ok {
		t.Fatal("general occupied control")
	}
	control()
}
func TestAdmissionMCPClassificationUsesPayload(t *testing.T) {
	g, _, token := taskAPITestGateway()
	store := &fakeAdmissionStore{decision: admissionDecision{Admitted: false, Reason: "org_rate_limited", RetryAfterMS: 1}}
	c, _ := newAdmissionController(store, defaultAdmissionConfig())
	g.admission = c
	defer c.close()
	for _, tc := range []struct {
		body   string
		class  admissionClass
		status int
	}{
		{`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT 1"}}}`, admissionGeneral, 429},
		{`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"task_renew","arguments":{}}}`, admissionControl, 429},
		{`{`, "", 400},
	} {
		before := store.calls
		r := httptest.NewRequest("POST", "/mcp", strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Mcp-Name", "task_renew")
		w := httptest.NewRecorder()
		g.routes().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("HTTP%d: %s", w.Code, w.Body.String())
		}
		if tc.class == "" {
			if store.calls != before {
				t.Fatal("malformed envelope got a grant")
			}
		} else if store.class != tc.class {
			t.Fatal("header selected reserved lane")
		}
	}
}

func TestAdmissionConfigRequiresBothQueueLanes(t *testing.T) {
	t.Setenv("ADMISSION_ORG_QUEUE", "1")
	t.Setenv("ADMISSION_TOTAL_QUEUE", "1")
	if _, err := admissionConfigFromEnv(); err == nil {
		t.Fatal("one queue position cannot reserve both lanes")
	}
	t.Setenv("ADMISSION_TOTAL_QUEUE", "2")
	if _, err := admissionConfigFromEnv(); err != nil {
		t.Fatal(err)
	}
}
