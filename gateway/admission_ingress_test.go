package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ingressCountingBody struct{ reads atomic.Int32 }

func (b *ingressCountingBody) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.EOF }
func (*ingressCountingBody) Close() error               { return nil }

func TestAdmissionMCPRejectsBeforeReadingBody(t *testing.T) {
	for _, reason := range []string{"disabled", "expired", "agent ingress", "global ingress"} {
		t.Run(reason, func(t *testing.T) {
			g, a, token := taskAPITestGateway()
			want := 429
			var held []func()
			defer func() {
				for _, release := range held {
					release()
				}
			}()
			switch reason {
			case "disabled":
				a.Disabled = true
				want = 403
			case "expired":
				a.Expired = true
				want = 403
			case "agent ingress":
				for i := 0; i < 4; i++ {
					release, err := g.acquireMCPIngress(a)
					if err != nil {
						t.Fatal(err)
					}
					held = append(held, release)
				}
			case "global ingress":
				for i := 0; i < 64; i++ {
					copy := *a
					copy.ID = string(rune(i + 100))
					release, err := g.acquireMCPIngress(&copy)
					if err != nil {
						t.Fatal(err)
					}
					held = append(held, release)
				}
			}
			body := &ingressCountingBody{}
			r := httptest.NewRequest("POST", "/mcp", body)
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			g.handleMCP(w, r)
			if w.Code != want || body.reads.Load() != 0 {
				t.Fatalf("status=%d reads=%d body=%s", w.Code, body.reads.Load(), w.Body.String())
			}
			if want == 429 && w.Header().Get("Retry-After") == "" {
				t.Fatal("missing retry hint")
			}
		})
	}
}

func TestAdmissionMCPMalformedBodyReleasesIngress(t *testing.T) {
	g, _, token := taskAPITestGateway()
	for i := 0; i < 100; i++ {
		r := httptest.NewRequest("POST", "/mcp", strings.NewReader("{"))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		g.handleMCP(w, r)
		if w.Code != 400 {
			t.Fatalf("iteration %d: %d %s", i, w.Code, w.Body.String())
		}
	}
	if len(g.mcpIngress) != 0 {
		t.Fatal("parse failure leaked ingress slot")
	}
}

func TestAdmissionReadinessDoesNotGrantIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	count := func() int64 {
		var n int64
		if err := f.g.admin.QueryRow(context.Background(), `SELECT coalesce(sum(granted),0)::bigint FROM swarmpg.admission_buckets WHERE org_id=$1::uuid`, f.agents[0].OrgID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := count()
	w := httptest.NewRecorder()
	f.g.routes().ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 200 || count() != before {
		t.Fatalf("readiness granted work or failed: %d %s", w.Code, w.Body.String())
	}
	f.g.quota = nil
	w = httptest.NewRecorder()
	f.g.routes().ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 {
		t.Fatalf("unavailable quota was ready: %d", w.Code)
	}
}

func TestAdmissionRetainsPermitAfterLostRuntimeConnectionIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	admissionPolicy(t, f, 1000, 1000, 1)
	// Drop only this test's runtime socket and deny its subsequent cancel
	// connection. This models an asymmetric network failure while the database
	// remains reachable by the independent admission/admin connection.
	config := f.g.runtime.Config().Copy()
	config.MaxConns = 1
	dial := config.ConnConfig.DialFunc
	var disconnected atomic.Bool
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if disconnected.Load() {
			return nil, errors.New("test runtime network unavailable")
		}
		return dial(ctx, network, address)
	}
	runtime, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	f.g.runtime = runtime
	f.g.admission.cfg.Execution = 3 * time.Second
	ctx, release, _, aerr := f.g.admission.enter(context.Background(), f.agents[0], admissionGeneral)
	if aerr != nil {
		t.Fatal(aerr)
	}
	defer release()
	var pid uint32
	cut := make(chan error, 1)
	err = f.g.withConn(ctx, f.agents[0], func(ctx context.Context, tx pgx.Tx) error {
		pid = tx.Conn().PgConn().PID()
		wire := tx.Conn().PgConn().Conn()
		go func() {
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				var running bool
				if err := f.g.admin.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep')`, pid).Scan(&running); err != nil {
					cut <- err
					return
				}
				if running {
					disconnected.Store(true)
					cut <- wire.Close()
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			cut <- errors.New("runtime statement never entered pg_sleep")
		}()
		_, err := tx.Exec(ctx, "SELECT pg_sleep(2)")
		return err
	})
	if cutErr := <-cut; cutErr != nil {
		t.Fatal(cutErr)
	}
	if err == nil {
		t.Fatal("lost runtime connection unexpectedly succeeded")
	}
	release()
	var stillRunning bool
	if err := f.g.admin.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep')`, pid).Scan(&stillRunning); err != nil || !stillRunning {
		t.Fatalf("test did not leave an orphan statement: running=%v err=%v", stillRunning, err)
	}
	if f.g.admission.retainedUncertain.Load() != 1 {
		t.Fatal("uncertain backend was not retained")
	}
	_, nextRelease, _, aerr := f.g.admission.enter(context.Background(), f.agents[0], admissionGeneral)
	if nextRelease != nil {
		nextRelease()
	}
	if aerr == nil || aerr.Code != "org_concurrency_limited" {
		t.Fatalf("orphan statement lost concurrency coverage: %v", aerr)
	}
	if f.g.admission.scheduler.snapshot()["general"].(map[string]int)["active"] != 0 {
		t.Fatal("local slot leaked")
	}
	// Wait for the orphan to finish before advancing this test-owned lease's
	// timestamp, avoiding a 13-second sleep while exercising expiry recovery.
	deadline := time.Now().Add(4 * time.Second)
	for stillRunning && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if err := f.g.admin.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active')`, pid).Scan(&stillRunning); err != nil {
			t.Fatal(err)
		}
	}
	if stillRunning {
		t.Fatal("orphan statement did not stop within its bounded deadline")
	}
	if _, err := f.g.admin.Exec(context.Background(), `UPDATE swarmpg.admission_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE org_id=$1::uuid`, f.agents[0].OrgID); err != nil {
		t.Fatal(err)
	}
	_, recoveredRelease, _, aerr := f.g.admission.enter(context.Background(), f.agents[0], admissionGeneral)
	if aerr != nil {
		t.Fatal("expired orphan permit blocked recovery", aerr)
	}
	recoveredRelease()
}
