package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func admissionPolicy(t *testing.T, f *sharedMemoryFixture, rps, burst, concurrency int) {
	t.Helper()
	p := map[string]int{"general_rps": rps, "general_burst": burst, "general_concurrency": concurrency, "control_rps": 100, "control_burst": 100, "control_concurrency": 2}
	raw, _ := json.Marshal(p)
	if _, err := f.g.admin.Exec(context.Background(), `SELECT swarmpg.admission_policy_set($1::uuid,$2::jsonb)`, f.agents[0].OrgID, string(raw)); err != nil {
		t.Fatal(err)
	}
}
func admissionSQL(t *testing.T, f *sharedMemoryFixture, gw, request, class string, ttl int) (admissionDecision, error) {
	t.Helper()
	s := &postgresAdmissionStore{pool: f.g.admin, gate: make(chan struct{}, 64)}
	return s.acquire(context.Background(), f.agents[0].OrgID, f.agents[0].ID, gw, request, admissionClass(class), ttl)
}
func admissionID(t *testing.T) string {
	t.Helper()
	id, e := auditID()
	if e != nil {
		t.Fatal(e)
	}
	return id
}

func TestAdmissionSQLSharedConcurrencyReplayAndPolicyIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	admissionPolicy(t, f, 1000, 1000, 3)
	type grant struct {
		id, gw string
		d      admissionDecision
		err    error
	}
	out := make(chan grant, 24)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, gw := admissionID(t), admissionID(t)
			d, e := admissionSQL(t, f, gw, id, "general", 10000)
			out <- grant{id, gw, d, e}
		}()
	}
	wg.Wait()
	close(out)
	var admitted []grant
	for g := range out {
		if g.err != nil {
			t.Fatal(g.err)
		}
		if g.d.Admitted {
			admitted = append(admitted, g)
		} else if g.d.Reason != "org_concurrency_limited" {
			t.Fatal(g.d)
		}
	}
	if len(admitted) != 3 {
		t.Fatalf("global concurrency=%d", len(admitted))
	}
	g := admitted[0]
	d, e := admissionSQL(t, f, g.gw, g.id, "general", 10000)
	if e != nil || !d.Replayed {
		t.Fatal("grant replay", d, e)
	}
	if _, e = admissionSQL(t, f, admissionID(t), g.id, "general", 10000); e == nil {
		t.Fatal("reused mismatched request")
	}
	admissionPolicy(t, f, 1000, 1000, 1)
	d, e = admissionSQL(t, f, admissionID(t), admissionID(t), "general", 10000)
	if e != nil || d.Admitted {
		t.Fatal("live reduction ignored", e)
	}
	d, e = admissionSQL(t, f, admissionID(t), admissionID(t), "control", 10000)
	if e != nil || !d.Admitted {
		t.Fatal("general used control quota", e)
	}
	var released bool
	for _, x := range admitted {
		if e := f.g.admin.QueryRow(context.Background(), `SELECT swarmpg.admission_release($1::uuid,$2::uuid,$3::uuid)`, f.agents[0].OrgID, x.gw, x.id).Scan(&released); e != nil || !released {
			t.Fatal("release", e)
		}
	}
	if e := f.g.admin.QueryRow(context.Background(), `SELECT swarmpg.admission_release($1::uuid,$2::uuid,$3::uuid)`, f.agents[0].OrgID, g.gw, g.id).Scan(&released); e != nil || released {
		t.Fatal("repeatrelease", e)
	}
}
func TestAdmissionSQLRateExpiryAndAuthorityIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	admissionPolicy(t, f, 1, 1, 1)
	id, gw := admissionID(t), admissionID(t)
	d, e := admissionSQL(t, f, gw, id, "general", 100)
	if e != nil || !d.Admitted {
		t.Fatal(e, d)
	}
	var released bool
	e = f.g.admin.QueryRow(context.Background(), `SELECT swarmpg.admission_release($1::uuid,$2::uuid,$3::uuid)`, f.agents[0].OrgID, admissionID(t), id).Scan(&released)
	if e != nil || released {
		t.Fatal("wrong gateway released", e)
	}
	time.Sleep(130 * time.Millisecond)
	if _, e = admissionSQL(t, f, gw, id, "general", 100); e == nil {
		t.Fatal("expired UUID revived")
	}
	admissionPolicy(t, f, 1, 1, 1)
	d, e = admissionSQL(t, f, gw, admissionID(t), "general", 100)
	if e != nil || d.Admitted || d.Reason != "org_rate_limited" {
		t.Fatal("policy refresh gave free burst", d, e)
	}
	if _, e = f.g.admin.Exec(context.Background(), `UPDATE swarmpg.admission_buckets SET refilled_at=clock_timestamp()-interval '2 seconds' WHERE org_id=$1::uuid`, f.agents[0].OrgID); e != nil {
		t.Fatal(e)
	}
	d, e = admissionSQL(t, f, gw, admissionID(t), "general", 100)
	if e != nil || !d.Admitted {
		t.Fatal("expired capacity not reclaimed", e)
	}
	if _, e = f.g.admin.Exec(context.Background(), `UPDATE swarmpg.agents SET disabled=true WHERE id=$1::uuid`, f.agents[0].ID); e != nil {
		t.Fatal(e)
	}
	if _, e = admissionSQL(t, f, gw, admissionID(t), "control", 100); e == nil {
		t.Fatal("disabled agent admitted")
	}
}
