package main

import (
	"strings"
	"testing"
	"time"
)

func validReport() (report, config) {
	c := config{Workers: 2000, Duration: 20 * time.Second, P99MS: 10000}
	r := report{SustainedSeconds: 20, Probes: make([]probe, 6), Samples: []sample{{}}, Faults: []map[string]any{{"passed": true}}}
	for i := range r.Probes {
		r.Probes[i] = probe{Name: "probe", Passed: true}
	}
	for i, name := range []string{"noisy", "quiet-a", "quiet-b"} {
		n := 500
		if i == 0 {
			n = 1000
		}
		r.Tenants = append(r.Tenants, tenantReport{Name: name, Workers: n, Participants: n, CompletingWorkers: 1, Policy: policy{100, 100, 4, 100, 100, 2}, General: classReport{Rejected: map[string]int{"429:org_rate_limited": 1}}, Created: 20, Completed: 20, Verified: 20, Renewed: 20, Progress: []int{10, 10}, CompletionLatency: latency{P99: 1500}})
	}
	return r, c
}
func TestValidateRejectsMissingEvidenceAndCorrectnessFailures(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*report)
		want   string
	}{
		{"participant", func(r *report) { r.Tenants[0].Participants-- }, "not every worker"},
		{"concurrency", func(r *report) { r.Tenants[1].MaxGeneral = 5 }, "ceiling exceeded"},
		{"progress", func(r *report) { r.Tenants[1].Progress[1] = 0 }, "no completion"},
		{"latency", func(r *report) { r.Tenants[2].CompletionLatency.P99 = 10001 }, "p99"},
		{"lost", func(r *report) { r.Lost = 1 }, "correctness"},
		{"duplicate", func(r *report) { r.Duplicates = 1 }, "correctness"},
		{"corrupt", func(r *report) { r.Corrupt = 1 }, "correctness"},
		{"probe", func(r *report) { r.Probes[0].Passed = false }, "probe failed"},
		{"duration", func(r *report) { r.SustainedSeconds = 19 }, "duration"},
		{"fault", func(r *report) { r.Faults = nil }, "crash evidence"},
	}
	r, c := validReport()
	if errs := validate(r, c); len(errs) != 0 {
		t.Fatalf("valid report rejected: %v", errs)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, c := validReport()
			tc.mutate(&r)
			errs := validate(r, c)
			if !strings.Contains(strings.Join(errs, ";"), tc.want) {
				t.Fatalf("missing %q in %v", tc.want, errs)
			}
		})
	}
}
