package main

import (
	"encoding/json"
	"testing"
	"time"
)

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
