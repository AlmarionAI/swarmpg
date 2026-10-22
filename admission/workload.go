package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

func (r *run) execute(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, r.cfg.Duration+45*time.Second)
	defer cancel()
	r.started = time.Now()
	snapshot := func() json.RawMessage {
		raw, e := r.sql(ctx, `SELECT json_build_object('at',clock_timestamp(),'connections',(SELECT count(*) FROM pg_stat_activity),'xact_commit',xact_commit,'xact_rollback',xact_rollback,'deadlocks',deadlocks,'temp_bytes',temp_bytes) FROM pg_stat_database WHERE datname=current_database()`)
		if e != nil {
			r.fail("database snapshot: %v", e)
		}
		return raw
	}
	r.report.DBBefore = snapshot()
	orgIDs := make([]string, 3)
	for i := range orgIDs {
		orgIDs[i] = r.report.Tenants[i].ID
	}
	before := make([]status, 3)
	for i, t := range r.report.Tenants {
		st, e := r.status(ctx, t.ID)
		if e != nil {
			r.fail("initial status: %v", e)
			return
		}
		before[i] = st
	}
	for i := range r.cfg.Gateways {
		out := r.call(ctx, agent{}, i, "GET", "/healthz", nil, "", false)
		if check(out) != nil {
			r.fail("health before: %v", check(out))
			return
		}
		r.report.GatewaysBefore = append(r.report.GatewaysBefore, out.Body)
	}
	var monitorWG sync.WaitGroup
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	monitorWG.Add(1)
	go func() {
		defer monitorWG.Done()
		for sleep(monitorCtx, 200*time.Millisecond) {
			for i, orgID := range orgIDs {
				st, e := r.status(monitorCtx, orgID)
				if e != nil {
					if monitorCtx.Err() == nil && time.Now().UnixNano() > r.faultUntil.Load() {
						r.fail("sample: %v", e)
					}
					continue
				}
				r.mu.Lock()
				r.report.Samples = append(r.report.Samples, sample{time.Now(), orgID, st.Active.General, st.Active.Control})
				r.report.Tenants[i].MaxGeneral = max(r.report.Tenants[i].MaxGeneral, st.Active.General)
				r.report.Tenants[i].MaxControl = max(r.report.Tenants[i].MaxControl, st.Active.Control)
				r.mu.Unlock()
			}
		}
	}()
	productionCtx, stopProduction := context.WithTimeout(ctx, r.cfg.Duration)
	defer stopProduction()
	var producers sync.WaitGroup
	for org := 1; org < 3; org++ {
		producers.Add(1)
		go func(org int) {
			defer producers.Done()
			for n := 0; productionCtx.Err() == nil; n++ {
				a := r.producers[org]
				key := r.uuid()
				out := r.call(ctx, a, n%3, "POST", "/v1/tasks", map[string]any{"queue": "sustained", "key": key, "payload": map[string]int{"a": n, "b": org}}, "general", true)
				receipt, e := decode[receipt](out)
				if e != nil || receipt.Task == nil {
					if ctx.Err() == nil {
						r.fail("enqueue org%d: %v", org, e)
					}
					return
				}
				t := receipt.Task
				r.mu.Lock()
				r.expected[t.ID] = expected{t.ID, t.OrgID, key, n + org}
				r.report.Tenants[org].Created++
				r.mu.Unlock()
				if !sleep(productionCtx, 200*time.Millisecond) {
					return
				}
			}
		}(org)
	}
	var workers sync.WaitGroup
	for i, a := range r.workers {
		workers.Add(1)
		go func(i int, a agent) {
			defer workers.Done()
			replica := i % 3
			// Every identity sends real work. The client has explicit backpressure; this
			// is thousands of active identities, not thousands of PostgreSQL sessions.
			out := r.call(ctx, a, replica, "POST", "/v1/query", map[string]string{"sql": "SELECT 1"}, "general", false)
			if ctx.Err() == nil && time.Now().UnixNano() > r.faultUntil.Load() && (out.Err != nil || (out.Status != 200 && out.Status != 429)) {
				r.fail("initial worker request: %v", check(out))
			}
			for ctx.Err() == nil {
				if a.Org == 0 {
					out = r.call(ctx, a, replica, "POST", "/v1/query", map[string]string{"sql": "SELECT pg_sleep(0.05)"}, "general", false)
					if ctx.Err() == nil && (out.Err != nil || (out.Status != 200 && out.Status != 429)) && time.Now().UnixNano() > r.faultUntil.Load() {
						r.fail("noisy request: %v", check(out))
					}
					if !sleep(ctx, time.Duration(300+i%200)*time.Millisecond) {
						return
					}
					replica = (replica + 1) % 3
					continue
				}
				if !take(ctx, r.claims[a.Org]) {
					return
				}
				out = r.call(ctx, a, replica, "POST", "/v1/tasks/claim", map[string]any{"queue": "sustained", "request_id": r.uuid(), "lease_ms": 3000}, "general", true)
				<-r.claims[a.Org]
				claimed, e := decode[receipt](out)
				if e != nil {
					if ctx.Err() != nil {
						return
					}
					if out.Status != 429 && time.Now().UnixNano() > r.faultUntil.Load() {
						r.fail("claim org%d: %v", a.Org, e)
					}
					sleep(ctx, time.Second)
					continue
				}
				if claimed.Task == nil {
					if !sleep(ctx, time.Duration(2000+i%2000)*time.Millisecond) {
						return
					}
					replica = (replica + 1) % 3
					continue
				}
				t := claimed.Task
				if !sleep(ctx, 750*time.Millisecond) {
					return
				}
				renewed := r.call(ctx, a, (replica+1)%3, "POST", "/v1/tasks/"+t.ID+"/renew", map[string]any{"fence": t.Fence, "lease_ms": 3000}, "control", true)
				if e := check(renewed); e != nil {
					r.mu.Lock()
					r.report.Tenants[a.Org].Expired++
					r.mu.Unlock()
					if renewed.Status != 409 && renewed.Status != 429 && ctx.Err() == nil {
						r.fail("renew: %v", e)
					}
					continue
				}
				r.mu.Lock()
				r.report.Tenants[a.Org].Renewed++
				r.mu.Unlock()
				if !sleep(ctx, 450*time.Millisecond) {
					return
				}
				completed := r.call(ctx, a, (replica+2)%3, "POST", "/v1/tasks/"+t.ID+"/complete", map[string]any{"fence": t.Fence, "result": map[string]int{"value": t.Payload.A + t.Payload.B}}, "control", true)
				receipt, e := decode[receipt](completed)
				if e != nil {
					if completed.Status != 409 && ctx.Err() == nil {
						r.fail("complete: %v", e)
					}
					continue
				}
				if receipt.Task == nil || receipt.Task.State != "completed" {
					r.fail("invalid completion receipt")
					continue
				}
				r.mu.Lock()
				r.completers[a.ID] = true
				r.report.Tenants[a.Org].Completed++
				r.report.Tenants[a.Org].completions = append(r.report.Tenants[a.Org].completions, float64(time.Since(t.Created).Microseconds())/1000)
				window := int(time.Since(r.started).Seconds() / 10)
				if window < len(r.report.Tenants[a.Org].Progress) {
					r.report.Tenants[a.Org].Progress[window]++
				}
				r.mu.Unlock()
			}
		}(i, a)
	}
	var faultWG sync.WaitGroup
	faultWG.Add(1)
	go func() {
		defer faultWG.Done()
		if !sleep(productionCtx, r.cfg.Duration/2) {
			return
		}
		start := time.Now()
		r.faultUntil.Store(start.Add(5 * time.Second).UnixNano())
		e := r.owned(ctx, "gateway")
		item := map[string]any{"type": "SIGKILL one of three gateways", "at_seconds": time.Since(r.started).Seconds(), "recovered_ms": time.Since(start).Milliseconds(), "passed": e == nil}
		if e != nil {
			r.fail("sustained gateway restart: %v", e)
			item["error"] = e.Error()
		}
		r.mu.Lock()
		r.report.Faults = append(r.report.Faults, item)
		r.mu.Unlock()
	}()
	<-productionCtx.Done()
	producers.Wait()
	faultWG.Wait()
	r.report.SustainedSeconds = time.Since(r.started).Seconds()
	// Stop generating new tasks, but allow already-created tasks to finish under
	// the same ceilings. The completion SLO includes their queueing time.
	for ctx.Err() == nil {
		r.mu.Lock()
		done := true
		for i := 1; i < 3; i++ {
			if r.report.Tenants[i].Completed < r.report.Tenants[i].Created {
				done = false
			}
		}
		r.mu.Unlock()
		if done {
			break
		}
		sleep(ctx, 100*time.Millisecond)
	}
	cancel()
	workers.Wait()
	stopMonitor()
	monitorWG.Wait()
	verifyCtx, stopVerify := context.WithTimeout(parent, 30*time.Second)
	defer stopVerify()
	elapsed := time.Since(r.started).Seconds()
	for i, t := range r.report.Tenants {
		st, e := r.status(verifyCtx, t.ID)
		if e != nil {
			r.fail("final status: %v", e)
			continue
		}
		general := st.Granted.General - before[i].Granted.General
		control := st.Granted.Control - before[i].Granted.Control
		r.report.Tenants[i].GrantedGeneral = general
		r.report.Tenants[i].GrantedControl = control
		if float64(general) > float64(t.Policy.GeneralBurst)+float64(t.Policy.GeneralRPS)*elapsed+1 {
			r.fail("%s global general token bound exceeded", t.Name)
		}
		if float64(control) > float64(t.Policy.ControlBurst)+float64(t.Policy.ControlRPS)*elapsed+1 {
			r.fail("%s global control token bound exceeded", t.Name)
		}
	}
	r.report.Settings["rate_verification_seconds"] = elapsed
	r.verifyTasks(verifyCtx)
	for i := range r.cfg.Gateways {
		out := r.call(verifyCtx, agent{}, i, "GET", "/healthz", nil, "", false)
		if check(out) != nil {
			r.fail("health after: %v", check(out))
		} else {
			r.report.GatewaysAfter = append(r.report.GatewaysAfter, out.Body)
		}
	}
	raw, e := r.sql(verifyCtx, `SELECT json_build_object('at',clock_timestamp(),'connections',(SELECT count(*) FROM pg_stat_activity),'xact_commit',xact_commit,'xact_rollback',xact_rollback,'deadlocks',deadlocks,'temp_bytes',temp_bytes) FROM pg_stat_database WHERE datname=current_database()`)
	if e != nil {
		r.fail("final database snapshot: %v", e)
	} else {
		r.report.DBAfter = raw
	}
}

func (r *run) verifyTasks(ctx context.Context) {
	for org := 1; org < 3; org++ {
		raw, e := r.sql(ctx, fmt.Sprintf(`SELECT coalesce(json_agg(json_build_object('id',t.id,'org_id',t.org_id,'key',t.task_key,'state',t.state,'value',t.result->'value','owner_valid',EXISTS(SELECT 1 FROM swarmpg.agents a WHERE a.id=t.owner_agent_id AND a.org_id=t.org_id),'completions',(SELECT count(*) FROM swarmpg.task_history h WHERE h.task_id=t.id AND h.event='completed'),'bad_history',(SELECT count(*) FROM swarmpg.task_history h WHERE h.task_id=t.id AND h.event='completed' AND (h.fence<>t.fence OR h.actor_agent_id<>t.owner_agent_id OR h.owner_agent_id<>t.owner_agent_id)),'fence',t.fence)),'[]'::json) FROM swarmpg.tasks t WHERE t.org_id='%s' AND t.queue='sustained'`, r.report.Tenants[org].ID))
		if e != nil {
			r.fail("verify org%d: %v", org, e)
			continue
		}
		var rows []struct {
			ID          string
			OrgID       string `json:"org_id"`
			Key, State  string
			Value       int
			OwnerValid  bool `json:"owner_valid"`
			Completions int
			BadHistory  int `json:"bad_history"`
			Fence       int64
		}
		if e = json.Unmarshal(raw, &rows); e != nil {
			r.fail("decode persisted results: %v", e)
			continue
		}
		seen := map[string]bool{}
		for _, row := range rows {
			expected, ok := r.expected[row.ID]
			if !ok || seen[row.ID] || row.Completions > 1 {
				r.report.Duplicates++
				continue
			}
			seen[row.ID] = true
			if row.State != "completed" {
				r.report.Lost++
				continue
			}
			if row.OrgID != expected.OrgID || row.Key != expected.Key || row.Value != expected.Value || !row.OwnerValid || row.Completions != 1 || row.BadHistory != 0 || row.Fence < 1 {
				r.report.Corrupt++
				continue
			}
			r.report.Tenants[org].Verified++
		}
		for id, e := range r.expected {
			if e.OrgID == r.report.Tenants[org].ID && !seen[id] {
				r.report.Lost++
			}
		}
	}
}
