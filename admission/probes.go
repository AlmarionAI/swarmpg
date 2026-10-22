package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

func (r *run) probes(ctx context.Context) error {
	o, err := decode[struct{ ID string }](r.call(ctx, agent{}, 0, "POST", "/v1/orgs", map[string]string{"name": "admission-probes"}, "", false))
	if err != nil {
		return err
	}
	a, err := decode[agent](r.call(ctx, agent{}, 0, "POST", "/v1/agents", map[string]any{"org_id": o.ID, "name": "probe", "rps_limit": 0, "max_concurrency": 32, "statement_timeout_ms": 0}, "", false))
	if err != nil {
		return err
	}
	p := policy{1000, 1000, 1, 100, 200, 2}
	record := func(name string, fn func() (map[string]any, error)) error {
		evidence, e := fn()
		item := probe{Name: name, Passed: e == nil, Evidence: evidence}
		if e != nil {
			item.Error = e.Error()
		}
		r.report.Probes = append(r.report.Probes, item)
		return e
	}
	query := func(replica int, sql string) response {
		return r.call(ctx, a, replica, "POST", "/v1/query", map[string]string{"sql": sql}, "", false)
	}
	if err = record("shared concurrency across three replicas", func() (map[string]any, error) {
		if e := r.setPolicy(ctx, o.ID, p); e != nil {
			return nil, e
		}
		var wg sync.WaitGroup
		outs := make([]response, 3)
		start := make(chan struct{})
		for i := range outs {
			wg.Add(1)
			go func(i int) { defer wg.Done(); <-start; outs[i] = query(i, "SELECT pg_sleep(0.7)") }(i)
		}
		close(start)
		wg.Wait()
		success, reject := 0, 0
		for _, out := range outs {
			if out.Status == 200 {
				success++
			} else if out.Status == 429 && out.Code == "org_concurrency_limited" {
				reject++
			} else {
				return nil, fmt.Errorf("unexpected concurrency response: %v", check(out))
			}
		}
		if success != 1 || reject != 2 {
			return nil, fmt.Errorf("success=%d rejected=%d", success, reject)
		}
		return map[string]any{"success": success, "org_rejected": reject}, nil
	}); err != nil {
		return err
	}
	if err = record("shared token bucket and no burst on policy reload", func() (map[string]any, error) {
		rate := p
		rate.GeneralRPS = 1
		rate.GeneralBurst = 1
		if e := r.setPolicy(ctx, o.ID, rate); e != nil {
			return nil, e
		}
		sleep(ctx, 1100*time.Millisecond)
		start := time.Now()
		if e := check(query(0, "SELECT 1")); e != nil {
			return nil, e
		}
		if e := r.setPolicy(ctx, o.ID, rate); e != nil {
			return nil, e
		}
		for i := 1; i < 3; i++ {
			out := query(i, "SELECT 1")
			if out.Status != 429 || out.Code != "org_rate_limited" {
				return nil, fmt.Errorf("expected shared rate rejection: %+v", out)
			}
		}
		if time.Since(start) >= time.Second {
			return nil, fmt.Errorf("rate probe too slow to prove one-second bucket")
		}
		return map[string]any{"success": 1, "rate_rejected": 2, "elapsed_ms": time.Since(start).Milliseconds()}, nil
	}); err != nil {
		return err
	}
	// Refill the isolated probe bucket explicitly before its next independent case.
	refill := func() error {
		_, e := r.sql(ctx, fmt.Sprintf("UPDATE swarmpg.admission_buckets SET tokens=1000,refilled_at=clock_timestamp() WHERE org_id='%s'", o.ID))
		return e
	}
	if err = record("live policy reduction retains running permits", func() (map[string]any, error) {
		cap2 := p
		cap2.GeneralConcurrency = 2
		if e := r.setPolicy(ctx, o.ID, cap2); e != nil {
			return nil, e
		}
		if e := refill(); e != nil {
			return nil, e
		}
		gateway := r.uuid()
		ids := []string{r.uuid(), r.uuid()}
		defer func() {
			for _, id := range ids {
				_, _ = r.sql(ctx, fmt.Sprintf("SELECT swarmpg.admission_release('%s','%s','%s')", o.ID, gateway, id))
			}
		}()
		for _, id := range ids {
			raw, e := r.sql(ctx, fmt.Sprintf("SELECT swarmpg.admission_acquire('%s','%s','%s','%s','general',13000)", o.ID, a.ID, gateway, id))
			if e != nil {
				return nil, e
			}
			var d struct{ Admitted bool }
			if json.Unmarshal(raw, &d) != nil || !d.Admitted {
				return nil, fmt.Errorf("probe grant failed %s", raw)
			}
		}
		if e := r.setPolicy(ctx, o.ID, p); e != nil {
			return nil, e
		}
		st, e := r.status(ctx, o.ID)
		if e != nil {
			return nil, e
		}
		if st.Active.General != 2 {
			return nil, fmt.Errorf("policy reduction removed active permits")
		}
		out := query(2, "SELECT 1")
		if out.Status != 429 || out.Code != "org_concurrency_limited" {
			return nil, fmt.Errorf("reduced policy admitted new work")
		}
		return map[string]any{"active_after_reduction": 2, "new_limit": 1}, nil
	}); err != nil {
		return err
	}
	if err = record("reserved control lane and MCP header spoof resistance", func() (map[string]any, error) {
		if e := r.setPolicy(ctx, o.ID, p); e != nil {
			return nil, e
		}
		if e := refill(); e != nil {
			return nil, e
		}
		t, e := decode[receipt](r.call(ctx, a, 0, "POST", "/v1/tasks", map[string]any{"queue": "probe", "key": r.uuid(), "payload": map[string]int{"a": 1}}, "", false))
		if e != nil {
			return nil, e
		}
		claimed, e := decode[receipt](r.call(ctx, a, 1, "POST", "/v1/tasks/claim", map[string]any{"queue": "probe", "request_id": r.uuid(), "lease_ms": 10000}, "", false))
		if e != nil || claimed.Task == nil {
			return nil, fmt.Errorf("claim: %v", e)
		}
		gateway, id := r.uuid(), r.uuid()
		_, e = r.sql(ctx, fmt.Sprintf("SELECT swarmpg.admission_acquire('%s','%s','%s','%s','general',13000)", o.ID, a.ID, gateway, id))
		if e != nil {
			return nil, e
		}
		defer r.sql(ctx, fmt.Sprintf("SELECT swarmpg.admission_release('%s','%s','%s')", o.ID, gateway, id))
		data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": "query", "arguments": map[string]string{"sql": "SELECT 1"}}})
		req, _ := http.NewRequestWithContext(ctx, "POST", r.cfg.Gateways[2]+"/mcp", bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+a.Token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Mcp-Name", "task_renew")
		res, e := r.client.Do(req)
		if e != nil {
			return nil, e
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 429 {
			return nil, fmt.Errorf("spoof escaped general cap: %d %s", res.StatusCode, body)
		}
		out := r.call(ctx, a, 2, "POST", "/v1/tasks/"+t.Task.ID+"/renew", map[string]any{"fence": claimed.Task.Fence, "lease_ms": 10000}, "", false)
		if e := check(out); e != nil {
			return nil, e
		}
		mcp := r.call(ctx, a, 1, "POST", "/mcp", map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": map[string]any{"name": "task_complete", "arguments": map[string]any{"id": t.Task.ID, "fence": claimed.Task.Fence, "result": map[string]int{"value": 1}}}}, "", false)
		if e := check(mcp); e != nil {
			return nil, e
		}
		var wire struct {
			Error  json.RawMessage
			Result struct {
				IsError bool `json:"isError"`
			}
		}
		if json.Unmarshal(mcp.Body, &wire) != nil || len(wire.Error) > 0 || wire.Result.IsError {
			return nil, fmt.Errorf("MCP completion: %s", mcp.Body)
		}
		return map[string]any{"spoof_status": 429, "rest_renew": 200, "mcp_complete": 200}, nil
	}); err != nil {
		return err
	}
	if err = record("quota dependency failure closes admission while database remains healthy", func() (map[string]any, error) {
		signature := "swarmpg.admission_acquire(uuid,uuid,uuid,uuid,text,integer)"
		definition, e := r.sql(ctx, "SELECT pg_get_functiondef('"+signature+"'::regprocedure)")
		if e != nil {
			return nil, e
		}
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, e := r.sql(restoreCtx, string(definition)); e != nil {
				r.fail("restore admission dependency: %v", e)
			}
		}()
		_, e = r.sql(ctx, `CREATE OR REPLACE FUNCTION swarmpg.admission_acquire(p_org uuid,p_agent uuid,p_gateway uuid,p_request uuid,p_class text,p_ttl_ms integer) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,swarmpg,pg_temp AS $$ BEGIN RAISE EXCEPTION 'injected owned admission dependency outage'; END $$`)
		if e != nil {
			return nil, e
		}
		outageKey := r.uuid()
		out := r.call(ctx, a, 1, "POST", "/v1/tasks", map[string]any{"queue": "outage", "key": outageKey, "payload": map[string]int{"value": 1}}, "", false)
		if out.Status != 503 || out.Code != "admission_unavailable" {
			return nil, fmt.Errorf("outage did not fail closed: %+v", out)
		}
		db, e := r.sql(ctx, "SELECT 1")
		if e != nil || string(db) != "1" {
			return nil, fmt.Errorf("database not independently healthy: %v", e)
		}
		rows, e := r.sql(ctx, fmt.Sprintf("SELECT count(*) FROM swarmpg.tasks WHERE org_id='%s' AND queue='outage' AND task_key='%s'", o.ID, outageKey))
		if e != nil || string(rows) != "0" {
			return nil, fmt.Errorf("write executed during quota failure: %s %v", rows, e)
		}
		return map[string]any{"http_status": 503, "database_select": 1, "writes_executed": 0}, nil
	}); err != nil {
		return err
	}
	return record("gateway death leaves a bounded reservation and expiry restores capacity", func() (map[string]any, error) {
		if e := refill(); e != nil {
			return nil, e
		}
		outCh := make(chan response, 1)
		go func() { outCh <- query(0, "SELECT pg_sleep(20)") }()
		deadline := time.Now().Add(5 * time.Second)
		active := false
		for time.Now().Before(deadline) {
			st, e := r.status(ctx, o.ID)
			if e != nil {
				return nil, e
			}
			if st.Active.General == 1 {
				active = true
				break
			}
			sleep(ctx, 20*time.Millisecond)
		}
		if !active {
			return nil, fmt.Errorf("no running reservation observed")
		}
		start := time.Now()
		if e := r.owned(ctx, "gateway"); e != nil {
			return nil, e
		}
		<-outCh
		st, e := r.status(ctx, o.ID)
		if e != nil {
			return nil, e
		}
		if st.Active.General != 1 {
			return nil, fmt.Errorf("crash did not leave reservation")
		}
		out := query(1, "SELECT 1")
		if out.Status != 429 {
			return nil, fmt.Errorf("stranded permit did not retain cap")
		}
		for time.Since(start) < 25*time.Second {
			st, e = r.status(ctx, o.ID)
			if e != nil {
				return nil, e
			}
			if st.Active.General == 0 {
				break
			}
			sleep(ctx, 100*time.Millisecond)
		}
		if st.Active.General != 0 {
			return nil, fmt.Errorf("crashed permit did not expire")
		}
		if e := check(query(2, "SELECT 1")); e != nil {
			return nil, e
		}
		return map[string]any{"stranded_active": 1, "capacity_recovered_ms": time.Since(start).Milliseconds()}, nil
	})
}
