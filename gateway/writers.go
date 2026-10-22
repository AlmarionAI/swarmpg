// Background writers for audit receipts and last-seen bookkeeping.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	auditQueue      = 10000
	auditBatch      = 500
	auditFlushEvery = 50 * time.Millisecond
	seenFlushEvery  = 5 * time.Second
)

func (g *Gateway) startWriters() error {
	mode := env("AUDIT_MODE", "best-effort")
	if mode != "best-effort" && mode != "strict" {
		return fmt.Errorf("AUDIT_MODE must be best-effort or strict")
	}
	capacity, err := strconv.Atoi(env("AUDIT_SPOOL_CAPACITY", "10000"))
	if err != nil || capacity < 1 {
		return fmt.Errorf("AUDIT_SPOOL_CAPACITY must be a positive integer")
	}
	dir := os.Getenv("AUDIT_SPOOL_DIR")
	if mode == "strict" && dir == "" {
		return fmt.Errorf("strict audit requires AUDIT_SPOOL_DIR on persistent storage")
	}
	g.auditStrict = mode == "strict"
	g.auditSpool, err = newAuditSpool(dir, capacity)
	if err != nil {
		return err
	}
	g.seenCh = make(chan string, auditQueue)
	go g.auditWriter(context.Background())
	go g.seenWriter()
	return nil
}

func (g *Gateway) auditStatus() map[string]any {
	mode := "best-effort"
	if g.auditStrict {
		mode = "strict"
	}
	queued, capacity, durable := 0, 0, false
	if g.auditSpool != nil {
		queued = g.auditSpool.size()
		capacity = g.auditSpool.capacity
		durable = g.auditSpool.dir != ""
	}
	return map[string]any{"mode": mode, "queued": queued, "capacity": capacity, "durable_spool": durable}
}

// Admission does not wait for database delivery or free capacity. Spool access
// is serialized, and disk mode waits for local persistence. Best-effort drops a
// new receipt when full; strict mode refuses the query before it runs.
func (g *Gateway) beginAudit(a *Agent, sql string, params int) (*auditRecord, *apiError) {
	if g.auditSpool == nil {
		if !g.auditStrict {
			return nil, nil
		}
		return nil, auditUnavailable()
	}
	id, err := auditID()
	if err == nil {
		h := sha256.Sum256([]byte(sql))
		unknown, detail := "audit_outcome_unknown", "Request admitted; execution outcome was not durably recorded."
		r := auditRecord{ID: id, AgentID: a.ID, OrgID: a.OrgID, At: time.Now().UTC(),
			SQLHash: hex.EncodeToString(h[:]), SQLSample: boundedText(sql, 200), Params: params,
			SQLState: &unknown, Error: &detail}
		if err = g.auditSpool.reserve(r); err == nil {
			return &r, nil
		}
	}
	if g.auditStrict {
		return nil, auditUnavailable()
	}
	g.stats.auditLost.Add(1)
	return nil, nil
}

func auditUnavailable() *apiError {
	return &apiError{Status: 503, Code: "audit_unavailable", Message: "audit spool is unavailable or full; statement was not executed", Retryable: true}
}

func (g *Gateway) finishAudit(r *auditRecord, result *QueryResponse, failure *apiError, elapsed time.Duration) {
	if r == nil {
		return
	}
	r.MS = float64(elapsed.Microseconds()) / 1000
	r.SQLState, r.Error = nil, nil
	if failure != nil {
		code, msg := failure.Code, boundedText(failure.Message, 1000)
		r.SQLState, r.Error = &code, &msg
	}
	if result != nil {
		r.Rows, r.Bytes, r.Affected = result.RowCount, result.Bytes, result.Affected
	}
	if err := g.auditSpool.complete(*r); err != nil {
		// The durable intent remains. Keep the completed receipt in memory for
		// database retry; after a crash, recovery reports the outcome as unknown.
		// Do not turn a committed SQL result into an apparently safe retry.
		log.Printf("audit: could not persist completed receipt %s: %v", r.ID, err)
	}
}

func (g *Gateway) auditWriter(ctx context.Context) {
	delay := auditFlushEvery
	backoff := auditFlushEvery
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		batch := g.auditSpool.batch(auditBatch)
		if len(batch) == 0 {
			delay = auditFlushEvery
			continue
		}
		if err := g.flushAudit(ctx, batch); err != nil {
			g.stats.auditRetries.Add(1)
			log.Printf("audit: retaining %d receipts for retry: %v", len(batch), err)
			delay = backoff
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			continue
		}
		if err := g.auditSpool.ack(batch); err != nil {
			g.stats.auditRetries.Add(1)
			log.Printf("audit: receipt cleanup will retry: %v", err)
			delay = time.Second
			continue
		}
		delay, backoff = 0, auditFlushEvery
	}
}

// Stable receipt IDs make replay safe even when the database committed a batch
// but the connection failed before returning its acknowledgement.
func (g *Gateway) flushAudit(parent context.Context, batch []auditRecord) error {
	n := len(batch)
	ids, agents, orgs := make([]string, n), make([]string, n), make([]string, n)
	hashes, samples := make([]string, n), make([]string, n)
	at := make([]time.Time, n)
	params, rows := make([]int32, n), make([]int32, n)
	bytes, affected := make([]int64, n), make([]int64, n)
	ms := make([]float64, n)
	states, messages := make([]*string, n), make([]*string, n)
	for i, r := range batch {
		ids[i], agents[i], orgs[i], at[i] = r.ID, r.AgentID, r.OrgID, r.At
		// Normalize again at delivery so receipts recovered from an older
		// spool cannot block the entire batch with PostgreSQL-invalid text.
		hashes[i], samples[i] = r.SQLHash, boundedText(r.SQLSample, 200)
		params[i], rows[i], bytes[i], affected[i], ms[i] = int32(r.Params), int32(r.Rows), r.Bytes, r.Affected, r.MS
		if r.SQLState != nil {
			state := boundedText(*r.SQLState, 64)
			states[i] = &state
		}
		if r.Error != nil {
			message := boundedText(*r.Error, 1000)
			messages[i] = &message
		}
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	err := g.adminExec(ctx, `
        SELECT swarmpg.record_audit_receipt(i, a, o, t, h, s, p, r, b, f, m, st, e)
        FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::timestamptz[], $5::text[], $6::text[],
                    $7::int[], $8::int[], $9::bigint[], $10::bigint[], $11::float8[], $12::text[], $13::text[])
             AS x(i, a, o, t, h, s, p, r, b, f, m, st, e)`,
		ids, agents, orgs, at, hashes, samples, params, rows, bytes, affected, ms, states, messages)
	return err
}

// seenWriter records last-seen times every few seconds, in one statement for
// every agent seen since the last flush.
func (g *Gateway) seenWriter() {
	pending := map[string]bool{}
	tick := time.NewTicker(seenFlushEvery)
	defer tick.Stop()
	for {
		select {
		case id := <-g.seenCh:
			pending[id] = true
		case <-tick.C:
			if len(pending) == 0 {
				continue
			}
			ids := make([]string, 0, len(pending))
			for id := range pending {
				ids = append(ids, id)
			}
			clear(pending)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = g.adminExec(ctx, `UPDATE swarmpg.agents SET last_seen_at = now() WHERE id = ANY($1::uuid[])`, ids)
			cancel()
		}
	}
}
