// Background writers for the gateway's own bookkeeping: the server-side audit
// log and agents' last-seen times. Each is batched onto one admin connection,
// so under load they cost a few statements instead of one per request, and
// they never queue in front of token lookups on the same pool.
package main

import (
	"context"
	"log"
	"time"
)

const (
	auditQueue      = 10000
	auditBatch      = 500
	auditFlushEvery = 50 * time.Millisecond
	seenFlushEvery  = 5 * time.Second
)

type auditRecord struct {
	agentID, orgID, sql string
	params, rows        int
	bytes, affected     int64
	ms                  float64
	sqlstate, errmsg    *string
}

func (g *Gateway) startWriters() {
	g.auditCh = make(chan auditRecord, auditQueue)
	g.seenCh = make(chan string, auditQueue)
	go g.auditWriter()
	go g.seenWriter()
}

// audit queues a record of what the gateway executed for an agent. When the
// queue is full it waits rather than drop the record: the audit trail must be
// complete, so a stalled audit write slows agents down instead.
func (g *Gateway) audit(a *Agent, sql string, nparams, nrows int, nbytes, affected int64, ms float64, ae *apiError) {
	if g.auditCh == nil {
		return
	}
	r := auditRecord{agentID: a.ID, orgID: a.OrgID, sql: sql, params: nparams, rows: nrows,
		bytes: nbytes, affected: affected, ms: ms}
	if ae != nil {
		code, msg := ae.Code, ae.Message
		r.sqlstate, r.errmsg = &code, &msg
	}
	g.auditCh <- r
}

func (g *Gateway) auditWriter() {
	batch := make([]auditRecord, 0, auditBatch)
	tick := time.NewTicker(auditFlushEvery)
	defer tick.Stop()
	for {
		select {
		case r := <-g.auditCh:
			if batch = append(batch, r); len(batch) < auditBatch {
				continue
			}
		case <-tick.C:
			if len(batch) == 0 {
				continue
			}
		}
		g.flushAudit(batch)
		batch = batch[:0]
	}
}

// flushAudit writes a batch in one statement through record_audit, which
// hashes and truncates the SQL exactly as for a single record.
func (g *Gateway) flushAudit(batch []auditRecord) {
	n := len(batch)
	agents, orgs, sqls := make([]string, n), make([]string, n), make([]string, n)
	params, rows := make([]int32, n), make([]int32, n)
	bytes, affected := make([]int64, n), make([]int64, n)
	ms := make([]float64, n)
	sqlstates, errmsgs := make([]*string, n), make([]*string, n)
	for i, r := range batch {
		agents[i], orgs[i], sqls[i] = r.agentID, r.orgID, r.sql
		params[i], rows[i] = int32(r.params), int32(r.rows)
		bytes[i], affected[i], ms[i] = r.bytes, r.affected, r.ms
		sqlstates[i], errmsgs[i] = r.sqlstate, r.errmsg
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := g.admin.Exec(ctx, `
		SELECT swarmpg.record_audit(a, o, s, p, r, b, f, m, st, e)
		FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::int[], $5::int[], $6::bigint[], $7::bigint[],
		            $8::float8[], $9::text[], $10::text[]) AS t(a, o, s, p, r, b, f, m, st, e)`,
		agents, orgs, sqls, params, rows, bytes, affected, ms, sqlstates, errmsgs)
	if err != nil {
		g.stats.auditLost.Add(int64(n))
		log.Printf("audit: %d records could not be written: %v", n, err)
	}
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
			_, _ = g.admin.Exec(ctx, `UPDATE swarmpg.agents SET last_seen_at = now() WHERE id = ANY($1::uuid[])`, ids)
			cancel()
		}
	}
}
