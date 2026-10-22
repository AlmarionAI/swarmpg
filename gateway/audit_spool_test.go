package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func auditTestAgent() *Agent {
	return &Agent{ID: "00000000-0000-0000-0000-000000000001", OrgID: "00000000-0000-0000-0000-000000000002"}
}

func auditTestSpool(t *testing.T, dir string, capacity int) *auditSpool {
	t.Helper()
	s, err := newAuditSpool(dir, capacity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	return s
}

func TestAuditSpoolDurableRecovery(t *testing.T) {
	dir := t.TempDir()
	s := auditTestSpool(t, dir, 3)
	g := &Gateway{auditSpool: s, auditStrict: true}
	completed, err := g.beginAudit(auditTestAgent(), "SELECT $1", 1)
	if err != nil {
		t.Fatal(err)
	}
	interrupted, err := g.beginAudit(auditTestAgent(), "UPDATE swarmpg.memory SET value = $1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.batch(10)) != 0 || s.size() != 2 {
		t.Fatal("in-flight receipts must occupy capacity without being flushed")
	}
	g.finishAudit(completed, &QueryResponse{RowCount: 1, Bytes: 24, Affected: 0}, nil, 125*time.Millisecond)
	if batch := s.batch(10); len(batch) != 1 || batch[0].ID != completed.ID {
		t.Fatalf("only completed receipts may be flushed before restart: %+v", batch)
	}
	s.close()
	recovered := auditTestSpool(t, dir, 3)
	batch := recovered.batch(10)
	if len(batch) != 2 {
		t.Fatalf("restart lost receipts: %+v", batch)
	}
	byID := make(map[string]auditRecord)
	for _, r := range batch {
		byID[r.ID] = r
	}
	done := byID[completed.ID]
	if done.SQLState != nil || done.Error != nil || done.Rows != 1 || done.Bytes != 24 || done.MS != 125 {
		t.Fatalf("completed outcome changed on restart: %+v", done)
	}
	unknown := byID[interrupted.ID]
	if unknown.SQLState == nil || *unknown.SQLState != "audit_outcome_unknown" || unknown.Error == nil {
		t.Fatalf("interrupted work must recover with an unknown outcome: %+v", unknown)
	}
	if err := recovered.ack([]auditRecord{done}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, done.ID+".json")); !os.IsNotExist(err) {
		t.Fatalf("acknowledged receipt remains on disk: %v", err)
	}
	recovered.close()
	again := auditTestSpool(t, dir, 3)
	if batch := again.batch(10); len(batch) != 1 || batch[0].ID != interrupted.ID {
		t.Fatalf("acknowledgement did not survive restart: %+v", batch)
	}
}

func TestAuditSpoolCapacityAndAdmission(t *testing.T) {
	s := auditTestSpool(t, t.TempDir(), 1)
	g := &Gateway{auditSpool: s, auditStrict: true} // nil runtime proves SQL is never attempted
	r, err := g.beginAudit(auditTestAgent(), "SELECT 1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := g.query(context.Background(), auditTestAgent(), QueryRequest{SQL: "SELECT 2"}); err == nil || err.Status != 503 || err.Code != "audit_unavailable" || result != nil {
		t.Fatalf("strict full spool must refuse before execution: result=%+v error=%+v", result, err)
	}
	if s.size() != 1 || len(s.batch(10)) != 0 || g.stats.auditLost.Load() != 0 {
		t.Fatal("strict admission lost or flushed an in-flight receipt")
	}
	g.finishAudit(r, &QueryResponse{RowCount: 1}, nil, time.Millisecond)
	if _, err := g.beginAudit(auditTestAgent(), "SELECT 3", 0); err == nil {
		t.Fatal("completed but unacknowledged receipts must still occupy capacity")
	}
	g.auditStrict = false
	if dropped, err := g.beginAudit(auditTestAgent(), "SELECT 4", 0); err != nil || dropped != nil || g.stats.auditLost.Load() != 1 {
		t.Fatalf("best-effort overflow must count exactly one dropped receipt: %+v, %+v, lost=%d", dropped, err, g.stats.auditLost.Load())
	}
	if err := s.ack(s.batch(10)); err != nil {
		t.Fatal(err)
	}
	if _, err := g.beginAudit(auditTestAgent(), "SELECT 5", 0); err != nil || s.size() != 1 {
		t.Fatalf("ack must free capacity: %+v", err)
	}
	missing := &Gateway{auditStrict: true}
	if _, err := missing.query(context.Background(), auditTestAgent(), QueryRequest{SQL: "SELECT 1"}); err == nil || err.Code != "audit_unavailable" {
		t.Fatalf("strict mode without a spool must fail closed: %+v", err)
	}
}

func TestAuditSpoolExclusiveDirectory(t *testing.T) {
	dir := t.TempDir()
	first := auditTestSpool(t, dir, 2)
	second, err := newAuditSpool(dir, 2)
	if err == nil {
		second.close()
		t.Fatal("two gateways acquired the same audit spool")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("expected an exclusive-lock error, got %v", err)
	}
	first.close()
	auditTestSpool(t, dir, 2) // lock must become available after the owner stops
}

func TestAuditConfigurationRejectsInvalidSettings(t *testing.T) {
	for _, tc := range []struct{ name, mode, capacity, dir, message string }{
		{"strict without durable path", "strict", "100", "", "AUDIT_SPOOL_DIR"},
		{"unknown mode", "invalid", "100", "", "AUDIT_MODE"},
		{"zero capacity", "best-effort", "0", "", "AUDIT_SPOOL_CAPACITY"},
		{"invalid capacity", "best-effort", "not-a-number", "", "AUDIT_SPOOL_CAPACITY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AUDIT_MODE", tc.mode)
			t.Setenv("AUDIT_SPOOL_CAPACITY", tc.capacity)
			t.Setenv("AUDIT_SPOOL_DIR", tc.dir)
			g := &Gateway{}
			if err := g.startWriters(); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("invalid configuration must fail before launching writers: %v", err)
			}
		})
	}
}

func cleanupAuditFixture(t *testing.T, f *sharedMemoryFixture) {
	t.Helper()
	admin := f.g.admin // retain the independent pool used to build the fixture
	ids := make([]string, len(f.agents))
	for i, a := range f.agents {
		ids[i] = a.ID
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"audit", "documents"} {
			if _, err := admin.Exec(ctx, "DELETE FROM swarmpg."+table+" WHERE agent_id = ANY($1::uuid[])", ids); err != nil {
				t.Errorf("cleanup audit fixture %s: %v", table, err)
			}
		}
	})
}

func TestAuditReceiptReplayIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	cleanupAuditFixture(t, f)
	g, ctx := f.g, context.Background()
	g.auditSpool = auditTestSpool(t, t.TempDir(), 10)
	g.auditStrict = true
	for i, a := range f.agents[:2] {
		r, err := g.beginAudit(a, fmt.Sprintf("SELECT %d", i), 0)
		if err != nil {
			t.Fatal(err)
		}
		g.finishAudit(r, &QueryResponse{RowCount: 1}, nil, time.Millisecond)
	}
	batch := g.auditSpool.batch(10)
	if len(batch) != 2 {
		t.Fatalf("want two receipts, got %d", len(batch))
	}
	// Model a committed batch whose acknowledgement was lost: replay the exact
	// stable receipt IDs before removing them from the spool.
	for i := 0; i < 2; i++ {
		if err := g.flushAudit(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	var count, distinct int
	if err := g.admin.QueryRow(ctx, `SELECT count(*), count(DISTINCT receipt_id) FROM swarmpg.audit
		WHERE agent_id = ANY($1::uuid[])`, []string{f.agents[0].ID, f.agents[1].ID}).Scan(&count, &distinct); err != nil {
		t.Fatal(err)
	}
	if count != 2 || distinct != 2 {
		t.Fatalf("replay duplicated receipts: count=%d distinct=%d", count, distinct)
	}
	if err := g.auditSpool.ack(batch); err != nil || g.auditSpool.size() != 0 {
		t.Fatalf("acknowledgement failed: %v", err)
	}
}

func waitForAuditCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-tick.C:
		}
	}
}

func TestAuditRetriesDatabaseOutageIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	cleanupAuditFixture(t, f)
	g, ctx := f.g, context.Background()
	fixtureAdmin := g.admin
	var blocked atomic.Bool
	cfg := fixtureAdmin.Config()
	cfg.MaxConns, cfg.MinConns = 2, 0
	cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		if blocked.Load() {
			return errors.New("test: admin connection temporarily unavailable")
		}
		return nil
	}
	retryingAdmin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	g.admin = retryingAdmin
	g.auditSpool = auditTestSpool(t, t.TempDir(), 20)
	g.auditStrict = true
	writerCtx, cancel := context.WithCancel(ctx)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		g.auditWriter(writerCtx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-writerDone:
		case <-time.After(2 * time.Second):
			t.Error("audit writer did not stop after cancellation")
		}
		g.admin = fixtureAdmin
		retryingAdmin.Close()
	})
	// Authentication is warmed before the outage so the test isolates audit
	// availability; all SQL still executes on the independent runtime pool.
	for _, token := range f.tokens[:2] {
		r := httptest.NewRequest(http.MethodPost, "/v1/query", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		if _, err := g.authenticate(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	blocked.Store(true)
	retryingAdmin.Reset()
	const requests = 6
	handler := g.routes()
	for i := 0; i < requests; i++ {
		r := httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(fmt.Sprintf(`{"sql":"SELECT $1::integer AS value","params":[%d]}`, i)))
		r.Header.Set("Authorization", "Bearer "+f.tokens[i%2])
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("SQL request during audit outage: %d %s", w.Code, w.Body.String())
		}
	}
	waitForAuditCondition(t, "at least one audit retry", func() bool { return g.stats.auditRetries.Load() > 0 })
	if g.auditSpool.size() != requests || g.stats.auditLost.Load() != 0 {
		t.Fatalf("outage lost receipts: queued=%d lost=%d", g.auditSpool.size(), g.stats.auditLost.Load())
	}
	var count int
	agentIDs := []string{f.agents[0].ID, f.agents[1].ID}
	if err := fixtureAdmin.QueryRow(ctx, `SELECT count(*) FROM swarmpg.audit WHERE agent_id = ANY($1::uuid[])`, agentIDs).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed admin pool unexpectedly wrote %d audit rows", count)
	}
	blocked.Store(false)
	waitForAuditCondition(t, "retained audit receipts to drain", func() bool { return g.auditSpool.size() == 0 })
	var distinct, successful int
	if err := fixtureAdmin.QueryRow(ctx, `SELECT count(*), count(DISTINCT receipt_id), count(*) FILTER (WHERE sqlstate IS NULL)
		FROM swarmpg.audit WHERE agent_id = ANY($1::uuid[])`, agentIDs).Scan(&count, &distinct, &successful); err != nil {
		t.Fatal(err)
	}
	if count != requests || distinct != requests || successful != requests || g.stats.auditLost.Load() != 0 {
		t.Fatalf("recovery mismatch: count=%d distinct=%d successful=%d lost=%d", count, distinct, successful, g.stats.auditLost.Load())
	}
}

func TestStrictAuditFullRefusesWriteIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	cleanupAuditFixture(t, f)
	g, ctx := f.g, context.Background()
	g.auditSpool = auditTestSpool(t, t.TempDir(), 1)
	g.auditStrict = true
	if _, err := g.beginAudit(f.agents[0], "SELECT 1", 0); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/query", nil)
	r.Header.Set("Authorization", "Bearer "+f.tokens[0])
	if _, err := g.authenticate(ctx, r); err != nil {
		t.Fatal(err)
	}
	statement := `INSERT INTO swarmpg.documents(agent_id, org_id, collection, body)
		VALUES(swarmpg.current_agent(), swarmpg.current_org(), 'strict-full-test', '{"should_not_exist":true}')`
	body, err := json.Marshal(QueryRequest{SQL: statement})
	if err != nil {
		t.Fatal(err)
	}
	r = httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer "+f.tokens[0])
	w := httptest.NewRecorder()
	g.routes().ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), `"code":"audit_unavailable"`) {
		t.Fatalf("strict full endpoint response: %d %s", w.Code, w.Body.String())
	}
	var count int
	if err := g.admin.QueryRow(ctx, `SELECT count(*) FROM swarmpg.documents WHERE agent_id=$1 AND collection='strict-full-test'`, f.agents[0].ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 || g.stats.auditLost.Load() != 0 {
		t.Fatalf("strict refusal executed work or lost audit: documents=%d audit_lost=%d", count, g.stats.auditLost.Load())
	}
}

func TestAuditNULRefusalCannotPoisonBatchIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	cleanupAuditFixture(t, f)
	g, ctx := f.g, context.Background()
	g.auditSpool = auditTestSpool(t, t.TempDir(), 10)
	g.auditStrict = true
	rawSQL := "SET\x00"
	handler := g.routes()
	for _, tc := range []struct {
		sql    string
		status int
	}{{rawSQL, http.StatusBadRequest}, {"SELECT 42", http.StatusOK}} {
		body, err := json.Marshal(QueryRequest{SQL: tc.sql})
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+f.tokens[0])
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("HTTP SQL %q: want %d, got %d %s", tc.sql, tc.status, w.Code, w.Body.String())
		}
	}
	if g.auditSpool.size() != 2 {
		t.Fatalf("both refusal and successful SQL must reserve receipts: %d", g.auditSpool.size())
	}
	writerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.auditWriter(writerCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitForAuditCondition(t, "NUL refusal and ordinary receipt to drain together", func() bool { return g.auditSpool.size() == 0 })
	var count, successes int
	if err := g.admin.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE sqlstate IS NULL)
		FROM swarmpg.audit WHERE agent_id=$1`, f.agents[0].ID).Scan(&count, &successes); err != nil {
		t.Fatal(err)
	}
	if count != 2 || successes != 1 || g.stats.auditLost.Load() != 0 || g.stats.auditRetries.Load() != 0 {
		t.Fatalf("poisoned batch: count=%d successes=%d lost=%d retries=%d", count, successes, g.stats.auditLost.Load(), g.stats.auditRetries.Load())
	}
	var digest, sample string
	if err := g.admin.QueryRow(ctx, `SELECT encode(sql_hash, 'hex'), sql_sample FROM swarmpg.audit
		WHERE agent_id=$1 AND sqlstate='bad_request'`, f.agents[0].ID).Scan(&digest, &sample); err != nil {
		t.Fatal(err)
	}
	wantDigest := sha256.Sum256([]byte(rawSQL))
	if digest != hex.EncodeToString(wantDigest[:]) || sample != "SET\uFFFD" {
		t.Fatalf("sanitized display must preserve original statement digest: hash=%s sample=%q", digest, sample)
	}
}

func TestAuditLegacyInvalidTextRecoveryIntegration(t *testing.T) {
	f := newSharedMemoryFixture(t)
	cleanupAuditFixture(t, f)
	g, ctx := f.g, context.Background()
	dir := t.TempDir()
	g.auditSpool = auditTestSpool(t, dir, 10)
	g.auditStrict = true
	legacyID, err := auditID()
	if err != nil {
		t.Fatal(err)
	}
	originalSQL := "legacy\x00\xff"
	hash := sha256.Sum256([]byte(originalSQL))
	const marker = "INVALID_UTF8_PLACEHOLDER"
	state, message := "state\x00"+marker, "failure\x00"+marker
	legacy := auditRecord{ID: legacyID, AgentID: f.agents[0].ID, OrgID: f.agents[0].OrgID,
		At: time.Now().UTC(), SQLHash: hex.EncodeToString(hash[:]), SQLSample: "legacy\x00" + marker,
		SQLState: &state, Error: &message}
	// Bypass the new admission formatter to create a pre-fix receipt on disk.
	if err := g.auditSpool.reserve(legacy); err != nil {
		t.Fatal(err)
	}
	if err := g.auditSpool.complete(legacy); err != nil {
		t.Fatal(err)
	}
	normal, aerr := g.beginAudit(f.agents[1], "SELECT 1", 0)
	if aerr != nil {
		t.Fatal(aerr)
	}
	g.finishAudit(normal, &QueryResponse{RowCount: 1}, nil, time.Millisecond)
	g.auditSpool.close()
	path := filepath.Join(dir, legacyID+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(data, []byte(marker)) != 3 {
		t.Fatalf("legacy fixture must contain invalid sample, state, and error: %s", data)
	}
	// Go's JSON recovery normalizes invalid UTF-8 but retains decoded NULs.
	// Both forms must ultimately be safe for PostgreSQL text array encoding.
	data = bytes.ReplaceAll(data, []byte(marker), []byte{0xff})
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	g.auditSpool = auditTestSpool(t, dir, 10)
	batch := g.auditSpool.batch(10)
	if len(batch) != 2 {
		t.Fatalf("recovery lost mixed legacy and ordinary receipts: %+v", batch)
	}
	for _, r := range batch {
		if r.ID == legacyID && (!strings.ContainsRune(r.SQLSample, '\x00') || !strings.ContainsRune(r.SQLSample, '\uFFFD')) {
			t.Fatalf("legacy fixture did not exercise recovered invalid text: %+v", r)
		}
	}
	if err := g.flushAudit(ctx, batch); err != nil {
		t.Fatalf("legacy text poisoned mixed recovery batch: %v", err)
	}
	if err := g.auditSpool.ack(batch); err != nil || g.auditSpool.size() != 0 {
		t.Fatalf("recovered batch did not drain: %v", err)
	}
	var digest, sample, gotState, gotMessage string
	if err := g.admin.QueryRow(ctx, `SELECT encode(sql_hash, 'hex'), sql_sample, sqlstate, error
		FROM swarmpg.audit WHERE receipt_id=$1`, legacyID).Scan(&digest, &sample, &gotState, &gotMessage); err != nil {
		t.Fatal(err)
	}
	if digest != hex.EncodeToString(hash[:]) || sample != "legacy\uFFFD\uFFFD" || gotState != "state\uFFFD\uFFFD" || gotMessage != "failure\uFFFD\uFFFD" {
		t.Fatalf("legacy normalization changed evidence: digest=%s sample=%q state=%q error=%q", digest, sample, gotState, gotMessage)
	}
	var count int
	if err := g.admin.QueryRow(ctx, `SELECT count(*) FROM swarmpg.audit WHERE receipt_id = ANY($1::uuid[])`,
		[]string{legacyID, normal.ID}).Scan(&count); err != nil || count != 2 {
		t.Fatalf("mixed batch must retain both receipts: count=%d err=%v", count, err)
	}
}
