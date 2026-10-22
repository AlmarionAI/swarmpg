// SwarmPG gateway: multiplexes many agents onto few Postgres connections and
// enforces identity (RLS), budgets, statement safety, idempotency and audit.
//
// The database is the real boundary: isolation (FORCE RLS on the sealed
// principal), DDL blocking, and the REVOKEs in sql/schema.sql all hold even if
// this gateway has a bug. The checks here are a fast path and defense in depth,
// not the primary control.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	tokenCacheTTL   = 5 * time.Second // short, so a disabled/expired token stops working quickly
	maxRequestBytes = 1 << 20         // 1 MiB request body cap
	maxFieldBytes   = 1 << 20         // 1 MiB cap on a single returned field
)

type Agent struct {
	ID, OrgID          string
	Name               string
	Shard              int16
	RPS, MaxRows       int
	MaxResultBytes     int64
	MaxConcurrency     int
	MaxCost            float64
	StatementTimeoutMS int
	ReadOnly, Disabled bool
	Expired            bool
}

type Gateway struct {
	admin    *pgxpool.Pool // privileged: agent registry + audit
	runtime  *pgxpool.Pool // logged in as agent_runtime: all agent SQL
	adminKey string

	mcpOrigins map[string]bool // Origins allowed to call /mcp; requests without one are allowed

	// SwarmPG kernel only: the principal is sealed in core, optionally signed,
	// and the row/cost budgets are enforced by the executor up to the
	// ceilings set on agent_runtime.
	sealed          bool
	principalSecret []byte
	ceilRows        int
	ceilCost        float64

	cache       sync.Map // tokenHash -> *cachedAgent
	buckets     sync.Map // agentID -> *bucket
	sems        sync.Map // agentID -> chan struct{} (per-agent concurrency)
	seen        sync.Map // agentID -> *lastSeen (throttled last_seen_at writes)
	auditSpool  *auditSpool
	auditStrict bool
	seenCh      chan string
	stats       struct {
		requests, rejected, errors, limited, auditLost, auditRetries atomic.Int64
	}
}

type cachedAgent struct {
	a  *Agent
	at time.Time
}

// ---- token bucket per agent (in-process; a multi-gateway deployment needs a
// shared store — see docs/DESIGN.md control plane. Documented, not pretended away.)
type bucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64
}

func (b *bucket) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.rate*2 {
		b.tokens = b.rate * 2
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (g *Gateway) allow(a *Agent) bool {
	if a.RPS <= 0 {
		return true // 0 = unlimited by convention
	}
	v, _ := g.buckets.LoadOrStore(a.ID, &bucket{tokens: float64(a.RPS), last: time.Now(), rate: float64(a.RPS)})
	return v.(*bucket).take()
}

// ---- per-agent concurrency cap: one agent cannot occupy the whole runtime pool.
func (g *Gateway) acquireSlot(a *Agent) (func(), bool) {
	if a.MaxConcurrency <= 0 {
		return func() {}, true
	}
	v, _ := g.sems.LoadOrStore(a.ID, make(chan struct{}, a.MaxConcurrency))
	ch := v.(chan struct{})
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	default:
		return nil, false
	}
}

// ---- auth
func hashToken(t string) []byte { h := sha256.Sum256([]byte(t)); return h[:] }

func (g *Gateway) authenticate(ctx context.Context, r *http.Request) (*Agent, error) {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if tok == "" {
		return nil, errAuth("missing bearer token")
	}
	h := hashToken(tok)
	key := hex.EncodeToString(h)
	if v, ok := g.cache.Load(key); ok {
		c := v.(*cachedAgent)
		if time.Since(c.at) < tokenCacheTTL {
			return c.a, nil
		}
	}
	a := &Agent{}
	var expiresAt *time.Time
	err := g.admin.QueryRow(ctx, `
		SELECT id::text, org_id::text, name, shard, rps_limit, max_rows, max_result_bytes,
		       max_concurrency, max_cost, statement_timeout_ms, read_only, disabled, token_expires_at
		FROM swarmpg.agents WHERE token_hash = $1`, h).
		Scan(&a.ID, &a.OrgID, &a.Name, &a.Shard, &a.RPS, &a.MaxRows, &a.MaxResultBytes,
			&a.MaxConcurrency, &a.MaxCost, &a.StatementTimeoutMS, &a.ReadOnly, &a.Disabled, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errAuth("invalid token")
	}
	if err != nil {
		return nil, &apiError{Status: http.StatusServiceUnavailable, Code: "auth_unavailable",
			Message: "authentication service temporarily unavailable", Retryable: true}
	}
	if expiresAt != nil && time.Now().After(*expiresAt) {
		a.Expired = true
	}
	g.cache.Store(key, &cachedAgent{a: a, at: time.Now()})
	return a, nil
}

type lastSeen struct{ at atomic.Int64 }

// Throttle last_seen_at writes to at most once per 60s per agent, batched by
// seenWriter, so the registry is not a write hotspot on the auth hot path.
func (g *Gateway) touchLastSeen(a *Agent) {
	v, _ := g.seen.LoadOrStore(a.ID, &lastSeen{})
	ls := v.(*lastSeen)
	now := time.Now().Unix()
	prev := ls.at.Load()
	if now-prev < 60 || !ls.at.CompareAndSwap(prev, now) {
		return
	}
	select {
	case g.seenCh <- a.ID:
	default: // best effort: a full queue skips one update
	}
}

// ---- statement guard (fast path + defense in depth; the DB is the real control).
// We normalise whitespace and strip comments (including nested block comments)
// before classifying, so tab/newline/comment tricks cannot hide the real first
// keyword. Classification is advisory: schema.sql blocks DDL, SET, signalling,
// etc. regardless of what this returns.
var forbiddenLeading = map[string]bool{
	"SET": true, "RESET": true, "DO": true, "CALL": true, "COPY": true,
	"BEGIN": true, "COMMIT": true, "ROLLBACK": true, "START": true, "END": true,
	"ABORT": true, "SAVEPOINT": true, "RELEASE": true, "PREPARE": true,
	"EXECUTE": true, "DEALLOCATE": true, "LISTEN": true, "NOTIFY": true,
	"UNLISTEN": true, "LOAD": true, "VACUUM": true, "ANALYZE": true,
	"CLUSTER": true, "REINDEX": true, "DISCARD": true, "SECURITY": true,
	"GRANT": true, "REVOKE": true, "ALTER": true, "CREATE": true, "DROP": true,
	"TRUNCATE": true, "LOCK": true, "CHECKPOINT": true, "REFRESH": true,
	"IMPORT": true, "COMMENT": true,
	// Cursors hold state on the pooled connection across transactions; agents
	// receive results directly, so there is no reason to allow them.
	"DECLARE": true, "FETCH": true, "MOVE": true, "CLOSE": true,
}

// stripSQL removes SQL comments (line and nested block) and collapses runs of
// whitespace to a single space. Returns the cleaned statement.
func stripSQL(sql string) string {
	var b strings.Builder
	i, n := 0, len(sql)
	depth := 0 // block-comment nesting
	inLine := false
	var inStr byte // 0, '\'' or '"'
	for i < n {
		c := sql[i]
		switch {
		case inLine:
			if c == '\n' {
				inLine = false
				b.WriteByte(' ')
			}
			i++
		case depth > 0:
			if c == '*' && i+1 < n && sql[i+1] == '/' {
				depth--
				i += 2
			} else if c == '/' && i+1 < n && sql[i+1] == '*' {
				depth++
				i += 2
			} else {
				i++
			}
		case inStr != 0:
			b.WriteByte(c)
			if c == inStr {
				inStr = 0
			}
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			inLine = true
			i += 2
		case c == '/' && i+1 < n && sql[i+1] == '*':
			depth++
			i += 2
			b.WriteByte(' ')
		case c == '\'' || c == '"':
			inStr = c
			b.WriteByte(c)
			i++
		default:
			if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
				b.WriteByte(' ')
			} else {
				b.WriteByte(c)
			}
			i++
		}
	}
	return strings.TrimSpace(b.String())
}

func firstKeyword(sql string) string {
	s := strings.TrimLeft(stripSQL(sql), " (")
	end := 0
	for end < len(s) && (s[end] >= 'a' && s[end] <= 'z' || s[end] >= 'A' && s[end] <= 'Z' || s[end] == '_') {
		end++
	}
	return strings.ToUpper(s[:end])
}

func guard(sql string, a *Agent) error {
	kw := firstKeyword(sql)
	if kw == "" {
		return errBadReq("empty statement")
	}
	if forbiddenLeading[kw] {
		return errBadReq(fmt.Sprintf("statement type %s is not permitted for agents", kw))
	}
	if a.ReadOnly {
		switch kw {
		case "SELECT", "WITH", "VALUES", "TABLE", "EXPLAIN", "SHOW":
		default:
			return errBadReq("agent is read-only")
		}
	}
	return nil
}

// ---- typed errors carrying an HTTP status and, for DB errors, structured fields
type apiError struct {
	Status    int
	Code      string // SQLSTATE or a gateway code
	Message   string
	Detail    string
	Hint      string
	Position  int
	Retryable bool
	Extra     map[string]any // further fields for the error body, e.g. current_version
}

func (e *apiError) Error() string { return e.Message }

func errAuth(m string) *apiError      { return &apiError{Status: 401, Code: "unauthorized", Message: m} }
func errBadReq(m string) *apiError    { return &apiError{Status: 400, Code: "bad_request", Message: m} }
func errForbidden(m string) *apiError { return &apiError{Status: 403, Code: "forbidden", Message: m} }

func classifyPg(err error) *apiError {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		// Map a few classes so an agent's control loop can decide retry vs repair.
		retryable := false
		switch {
		case strings.HasPrefix(pg.Code, "40"): // serialization / deadlock
			retryable = true
		case pg.Code == "57014": // query canceled (timeout)
			retryable = true
		}
		status := 400
		code := pg.Code
		var extra map[string]any
		switch pg.Code {
		case "42501": // insufficient_privilege
			status = 403
		case "SW409": // memory version conflict (schema.sql)
			status, code = 409, "version_conflict"
			var cur, expected int64
			if _, err := fmt.Sscanf(pg.Detail, "current_version=%d expected_version=%d", &cur, &expected); err == nil {
				extra = map[string]any{"current_version": cur}
			}
		case "SW404": // memory key or version not found (schema.sql)
			status, code = 404, "not_found"
		case "PT400":
			status, code = 400, "bad_request"
		case "PT404":
			status, code = 404, "not_found"
		case "PT409":
			status, code = 409, "task_conflict"
		case "PT410":
			status, code = 409, "stale_claim"
		case "PT411":
			status, code = 409, "lease_expired"
		case "PT412":
			status, code = 409, "inputs_changed"
		case "PT413":
			status, code = 413, "result_limit_exceeded"
		}
		return &apiError{Status: status, Code: code, Message: pg.Message,
			Detail: pg.Detail, Hint: pg.Hint, Position: int(pg.Position), Retryable: retryable, Extra: extra}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &apiError{Status: 504, Code: "statement_timeout", Message: "statement timed out", Retryable: true}
	}
	if e, ok := err.(*apiError); ok {
		return e
	}
	return &apiError{Status: 500, Code: "internal", Message: err.Error()}
}

// ---- query execution with per-request connection reset and byte budget
type QueryRequest struct {
	SQL    string            `json:"sql"`
	Params []json.RawMessage `json:"params,omitempty"`
}

type QueryResponse struct {
	Columns   []string `json:"columns"`
	Types     []string `json:"types"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Bytes     int64    `json:"bytes"`
	Truncated bool     `json:"truncated,omitempty"`
	Command   string   `json:"command,omitempty"`
	Affected  int64    `json:"affected,omitempty"`
	Duration  float64  `json:"duration_ms"`
}

// withConn acquires a runtime connection, runs the agent's transaction with the
// sealed identity + limits set LOCAL, then ALWAYS resets the connection
// (DISCARD ALL) before returning it to the pool, so nothing — temp tables, held
// cursors, prepared statements, session settings — leaks to the next agent.
func (g *Gateway) withConn(ctx context.Context, a *Agent, fn func(ctx context.Context, tx pgx.Tx) error) error {
	timeout := time.Duration(a.StatementTimeoutMS)*time.Millisecond + 2*time.Second
	if a.StatementTimeoutMS <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := g.runtime.Acquire(ctx)
	if err != nil {
		return err
	}
	poisoned := false
	defer func() {
		// Reset on a fresh context so a request-timeout does not skip the reset.
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		if poisoned {
			// Drop a connection we could not leave in a known-clean state.
			if h := conn.Hijack(); h != nil {
				_ = h.Close(context.Background())
			}
			return
		}
		// Reset session-held state so nothing leaks to the next agent on this
		// pooled connection: close any held cursors, drop temp tables and temp
		// sequences, and reset session GUCs. We deliberately do NOT run the full
		// DISCARD ALL: its DEALLOCATE ALL would invalidate pgx's prepared-
		// statement cache and make the next reuse fail with SQLSTATE 26000.
		if _, derr := conn.Exec(rctx, "CLOSE ALL; RESET ALL; DISCARD TEMP; DISCARD SEQUENCES"); derr != nil {
			if h := conn.Hijack(); h != nil {
				_ = h.Close(context.Background())
			}
			return
		}
		conn.Release()
	}()

	tx, err := conn.Begin(ctx)
	if err != nil {
		poisoned = true
		return err
	}
	// Identity + per-statement limits, transaction-local. agent_id/org_id are
	// UUIDs from the registry, so interpolation here is safe; still, they are
	// validated as UUIDs at creation.
	seal := ""
	if g.sealed {
		seal = g.kernelSetup(a)
	}
	stmtTimeout := a.StatementTimeoutMS
	setup := seal + fmt.Sprintf(
		"SET LOCAL app.agent_id = '%s'; SET LOCAL app.org_id = '%s'; "+
			"SET LOCAL statement_timeout = %d; SET LOCAL idle_in_transaction_session_timeout = 2000; "+
			"SET LOCAL lock_timeout = 1000; SET LOCAL transaction_read_only = %t",
		a.ID, a.OrgID, stmtTimeout, a.ReadOnly)
	if _, err := tx.Exec(ctx, setup); err != nil {
		_ = tx.Rollback(ctx)
		poisoned = true
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// kernelSetup seals the agent's principal (agent and org in one value, signed
// when a secret is configured) and lowers the executor budgets to this agent's,
// all transaction-local. Once sealed, nothing the agent runs in the transaction
// can change its identity or raise its budgets.
func (g *Gateway) kernelSetup(a *Agent) string {
	principal := "agent=" + a.ID + ",org=" + a.OrgID
	if len(g.principalSecret) > 0 {
		m := hmac.New(sha256.New, g.principalSecret)
		m.Write([]byte(principal))
		principal += ",sig=" + hex.EncodeToString(m.Sum(nil))
	}
	setup := fmt.Sprintf("SET LOCAL agent_principal = '%s'; ", principal)
	// One row past max_rows, so the gateway still sees that a result was cut
	// and reports it as truncated; writes over that fail in the executor.
	if rows := capInt(a.MaxRows+1, g.ceilRows); rows > 0 {
		setup += fmt.Sprintf("SET LOCAL agent_max_rows = %d; ", rows)
	}
	if cost := capFloat(a.MaxCost, g.ceilCost); cost > 0 {
		setup += "SET LOCAL agent_max_cost = " + strconv.FormatFloat(cost, 'f', -1, 64) + "; "
	}
	return setup
}

// capInt and capFloat bound v by a ceiling, where 0 means "no limit" for both.
func capInt(v, ceil int) int {
	if ceil > 0 && (v <= 0 || v > ceil) {
		return ceil
	}
	return v
}

func capFloat(v, ceil float64) float64 {
	if ceil > 0 && (v <= 0 || v > ceil) {
		return ceil
	}
	return v
}

// approxBytes is a cheap size estimate for the byte budget (not exact JSON size).
func approxBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 4
	case string:
		return int64(len(x))
	case []byte:
		return int64(len(x))
	default:
		return 16
	}
}

func (g *Gateway) query(ctx context.Context, a *Agent, q QueryRequest) (result *QueryResponse, failure *apiError) {
	// Reserve before executing (or refusing) SQL. Strict mode must reject work
	// before a write can commit if its audit intent cannot be retained.
	receipt, aerr := g.beginAudit(a, q.SQL, len(q.Params))
	if aerr != nil {
		return nil, aerr
	}
	started := time.Now()
	defer func() { g.finishAudit(receipt, result, failure, time.Since(started)) }()
	if err := guard(q.SQL, a); err != nil {
		// Refused statements are audited too: they are the attempts worth seeing.
		g.stats.rejected.Add(1)
		ae := err.(*apiError)
		return nil, ae
	}
	params, perr := decodeParams(q.Params)
	if perr != nil {
		return nil, errBadReq(perr.Error())
	}
	start := time.Now()
	resp := &QueryResponse{Columns: []string{}, Types: []string{}, Rows: [][]any{}}
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, q.SQL, params...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for _, fd := range rows.FieldDescriptions() {
			resp.Columns = append(resp.Columns, fd.Name)
			resp.Types = append(resp.Types, oidName(fd.DataTypeOID))
		}
		for rows.Next() {
			if resp.RowCount >= a.MaxRows || (a.MaxResultBytes > 0 && resp.Bytes >= a.MaxResultBytes) {
				resp.Truncated = true
				break
			}
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			out := make([]any, len(vals))
			for i, v := range vals {
				sv := stableValue(v)
				resp.Bytes += approxBytes(sv)
				if a.MaxResultBytes > 0 && resp.Bytes > a.MaxResultBytes {
					resp.Truncated = true
				}
				out[i] = sv
			}
			resp.Rows = append(resp.Rows, out)
			resp.RowCount++
			if resp.Truncated {
				break
			}
		}
		if err := rows.Err(); err != nil && !resp.Truncated {
			return err
		}
		// CommandTag is only meaningful once the result is fully consumed; on a
		// truncated result it may be empty, so guard the field access.
		ct := rows.CommandTag()
		if fields := strings.Fields(ct.String()); len(fields) > 0 {
			resp.Command = fields[0]
		}
		resp.Affected = ct.RowsAffected()
		return nil
	})
	resp.Duration = float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		ae := classifyPg(err)
		return nil, ae
	}
	return resp, nil
}

// decodeParams converts JSON params to pgx-compatible values WITHOUT losing
// integer precision (encoding/json would coerce big ints to float64).
func decodeParams(raw []json.RawMessage) ([]any, error) {
	out := make([]any, len(raw))
	for i, r := range raw {
		dec := json.NewDecoder(strings.NewReader(string(r)))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("param %d: %v", i+1, err)
		}
		out[i] = normalizeParam(v)
	}
	return out, nil
}

func normalizeParam(v any) any {
	switch x := v.(type) {
	case json.Number:
		if iv, err := x.Int64(); err == nil {
			return iv
		}
		// Not an int64: keep the exact textual form; PG casts it per the
		// parameter's target type without precision loss.
		return x.String()
	case map[string]any, []any:
		// Pass structured JSON through as a JSON string for jsonb params.
		b, _ := json.Marshal(x)
		return string(b)
	default:
		return v
	}
}

// oidName maps the common PostgreSQL type OIDs to names so the agent sees the
// column type alongside the value. Unknown OIDs fall back to the numeric OID.
func oidName(oid uint32) string {
	switch oid {
	case 16:
		return "bool"
	case 17:
		return "bytea"
	case 20:
		return "int8"
	case 21:
		return "int2"
	case 23:
		return "int4"
	case 25:
		return "text"
	case 114:
		return "json"
	case 3802:
		return "jsonb"
	case 700:
		return "float4"
	case 701:
		return "float8"
	case 1042:
		return "bpchar"
	case 1043:
		return "varchar"
	case 1082:
		return "date"
	case 1114:
		return "timestamp"
	case 1184:
		return "timestamptz"
	case 1700:
		return "numeric"
	case 2950:
		return "uuid"
	case 1009:
		return "text[]"
	case 1007:
		return "int4[]"
	default:
		return fmt.Sprintf("oid:%d", oid)
	}
}

// stableValue maps pgx's Go representations to JSON-stable, LLM-friendly forms.
func stableValue(v any) any {
	switch x := v.(type) {
	case [16]byte: // uuid
		return fmt.Sprintf("%x-%x-%x-%x-%x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16])
	case []byte:
		if len(x) > maxFieldBytes {
			x = x[:maxFieldBytes]
		}
		return "\\x" + hex.EncodeToString(x)
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return fmt.Sprintf("%v", x) // "+Inf"/"NaN" as text, never breaks JSON
		}
		return x
	case float32:
		f := float64(x)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return fmt.Sprintf("%v", f)
		}
		return f
	case string:
		if len(x) > maxFieldBytes {
			return x[:maxFieldBytes]
		}
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return v
	}
}

// ---- LLM-oriented schema description (static; same for every agent, no leakage).
func (g *Gateway) describe(ctx context.Context, a *Agent) (string, *apiError) {
	var sb strings.Builder
	sb.WriteString("# Tables you can use. Private data is isolated to your agent_id; shared memory and task queues are scoped to your org by the database.\n")
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT c.table_name,
			       string_agg(c.column_name || ' ' || c.data_type, ', ' ORDER BY c.ordinal_position)
			FROM information_schema.columns c
			WHERE c.table_schema = 'swarmpg'
			  AND c.table_name IN ('memory','memory_history','documents','events','tasks','task_history')
			GROUP BY c.table_name ORDER BY c.table_name`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var t, cols string
			if err := rows.Scan(&t, &cols); err != nil {
				return err
			}
			fmt.Fprintf(&sb, "swarmpg.%s(%s)\n", t, cols)
		}
		return rows.Err()
	})
	sb.WriteString("\n# Rules\n" +
		"- One statement per request. SET / DO / COPY / DDL / transaction control are rejected by the database.\n" +
		"- swarmpg.events is append-only and hash-chained.\n" +
		"- Every memory write gets a new version, kept in swarmpg.memory_history (read-only). Write with\n" +
		"  swarmpg.memory_put(key, value, shared, expected_version) for compare-and-set (0 = must not exist;\n" +
		"  a stale version fails with version_conflict). Read the past with swarmpg.memory_at_version(key, v)\n" +
		"  or swarmpg.memory_as_of(key, ts); swarmpg.memory_undo(key) restores the previous version.\n" +
		"- Use swarmpg.documents (collection, body jsonb) for your own structured data; you cannot CREATE TABLE.\n" +
		"- Task queues are shared within your org. Use task_enqueue(queue,key,payload,dependencies,inputs),\n" +
		"  task_claim(queue,request_id,lease_ms), task_get(id), task_renew(id,fence,lease_ms),\n" +
		"  task_release(id,fence), task_complete(id,fence,result), task_cancel(id) and task_history(id,limit).\n" +
		"  Prefix SQL calls with swarmpg.; task tables/history are read-only. Keep fencing tokens as strings\n" +
		"  in REST/MCP; SQL takes bigint. Reuse a claim request UUID on retry. Completion checks your current\n" +
		"  unexpired lease and all declared producer-selected memory input versions in one transaction.\n" +
		"- Parameterise with $1,$2,...; results are capped by your row and byte budgets.\n" +
		"- On error you get a SQLSTATE code, detail, hint and position for repair.\n")
	if err != nil {
		return "", classifyPg(err)
	}
	return sb.String(), nil
}

// ---- HTTP
func writeJSON(w http.ResponseWriter, code int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":{"code":"encode_error","message":"response could not be encoded"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(buf)
}

// errorBody is an error as agents see it, over REST and MCP alike: the code,
// message and, when present, the detail, hint, position and retryable flag.
func errorBody(err *apiError) map[string]any {
	body := map[string]any{"code": err.Code, "message": err.Message}
	if err.Detail != "" {
		body["detail"] = err.Detail
	}
	if err.Hint != "" {
		body["hint"] = err.Hint
	}
	if err.Position > 0 {
		body["position"] = err.Position
	}
	if err.Retryable {
		body["retryable"] = true
	}
	for k, v := range err.Extra {
		body[k] = v
	}
	return body
}

func writeErr(w http.ResponseWriter, err *apiError) {
	writeJSON(w, err.Status, map[string]any{"error": errorBody(err)})
}

// admit authenticates an agent request and applies the kill switch, token
// expiry, rate limit and per-agent concurrency cap. On success the caller must
// call release when the request is done.
func (g *Gateway) admit(r *http.Request) (a *Agent, release func(), aerr *apiError) {
	g.stats.requests.Add(1)
	a, err := g.authenticate(r.Context(), r)
	if err != nil {
		return nil, nil, err.(*apiError)
	}
	if a.Disabled {
		return nil, nil, errForbidden("agent disabled")
	}
	if a.Expired {
		return nil, nil, errForbidden("token expired")
	}
	g.touchLastSeen(a)
	if !g.allow(a) {
		g.stats.limited.Add(1)
		return nil, nil, &apiError{Status: 429, Code: "rate_limited", Message: fmt.Sprintf("rate limit %d req/s exceeded", a.RPS), Retryable: true}
	}
	release, ok := g.acquireSlot(a)
	if !ok {
		g.stats.limited.Add(1)
		return nil, nil, &apiError{Status: 429, Code: "concurrency_limited", Message: fmt.Sprintf("concurrency limit %d exceeded", a.MaxConcurrency), Retryable: true}
	}
	return a, release, nil
}

func (g *Gateway) withAgent(h func(http.ResponseWriter, *http.Request, *Agent)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a, release, aerr := g.admit(r)
		if aerr != nil {
			if aerr.Status == 429 {
				w.Header().Set("Retry-After", "1")
			}
			writeErr(w, aerr)
			return
		}
		defer release()
		h(w, r, a)
	}
}

func (g *Gateway) withAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-Admin-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(g.adminKey)) != 1 {
			writeErr(w, errAuth("admin key required"))
			return
		}
		h(w, r)
	}
}

func newToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return "spg_" + hex.EncodeToString(b)
}

// queryInt reads an optional integer query parameter.
func queryInt(r *http.Request, name string) (*int64, *apiError) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return nil, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, errBadReq(name + " must be an integer")
	}
	return &n, nil
}

func readJSON(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

// idempotentWrite claims (agent, key); on a repeat it returns the stored response.
// Returns (storedResponse, isReplay, error). If key is empty, no idempotency.
func (g *Gateway) idempotentWrite(ctx context.Context, a *Agent, key string, do func(ctx context.Context, tx pgx.Tx) (json.RawMessage, error)) (json.RawMessage, bool, *apiError) {
	if key == "" {
		var out json.RawMessage
		err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
			r, e := do(ctx, tx)
			out = r
			return e
		})
		if err != nil {
			return nil, false, classifyPg(err)
		}
		return out, false, nil
	}
	var out json.RawMessage
	var replay bool
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		var existing json.RawMessage
		e := tx.QueryRow(ctx, `SELECT response FROM swarmpg.idempotency WHERE agent_id=swarmpg.current_agent() AND key=$1`, key).Scan(&existing)
		if e == nil {
			out, replay = existing, true
			return nil
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		r, e := do(ctx, tx)
		if e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, `INSERT INTO swarmpg.idempotency(agent_id, key, response) VALUES(swarmpg.current_agent(), $1, $2)`, key, r); e != nil {
			return e
		}
		out = r
		return nil
	})
	if err != nil {
		return nil, false, classifyPg(err)
	}
	return out, replay, nil
}

// memoryState is one memory key as an agent sees it: live, or as it was at a
// version or a point in time.
type memoryState struct {
	Key       string          `json:"key"`
	AgentID   string          `json:"agent_id"`
	Value     json.RawMessage `json:"value"`
	Shared    bool            `json:"shared"`
	Version   int64           `json:"version"`
	Own       bool            `json:"own"`
	ChangedAt *time.Time      `json:"changed_at,omitempty"`
}

type sharedMemoryResponse struct {
	Key      string        `json:"key"`
	Memories []memoryState `json:"memories"`
}

// Validate selectors before acquiring a connection. Historical memory stays
// private even when its live value has been shared with the organization.
func validateMemoryRead(key string, version *int64, asOf *time.Time, from *string, listShared bool) *apiError {
	if key == "" {
		return errBadReq("key required")
	}
	if version != nil && *version <= 0 {
		return errBadReq("version must be > 0")
	}
	if version != nil && asOf != nil {
		return errBadReq("pass version or as_of, not both")
	}
	if listShared && (from != nil || version != nil || asOf != nil) {
		return errBadReq("list_shared cannot be combined with from, version or as_of")
	}
	if from != nil {
		if version != nil || asOf != nil {
			return errBadReq("from selects live memory only; it cannot be combined with version or as_of")
		}
		if len(*from) != 32 && (len(*from) != 36 || (*from)[8] != '-' || (*from)[13] != '-' || (*from)[18] != '-' || (*from)[23] != '-') {
			return errBadReq("from must be an agent UUID")
		}
		var id pgtype.UUID
		if err := id.Scan(*from); err != nil || !id.Valid {
			return errBadReq("from must be an agent UUID")
		}
	}
	return nil
}

// memoryPut writes one of the agent's own keys and returns its new version.
// expected is a compare-and-set guard: nil writes unconditionally, 0 creates
// only if the key is absent, N updates only if the live version is still N.
func (g *Gateway) memoryPut(ctx context.Context, a *Agent, key string, value json.RawMessage, shared bool, expected *int64) (int64, *apiError) {
	if key == "" {
		return 0, errBadReq("key required")
	}
	if len(value) == 0 {
		return 0, errBadReq("value required")
	}
	if expected != nil && *expected < 0 {
		return 0, errBadReq("expected_version must be >= 0")
	}
	var version int64
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT swarmpg.memory_put($1, $2::jsonb, $3, $4)`, key, string(value), shared, expected).Scan(&version)
	})
	if err != nil {
		return 0, classifyPg(err)
	}
	return version, nil
}

// memoryGet reads a key. A live read returns the agent's own key, falling back
// to an org-shared key of the same name, ordered by version then producer ID.
// from selects one visible live producer; a read of the past (at a version, or
// as of a time) sees only the agent's own history.
func (g *Gateway) memoryGet(ctx context.Context, a *Agent, key string, version *int64, asOf *time.Time, from *string) (*memoryState, *apiError) {
	if aerr := validateMemoryRead(key, version, asOf, from, false); aerr != nil {
		return nil, aerr
	}
	m := &memoryState{Key: key, AgentID: a.ID, Own: true}
	var deleted bool
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		switch {
		case version != nil:
			return tx.QueryRow(ctx, `SELECT value, shared, version, deleted, changed_at FROM swarmpg.memory_at_version($1, $2)`,
				key, *version).Scan(&m.Value, &m.Shared, &m.Version, &deleted, &m.ChangedAt)
		case asOf != nil:
			return tx.QueryRow(ctx, `SELECT value, shared, version, deleted, changed_at FROM swarmpg.memory_as_of($1, $2)`,
				key, *asOf).Scan(&m.Value, &m.Shared, &m.Version, &deleted, &m.ChangedAt)
		default:
			return tx.QueryRow(ctx, `
				SELECT value, shared, version, agent_id = swarmpg.current_agent(), agent_id::text, updated_at
				FROM swarmpg.memory
				WHERE key = $1 AND (agent_id = swarmpg.current_agent()
				                    OR (shared AND org_id = swarmpg.current_org()))
				  AND ($2::uuid IS NULL OR agent_id = $2::uuid)
				ORDER BY (agent_id = swarmpg.current_agent()) DESC, version DESC, agent_id ASC LIMIT 1`,
				key, from).Scan(&m.Value, &m.Shared, &m.Version, &m.Own, &m.AgentID, &m.ChangedAt)
		}
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &apiError{Status: 404, Code: "not_found", Message: "not found"}
	}
	if err != nil {
		return nil, classifyPg(err)
	}
	if deleted {
		return nil, &apiError{Status: 404, Code: "not_found", Message: fmt.Sprintf("deleted at version %d", m.Version),
			Extra: map[string]any{"deleted_version": m.Version}}
	}
	return m, nil
}

// memoryShared lists every live, org-shared producer of a key. It never returns
// a partial list: exceeding a row or byte budget is an explicit error so callers
// cannot mistake omitted producers for absent ones.
func (g *Gateway) memoryShared(ctx context.Context, a *Agent, key string) (*sharedMemoryResponse, *apiError) {
	if aerr := validateMemoryRead(key, nil, nil, nil, true); aerr != nil {
		return nil, aerr
	}
	out := &sharedMemoryResponse{Key: key, Memories: []memoryState{}}
	maxRows := a.MaxRows
	if g.sealed && g.ceilRows > 0 && maxRows >= g.ceilRows {
		maxRows = g.ceilRows - 1 // reserve one row to detect an incomplete list
	}
	limitError := &apiError{Status: http.StatusRequestEntityTooLarge, Code: "result_limit_exceeded",
		Message: "shared memory list exceeds the agent's result budget",
		Hint:    "Use from to read a specific producer, or ask an administrator to raise the result budget."}
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT agent_id::text, value, version, agent_id = swarmpg.current_agent(), updated_at
			FROM swarmpg.memory
			WHERE key = $1 AND shared AND org_id = swarmpg.current_org()
			ORDER BY version DESC, agent_id ASC LIMIT $2`, key, int64(maxRows)+1)
		if err != nil {
			return err
		}
		defer rows.Close()
		// Include the enclosing JSON object and separators in the byte budget.
		envelope, err := json.Marshal(out)
		if err != nil {
			return err
		}
		bytes := int64(len(envelope))
		for rows.Next() {
			if len(out.Memories) >= maxRows {
				return limitError
			}
			m := memoryState{Key: key, Shared: true}
			if err := rows.Scan(&m.AgentID, &m.Value, &m.Version, &m.Own, &m.ChangedAt); err != nil {
				return err
			}
			encoded, err := json.Marshal(m)
			if err != nil {
				return err
			}
			bytes += int64(len(encoded))
			if len(out.Memories) > 0 {
				bytes++
			}
			if a.MaxResultBytes > 0 && bytes > a.MaxResultBytes {
				return limitError
			}
			out.Memories = append(out.Memories, m)
		}
		if a.MaxResultBytes > 0 && bytes > a.MaxResultBytes {
			return limitError
		}
		return rows.Err()
	})
	if err != nil {
		return nil, classifyPg(err)
	}
	return out, nil
}

// memoryDelete deletes one of the agent's own keys and returns the version of
// the delete marker. expected guards it like memoryPut, and must be > 0.
func (g *Gateway) memoryDelete(ctx context.Context, a *Agent, key string, expected *int64) (int64, *apiError) {
	if key == "" {
		return 0, errBadReq("key required")
	}
	if expected != nil && *expected <= 0 {
		return 0, errBadReq("expected_version must be > 0 to delete")
	}
	var version int64
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT swarmpg.memory_delete($1, $2)`, key, expected).Scan(&version)
	})
	if err != nil {
		return 0, classifyPg(err)
	}
	return version, nil
}

// memoryHistory lists the newest versions of one of the agent's own keys.
func (g *Gateway) memoryHistory(ctx context.Context, a *Agent, key string, limit int) ([]map[string]any, *apiError) {
	if key == "" {
		return nil, errBadReq("key required")
	}
	if limit <= 0 {
		limit = 50
	}
	var versions []map[string]any
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT version, value, shared, deleted, changed_at FROM swarmpg.memory_history($1, $2)`, key, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var version int64
			var value json.RawMessage
			var shared, deleted bool
			var changedAt time.Time
			if err := rows.Scan(&version, &value, &shared, &deleted, &changedAt); err != nil {
				return err
			}
			v := map[string]any{"version": version, "shared": shared, "deleted": deleted, "changed_at": changedAt}
			if !deleted {
				v["value"] = value
			}
			versions = append(versions, v)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, classifyPg(err)
	}
	if len(versions) == 0 {
		return nil, &apiError{Status: 404, Code: "not_found", Message: "no history for this key"}
	}
	return versions, nil
}

// memoryUndo restores the state before the key's latest change, or the state
// at toVersion, as a new version; restoring an absent or deleted state deletes
// the key. expected guards it like memoryPut.
func (g *Gateway) memoryUndo(ctx context.Context, a *Agent, key string, toVersion, expected *int64) (map[string]any, *apiError) {
	if key == "" {
		return nil, errBadReq("key required")
	}
	var version int64
	var restored *int64
	var deleted bool
	err := g.withConn(ctx, a, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT version, restored_version, deleted FROM swarmpg.memory_undo($1, $2, $3)`,
			key, toVersion, expected).Scan(&version, &restored, &deleted)
	})
	if err != nil {
		return nil, classifyPg(err)
	}
	return map[string]any{"key": key, "version": version, "restored_version": restored, "deleted": deleted}, nil
}

// appendEvent appends to the agent's hash-chained journal. With an
// idempotency key, a retry replays the stored response (replay = true).
func (g *Gateway) appendEvent(ctx context.Context, a *Agent, kind string, payload json.RawMessage, idemKey string) (json.RawMessage, bool, *apiError) {
	if kind == "" {
		return nil, false, errBadReq("kind required")
	}
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	return g.idempotentWrite(ctx, a, idemKey, func(ctx context.Context, tx pgx.Tx) (json.RawMessage, error) {
		var id, seq int64
		var hash []byte
		err := tx.QueryRow(ctx, `
			INSERT INTO swarmpg.events(agent_id, org_id, kind, payload)
			VALUES(swarmpg.current_agent(), swarmpg.current_org(), $1, $2) RETURNING id, seq, hash`,
			kind, string(payload)).Scan(&id, &seq, &hash)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"id": id, "seq": seq, "hash": hex.EncodeToString(hash)})
	})
}

func (g *Gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"ok": true, "requests": g.stats.requests.Load(), "rejected": g.stats.rejected.Load(),
			"errors": g.stats.errors.Load(), "rate_limited": g.stats.limited.Load(), "audit_lost": g.stats.auditLost.Load(),
			"audit_retries": g.stats.auditRetries.Load(), "audit": g.auditStatus(),
			"runtime_pool": g.runtime.Stat().TotalConns(), "admin_pool": g.admin.Stat().TotalConns(),
		})
	})

	mux.HandleFunc("POST /v1/orgs", g.withAdmin(func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Name string }
		if err := readJSON(r, &in); err != nil {
			writeErr(w, errBadReq(err.Error()))
			return
		}
		var id string
		if err := g.admin.QueryRow(r.Context(), `INSERT INTO swarmpg.orgs(name) VALUES($1) RETURNING id::text`, in.Name).Scan(&id); err != nil {
			writeErr(w, classifyPg(err))
			return
		}
		writeJSON(w, 201, map[string]string{"id": id})
	}))

	mux.HandleFunc("POST /v1/agents", g.withAdmin(func(w http.ResponseWriter, r *http.Request) {
		in := struct {
			OrgID              string  `json:"org_id"`
			Name               string  `json:"name"`
			RPS                int     `json:"rps_limit"`
			MaxRows            int     `json:"max_rows"`
			MaxResultBytes     int64   `json:"max_result_bytes"`
			MaxConcurrency     int     `json:"max_concurrency"`
			MaxCost            float64 `json:"max_cost"`
			StatementTimeoutMS int     `json:"statement_timeout_ms"`
			ReadOnly           bool    `json:"read_only"`
			TTLSeconds         int     `json:"ttl_seconds"`
		}{RPS: 20, MaxRows: 1000, MaxResultBytes: 4 << 20, MaxConcurrency: 4, MaxCost: 1e6, StatementTimeoutMS: 5000}
		if err := readJSON(r, &in); err != nil {
			writeErr(w, errBadReq(err.Error()))
			return
		}
		if in.OrgID == "" {
			writeErr(w, errBadReq("org_id required"))
			return
		}
		if in.RPS < 0 || in.MaxRows < 0 || in.MaxResultBytes < 0 || in.MaxConcurrency < 0 || in.MaxCost < 0 || in.StatementTimeoutMS < 0 {
			writeErr(w, errBadReq("budget values must be >= 0"))
			return
		}
		var expires any
		if in.TTLSeconds > 0 {
			expires = time.Now().Add(time.Duration(in.TTLSeconds) * time.Second)
		}
		tok := newToken()
		var id string
		err := g.admin.QueryRow(r.Context(), `
			INSERT INTO swarmpg.agents(org_id, name, token_hash, rps_limit, max_rows, max_result_bytes, max_concurrency, max_cost, statement_timeout_ms, read_only, token_expires_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id::text`,
			in.OrgID, in.Name, hashToken(tok), in.RPS, in.MaxRows, in.MaxResultBytes, in.MaxConcurrency, in.MaxCost, in.StatementTimeoutMS, in.ReadOnly, expires).Scan(&id)
		if err != nil {
			writeErr(w, classifyPg(err))
			return
		}
		writeJSON(w, 201, map[string]string{"id": id, "token": tok})
	}))

	mux.HandleFunc("POST /v1/agents/{id}/disable", g.withAdmin(func(w http.ResponseWriter, r *http.Request) {
		_, err := g.admin.Exec(r.Context(), `UPDATE swarmpg.agents SET disabled = true WHERE id = $1`, r.PathValue("id"))
		if err != nil {
			writeErr(w, classifyPg(err))
			return
		}
		g.cache.Range(func(k, v any) bool {
			if v.(*cachedAgent).a.ID == r.PathValue("id") {
				g.cache.Delete(k)
			}
			return true
		})
		writeJSON(w, 200, map[string]bool{"disabled": true})
	}))

	mux.HandleFunc("POST /v1/query", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var q QueryRequest
		if err := readJSON(r, &q); err != nil {
			writeErr(w, errBadReq(err.Error()))
			return
		}
		resp, aerr := g.query(r.Context(), a, q)
		if aerr != nil {
			g.stats.errors.Add(1)
			writeErr(w, aerr)
			return
		}
		writeJSON(w, 200, resp)
	}))

	mux.HandleFunc("GET /v1/schema", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		s, aerr := g.describe(r.Context(), a)
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(s))
	}))

	mux.HandleFunc("PUT /v1/memory/{key}", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in struct {
			Value           json.RawMessage `json:"value"`
			Shared          bool            `json:"shared"`
			ExpectedVersion *int64          `json:"expected_version"`
		}
		if err := readJSON(r, &in); err != nil {
			writeErr(w, errBadReq(err.Error()))
			return
		}
		version, aerr := g.memoryPut(r.Context(), a, r.PathValue("key"), in.Value, in.Shared, in.ExpectedVersion)
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "key": r.PathValue("key"), "version": version})
	}))

	mux.HandleFunc("GET /v1/memory/{key}", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		version, aerr := queryInt(r, "version")
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		var asOf *time.Time
		if s := r.URL.Query().Get("as_of"); s != "" {
			t, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				writeErr(w, errBadReq("as_of must be an RFC 3339 timestamp"))
				return
			}
			asOf = &t
		}
		var from *string
		if r.URL.Query().Has("from") {
			id := r.URL.Query().Get("from")
			from = &id
		}
		m, aerr := g.memoryGet(r.Context(), a, r.PathValue("key"), version, asOf, from)
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		writeJSON(w, 200, m)
	}))

	mux.HandleFunc("GET /v1/memory/{key}/shared", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		for _, selector := range []string{"from", "version", "as_of"} {
			if r.URL.Query().Has(selector) {
				writeErr(w, errBadReq("shared listing cannot be combined with from, version or as_of"))
				return
			}
		}
		out, aerr := g.memoryShared(r.Context(), a, r.PathValue("key"))
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		writeJSON(w, 200, out)
	}))

	mux.HandleFunc("DELETE /v1/memory/{key}", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		expected, aerr := queryInt(r, "expected_version")
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		version, aerr := g.memoryDelete(r.Context(), a, r.PathValue("key"), expected)
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "key": r.PathValue("key"), "version": version})
	}))

	mux.HandleFunc("GET /v1/memory/{key}/history", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		limit, aerr := queryInt(r, "limit")
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		n := 0
		if limit != nil {
			n = int(*limit)
		}
		versions, aerr := g.memoryHistory(r.Context(), a, r.PathValue("key"), n)
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		writeJSON(w, 200, map[string]any{"key": r.PathValue("key"), "versions": versions})
	}))

	mux.HandleFunc("POST /v1/memory/{key}/undo", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in struct {
			ToVersion       *int64 `json:"to_version"`
			ExpectedVersion *int64 `json:"expected_version"`
		}
		if r.ContentLength != 0 {
			if err := readJSON(r, &in); err != nil {
				writeErr(w, errBadReq(err.Error()))
				return
			}
		}
		out, aerr := g.memoryUndo(r.Context(), a, r.PathValue("key"), in.ToVersion, in.ExpectedVersion)
		if aerr != nil {
			writeErr(w, aerr)
			return
		}
		writeJSON(w, 200, out)
	}))

	mux.HandleFunc("POST /v1/events", g.withAgent(func(w http.ResponseWriter, r *http.Request, a *Agent) {
		var in struct {
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := readJSON(r, &in); err != nil {
			writeErr(w, errBadReq(err.Error()))
			return
		}
		out, replay, aerr := g.appendEvent(r.Context(), a, in.Kind, in.Payload, r.Header.Get("Idempotency-Key"))
		if aerr != nil {
			g.stats.errors.Add(1)
			writeErr(w, aerr)
			return
		}
		code := 201
		if replay {
			code = 200
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(out)
	}))

	g.taskRoutes(mux)
	mux.HandleFunc("/mcp", g.handleMCP)

	return mux
}

// readCeilings reads the budget ceilings set on agent_runtime, which the
// gateway may lower per agent but cannot raise. SHOW is a utility statement,
// so it runs before any principal is sealed.
func (g *Gateway) readCeilings(ctx context.Context) error {
	conn, err := g.runtime.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var rows, cost string
	if err := conn.QueryRow(ctx, "SHOW agent_max_rows").Scan(&rows); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, "SHOW agent_max_cost").Scan(&cost); err != nil {
		return err
	}
	if g.ceilRows, err = strconv.Atoi(rows); err != nil {
		return err
	}
	g.ceilCost, err = strconv.ParseFloat(cost, 64)
	return err
}

func mustPool(ctx context.Context, url string, max int32, reset bool) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxConns = max
	cfg.MinConns = max / 4
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := p.Ping(ctx); err != nil {
		log.Fatalf("ping %s: %v", safeURL(url), err)
	}
	return p
}

func safeURL(u string) string {
	if i := strings.Index(u, "@"); i >= 0 {
		if j := strings.Index(u, "://"); j >= 0 {
			return u[:j+3] + "***" + u[i:]
		}
	}
	return u
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	adminURL := env("DATABASE_URL", "postgres://127.0.0.1:5499/swarmpg?sslmode=disable")
	runtimeURL := env("RUNTIME_DATABASE_URL", "")
	if runtimeURL == "" {
		c, err := pgx.ParseConfig(adminURL)
		if err != nil {
			log.Fatal(err)
		}
		runtimeURL = fmt.Sprintf("postgres://agent_runtime@%s:%d/%s?sslmode=%s", c.Host, c.Port, c.Database, sslmode(adminURL))
	}
	adminKey := env("ADMIN_KEY", "dev-admin-key")
	if adminKey == "dev-admin-key" {
		log.Printf("WARNING: using the default admin key; set ADMIN_KEY in any non-local deployment")
	}
	g := &Gateway{
		admin:    mustPool(ctx, adminURL, 4, false),
		runtime:  mustPool(ctx, runtimeURL, 32, true),
		adminKey: adminKey,
	}
	if err := g.startWriters(); err != nil {
		log.Fatalf("configure audit: %v", err)
	}
	if err := g.admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_settings WHERE name = 'agent_principal')`).Scan(&g.sealed); err != nil {
		log.Fatalf("detect kernel: %v", err)
	}
	secret := os.Getenv("PRINCIPAL_SECRET")
	if g.sealed {
		g.principalSecret = []byte(secret)
		if err := g.readCeilings(ctx); err != nil {
			log.Fatalf("read kernel budget ceilings: %v", err)
		}
		signed := "unsigned (set PRINCIPAL_SECRET and agent_principal_secret to sign them)"
		if secret != "" {
			signed = "signed"
		}
		log.Printf("SwarmPG kernel detected: principals are sealed in core, %s; budget ceilings rows=%d cost=%g", signed, g.ceilRows, g.ceilCost)
	} else {
		if secret != "" {
			log.Printf("WARNING: PRINCIPAL_SECRET is ignored on stock Postgres")
		}
		log.Printf("stock Postgres: agent identity is enforced by RLS plus the gateway's statement guard")
	}
	g.mcpOrigins = map[string]bool{}
	for _, o := range strings.Split(os.Getenv("MCP_ALLOWED_ORIGINS"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			g.mcpOrigins[o] = true
		}
	}
	addr := env("ADDR", ":8080")
	srv := &http.Server{
		Addr:              addr,
		Handler:           g.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	log.Printf("swarmpg gateway on %s (runtime pool max=%d)", addr, g.runtime.Config().MaxConns)
	log.Fatal(srv.ListenAndServe())
}

func sslmode(u string) string {
	if strings.Contains(u, "sslmode=") {
		for _, p := range strings.Split(u[strings.Index(u, "?")+1:], "&") {
			if strings.HasPrefix(p, "sslmode=") {
				return strings.TrimPrefix(p, "sslmode=")
			}
		}
	}
	return "prefer"
}
