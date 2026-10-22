# Gateway reference

The gateway sits between agents and Postgres. It authenticates each agent's token, applies
its limits, runs its request as one transaction on a pooled connection under the agent's
identity, cleans the connection, and submits SQL-query audit receipts under the configured
delivery mode. It serves REST and MCP, targeting stock Postgres 14+ or the SwarmPG kernel
(stock CI uses Postgres 17). For a walkthrough, start
with [getting-started.md](getting-started.md).

## Run it

<!-- doctest: skip -->
```bash
make pg-start && make gateway                         # stock Postgres on :5499, gateway on :8080
make pg-start KERNEL=1 && make gateway KERNEL=1       # the same on the SwarmPG kernel (after make kernel)
```

`make pg-start` creates a throwaway cluster in `./.pg` (or `./.pg-kernel`) with `trust`
authentication and the current OS user as superuser: a development setup, not a
deployment.

## Configuration

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `postgres://127.0.0.1:5499/swarmpg?sslmode=disable` | Privileged connection URL (a superuser or the schema owner): admin/audit, authentication and admission pools |
| `RUNTIME_DATABASE_URL` | `agent_runtime` on the same host and database | The connection all agent SQL runs on; must log in as `agent_runtime` |
| `ADMIN_KEY` | `dev-admin-key` (logged as a warning) | Required in `X-Admin-Key` on admin calls |
| `ADDR` | `:8080` | Listen address. Plain HTTP: terminate TLS in front |
| `PRINCIPAL_SECRET` | none | Kernel only: signs each sealed principal; must equal the server's `agent_principal_secret` |
| `MCP_ALLOWED_ORIGINS` | none | Comma-separated browser origins allowed to call `/mcp`; requests with any other `Origin` get 403 |
| `AUDIT_MODE` | `best-effort` | `best-effort` drops new receipts when the bounded spool is full; `strict` refuses SQL-query admission unless an intent can be durably reserved |
| `AUDIT_SPOOL_CAPACITY` | `10000` | Maximum outstanding receipts for new admission, including active requests and retries; must be positive; a recovered backlog is never discarded to meet a smaller limit |
| `AUDIT_SPOOL_DIR` | none | Optional persistent spool directory in best-effort mode; required on persistent storage in strict mode. One gateway process owns each directory |

The runtime pool holds 32 connections; administrative/audit, authentication and admission
pools each hold 4, for up to 44 database connections per gateway. Organization admission
is always enabled in the production gateway. Its default local slots reserve 24 for
general work and 8 for task renew/release/complete. See [organization admission](admission.md)
for `ADMISSION_*` settings, shared policies, overload codes and capacity planning. The
gateway detects the kernel at startup and logs which mode it runs in.

## API

| Call | Auth | Purpose |
|---|---|---|
| `POST /v1/orgs {name}` | admin | create an org |
| `GET /v1/orgs/{id}/admission` | admin | shared policy, active permits and cumulative grant counters |
| `PUT /v1/orgs/{id}/admission` | admin | replace the six general/control rate, burst and concurrency limits; [contract](admission.md#organization-policy-api) |
| `POST /v1/agents {org_id,name,rps_limit,max_rows,max_result_bytes,max_concurrency,max_cost,statement_timeout_ms,read_only,ttl_seconds}` | admin | mint an agent + token (TTL optional; `max_cost` defaults to 1e6 and applies on the kernel) |
| `POST /v1/agents/{id}/disable` | admin | kill switch |
| `POST /v1/query {sql,params}` | agent | one statement, parameterised, row- and byte-capped, timed out |
| `GET /v1/schema` | agent | compact schema text written for an LLM |
| `PUT /v1/memory/{key} {value,shared,expected_version}` | agent | write an own key; returns its new `version`. `expected_version` makes it compare-and-set (`0` = must not exist); a stale one is `409 version_conflict` with `current_version` |
| `GET /v1/memory/{key}[?from=UUID\|?version=N\|?as_of=RFC3339]` | agent | read a live key or own historical version; response includes `agent_id`, `changed_at`, `version`, `shared` and `own`. `from` selects a visible live producer |
| `GET /v1/memory/{key}/shared` | agent | return `{key,memories:[]}` containing every visible org-shared live producer, ordered by version descending then agent ID ascending; no partial list |
| `DELETE /v1/memory/{key}[?expected_version=N]` | agent | delete an own key; it stays in history |
| `GET /v1/memory/{key}/history[?limit=N]` | agent | the key's versions, newest first, including deletes |
| `POST /v1/memory/{key}/undo {to_version,expected_version}` | agent | restore the state before the latest change, or `to_version`, as a new version |
| `POST /v1/events {kind,payload}` | agent | hash-chained append-only journal; honours `Idempotency-Key` |
| `/v1/tasks` and `/v1/tasks/{id}/…` | agent | enqueue/get/history, claim, renew, release, complete and cancel; [task contract and route reference](tasks.md) |
| `POST /v1/admin/tasks/{id}/recover {reason}` | admin | invalidate a nonterminal claim and requeue, with attributed recovery history |
| `GET /v1/tasks/{id}/outbox` | agent | organization-scoped committed delivery intents and their current status |
| `/v1/admin/outbox/…` | admin | claim/renew/ack/nack, inspect and retry external deliveries; [outbox contract and dispatcher](outbox.md) |
| `POST /mcp` | agent | the same operations as MCP tools (see below) |
| `GET /healthz` | none | liveness, request/error counters, pool sizes, audit status and local admission counters/capacities |
| `GET /readyz` | none | controller presence, admission-pool connectivity and required admission catalog objects |

`POST /v1/events` honours an `Idempotency-Key` header: a retry after a timeout replays the
stored response instead of appending twice. Memory `expected_version` detects a stale or
repeated write with a conflict; it does not replay the original response. Query errors carry the SQLSTATE `code`, `detail`, `hint`,
`position` and a `retryable` flag so an agent's control loop can decide retry vs repair.

## Shared memory reads

With no selector, a live read prefers your own key. If it is absent, the fallback is an
org-shared producer ordered by `version DESC, agent_id ASC`. Versions belong to individual
producers: that ordering is deterministic, not a claim about which producer is most
correct or most recent across the organization. Use `from=<agent UUID>` when provenance
matters, or list `/shared` and choose explicitly.

`from` can select your own live key, or a peer's shared key in your org. An invisible or
missing producer/key returns `404 not_found`. A shared listing includes your own key only
if it is shared; no matches returns `memories: []`. If the complete listing cannot fit the
row or byte result budget, it returns `413 result_limit_exceeded` instead of silently
omitting producers. Select a producer or raise the result budget.

Historical reads (`version` or `as_of`) remain owner-only. Do not combine them with each
other, `from`, or shared listing. REST shared listing rejects all selectors; MCP
`list_shared: true` rejects `from`, `version` and `as_of`. Invalid combinations return
`400 bad_request`. A peer's shared live value never exposes its private history.

## Audit delivery

Audit covers authenticated SQL-query calls through `POST /v1/query` and the MCP `query`
tool, including statements rejected by the guard. It does **not** cover typed task, memory or
event API/tool writes, raw SQL sent directly to PostgreSQL, or requests rejected before
query admission. Using strict audit does not extend this scope.

- **Best effort (default).** Admission does not wait for audit-database availability or
  free spool space. When the spool is full or cannot accept a receipt, the query proceeds
  and `audit_lost` increments. Without `AUDIT_SPOOL_DIR`, queued receipts live only in
  memory and are lost on process failure. A directory enables disk persistence but does
  not change the drop-on-admission-failure policy; disk sync adds request latency.
- **Strict.** Startup requires `AUDIT_SPOOL_DIR`. Place it on persistent storage and keep
  it available across process restarts. Before SQL execution, the gateway reserves a
  receipt and fsyncs its intent. If it cannot do that, REST returns `503 audit_unavailable`
  and the statement has not executed; MCP reports the same code as a tool error. It does
  not wait indefinitely for a queue slot. Completion is persisted and fsynced afterwards.
- **Retries and bounds.** The spool capacity includes active queries and outstanding
  retry receipts. Failed database batches stay queued. Delivery uses exponential backoff
  starting from a 50 ms interval, capped at 5 s; `audit_retries` counts failed delivery or
  cleanup attempts. Stable receipt UUIDs deduplicate retries if a batch committed but its
  acknowledgement was lost. Receipts are removed after database acknowledgement.
- **Crash semantics.** SQL commit and audit completion are separate writes. A crash after
  SQL commit but before completion persistence can leave `audit_outcome_unknown`. An
  unknown outcome does not mean rollback, and must not trigger a blind replay of a write.
  If completion persistence fails, the durable intent remains and the completed receipt
  is retained in memory for retry. Strict mode is not transactional exactly-once auditing
  or exactly-once SQL execution; loss of the spool volume can still lose pending records.

Receipts contain identity, a statement hash and the first 200 characters, parameter count,
rows/bytes/affected rows, duration and error status (error text capped at 1,000 characters).
They do not contain parameter values or full query results. Statement samples can still
contain literals; secure the spool and audit table accordingly. Monitor `queued` relative
to `capacity`, `audit_lost`, `audit_retries`, and durable-volume health. `durable_spool`
reports that a disk directory is configured; it is not a replication or backup guarantee.

## MCP endpoint

`POST /mcp` serves the Model Context Protocol over Streamable HTTP. It is stateless: one
JSON-RPC message per POST, one JSON response (202 for a notification), no sessions, no
server-sent events. `GET` and `DELETE` return 405.

| Tool | Does | Same as |
|---|---|---|
| `schema` | describes the agent's tables and the SQL rules | `GET /v1/schema` |
| `query` `{sql, params}` | one parameterised statement, budget-capped | `POST /v1/query` |
| `memory_get` `{key, from, list_shared, version, as_of}` | read a visible live producer, list all shared producers, or read own history; selectors are mutually exclusive | `GET /v1/memory/{key}` or `/shared` |
| `memory_put` `{key, value, shared, expected_version}` | write an own key, optionally compare-and-set | `PUT /v1/memory/{key}` |
| `memory_delete` `{key, expected_version}` | delete an own key | `DELETE /v1/memory/{key}` |
| `memory_history` `{key, limit}` | the key's versions, newest first | `GET /v1/memory/{key}/history` |
| `memory_undo` `{key, to_version, expected_version}` | undo the latest change, or restore a version | `POST /v1/memory/{key}/undo` |
| `events_append` `{kind, payload, idempotency_key}` | append to the journal; a repeated key replays | `POST /v1/events` |
| `task_enqueue`, `task_get`, `task_claim`, `task_renew`, `task_release`, `task_complete`, `task_cancel`, `task_history`, `task_outbox` | database-enforced task coordination and outbox status | [task API reference](tasks.md#rest-and-mcp) |

- **Versions.** 2026-07-28 requests carry `io.modelcontextprotocol/protocolVersion` and
  `clientCapabilities` in `params._meta`, mirrored in the `MCP-Protocol-Version`,
  `Mcp-Method` and (for `tools/call`) `Mcp-Name` headers. A mismatch returns 400 with
  -32020, and an unsupported version returns 400 with -32022 listing the supported ones.
  `server/discover` describes the server. Older clients use `initialize` (2025-11-25,
  2025-06-18, 2025-03-26) and get the same tools.
- **Auth and limits.** `Authorization: Bearer <agent token>` on every request; a missing or
  bad token is 401 with `WWW-Authenticate`. An unavailable authentication database returns
  retryable 503 `auth_unavailable`, without a bearer challenge. Rate, concurrency and every budget apply as on
  the REST API.
- **Origin.** A request with an `Origin` header is refused with 403 unless that origin is in
  `MCP_ALLOWED_ORIGINS` (comma-separated), which blocks DNS-rebinding attacks from
  browsers. Server-side agents send no `Origin`.
- **Errors.** Unknown tools and malformed requests are JSON-RPC errors. A tool that runs and
  fails (bad SQL, budget exceeded, not found, invalid arguments) returns a result with
  `isError: true` and the same error body as the REST API, so the model can read it and
  repair its call.

## What the database enforces

Every agent query runs as one shared low-privilege role, `agent_runtime`. `sql/schema.sql`
enforces the following after a trustworthy principal has been established. On stock
Postgres, that identity depends on requests staying behind the gateway:

- **Isolation.** Agent data lives in shared tables (`memory`, `memory_history`,
  `documents`, `events`) with forced row-level security keyed on the agent's identity.
  Org-shared memory is readable by peers and writable only by its owner.
- **No objects.** Agents cannot create anything; an event trigger refuses all agent DDL.
  `documents` (JSON in named collections) stands in for private tables.
- **No fleet-wide levers.** Revoked from agents: `set_config`, backend signalling, the
  advisory-lock family, large objects, `pg_notify`, `pg_stat_activity` and every
  `pg_stat_get_*` statistics function (still granted to `pg_monitor`), and `SELECT` on the
  event sequence. Procedural languages are revoked, so agents cannot write functions.
- **Versioned memory.** Triggers version every memory write and keep a read-only history;
  the compare-and-set, history and undo functions run with the agent's own rights.
- **A tamper-evident journal.** `events` is append-only and hash-chained per agent; each
  hash binds the agent, sequence and canonical payload, so an auditor can recompute it.
- **An operator-only audit table.** Agents can neither read nor write audit records or
  call either audit insertion helper. Receipt delivery follows the audit contract above.

On top of that the gateway applies shared organization rate/concurrency admission and
locally fair queues, then each agent's row, byte and time limits. Per-agent rate and
concurrency limits remain process-local and are separate for general/control requests.
The fixed admission execution deadline also caps the server timeout, even if the agent
timeout is zero. The gateway applies a statement guard that refuses `SET`, transaction
control, DDL, `COPY`, cursors and multiple statements after stripping comments and
normalising whitespace; and a reset of
every connection after every request (`CLOSE ALL; RESET ALL; DISCARD TEMP; DISCARD
SEQUENCES`). Audit delivery and last-seen updates run in background batches. Audit
admission follows the selected policy; strict disk persistence adds synchronous work.

The probes that check all of this, and what is not covered, are in
[THREAT-MODEL.md](THREAT-MODEL.md).

## Stock Postgres vs the SwarmPG kernel

On stock Postgres a bare `SET app.agent_id = ...` is blocked by the gateway's guard, not by
the database. On the SwarmPG kernel identity is sealed in core. `schema.sql` makes
`swarmpg.current_agent()` and `swarmpg.current_org()` read only the sealed `agent_principal`,
turns on `agent_require_principal`, and sets budget ceilings on `agent_runtime`. The gateway
detects the kernel at startup, and in every transaction:

- seals `agent_principal = 'agent=<id>,org=<org>'`, adding `,sig=<HMAC-SHA256>` when
  `PRINCIPAL_SECRET` is set (it must equal the server's `agent_principal_secret`);
- lowers `agent_max_rows` to the agent's `max_rows + 1` and `agent_max_cost` to its `max_cost`.

With a principal secret configured, an agent reaching Postgres directly cannot forge a
principal without the secret, change a sealed identity, or raise the role's row/cost
ceilings. Private rows remain protected by RLS. An unsealed query is refused. Raw SQL still
has the session-setting and capability gaps listed in the threat model.

The kernel's `agent_max_rows` counts cumulative attempted ModifyTable rows across nested
functions, data-modifying CTEs and triggers for one statement. Internal bookkeeping counts:
a memory put/delete/undo normally writes memory plus history; an event append also updates
the event head. A refused statement raises `53400` (HTTP 400) and rolls back its writes.
Returned-row limiting is separate. The gateway retains its `max_rows + 1` kernel ceiling to
detect result truncation, so the kernel write ceiling also has that one-row headroom;
`max_rows` is not an exact count of user-visible changed records. Non-atomic raw `CALL`
transactions are a separate boundary; the gateway refuses `CALL`.
