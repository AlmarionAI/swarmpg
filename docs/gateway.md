# SwarmPG gateway (out-of-tree frontend prototype)

Postgres for agent fleets: one gateway in front of Postgres gives every agent its own identity,
database-enforced isolation, budgets, an append-only audit journal, a server-side audit log and an
LLM-friendly API, while thousands of agents share a few dozen backend connections.

This is the phase-1 prototype and the executable specification for the in-core frontend
(`DESIGN.md`, milestone M4). It runs on **stock Postgres 14+**; on the SwarmPG kernel
(`../swarmpg-core`) the sealed principal and `agent_require_principal` replace the gateway's
statement guard as the primary control.

## Quickstart (macOS/Linux, Postgres 14+ and Go 1.23+ installed)

```bash
make pg-start          # throwaway cluster in ./.pg on :5499, schema applied
make gateway           # :8080, admin key "dev-admin-key" (override with ADMIN_KEY)
make loadtest AGENTS=2000 REQUESTS=10   # in another shell; exits non-zero if any isolation probe fails
make pg-stop
```

`make pg-start` assumes Homebrew/packaged Postgres on `PATH` with `trust` auth and the current OS
user as a superuser (prototype convenience — not a deployment posture). The cluster and its WAL
live in `./.pg`, which is git-ignored.

## API

| Call | Auth | Purpose |
|---|---|---|
| `POST /v1/orgs {name}` | admin | create an org |
| `POST /v1/agents {org_id,name,rps_limit,max_rows,max_result_bytes,max_concurrency,max_cost,statement_timeout_ms,read_only,ttl_seconds}` | admin | mint an agent + token (TTL optional; `max_cost` defaults to 1e6 and applies on the kernel) |
| `POST /v1/agents/{id}/disable` | admin | kill switch |
| `POST /v1/query {sql,params}` | agent | one statement, parameterised, row- and byte-capped, timed out |
| `GET /v1/schema` | agent | compact schema text written for an LLM |
| `PUT /v1/memory/{key} {value,shared,expected_version}` | agent | write an own key; returns its new `version`. `expected_version` makes it compare-and-set (`0` = must not exist); a stale one is `409 version_conflict` with `current_version` |
| `GET /v1/memory/{key}[?version=N\|?as_of=RFC3339]` | agent | read a key with its `version` (live reads fall back to an org-shared key, `own: false`); `version`/`as_of` read the agent's own key as it was |
| `DELETE /v1/memory/{key}[?expected_version=N]` | agent | delete an own key; it stays in history |
| `GET /v1/memory/{key}/history[?limit=N]` | agent | the key's versions, newest first, including deletes |
| `POST /v1/memory/{key}/undo {to_version,expected_version}` | agent | restore the state before the latest change, or `to_version`, as a new version |
| `POST /v1/events {kind,payload}` | agent | hash-chained append-only journal; honours `Idempotency-Key` |
| `POST /mcp` | agent | the same operations as MCP tools (see below) |

Writes honour an `Idempotency-Key` header: a retry after a timeout replays the stored response
instead of duplicating the write. Query errors carry the SQLSTATE `code`, `detail`, `hint`,
`position` and a `retryable` flag so an agent's control loop can decide retry vs repair.

## MCP endpoint

`POST /mcp` serves the Model Context Protocol over Streamable HTTP. It is stateless: one
JSON-RPC message per POST, one JSON response (202 for a notification), no sessions, no
server-sent events. `GET` and `DELETE` return 405.

| Tool | Does | Same as |
|---|---|---|
| `schema` | describes the agent's tables and the SQL rules | `GET /v1/schema` |
| `query` `{sql, params}` | one parameterised statement, budget-capped | `POST /v1/query` |
| `memory_get` `{key, version, as_of}` | own key, else an org-shared key of that name; or an own key as it was | `GET /v1/memory/{key}` |
| `memory_put` `{key, value, shared, expected_version}` | write an own key, optionally compare-and-set | `PUT /v1/memory/{key}` |
| `memory_delete` `{key, expected_version}` | delete an own key | `DELETE /v1/memory/{key}` |
| `memory_history` `{key, limit}` | the key's versions, newest first | `GET /v1/memory/{key}/history` |
| `memory_undo` `{key, to_version, expected_version}` | undo the latest change, or restore a version | `POST /v1/memory/{key}/undo` |
| `events_append` `{kind, payload, idempotency_key}` | append to the journal; a repeated key replays | `POST /v1/events` |

- **Versions.** 2026-07-28 requests carry `io.modelcontextprotocol/protocolVersion` and
  `clientCapabilities` in `params._meta`, mirrored in the `MCP-Protocol-Version`,
  `Mcp-Method` and (for `tools/call`) `Mcp-Name` headers. A mismatch returns 400 with
  -32020, and an unsupported version returns 400 with -32022 listing the supported ones.
  `server/discover` describes the server. Older clients use `initialize` (2025-11-25,
  2025-06-18, 2025-03-26) and get the same tools.
- **Auth and limits.** `Authorization: Bearer <agent token>` on every request; a missing or
  bad token is 401 with `WWW-Authenticate`. Rate, concurrency and every budget apply as on
  the REST API.
- **Origin.** A request with an `Origin` header is refused with 403 unless that origin is in
  `MCP_ALLOWED_ORIGINS` (comma-separated), which blocks DNS-rebinding attacks from
  browsers. Server-side agents send no `Origin`.
- **Errors.** Unknown tools and malformed requests are JSON-RPC errors. A tool that runs and
  fails (bad SQL, budget exceeded, not found, invalid arguments) returns a result with
  `isError: true` and the same error body as the REST API, so the model can read it and
  repair its call.

## What the database enforces (not the prompt, not the gateway)

Every agent query runs as one shared low-privilege role, `agent_runtime`, with identity carried in
transaction-local `app.agent_id`/`app.org_id`. `sql/schema.sql`:

- **Isolation** — agent data lives in tenant-scoped shared tables (`memory`, `documents`, `events`)
  with `FORCE ROW LEVEL SECURITY` keyed on the principal. Org-shared memory rows are readable by
  peers but writable only by the owner (separate per-command policies).
- **No DDL / no object ownership** — agents cannot `CREATE` anything. A shared DDL scratch schema
  cannot be isolated per-agent on stock Postgres under pooling, so "private DDL" is deferred
  (DESIGN.md tier 2). Agents use `swarmpg.documents` (arbitrary JSON in named collections) instead.
- **No fleet-wide levers** — `set_config`, `pg_terminate_backend`/`pg_cancel_backend`, the advisory
  lock family and large objects are revoked from `PUBLIC`; `plpgsql`/`sql` languages are revoked
  from agents; an event trigger blocks all agent DDL.
- **Tamper-evident journal** — `swarmpg.events` is append-only and hash-chained; the chain is
  serialized per agent (a chain-head row under `FOR UPDATE`), so concurrent appends never fork, and
  each hash binds the agent, sequence and canonical payload so an auditor can recompute it.
- **Server-side audit** — `swarmpg.audit` records what the gateway actually executed (statement
  hash, rows, bytes, duration, SQLSTATE) through a `SECURITY DEFINER` function agents cannot read or
  write. This is the real audit trail; `events` is the agent's own journal.

The gateway adds, on top of the database boundary: a byte budget and per-agent concurrency cap
(one agent cannot exhaust the pool or OOM the frontend), a statement guard that strips comments and
normalises whitespace before classifying, per-request connection reset (`CLOSE ALL; RESET ALL;
DISCARD TEMP; DISCARD SEQUENCES`) so nothing leaks across the pool, precision-preserving parameter
decoding, request-body and server timeouts, and a constant-time admin-key check.

`make loadtest` drives thousands of agents through the gateway and then runs 25 isolation probes
that **assert** their outcome (identity spoofing, RLS reads, DDL, pool-leak via temp/cursor,
backend termination, advisory locks, registry/audit reads, log tampering); it exits non-zero if any
probe fails.

## Stock Postgres vs the SwarmPG kernel

On stock Postgres a bare `SET app.agent_id = ...` is blocked by the gateway's guard, not by
the database. On the SwarmPG kernel identity is sealed in core. `schema.sql` makes
`swarmpg.current_agent()` and `swarmpg.current_org()` read only the sealed `agent_principal`,
turns on `agent_require_principal`, and sets budget ceilings on `agent_runtime`. The gateway
detects the kernel at startup, and in every transaction:

- seals `agent_principal = 'agent=<id>,org=<org>'`, adding `,sig=<HMAC-SHA256>` when
  `PRINCIPAL_SECRET` is set (it must equal the server's `agent_principal_secret`);
- lowers `agent_max_rows` to the agent's `max_rows + 1` and `agent_max_cost` to its `max_cost`.

An agent that reached Postgres directly still could not change or forge its identity, read
another agent's or org's rows, run an unsealed query, or raise a budget; the kernel probes in
`make e2e KERNEL=1` check exactly that. On the kernel, `max_rows` also bounds the rows one
statement may modify: a larger write fails with SQLSTATE 53400 and nothing is applied.

All agents share one role, so Postgres would show each agent the others' in-flight SQL in
`pg_stat_activity`. `schema.sql` revokes the view and its backing functions from `PUBLIC` on
both stock Postgres and the kernel.
