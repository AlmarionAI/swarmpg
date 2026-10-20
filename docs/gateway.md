# Gateway reference

The gateway sits between agents and Postgres. It authenticates each agent's token, applies
its limits, runs its request as one transaction on a pooled connection under the agent's
identity, cleans the connection, and records what ran. It serves a REST API and an MCP
endpoint, and works on stock Postgres 14+ or the SwarmPG kernel. For a walkthrough, start
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
| `DATABASE_URL` | `postgres://127.0.0.1:5499/swarmpg?sslmode=disable` | Admin connection (a superuser or the schema owner): the agent registry and the audit log |
| `RUNTIME_DATABASE_URL` | `agent_runtime` on the same host and database | The connection all agent SQL runs on; must log in as `agent_runtime` |
| `ADMIN_KEY` | `dev-admin-key` (logged as a warning) | Required in `X-Admin-Key` on admin calls |
| `ADDR` | `:8080` | Listen address. Plain HTTP: terminate TLS in front |
| `PRINCIPAL_SECRET` | none | Kernel only: signs each sealed principal; must equal the server's `agent_principal_secret` |
| `MCP_ALLOWED_ORIGINS` | none | Comma-separated browser origins allowed to call `/mcp`; requests with any other `Origin` get 403 |

The runtime pool holds 32 connections and the admin pool 4. The gateway detects the
kernel at startup and logs which mode it runs in.

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
| `GET /healthz` | none | liveness, request and error counters, pool sizes |

`POST /v1/events` honours an `Idempotency-Key` header: a retry after a timeout replays the
stored response instead of appending twice. Memory writes get the same safety from
`expected_version`. Query errors carry the SQLSTATE `code`, `detail`, `hint`,
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

## What the database enforces

Every agent query runs as one shared low-privilege role, `agent_runtime`. `sql/schema.sql`
makes these hold whatever SQL an agent sends:

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
- **A server-side audit log.** `audit` records every SQL statement the gateway ran or
  refused for an agent: a hash and the first 200 characters of the statement, rows, bytes,
  duration and SQLSTATE. Agents can neither read nor write it.

On top of that the gateway applies each agent's rate, concurrency, row, byte and time
limits; a statement guard that refuses `SET`, transaction control, DDL, `COPY`, cursors and
multiple statements after stripping comments and normalising whitespace; and a reset of
every connection after every request (`CLOSE ALL; RESET ALL; DISCARD TEMP; DISCARD
SEQUENCES`). Audit records and last-seen times are written in batches in the background,
so they never queue in front of agent requests.

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

An agent that reached Postgres directly still could not change or forge its identity, read
another agent's or org's rows, run an unsealed query, or raise a budget; the kernel probes in
`make e2e KERNEL=1` check exactly that. On the kernel, `max_rows` also bounds the rows one
statement may modify: a larger write fails with SQLSTATE 53400 and nothing is applied.
