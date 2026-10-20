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
| `POST /v1/agents {org_id,name,rps_limit,max_rows,max_result_bytes,max_concurrency,statement_timeout_ms,read_only,ttl_seconds}` | admin | mint an agent + token (TTL optional) |
| `POST /v1/agents/{id}/disable` | admin | kill switch |
| `POST /v1/query {sql,params}` | agent | one statement, parameterised, row- and byte-capped, timed out |
| `GET /v1/schema` | agent | compact schema text written for an LLM |
| `PUT/GET /v1/memory/{key}` | agent | per-agent JSON memory (org-shareable; GET also returns an org-shared key) |
| `POST /v1/events {kind,payload}` | agent | hash-chained append-only journal; honours `Idempotency-Key` |

Writes honour an `Idempotency-Key` header: a retry after a timeout replays the stored response
instead of duplicating the write. Query errors carry the SQLSTATE `code`, `detail`, `hint`,
`position` and a `retryable` flag so an agent's control loop can decide retry vs repair.

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
the database. On the SwarmPG kernel identity is sealed in core: `schema.sql` makes
`swarmpg.current_agent()` read only the sealed `agent_id` and turns on
`agent_require_principal` for `agent_runtime`, and the gateway detects the kernel at startup
and seals `agent_id` in every transaction. An agent that reached Postgres directly could
still not change its identity or run an unsealed query; the kernel probes in `make e2e
KERNEL=1` check exactly that.

All agents share one role, so Postgres would show each agent the others' in-flight SQL in
`pg_stat_activity`. `schema.sql` revokes the view and its backing functions from `PUBLIC` on
both stock Postgres and the kernel.
