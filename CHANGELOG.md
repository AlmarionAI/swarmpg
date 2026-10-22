# Changelog

## Unreleased

- Add a transactional task outbox, administrative delivery claims/leases/fences,
  stable idempotency keys, bounded retry cycles, explicit failed-message requeue and
  attributed delivery history. A separate HTTP dispatcher routes operator-configured
  destinations; external receivers must atomically deduplicate effects. Stock/kernel
  recovery tests kill dispatchers and receivers after durable effects, crash services,
  and verify persisted delivery and receiver ledgers.
- Add org-shared task queues with immutable dependencies and producer-selected input
  snapshots, lease-based claims, monotonic fencing, renew/release/cancel, administrative
  recovery, durable claim replay and atomic result/state/history completion. Eight new
  MCP tools and REST/SQL APIs share the same database contract.
- Return retryable `503 auth_unavailable` when the authentication database is
  unavailable, preserving `401` for genuinely invalid tokens.
- Add a reproducible 100–1,000-worker failure workload with restarts, abandoned leases,
  duplicate requests, stale workers, changed inputs and a noisy tenant; retain correctness,
  latency, retry and recovery reports for stock/kernel comparison.
- Share one attempted-write budget across nested execution, including function calls,
  data-modifying CTEs and trigger bookkeeping; retain atomic rollback for a refused statement.
- Add bounded audit spooling, retained retry batches, stable receipt IDs and optional disk
  persistence. Default `best-effort` mode drops new receipts when full and reports
  `audit_lost`. `strict` mode requires a durable spool and rejects SQL queries before
  execution when an intent cannot be reserved. A crash can leave an explicitly unknown
  outcome; audit and SQL commit are not one transaction.
- Add producer identity and timestamps to memory reads, explicit `from` selection, and a
  complete shared-producer listing over REST and MCP. History remains owner-only.
- Tighten negative probes to require their intended application code/SQLSTATE and HTTP
  status. Unexpected HTTP 5xx, transport failures and failed load requests fail the run.
  Add aggregate, CTE and nested-function write-budget probes and rollback checks.
- Keep workspaces, storage/catalog/process-model rewrites and broader operational
  acceptance separate from the implemented task-coordination milestone.

## 0.1.0 development baseline (2026-10-20)

The initial implementation: Postgres for agent fleets, as a gateway and agent schema that run
on stock Postgres 14+, and a PostgreSQL 19 kernel patch series that enforces agent identity
and budgets in core.

### Added at this baseline

- **Gateway** (`gateway/`): agent tokens, per-agent rate, concurrency, row, byte, cost and
  time limits, a statement guard, a clean connection for every request, and errors that
  carry the SQLSTATE, hint and position.
  - REST API for SQL queries, the schema, memory, and the event journal.
  - MCP endpoint at `/mcp`: protocol 2026-07-28, plus 2025-11-25, 2025-06-18 and
    2025-03-26 through `initialize`. Eight tools, agent-token auth, an Origin allowlist.
- **Agent schema** (`sql/schema.sql`):
  - Forced row-level security on every agent table, and no DDL.
  - Fleet-wide levers revoked: backend signalling, advisory locks, large objects,
    `set_config`, `pg_notify`, `pg_stat_activity` and the statistics functions.
  - A hash-chained, append-only event journal and asynchronous SQL-query audit.
    The initial audit writer was best effort and could lose records.
- **Versioned memory:**
  - Every write is versioned by trigger into a read-only history.
  - Compare-and-set writes with `expected_version`.
  - Reads by version or time, and undo.
  - Available over REST, as MCP tools, and as SQL functions.
- **Kernel** (`kernel/`), PostgreSQL 19rc1 plus two patches:
  - `[principal]`: agent and org sealed together as one value, optionally HMAC-signed,
    with a fail-closed gate that refuses queries until an identity is sealed.
  - `[budget]`: tighten-only budgets, a pre-execution cost gate, and per-statement row caps.
- **Verification reported for this baseline (not current-revision acceptance):**
  - 65 attack probes on stock Postgres and 83 on the kernel, in CI.
  - The upstream regression suite plus two new tests (241/241).
  - Interop tested with the official TypeScript and Python MCP SDKs.
  - Runnable documentation blocks are exercised in CI; server-start commands are skipped.
- **Benchmark** (`bench/`): a reproducible Docker comparison with PgBouncer + row-level
  security at 10,000 agents.
- **Docs:** getting started, the gateway reference, the threat model, and the design with
  implemented behavior against the longer-term plan.

### Known limits

See the residual risks in [docs/THREAT-MODEL.md](docs/THREAT-MODEL.md#residual-risks).
Storage branching, storage-level time travel and per-agent spaces remain unbuilt and
deferred. [docs/DESIGN.md](docs/DESIGN.md) defines the smaller coordination milestone.
