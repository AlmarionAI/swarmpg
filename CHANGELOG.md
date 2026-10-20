# Changelog

## 0.1.0 (2026-10-20)

The first public release: Postgres for agent fleets, as a gateway and agent schema that run
on stock Postgres 14+, and a PostgreSQL 19 kernel patch series that enforces agent identity
and budgets in core.

### Added

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
  - A hash-chained, append-only event journal, and a server-side audit log of every SQL
    statement an agent sends, refused ones included.
- **Versioned memory:**
  - Every write is versioned by trigger into a read-only history.
  - Compare-and-set writes with `expected_version`.
  - Reads by version or time, and undo.
  - Available over REST, as MCP tools, and as SQL functions.
- **Kernel** (`kernel/`), PostgreSQL 19rc1 plus two patches:
  - `[principal]`: agent and org sealed together as one value, optionally HMAC-signed,
    with a fail-closed gate that refuses queries until an identity is sealed.
  - `[budget]`: tighten-only budgets, a pre-execution cost gate, and per-statement row caps.
- **Verification:**
  - 65 attack probes on stock Postgres and 83 on the kernel, in CI.
  - The upstream regression suite plus two new tests (241/241).
  - Interop tested with the official TypeScript and Python MCP SDKs.
  - Every command in the README and guides runs in CI.
- **Benchmark** (`bench/`): a reproducible Docker comparison with PgBouncer + row-level
  security at 10,000 agents.
- **Docs:** getting started, the gateway reference, the threat model, and the design with
  what v0.1 ships against the long-term plan.

### Known limits

See the residual risks in [docs/THREAT-MODEL.md](docs/THREAT-MODEL.md#residual-risks).
The storage rebuild in [docs/DESIGN.md](docs/DESIGN.md) is not built yet; this includes
branching, storage-level time travel and per-agent spaces.
