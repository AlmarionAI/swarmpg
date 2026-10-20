# SwarmPG — Postgres for agentic workloads (phase-1 prototype notes)

> Superseded by `DESIGN.md`. This document described a layer on stock Postgres; the project is
> now a fork of PostgreSQL 19 (`../swarmpg-core`). The gateway here remains the out-of-tree
> frontend prototype and the fleet test harness.

## 1. The thought, analyzed

**Claim:** "Build a version of PostgreSQL that is scalable and built for millions of agents."

**What is true in it**

Agents stress Postgres in ways a web app does not:

| Pressure | Web app | Agent fleet |
|---|---|---|
| Connections | hundreds, long-lived, pooled by the app | millions of short, bursty sessions; each agent run wants "its own database" |
| Tenancy | one schema, tenant_id column | one *principal per agent*, isolation must be enforced by the DB, not by prompt-following |
| Schema | fixed, migrated by humans | ad hoc, JSON-heavy, agents create tables/columns at runtime |
| Writes | OLTP rows | append-only memory, traces, tool-call logs, embeddings |
| Safety | trusted code | untrusted SQL from an LLM: needs budgets, row caps, timeouts, kill switches |
| Undo | rare | constant: "try this, roll back if the plan failed" → branching / time travel |
| Interface | wire protocol via ORM | HTTP/MCP, schema introspection written *for* an LLM |

**What is wrong in it (as literally stated)**

Forking Postgres core is the most expensive and least differentiated path. Aurora, Neon, Yugabyte and Cockroach each spent hundreds of engineer-years and still either (a) kept the Postgres query layer unmodified, or (b) only kept wire compatibility. The agent-specific value is *not* in the executor or planner. It is in:

1. the connection/session model,
2. the identity + isolation model,
3. per-principal resource governance,
4. storage that forks cheaply (branching),
5. the API surface agents actually use.

All five can be built **around** stock Postgres using proxies, extensions (C or Rust via `pgrx`), hooks, background workers and a control plane. Nothing requires patching `src/backend`.

**Where stock Postgres genuinely runs out**

| Limit | Why it bites | Fix |
|---|---|---|
| Process-per-connection (~few thousand max) | millions of agents | Gateway multiplexes agent sessions onto a small pool; agents never hold a connection |
| Catalog bloat (databases/schemas scale to ~10⁴, not 10⁶) | one schema per agent dies | Virtual tenancy: shared tables + RLS keyed on `agent_id`, plus sharding tenants across many Postgres instances by hash |
| Single-writer per cluster | write throughput ceiling | Shard by agent/org across N clusters at the gateway; agents are naturally shard-local |
| No cheap copy-on-write branching | undo / what-if is expensive | Phase 3: Neon-style separated storage, or ZFS/Btrfs snapshots per cluster, or logical "versioned tables" in an extension |
| No per-role CPU/IO quotas | one looping agent starves the fleet | Gateway token buckets + `statement_timeout` + row caps + `pg_stat_statements` feedback |

**Competitive reality (be honest about it)**

Neon, Supabase, Nile, Turso (SQLite), Cloudflare D1/Durable Objects all market "database for agents." The defensible niche is not "Postgres that scales"; it is **agent-native semantics**: identity, isolation, budgets, branching, memory primitives, and an LLM-oriented interface, delivered as one coherent system that runs on any Postgres.

## 2. Architecture

```
            agents (HTTP / MCP / wire-compatible psql)
                          │
          ┌───────────────▼────────────────┐
          │        Agent Gateway (Go)       │  ← this repo, phase 1
          │  auth (agent token → principal) │
          │  budgets: rate, rows, timeout   │
          │  session multiplexing           │
          │  shard routing (agent → cluster)│
          │  LLM-friendly introspection     │
          └───┬──────────┬──────────┬──────┘
              │          │          │
        ┌─────▼───┐ ┌────▼────┐ ┌───▼─────┐
        │ PG shard│ │ PG shard│ │ PG shard│   stock Postgres + extensions
        │ 0       │ │ 1       │ │ N       │   (pgvector, swarmpg ext)
        └─────────┘ └─────────┘ └─────────┘
              ▲
   control plane: provision agent, assign shard, GC idle agents, snapshots
```

**Isolation model.** Every query runs as a low-privilege role inside a transaction that sets `app.agent_id`. All agent-visible tables carry RLS policies keyed on that setting. An agent cannot read another agent's rows even with arbitrary SQL. Organisation-level sharing is a second policy layer (`app.org_id`).

**Session model.** Agents are stateless to the gateway. A "session" is a token plus a budget record; the gateway borrows a pooled connection per request and returns it. 1M agents ≈ a few hundred backend connections per shard.

**Memory primitives** (SQL schema, extension later): `agent_memory` (kv + vector), `agent_events` (append-only, hash-chained for audit), `agent_tables` (agent-created tables registered and RLS-wrapped automatically).

**Governance.** Per-agent: requests/sec, max rows returned, statement timeout, bytes/day, kill switch. Budgets are enforced before SQL hits Postgres and again by `SET LOCAL statement_timeout`.

## 3. Phased plan

| Phase | Deliverable | Postgres changes |
|---|---|---|
| 1 (this repo) | Gateway + schema + RLS + budgets + load test | none, stock PG |
| 2 | Shard routing across N clusters; control plane (provision/GC); MCP server | none |
| 3 | `swarmpg` extension (pgrx): versioned tables, hash-chained event log, agent-safe DDL | extension only |
| 4 | Branching: ZFS snapshots per shard → later separated storage (Neon-compatible pageserver) | storage layer, not core |
| 5 | Wire-protocol front end so `psql`/ORMs connect through the gateway with agent tokens | none |

## 4. What is in this repo

- `sql/schema.sql` — agents, budgets, memory, events, RLS policies
- `gateway/` — Go gateway: `/v1/agents`, `/v1/query`, `/v1/schema`, `/v1/memory`
- `loadtest/` — simulates thousands of concurrent agents through the gateway
- `Makefile` — spin up a throwaway local Postgres, apply schema, run gateway + load test
