# SwarmPG — a new open-source PostgreSQL built for agent fleets

Status: design v0.3, 2026-10-20. §8 ("Design corrections") revises the mechanism-level claims in
§1.2–§1.6 that a review found contradictory or physically impossible; read §8 alongside those
sections. Supersedes `prototype-architecture.md` (which described a layer on
stock Postgres; that gateway now becomes the out-of-tree prototype of the new frontend).

## 0. The decision

Goal: a new open-source database that **is PostgreSQL at the SQL and wire level** and is **rebuilt
beneath that level** for workloads of thousands to millions of concurrently acting agents.

There are three ways to "recreate PostgreSQL":

| Path | Examples | Years to credible 1.0 | What you get |
|---|---|---|---|
| A. Hard fork of core, rewrite subsystems in place | OrioleDB, Greenplum, Postgres-XL, Babelfish | 2–3 | full compatibility, real control over internals |
| B. Keep the PG query layer, replace storage and distribution underneath | Neon, YugabyteDB | 2–3 | compatibility, new storage semantics, limited control over sessions and catalog |
| C. New engine from scratch, PG-compatible dialect and protocol | CockroachDB, Materialize | 7–10 | total freedom, permanent compatibility gap |

**Decision: A and B together.** Fork PostgreSQL 19 as the compute kernel. Keep the parser, analyzer,
planner, executor, type system, function library, index access methods and the extension ABI.
Replace the process model, the storage layer, the tenancy/catalog model, the security model and
the protocol front. Path C is rejected because the reason to be "PostgreSQL" at all is the
ecosystem (drivers, ORMs, pgvector, PostGIS, tooling, 30 years of planner correctness), and none
of the agent-specific problems live in the query layer.

Rule for the fork: **minimise the core diff, maximise new subsystems.** Attach new code through
existing extension points (table access methods, the storage manager interface, utility and
executor hooks, background workers) and add new hook points to core only where none exist.
Neon keeps its core patch under a few thousand lines and rebases on every major release; we hold
ourselves to the same discipline so we can absorb upstream Postgres yearly.

Naming and licensing: the PostgreSQL License permits forks. Core patches stay under the
PostgreSQL License; new components (page service, frontend, control plane) are Apache-2.0.
"PostgreSQL" is a trademark of the PostgreSQL Community Association; the project is named
distinctly and described as PostgreSQL-compatible.

## 1. What breaks at a million agents, and what the rebuild does about it

### 1.1 Process-per-connection
Postgres forks a backend per connection (~10 MB each, `max_connections` in the low thousands).
Upstream never built in-core pooling because session state (GUCs, temp tables, prepared
statements, advisory locks, cursors) is bound to the backend.

**Rebuild:** a new in-core *frontend* accepts connections asynchronously and owns *sessions* as
plain state objects; a pool of *executor backends* run transactions on behalf of any session.
Agent sessions are stateless by construction: no temp tables, no session GUCs except the sealed
principal, prepared statements cached in the frontend per session. This is what makes in-core
multiplexing tractable here when it was not upstream. Target: 1M attached sessions per compute
node, a few hundred executor backends (the 1M figure is conditional on bounded think-time, see §8.5). Phase 1 builds the frontend out-of-tree (the Go gateway
already in this repo); it moves in-core once the session/backend interface is stable.

### 1.2 Catalog and tenancy
A database per agent is impossible: `CREATE DATABASE` copies a template (seconds, megabytes),
every backend's relcache and syscache scale with catalog size, and autovacuum, statistics and
checkpoints are per-database. Schemas scale to ~10⁴, not 10⁶.

**Rebuild:** a *space* is the agent-facing unit of isolation and is a first-class lightweight
object, not a catalog database. Two tiers:

1. **Tenant-scoped relations** (the 99% case: memory, events, key-value, vectors, task queues).
   One shared relation; every tuple carries the owning space id in its header-adjacent system
   column; the storage layer indexes by `(space, relation, block)`. Scoping is a property of the
   access path, not a policy (corrected in §8.1 to per-(space,relation) virtual relations, which is the real physical boundary): a session sealed to space S cannot address another
   space's blocks. Zero catalog cost per space.
2. **Private DDL.** Agents that create their own tables get catalog rows materialised lazily in a
   per-space namespace, garbage-collected when the space is dropped or idle. Catalog caches are
   partitioned by space so a backend only loads the spaces it is currently serving.

This is the highest-risk research item in the project (see §6).

### 1.3 Storage, vacuum and branching
Heap plus WAL on local disk. No copy-on-write; branching is `pg_basebackup`. VACUUM and 32-bit
transaction-id wraparound are operational burdens that multiply by tenant count. Agents are
append-heavy (logs, traces, memory) and constantly want "try this plan, roll back if it fails".

**Rebuild:** separate compute from storage. Compute nodes are stateless; the storage manager
interface (`smgr`) is redirected to a Rust *page service* that ingests WAL and stores page
versions in an LSM keyed by `(space, relation, block, LSN)` over local NVMe with object storage
as the durable tier. This yields, per space and in O(1):

- **branches** (`CREATE BRANCH b OF SPACE s AT LSN|NOW`) and merges of agent work back to main,
- **time travel** (`SELECT ... AS OF '10 minutes ago'`) for undo and audit,
- **scale-to-zero**: a cold space costs object storage only; compute attaches on first query,
- **no wraparound-driven vacuum storm on compute** (§8.4): old page versions are garbage-collected
  by the page service per space behind an LSN horizon; 64-bit transaction ids remove wraparound (§8.2
  notes this is a page-format change, not a free side effect).

The WAL format is kept unchanged so the page service reuses Postgres's own redo code (as Neon
does), which is the single biggest de-risking choice in the storage design.

### 1.4 Security and governance
Roles are global catalog objects; row-level security keyed on a GUC is spoofable by anyone who
can run SQL; there are no per-role CPU, IO or row quotas; a looping agent can starve a fleet.

**Rebuild:** a *principal* is established at authentication (token → `{agent, org, space,
capabilities, budget}` via a pluggable authenticator) and is **sealed**: immutable for the
session or transaction, exposed as `current_agent()`, `current_space()`. Capabilities are
`read | write | ddl | share | branch` per schema. Budgets (rows, bytes, CPU ms, IO ops,
statement time, concurrent statements) are enforced in the executor and metered to a stream the
control plane bills and alerts on. A kill switch revokes a principal fleet-wide in one round trip.

### 1.5 Protocol
Wire protocol v3 is stateful and designed for long-lived clients.

**Rebuild:** keep v3 for every existing driver. Add, in the frontend, a stateless HTTP/2 and
gRPC mode (one authenticated statement, one response) and a native MCP endpoint. Errors are
structured for LLM repair: error, hint, the offending span, and the relevant schema fragment.
`DESCRIBE FOR AGENT` returns a compact, token-efficient schema.

### 1.6 Distribution
One cluster, one writer.

**Rebuild:** shared-nothing across compute nodes **by space**. A space has one writer compute at a
time; the frontend routes by space id; placement, failover and space→storage-shard maps live in a
Raft-replicated control plane. No distributed transactions in v1: the agent workload is millions
of small databases, not one giant one, and that is the trade-off that makes horizontal scale
cheap. Cross-space reads for org-level sharing go through *shared relations* that are
org-scoped rather than space-scoped.

### 1.7 Agent-native SQL
Lifted into core rather than bolted on:

```sql
CREATE SPACE s OWNER agent '…';                 -- O(1), no template copy
CREATE BRANCH attempt_3 OF SPACE s;             -- copy-on-write at storage layer
SELECT * FROM memory AS OF now() - interval '1 hour';
MERGE BRANCH attempt_3 INTO s;                  -- fast-forward only (§8.4); or DROP BRANCH
CREATE TABLE memory (…) WITH (append_only, hash_chained);   -- enforced by storage
SELECT embedding <-> $1 FROM memory ORDER BY 1 LIMIT 10;    -- vector type in core
SELECT … WITH BUDGET (rows 100, time '500ms');
```

## 2. Architecture

```
  agents ── pg wire v3 ─┐          ┌── HTTP/2 · gRPC · MCP ── agents
                        ▼          ▼
            ┌──────────────────────────────┐
            │  frontend (sessions, auth,   │  stateless; 1M sessions/node
            │  budgets, routing by space)  │
            └──────────────┬───────────────┘
                           │ executor backends (pool)
            ┌──────────────▼───────────────┐
            │  compute kernel = PG 19 fork │  parser · planner · executor · types
            │  + sealed principal          │  + tenant-scoped access path
            │  + space-aware catalog       │  + 64-bit xid
            └──────────────┬───────────────┘
                WAL stream │          │ page reads (space, rel, block, LSN)
            ┌──────────────▼──────────────▼┐
            │  page service (Rust)          │  LSM of page versions; branching;
            │  NVMe cache + object storage  │  GC per space; PG redo code for WAL
            └───────────────────────────────┘
  control plane (Raft): principals, spaces, placement, metering, kill switch, GC
```

## 3. Keep / replace matrix

| Subsystem | Keep | Replace / add |
|---|---|---|
| Parser, analyzer, rewriter, planner, executor | ✔ | new nodes: `AS OF`, `WITH BUDGET`, space DDL |
| Types, functions, operators | ✔ | `vector` moved into core; `current_agent()` family |
| Index AMs (btree, hash, GIN, GiST, BRIN) | ✔ | HNSW in core |
| Heap tuple format, WAL record format | ✔ (so redo code is reusable) | 64-bit xid epoch handling |
| Postmaster / backend process model | ✘ | frontend + executor pool |
| `smgr` / local files / checkpointer / bgwriter | ✘ | remote page service, no local data dir |
| VACUUM / autovacuum | ✘ on compute | GC in page service |
| Catalog structure | ✔ | space virtualisation, partitioned caches |
| Roles, GRANT | ✔ for operators | principals + capabilities for agents |
| RLS | ✔ as a feature | not the isolation mechanism |
| Wire protocol v3 | ✔ | HTTP/2, gRPC, MCP in frontend |
| Extensions ABI | ✔ best-effort | extensions that touch smgr/process model will break |

## 4. Repository layout

```
kernel/            PostgreSQL 19 fork (C) as a patch series, one topic per patch prefix:
                   [principal] [frontend] [space] [smgr-remote] [xid64] [sql-agent]
gateway/           out-of-tree frontend prototype (Go). Becomes the spec for the in-core frontend.
sql/               built-in agent schema until it is generated by initdb
pageserver/        (M2) Rust page service
control/           (M3) Rust control plane
tests/fleet/       (M1) load + isolation harness (today: loadtest/)
```

## 5. Roadmap

| Milestone | Scope | Exit criterion |
|---|---|---|
| **M0 (now)** | Fork builds and passes upstream regression; first core patch: sealed principal + `current_agent()` + regression test | `make check` green with the patch |
| **M1 (0–3 mo)** | Tenant-scoped relations prototype (space id in access path); budget enforcement hooks in executor; frontend multiplexing out-of-tree; fleet test harness | 100k spaces on one node, isolation proven at the access-path level |
| **M2 (3–9 mo)** | `smgr` → page service over local NVMe; WAL ingest with PG redo; per-space branching and time travel | branch in <10 ms regardless of size; compute restarts with no local data |
| **M3 (9–15 mo)** | Space-partitioned catalog caches; lazy private DDL; scale-to-zero; control plane with Raft metadata and metering | 1M spaces across a 3-node cluster; cold start <200 ms |
| **M4 (15–20 mo)** | Frontend in-core; HTTP/2, gRPC, MCP native; routing by space across computes | 1M concurrent sessions per node |
| **M5 (20–24 mo)** | 64-bit xid; vacuum-free compute; object storage tier; `vector`/HNSW in core | 1.0 |

Effort, honestly: 5–8 senior systems engineers for ~24 months to a credible 1.0. Reference
points: Neon reached GA with ~30 engineers over ~3 years; OrioleDB took ~5 engineers ~4 years to
beta. Everything in M0–M1 is doable by one or two people and is what we start today.

## 6. Risks, ranked

1. **Catalog virtualisation (§1.2).** No fork has solved "millions of tenants with private DDL"
   inside the Postgres catalog. Mitigation: tier 1 (tenant-scoped shared relations) covers most
   agent data and needs no catalog change; tier 2 can ship late or be capped per space.
2. **Upstream drift.** Every subsystem we replace is one we must re-merge yearly. Mitigation:
   hook-based attachment, patch series discipline, continuous rebase against `master`.
3. **Extension breakage.** Anything touching smgr or the process model will not load.
   Mitigation: compatibility list; pgvector and PostGIS are first-class test targets.
4. **Page-service correctness.** A bug here is data loss. Mitigation: reuse PG redo code
   unchanged; property-based tests against a stock Postgres oracle on identical WAL.
5. **Scope.** The single most likely failure is building everything at once. Mitigation: the
   milestone exit criteria above are the only definition of progress.
6. **WAL durability (§8.3).** Until an object-storage/quorum tier exists, a committed transaction
   lives only on one page-service node's NVMe. Mitigation: a replicated WAL tier moves to M2, and
   commit semantics are defined explicitly (what is acked before `CommandComplete`).
7. **Think-time transactions (§8.5).** An agent that holds a transaction open across an LLM call
   pins an executor backend, breaking the "1M sessions / few hundred backends" ratio. Mitigation:
   bounded `idle_in_transaction_session_timeout`/`transaction_timeout`, an "open-transaction
   seconds" budget dimension, and optimistic compare-and-set / branch-per-attempt instead of
   interactive transactions.
8. **Seal mechanism (§7, §8.4).** Pool-wide pre-seal via role/db defaults or startup options, and
   a session-scoped seal cleared by transaction abort. Mitigation (landed): establishment-source
   restriction and the fail-closed `agent_require_principal` gate; a multi-field principal needs
   seal-group semantics so a partial seal cannot be completed by the agent.
9. **Economics & lifecycle.** Object-storage cost and GC per space, backup/restore per space,
   right-to-erasure vs time travel, and schema-migration fan-out across millions of tier-2 spaces
   are unmodelled. Mitigation: treat per-space maintenance (freeze-before-sleep, GC) as a metered
   cost and cap tier-2 DDL per space.

## 7. First core patch (M0): sealed principal

Implemented in `swarmpg-core` today:

- A new core GUC flag `GUC_SEALED`, enforced at the single point all `SET`/`RESET`/`set_config()`
  paths pass through. A sealed variable holding a value refuses every assignment, including an
  identical one. `SET LOCAL` seals for the transaction and reverts at commit/abort through the
  normal GUC stack, which is exactly the shape a multiplexing frontend needs.
- GUC `agent_id` (text) carrying that flag. A check-hook design was tried first and rejected:
  PostgreSQL does not re-run check hooks on `RESET`, so it could be cleared.
- An **establishment-source restriction**: a sealed value may only be set by the connected session
  itself, never by a role/database default, the command line, the environment or the startup
  packet. Without this, an operator or a poisoned role default could pre-seal every pooled backend
  and lock the frontend out (or force one identity on the fleet).
- A **fail-closed gate**, `agent_require_principal` (`PGC_SUSET` bool): set per pool role, it makes
  a backend refuse to execute any query plan while `current_agent()` is NULL, so an unsealed or
  abort-cleared session cannot read another principal's rows.
- Built-in `current_agent() RETURNS text` in `pg_catalog`.
- Regression test `agent_identity` (source restriction, fail-closed gate, flag reporting) wired
  into the parallel schedule. `make check`: 240/240.

Known residual (tracked for the in-core frontend, M4): a session-level `SET agent_id` is cleared if
the agent can force a transaction abort, and the GUC seal is per-variable. The frontend therefore
(a) owns the transaction boundary and never lets the agent issue transaction control, (b) runs with
`agent_require_principal = on`, and (c) will seal the whole principal as a group. See §8.4.

## 8. Design corrections (v0.3)

A mechanism-level review found several claims in §1 that are contradictory or physically impossible
as stated. Each is corrected here; the prose in §1 is kept for continuity and should be read with
these.

### 8.1 Tenant isolation is by *virtual relation*, not by a per-tuple column (corrects §1.2)

§1.2 says tier-1 rows carry a space id and "a session sealed to space S physically cannot address
another space's blocks." That is not physical isolation: a heap block holds tuples from many spaces,
so `(space, relation, block)` addressing is a filter a planner bug or a crafted scan can skip, and a
shared B-tree indexes every space's rows together.

Corrected model: a tier-1 relation is a **template**; each `(space, relation)` owns its own
`RelFileNumber` for the heap, its TOAST table, the TOAST index and every index, allocated lazily
from the control-plane space manifest. `rd_locator` is resolved through the sealed `space_id` at
relation open, so the buffer manager, WAL (every block reference carries a `RelFileLocator`) and the
page service all key on that relfilenumber — *that* is the physical boundary. The per-tuple space
column stays only as a belt-and-braces check. Cost: not "zero catalog cost per space" but "no
`pg_class`/`pg_attribute` row per space" — per-virtual-relation `nblocks`/FSM/VM/`relfrozenxid`/
`reltuples` live in the manifest, and the planner scales the template's statistics by the per-space
row count (or reads per-space `ANALYZE` output from the page service).

### 8.2 64-bit xids change the page format; keep them off the v1 critical path (corrects §1.3, §3)

"64-bit transaction ids" while "heap tuple format kept" and "WAL record format never changed" is
self-contradictory: tuple headers store 32-bit `xmin`/`xmax`, and every real 64-bit-xid
implementation adds a per-page xid base in the page special area and touches heap redo. Options,
in preference order: (a) keep 32-bit xids plus epoch handling and let the page service freeze
aggressively — no format change, wraparound handled per space; (b) adopt a page-local xid base,
accepting a heap-page and redo change (drop the "WAL format never changes" rule for that one record
class). v1 takes (a); (b) is a researched M5 item, not a free side effect.

### 8.3 Durability: a WAL tier owns the commit (new; corrects §1.3, §2)

§1.3/§2 route WAL from compute to the page service but never say what acknowledges a COMMIT. Until
object storage (M5) that leaves a committed transaction on one node's NVMe. Correction: add a
replicated WAL tier (safekeeper-style 3-node quorum ack before COMMIT returns, or synchronous
replication of the page service's ingest) and move object-storage upload to **M2, not M5**. Commit
semantics are defined explicitly: the WAL up to the commit LSN must be quorum-durable before
`CommandComplete`. Reusing Neon's Apache-2.0 safekeeper/pageserver is on the table — the design
already reuses PG redo "as Neon does."

### 8.4 `no vacuum on compute`, MERGE, budgets, seal-group (corrects §1.3, §1.4, §1.7)

- **Vacuum.** Overstated. With an unchanged heap format, pruning, hint bits, the visibility map,
  index cleanup and freezing still run on compute and are WAL-logged; the page service only takes
  over *version GC* (old page images behind an LSN horizon). Neon still runs autovacuum. Reword to
  "no wraparound-driven vacuum storm on compute," and add a **freeze-before-sleep** invariant:
  a space may go cold only after its virtual relations are frozen and their `relfrozenxid` recorded,
  so cold spaces never force a wake-and-freeze.
- **MERGE BRANCH.** Page-level copy-on-write gives fork and fast-forward, not a merge of divergent
  branches, and an append-only hash-chained table cannot be merged at all. Honest semantics:
  `MERGE BRANCH` is fast-forward only; divergent merges are a logical, row-level operation with
  conflict detection, out of scope for v1. Default to `DROP BRANCH` or fast-forward.
- **Budgets.** "Enforced in the executor" needs concrete hooks: rows/bytes at `ExecutorRun`,
  statement time via `statement_timeout`, CPU/IO via the existing `pgstat` IO counters sampled at
  `CHECK_FOR_INTERRUPTS`, and a **pre-execution cost gate** (reject when the planner's estimated
  cost exceeds the budget) — the cheapest defense against a runaway plan. "Open-transaction seconds"
  is a budget dimension (§8.5). The kill switch is a control-plane revocation the frontend observes;
  "one round trip" describes the frontend dropping the principal, not an in-core broadcast.
- **Seal-group.** The principal is `{agent, org, space, capabilities, budget}` but the seal is
  per-GUC, so a frontend that seals `agent_id` and forgets `org_id` lets the agent set `org_id`
  itself. Seal the fields as a group (all-or-nothing at connect) or carry the principal in one
  composite sealed value, so a partially-sealed principal cannot be completed by the agent.

### 8.5 v3 compatibility and think-time transactions (corrects §1.1, §1.5)

"Keep v3 for every existing driver" and "sessions are stateless by construction" only reconcile
through a **transaction-pooling** compatibility mode (PgBouncer/Odyssey-style): the frontend tracks
an allow-list of session GUCs (`timezone`, `search_path`, `client_min_messages`, `statement_timeout`,
`DateStyle`, …) and replays them as `SET LOCAL` per transaction, emulates named prepared statements
per backend, and returns a structured error for `LISTEN`, session advisory locks, `WITH HOLD`
cursors and temp tables. Publish the breakage list rather than implying full v3 fidelity.

The agentic hazard is **think-time**: an agent that opens `BEGIN`, calls an LLM for 5–30 s, then
continues pins an executor backend for the whole think-time, so "1M sessions / a few hundred
backends" holds only if that is forbidden or bounded. The frontend therefore sets a short
`idle_in_transaction_session_timeout`/`transaction_timeout` on every agent transaction, exposes
"open-transaction seconds" as a budget, and steers read-modify-write-across-an-LLM-call onto
optimistic compare-and-set (a version column on `memory`) or branch-per-attempt rather than an
interactive transaction. The capacity target is restated as conditional on p99 open-transaction
time.

### 8.6 Org-scoped writes and the in-core frontend (corrects §1.1, §1.6)

- **Org-scoped relations (§1.6).** An org-shared relation needs a defined writer. Either it has its
  own single-writer compute (routed by org id, like a space), and a transaction may write a space
  relation *or* an org relation but not both atomically; or org sharing is read-mostly with writes
  funnelled to the org's writer. A transaction spanning a space and an org relation *is* distributed
  — so v1 forbids it, consistent with "no distributed transactions."
- **In-core frontend (§1.1, M4).** Moving the frontend in-core means replacing the postmaster — the
  largest upstream-drift exposure in the project — for a benefit an out-of-process frontend (Neon
  proxy / PgBouncer / Odyssey) plus a small backend-side session-handoff already delivers. Reframe
  M4 as "a separate high-concurrency frontend process with a thin in-core session-switch protocol,"
  and treat full in-core multiplexing as optional, justified only if the out-of-process design
  measurably caps throughput. This also de-risks extension compatibility (§6.3).

This closes the one gap the out-of-tree prototype could not close (a bare `SET` of the identity
GUC) and establishes the fork's build, test and patch workflow.
