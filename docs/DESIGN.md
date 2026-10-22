# SwarmPG design and next milestone

Status: development design, 2026-10-22. This document separates implemented behavior from
proposed work. It is not a production-readiness statement or a launch announcement.

## The problem to solve

A fleet needs to know which agent may read or change each piece of state, what a worker
has claimed, and whether a result is still valid after retries or a crash. PostgreSQL
already supplies transactions, locking, indexes and recovery. SwarmPG should make those
capabilities dependable and accessible to agents while keeping its core changes small.

The current kernel contribution is a sealed principal for a shared connection pool,
plus executor budgets. A trusted gateway can protect stock PostgreSQL; distinct database
roles can also provide strong identity. SwarmPG's kernel makes the shared-role principal
immutable inside the transaction and, when signing is configured, prevents a raw SQL
client from choosing an identity. This is the advantage to preserve and measure.

## What is implemented

| Capability | Current boundary |
|---|---|
| Pooled REST/MCP access | Go gateway, 32 runtime connections, short transactions, per-process admission limits |
| Agent identity | One composite agent/org principal; kernel seal, optional HMAC verification and fail-closed execution gate |
| Private data and sharing | Shared tables under forced RLS; own writes, org-readable shared memory; no private DDL |
| Budgets | Gateway rate/concurrency/time/result limits; kernel plan-cost gate and cumulative attempted-write cap |
| Memory | Per-key versions, compare-and-set, history, time reads and undo; producer identity, explicit producer selection and shared listing |
| Records | Hash-chained per-agent journal; SQL-query audit with bounded retry spooling and a strict durable-intent mode |
| Validation | Go tests, stock/kernel e2e probes and upstream kernel regressions; a small independent-agent benchmark |

Memory history is logical history for this schema, not PostgreSQL storage-level time
travel. A memory key belongs to one producer. Selecting a peer's shared value does not
grant permission to overwrite it. There is no task queue, lease protocol, fencing-token
API, shared editable workspace or branch/merge workflow yet.

The write budget is cumulative across nested function execution, CTEs and triggers.
Internal writes count too: a memory update also records history. It is a cap on attempted
ModifyTable rows, not CPU time, IO or bytes written. Returned-row limiting is separate;
[the gateway reference](gateway.md#stock-postgres-vs-the-swarmpg-kernel) explains its
one-row result-detection headroom and remaining raw-session limits.

## Current architecture

```text
Agents → REST / MCP gateway → pooled agent_runtime connections → PostgreSQL
            │                          │                         + RLS/schema
            │                          └─ signed sealed principal + optional kernel patches
            └─ bounded audit spool → admin connection → audit receipts
```

The gateway remains outside PostgreSQL. Keep the upstream parser, planner, storage,
WAL, catalogs, process model and autovacuum. New core changes need a demonstrated
requirement that a gateway, schema or existing extension point cannot satisfy.

Audit is a separate durability boundary. Strict mode fsyncs an intent before admitting a
SQL query and persists completion afterwards. The SQL transaction and audit completion
are not atomic: a crash between them produces `audit_outcome_unknown`. Stable receipt
IDs deduplicate delivery, not execution of arbitrary SQL. See [audit delivery](gateway.md#audit-delivery).

## Next milestone: durable task coordination

**Planned; not implemented.** Build coordination on the current transactional database
before introducing workspaces or replacing PostgreSQL internals.

1. **Task claims.** A worker atomically claims an eligible task; ownership and state
   transitions are checked in the database. Concurrent claimers cannot both acquire the
   same current claim. Include organization boundaries and explicit authorization.
2. **Leases and recovery.** Claims expire using a documented database-time rule. Heartbeats
   renew a current claim; another worker can reclaim abandoned work. Agents do not hold
   database transactions open while waiting on a model or an external tool.
3. **Fencing tokens.** Each new claim gets a monotonically advancing token. Every task
   state update and completion checks it, so a paused worker cannot resume and overwrite
   the new owner's result. External systems must support fencing or idempotency for the
   same guarantee to extend beyond PostgreSQL.
4. **Atomic completion.** Persist the result and transition the task to completed in one
   transaction, guarded by the current claim/token. Retried completion returns a defined
   outcome. If publishing an external event is required, use an outbox and explicitly
   document its delivery/retry contract; do not promise exactly-once external effects.

Define the SQL contract first, then expose it consistently over REST and MCP. Add clear
conflict, stale-claim and expired-lease errors, bounded retries, inspectable task history
and administrative recovery. This milestone does not require a new page service.

## Acceptance workload: collaborative work under failure

**Planned; not implemented.** Run 100–1,000 worker identities on a shared task graph with
real dependencies and competing claims. Mix workers that complete useful steps with
workers that retry, crash, pause past their lease, or attempt stale writes. Exercise
producer-selected shared memory as input to the workflow. Include one noisy organization
beside others and inject gateway/database disconnects and restarts.

Run the same schema, task contract, workload, pool sizes and durability settings on:

- A secure stock-Postgres deployment through the SwarmPG gateway.
- The same gateway and workload on the SwarmPG kernel.

Separate any PgBouncer comparison from the kernel comparison. Equalize transaction
batching and audit mode; a vulnerable user-writable identity setting is not the sole
baseline for a production security claim. Repeated, sustained runs must include dataset
and history growth, not just a few seconds of fresh small keys.

| Acceptance question | Required evidence |
|---|---|
| Can workers collaborate correctly? | Valid completed tasks/results; no conflicting current owners; dependencies preserved |
| Can stale workers damage state? | Stale tokens rejected after reclaim, with reproducible timing/fault traces |
| Do retries duplicate work? | Defined completion replay and measured duplicate attempts; external-effect limits stated |
| Does failure lose work? | Abandoned tasks reclaimed; no lost committed task results; time to recover reported |
| Is one noisy agent contained? | Per-org throughput and p95/p99 completion latency, rejection reasons and queue delay |
| Does the kernel help? | Same-workload stock/kernel results, resource overhead and controls that remain enforced under raw SQL |
| Is the evidence reproducible? | Exact commit, environment, settings, seeds, faults, durations and raw results retained |

Count successful completed work, correctness failures, retry amplification and recovery
time alongside requests/sec. Do not claim a million-agent system from a count of
registered identities or idle connections.

## Work after the coordination proof

**Planned; not implemented.** Design shared workspaces around explicit membership and
capabilities once the task contract has been validated. Decide ownership, provenance,
conflict handling, retention and deletion before adding more shared-write APIs. Keep
private agent memory and shared workspace state distinct.

Operational readiness is also required: schema migration and rollback procedures,
backup/restore and PITR tests, deployment authentication/TLS, token rotation, audit-volume
operations, retention/purge, observable admission limits, multi-gateway fairness and
failure recovery. The development Compose setup is not a production deployment recipe.
Extension compatibility and supported PostgreSQL versions need explicit test matrices.

## Deferred research

These are outside the near-term plan and have no delivery dates or capacity promises:

| Topic | Evidence needed before committing to it |
|---|---|
| Storage branching / remote page service | A workflow that needs it, measured limits of existing options, and a complete WAL durability/recovery design |
| Catalog virtualization / millions of private spaces | Demonstrated catalog or isolation bottleneck; metadata lifecycle, maintenance and migration costs quantified |
| Process-model replacement / in-core multiplexing | Evidence the out-of-process gateway is the limiting factor and extension compatibility can be retained |
| Scale-to-zero / distributed compute | Placement, failover, admission, persistence and cold-start requirements measured against actual workloads |
| New transaction-ID or page formats | A correctness argument, WAL/upgrade implications and a justified need beyond upstream behavior |
| Vector/HNSW in core | An advantage over extension-based retrieval that outweighs maintenance and compatibility cost |

If storage research resumes, keep these constraints explicit: page-copy branching does
not merge divergent writes; physical isolation cannot be inferred from a per-row tenant
column; 64-bit transaction IDs can affect page/WAL formats; a page-version service does
not remove heap pruning, index cleanup or freezing; and a commit acknowledgement needs
a defined durable boundary. Do not revive the earlier rebuild plan as an implicit
prerequisite for the coordination milestone.
