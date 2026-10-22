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
| Pooled REST/MCP access | Go gateway, 32 runtime connections, short transactions, local queues and shared organization admission |
| Agent identity | One composite agent/org principal; kernel seal, optional HMAC verification and fail-closed execution gate |
| Private data and sharing | Shared tables under forced RLS; own writes, org-readable shared memory; no private DDL |
| Budgets | Shared organization rate/concurrency ceilings; local per-agent limits; gateway time/result limits; kernel plan-cost and attempted-write caps |
| Admission fairness | Per-gateway organization round-robin queues, independent shared general/control quotas, reserved renewal/completion capacity |
| Memory | Per-key versions, compare-and-set, history, time reads and undo; producer identity, explicit producer selection and shared listing |
| Records | Hash-chained per-agent journal; SQL-query audit with bounded retry spooling and a strict durable-intent mode |
| Tasks | Org-shared queues, immutable dependencies/input snapshots, atomic claims, leases, fencing, completion replay and attributed history |
| Recovery | Opt-in primary identity/timeline pins, synchronous barriers and generation-based PITR quarantine; external fencing and effect reconciliation remain required |
| Validation | Go tests, stock/kernel e2e probes, upstream kernel regressions and a reproducible collaborative failure workload |

Memory history is logical history for this schema, not PostgreSQL storage-level time
travel. A memory key belongs to one producer. Selecting a peer's shared value does not
grant permission to overwrite it. Task state is shared within an org through controlled
SQL transitions. Shared editable workspaces and branch/merge workflows remain unbuilt.

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
            ├─ quota connection → shared org buckets and expiring permits
            └─ bounded audit spool → admin connection → audit receipts
```

The gateway remains outside PostgreSQL. Keep the upstream parser, planner, storage,
WAL, catalogs, process model and autovacuum. New core changes need a demonstrated
requirement that a gateway, schema or existing extension point cannot satisfy.

Audit is a separate durability boundary. Strict mode fsyncs an intent before admitting a
SQL query and persists completion afterwards. The SQL transaction and audit completion
are not atomic: a crash between them produces `audit_outcome_unknown`. Stable receipt
IDs deduplicate delivery, not execution of arbitrary SQL. See [audit delivery](gateway.md#audit-delivery).

## Durable task coordination

**Implemented in the schema, REST and MCP.** The [task contract](tasks.md) is enforced
inside PostgreSQL; it adds no kernel patch or alternative storage layer.

1. **Task claims.** `FOR UPDATE SKIP LOCKED` selects one eligible org task after its
   dependencies complete. A durable per-agent request UUID prevents a lost reply from
   causing a retry to claim another task. Direct agent DML on task tables is revoked.
2. **Leases and recovery.** Claims expire using database `clock_timestamp()`. Heartbeats
   renew a current claim; another worker can reclaim abandoned work. Agents do not hold
   database transactions open while waiting on a model or an external tool.
3. **Fencing tokens.** Each new claim gets a monotonically advancing per-task token. Every task
   state update and completion checks it, so a paused worker cannot resume and overwrite
   the new owner's result. External systems must support fencing or idempotency for the
   same guarantee to extend beyond PostgreSQL.
4. **Atomic completion.** Result, completed state and history commit together. The
   current owner/fence and unexpired lease are checked before and after locking all
   declared producer-selected memory inputs and checking their versions. Identical
   successful completion retries replay the result and normalized effect set. Optional
   external intents commit in the same transaction. The [outbox dispatcher](outbox.md)
   retries them using stable keys; receivers must atomically deduplicate external effects.

SQL, REST and MCP expose matching conflict, stale-claim, expired-lease and changed-input
errors. The task creator can cancel; an administrator can requeue a nonterminal task
with a reason while invalidating its fence. History and claim receipts persist without
automatic retention. Read-only/disabled/expired-agent checks run inside SQL mutations.

## Organization admission across gateways

**Implemented in the schema and gateway.** Shared token buckets and expiring permits
serialize grants per organization and class across replicas. Local bounded queues rotate
waiting organizations; task renew/release/complete have a separate control lane. This
establishes shared ceilings and local scheduling fairness, not global queue ordering or
minimum throughput. Per-agent limits remain process-local.

The [admission contract](admission.md) specifies live policy updates, conservative permit
expiry, bounded request execution, fail-closed quota errors and admin-only controls. The
owned-service workload uses three gateways and at least 2,000 actual participating workers,
with sustained quiet-tenant task completion under noisy traffic and a gateway crash.
Reports must retain policy settings and client backpressure along with latency and
correctness evidence. Quotas do not meter CPU, IO or storage, and direct SQL does not pass
through HTTP admission.

## Acceptance workload: collaborative work under failure

The [reproducible runner](../coordination/README.md) exercises 100–1,000 worker identities on a shared task graph with
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

## Failover and point-in-time recovery

**Implemented as an opt-in schema/gateway contract.** The [recovery guide](recovery.md)
defines pinned PostgreSQL identity/timeline, synchronous acknowledgement barriers and an
externally allocated recovery generation. Existing deployments without recovery activation
retain legacy behavior. Primary selection and fencing belong to the operator or existing
HA manager; SwarmPG does not introduce an election protocol.

Eligible synchronous failover must preserve acknowledged results and effect intents.
PITR is a separate contract: it intentionally rewinds history, invalidates unfinished
ownership into a new generation and quarantines runtime/dispatch until retained messages
have explicit dispositions and the operator attests reconciliation of effects missing
from restored history. Stable receiver keys cannot deduplicate a recreated intent whose
original key disappeared in the rewind.

The owned-cluster recovery workload covers primary failure, replication partitions,
promotion and archive-based PITR while driving task/lease/effect traffic. Retain the
recorded workload duration, recovery time, replication settings and independent receiver
ledger when interpreting results. This adds bounded fault evidence, not a production HA
service or a statement about hundreds of thousands of active agents.

## Work after the coordination proof

**Planned; not implemented.** Design shared workspaces around explicit membership and
capabilities once the task contract has been validated. Decide ownership, provenance,
conflict handling, retention and deletion before adding more shared-write APIs. Keep
private agent memory and shared workspace state distinct.

Operational readiness is also required: schema migration and rollback procedures,
production backup/restore drills, deployment authentication/TLS, token rotation, audit-volume
operations, retention/purge, capacity tuning for shared admission, longer fairness soaks and
deployment-specific HA/fencing acceptance. The recovery guide supplies a reference
procedure and owned-cluster fault tests; the development Compose setup is not a production deployment recipe.
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
