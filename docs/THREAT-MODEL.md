# SwarmPG threat model

Development revision, 2026-10-22. The load harness contains 70 probes on stock Postgres
and 99 with the signed SwarmPG kernel. The tables cite those probe names; audit delivery
and shared-producer semantics also have gateway unit/integration tests run by the e2e
script. Test presence is not a production-readiness claim. Negative query probes require
the expected application code/SQLSTATE and HTTP status; unexpected HTTP 5xx, transport
errors and failed load requests fail the run.

## System and trust boundaries

```
 agents (untrusted)                 trusted                          trusted
 ┌───────────────┐  HTTPS/REST  ┌──────────────┐  32 pooled conns  ┌───────────────────────┐
 │ agent + token │─────────────▶│   gateway    │──────────────────▶│ Postgres, as role     │
 │ (writes SQL)  │  MCP (/mcp)  │ auth, guard, │  agent_runtime    │ agent_runtime         │
 └───────────────┘              │ budgets,     │                   │ stock: RLS + revokes  │
         ┊ raw SQL (attacker A2)│ reset, audit │                   │ kernel: + sealed      │
         └┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┴┄┄┄┄┄┄┄┄┄┄┄┄┄┄┴┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄▶│ principal, budgets    │
                                                                    └───────────────────────┘
 operator (trusted): superuser, admin key, principal secret
```

- **Agents are untrusted.** They write arbitrary SQL, may be prompt-injected, and may
  collude. Every agent is assumed hostile toward every other agent and toward the fleet.
- **The gateway, Postgres and the operator are trusted.** The operator holds the
  superuser, the admin key and, on the kernel, the principal secret.
- **Two modes.**
  - On **stock Postgres**, the gateway is the control for identity; the database enforces
    row-level security, privileges and revokes.
  - On the **SwarmPG kernel**, the sealed principal, row/cost ceilings and fail-closed
    gate are enforced in core. A principal secret is required to reject an unsigned or
    forged identity on raw SQL access. Gateway-only limits do not become kernel limits.

## Attackers

| | Attacker | Can |
|---|---|---|
| A1 | An agent using the API | Call REST or MCP with its own token and send any SQL through `query` |
| A2 | An agent with raw SQL as `agent_runtime` | Reach Postgres directly (leaked credentials, misrouted network, a gateway bug) and run any SQL in its own session |
| A3 | A web page in a browser next to a local gateway | Send requests to `localhost` via DNS rebinding |
| A4 | Colluding agents, possibly in different orgs | Coordinate to move data or signals between them |

Out of scope: the operator and anyone holding the superuser, admin key or principal secret;
host compromise; a network attacker (terminate TLS in front of the gateway); side channels
of timing and lock waits.

## Assets

Agent identity; agent data (memory and its history, documents, the event journal); journal
and audit integrity; the agent registry (token hashes); fleet availability (pooled
connections, CPU, memory); metadata about other agents.

## Threats and controls

Stock / kernel: ✔ enforced, — not applicable, ✘ not enforced.

### T1. Identity spoofing

An agent tries to act as another agent or org.

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| `SET app.agent_id`, including hidden in comments, tabs or a second statement | Gateway guard refuses SET and multiple statements; comments and whitespace are stripped before classifying | spoof identity via SET; obfuscated SET (nested comment); obfuscated SET (tab); multi-statement smuggling; query cannot change identity (MCP) | ✔ | ✔ |
| `set_config()`, `DO` blocks, functions that hide a SET | `set_config` revoked; `DO`, `CREATE FUNCTION` refused; procedural languages revoked | spoof identity via set_config(); anonymous code via DO; create function to hide SET | ✔ | ✔ |
| `SET ROLE` / `RESET ROLE` to a privileged role | The gateway logs in as `agent_runtime`, so there is nothing to escalate to; the guard also refuses both | escalate via SET ROLE; escalate via RESET ROLE | ✔ | ✔ |
| Raw SQL: change identity after the gateway set it | Sealed `agent_principal`: no SET, RESET or `set_config()` can change it, nor either half of it | re-seal to another agent refused; RESET of a sealed principal refused | ✘ | ✔ |
| Raw SQL: set `app.agent_id` / `app.org_id` directly | On the kernel, `current_agent()`/`current_org()` read only the sealed principal | SET app.agent_id cannot read peer rows; SET app.org_id cannot read another org's rows | ✘ | ✔ |
| Raw SQL: complete a half-set identity, or claim one when none was set | Agent and org are one value; with a principal secret it must carry the gateway's HMAC signature | half-set principal refused (org missing); forge an unsigned principal; forge a principal with a bad signature | ✘ | ✔ |
| Raw SQL: run queries with no identity at all | `agent_require_principal`: no plan runs until a principal is sealed | unsealed query refused | ✘ | ✔ |

### T2. Reading another agent's data

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Query another agent's rows by id, or everything but one's own | Forced row-level security on every agent table | read peer memory by id; read all non-self memory; query cannot read a peer's rows (MCP) | ✔ | ✔ |
| Read another agent's private memory key | Live fallback, explicit `from` and shared listings expose only own keys or same-org shared keys; historical reads remain own-only | memory_get cannot read a peer's private key (MCP); `TestSharedMemoryProducersIntegration` | ✔ | ✔ |
| Read another agent's memory history | History is visible only to its owner | a peer cannot read your key's history; a peer cannot read your history rows | ✔ | ✔ |
| Read the agent registry or the audit log | No privileges on either | read agent registry (token hashes); read server audit log | ✔ | ✔ |

### T3. State leaking between agents on pooled connections

Hundreds of agents share each Postgres connection over time.

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Temp table or `WITH HOLD` cursor left for the next agent | No temp tables, no cursors; `CLOSE ALL; RESET ALL; DISCARD TEMP; DISCARD SEQUENCES` after every request | create temp table (pool leak); declare WITH HOLD cursor (pool leak) | ✔ | ✔ |
| A session-level SET that outlives the transaction | The guard refuses SET; on the kernel identity and budgets are `SET LOCAL` and the principal cannot be set at session level by an agent | (covered by T1 probes; the baseline benchmark demonstrates the attack succeeding without the gateway) | ✔ | ✔ |

### T4. Creating objects

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Create tables, `CREATE TABLE AS` to copy data, create functions | No `CREATE` privilege; an event trigger refuses all agent DDL; the guard refuses DDL | create scratch table (DDL); CREATE TABLE AS to exfil own rows; create function to hide SET | ✔ | ✔ |

### T5. Denial of service against the fleet

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Terminate or cancel other agents' backends | `pg_terminate_backend`/`pg_cancel_backend` revoked | terminate peer backends; cancel peer backends | ✔ | ✔ |
| Exhaust the lock table or block others with advisory locks; fill storage with large objects | Advisory-lock family and large-object functions revoked | session advisory lock; large object spam | ✔ | ✔ |
| Flood requests or hold many connections | Per-agent rate limit and concurrency cap; statement, lock and idle-in-transaction timeouts | (load phase; `rate_limited` and errors are counted) | ✔ | ✔ |
| Runaway query (e.g. a cross join) | Refused before it runs when its estimated cost exceeds the agent's budget | runaway query refused before it runs; MCP query tool is held to the same budget | ✘ (statement timeout only) | ✔ |
| Huge results or bulk writes | Gateway row and byte caps; kernel cumulative attempted-write cap including trigger bookkeeping, with statement rollback on excess | write over the row budget refused; refused write left nothing behind | partial (reads only) | ✔ |
| Hide many writes behind one aggregate, CTE result or function call | Nested execution shares the statement's attempted-write budget; live data and history roll back together | aggregate cannot hide repeated memory writes; data-modifying CTE cannot hide repeated function writes; function calling function shares the write budget; residual-row checks | — | ✔ |
| Dodge the cost budget with `EXPLAIN ANALYZE` or SQL inside functions | The gate applies at every nesting level and under `EXPLAIN ANALYZE`; plain `EXPLAIN` stays allowed | EXPLAIN ANALYZE cannot dodge the cost budget; query_to_xml cannot dodge the cost budget; EXPLAIN still shows the estimate | — | ✔ |
| Raw SQL: raise one's own budget, or the role's ceiling | Budgets are tighten-only (`GUC_RATCHET`); role defaults need privilege | raise the row budget refused; raise the cost budget refused; change the role's budget defaults refused | — | ✔ |

### T6. Leaks through metadata and side channels

All agents run as one role, so anything Postgres shows "to the same role" is shown to every
agent.

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Read other agents' in-flight SQL (`pg_stat_activity`) | View and backing functions revoked from `PUBLIC` (granted to `pg_monitor`) | read peer SQL text (pg_stat_activity); read peer SQL text (backend function) | ✔ | ✔ |
| Read fleet-wide counters (how many agents, rows, audit volume) | Every `pg_stat_get_*` function revoked from `PUBLIC` (granted to `pg_monitor`) | read fleet-wide row counters (pg_stat_user_tables) | ✔ | ✔ |
| Read planner statistics, which hold sample values of every agent's rows | Postgres hides `pg_stats` rows for tables under active row-level security; probed so it stays so | read planner statistics of agent tables (pg_stats) | ✔ | ✔ |
| Read the global event sequence | Agents get `USAGE` (for the id default), not `SELECT` | read the fleet-wide event sequence | ✔ | ✔ |
| Signal another agent through the database | `pg_notify` revoked; the guard refuses `LISTEN`/`NOTIFY` | signal other agents (pg_notify) | ✔ | ✔ |

### T7. Tampering with records

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Delete or truncate the event journal | Append-only: no UPDATE/DELETE/TRUNCATE privilege and a trigger that refuses changes; each event is hash-chained to the previous one | tamper with event log (DELETE); truncate event log | ✔ | ✔ |
| Rewrite, erase or forge memory history | History is written only by triggers; agents have `SELECT` only | rewrite history (UPDATE); erase history (DELETE); forge history (INSERT) | ✔ | ✔ |
| Forge a version number with raw `UPDATE` | A trigger assigns every version | raw UPDATE cannot set a version; ... the version still moves forward | ✔ | ✔ |
| File audit records in another agent's name | Only the trusted admin connection can call `record_audit` or `record_audit_receipt`; delivery completeness depends on the audit mode | forge an audit record for another agent; forge a deduplicated audit receipt | ✔ | ✔ |

### T8. Lost updates and unrecoverable mistakes

Not attacks, but the failure modes that hurt agent fleets most.

| Risk | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Concurrent requests overwrite the same agent-owned key after reading one version | Compare-and-set on `expected_version`; peers still cannot write the owner's key | create-only write (expected_version 0); second create-only write conflicts; write at the current version; stale write conflicts and writes nothing; 20 concurrent writers at one version: one wins; MCP memory_put with a stale version conflicts | ✔ | ✔ |
| A bad write or delete cannot be taken back | Versioned history, reads of the past, undo | read an earlier version; history lists every version; as_of reads the value at that time; undo restores the previous value as a new version; a delete can be undone; MCP memory_history; MCP memory_get at a version; MCP memory_undo to a version | ✔ | ✔ |
| An event-append retry after a timeout creates a second event | `Idempotency-Key` / `idempotency_key` replays the stored event result; arbitrary SQL is not deduplicated | events_append retry with the same key is replayed | ✔ | ✔ |

### T9. The MCP endpoint

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Call tools without a token, or with a forged one | Every MCP request is authenticated with the agent's token | no token refused; invalid token refused | ✔ | ✔ |
| A web page drives a local gateway (DNS rebinding, A3) | Requests with an `Origin` outside `MCP_ALLOWED_ORIGINS` get 403 | foreign Origin refused (DNS rebinding) | ✔ | ✔ |
| Bypass REST controls through MCP | Tools call the same code paths, under the same identity, budgets and limits | query cannot read a peer's rows; query cannot change identity; MCP query tool is held to the same budget | ✔ | ✔ |

Protocol conformance (both eras, sixteen tools) is covered by: legacy initialize negotiates
2025-06-18; 2026-07-28 server/discover; tools/list returns the 16 tools; schema tool
describes the tables; memory_put then memory_get round-trips.

### T10. Audit loss and ambiguous outcomes

SQL-query audit covers the REST query endpoint and MCP query tool, including guard
rejections. It does not cover typed task/memory/event API writes or direct database sessions.
Task transition history is a separate synchronous record committed with each task mutation.

| Risk | Control and limit | Validation |
|---|---|---|
| Audit database outage drops a dequeued batch | Retain receipts and retry with bounded backoff; stable UUIDs deduplicate database replay | `TestAuditRetriesDatabaseOutageIntegration`; `TestAuditReceiptReplayIntegration` |
| Full spool silently violates a strict admission policy | Strict mode requires disk spooling and rejects with `audit_unavailable` before execution when an intent cannot be reserved | `TestAuditSpoolCapacityAndAdmission`; `TestStrictAuditFullRefusesWriteIntegration` |
| Gateway restart loses queued receipts | A configured persistent spool fsyncs receipts and restores them; default memory-only mode offers no crash durability | `TestAuditSpoolDurableRecovery` |
| Rejected query text poisons a retry batch | Samples and error metadata normalize NUL/invalid UTF-8 before storage and delivery; the digest retains the original SQL bytes | `TestAuditNULRefusalCannotPoisonBatchIntegration`; `TestAuditLegacyInvalidTextRecoveryIntegration` |
| SQL commits, then the gateway crashes before recording completion | Recover the durable intent as `audit_outcome_unknown`; do not infer rollback or safe replay | `TestAuditSpoolDurableRecovery`; explicit limitation, not atomic SQL/audit commit |

Best-effort mode can drop new receipts on capacity or persistence failure and increments
`audit_lost`. Strict mode bounds admission, not availability: an unavailable/full spool
stops SQL-query admission. Disk failure after execution can leave an unknown outcome.
See [configuration, health fields and recovery semantics](gateway.md#audit-delivery).

### T11. Task ownership and completion

| Attack or failure | Database control | Validation |
|---|---|---|
| Another organization reads, claims or completes a task | Org RLS and explicit function authorization | `foreign org cannot read task rows`; `foreign org cannot claim or complete a task` |
| Competing claims or a lost claim response acquire conflicting ownership | Task row locking, `SKIP LOCKED`, durable request UUID replay | `TestTaskConcurrentClaimsAndRequestReplayIntegration` |
| Paused worker writes after lease expiry/reclaim | Database-time lease checks and monotonically advancing fences | `TestTaskLeaseExpiryFencingAndRecoveryIntegration` |
| Inputs change while completion waits on a lock | Declared input rows locked and versions/visibility rechecked; lease checked again after the wait | `TestTaskInputGuardLocksAndRollsBackIntegration` |
| Duplicate completion or changed retry result | Successful owner/fence/result replay; mismatched result conflicts; one completion history event | `TestTaskLifecycleAndAtomicReplayIntegration` |
| Direct SQL changes owner/history or recovers another agent's task | No direct DML grants; private/admin function execution revoked; explicit org and actor checks | `raw task UPDATE cannot transfer ownership`; `TestTaskAuthorityAndSQLBypassIntegration` |
| Token expires or registry authority changes during a lock wait | Post-wait authorization checks and a registry lock through commit | `TestTaskSQLAuthorizationAfterLockWaitIntegration`; `TestTaskSQLRegistryRevocationSerializesWithCommitIntegration` |
| Gateway or raw task call commits an oversized receipt | Full task receipt budget checked in SQL and typed adapters before commit | `TestTaskResponseBudgetRollsBackAndFenceIsExactIntegration`; `TestTaskSQLMutatingReceiptRollbackIntegration` |

The [task contract](tasks.md) covers declared inputs and database state only. It does not
make arbitrary external effects exactly once or validate undeclared reads. Task queues
are org-shared, with no narrower workspace/queue membership policy. Completed dependencies
are immutable; cancelled dependencies block dependents until explicit replanning.

### Positive controls

Probes that must *succeed*, so a control that breaks normal work is caught too: read own
memory; write own document; one memory write fits the nested-write budget; EXPLAIN still
shows the estimate; and every memory and MCP
round-trip above.

## Residual risks

What is **not** covered today, in rough order of importance.

1. **Raw SQL on stock Postgres (A2).** Without the kernel, an agent that reaches Postgres
   directly can `SET app.agent_id` and read any agent's rows. If raw access is possible,
   run the kernel with a principal secret.
2. **A compromised gateway.** On stock it is a full compromise. On the kernel it holds
   `PRINCIPAL_SECRET` and can sign any principal; the kernel protects agents from each
   other, not from the gateway.
3. **Raw SQL on the kernel can still change other session settings.** `work_mem` (memory
   pressure) and `statement_timeout` (longer queries; the cost gate still applies) are not
   yet budgets. The `NOTIFY`/`LISTEN` statements cannot be revoked for a raw session, so
   two raw-access agents can signal each other.
4. **Planner estimates.** `pg_stats` is hidden, but `EXPLAIN` row estimates for leakproof
   operators (e.g. text or uuid equality) are computed from table-wide statistics, so an
   agent can infer roughly how common a value is across all agents.
5. **Low-grade metadata.** Event ids come from one sequence, so gaps between an agent's own
   ids reveal fleet activity. `pg_locks` shows other backends' locks (relations and pids,
   no data). Query latency reveals load.
6. **Cheap but slow work.** `pg_sleep` and similar cost almost nothing to the planner, so
   an agent can hold up to `max_concurrency` connections for its statement timeout each.
7. **Budget gaps.** CPU, IO and storage size are not metered. Generic query byte budgets
   exist in the gateway; task functions also bound their receipts in SQL. The gateway
   passes a `max_rows + 1` kernel ceiling to detect result
   truncation; attempted internal writes consume that same ceiling. Raw non-atomic
   `CALL` can commit between statements; the gateway refuses `CALL` and cursors.
8. **Memory history is kept forever.** Deleting an agent's data for good (right to erasure)
   needs an operator purge; there is no API for it yet.
9. **Capabilities are coarse.** All agents share one role, so per-agent privileges are not
   expressible in SQL; `read_only` is enforced by the gateway and a transaction-local
   setting, not by the kernel. Task mutations additionally check the agent registry.
10. **Deployment defaults.** Trust authentication, the default admin key and the default
    principal secret in the quickstart and benchmark are for local use only. TLS is not
    provided by SwarmPG. Rate limits live in one gateway process.
11. **Audit completeness.** Default best-effort mode may drop receipts; memory-only spools
    do not survive process failure. Strict mode requires an operator-maintained persistent
    volume and still cannot atomically couple SQL commit with audit completion. It does
    not audit typed memory/event/task API calls or raw SQL sessions. Task transitions
    have their own synchronous history.
12. **Shared information is not trusted information.** Producer identity makes provenance
    explicit; it does not establish that an org peer's content is correct. Shared keys
    remain owner-written, with no jointly editable workspace. Task completion validates
    declared versions and provenance, not the truth of an input or result.

## Reproduce

```bash
make e2e              # stock Postgres: 70 probes
make kernel && make e2e KERNEL=1   # SwarmPG kernel: 99 probes
```

`bench/run.sh` also runs three of these attacks against a PgBouncer + row-level security
baseline; see [`bench/README.md`](../bench/README.md).
