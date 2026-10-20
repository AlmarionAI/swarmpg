# SwarmPG threat model

Version 0.1, 2026-10-20. Every control below is backed by a probe in `loadtest/` that runs
in CI (`make e2e`: 65 probes on stock Postgres; `make e2e KERNEL=1`: 83 on the SwarmPG
kernel). The probe column cites probe names as printed by the load test.

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
  - On the **SwarmPG kernel**, identity, budgets and the fail-closed gate are enforced in
    core, so they hold even for SQL that never passed through the gateway.

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
| Read another agent's private memory key | Live reads fall back only to keys the owner marked `shared` within the same org | memory_get cannot read a peer's private key (MCP) | ✔ | ✔ |
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
| Huge results or bulk writes | Gateway row and byte caps; on the kernel a per-statement row cap that fails over-budget writes as a whole | write over the row budget refused; refused write left nothing behind | partial (reads only) | ✔ |
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
| File audit records in another agent's name | Only the gateway's admin connection can call `record_audit`; every statement an agent sends through the gateway, including ones the guard refuses, is recorded | forge an audit record for another agent | ✔ | ✔ |

### T8. Lost updates and unrecoverable mistakes

Not attacks, but the failure modes that hurt agent fleets most.

| Risk | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Two agents overwrite each other after reading the same version | Compare-and-set on `expected_version` | create-only write (expected_version 0); second create-only write conflicts; write at the current version; stale write conflicts and writes nothing; 20 concurrent writers at one version: one wins; MCP memory_put with a stale version conflicts | ✔ | ✔ |
| A bad write or delete cannot be taken back | Versioned history, reads of the past, undo | read an earlier version; history lists every version; as_of reads the value at that time; undo restores the previous value as a new version; a delete can be undone; MCP memory_history; MCP memory_get at a version; MCP memory_undo to a version | ✔ | ✔ |
| A retry after a timeout duplicates a write | `Idempotency-Key` / `idempotency_key` replays the stored result | events_append retry with the same key is replayed | ✔ | ✔ |

### T9. The MCP endpoint

| Attack | Control | Probes | Stock | Kernel |
|---|---|---|:-:|:-:|
| Call tools without a token, or with a forged one | Every MCP request is authenticated with the agent's token | no token refused; invalid token refused | ✔ | ✔ |
| A web page drives a local gateway (DNS rebinding, A3) | Requests with an `Origin` outside `MCP_ALLOWED_ORIGINS` get 403 | foreign Origin refused (DNS rebinding) | ✔ | ✔ |
| Bypass REST controls through MCP | Tools call the same code paths, under the same identity, budgets and limits | query cannot read a peer's rows; query cannot change identity; MCP query tool is held to the same budget | ✔ | ✔ |

Protocol conformance (both eras, eight tools) is covered by: legacy initialize negotiates
2025-06-18; 2026-07-28 server/discover; tools/list returns the 8 tools; schema tool
describes the tables; memory_put then memory_get round-trips.

### Positive controls

Probes that must *succeed*, so a control that breaks normal work is caught too: read own
memory; write own document; EXPLAIN still shows the estimate; and every memory and MCP
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
7. **Budget gaps.** The kernel row cap does not apply to SQL `FETCH` from a cursor (the
   gateway refuses cursors); the byte budget exists only in the gateway.
8. **Memory history is kept forever.** Deleting an agent's data for good (right to erasure)
   needs an operator purge; there is no API for it yet.
9. **Capabilities are coarse.** All agents share one role, so per-agent privileges are not
   expressible in SQL; `read_only` is enforced by the gateway and a transaction-local
   setting, not by the kernel.
10. **Deployment defaults.** Trust authentication, the default admin key and the default
    principal secret in the quickstart and benchmark are for local use only. TLS is not
    provided by SwarmPG. Rate limits live in one gateway process.

## Reproduce

```bash
make e2e              # stock Postgres: 65 probes
make kernel && make e2e KERNEL=1   # SwarmPG kernel: 83 probes
```

`bench/run.sh` also runs three of these attacks against a PgBouncer + row-level security
baseline; see [`bench/README.md`](../bench/README.md).
