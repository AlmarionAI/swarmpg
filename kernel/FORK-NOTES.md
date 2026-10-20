# SwarmPG kernel — fork notes

Base: PostgreSQL `REL_19_STABLE` (19rc1 at fork time, upstream commit `0da632e`).

## Patch series discipline

Every change to core is a commit whose subject starts with a topic tag. Topics map to the
subsystems in `../docs/DESIGN.md` §3 and are rebased as independent series onto each upstream release.

| Tag | Subsystem | Status |
|---|---|---|
| `[principal]` | sealed agent principal, capabilities | Landed: `GUC_SEALED`; `agent_principal` (agent + org sealed as one value, optionally HMAC-signed); `current_agent()`, `current_org()`; `agent_require_principal` fail-closed gate |
| `[budget]` | executor-level agent budgets | Landed: `GUC_RATCHET` (tighten-only); `agent_max_rows` (SELECT cap, write cap); `agent_max_cost` (pre-execution cost gate) |
| `[space]` | tenant-scoped relations, space-aware catalog | M1 |
| `[smgr-remote]` | storage manager → page service | M2 |
| `[frontend]` | in-core session multiplexing, HTTP/gRPC/MCP | M4 |
| `[xid64]` | 64-bit transaction ids | M5 |
| `[sql-agent]` | `CREATE SPACE`, `CREATE BRANCH`, `AS OF`, `WITH BUDGET` | M2–M3 |

Rules:
1. Prefer a hook, access method or background worker to an in-place edit. Add a new hook point
   to core rather than special-casing agent behaviour inside upstream code paths.
2. Every patch ships with a regression test under `src/test/regress` (or TAP under
   `src/test/modules`) and an entry in this file.
3. Bump `CATALOG_VERSION_NO` whenever `pg_proc.dat` or any other catalog `.dat` changes.
4. New OIDs come from `src/include/catalog/unused_oids`, allocated in the 8000–9999 range.
   SwarmPG currently uses 8520–8521.
5. Never change WAL record formats: the page service reuses upstream redo code.

## Landed patches

### 0001 [principal] sealed agent principal + fail-closed principal gate

Files: `src/backend/utils/misc/guc.c` (`GUC_SEALED` enforcement), `guc_funcs.c` (`MAX_GUC_FLAGS`),
`guc_parameters.dat` (`agent_principal`, `agent_principal_secret`, `agent_require_principal`),
`guc_tables.c`, `agent_principal.c` (parsing, signature check, `current_agent()`, `current_org()`),
`src/include/utils/agent_principal.h`, `guc_hooks.h`, `src/backend/executor/execMain.c`
(fail-closed gate), `src/include/utils/guc.h`, `src/include/catalog/pg_proc.dat` (OIDs 8520–8521),
`catversion.h`, `src/test/regress/sql/agent_identity.sql`, `expected/agent_identity.out`,
`parallel_schedule`.

Mechanism: a new core GUC flag, `GUC_SEALED`, enforced in `set_config_with_handle()` (the single
funnel for `SET`, `SET LOCAL`, `RESET`, `set_config()` and function `SET` clauses) by two rules:

1. **Immutability.** A sealed variable that holds a non-empty value refuses every assignment,
   including an identical one, so an agent cannot repin its identity in a pooled session. The only
   things that clear it are the GUC stack unwinding at the end of a `SET LOCAL` scope or a function
   call, and parallel-worker state restore (which copies the leader's sealed value). The check is
   gated on `changeVal` so a validation-only probe (`PGC_S_TEST`, e.g. `ALTER ROLE ... RESET`
   re-validation) no longer raises a spurious "already sealed" error or locks a database out.
2. **Establishment source.** A sealed value may only be *established* from `PGC_S_SESSION` (the
   connected frontend's own `SET`), the forced-default paths, or parallel/worker restore. Per-role
   and per-database defaults (`ALTER ROLE/DATABASE ... SET`), the postmaster command line, the
   environment and the startup packet / `PGOPTIONS` are refused. This closes the pool-wide pre-seal
   attack: previously any operator or poisoned role default could seed the principal so every pooled
   backend started pre-sealed, locking the frontend out (or, worse, running every agent under one
   forced identity). A stored role default is now inert — a new session of that role starts
   unsealed (with a warning) and can still be sealed normally; a command-line `-c agent_principal=...`
   is refused at postmaster start, which is correct because the principal is never cluster-wide.

`RESET ALL`/`DISCARD ALL` skip it (`GUC_NO_RESET_ALL`); `ALTER SYSTEM` and `postgresql.conf` are
refused (`GUC_DISALLOW_IN_FILE`). A check hook was deliberately not used for sealing: PostgreSQL
does not re-run check hooks on `RESET`, so a hook-based seal is bypassable. `pg_settings_get_flags`
reports `SEALED`; `MAX_GUC_FLAGS` was bumped 6→7 to match the added push site (the array was one
short of the number of flags a single GUC can now carry — a latent overflow in non-assert builds).

**Fail-closed gate.** `agent_require_principal` (new `PGC_SUSET` bool) makes a backend refuse to
execute any query plan while `current_agent()` is NULL, enforced at `standard_ExecutorStart`. Set
it per pool role (`ALTER ROLE agent_runtime SET agent_require_principal = on`); a non-superuser
agent cannot clear it. This is defense in depth for the RLS-protected data paths: even if the
frontend ever hands out a backend without sealing, or a transaction abort clears a session-scoped
seal, the next unsealed read errors out instead of returning another principal's rows. Default
`off`, so it is inert unless an operator opts in.

**One value, sealed as a group.** The principal is a single sealed string,
`agent_principal = 'agent=<id>,org=<id>'`, parsed by a check hook into the GUC's `extra`. Agent and
org are therefore established by one assignment and immutable together: there is no state in which
one field is sealed and the other is still open for the agent to fill in (the seal-group problem in
`docs/DESIGN.md` §8.4). The grammar is strict (both fields, this order, ids of 1–63 characters from
`[A-Za-z0-9_.:-]`, nothing else) so the signed payload has exactly one spelling. `current_agent()`
and `current_org()` return the two fields, or NULL. `space_id` and capabilities will be added as
further fields of the same value.

**Signed principals.** When `agent_principal_secret` is set, the value must end with
`,sig=<lowercase hex HMAC-SHA256(secret, "agent=<id>,org=<id>")>`, compared in constant time. A
session that does not know the secret then cannot establish *any* principal, even if the frontend
never sealed one, so raw SQL access as the pool role no longer lets an agent pick an identity. The
secret is `PGC_SUSET` + `GUC_SUPERUSER_ONLY`, and its check hook refuses per-role/per-database
defaults, the startup packet and `PGC_S_TEST` validation, so it is never stored in a catalog
(`pg_db_role_setting` is readable by everyone). A signed value with no secret configured is refused
rather than accepted, so a misconfigured server fails closed. Parallel workers do not re-verify:
they restore the leader's already-verified value, possibly before the secret itself.

Why this is the first patch: it is the one isolation gap a proxy in front of stock Postgres cannot
close, and it is the primitive every later `[space]` and `[budget]` patch keys on.

### 0002 [budget] tighten-only agent budgets: row cap and pre-execution cost gate

Files: `src/include/utils/guc.h` (`GUC_RATCHET`), `src/backend/utils/misc/guc.c` (ratchet
enforcement), `guc_funcs.c`, `guc_parameters.dat` (`agent_max_rows`, `agent_max_cost`),
`guc_tables.c`, `src/backend/executor/execMain.c` (cost gate, SELECT cap),
`src/backend/executor/nodeModifyTable.c` (write cap), `src/include/nodes/execnodes.h`
(`es_agent_rows_modified`), `src/test/regress/sql/agent_budget.sql`, `expected/agent_budget.out`,
`parallel_schedule`.

**Tighten-only.** Both budgets are `PGC_SUSET` with the new `GUC_RATCHET` flag. A role without SET
privilege on them may still set them from its own session (`PGC_S_SESSION`: `SET`, `SET LOCAL`,
`RESET`, `set_config()`, function `SET` clauses), but only to a tighter limit; 0 means no limit, so
it is the loosest value. The check runs in `set_config_with_handle()` after the new value is known,
including the reset value for `RESET`. The GUC stack unwinding at the end of a `SET LOCAL` scope
restores the previous limit, as it should. Role defaults still require privilege, so an agent cannot
`ALTER ROLE` its own ceiling. Both are `GUC_NO_RESET_ALL`. The result: the pool role carries a
ceiling (`ALTER ROLE agent_runtime SET agent_max_rows = ...`), the gateway lowers it to the agent's
own budget in every transaction, and nothing the agent runs can raise it again.

**Cost gate.** `standard_ExecutorStart` refuses a plan whose `planTree->total_cost` exceeds
`agent_max_cost`, with SQLSTATE 53400, before any work is done. It applies at every nesting level,
because functions such as `query_to_xml()` run arbitrary SQL through SPI, and under `EXPLAIN
ANALYZE`, which executes the plan. `EXPLAIN` without `ANALYZE` (`EXEC_FLAG_EXPLAIN_ONLY`) is
allowed so the agent can see the estimate.

**Row cap.** `agent_max_rows` bounds two things:

- *Rows returned to the client* by a SELECT, counted across every `ExecutorRun` of the portal
  (`es_total_processed + es_processed`). Execution stops at the cap with a NOTICE, emitted only when
  a further row exists. Only client-bound destinations are capped (`DestRemote*`, `DestCopyOut`):
  the same query inside a function must see all of its rows, or it would silently compute wrong
  results. SQL `FETCH` from a cursor is not capped, because `FETCH` runs into the portal's own
  tuplestore before the rows reach the client; the gateway refuses cursors, and the cost gate still
  bounds the work.
- *Rows one statement may modify*, counted in `ExecModifyTable` per input row on a per-query EState
  counter, so every ModifyTable node of the statement counts, including data-modifying CTEs (which
  do not touch `es_processed`). Exceeding it is an ERROR, so the statement rolls back as a whole
  instead of half-applying. This applies at every nesting level. `COPY FROM` does not go through
  ModifyTable; it is refused anyway on the agent tables, which use row-level security.
