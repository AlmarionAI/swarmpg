# SwarmPG kernel — fork notes

Base: PostgreSQL `REL_19_STABLE` (19rc1 at fork time, upstream commit `0da632e`).

## Patch series discipline

Every change to core is a commit whose subject starts with a topic tag. Topics map to the
subsystems in `../docs/DESIGN.md` §3 and are rebased as independent series onto each upstream release.

| Tag | Subsystem | Status |
|---|---|---|
| `[principal]` | sealed agent identity, capabilities, budgets | M0 landed: `GUC_SEALED` (immutable + source-restricted), `agent_require_principal` fail-closed gate, `current_agent()`; `make check` 240/240 on 2026-10-20 |
| `[space]` | tenant-scoped relations, space-aware catalog | M1 |
| `[budget]` | executor-level row/byte/time quotas | M1 |
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
   SwarmPG currently uses 8520–8529.
5. Never change WAL record formats: the page service reuses upstream redo code.

## Landed patches

### [principal] sealed agent identity + fail-closed principal gate (M0)

Files: `src/backend/utils/misc/guc.c` (`GUC_SEALED` enforcement), `guc_funcs.c` (`MAX_GUC_FLAGS`),
`guc_parameters.dat` (`agent_id`, `agent_require_principal`), `guc_tables.c`,
`src/backend/utils/adt/misc.c`, `src/backend/executor/execMain.c` (fail-closed gate),
`src/include/utils/guc.h`, `src/include/catalog/pg_proc.dat` (OID 8520), `catversion.h`,
`src/test/regress/sql/agent_identity.sql`, `expected/agent_identity.out`, `parallel_schedule`.

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
   attack: previously any operator or poisoned role default could seed `agent_id` so every pooled
   backend started pre-sealed, locking the frontend out (or, worse, running every agent under one
   forced identity). A stored role default is now inert — a new session of that role starts
   unsealed (with a warning) and can still be sealed normally; a command-line `-c agent_id=...`
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

`agent_id` is the first sealed variable; `org_id`, `space_id` and the capability set will reuse the
same flag. For a multi-field principal, seal the fields together at connect and consider a
seal-group so a partially-sealed principal cannot be completed by the agent (see DESIGN.md §1.4).
`current_agent()` returns the identity or NULL.

Why this is the first patch: it is the one isolation gap a proxy in front of stock Postgres cannot
close, and it is the primitive every later `[space]` and `[budget]` patch keys on.
