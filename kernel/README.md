# SwarmPG kernel

Upstream PostgreSQL `REL_19_STABLE` at the commit in [`BASE`](BASE) (19rc1), plus the patch
series in [`patches/`](patches). The patches are under the PostgreSQL License
([LICENSE](LICENSE)).

The pinned base is still **PostgreSQL 19rc1**, not the PostgreSQL 19 GA release.
Rebasing the patch series onto the chosen PostgreSQL 19 GA/stable revision, then rerunning
upstream regression, protocol and gateway end-to-end tests, remains a release-readiness
task. No GA rebase is claimed here.

| # | Topic | Summary | Verified |
|---|---|---|---|
| 0001 | `[principal]` | `GUC_SEALED` flag (immutable once set, establishment-source restricted); `agent_principal = 'agent=…,org=…'`, one value so agent and org are sealed together, HMAC-signed when `agent_principal_secret` is set; `current_agent()`, `current_org()`; fail-closed `agent_require_principal` gate; regression test `agent_identity` | `make check` |
| 0002 | `[budget]` | `GUC_RATCHET` flag (without SET privilege, a session may only lower the value); `agent_max_rows` caps rows returned to the client and rows one statement may modify; `agent_max_cost` refuses over-budget plans before they run; regression test `agent_budget` | `make check` |
| 0003 | `[budget]` | Share the write budget across the top-level statement's nested functions, triggers and CTEs; preserve it across protocol suspension; a caught budget error still fails the statement | `agent_budget`, protocol regression |

`make check`: 241/241 (239 upstream + `agent_identity` + `agent_budget`).

[FORK-NOTES.md](FORK-NOTES.md) explains each mechanism and why it is built the way it is.

## Build

```bash
kernel/build.sh fetch    # clone upstream at BASE into ./swarmpg-core and apply the series
kernel/build.sh build    # configure, compile, install into ./.pginstall
kernel/build.sh check    # upstream regression suite; on macOS run build first (SIP strips DYLD_LIBRARY_PATH)
```

`make kernel` and `make kernel-check` wrap these. Set `SWARMPG_SRC`, `PREFIX` or
`CONFIGURE_FLAGS` to override the defaults (debug build with assertions).

`make e2e KERNEL=1` also runs the suspended/interleaved-portal protocol regression;
the kernel CI job uses that target. Stock e2e skips this kernel-only check. The script
uses the same disposable, trust-authenticated admin `DATABASE_URL`. To run it separately:

```bash
DATABASE_URL='postgres://postgres@127.0.0.1:5511/swarmpg?sslmode=disable' \
  python3 kernel/tests/agent_budget_protocol.py
```

`PGHOST`, `PGPORT`, `PGUSER` and `PGDATABASE` override corresponding URL values;
`PGHOST` may be a Unix socket directory. This standard-library test client supports
trust authentication without TLS only. It creates session-local tables/functions and
checks the precise budget error, rollback and a fresh budget on the next statement.

Write accounting charges each input row to `ModifyTable`, including nested trigger
bookkeeping and attempts skipped by `ON CONFLICT` or a `BEFORE` trigger. A normal
memory write costs two such rows (memory plus history); an event append costs three
(event plus the attempted head insert and head update). Client result rows have a
separate counter. Each top-level statement receives a fresh write budget; repeated
protocol Execute messages for the same suspended statement share its existing budget.

## Change a patch

Work in `swarmpg-core/`: one commit per patch on top of `BASE`, subject prefixed with its
topic tag (`[principal]`, `[budget]`, `[space]`, ...). Then regenerate the series:

```bash
kernel/build.sh export   # rewrites kernel/patches/ from BASE..HEAD
```

## Using it with the gateway

`sql/schema.sql` detects the kernel: `swarmpg.current_agent()` and `swarmpg.current_org()` then
read only the sealed principal, and `agent_runtime` gets `agent_require_principal = on` plus budget
ceilings (`agent_max_rows = 100001`, `agent_max_cost = 1e9`). The gateway detects the kernel too.
In every transaction it runs for an agent, it seals the principal (signed with `PRINCIPAL_SECRET`,
which must match the server's `agent_principal_secret`) and lowers the budgets to that agent's
`max_rows + 1` and `max_cost`.
