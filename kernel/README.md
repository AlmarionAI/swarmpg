# SwarmPG kernel

Upstream PostgreSQL `REL_19_STABLE` at the commit in [`BASE`](BASE) (19rc1), plus the patch
series in [`patches/`](patches). The patches are under the PostgreSQL License
([LICENSE](LICENSE)).

| # | Topic | Summary | Verified |
|---|---|---|---|
| 0001 | `[principal]` | `GUC_SEALED` flag (immutable once set, establishment-source restricted); `agent_id` and `current_agent()`; fail-closed `agent_require_principal` gate in the executor; `MAX_GUC_FLAGS` fix; regression test `agent_identity` | `make check` 240/240 (239 upstream + `agent_identity`) |

[FORK-NOTES.md](FORK-NOTES.md) explains each mechanism and why it is built the way it is.

## Build

```bash
kernel/build.sh fetch    # clone upstream at BASE into ./swarmpg-core and apply the series
kernel/build.sh build    # configure, compile, install into ./.pginstall
kernel/build.sh check    # upstream regression suite; on macOS run build first (SIP strips DYLD_LIBRARY_PATH)
```

`make kernel` and `make kernel-check` wrap these. Set `SWARMPG_SRC`, `PREFIX` or
`CONFIGURE_FLAGS` to override the defaults (debug build with assertions).

## Change a patch

Work in `swarmpg-core/`: one commit per patch on top of `BASE`, subject prefixed with its
topic tag (`[principal]`, `[budget]`, `[space]`, ...). Then regenerate the series:

```bash
kernel/build.sh export   # rewrites kernel/patches/ from BASE..HEAD
```

## Using it with the gateway

`sql/schema.sql` detects the kernel: `swarmpg.current_agent()` then reads only the sealed
`agent_id`, and `agent_runtime` gets `agent_require_principal = on`. The gateway detects it
too and seals `agent_id` at the start of every transaction it runs for an agent.
