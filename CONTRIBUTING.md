# Contributing to SwarmPG

## Before you start

- Sign the [Contributor License Agreement](CLA.md). The CLA check on your first pull
  request walks you through it; one signature covers all future contributions.
- For anything larger than a bug fix, open an issue first so we can agree on the approach.
- Security issues go through [SECURITY.md](SECURITY.md), never a public issue.

## Layout and licenses

| Path | What | License |
|---|---|---|
| `gateway/` | Go frontend: auth, budgets, statement guard, HTTP API | Apache-2.0 |
| `sql/` | agent schema: RLS, hardening, journal, audit | Apache-2.0 |
| `loadtest/` | fleet load test and isolation probes | Apache-2.0 |
| `kernel/` | PostgreSQL patch series and build script | PostgreSQL License |

## Development loop

```bash
make pg-start && make gateway             # stock Postgres 14+ on :5499, gateway on :8080
make loadtest AGENTS=500 REQUESTS=8       # another shell; exits non-zero if a probe fails

make kernel                               # fetch upstream + apply patches + build (first run ~10 min)
make kernel-check                         # upstream regression suite plus agent_identity
make e2e KERNEL=1                         # gateway + load test + kernel probes on the kernel
```

## Kernel patches

The kernel is upstream PostgreSQL at the commit in `kernel/BASE` plus the series in
`kernel/patches/`. Work in `swarmpg-core/` (created by `make kernel`, ignored by git):

1. Commit each change on top of the base with a topic tag in the subject:
   `[principal]`, `[budget]`, `[space]`, `[frontend]`, `[sql-agent]`.
2. Prefer a hook, access method or background worker over editing upstream code paths.
3. Ship a regression test with every patch and keep `make kernel-check` green.
4. Never change a WAL record format.
5. Run `kernel/build.sh export` and commit the regenerated `kernel/patches/`.

See [kernel/FORK-NOTES.md](kernel/FORK-NOTES.md) for the mechanism notes behind each patch.

## Pull requests

- CI must pass: Go vet and tests, the end-to-end run on stock Postgres and on the kernel,
  and the kernel regression suite.
- A change to the isolation boundary needs a probe in `loadtest/` that fails without it.
- The README, `docs/getting-started.md` and `docs/gateway.md` are run as written in CI
  (`scripts/check-docs.sh`). Keep their `bash` blocks runnable against a local gateway, or
  mark a block `<!-- doctest: skip -->`; `<!-- doctest: expect TEXT -->` asserts its output.
