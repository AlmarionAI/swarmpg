# SwarmPG

**Postgres for agent fleets.** Every agent gets its own identity, enforced by the database
itself, plus isolation, budgets and an audit trail, while thousands of agents share a few
dozen Postgres connections.

> **Status: v0.1, pre-release.** The gateway and agent schema work on stock Postgres 14+.
> The SwarmPG kernel (PostgreSQL 19 plus a small patch series) adds an agent identity that
> SQL cannot change. The storage rebuild in [docs/DESIGN.md](docs/DESIGN.md) (branching,
> time travel, millions of lightweight per-agent spaces) is the long-term direction and is
> **not** built yet.

## Why

Agents write their own SQL. On plain Postgres, row-level security keyed on a session
setting is only as strong as the agent's inability to change that setting, a shared role
lets one looping agent exhaust the fleet, and nothing records what each agent actually ran.
SwarmPG closes those gaps in three layers:

| Layer | What it enforces |
|---|---|
| **Kernel** (`kernel/`) | `agent_id` is a *sealed* setting: once the gateway sets it for a transaction, no `SET`, `RESET` or `set_config()` can change it, and role defaults or startup options cannot pre-set it. With `agent_require_principal` on, the kernel refuses to run any query until an identity is sealed. |
| **Schema** (`sql/schema.sql`) | Forced row-level security on every agent table; no DDL; fleet-wide levers revoked (backend signalling, other agents' in-flight SQL, advisory locks, large objects, `set_config`); an append-only, hash-chained event journal; a server-side audit log agents cannot read or write. |
| **Gateway** (`gateway/`) | Token auth, per-agent rate, row, byte, concurrency and time budgets, a statement guard, connection reset between agents, idempotent writes, and errors structured for an LLM to repair its own SQL. |

## Quickstart

```bash
docker compose up --build        # kernel + gateway on localhost; first build compiles Postgres (~10 min)
```

Then mint an agent and query as it:

```bash
ORG=$(curl -s -H 'X-Admin-Key: dev-admin-key' -d '{"name":"demo"}' localhost:8080/v1/orgs | jq -r .id)
TOKEN=$(curl -s -H 'X-Admin-Key: dev-admin-key' -d "{\"org_id\":\"$ORG\",\"name\":\"a1\"}" localhost:8080/v1/agents | jq -r .token)
curl -s -H "Authorization: Bearer $TOKEN" localhost:8080/v1/schema
curl -s -H "Authorization: Bearer $TOKEN" -d '{"sql":"SELECT count(*) FROM swarmpg.memory"}' localhost:8080/v1/query
```

The quickstart uses trust authentication and a default admin key. It is for local
development only.

## From source

Needs Go 1.23+, and Postgres 14+ on `PATH` for the stock path.

```bash
make test                         # Go vet and unit tests
make e2e                          # stock Postgres: gateway + fleet load test + isolation probes
make kernel && make kernel-check  # build the SwarmPG kernel and run the upstream regression suite
make e2e KERNEL=1                 # the same end-to-end run on the kernel, plus kernel probes
```

## What is verified

Every CI run asserts the isolation boundary. The load test drives a fleet through the
gateway, then 27 probes attack it through the API: identity spoofing, privilege
escalation, reading another agent's rows, DDL, pool leaks through temp tables or held
cursors, terminating other backends, advisory locks, and reading or tampering with the
registry, audit log and journal. On the kernel, 4 more probes connect to Postgres directly
as the agent role, **bypassing the gateway**, and check that the kernel still refuses
unsealed queries and identity changes.

A single laptop run (Apple M-series, stock Postgres 14): 2,000 agents on 32 backend
connections, 6,120 requests/s, p99 519 ms, all probes passing. This is a smoke test, not a
benchmark.

## Known limits (v0.1)

- Only `agent_id` is sealed in the kernel; `org_id` is protected by the gateway and revoked
  functions, not sealed yet.
- Budgets are enforced by the gateway, not yet in the executor.
- No private DDL: agents use the shared `memory`, `documents` and `events` tables.
- One gateway process; rate limits are in memory.

## Docs

- [docs/gateway.md](docs/gateway.md): the HTTP API and the security model in detail
- [docs/DESIGN.md](docs/DESIGN.md): the long-term architecture and its risks
- [kernel/README.md](kernel/README.md): the kernel patch series and how to work on it
- [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md)

## License

Apache-2.0 ([LICENSE](LICENSE)), except the kernel patches in `kernel/`, which modify
PostgreSQL and are under the PostgreSQL License ([kernel/LICENSE](kernel/LICENSE)).
Contributions require the [CLA](CLA.md).

PostgreSQL is a trademark of the PostgreSQL Community Association of Canada. SwarmPG is an
independent project, not affiliated with or endorsed by the PostgreSQL Global Development
Group.
