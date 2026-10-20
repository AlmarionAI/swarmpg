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
| **Kernel** (`kernel/`) | The agent's identity, agent and org together, is one *sealed* value: once the gateway sets it for a transaction, no `SET`, `RESET` or `set_config()` can change either half, and role defaults or startup options cannot pre-set it. With a server secret, it must also carry the gateway's HMAC signature, so SQL alone cannot claim any identity. No query runs until an identity is sealed. Per-agent budgets live in the executor: a cost gate refuses runaway plans before they start, and a row cap bounds what a statement returns or modifies. The gateway can lower budgets per agent; the agent cannot raise them. |
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
make e2e KERNEL=1                 # the same run on the kernel, plus kernel and budget probes
```

## What is verified

Every CI run asserts the isolation boundary. The load test drives a fleet through the
gateway, then 27 probes attack it through the API: identity spoofing, privilege
escalation, reading another agent's rows, DDL, pool leaks through temp tables or held
cursors, terminating other backends, advisory locks, and reading or tampering with the
registry, audit log and journal. On the kernel, 11 more probes connect to Postgres directly
as the agent role, **bypassing the gateway**: unsealed queries, half-set or forged
identities, identity changes, reads of another agent's or org's rows, and attempts to
raise a budget are all refused. 6 budget probes run through the gateway: an over-budget
write fails as a whole, and runaway queries are refused before they run, including under
`EXPLAIN ANALYZE` and inside `query_to_xml()`.

A single laptop run (Apple M-series, stock Postgres 14): 2,000 agents on 32 backend
connections, 6,120 requests/s, p99 519 ms, all probes passing. This is a smoke test, not a
benchmark.

## Known limits (v0.1)

- On stock Postgres, identity and budgets depend on the gateway; the kernel is what makes
  them hold for raw SQL.
- The kernel row cap does not apply to SQL `FETCH` from a cursor (the gateway refuses
  cursors; the cost budget still applies). There is no byte budget in the kernel yet;
  the gateway enforces one.
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
