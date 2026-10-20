# SwarmPG

[![ci](https://github.com/AlmarionAI/swarmpg/actions/workflows/ci.yml/badge.svg)](https://github.com/AlmarionAI/swarmpg/actions/workflows/ci.yml)

**Postgres for agent fleets.** Give thousands of AI agents their own slice of one Postgres
database: an identity they cannot change, data no other agent can read, budgets they
cannot raise, and memory they can safely share, roll back and audit. They write ordinary
SQL, over REST, MCP or both.

Agents are the new database users, and the usual setup was built for trusted application
code. If an agent can send SQL on a connection, it can change the setting row-level
security trusts and read another agent's data, run a query that occupies the database for
minutes, or overwrite another agent's work. SwarmPG closes each of those in the database
itself, and the [test suite attacks every one of them](docs/THREAT-MODEL.md) on every change.

## What agents get

- **An identity the database enforces.** Agent and org are sealed into the transaction;
  no `SET`, function or role trick can change them. On the SwarmPG kernel the identity is
  also signed, so it holds even for SQL that never went through the gateway.
- **Isolation by default.** Forced row-level security on every agent table, no DDL, and
  no view of other agents' queries or of fleet-wide statistics.
- **Budgets.** Rate, concurrency, row, byte and time limits per agent. On the kernel they
  live in the executor: a query whose estimated cost exceeds the agent's budget is refused
  before it runs, a write over the row budget fails as a whole, and no agent can raise its
  own.
- **Memory built for agents.** Every write is versioned. Compare-and-set makes
  read-think-write safe without holding a transaction open, and any key can be read as it
  was, or undone.
- **MCP and REST.** A built-in MCP endpoint (protocol 2026-07-28 and earlier) with eight
  tools, the same operations over REST and as SQL functions, and errors written for a model
  to repair its own SQL.
- **A record of what happened.** An append-only, hash-chained journal per agent, and a
  server-side audit log of every SQL statement an agent sends, refused ones included.

## Quickstart

<!-- doctest: skip -->
```bash
docker compose up --build    # the kernel and the gateway on localhost:8080 (first build ~10 min)
```

<!-- doctest: expect swarmpg.memory -->
```bash
ORG=$(curl -s -H 'X-Admin-Key: dev-admin-key' -d '{"name":"demo"}' localhost:8080/v1/orgs | jq -r .id)
TOKEN=$(curl -s -H 'X-Admin-Key: dev-admin-key' -d "{\"org_id\":\"$ORG\",\"name\":\"a1\"}" localhost:8080/v1/agents | jq -r .token)
curl -s -H "Authorization: Bearer $TOKEN" localhost:8080/v1/schema
curl -s -H "Authorization: Bearer $TOKEN" -d '{"sql":"SELECT count(*) FROM swarmpg.memory"}' localhost:8080/v1/query
```

Then connect an MCP client to `http://localhost:8080/mcp` with the header
`Authorization: Bearer <token>`. [Getting started](docs/getting-started.md) walks through
REST, MCP, SQL, versioned memory and the error codes; every command in it runs in CI. The
quickstart uses development credentials; see the deploy checklist at the end of that guide.

## Benchmark

10,000 agents, 200,000 requests per setup, the same Postgres build and the same 32 database
connections everywhere ([method, both runs, caveats](bench/README.md)):

| Setup | Req/s | p95 ms | p99 ms | Another agent's rows via `SET LOCAL` |
|---|---:|---:|---:|---|
| PgBouncer + row-level security | 10.7–11.0k | 110–115 | 115–128 | readable |
| SwarmPG gateway + stock Postgres | 22.0–26.5k | 47–65 | 75–121 | refused (gateway) |
| SwarmPG gateway + SwarmPG kernel | 22.1–25.8k | 48–63 | 59–79 | refused, even with raw SQL |

The gateway runs each transaction next to the database instead of across the agent's round
trips, so the same connections serve 2.0–2.4× the requests. `bench/run.sh` reproduces it.

## How it works

| Layer | Enforces |
|---|---|
| **Gateway** (`gateway/`, Go) | Agent tokens, rate and concurrency limits, result caps, a statement guard, a clean connection for every request, REST and MCP |
| **Schema** (`sql/schema.sql`) | Row-level security, revoked fleet-wide levers, versioned memory, the journal, the audit log. Works on stock Postgres 14+ |
| **Kernel** (`kernel/`, PostgreSQL 19 + 2 patches) | The sealed, signed agent principal and a fail-closed gate; tighten-only row and cost budgets in the executor |

The gateway and schema run on any Postgres 14+. The kernel is PostgreSQL 19 plus a patch
series of about 1,200 lines (half of it tests), rebased on upstream; use it wherever an
agent could reach Postgres without the gateway.

## Status

**v0.1, the first public release.** What is here is tested end to end: 65 attack probes on
stock Postgres and 83 on the kernel, the upstream Postgres regression suite (241/241 on
the kernel), the official TypeScript and Python MCP SDKs, and the benchmark above.

[docs/DESIGN.md](docs/DESIGN.md) describes where SwarmPG is going: storage-level branching
and time travel, millions of lightweight per-agent spaces, scale to zero. **None of that
is built yet**; the design marks what v0.1 ships. [Known limits](docs/THREAT-MODEL.md#residual-risks)
are listed in the threat model.

## Docs

- [Getting started](docs/getting-started.md): REST, MCP and SQL, step by step
- [Gateway reference](docs/gateway.md): the API, configuration and what each layer enforces
- [Threat model](docs/THREAT-MODEL.md): attackers, controls, the probe for each, residual risks
- [Benchmark](bench/README.md): method, results, how to reproduce
- [Kernel](kernel/README.md): the patch series and how to build it
- [Design](docs/DESIGN.md): the long-term architecture

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). Contributions
require the [CLA](CLA.md). Report security issues privately as described in
[SECURITY.md](SECURITY.md).

## License

Apache-2.0 ([LICENSE](LICENSE)), except the kernel patches in `kernel/`, which modify
PostgreSQL and are under the PostgreSQL License ([kernel/LICENSE](kernel/LICENSE)).

PostgreSQL is a trademark of the PostgreSQL Community Association of Canada. SwarmPG is an
independent project, not affiliated with or endorsed by the PostgreSQL Global Development
Group.
