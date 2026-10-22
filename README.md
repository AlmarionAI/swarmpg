# SwarmPG

[![ci](https://github.com/AlmarionAI/swarmpg/actions/workflows/ci.yml/badge.svg)](https://github.com/AlmarionAI/swarmpg/actions/workflows/ci.yml)

**Postgres for agent fleets.** SwarmPG gives agents private data, versioned memory,
controlled sharing and resource limits through a pooled REST/MCP gateway. A small
PostgreSQL kernel patch adds a sealed agent identity and executor budgets.

A shared connection pool needs a trustworthy answer to “which agent is acting now?”
A user-writable identity setting is unsafe when that same user can submit arbitrary SQL.
SwarmPG binds the agent and org together inside each transaction; with a principal secret,
the kernel verifies that identity and refuses SQL without it. This makes one low-privilege
role usable across a pool without creating a database role per agent. Secure stock
PostgreSQL deployments can use trusted gateways or distinct roles; the kernel's advantage
is enforcing this pooled principal even when an agent bypasses the gateway.

## What is implemented

- **Identity and isolation.** Forced row-level security protects private rows. On the
  kernel, the agent and org are sealed together; signing requires `PRINCIPAL_SECRET`.
  Agents cannot create objects or inspect other agents' SQL.
- **Budgets.** The gateway limits rate, concurrency, result rows/bytes and statement time.
  The kernel adds a planner-cost gate and cumulative attempted-write limits, including
  nested functions, CTEs and trigger bookkeeping. It does not meter CPU or IO.
- **Versioned memory.** Compare-and-set, history and undo support read-think-write without
  an open transaction. Shared reads identify their producer; callers can choose a
  producer or list the visible shared values. Sharing is owner-written and org-readable,
  not a jointly editable workspace.
- **Task coordination.** Org-shared queues, dependency-aware claims, expiring leases,
  monotonic fencing tokens and atomic completion against declared memory versions.
  Durable request receipts resolve retries; attributed history commits with transitions.
- **REST and MCP.** Sixteen MCP tools expose queries, schema, memory, tasks and the event journal,
  with structured errors for clients to handle.
- **Journal and query audit.** The event journal is append-only and hash-chained per
  agent. SQL-query audit has a bounded retry spool and optional disk persistence; strict mode
  requires a persistent spool and refuses queries when an audit intent cannot be saved.
  Default best-effort mode can lose audit records. See the [audit contract](docs/gateway.md#audit-delivery).

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
REST, MCP, SQL, versioned memory and the error codes; CI runs its marked runnable
examples. The quickstart uses development credentials; see the deploy checklist at the end of that guide.

## Benchmark

Historical runs from 2026-10-20 used 10,000 agent identities, 1,000 active workers and
200,000 requests per setup, with the same Postgres build and 32 database connections
([method, both runs, caveats](bench/README.md)):

| Setup | Req/s | p95 ms | p99 ms | Another agent's rows via `SET LOCAL` |
|---|---:|---:|---:|---|
| PgBouncer + row-level security | 10.7–11.0k | 110–115 | 115–128 | readable |
| SwarmPG gateway + stock Postgres | 22.0–26.5k | 47–65 | 75–121 | refused (gateway) |
| SwarmPG gateway + SwarmPG kernel | 22.1–25.8k | 48–63 | 59–79 | refused, even with raw SQL |

These short, single-machine runs compare the gateway with a four-round-trip transaction
baseline whose RLS identity is user-writable. The gain mainly measures frontend transaction
handling, not a faster PostgreSQL storage engine. They do not prove sustained swarm
collaboration, fairness or recovery under failures. They also predate the current audit and
budget changes; rerun `bench/run.sh` before making new performance claims.

## How it works

| Layer | Enforces |
|---|---|
| **Gateway** (`gateway/`, Go) | Agent tokens, rate and concurrency limits, result caps, a statement guard, a clean connection for every request, REST and MCP |
| **Schema** (`sql/schema.sql`) | Row-level security, revoked fleet-wide levers, versioned memory, the journal, the audit log. Targets stock Postgres 14+ |
| **Kernel** (`kernel/`, PostgreSQL 19 + a small patch series) | The sealed, signed agent principal and a fail-closed gate; tighten-only row and cost budgets in the executor |

The gateway and schema target Postgres 14+; the CI stock-database job uses Postgres 17.
The kernel is the pinned upstream revision in `kernel/BASE` plus a small patch series.
Its principal secret is required when raw SQL access is in the threat model. Broader version and extension
compatibility need their own validation.

## Status and next milestone

**Pre-public v0.1 development snapshot.** This is an early implementation for evaluation,
not evidence of production readiness or a completed public launch. CI defines Go checks, stock/kernel
end-to-end probes and upstream kernel regression tests. SDK interoperability and benchmark
results reported for earlier revisions do not substitute for checks on the current commit.

**Durable task coordination** is implemented in the schema and exposed through REST/MCP.
The [task contract](docs/tasks.md) defines ownership, lease timing, fencing, input-version
validation and retry behavior. The [failure workload](coordination/README.md)
exercises 100–1,000 worker identities against stock and kernel deployments, with restarts,
abandoned claims, duplicate requests, changed inputs and a noisy tenant. Its reports count
correct completed work and record the conditions tested; they are not a production scale guarantee.

Workspaces come after that proof. Storage branching, catalog virtualization, process-model
rewrites and scale-to-zero are deferred research, outside the next milestone.
[Design and acceptance criteria](docs/DESIGN.md) and [known limits](docs/THREAT-MODEL.md#residual-risks)
separate implemented behavior from planned work.

## Docs

- [Getting started](docs/getting-started.md): REST, MCP and SQL, step by step
- [Gateway reference](docs/gateway.md): the API, configuration and what each layer enforces
- [Tasks](docs/tasks.md): claims, leases, fencing, atomic completion and retry contracts
- [Threat model](docs/THREAT-MODEL.md): attackers, controls, the probe for each, residual risks
- [Benchmark](bench/README.md): method, results, how to reproduce
- [Kernel](kernel/README.md): the patch series and how to build it
- [Design](docs/DESIGN.md): scope, next milestone and deferred research

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
