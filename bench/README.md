# Benchmark: 10,000 agents

SwarmPG against the common way of giving agents SQL access: PgBouncer in transaction mode
in front of Postgres with row-level security, where every agent holds its own connection
and sets its identity with `SET LOCAL`. Same Postgres build, same schema and policies,
same statements, and the same 32 server connections in every setup.

## Results

Two consecutive runs on 2026-10-20, each with 10,000 agents × 20 requests (200,000
requests per setup), 1,000 agents in flight, a fresh cluster per setup.

| Setup | Agent connections | Errors | Req/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| PgBouncer + RLS (baseline) | 10,000 | 0 / 0 | 11,000 / 10,706 | 87 / 89 | 110 / 115 | 115 / 128 |
| SwarmPG gateway + stock Postgres | 1,000 | 0 / 0 | 26,480 / 21,990 | 37 / 42 | 47 / 65 | 75 / 121 |
| SwarmPG gateway + SwarmPG kernel | 1,000 | 0 / 0 | 25,763 / 22,062 | 38 / 43 | 48 / 63 | 59 / 79 |

Values are run 1 / run 2 ([`20261020T220935Z`](results/20261020T220935Z/results.md),
[`20261020T221143Z`](results/20261020T221143Z/results.md)). Every setup peaked at 32
Postgres backends.

- **Throughput.** SwarmPG served 2.0–2.4× the baseline's requests per second, with lower
  median, p95 and p99 latency in both runs (the closest: p99 of 121 ms against 128 ms).
- **The kernel costs nothing measurable here.** Sealing a signed principal, the budgets
  and the cost gate came within run-to-run noise of stock Postgres.
- **Frontend memory.** PgBouncer used about 67 MiB holding 10,000 client connections; the
  gateway about 100 MiB serving 10,000 agents. (Postgres container memory varied between
  54 and 224 MiB from run to run because of cache accounting, so it is not compared.)
- **Variance.** The gateway moved about 17% between the two runs and the baseline about
  3%. The gateway does more CPU work (HTTP, JSON, auth) on the same machine as the load
  generator and Postgres, so it is more sensitive to whatever else the machine is doing.

### Why the gateway is faster here

With PgBouncer in transaction mode, the agent drives each transaction: `BEGIN`, the
`SET LOCAL`s, the statement and `COMMIT` are four round trips from the agent through
PgBouncer, and the server connection stays assigned for all of them. The gateway receives
one HTTP request and runs the same transaction next to the database, so each server
connection is held for less time and the same 32 connections serve more agents. Agents also
hold no database connection between requests.

### Security, measured the same way

After the load phase the benchmark tries three attacks on the baseline, as an agent with
its own connection:

| Attack | PgBouncer + RLS | SwarmPG |
|---|---|---|
| Read another agent's memory by changing identity (`SET LOCAL app.agent_id`) | **succeeds** (read 5 rows) | refused (guard on stock; sealed principal on the kernel) |
| Runaway cross join | **runs** for the full 5 s statement timeout, holding a server connection | refused before it runs (kernel cost budget) |
| Session-level `SET` of the identity, which then persists on the pooled server connection | **succeeds** | refused |

SwarmPG's side is covered by the 65 stock and 83 kernel probes in `loadtest/`; see
[docs/THREAT-MODEL.md](../docs/THREAT-MODEL.md).

### A finding this benchmark produced

The first run showed the gateway at 10.4k req/s with a p95 of 974 ms. Each agent's first
request needs a token lookup, and those lookups shared a 4-connection admin pool with one
audit insert per query and one last-seen update per agent, so they queued behind thousands
of bookkeeping writes. The gateway now batches both: up to 500 audit records per statement
every 50 ms, and last-seen times every 5 s. That produced the numbers above.

## Method

- **Workload.** Each agent sends its requests back to back (closed loop, no think time),
  cycling through four operations, half of them writes: write a memory key
  (`swarmpg.memory_put`, versioned), read the 5 newest memory rows, append an event to the
  hash-chained journal, and count events of one kind.
- **Concurrency.** 1,000 agents in flight at a time; each finishes its 20 requests, then
  the next agent starts. 10,000 agents in total.
- **Baseline.** Each agent opens its own connection to PgBouncer 1.24.1 (Debian package)
  before the timed phase: transaction pooling, `default_pool_size = 32`, protocol-level
  prepared statements. Per request: `BEGIN`; `SET LOCAL` of the identity and the same
  statement, lock and idle timeouts the gateway sets; the statement; `COMMIT`.
- **SwarmPG.** Each agent has a token; requests go to the REST API over keep-alive HTTP. The
  gateway's runtime pool is 32 connections. Per request it authenticates the token,
  applies the agent's limits, runs the transaction (identity, timeouts and, on the kernel,
  the signed principal and budgets), resets the connection, and audits every query.
- **Postgres.** PostgreSQL 19rc1 at upstream commit `0da632e` (`kernel/BASE`) in every
  setup: unpatched for the baseline and SwarmPG-stock, with `kernel/patches` for
  SwarmPG-kernel. `shared_buffers = 512MB`, `max_connections = 200`, defaults otherwise.
- **Measured.** Latency per request on the client, from send to full response;
  throughput over the timed phase only (connecting or provisioning is excluded); peak
  agent backends sampled from `pg_stat_activity` every 100 ms; container memory streamed
  from `docker stats`.
- **Machine.** Apple M3 Max; Docker Desktop 29.7.2 with 16 CPUs and 15 GiB. The load
  generator, the frontend and Postgres all ran on it, and two unrelated containers were
  running.

## Caveats

- One machine: the load generator, frontend and database compete for the same CPUs.
- Closed-loop load with no think time measures saturation. Real agents spend most of
  their time thinking, which makes connection-heavy designs costlier (idle connections
  still cost PgBouncer and the agents' hosts) and changes nothing for the gateway, which
  holds none.
- PgBouncer could serve more with a larger pool; the pool size was held equal on purpose.
- Two runs are a small sample. Run it yourself, and expect the gateway's numbers to move
  with the machine's load.

## Reproduce

Needs Docker. The first run builds Postgres twice (upstream and kernel), about 10–15
minutes; later runs take about 3 minutes.

```bash
bench/run.sh                                   # 10,000 agents x 20 requests, 1,000 in flight
AGENTS=2000 REQUESTS=5 CONCURRENCY=500 bench/run.sh
```

Results land in `bench/results/<UTC timestamp>/`: one JSON file per setup and
`results.md`. The stack is in [`docker-compose.yml`](docker-compose.yml), the client in
[`main.go`](main.go).
