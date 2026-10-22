# Collaborative task failure workload

This workload tests the durable task contract through the real HTTP gateway. It
creates 100–1,000 worker identities in two organizations, repeatedly produces
arithmetic dependency graphs, and checks persisted results and transition history
after failures. Every registered worker must dispatch a claim request; waiting for
client admission does not count as participation. The report separately counts
workers that actually complete tasks.

Run from the repository root with Go, Python 3, curl and PostgreSQL client tools:

```bash
# Stock PostgreSQL 17 in an owned Docker container.
COORD_DB_MODE=docker COORD_WORKERS=100 \
  COORD_REPORT_DIR="$PWD/.coord-test/stock-100" scripts/coordination-e2e.sh
COORD_DB_MODE=docker COORD_WORKERS=1000 \
  COORD_REPORT_DIR="$PWD/.coord-test/stock-1000" scripts/coordination-e2e.sh

# Patched kernel, after make kernel. Each run creates a fresh cluster.
KERNEL=1 PG_BIN_DIR="$PWD/.pginstall/bin" COORD_WORKERS=100 \
  COORD_REPORT_DIR="$PWD/.coord-test/kernel-100" scripts/coordination-e2e.sh
KERNEL=1 PG_BIN_DIR="$PWD/.pginstall/bin" COORD_WORKERS=1000 \
  COORD_REPORT_DIR="$PWD/.coord-test/kernel-1000" scripts/coordination-e2e.sh
```

Choose a new report directory for each run. The wrapper starts and stops only its
own database and gateway. It deliberately kills/restarts both services; it never
accepts an existing database URL. Reports and logs survive cleanup. Local cluster
files also remain for diagnosis. Docker containers are removed.

## What is exercised

- Immutable producer memory inputs and dependency results determine each expected
  arithmetic answer. Final checks read the database through the API after workers
  stop; successful HTTP responses alone do not establish correctness.
- Workers pause beyond their lease, abandon claims and retry uncertain completion
  acknowledgements. Separate deterministic probes exercise renewal, reclaim,
  stale fencing, changed inputs, identical completion replay and conflicting results.
- Healthy workers renew every third of a lease while reading inputs and calculating;
  deliberate pause faults never renew. Idle workers use seeded jitter with an
  exponential backoff capped at two seconds. Claim requests have a separate admission
  limit of 16, acquired before the shared HTTP limit, to leave capacity for lease
  holders. A heartbeat already in flight finishes before completion. Reports include
  both admission limits, empty claims and renewal outcomes.
- The gateway receives SIGKILL and restarts; the database is crash-restarted while
  workers are active. Each fault must interrupt requests and be followed by a
  successful completion. Transient failures are retried within bounded fault windows;
  authentication and other unexpected failures fail the run.
- Each retrying operation has a 30-second deadline, or the caller's earlier
  deadline, covering queue waits, HTTP attempts and backoff. This exceeds the
  default 20-second organization admission permit lease so reservations left by
  a killed gateway can expire safely. Every 429 is recorded and retried after
  one second with the identical body and request ID; stale or expired task leases
  still follow the normal reclaim/fencing path. Transport and server errors
  remain retryable only within the recorded fault window.
- A noisy agent floods queries under a small admission budget while the quiet
  organization continues processing tasks. Both organizations must finish every
  generated task. Per-organization latency and rejection outcomes are reported.
- Validation rejects lost work, incorrect results, foreign attribution, altered
  dependencies/inputs, nonmonotonic fences, overlapping live owners and duplicate
  completion history. Unit tests deliberately corrupt fixtures to check the validator.

## Settings and evidence

| Environment variable | Default | Meaning |
|---|---|---|
| `COORD_WORKERS` | `100` | Worker identities, 100–1,000 |
| `COORD_DURATION` | `30s` | Continuous graph production; setup, drain and verification take additional time |
| `COORD_IN_FLIGHT` | `128` | Maximum concurrent HTTP requests across workers |
| `COORD_LEASE_MS` | `3000` | Normal worker lease, in milliseconds |
| `COORD_SEED` | `20261022` | Deterministic graph/request seed; concurrency scheduling still varies |
| `COORD_DB_MODE` | `local` | `local` cluster or `docker` container |
| `COORD_PG_IMAGE` | `postgres:17` | Stock Docker image |
| `PG_BIN_DIR` | PostgreSQL tools on PATH | Binary directory; use the patched install with `KERNEL=1` |
| `COORD_DATABASE_PORT` / `COORD_GATEWAY_PORT` | `55450` / `55451` | Must be unused |
| `COORD_REPORT_DIR` | Fresh temporary directory | Retained evidence directory |

`report.json` includes the source commit and source-tree SHA-256, database/Go/OS
versions, settings, worker participation, throughput, latency percentiles, retries,
rejections, correctness counters, fault recovery times and event traces. The wrapper
also retains gateway, schema and workload logs, plus local PostgreSQL logs. CI runs
both worker counts on stock and kernel and uploads the reports and logs even on failure.

Workers share a 32-connection runtime pool. Claim concurrency is
`min(16, max(1, COORD_IN_FLIGHT / 2))`; other requests share the total HTTP limit.
Thus 1,000 worker identities do not mean 1,000 simultaneous database connections.
This exercises clients using backpressure, not unthrottled overload. The default 30-second production window
is a reproducible correctness/recovery experiment, not a saturation benchmark or
production capacity guarantee. The noisy-neighbor case measures current per-agent
admission; it does not establish organization-wide or multi-gateway fairness.
External tool side effects, retention, backup/PITR and distributed failover are outside
this test. See [the task contract](../docs/tasks.md) for those limits.

## Local acceptance on 2026-10-22

All four 30-second production runs passed, including gateway/database crash recovery:

| Engine | Workers participating | Workers completing tasks | Verified tasks | Reclaimed tasks |
|---|---:|---:|---:|---:|
| Stock PostgreSQL 17 | 100 | 100 | 1,728 | 59 |
| Stock PostgreSQL 17 | 1,000 | 972 | 7,200 | 248 |
| Signed SwarmPG kernel | 100 | 99 | 1,776 | 61 |
| Signed SwarmPG kernel | 1,000 | 966 | 7,360 | 253 |

Every run recorded zero lost results, duplicate completions, stale overwrites,
attribution failures and dependency failures. See the [measurement summaries](results/2026-10-22.json)
for settings, source fingerprints, per-tenant latency, retries, rejections and fault
recovery traces. Full local reports remain in the recorded `.coord-test` paths; CI
uploads full reports/logs for each PR revision. These local runs used Linux ARM64
Docker for stock and native macOS ARM64 for the kernel, so their timings do not
establish a performance advantage for either engine.
