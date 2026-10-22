# Sustained multi-gateway admission workload

This runner checks shared organization ceilings and useful quiet-tenant progress under
sustained noisy traffic. It runs through the [owned-service wrapper](../scripts/admission-e2e.sh),
which starts one temporary PostgreSQL database and three gateway processes. Use it on
stock PostgreSQL and the signed SwarmPG kernel with the same policies and client settings.
The [admission contract](../docs/admission.md) defines the feature's guarantees and limits.

This is a bounded correctness and isolation experiment. Thousands of participating agent
identities do not mean thousands of simultaneous HTTP requests or PostgreSQL sessions.
The client deliberately bounds concurrency and polls with backoff. Reports must retain
those settings when quoted; this runner is not a claim of unrestricted fleet capacity or
higher throughput than stock PostgreSQL.

## Run

From the repository root, with Go, Python 3, curl and PostgreSQL client tools installed:

<!-- doctest: skip -->
```bash
# PostgreSQL 17 in an owned Docker container; local gateway processes.
ADMISSION_DB_MODE=docker ADMISSION_WORKERS=2000 ADMISSION_DURATION=300s make admission-test

# A validated signed-kernel build; explicit binary directory is required.
KERNEL=1 PG_BIN_DIR=/path/to/validated/kernel/bin \
  ADMISSION_WORKERS=2000 ADMISSION_DURATION=300s make admission-test
```

CI runs 120 seconds of sustained production on each engine. The longer local acceptance
configuration is 300 seconds per engine. Setup, six isolated probes, crash recovery and
final task drain add time beyond `ADMISSION_DURATION`. A 20-second development run can
check the harness but is not a substitute for either acceptance duration.

| Setting | Default | Meaning |
|---|---|---|
| `ADMISSION_DB_MODE` | `local` | Fresh local cluster, or owned Docker `postgres:17` container |
| `PG_BIN_DIR` | Directory containing `psql` | PostgreSQL client/server tools; required explicitly for `KERNEL=1` |
| `ADMISSION_WORKERS` | `2000` | Worker identities, minimum 2000 |
| `ADMISSION_DURATION` | `120s` | Sustained production duration, minimum 20s |
| `ADMISSION_EXECUTION_MS` | `3000` | Gateway execution budget for this workload; permit TTL adds 10 seconds |
| `ADMISSION_DATABASE_PORT` | `55500` | Owned database listener |
| `ADMISSION_GATEWAY_PORT` | `55501` | First of three consecutive gateway ports |
| `ADMISSION_REPORT_DIR` | New temporary directory | Must be empty; evidence is never overwritten |

The wrapper refuses occupied ports, uses isolated test credentials and a fresh database, and
only kills services it created. It injects failures into that owned database and gateway
zero. Do not invoke the binary against an existing deployment: probes temporarily replace
the admission function and use administrative SQL to establish isolated test conditions.
Cleanup removes the owned Docker container or stops the owned local cluster; reports,
binaries and logs remain in the printed run directory. Local cluster files are retained
there too. Audit uses `best-effort` with an in-memory spool; audit crash durability is not
part of this experiment.

The standalone binary has `-seed` (default `20261022`) and `-quiet-p99-ms` (default `10000`)
options. The wrapper uses those defaults. Set acceptance criteria before running; changing
an SLO after seeing a failure is a different experiment, not a passing original run.

## Traffic and capacity

At the default 2,000 workers, identity assignment is 1,400 `noisy`, 200 `quiet-a` and 400
`quiet-b`. Three additional producer identities and an isolated probe organization are
setup actors and are not counted as participating workers. Every worker dispatches a
real initial query; noisy workers then repeatedly execute `pg_sleep(0.05)`, while quiet
workers claim and complete tasks. Requests rotate across replicas; renewal and completion
use different replicas from the claim.

| Tenant | General rate / burst / concurrency | Control rate / burst / concurrency |
|---|---|---|
| noisy | 40 / 80 / 4 | 100 / 200 / 4 |
| quiet-a | 200 / 400 / 12 | 100 / 200 / 6 |
| quiet-b | 200 / 400 / 12 | 100 / 200 / 6 |

Each worker's local per-agent rate cap is disabled for this experiment; concurrency is 8
and its statement timeout is 2,500 ms. The gateway still enforces organization admission
and the independent execution deadline. Each gateway has 32 runtime connections, plus
4 admin/audit, 4 authentication and 4 admission connections. The test database allows 200
connections to accommodate three gateways and verification.

Client backpressure is explicit:

- At most 384 HTTP calls pass the global request gate at once. This is a bound on active
  calls, not the number of registered identities or a count of open sockets. The HTTP
  transport also caps connections per host at 192.
- Noisy general calls have a 96-call gate; each quiet tenant's general calls have a
  48-call gate. Each tenant's control calls have a separate 32-call gate.
- Each quiet tenant permits at most eight simultaneous claim operations, leaving room
  for useful work instead of allowing empty polls to fill every request slot.
- Noisy workers pause 300–499 ms between attempts. Empty claim polls pause 2–4 seconds.
  Retried calls preserve their encoded request body and UUID, with bounded backoff.
  The HTTP client timeout is eight seconds.

Each quiet producer submits at most about five tasks per second, reduced by enqueue and
retry time. Tasks contain a small arithmetic payload. A worker claims a three-second
lease, renews after 750 ms, and completes after another 450 ms with the computed result.
The simulated 1,200 ms work interval happens outside a database transaction. General
claims and control renewals/completions therefore exercise different admission lanes.
Sustained traffic uses REST; the isolated class probe also exercises MCP completion.

## Failure probes

The six fixed probes run before sustained production, in a separate organization:

1. **Shared concurrency:** simultaneous slow queries through all three gateways under a
   concurrency limit of one must produce one success and two organization denials.
2. **Shared rate and policy reload:** a one-token, one-token-per-second policy grants
   once, then rejects other replicas within the same refill interval. Reloading the
   policy must not supply another free burst.
3. **Live reduction:** two existing permits remain live after reducing the limit to one;
   a new request is refused until capacity becomes available.
4. **Reserved control and header spoofing:** general capacity is filled; a query with a
   forged control-tool header remains general and is refused. REST renewal and MCP task
   completion still use their control capacity successfully.
5. **Quota failure:** the owned admission function temporarily raises an error while an
   independent database `SELECT 1` still works. Gateway admission must fail closed with
   `503 admission_unavailable`, with no task committed by the attempted write; the original
   function is restored.
6. **Gateway death:** a gateway is killed while holding a slow-query permit. The
   reservation remains, other replicas refuse excess work, and expiry restores capacity
   without a manual early release.

Halfway through sustained production, gateway zero is killed and restarted again. The
other two continue operating. A bounded five-second fault window permits transient
transport/service failures and retries; it does not exempt quiet tenants from the
completion progress or latency checks.

## Acceptance and retained evidence

Before execution, the runner sets a **10,000 ms quiet-tenant completion p99 ceiling** and
**10-second progress windows**. Completion latency starts at the task's database creation
time and includes queueing, retries and work. Each quiet tenant must complete at least
one task in every full sustained window after the initial ten-second startup window.
The final partial window is not required. After production stops, workers get up to
45 seconds beyond the configured duration to drain already-created tasks under the same
limits; those completions still count toward the latency ceiling.

A passing run requires all configured workers to have dispatched a real request, observed
organization rejections for noisy traffic, successful probes and fault recovery, useful
renewed/completed quiet work, and no missing, duplicated or corrupt persisted task results.
Participation and completing-worker counts are separate: every quiet identity need not
win a task. Final SQL checks compare every sustained task with the expected organization,
key and result, require a valid owner from that organization, one completion history row,
and matching completion owner/fence attribution.

`report.json` retains:

- Commit and source-tree fingerprint, runtime/database environment, policies, pool sizes,
  client gates, seed, timing settings and actual duration.
- Requests, successes, rejections, HTTP and server admission latency by tenant/class;
  participants, completing workers, created/completed/verified tasks, renewal counts,
  completion latency and progress windows.
- Shared active-permit samples, cumulative grant deltas, six probe receipts, gateway crash
  timing, gateway health before/after, database transaction/connection snapshots, retries
  and correctness errors.

Active permits are sampled about every 200 ms, sequentially across organizations. Samples
can detect an observed breach but cannot exclude a shorter unsampled breach. Exact
serialization is also tested by parallel SQL acquisitions in the gateway integration
suite and by the three-replica concurrency probe. Rate verification uses authoritative
SQL grant-counter deltas rather than sampled HTTP successes, and requires each class's
grants to remain within `burst + rate × measured elapsed seconds` (plus one rounding
allowance). The elapsed interval includes drain and cancellation cleanup.

Keep the JSON report together with `workload.log`, gateway logs and schema/database logs
when comparing runs. `passed: true` is evidence for those settings and faults only. This
runner does not test global weighted scheduling, distributed PostgreSQL failover, clock
jumps, unbounded offered concurrency, production datasets or long-term storage growth.
It does not replace the full stock/kernel suites, upstream PostgreSQL regressions, the
[task failure workload](../coordination/README.md) or [outbox recovery tests](../outbox/README.md).
