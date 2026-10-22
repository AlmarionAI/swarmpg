# Organization admission and gateway fairness

SwarmPG enforces one organization rate and concurrency policy across every gateway that
uses the same PostgreSQL database. Each gateway also has bounded queues that rotate
between waiting organizations. This protects shared runtime connections from a tenant
that spreads requests across many agents or gateway replicas.

The shared limits and permit state live in ordinary PostgreSQL tables and functions, so
the contract is the same on stock PostgreSQL and the signed SwarmPG kernel. There is no
new kernel patch, external quota service or local-only fallback. Admission is enabled
when the gateway starts; apply the current `sql/schema.sql` before starting a new binary.
Upgrade every gateway before relying on the organization ceiling: older binaries bypass
shared admission, so a mixed-version rollout does not provide the fleet-wide guarantee.

## What is shared, and what is local

| Boundary | Enforcement |
|---|---|
| Organization rate and burst | One PostgreSQL token bucket per organization and request class, shared by all gateways |
| Organization concurrency | One set of unexpired PostgreSQL permits per organization and class, shared by all gateways |
| Fair scheduling | Each gateway rotates queued organizations after each local slot grant; FIFO within an organization's class queue |
| Per-agent rate and concurrency | Process-local limits, independently applied to general and control requests; using more replicas increases an agent's possible aggregate allowance |
| Runtime capacity | Each gateway reserves separate general and control slots within its 32-connection runtime pool |
| CPU, IO, storage and external services | Not metered by admission; outbox dispatch and administrative APIs are separate operator traffic |

This is shared quota isolation plus local round-robin scheduling. There is no global FIFO,
weighted allocation, minimum throughput reservation or cross-gateway queue. A new quiet
organization does not preempt already running requests. Total organization ceilings may
exceed physical fleet capacity; operators must size policies, pools and PostgreSQL for the
workload and measure quiet-tenant latency under contention.

## Request classes

`general` includes SQL queries, schema, memory, events, enqueue, claim, task reads and
cancel. `control` is reserved for the typed task **renew, release and complete** operations.
This lets active task holders renew or finish when claim polls and new work saturate the
general lane. The classes have separate organization buckets, concurrency ceilings,
per-agent limits and local runtime slots. General work cannot borrow idle control slots.

REST classification uses the actual matched route. MCP classification uses the parsed and
validated envelope and actual tool name. A `Mcp-Name` header cannot turn a `query` call into
control work. Invoking a task SQL function through the generic query endpoint remains
general work. Control priority does not bypass task authorization, argument validation,
lease/fence checks, row budgets or the execution deadline.

## Organization policy API

Only the admin key may read or update policies. There are no policy MCP tools. The six
limits must all be supplied on `PUT`; unknown, missing, null, zero, negative and fractional
values are rejected. Rates and bursts may be at most 1,000,000; concurrency at most 10,000.
These are validation ceilings, not tested capacity claims.

| Field | Default | Meaning |
|---|---:|---|
| `general_rps` | 10000 | General tokens replenished per second |
| `general_burst` | 20000 | Maximum general tokens |
| `general_concurrency` | 32 | Unexpired general permits across gateways |
| `control_rps` | 1000 | Control tokens replenished per second |
| `control_burst` | 2000 | Maximum control tokens |
| `control_concurrency` | 8 | Unexpired control permits across gateways |

A default policy is created on first use. One admitted request consumes one rate token;
release restores concurrency capacity but does not refund the token. These are token
buckets, not fixed one-second windows: an initially full bucket can grant up to its burst
plus the tokens replenished over an interval.

For an organization already created by the getting-started examples:

<!-- doctest: expect general_concurrency -->
```bash
curl -s -H 'X-Admin-Key: dev-admin-key' "localhost:8080/v1/orgs/$ORG/admission" | jq .
```

`GET /v1/orgs/{id}/admission` returns `policy` (organization ID, string `revision`, and six
limits), `active:{general,control}`, and cumulative `granted:{general,control}` counts.
The active counts are diagnostic snapshots, not reservations for the next request.
`PUT` returns the updated policy object directly:

<!-- doctest: expect general_concurrency -->
```bash
curl -s -X PUT -H 'X-Admin-Key: dev-admin-key' -H 'Content-Type: application/json' \
  -d '{"general_rps":10000,"general_burst":20000,"general_concurrency":32,"control_rps":1000,"control_burst":2000,"control_concurrency":8}' \
  "localhost:8080/v1/orgs/$ORG/admission" | jq .
```

Updates serialize with grants for both classes. The next acquisition uses the new limits.
A reduction does not cancel existing work: the active count may temporarily exceed the
new ceiling, and no further grant is issued until it falls below that ceiling. Existing
tokens are refilled under the old rate and clamped to the new burst; a policy update does
not hand out a free burst or extend existing permits. Per-agent metadata uses the gateway's
existing short authentication cache; its local counters apply refreshed settings without
resetting active requests.

## Local capacity and database connections

| Environment variable | Default | Allowed values |
|---|---:|---|
| `ADMISSION_GENERAL_CAPACITY` | 24 | 1–31 |
| `ADMISSION_CONTROL_CAPACITY` | 8 | 1–31; general + control must be at most 32 |
| `ADMISSION_ORG_QUEUE` | 128 | 1–10000; per organization, per class; at most total queue |
| `ADMISSION_TOTAL_QUEUE` | 1024 | 2–100000; total waiting requests in one gateway |
| `ADMISSION_WAIT_MS` | 2000 | 1–10000 |
| `ADMISSION_EXECUTION_MS` | 10000 | 100–60000 |

With the defaults, up to 768 waiting positions belong to general traffic and 256 to
control traffic; the total cap also applies. Each class still has its per-organization
queue cap. Waiting requests hold neither a runtime connection nor a shared permit. On a
local grant, the gateway asks PostgreSQL for an organization permit; a shared denial
immediately frees the local slot. The queue does not indefinitely retry a shared denial.

Each gateway can open **44 database connections**: 32 runtime, 4 administrative/audit,
4 authentication and 4 admission. Authentication and admission have separate bounded
waiting gates (1024 and 64 calls) and two-second database deadlines. Plan PostgreSQL's
connection budget for every replica, dispatchers and operator connections. Three gateways
alone can consume 132 connections. Raising HTTP replicas without sizing the database does
not create more database capacity.

MCP must read its envelope before selecting a class. Its separate local ingress gate
allows 64 simultaneous body reads, with at most 4 per agent (or the agent's smaller
positive concurrency cap), and bounds each body to 1 MiB. A full ingress gate returns
`429 mcp_ingress_busy` before class admission.

## Deadlines, crashes and retries

A permit is a short-lived capacity reservation, not a task claim or durable execution
receipt. Its lifetime is the configured execution budget plus a fixed 10-second safety
margin. The gateway conservatively subtracts acquisition latency from the remaining lease
and imposes a monotonic execution context. Server `statement_timeout` is clamped to the
remaining execution deadline even when the agent's own timeout is zero. A longer
per-agent timeout cannot override this ceiling. Long model or tool calls should happen
outside the SQL request, using [task leases](tasks.md).

After execution, a confirmed clean, idle runtime session allows independent, bounded
permit release. A poisoned connection, failed reset or non-idle transaction leaves the
shared permit reserved until expiry, even though the local slot is freed: disconnecting
a client is not proof that PostgreSQL has stopped its work. This path increments
`retained_uncertain_permits`. A failed release also leaves expiry as the fallback and
increments `release_failures`.

If an acquisition reply is lost, the gateway has not executed request work. It does not
issue another acquisition under a fresh UUID; it returns `admission_unavailable` and
attempts to release the original UUID. If cleanup fails or a gateway dies, capacity
remains reserved until expiry; restarting does not reclaim it early. Expired permits no
longer count against concurrency. A bounded background sweep runs every five seconds,
and acquisitions also perform bounded cleanup; both retain expired rows for another
120 seconds. These lifetime bounds assume a healthy database clock and functioning
statement timeouts; clock jumps and database failover need separate acceptance.

The internal SQL acquire function can replay an identical live request without extending
its lease or consuming another token. Changed parameters or an expired retained UUID are
conflicts. Explicit release deletes the row; expiry cleanup eventually does too. Callers
must use fresh UUIDs for new acquisitions and must never retry after their acquisition
deadline. Admission provides no durable replay guarantee after deletion. Task completion
receipts and outbox idempotency keys have separate, durable contracts.

| HTTP | Code | Meaning |
|---|---|---|
| 429 | `org_rate_limited` | Shared class token bucket lacks a token |
| 429 | `org_concurrency_limited` | Shared class permits are at the organization ceiling |
| 429 | `local_queue_full`, `local_queue_timeout` | This gateway's bounded queue cannot admit the request |
| 429 | `auth_busy`, `admission_busy`, `mcp_ingress_busy` | A bounded authentication, quota-service or MCP body-read gate is full |
| 429 | `rate_limited`, `concurrency_limited` | The agent's process-local class limit is reached |
| 503 | `admission_unavailable` | Organization admission could not be established; request work was not executed |

429 responses include `Retry-After` in whole seconds and a `retry_after_ms` field. Back off
with jitter and cap concurrent requests. A retry does not reserve capacity and may still
be refused. For concurrency denial the advisory delay follows the earliest permit expiry;
a normal release may free capacity sooner. MCP returns structured admission errors with
the same application codes. Once execution begins, transport loss can still leave an
unknown write outcome: use the task, event or memory retry contract instead of replaying
arbitrary SQL blindly.

## Operations and scope

`GET /healthz` includes `admission`: local active/queued counts by class, configured
capacities and timing, admitted counts, rejections by reason, wait/execution counts and
total milliseconds, `release_failures` and `retained_uncertain_permits`. The latter two
are cumulative counts, not current active-permit counts. No agent-token labels are exposed.
`Server-Timing: admission;dur=…` and `X-SwarmPG-Admission-Wait-Ms` report admission time on
requests that reach the shared admission path. Use organization status for shared counts;
a single gateway's health counts are not fleet totals.

`GET /readyz` checks that the controller exists and its database pool can query PostgreSQL,
then verifies the required admission tables and function signatures in the catalog. It
rejects an older schema with missing objects. It does not execute a grant or verify every
function body, permission, column definition or runtime dependency.
Monitor 503s, retained uncertain permits and release failures alongside queue delay and
completed-task latency. A
quota outage fails closed even if cached authentication and ordinary queries could work.

Admission tables and functions are admin-only, revoked from `PUBLIC` and `agent_runtime`.
Agents cannot change policies, mint permits or release another gateway's reservation.
The trusted gateway and schema owner remain inside the security boundary. Direct SQL
sessions bypass HTTP admission even on the kernel; a signed principal does not constitute
an organization permit. Keep raw access restricted or supply a separately enforced
connection/resource policy. This feature adds no guarantee for PostgreSQL failover,
clock jumps, CPU/IO fairness, backup/PITR, storage quotas or arbitrary external effects.

## Reproducible validation

The [owned-service runner](../admission/README.md) starts one temporary database and three
real gateway processes.
It provisions at least 2,000 agent identities across three organizations, drives sustained
noisy traffic alongside useful leased tasks, and retains a JSON report and service logs.
Deterministic probes cover shared ceilings, policy changes, class isolation, quota failure
and gateway crashes. The report records actual worker participation, successful task
completion, per-tenant latency, rejection reasons, sampled shared permits and persisted
result checks; registration count alone is not a scale result.

<!-- doctest: skip -->
```bash
ADMISSION_DB_MODE=docker ADMISSION_WORKERS=2000 ADMISSION_DURATION=300s make admission-test
KERNEL=1 PG_BIN_DIR=/path/to/validated/kernel/bin ADMISSION_WORKERS=2000 ADMISSION_DURATION=300s make admission-test
```

The wrapper only creates and disrupts its own services. CI defines a 120-second run on
each engine in addition to existing stock/kernel suites. Passing reports support only the
recorded duration, hardware, policies, backpressure and failure scenarios; they do not
establish production capacity or a throughput advantage over stock PostgreSQL.
