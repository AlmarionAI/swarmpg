# Failover-safe coordination and recovery

Recovery mode pins every gateway to a PostgreSQL system identifier, timeline and an
externally allocated recovery generation. It requires synchronous replication before
acknowledging work. A promoted or restored database remains unavailable to gateways
whose pins no longer match; point-in-time restoration also quarantines work until an
operator reconciles external effects.

This is an opt-in gateway/schema contract on stock PostgreSQL and the signed kernel.
It uses PostgreSQL physical replication, WAL archives and recovery. SwarmPG does not
elect a primary or provide a consensus service. An operator or an established HA
manager must fence the former primary and choose a sufficiently durable promotion
candidate. The development Compose topology does not provide those services.

## Durability and failure policy

| Event | Required behavior |
|---|---|
| Primary process failure | Promote an eligible synchronous standby only after fencing the former primary; preserve acknowledged coordination results and effect intents |
| Replication partition | Refuse new guarded work or withhold a successful response; never silently downgrade to asynchronous acknowledgement |
| Gateway/database connection loss during commit | Treat the outcome as unknown; replay the same durable operation identity after recovery |
| New PostgreSQL timeline | Old gateway pins are refused; explicitly activate the new timeline before routing traffic |
| Point-in-time restore | Allocate a new external generation; invalidate unfinished ownership and quarantine runtime work and dispatch until reconciliation |
| Former primary returns | Keep it fenced; rebuild or rewind it as a replica before allowing any client connectivity |

The eligible-failover durability objective is **zero lost acknowledged coordination
transactions**. This depends on promoting a node that contains those transactions;
arbitrary asynchronous promotion cannot satisfy it. `remote_apply` waits for a selected
synchronous replica to apply a transaction. A disconnected client or canceled wait can
leave a transaction committed locally without a successful client acknowledgement.
Callers must retain request UUIDs and use the task/outbox retry contracts; arbitrary SQL
does not become exactly-once. See PostgreSQL's [synchronous replication
settings](https://www.postgresql.org/docs/17/runtime-config-replication.html) and
[replication behavior](https://www.postgresql.org/docs/17/warm-standby.html).

PITR deliberately rewinds database history. It cannot preserve acknowledgements after
the chosen recovery point, and it cannot undo effects already committed by receivers.
The contract is safe quarantine and explicit reconciliation, not zero data loss across
an intentional rewind.

## Reference deployment

Use one writable primary and at least one physical synchronous standby, on separate
failure domains, with an independent durable base-backup/WAL archive and an external
recovery-generation ledger. For recovery without waiting to provision a replacement
standby, maintain an additional replica and an explicit promotion policy. The selected
candidate must contain every acknowledged commit; being reachable or having the
largest observed replay position alone is not proof of that guarantee.

Before enabling traffic:

1. Provision identical supported PostgreSQL binaries/extensions on replication peers,
   durable storage, time synchronization, and tested backup/WAL retention. The stock CI
   recovery target is PostgreSQL 17. The kernel remains the pinned **19rc1** base plus
   patches; this milestone does not make it a stable upstream release.
2. Use SCRAM authentication and TLS with certificate verification for database and
   replication connections. Restrict `pg_hba.conf` and network rules to gateway,
   replication and operator identities. Keep schema-owner/admin credentials and the
   kernel principal secret out of agent environments. Terminate gateway HTTPS at a
   trusted proxy; use HTTPS and authenticated receivers for dispatch.
3. Configure physical replication and a tested archive/restore mechanism. Typical
   primary settings include `wal_level=replica`, `hot_standby=on`, sufficient
   `max_wal_senders`/`max_replication_slots`, `synchronous_standby_names='FIRST 1
   (swarm_sync)'`, and `synchronous_commit=remote_apply`. Connect the selected standby
   with `application_name=swarm_sync`. Do not configure an automatic asynchronous
   fallback. Size WAL retention and monitor slot/archiver growth while a replica is
   unavailable. See the [PostgreSQL standby setup
   procedure](https://www.postgresql.org/docs/17/warm-standby.html).
4. Apply the schema before serving agents. Allocate generation `1` or another unused
   positive generation in a durable system outside the database recovery domain. Read
   the primary's actual identity and activate recovery mode using the owner operations
   below. Keep the external ledger available even if the entire database archive is
   restored to an older point. When adopting recovery on an existing deployment, stop
   admission, drain active gateway/dispatcher work and task leases, and resolve ambiguous
   pre-bootstrap operations first. Bootstrap advances from legacy generation `0`:
   replaying an old claim receipt afterward is rejected. Start reconciled work with fresh
   request IDs after bringing the standby fully up to date and activating the pins.
5. Configure every gateway with all three pins and a verified TLS primary endpoint.
   Readiness must pass before routing runtime or dispatcher traffic. Keep raw agent SQL
   access restricted: gateway recovery pins do not fence an independently connected
   privileged client or an old primary that remains reachable.
6. Reserve database capacity: each gateway can open 44 connections, before replication,
   backup, maintenance and operator connections. Choose fleet quotas, connection limits
   and failover capacity together; a standby must support the promoted workload.
   Recovery barriers add database writes and synchronous replication latency. Keep
   audit spools on persistent volumes when using strict audit and recover those volumes
   separately from PostgreSQL.

The HA manager must positively fence the former primary, for example through power or
storage fencing with a verified result. Changing DNS or blocking one gateway route is
insufficient if another gateway or privileged client can still write to that server.
SwarmPG's timeline/generation checks supplement this infrastructure requirement; they
cannot resolve a network partition by themselves. See PostgreSQL's [failover
guidance](https://www.postgresql.org/docs/17/warm-standby-failover.html).

## Gateway configuration

| Variable | Meaning |
|---|---|
| `RECOVERY_SYSTEM_ID` | Exact decimal PostgreSQL system identifier returned by `pg_control_system()` |
| `RECOVERY_TIMELINE` | Current writable-primary timeline reported by `recovery_status().current.timeline`, 1–4,294,967,295 |
| `RECOVERY_GENERATION` | Generation recorded by the external allocator and activated in the database, 1–2,147,483,647 |

Set all three or none; partial configuration is rejected. Existing deployments without
an activated recovery control row retain legacy behavior. Recovery mode is not enabled
merely by installing the schema. A guarded deployment must pin every gateway and route
all supported runtime/dispatch work through those gateways.

Guarded transactions check the live identity and active control state, enforce
`remote_apply`, and write a durability barrier even when replaying an existing receipt.
That barrier prevents a locally visible, previously unacknowledged commit from being
reported as durable before replication catches up. Losing synchronous replication or
canceling its commit wait must not produce a successful HTTP response. Re-establish
replication and retry the same operation identity to learn its final outcome.

| HTTP / SQLSTATE | Meaning |
|---|---|
| 503 `recovery_unavailable` / `SR503` | Pins, activated state or synchronous durability do not permit work |
| 503 `commit_unconfirmed` | Commit may have happened, but the gateway could not confirm its durability |
| 409 `recovery_conflict` / `SR409` | Invalid recovery transition, unresolved reconciliation or exhausted/inconsistent fence range |
| 409 `stale_claim` / `PT410` | Claim/fence is no longer current, including an old-generation claim receipt |

Treat recovery unavailability as an operational outage and back off with a bounded retry
budget. Only the operator changes pins or resumes a quarantined history.

## Owner operations

These are SQL owner operations, not agent or MCP APIs. Use a secure operator connection
to the intended primary. Activation, resolution and resume require a live synchronous
replica and a transaction using `synchronous_commit=remote_apply`. Their reasons and
resolution evidence are recorded for inspection. Reason/evidence text is required and
limited to 4,000 characters; put an evidence reference in it rather than credentials or
large receiver payloads.

PostgreSQL can emit a warning and return `COMMIT` after a synchronous replication
wait is canceled. A successful `psql` exit alone therefore does not confirm durability.
Do not cancel these owner commits; if interrupted or warned, keep traffic fenced and
verify the resulting control state and its WAL replay on the eligible synchronous
replica before continuing. The gateway treats every warning during its guarded COMMIT
as `commit_unconfirmed` and retires that connection.

```sql
SELECT system_identifier::text FROM pg_control_system();
SELECT swarmpg.recovery_status();
```

Use the status result's `current.timeline`, which is taken from the current WAL timeline.
A checkpoint's timeline can still describe older history immediately after promotion.

Function signatures:

```sql
swarmpg.recovery_activate(system_id text, timeline bigint, generation bigint,
                         mode text, reason text) RETURNS jsonb
swarmpg.recovery_resolve_outbox(org_id uuid, message_id uuid,
                               action text, evidence text) RETURNS jsonb
swarmpg.recovery_resume(system_id text, timeline bigint, generation bigint,
                       reason text) RETURNS jsonb
swarmpg.recovery_status() RETURNS jsonb
```

The `mode` is `bootstrap`, `failover` or `restore`. Bootstrap activates a previously
unmanaged cluster. Failover changes timeline while retaining the current generation.
Restore requires a strictly higher external generation and enters quarantine. Do not
derive the next generation solely from a restored database: its history may have lost
previous allocations. Never reuse a generation for a different restored history.

An activation transaction has this form; replace every placeholder with verified values:

```sql
BEGIN;
SET LOCAL synchronous_commit = 'remote_apply';
SELECT swarmpg.recovery_activate(
  'ACTUAL_SYSTEM_IDENTIFIER', ACTUAL_TIMELINE, EXTERNAL_GENERATION,
  'bootstrap', 'Deployment record identifying the synchronous replica and fencing policy');
COMMIT;
```

## Failover procedure

1. Stop admitting traffic and pause dispatch while determining the failure. Fence the
   former primary and record proof of fencing. If fencing or candidate durability
   cannot be established, remain unavailable; do not force promotion under this
   contract.
2. Select a replica containing all acknowledged commits, promote it, and verify its
   system identifier and new timeline. The external HA policy is responsible for this
   selection; SwarmPG cannot prove the state of an unreachable former primary.
3. Establish a healthy synchronous replica for the promoted primary. Keep the former
   primary fenced while rebuilding/rewinding it. Recovery activation refuses operation
   without the required synchronous durability conditions.
4. Call `recovery_activate` with mode `failover`, the new timeline, the unchanged
   generation, and a reason referencing the fencing/durability evidence. Wait for the
   activation transaction's successful synchronous commit.
5. Update every gateway's pins and primary endpoint, restart/drain old gateways, and
   check readiness before resuming dispatch and agent traffic. Retry ambiguous task
   claims/completions and outbox acknowledgements using their original identities.
6. Compare acknowledged operation records with recovered tasks, completions and effect
   intents. Measure outage duration and recovery backlog. Preserve operator and HA
   logs with the recovery history.

Claims may expire during an outage. Failover does not promise to keep every worker's
lease valid; it preserves committed ownership history and rejects stale attempts after
reclaim. External effects remain dependent on receiver idempotency.

## PITR and external-effect reconciliation

Restore into an isolated environment using a verified base backup and WAL archive. Keep
all previous primaries fenced and stop every old gateway and dispatcher. Rotate the
gateway `ADMIN_KEY` before starting replacements, then supply it only to newly started
dispatchers after reconciliation. Administrative credentials live outside PostgreSQL
and are not revoked by restore activation: an old dispatcher's request UUID may have
lost its receipt in PITR, so retaining its credential could let it claim different work.
Record the recovery target, archive lineage and restored identity. PostgreSQL's [PITR
procedure](https://www.postgresql.org/docs/17/continuous-archiving.html) describes the
underlying restoration mechanics.

Allocate a new generation in the external ledger, establish a synchronous standby, then
activate with mode `restore`. Activation places unfinished task and nondelivered outbox
fences in the new generation's range and quarantines runtime operations, admission and
outbox dispatch. Old-generation claim replay is rejected, rather than returning an
apparently current owner from rewound history. Reconfigure gateways to the new pins;
quarantine still prevents them from serving work.

Restore activation also invalidates every existing agent token and disables all agents.
Provision fresh credentials and selectively re-enable workers only after reconciling
their workflows. Perform this maintenance through the schema owner while quarantined;
normal gateway administration is guarded too. Restart every gateway with the new pins to clear cached credentials
before resume. This prevents a worker from using an old request UUID whose receipt was
removed by PITR to accidentally claim different work. A rejected old credential is not
a signal to regenerate a request UUID and blindly continue the old workflow.

For every retained nondelivered message, compare its stable key and payload with the
receiver's durable deduplication/effect ledger and call `recovery_resolve_outbox`:

| Action | Required evidence |
|---|---|
| `delivered` | Receiver ledger proves the identical intent already took effect |
| `retry` | Receiver contract and evidence establish that retrying this same stable key is safe |
| `failed` | Operator has decided to stop delivery and records the disposition |

Record an incident/evidence reference and the outcome in the required evidence text.
A database row's `pending` status is not proof that the receiver did nothing. An in-flight
request might have arrived just before the failure. Stopping a dispatcher cannot recall
requests already sent to the network or accepted by a receiver. Before attesting that
reconciliation is complete, establish terminal outcomes for those requests, or use a
receiver-supported fence checked when the effect commits to prevent late effects from
the old dispatchers. A timeout or an empty receiver lookup alone is insufficient. If the
receiver cannot establish that an unresolved request will not take effect later, keep
the restored deployment quarantined; do not recreate an absent intent with a new key.

Reconciliation must also cover **effects absent from the restored database**. An effect
created after the recovery point may already exist at its receiver even though PITR
removed the intent and its original idempotency key. Re-enqueuing the task can create a
new key and repeat the effect. Compare independent business/effect records for the full
rewound interval, repair or suppress recreation, and keep receiver deduplication records.
SwarmPG cannot enumerate or automatically reverse those missing intents.

Once every retained nondelivered message has an explicit disposition and the independent
reconciliation is complete, call `recovery_resume` in a `remote_apply` transaction with
the current pins and a reason attesting the reconciliation, including effects absent
from restored history. Resume refuses unresolved retained messages. The database can
verify retained-row dispositions; the external reconciliation reason is an operator
attestation, not an automated proof of a receiver's complete business history.

Only then restore gateway routing and dispatch. Previously completed rows may remain
completed; do not replay arbitrary business workflows solely because their history was
rewound. Keep the generation allocation and reconciliation record outside the restored
database as well.

## Reproducible validation

The owned-service runner creates isolated PostgreSQL primary/standby clusters, TCP fault
proxies, a base backup/WAL archive, gateway/dispatcher processes and an independent
durable receiver ledger. It never attaches to an existing deployment. Run it as a
non-root user with Go, Python 3 and a complete matching PostgreSQL binary installation:

<!-- doctest: skip -->
```bash
PG_BIN_DIR=/path/to/postgresql17/bin RECOVERY_DURATION=300 make recovery-test
KERNEL=1 PG_BIN_DIR="$PWD/.pginstall/bin" RECOVERY_DURATION=300 make recovery-test
```

`RECOVERY_DURATION` is seconds, not a Go duration string. `RECOVERY_WORKERS` defaults to
32 concurrent task/lease/effect loops with distinct agent identities, configurable from
1 to 256. `RECOVERY_REPORT_DIR` selects a new or empty evidence
directory; omission creates a temporary one. The equivalent direct CLI accepts
`--duration 300 --directory /path/to/empty/evidence`. CI runs 120 seconds per engine;
local sustained acceptance uses 300 seconds per engine. Setup, isolated faults, backup,
restoration and verification add time beyond the sustained workload interval.

The sustained interval includes the replication partition and primary failover while
the worker loops continue attempting work. Before a run, the suite fixes a **60-second
failover recovery ceiling**, measured from primary failure to the first successful real
task completion after recovery, including replacement-standby setup and activation. It
requires zero lost acknowledged completions on eligible failover. This is a local test
threshold, not a production or automatic-failover service-level promise. PITR is assessed
separately: post-target acknowledgements are intentionally rewound and must be detected
and reconciled against the independent receiver ledger.

The suite exercises primary failure, replication disconnection, fail-closed operation,
promoted timeline activation, stale ownership, receiver commit with a lost reply, and
real archive-based PITR with quarantine/reconciliation. Retained `report.json` and logs
identify the source revision/fingerprint, binary version, settings, faults, elapsed
times and correctness results. A passing report is evidence for its recorded workload
and fault model, not a fleet-size or cross-region availability guarantee.

The test uses best-effort audit and does not establish audit-spool crash durability.

Run the existing full stock/kernel suites and upstream kernel regression suite too.
Recovery tests do not replace organization-fairness, isolation, task-failure or outbox
crash coverage. They use local disposable services and development authentication;
production TLS, HA-manager integration, remote storage failure, multi-region latency,
clock jumps, long-term retention and rolling upgrades need separate acceptance.
