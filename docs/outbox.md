# Transactional external-tool outbox

Complete a task with an optional `effects` array to commit its result, completion
history and external delivery intents in **one PostgreSQL transaction**. A separate
dispatcher delivers those intents to an operator-configured HTTP receiver. A failed
completion leaves no deliverable intents. The same contract runs on stock PostgreSQL
through the gateway and on the signed kernel.

Delivery is at least once. Every retry carries the same `Idempotency-Key` and body.
The receiver must atomically store that key with its effect and return the original
successful outcome on duplicates. Without that receiver contract, a crash after the
external effect but before acknowledgement can produce duplicate effects. Database
fencing alone cannot prevent an already-sent HTTP request from reaching a receiver.

## Commit intents with the result

`POST /v1/tasks/{id}/complete`, MCP `task_complete`, and the four-argument SQL function
`swarmpg.task_complete(id, fence, result, effects)` accept the same effect set:

```json
{
  "fence": "3",
  "result": {"ticket_prepared": true},
  "effects": [
    {"key": "create-ticket", "destination": "ticketing", "payload": {"title": "Investigate failed build"}}
  ]
}
```

The current task owner and fence must hold an unexpired lease, and declared memory
inputs must still match. Effects are normalized by key. An identical completion retry
returns the original task receipt; a different result or effect set conflicts. Omitting
`effects` means `[]`; explicit `null` is invalid. The existing three-argument SQL function
continues to mean completion without effects. Each task stores its immutable
`completion_effects`; mutable delivery status is read separately.

Each effect requires exactly `key`, `destination`, and `payload`. Key and destination
match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Keys are unique within a task. There are at most
32 effects, each payload is at most 64 KiB and the entire normalized array at most
256 KiB, measured as PostgreSQL JSONB text. Payload may be any JSON value, including
JSON null. The completion receipt must also fit the agent's result budget: an oversized
receipt rolls back the result, history and intents together. Kernel write budgets include
both the outbox message and history row for each effect, in addition to task bookkeeping.

Read delivery status with `GET /v1/tasks/{id}/outbox`, MCP `task_outbox`, or SQL
`swarmpg.task_outbox(id)`. All return `{messages:[...]}` within the current organization
and enforce the complete response budget. Agents can read their organization's messages
and delivery history through SQL, but cannot modify them or invoke dispatcher functions.

The task is completed once its intents are committed. Its dependent tasks may become
eligible before external delivery finishes. Applications that need delivery confirmation
must inspect outbox status before starting that part of their workflow.

## Run a dispatcher

Build the standalone Go executable from the repository root:

<!-- doctest: skip -->
```bash
mkdir -p .bin
(cd outbox && go build -o ../.bin/outbox .)
```

Each process binds one organization and destination name to one receiver URL. Agents
cannot supply URLs, HTTP headers or credentials in the routing configuration. Use an
authenticated receiver that validates the payload and applies the intended business
authorization. The dispatcher is trusted infrastructure: `OUTBOX_ADMIN_KEY` gives it
gateway administrative authority and must never be exposed to agents.

<!-- doctest: skip -->
```bash
export OUTBOX_ADMIN_KEY='your-gateway-admin-key'
export OUTBOX_TARGET_AUTHORIZATION='Bearer your-receiver-credential'
.bin/outbox \
  -gateway https://gateway.example \
  -org-id 00000000-0000-4000-8000-000000000001 \
  -destination ticketing \
  -endpoint https://tools.example/tickets
```

`OUTBOX_TARGET_AUTHORIZATION` is optional and is sent only to the configured receiver.
Neither credential is stored in outbox rows or delivery history. Both gateway and target
URLs require HTTPS, with loopback HTTP permitted for local development. Redirects are
refused. Receiver bodies are read with a size bound and are not logged or stored.

| Flag | Default | Purpose |
|---|---|---|
| `-gateway` | `http://127.0.0.1:8080` | Gateway URL |
| `-org-id` / `-destination` / `-endpoint` | Required | Explicit organization and destination mapping |
| `-worker-id` | Random UUID per process | Dispatcher identity; concurrent processes may use different identities |
| `-concurrency` | `4` | Concurrent delivery loops, maximum 128 |
| `-lease-ms` | `30000` | Database-time lease; 100–300,000 ms |
| `-poll-interval` | `250ms` | Delay after an empty claim |
| `-request-timeout` | `10s` | Gateway and receiver HTTP request deadline |

Run multiple processes for the same organization/destination to share delivery work.
Claims use `FOR UPDATE SKIP LOCKED`. Each claim advances a per-message fence; the
dispatcher checks the lease before sending, renews during the request, cancels local work
on lease loss, and joins its heartbeat before acknowledgement. An uncertain claim reply
is retried with its original request UUID, so it cannot acquire a second message.

## Receiver protocol and retries

The receiver gets a POST with `Content-Type: application/json`, `Idempotency-Key`, and
`X-SwarmPG-Delivery-Fence` headers. The JSON body contains only:

```json
{
  "id": "message UUID",
  "org_id": "organization UUID",
  "task_id": "completed task UUID",
  "key": "create-ticket",
  "destination": "ticketing",
  "payload": {"title": "Investigate failed build"},
  "idempotency_key": "swarmpg:<organization UUID>:<message UUID>"
}
```

The key and body remain identical across retries, process restarts, expired leases,
and administrative requeues. The delivery fence header changes when ownership changes.
Receivers must reject a reused key with a different body, serialize concurrent duplicate
requests, and retain deduplication records for the entire possible retry lifetime. Do not
include credentials in effect payloads: organization members can read them.

HTTP 2xx acknowledges delivery and stores only `{status:<HTTP status>}`. This means the
receiver accepted the request; application-level success depends on its own contract.
Transport failures, interrupted responses, 408, 425, 429 and 5xx are retried with
exponential delay from one second to a 60-second cap. Other statuses become permanent
failures. Automatic delivery stops after ten attempts in a retry cycle; an expired final
lease is also moved to `failed` when a dispatcher next polls. Failed messages remain
inspectable and require an explicit administrator retry.

```text
pending -> delivering -> delivered
               |
               +-- transient failure -> pending (available_at in the future)
               +-- permanent failure / exhausted attempts -> failed
               +-- lease expiry -> new fenced delivery attempt
failed -- administrator retry --> pending (same message and idempotency key)
```

Acknowledgement is atomic with delivery history. Repeating the successful worker/fence
and response replays the receipt, including after its old lease expires. Stale workers
cannot acknowledge or renew another claim. An uncertain negative acknowledgement may
return a stale claim on retry; that means the caller no longer owns the delivery, not
that the external effect succeeded.

## Administrative operations

All routes below require `X-Admin-Key`. There are no administrative MCP tools. Each
operation requires the organization explicitly, even though the credential is privileged.
Fences are positive signed-64-bit decimal **strings**, including values above 2^53.

| REST | Body / query | Result |
|---|---|---|
| `POST /v1/admin/outbox/claim` | `org_id`, `destination`, `worker_id`, `request_id`, optional `lease_ms` | `{message,replayed}`; message can be null |
| `GET /v1/admin/outbox/{id}` | Query `org_id` | Message |
| `POST /v1/admin/outbox/{id}/renew` | `org_id`, `worker_id`, `fence`, `lease_ms` | Message |
| `POST /v1/admin/outbox/{id}/ack` | `org_id`, `worker_id`, `fence`, `response` | `{message,replayed}` |
| `POST /v1/admin/outbox/{id}/nack` | `org_id`, `worker_id`, `fence`, `error`, `retry_ms`, `permanent` | Message |
| `POST /v1/admin/outbox/{id}/retry` | `org_id`, `reason` | Message |

`response` is JSON up to 16 KiB. Error/reason text is 1–1,000 characters, and `retry_ms`
is 0–3,600,000. SQL exposes corresponding owner-only `swarmpg.outbox_*` functions.
Administrative retry accepts only failed messages, increments the fence, clears the
owner, resets `attempts`, and preserves `total_attempts` and the stable key. Delivered
messages are immutable. The [task error codes](tasks.md#rest-and-mcp) also apply here.

## Runnable enqueue example

With the development gateway listening on port 8080, this creates a pending intent.
It does not start a dispatcher or contact an external receiver.

<!-- doctest: expect pending -->
```bash
OUTBOX_GW=http://localhost:8080
OUTBOX_ORG=$(curl -fsS -H 'X-Admin-Key: dev-admin-key' -d '{"name":"outbox-example"}' "$OUTBOX_GW/v1/orgs" | jq -r .id)
OUTBOX_AGENT=$(curl -fsS -H 'X-Admin-Key: dev-admin-key' -d "{\"org_id\":\"$OUTBOX_ORG\",\"name\":\"producer\"}" "$OUTBOX_GW/v1/agents")
OUTBOX_AUTH="Authorization: Bearer $(echo "$OUTBOX_AGENT" | jq -r .token)"
OUTBOX_TASK=$(curl -fsS -H "$OUTBOX_AUTH" -d '{"queue":"tickets","key":"build-42","payload":{"build":42}}' "$OUTBOX_GW/v1/tasks" | jq -r .task.id)
OUTBOX_REQUEST=$(python3 -c 'import uuid; print(uuid.uuid4())')
OUTBOX_FENCE=$(curl -fsS -H "$OUTBOX_AUTH" -d "{\"queue\":\"tickets\",\"request_id\":\"$OUTBOX_REQUEST\"}" "$OUTBOX_GW/v1/tasks/claim" | jq -r .task.fence)
OUTBOX_COMPLETE=$(jq -n --arg fence "$OUTBOX_FENCE" '{fence:$fence,result:{prepared:true},effects:[{key:"ticket",destination:"ticketing",payload:{build:42}}]}')
curl -fsS -H "$OUTBOX_AUTH" -d "$OUTBOX_COMPLETE" "$OUTBOX_GW/v1/tasks/$OUTBOX_TASK/complete" | jq -c '{replayed,state:.task.state}'
curl -fsS -H "$OUTBOX_AUTH" "$OUTBOX_GW/v1/tasks/$OUTBOX_TASK/outbox" | jq -c '.messages | map({id,state,idempotency_key})'
```

## Recovery evidence and operational limits

`scripts/outbox-e2e.sh` creates its own database, gateway, dispatcher processes and local
receiver. The receiver commits its deduplication key and effect to SQLite before replying.
The test kills dispatchers and the receiver after that commit, crashes the gateway and
database, injects lost replies and retryable/permanent errors, runs concurrent dispatchers,
and checks final persisted effects and outbox histories. `report.json` and logs survive
cleanup; CI runs this on stock PostgreSQL and the signed kernel. It reuses the same owned
service lifecycle as the [coordination workload](../coordination/README.md).

Outbox messages, history, claim receipts and receiver deduplication records currently have
no automatic retention. Polling creates durable claim receipts, including empty claims.
Configure polling and concurrency for the workload and monitor pending age, failed rows,
attempts and growing tables. An unconfigured destination stays pending. This milestone
does not establish cross-destination ordering, production throughput, backup/PITR
acceptance or distributed failover guarantees. [Organization admission](admission.md)
limits agent requests across gateways; administrative outbox delivery calls and receiver
capacity require separate operator limits.
