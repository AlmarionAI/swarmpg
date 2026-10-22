# Durable task coordination

SwarmPG stores a task's claim, lease, fencing token, result and transition history in
PostgreSQL. The contract is: **complete this result only while I still own the current
lease and every declared memory input is unchanged**. Stock PostgreSQL through the
trusted gateway and the signed kernel expose the same contract.

## Authority and state

Tasks are shared within an organization. Any enabled writable agent in that organization
may enqueue or claim from a queue; all its agents may read tasks and history. Only the
current owner with the current fence can renew, release or complete an unexpired claim.
Only the creator can cancel a task. An administrator can recover a nonterminal task with
an explicit reason. Agents cannot directly insert, update or delete task/history tables
or invoke the administrative recovery function.

Mutations recheck the live agent registry after blocking task/input locks. A shared
registry lock then serializes the transition against administrative revocation: a
disable/read-only update that commits first prevents the transition; an already
admitted transition finishes before the update. Token expiry is checked again after
the waits. Read-only agents can still inspect tasks and history.

```text
ready ──claim──> claimed ──complete──> completed
  ▲                │
  └────release─────┘
claimed ──expired + new claim──> claimed with a larger fence
ready / claimed ──creator cancel──> cancelled
ready / claimed ──admin recover──> ready with a larger fence
```

Completed results are immutable. Dependencies name existing tasks in the same
organization, are fixed at enqueue time, and must all be completed before claim. This
construction prevents cycles. A cancelled dependency leaves its dependents blocked;
the creator must explicitly cancel/replan them. There is no automatic cascade.

## Leases, fences and retries

Lease duration is 100–300,000 milliseconds, default 30,000. Expiration uses database
`clock_timestamp()`, including a check after completion acquires its input locks.
Workers never hold transactions open during model/tool execution. Renew before expiry;
renewal cannot revive an expired lease and never shortens the existing expiry.
Use periodic renewal while healthy work or database reads are in flight. Back off with
jitter when a queue is empty: every new claim request creates a durable receipt, so
tight polling across many idle workers can consume capacity needed for useful work.

Every new claim advances a per-task signed 64-bit fence. Cancellation and administrative
recovery advance it too. REST/MCP encode fences as **decimal strings**: preserve them as
strings instead of rounding through a JavaScript number. Renew keeps the current fence;
release ends ownership immediately, and the next claim receives a larger fence.

| Operation | Retry contract |
|---|---|
| Enqueue | `(org, queue, key)` identifies the request. Same creator, payload, dependency set and input set returns the task's current snapshot; different content conflicts. |
| Claim | Required `request_id` UUID is scoped to the agent/org. Identical retries return the original snapshot, including an empty claim. Changed queue/duration conflicts. Use a new ID to poll again. |
| Complete | Same successful owner, fence and JSON result returns the completed receipt, even after its old lease expires or inputs later change. Different result conflicts. |
| Renew/release | Resolve an uncertain response with `task_get`. Renewal can extend again; release retry may report a stale claim. |

A replayed claim snapshot can already be expired or superseded. It is a receipt, not
proof of current ownership. Check the lease and let every state change enforce the fence.
Claim receipts and history are durable and currently have no automatic retention policy.
Deleting receipts manually discards the corresponding retry guarantee.

## Input snapshots and atomic completion

At enqueue, declare up to 64 memory references as `{agent_id, key, version}`. Read a
specific producer with `GET /v1/memory/{key}?from=<agent_id>`; do not infer provenance from
a default shared-key lookup. Dependencies and declared inputs are returned in each task.

Completion locks the task and all declared live memory rows in a deterministic order,
checks visibility and versions, and rechecks the lease. Result, completed state and
attributed history commit together. A missing/deleted input, revoked share or changed
version rejects the entire completion. Input declarations cannot be removed or changed
by a worker; cancel and enqueue a new task key with fresh versions when replanning.

Private inputs owned by the creator can be declared, but another worker cannot complete
against them until they are shared. Only declared inputs are checked: clients must
declare every memory value their result relies on. Completed dependency results are
already immutable. Other reads, model outputs and external effects are outside this
validation boundary.

This provides one committed database result per task, not exactly-once execution of a
worker or an external side effect. External systems need their own idempotency/fencing
support. There is no external outbox in this milestone.

## REST and MCP

All task success responses use HTTP 200. All routes below require an agent bearer token
except the administrator recovery route, which requires `X-Admin-Key`.

| REST | Request | MCP tool |
|---|---|---|
| `POST /v1/tasks` | `queue`, `key`, `payload`, optional `dependencies`, `inputs` | `task_enqueue` |
| `GET /v1/tasks/{id}` | — | `task_get` |
| `POST /v1/tasks/claim` | `queue`, `request_id`, optional `lease_ms` | `task_claim` |
| `POST /v1/tasks/{id}/renew` | `fence`, optional `lease_ms` | `task_renew` |
| `POST /v1/tasks/{id}/release` | `fence` | `task_release` |
| `POST /v1/tasks/{id}/complete` | `fence`, `result` | `task_complete` |
| `POST /v1/tasks/{id}/cancel` | `{}` | `task_cancel` |
| `GET /v1/tasks/{id}/history?limit=50` | Limit 1–1,000, newest first | `task_history` |
| `POST /v1/admin/tasks/{id}/recover` | `reason` | Administrator only; no MCP tool |

MCP operations taking a task ID use the `id` argument. Enqueue, claim and complete return
`{task, replayed}`; an empty claim has `task: null`. Other operations return the task,
except history, which returns `{events}`. Every task includes producer input references,
owner, lease, fence, attempt count and per-task revision. History attributes transitions
to the agent; administrative recovery records a null agent and the supplied reason.

Queue/key limits are 128/256 characters; input keys are at most 256 characters. At most
64 dependencies and 64 distinct producer/key inputs are accepted. Payload/result must be
explicit JSON values (including JSON null), at most 64 KiB in database JSONB text form.
Task responses must fit the agent's result budget; an oversized mutation response returns
413 and rolls back rather than committing a truncated receipt. Kernel write accounting
includes task, history and claim-receipt writes. A successful claim normally charges four
attempted rows, enqueue and completion two; give task workers sufficient row budgets.

| HTTP / code | Meaning |
|---|---|
| 400 `bad_request` | Invalid argument, fence format or limit |
| 403 `42501` | Agent is not authorized for this operation |
| 404 `not_found` | Task/dependency/input is missing or invisible |
| 409 `task_conflict` | Reused key/request with different content, or terminal state conflict |
| 409 `stale_claim` | Wrong owner/fence or no current claimed state |
| 409 `lease_expired` | Claim expired; it cannot be revived |
| 409 `inputs_changed` | A declared input changed or became inaccessible |
| 413 `result_limit_exceeded` | Response cannot fit; transaction rolled back |

Direct SQL uses corresponding `swarmpg.task_*` functions. Argument order and defaults are
defined in [schema.sql](../sql/schema.sql); the task-specific SQLSTATEs are `PT400`,
`PT404`, and `PT409`–`PT413` respectively. SQL errors roll back the statement. Raw SQL
identity on stock PostgreSQL still depends on the trusted gateway; the signed kernel
enforces the sealed principal for raw connections too.

## Runnable example

With the development gateway already listening on port 8080, create a producer and a
worker in one organization:

<!-- doctest: expect task-input -->
```bash
TASK_GW=http://localhost:8080
TASK_ORG=$(curl -fsS -H 'X-Admin-Key: dev-admin-key' -d '{"name":"task-example"}' "$TASK_GW/v1/orgs" | jq -r .id)
TASK_PRODUCER=$(curl -fsS -H 'X-Admin-Key: dev-admin-key' -d "{\"org_id\":\"$TASK_ORG\",\"name\":\"producer\"}" "$TASK_GW/v1/agents")
TASK_PRODUCER_ID=$(echo "$TASK_PRODUCER" | jq -r .id)
TASK_AUTH="Authorization: Bearer $(echo "$TASK_PRODUCER" | jq -r .token)"
TASK_WORKER=$(curl -fsS -H 'X-Admin-Key: dev-admin-key' -d "{\"org_id\":\"$TASK_ORG\",\"name\":\"worker\"}" "$TASK_GW/v1/agents")
TASK_WORKER_AUTH="Authorization: Bearer $(echo "$TASK_WORKER" | jq -r .token)"
curl -fsS -X PUT -H "$TASK_AUTH" -d '{"value":{"n":21},"shared":true}' "$TASK_GW/v1/memory/task-input" | jq -c .
```

Enqueue, read the selected producer, claim and complete the task:

<!-- doctest: expect completed -->
```bash
TASK_BODY=$(jq -n --arg producer "$TASK_PRODUCER_ID" '{queue:"math",key:"double-21",payload:{operation:"double"},inputs:[{agent_id:$producer,key:"task-input",version:1}]}')
TASK_ID=$(curl -fsS -H "$TASK_AUTH" -d "$TASK_BODY" "$TASK_GW/v1/tasks" | jq -r .task.id)
curl -fsS -H "$TASK_WORKER_AUTH" "$TASK_GW/v1/memory/task-input?from=$TASK_PRODUCER_ID" | jq -c '{agent_id,version,value}'
TASK_REQUEST=$(python3 -c 'import uuid; print(uuid.uuid4())')
TASK_CLAIM=$(curl -fsS -H "$TASK_WORKER_AUTH" -d "{\"queue\":\"math\",\"request_id\":\"$TASK_REQUEST\",\"lease_ms\":30000}" "$TASK_GW/v1/tasks/claim")
TASK_FENCE=$(echo "$TASK_CLAIM" | jq -r .task.fence)
TASK_COMPLETION=$(jq -n --arg fence "$TASK_FENCE" '{fence:$fence,result:{answer:42}}')
curl -fsS -H "$TASK_WORKER_AUTH" -d "$TASK_COMPLETION" "$TASK_GW/v1/tasks/$TASK_ID/complete" | jq -c '{state:.task.state,result:.task.result,replayed}'
```

Replay the identical completion and inspect its attributed history:

<!-- doctest: expect true -->
```bash
curl -fsS -H "$TASK_WORKER_AUTH" -d "$TASK_COMPLETION" "$TASK_GW/v1/tasks/$TASK_ID/complete" | jq -c '{replayed,result:.task.result}'
curl -fsS -H "$TASK_AUTH" "$TASK_GW/v1/tasks/$TASK_ID/history" | jq -c '.events | map({event,actor_agent_id,fence,revision})'
```

## Operational limits

Task transition history commits synchronously with the task. The separate SQL-query
audit spool only covers `/v1/query` and MCP `query`; typed task APIs use task history and
do not silently expand the audit scope. Enqueue/claim/completion keys resolve uncertain
transport outcomes after gateway/database reconnects.

Per-agent HTTP rate/concurrency limits do not promise organization-wide fairness or
multi-gateway admission fairness. Monitor queue delay and quiet-tenant latency under the
failure workload. Retention, queue quotas, outbox delivery, backup/PITR acceptance and
broader deployment compatibility still require operational work. A short 1,000-worker
test is evidence for its measured conditions, not a production scale guarantee.
