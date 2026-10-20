# Getting started

From nothing to an agent reading and writing its own data over REST, MCP and SQL. Every
command here runs as written against a local gateway; CI runs this page on every change.

## 1. Start SwarmPG

With Docker (the SwarmPG kernel and the gateway on `localhost:8080`; the first build
compiles Postgres, about 10 minutes):

<!-- doctest: skip -->
```bash
docker compose up --build
```

Or from source, with Postgres 14+ on `PATH` and Go 1.23+:

<!-- doctest: skip -->
```bash
make pg-start && make gateway
```

The examples use `curl` and `jq`.

## 2. Create an org and an agent

Admin calls carry the `X-Admin-Key` header. The development default is `dev-admin-key`;
set `ADMIN_KEY` on the gateway for anything shared.

<!-- doctest: expect "token" -->
```bash
GW=http://localhost:8080
ADMIN='X-Admin-Key: dev-admin-key'
ORG=$(curl -s -H "$ADMIN" -d '{"name":"acme"}' $GW/v1/orgs | jq -r .id)
AGENT=$(curl -s -H "$ADMIN" -d "{\"org_id\":\"$ORG\",\"name\":\"researcher\",\"max_rows\":100}" $GW/v1/agents)
TOKEN=$(echo "$AGENT" | jq -r .token)
AUTH="Authorization: Bearer $TOKEN"
echo "$AGENT" | jq .
```

The token is shown once and stored only as a hash. An agent's limits are set when it is
created:

| Field | Default | Limits |
|---|---|---|
| `rps_limit` | 20 | requests per second (0: none) |
| `max_concurrency` | 4 | requests in flight at once |
| `max_rows` | 1000 | rows returned per query; on the kernel also rows one statement may modify |
| `max_result_bytes` | 4 MiB | bytes returned per query |
| `max_cost` | 1e6 | planner cost a query may have before it runs (kernel) |
| `statement_timeout_ms` | 5000 | time per statement |
| `read_only` | false | refuse writes |
| `ttl_seconds` | none | token lifetime |

`POST /v1/agents/{id}/disable` revokes an agent within 5 seconds.

## 3. Query over REST

Ask for the schema, written for a model to read:

<!-- doctest: expect swarmpg.memory -->
```bash
curl -s -H "$AUTH" $GW/v1/schema
```

Run one statement per request, with values passed as parameters:

<!-- doctest: expect pricing -->
```bash
curl -s -H "$AUTH" -d '{"sql":"INSERT INTO swarmpg.documents(agent_id, org_id, collection, body) VALUES (swarmpg.current_agent(), swarmpg.current_org(), $1, $2)", "params":["notes", {"topic":"pricing"}]}' $GW/v1/query | jq -c .
curl -s -H "$AUTH" -d '{"sql":"SELECT body FROM swarmpg.documents WHERE collection = $1", "params":["notes"]}' $GW/v1/query | jq -c .rows
```

A failed statement returns what a model needs to fix it: the SQLSTATE, message, hint and
position.

<!-- doctest: expect 42601 -->
```bash
curl -s -H "$AUTH" -d '{"sql":"SELEC body FROM swarmpg.documents"}' $GW/v1/query | jq -c .error
```

## 4. Versioned memory

Every write to a memory key gets a version. Pass `expected_version` to write only if
nothing changed since you read: `0` creates the key only if it does not exist yet.

<!-- doctest: expect version_conflict -->
```bash
curl -s -X PUT -H "$AUTH" -d '{"value":{"step":1},"expected_version":0}' $GW/v1/memory/plan | jq -c .
curl -s -X PUT -H "$AUTH" -d '{"value":{"step":2},"expected_version":1}' $GW/v1/memory/plan | jq -c .
curl -s -X PUT -H "$AUTH" -d '{"value":{"step":9},"expected_version":1}' $GW/v1/memory/plan | jq -c .error
```

The last write was based on version 1, so it changed nothing and returned the current
version. Read the key again, then retry.

Read the past, and undo:

<!-- doctest: expect restored_version -->
```bash
curl -s -H "$AUTH" "$GW/v1/memory/plan?version=1" | jq -c .value
curl -s -H "$AUTH" $GW/v1/memory/plan/history | jq -c '[.versions[] | {version, value}]'
curl -s -X POST -H "$AUTH" $GW/v1/memory/plan/undo | jq -c .
```

Undo writes the previous value as a new version, so it can be undone too, and a deleted
key (`DELETE /v1/memory/{key}`) can be restored the same way. `?as_of=<RFC 3339 time>`
reads a key as it was at that time. A key written with `"shared": true` can be read, not
written, by the other agents in your org.

## 5. The event journal

An append-only, hash-chained log of what the agent did. An `Idempotency-Key` makes a retry
after a timeout safe: the second call returns the first result instead of a new event.

<!-- doctest: expect hash -->
```bash
curl -s -H "$AUTH" -H 'Idempotency-Key: step-1' -d '{"kind":"tool_call","payload":{"tool":"search"}}' $GW/v1/events | jq -c .
curl -s -H "$AUTH" -H 'Idempotency-Key: step-1' -d '{"kind":"tool_call","payload":{"tool":"search"}}' $GW/v1/events | jq -c .
```

## 6. Connect over MCP

Most MCP clients take a URL and headers:

```json
{
  "mcpServers": {
    "swarmpg": {
      "url": "http://localhost:8080/mcp",
      "headers": { "Authorization": "Bearer <agent token>" }
    }
  }
}
```

The agent gets eight tools: `schema`, `query`, `memory_get`, `memory_put`, `memory_delete`,
`memory_history`, `memory_undo` and `events_append`, each under the same identity and
limits as the REST API. To see the protocol itself, here are the same calls with `curl`, as
a 2026-07-28 client makes them:

<!-- doctest: expect memory_history -->
```bash
META='"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}'
mcp() {
  local headers=(-H "$AUTH" -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H "Mcp-Method: $1")
  if [ -n "${3:-}" ]; then headers+=(-H "Mcp-Name: $3"); fi
  curl -s "$GW/mcp" "${headers[@]}" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}"
}
mcp tools/list "{$META}" | jq -c '[.result.tools[].name]'
```

A tool call returns `structuredContent`, plus the same JSON as text for clients that only
read text:

<!-- doctest: expect step -->
```bash
mcp tools/call "{\"name\":\"memory_get\",\"arguments\":{\"key\":\"plan\"},$META}" memory_get | jq -c .result.structuredContent
```

A failed call is a tool result with `isError: true` and the same error body as the REST API,
so the model sees it and can repair the call:

<!-- doctest: expect 42P01 -->
```bash
mcp tools/call "{\"name\":\"query\",\"arguments\":{\"sql\":\"SELECT * FROM swarmpg.nope\"},$META}" query | jq -c '.result | {isError, code: .structuredContent.error.code}'
```

Older clients (protocol 2025-11-25, 2025-06-18 or 2025-03-26) start with `initialize` and get
the same tools. A browser page can only reach the endpoint from an origin listed in
`MCP_ALLOWED_ORIGINS`.

## 7. The same operations in SQL

Memory has SQL functions, so an agent that only has the `query` tool can still use
compare-and-set, history and undo:

<!-- doctest: expect fast -->
```bash
q() { curl -s -H "$AUTH" -d "$(jq -n --arg sql "$1" '{sql: $sql}')" $GW/v1/query; }
q "SELECT swarmpg.memory_put('config', '{\"mode\": \"fast\"}', false, 0) AS version" | jq -c .rows
q "SELECT version, value FROM swarmpg.memory_history('config')" | jq -c .rows
```

| Function | Does |
|---|---|
| `memory_put(key, value, shared, expected_version)` | write, optionally compare-and-set; returns the version |
| `memory_delete(key, expected_version)` | delete; returns the delete's version |
| `memory_history(key, limit)` | versions, newest first |
| `memory_at_version(key, version)`, `memory_as_of(key, time)` | the key as it was |
| `memory_undo(key, to_version, expected_version)` | restore the previous or a given version |

All live in the `swarmpg` schema and see only the calling agent's keys.

## 8. What an agent cannot do

Identity, other agents' rows and the control plane are out of reach, whatever SQL the agent
writes:

<!-- doctest: expect not permitted -->
```bash
q "SET app.agent_id = '00000000-0000-0000-0000-000000000000'" | jq -c .error.message
q "SELECT token_hash FROM swarmpg.agents" | jq -c .error.code
q "SELECT count(*) FROM swarmpg.memory WHERE agent_id <> swarmpg.current_agent()" | jq -c .rows
```

[THREAT-MODEL.md](THREAT-MODEL.md) lists every attack the tests try, and what is not
covered yet.

## 9. Errors and retries

| HTTP | `code` | Meaning | Do |
|---|---|---|---|
| 400 | SQLSTATE (e.g. `42601`) or `bad_request` | the statement or request is wrong | fix it using `message`, `hint`, `position` |
| 401 | `unauthorized` | missing or unknown token | — |
| 403 | `42501`, `forbidden` | not allowed, agent disabled, or token expired | — |
| 404 | `not_found` | no such key or version | — |
| 409 | `version_conflict` | the key changed since you read it; `current_version` says to what | read again, then retry |
| 429 | `rate_limited`, `concurrency_limited` | over the rate or concurrency limit | wait `Retry-After` seconds |
| 400 | `53400` | over the cost or row budget (kernel) | narrow the query or the write |
| 400/504 | `57014`, `statement_timeout` | took longer than `statement_timeout_ms` | retry or narrow; `retryable` is true |

`retryable: true` marks errors where sending the same request again can succeed.

## 10. Run on the SwarmPG kernel

The kernel makes the agent's identity and budgets hold even for SQL that never went
through the gateway. Build it once (about 10 minutes), then run the same stack on it:

<!-- doctest: skip -->
```bash
make kernel
make pg-start KERNEL=1 && make gateway KERNEL=1
```

`KERNEL=1` runs the cluster with `agent_principal_secret` set to `PRINCIPAL_SECRET`
(`dev-principal-secret` by default) and gives the gateway the same secret, so every
identity it seals is signed. Budgets now hold in the executor: a runaway query is refused
before it runs.

<!-- doctest: skip -->
```bash
q "SELECT count(*) FROM generate_series(1, 100000) a, generate_series(1, 100000) b" | jq -c .error
```

## 11. Before you deploy

- Set `ADMIN_KEY`, and on the kernel `PRINCIPAL_SECRET` and the server's matching
  `agent_principal_secret`, to long random values.
- Give `agent_runtime` and the admin connection real authentication; the quickstarts use
  `trust`. Point the gateway at them with `DATABASE_URL` and `RUNTIME_DATABASE_URL`.
- Terminate TLS in front of the gateway; it serves plain HTTP.
- Use the kernel wherever an agent could reach Postgres without the gateway.
- Monitor with a role in `pg_monitor`: statistics views are closed to everyone else.
- Read the residual risks in [THREAT-MODEL.md](THREAT-MODEL.md).
