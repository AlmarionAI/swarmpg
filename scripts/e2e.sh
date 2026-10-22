#!/usr/bin/env bash
# Build the gateway and load test, run the gateway against DATABASE_URL (schema already
# applied), drive the fleet load test and isolation probes, and stop the gateway.
set -euo pipefail
: "${DATABASE_URL:?set DATABASE_URL to an admin URL for a database with sql/schema.sql applied}"

root=$(cd "$(dirname "$0")/.." && pwd)
port=${GATEWAY_PORT:-8080}
mkdir -p "$root/.bin"
(cd "$root/gateway" && go build -o "$root/.bin/gateway" .)
(cd "$root/loadtest" && go build -o "$root/.bin/loadtest" .)

# Integration regressions use generated fixture identities in this disposable
# test database, including audit outage/recovery and shared-producer isolation.
(cd "$root/gateway" && SWARMPG_TEST_DATABASE_URL="$DATABASE_URL" \
	SWARMPG_TEST_PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-}" go test -race -count=1 ./...)

# GNU make exports command-line KERNEL=1 to recipes, including the kernel CI
# job. Stock runs leave it unset and do not execute kernel-only wire tests.
# Use the same disposable admin DATABASE_URL; the protocol client resolves it
# directly, with optional PGHOST/PGPORT/PGUSER/PGDATABASE overrides.
if [ "${KERNEL:-}" = "1" ]; then
	python3 "$root/kernel/tests/agent_budget_protocol.py"
fi

ADDR=":$port" "$root/.bin/gateway" &
gw=$!
trap 'kill $gw 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
	curl -sf -o /dev/null "http://127.0.0.1:$port/healthz" && break
	sleep 0.1
done

"$root/.bin/loadtest" -gateway "http://127.0.0.1:$port" \
	-agents "${AGENTS:-500}" -requests "${REQUESTS:-8}" -pg "$DATABASE_URL"
