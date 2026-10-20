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

ADDR=":$port" "$root/.bin/gateway" &
gw=$!
trap 'kill $gw 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
	curl -sf -o /dev/null "http://127.0.0.1:$port/healthz" && break
	sleep 0.1
done

"$root/.bin/loadtest" -gateway "http://127.0.0.1:$port" \
	-agents "${AGENTS:-500}" -requests "${REQUESTS:-8}" -pg "$DATABASE_URL"
