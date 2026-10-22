#!/usr/bin/env bash
# Own every service that this failure workload may stop. Existing databases and
# gateways are never used or restarted. Reports and logs survive cleanup.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
workload=${COORD_WORKLOAD:-coordination}
case "$workload" in coordination|outbox) ;; *) echo "unknown workload: $workload" >&2; exit 2 ;; esac
export COORD_DB_MODE=${COORD_DB_MODE:-local}
export COORD_RUN_DIR=${COORD_REPORT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/swarmpg-coordination.XXXXXX")}
mkdir -p "$COORD_RUN_DIR"
COORD_RUN_DIR=$(cd "$COORD_RUN_DIR" && pwd)
export COORD_RUN_DIR
# Refuse evidence-directory reuse before installing cleanup or touching services.
# Old PID/PGDATA files must never become resources owned by a new invocation.
python3 - "$COORD_RUN_DIR" <<'PY'
import os, sys
if any(os.scandir(sys.argv[1])):
    sys.exit("COORD_REPORT_DIR must be an empty directory; existing evidence is retained")
PY
# Keep defaults outside Linux/macOS ephemeral ranges so outbound test traffic
# cannot temporarily occupy a future service listener. Explicit ports are checked.
export COORD_GATEWAY_PORT=${COORD_GATEWAY_PORT:-16451}
export COORD_DATABASE_PORT=${COORD_DATABASE_PORT:-16450}
export COORD_GATEWAY_BIN="$COORD_RUN_DIR/gateway"
export COORD_DATABASE_NAME=coordination_test
export COORD_DATABASE_USER=coord_admin
export COORD_PGDATA="$COORD_RUN_DIR/pgdata"
export ADMIN_KEY=${ADMIN_KEY:-dev-admin-key}
export PRINCIPAL_SECRET=${PRINCIPAL_SECRET:-coordination-test-secret}
# Both database modes compare the exact same gateway audit and pool settings.
export AUDIT_MODE=best-effort
export AUDIT_SPOOL_CAPACITY=10000
unset AUDIT_SPOOL_DIR

python3 - "$COORD_GATEWAY_PORT" "$COORD_DATABASE_PORT" <<'PY'
import socket, sys
for value in sys.argv[1:]:
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", int(value)))
PY

export PG_BIN_DIR=${PG_BIN_DIR:-}
if [ -z "$PG_BIN_DIR" ]; then
  if [ "${KERNEL:-}" = "1" ]; then
    PG_BIN_DIR="$root/.pginstall/bin"
  else
    PG_BIN_DIR=$(dirname "$(command -v psql)")
  fi
fi
PG_BIN_DIR=$(cd "$PG_BIN_DIR" && pwd)
export PG_BIN_DIR
export DATABASE_URL="postgres://$COORD_DATABASE_USER@127.0.0.1:$COORD_DATABASE_PORT/$COORD_DATABASE_NAME?sslmode=disable"
export COORD_DATABASE_CONTAINER="swarmpg-coordination-$$-$(date +%s)"
coord_database_owned=false
coord_gateway_owned=false

cleanup() {
  if [ "$coord_gateway_owned" = true ] && [ -f "$COORD_RUN_DIR/gateway.pid" ]; then
    kill "$(cat "$COORD_RUN_DIR/gateway.pid")" 2>/dev/null || true
  fi
  if [ "$coord_database_owned" != true ]; then
    return
  elif [ "$COORD_DB_MODE" = docker ]; then
    docker rm -f "$COORD_DATABASE_CONTAINER" >/dev/null 2>&1 || true
  elif [ -f "$COORD_PGDATA/postmaster.pid" ]; then
    "$PG_BIN_DIR/pg_ctl" -D "$COORD_PGDATA" -m immediate -w stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ "$COORD_DB_MODE" = docker ]; then
  if [ "${KERNEL:-}" = "1" ]; then
    echo "KERNEL=1 requires local mode with PG_BIN_DIR pointing at the patched build" >&2
    exit 2
  fi
  docker run -d --name "$COORD_DATABASE_CONTAINER" \
    --label swarmpg.coordination.owner="$$" \
    -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER="$COORD_DATABASE_USER" \
    -e POSTGRES_DB="$COORD_DATABASE_NAME" -p "127.0.0.1:$COORD_DATABASE_PORT:5432" \
    "${COORD_PG_IMAGE:-postgres:17}" > "$COORD_RUN_DIR/container.id"
  coord_database_owned=true
elif [ "$COORD_DB_MODE" = local ]; then
  "$PG_BIN_DIR/initdb" -D "$COORD_PGDATA" -U "$COORD_DATABASE_USER" --auth=trust > "$COORD_RUN_DIR/initdb.log"
  coord_database_owned=true
  pg_options="-p $COORD_DATABASE_PORT -h 127.0.0.1 -k /tmp -c max_connections=100 -c shared_buffers=128MB"
  if [ "${KERNEL:-}" = "1" ]; then
    pg_options="$pg_options -c agent_principal_secret=$PRINCIPAL_SECRET"
  fi
  export COORD_PG_OPTIONS="$pg_options"
  "$PG_BIN_DIR/pg_ctl" -D "$COORD_PGDATA" -l "$COORD_RUN_DIR/postgres.log" -o "$pg_options" -w start >/dev/null
else
  echo "COORD_DB_MODE must be local or docker" >&2
  exit 2
fi
for i in $(seq 1 100); do
  "$PG_BIN_DIR/pg_isready" -h 127.0.0.1 -p "$COORD_DATABASE_PORT" -U "$COORD_DATABASE_USER" -d postgres >/dev/null 2>&1 && break
  sleep 0.1
done
if [ "$COORD_DB_MODE" = local ]; then
  "$PG_BIN_DIR/createdb" -h 127.0.0.1 -p "$COORD_DATABASE_PORT" -U "$COORD_DATABASE_USER" "$COORD_DATABASE_NAME"
fi
"$PG_BIN_DIR/psql" "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -q -f "$root/sql/schema.sql" > "$COORD_RUN_DIR/schema.log" 2>&1
export COORD_DB_VERSION
COORD_DB_VERSION=$("$PG_BIN_DIR/psql" "$DATABASE_URL" -X -tAc 'SELECT version()')
export COORD_SOURCE_COMMIT
COORD_SOURCE_COMMIT=$(git -C "$root" rev-parse HEAD)
export COORD_SOURCE_TREE_SHA256
COORD_SOURCE_TREE_SHA256=$(python3 - "$root" <<'PY'
import hashlib, pathlib, subprocess, sys
root = pathlib.Path(sys.argv[1])
paths = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z", "-c", "-o", "--exclude-standard"]).split(b"\0")
h = hashlib.sha256()
for raw in sorted(set(p for p in paths if p)):
    path = root / raw.decode()
    if path.is_file():
        h.update(raw + b"\0" + path.read_bytes() + b"\0")
print(h.hexdigest())
PY
)
(cd "$root/gateway" && go build -o "$COORD_GATEWAY_BIN" .)
if [ "$workload" = outbox ]; then
  (cd "$root/outbox" && go build -o "$COORD_RUN_DIR/outbox-dispatcher" .)
else
  (cd "$root/coordination" && go build -o "$COORD_RUN_DIR/coordination" .)
fi

# This private controller only knows the services created above. The workload
# passes a fixed operation name; it cannot choose a PID, container or cluster.
cat > "$COORD_RUN_DIR/control.sh" <<'CONTROL'
#!/usr/bin/env bash
set -euo pipefail
start_gateway() {
  ADDR="127.0.0.1:$COORD_GATEWAY_PORT" "$COORD_GATEWAY_BIN" >> "$COORD_RUN_DIR/gateway.log" 2>&1 &
  echo "$!" > "$COORD_RUN_DIR/gateway.pid"
}
case "${1:-}" in
  initial) start_gateway ;;
  gateway)
    kill -KILL "$(cat "$COORD_RUN_DIR/gateway.pid")"
    sleep 0.3
    start_gateway
    ;;
  database)
    if [ "$COORD_DB_MODE" = docker ]; then
      docker restart -t 0 "$COORD_DATABASE_CONTAINER" >/dev/null
    else
      "$PG_BIN_DIR/pg_ctl" -D "$COORD_PGDATA" -m immediate -w stop >/dev/null
      "$PG_BIN_DIR/pg_ctl" -D "$COORD_PGDATA" -l "$COORD_RUN_DIR/postgres.log" -o "$COORD_PG_OPTIONS" -w start >/dev/null
    fi
    ;;
  *) echo "unknown owned-service fault" >&2; exit 2 ;;
esac
for i in $(seq 1 200); do
  if "$PG_BIN_DIR/psql" "$DATABASE_URL" -X -tAc 'SELECT 1' >/dev/null 2>&1 && \
      curl -sf -o /dev/null "http://127.0.0.1:$COORD_GATEWAY_PORT/healthz"; then
    exit 0
  fi
  sleep 0.1
done
echo "owned services did not recover" >&2
exit 1
CONTROL
chmod +x "$COORD_RUN_DIR/control.sh"
coord_gateway_owned=true
"$COORD_RUN_DIR/control.sh" initial

if [ "$workload" = outbox ]; then
  python3 "$root/scripts/outbox-recovery.py" | tee "$COORD_RUN_DIR/workload.log"
else
  "$COORD_RUN_DIR/coordination" \
  -gateway "http://127.0.0.1:$COORD_GATEWAY_PORT" -admin-key "$ADMIN_KEY" \
  -workers "${COORD_WORKERS:-100}" -duration "${COORD_DURATION:-30s}" \
  -in-flight "${COORD_IN_FLIGHT:-128}" -lease-ms "${COORD_LEASE_MS:-3000}" \
  -seed "${COORD_SEED:-20261022}" -fault-control "$COORD_RUN_DIR/control.sh" \
  -output "$COORD_RUN_DIR/report.json" | tee "$COORD_RUN_DIR/workload.log"
fi
echo "$workload evidence retained in $COORD_RUN_DIR"
