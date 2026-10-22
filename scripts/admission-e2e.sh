#!/usr/bin/env bash
# Only creates/crashes services owned by this invocation; never accepts an existing DB.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
export ADMISSION_DB_MODE=${ADMISSION_DB_MODE:-local}
export ADMISSION_RUN_DIR=${ADMISSION_REPORT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/swarmpg-admission.XXXXXX")}
mkdir -p "$ADMISSION_RUN_DIR"
ADMISSION_RUN_DIR=$(cd "$ADMISSION_RUN_DIR" && pwd)
export ADMISSION_RUN_DIR
python3 - "$ADMISSION_RUN_DIR" <<'PY'
import os,sys
if any(os.scandir(sys.argv[1])): sys.exit('report directory must be empty; old evidence retained')
PY
export ADMISSION_DATABASE_PORT=${ADMISSION_DATABASE_PORT:-55500}
export ADMISSION_GATEWAY_PORT=${ADMISSION_GATEWAY_PORT:-55501}
export ADMIN_KEY=admission-test-admin
export PRINCIPAL_SECRET=admission-test-principal
export ADMISSION_EXECUTION_MS=${ADMISSION_EXECUTION_MS:-3000}
export AUDIT_MODE=best-effort AUDIT_SPOOL_CAPACITY=10000
unset AUDIT_SPOOL_DIR
python3 - "$ADMISSION_DATABASE_PORT" "$ADMISSION_GATEWAY_PORT" <<'PY'
import socket,sys
for port in [int(sys.argv[1]),*range(int(sys.argv[2]),int(sys.argv[2])+3)]:
 with socket.socket() as s:
  s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
  s.bind(('127.0.0.1',port))
PY
if [ "${KERNEL:-}" = 1 ] && [ -z "${PG_BIN_DIR:-}" ]; then echo "kernel mode requires explicit PG_BIN_DIR" >&2; exit 2; fi
export PG_BIN_DIR=${PG_BIN_DIR:-$(dirname "$(command -v psql)")}
export DATABASE_URL="postgres://admission_admin@127.0.0.1:$ADMISSION_DATABASE_PORT/admission_test?sslmode=disable"
export ADMISSION_PGDATA="$ADMISSION_RUN_DIR/pgdata"
container="swarmpg-admission-$$-$(date +%s)"
owned=false
signal_gateway(){
 local i=$1 signal=$2 pid command
 [ -f "$ADMISSION_RUN_DIR/gateway-$i.pid" ] || return 0
 pid=$(cat "$ADMISSION_RUN_DIR/gateway-$i.pid")
 command=$(ps -p "$pid" -o command= 2>/dev/null || true)
 # PID files survive crashes. Do not signal a recycled PID belonging to a
 # different process; this invocation's binary lives in its unique directory.
 if [ "$command" = "$ADMISSION_RUN_DIR/gateway" ]; then kill -"$signal" "$pid" 2>/dev/null || true; fi
}
cleanup(){
 for i in 0 1 2; do signal_gateway "$i" TERM; done
 # Give owned gateways time to cancel requests and release reservations.
 for attempt in $(seq 1 50); do
  running=false
  for i in 0 1 2; do if [ -f "$ADMISSION_RUN_DIR/gateway-$i.pid" ] && kill -0 "$(cat "$ADMISSION_RUN_DIR/gateway-$i.pid")" 2>/dev/null; then running=true; fi; done
  [ "$running" = false ] && break
  sleep .1
 done
 for i in 0 1 2; do signal_gateway "$i" KILL; done
 if [ "$owned" = true ]; then
  if [ "$ADMISSION_DB_MODE" = docker ]; then docker rm -f "$container" >/dev/null 2>&1 || true
  elif [ -f "$ADMISSION_PGDATA/postmaster.pid" ]; then "$PG_BIN_DIR/pg_ctl" -D "$ADMISSION_PGDATA" -m immediate -w stop >/dev/null 2>&1 || true; fi
 fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [ "$ADMISSION_DB_MODE" = docker ]; then
 if [ "${KERNEL:-}" = 1 ]; then echo 'kernel requires local mode and PG_BIN_DIR' >&2; exit 2; fi
 docker run -d --name "$container" --label swarmpg.admission.owner="$$" \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=admission_admin -e POSTGRES_DB=admission_test \
  -p "127.0.0.1:$ADMISSION_DATABASE_PORT:5432" postgres:17 -c max_connections=200 > "$ADMISSION_RUN_DIR/container.id"
 owned=true
elif [ "$ADMISSION_DB_MODE" = local ]; then
 "$PG_BIN_DIR/initdb" -D "$ADMISSION_PGDATA" -U admission_admin --auth=trust > "$ADMISSION_RUN_DIR/initdb.log"
 owned=true
 opts="-p $ADMISSION_DATABASE_PORT -h 127.0.0.1 -k /tmp -c max_connections=200 -c shared_buffers=128MB"
 if [ "${KERNEL:-}" = 1 ]; then opts="$opts -c agent_principal_secret=$PRINCIPAL_SECRET"; fi
 "$PG_BIN_DIR/pg_ctl" -D "$ADMISSION_PGDATA" -l "$ADMISSION_RUN_DIR/postgres.log" -o "$opts" -w start >/dev/null
 "$PG_BIN_DIR/createdb" -h 127.0.0.1 -p "$ADMISSION_DATABASE_PORT" -U admission_admin admission_test
else echo 'ADMISSION_DB_MODE must be local or docker' >&2; exit 2; fi
for i in $(seq 1 100); do "$PG_BIN_DIR/pg_isready" -h 127.0.0.1 -p "$ADMISSION_DATABASE_PORT" >/dev/null 2>&1 && break; sleep .1; done
"$PG_BIN_DIR/psql" "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -q -f "$root/sql/schema.sql" > "$ADMISSION_RUN_DIR/schema.log" 2>&1
export ADMISSION_DB_VERSION
ADMISSION_DB_VERSION=$("$PG_BIN_DIR/psql" "$DATABASE_URL" -X -tAc 'SELECT version()')
export ADMISSION_SOURCE_COMMIT
ADMISSION_SOURCE_COMMIT=$(git -C "$root" rev-parse HEAD)
export ADMISSION_SOURCE_TREE_SHA256
ADMISSION_SOURCE_TREE_SHA256=$(python3 - "$root" <<'PY'
import hashlib,pathlib,subprocess,sys
root=pathlib.Path(sys.argv[1]);h=hashlib.sha256()
for raw in sorted(set(subprocess.check_output(['git','-C',str(root),'ls-files','-z','-c','-o','--exclude-standard']).split(b'\0'))):
 if raw:
  p=root/raw.decode()
  if p.is_file(): h.update(raw+b'\0'+p.read_bytes()+b'\0')
print(h.hexdigest())
PY
)
(cd "$root/gateway" && go build -o "$ADMISSION_RUN_DIR/gateway" .)
(cd "$root/admission" && go build -o "$ADMISSION_RUN_DIR/workload" .)
cat > "$ADMISSION_RUN_DIR/control.sh" <<'CONTROL'
#!/usr/bin/env bash
set -euo pipefail
start(){
 local i=$1
 ADDR="127.0.0.1:$((ADMISSION_GATEWAY_PORT+i))" "$ADMISSION_RUN_DIR/gateway" >> "$ADMISSION_RUN_DIR/gateway-$i.log" 2>&1 &
 echo $! > "$ADMISSION_RUN_DIR/gateway-$i.pid"
}
case "${1:-}" in
 initial) for i in 0 1 2; do start "$i"; done ;;
 gateway)
  pid=$(cat "$ADMISSION_RUN_DIR/gateway-0.pid")
  command=$(ps -p "$pid" -o command= 2>/dev/null || true)
  [ "$command" = "$ADMISSION_RUN_DIR/gateway" ] || { echo 'owned gateway identity mismatch' >&2; exit 1; }
  kill -KILL "$pid"; sleep .3; start 0 ;;
 *) echo 'unknown owned-service operation' >&2; exit 2 ;;
esac
for i in 0 1 2; do
 ready=false
 for attempt in $(seq 1 200); do
  if curl -sf -o /dev/null "http://127.0.0.1:$((ADMISSION_GATEWAY_PORT+i))/readyz"; then ready=true; break; fi
  sleep .1
 done
 if [ "$ready" != true ]; then echo 'owned gateway did not become ready' >&2;exit 1;fi
done
CONTROL
chmod +x "$ADMISSION_RUN_DIR/control.sh"
"$ADMISSION_RUN_DIR/control.sh" initial
"$ADMISSION_RUN_DIR/workload" \
 -gateways "http://127.0.0.1:$ADMISSION_GATEWAY_PORT,http://127.0.0.1:$((ADMISSION_GATEWAY_PORT+1)),http://127.0.0.1:$((ADMISSION_GATEWAY_PORT+2))" \
 -admin-key "$ADMIN_KEY" -database "$DATABASE_URL" -psql "$PG_BIN_DIR/psql" \
 -fault-control "$ADMISSION_RUN_DIR/control.sh" -workers "${ADMISSION_WORKERS:-2000}" \
 -duration "${ADMISSION_DURATION:-120s}" -output "$ADMISSION_RUN_DIR/report.json" | tee "$ADMISSION_RUN_DIR/workload.log"
echo "admission evidence retained in $ADMISSION_RUN_DIR"
