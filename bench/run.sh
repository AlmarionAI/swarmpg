#!/usr/bin/env bash
# Reproducible SwarmPG benchmark: the same workload against three setups, each
# on a fresh cluster. Results land in bench/results/<UTC timestamp>/.
#
#   bench/run.sh                                  # 10,000 agents x 20 requests, 1,000 in flight
#   AGENTS=2000 REQUESTS=5 CONCURRENCY=500 bench/run.sh
set -euo pipefail
cd "$(dirname "$0")"

AGENTS=${AGENTS:-10000}
REQUESTS=${REQUESTS:-20}
CONCURRENCY=${CONCURRENCY:-1000}
stamp=$(date -u +%Y%m%dT%H%M%SZ)
out=results/$stamp
mkdir -p "$out"
compose=(docker compose -f docker-compose.yml)
others=$(docker ps -q | wc -l | tr -d ' ')
env_desc="$(uname -sm); Docker $(docker version -f '{{.Server.Version}}'); $(docker info -f '{{.NCPU}}') CPUs and $(($(docker info -f '{{.MemTotal}}') / 1073741824)) GiB for containers; $others unrelated containers running"

"${compose[@]}" --profile baseline --profile swarmpg-stock --profile swarmpg-kernel --profile client build

# Stream the memory of the setup's containers (not the load generator) until
# the stop file appears.
sample() {
	ids=$(docker ps -q --filter label=com.docker.compose.project=swarmpg-bench)
	docker stats --format '{{.Name}} {{.MemUsage}}' $ids 2>/dev/null | sed -u 's/\x1b\[[0-9;]*[A-Za-z]//g' >>"$1" &
	local stream=$!
	while [ ! -e "$2" ]; do sleep 0.5; done
	kill "$stream" 2>/dev/null || true # docker stats then exits on its next write
}

run() {
	local profile=$1 target=$2 label=$3 pg=$4
	shift 4
	echo "== $label"
	"${compose[@]}" --profile "$profile" up -d --wait
	rm -f "$out/.stop"
	sample "$out/$profile.mem" "$out/.stop" &
	local sampler=$! status=0
	"${compose[@]}" run --rm -e BENCH_ENV="$env_desc" bench \
		-target "$target" -label "$label" -agents "$AGENTS" -requests "$REQUESTS" -concurrency "$CONCURRENCY" \
		-pg "postgres://postgres@$pg:5432/swarmpg?sslmode=disable" -out "/results/$stamp/$profile.json" "$@" || status=$?
	touch "$out/.stop"
	wait "$sampler" || true
	"${compose[@]}" --profile "$profile" down -v --remove-orphans >/dev/null 2>&1
	return $status
}

run baseline pgbouncer "PgBouncer + RLS (baseline)" pg-upstream
run swarmpg-stock gateway "SwarmPG gateway + stock Postgres" pg-upstream -gateway http://gateway-stock:8080
run swarmpg-kernel gateway "SwarmPG gateway + SwarmPG kernel" pg-kernel -gateway http://gateway-kernel:8080

{
	echo "Run $stamp: $AGENTS agents x $REQUESTS requests, $CONCURRENCY agents in flight. $env_desc."
	echo
	"${compose[@]}" run --rm bench report "/results/$stamp/baseline.json" "/results/$stamp/swarmpg-stock.json" "/results/$stamp/swarmpg-kernel.json"
	echo
	echo "Peak memory per container (MiB):"
	echo
	echo "| Setup | Container | Peak MiB |"
	echo "|---|---|---:|"
	for profile in baseline swarmpg-stock swarmpg-kernel; do
		awk -v p="$profile" '$1 ~ /^swarmpg-bench-/ {
			v = $2; u = v; sub(/[0-9.]+/, "", u); sub(/[A-Za-z]+$/, "", v)
			m = (u == "GiB") ? v * 1024 : (u == "KiB") ? v / 1024 : (u == "B") ? v / 1048576 : v
			name = $1; sub(/^swarmpg-bench-/, "", name); sub(/-[0-9]+$/, "", name)
			if (m > max[name]) max[name] = m
		} END { for (n in max) printf "| %s | %s | %.0f |\n", p, n, max[n] }' "$out/$profile.mem" | sort
	done
} | tee "$out/results.md"
