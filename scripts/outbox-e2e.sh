#!/usr/bin/env bash
# Reuse the owned-service lifecycle and fault controller; never target an
# existing database, gateway, dispatcher or external tool provider.
set -euo pipefail
export COORD_WORKLOAD=outbox
exec "$(dirname "$0")/coordination-e2e.sh"
