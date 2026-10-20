# KERNEL=1 runs the cluster on the SwarmPG kernel (built by `make kernel`) instead of
# the stock Postgres on PATH, on its own port and data directory. The kernel requires
# agent principals signed with PRINCIPAL_SECRET, which the gateway and load test share.
ifdef KERNEL
PGBIN ?= $(CURDIR)/.pginstall/bin/
PGDATA ?= $(CURDIR)/.pg-kernel
PGPORT ?= 5511
export PRINCIPAL_SECRET ?= dev-principal-secret
PGOPTS = -c agent_principal_secret=$(PRINCIPAL_SECRET)
else
PGBIN ?=
PGDATA ?= $(CURDIR)/.pg
PGPORT ?= 5499
endif
export DATABASE_URL ?= postgres://$(USER)@127.0.0.1:$(PGPORT)/swarmpg?sslmode=disable

pg-start:
	@test -d "$(PGDATA)" || $(PGBIN)initdb -D "$(PGDATA)" -U $(USER) --auth=trust >/dev/null
	@$(PGBIN)pg_ctl -D "$(PGDATA)" -o "-p $(PGPORT) -c max_connections=100 -c shared_buffers=256MB $(PGOPTS)" -l "$(PGDATA)/log" -w start >/dev/null || true
	@$(PGBIN)psql -h 127.0.0.1 -p $(PGPORT) -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='swarmpg'" | grep -q 1 || $(PGBIN)createdb -h 127.0.0.1 -p $(PGPORT) swarmpg
	@$(PGBIN)psql -h 127.0.0.1 -p $(PGPORT) -d swarmpg -v ON_ERROR_STOP=1 -q -f sql/schema.sql
	@echo "postgres ready on :$(PGPORT)"

pg-stop:
	@$(PGBIN)pg_ctl -D "$(PGDATA)" stop >/dev/null || true

gateway:
	cd gateway && go run .

loadtest:
	cd loadtest && go run . -agents $(or $(AGENTS),2000) -requests $(or $(REQUESTS),10)

# Cluster up, gateway + load test + isolation probes (kernel probes too under KERNEL=1), down.
e2e: pg-start
	@scripts/e2e.sh; status=$$?; $(MAKE) --no-print-directory pg-stop; exit $$status

kernel:
	@test -d swarmpg-core || kernel/build.sh fetch
	kernel/build.sh build

kernel-check:
	kernel/build.sh check

test:
	cd gateway && go vet ./... && go test ./...
	cd loadtest && go vet ./...
	cd bench && go vet ./...

.PHONY: pg-start pg-stop gateway loadtest e2e kernel kernel-check test
