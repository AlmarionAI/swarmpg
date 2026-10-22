# External tool dispatcher

This standalone Go process delivers transactional task outbox messages to one
operator-configured receiver per organization and destination. See the
[outbox contract and operating guide](../docs/outbox.md) for configuration, receiver
deduplication requirements, retry semantics and administrative recovery.

From this directory, run unit and HTTP failure tests with:

```bash
go vet ./...
go test -race -count=1 ./...
```

From the repository root, run the real crash recovery workload on an owned stock
PostgreSQL container:

```bash
COORD_DB_MODE=docker scripts/outbox-e2e.sh
```

Or use a built SwarmPG kernel:

```bash
KERNEL=1 PG_BIN_DIR="$PWD/.pginstall/bin" scripts/outbox-e2e.sh
```

The harness kills only services it creates. It checks atomic completion rollback,
dispatcher and receiver termination after external commit, gateway and database
crashes, lost replies, retryable and permanent failures, operator retry, stale
fences, claim replay, concurrent dispatchers, organization/destination isolation,
and agreement between immutable intents, the durable receiver ledger and delivery
history. Reports and logs remain in the printed evidence directory after cleanup.

For synchronous primary failure, replication partitions and archive-based PITR, run the
separate [recovery workload](../docs/recovery.md#reproducible-validation). It verifies
quarantine and operator reconciliation as well as receiver deduplication; the crash
workload above does not establish those deployment guarantees.
