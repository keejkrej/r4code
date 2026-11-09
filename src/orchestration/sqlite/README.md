# SQLite durability

`sqlite::SqliteStore` implements the orchestration `Store` and `OutboxStore` traits using `rusqlite` with bundled SQLite.

## Durable tables

- `orchestration_events`: normalized event JSON, global monotonic sequence, app thread, command, event type, and raw-event correlation.
- `orchestration_receipts`: accepted/rejected command receipts and result cursors.
- `orchestration_effects`: provider effects, status, attempts, worker lease, errors, and timestamps.

`commit` uses `BEGIN IMMEDIATE` (`TransactionBehavior::Immediate`) so expected thread cursors, event batches, receipts, and effect enqueueing commit atomically. Multi-thread delegated-task batches use the same transaction. Projection validation runs before insert, and snapshots rebuild through the shared `orchestration::projection::replay` implementation.

## Effect recovery

Effects use at-least-once execution semantics:

```text
pending
  -> claimed running (worker ID + lease + attempts + 1)
  -> completed or failed

running + expired lease
  -> claimed by another worker
```

Only the current leaseholder can complete an effect. A failed effect remains `failed` with its error. Provider adapters use deterministic effect IDs and native lifecycle correlation to make replay safe.

`EffectWorker::run_once` claims and executes a batch; `EffectWorker::run` polls for new work while renewing active leases via background heartbeats.

## Tests

```sh
cargo test orchestration::sqlite::tests
```
