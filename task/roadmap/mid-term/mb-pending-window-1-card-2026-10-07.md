# Card MB-PENDING-WINDOW-1: a restart after a failed batch replans the batch's window

**Date:** 2026-10-07 · **Filed by:** claude-opus-5-5 (opus-worker build lane), for the orchestrator · **Source:** MB-3 fold 1 (PR #994), verifier finding F4. **Status:** filed, needs an owner decision. No product code in this filing. This card closes when the owner picks an option below and, for option (a), when the unit that builds it merges.

## Measured

| Engine | First run (body fails at batch 0) | Restart after the source grew by `(4, 5)` |
|---|---|---|
| RePark, `availableNow`, no cap | batch 0 `(1, 2, 3)`, query `Failed`, no durable offset | batch 0 `(1, 2, 3, 4, 5)` |
| RePark, `availableNow`, `streaming-max-files-per-micro-batch=1` | batch 0 `(1, 2, 3)`, query `Failed` | batch 0 `(1, 2, 3)`, batch 1 `(4, 5)` |
| Spark 4.1.2 + Iceberg 1.11.0, `availableNow` | batch 0 `(1, 2, 3)`, `STREAM_FAILED`, `offsets` `0`, no commit | batch 0 `(1, 2, 3)`, batch 1 `(4, 5)`, `offsets` `0, 1`, `commits` `0, 1` |

RePark's rows are the pin
`crates/repark-core/src/microbatch/lifecycle_tests.rs::a_restart_after_a_failed_batch_replans_its_window_over_a_grown_source`.
Spark's row is cell MB3-W9 in
[mb3_fold_oracle.json](../../../python/repark-parity/tests/live_spark/mb3_fold_oracle.json).
The declared difference is registry row `MB-3-REPLAY-WINDOW-1` in
[spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

Spark writes a batch's offset range to its offset log before the batch runs, so a replayed
batch id always covers identical data. RePark keeps offsets in the sink's snapshot summary and
table property and has no offset log (North Star: the sink is the checkpoint). The window of a
batch is therefore durable only once the batch's stamp commits. `Run::next_batch` in
`crates/repark-core/src/microbatch/run.rs` plans each window from the durable offset to the
source's head at the time of the trigger, and a restart does the same.

The consequence falls on a `foreachBatch` body that deduplicates on the batch id, which is the
idempotence recipe Spark documents. If such a body records batch 0 as done and then fails, it
skips the wider replay of batch 0, and the driver's trailing stamp moves the offset past rows
the body never wrote. The `toTable` door is not affected: its rows and its stamp are one commit.

## Options

1. **Option (a): a pending-window stamp.** Before the body runs, commit the batch's end offset
   to the sink as a pending record (a table property such as
   `repark.cdc.pending.<query>`, written by a stamp-only commit). A restart that finds a
   pending record above the durable one replays exactly that window under the same batch id,
   then plans on. Cost: one more catalog commit per batch on the `foreachBatch` door, on top
   of the trailing stamp (three commits per batch for a one-write body), and a new record to
   fence, expire and read at resume. The `toTable` door does not need it.
2. **Option (b): leave it declared.** Keep registry row `MB-3-REPLAY-WINDOW-1` and document
   that a body on RePark deduplicates on its rows' keys. Cost: none in commits; a body ported
   from Spark with the batch-id recipe can lose rows after a failure that follows a source
   append.

## Pins (for option (a))

- The pin named above flips: the restart delivers batch 0 `(1, 2, 3)` and batch 1 `(4, 5)`
  with no cap, as MB3-W9 records. Mutation: the resume ignores the pending record.
- A pending record with no later stamp survives two restarts and replays the same window each
  time.
- A pending record written by another run of the same query fences as a durable record does.
