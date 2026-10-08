# F-APPEND-PIN-BASE-1: an append that fails, instead of re-basing, when its pinned base moved (fork card)

**Filed:** 2026-10-07. **Revised:** 2026-10-07 (MB-2c fold 1, the verifier's findings 1, 2, 3 and 7): the ask now fences only this query's own concurrent commits, the two edge cases are specified, and A′ is recorded. This is the micro-batch sink's third fork request. It is the conditional ask of [the design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §7. OQ-4 ("filed only if DM-6 fails") was ratified with the sketch (#971, 2026-10-06). DM-6 failed on 2026-10-07 ([MB-2c ledger](../../ledgers/staging/mb-2c-ledger.md) C-001), and the orchestrator's ruling Q1 of 2026-10-07 files it.

**2026-10-07 (~20:55 EDT, owner ruling; MB-2c closing slice): the RePark-side stopgap is in, and this card is now its retirement, not a blocker.** The owner ruled that this card is not built in the fork now and that the catalog wrapper may stand in. `AppendFence` (`crates/repark-iceberg/src/write/sink_offsets_append_fence.rs`) applies "The request" below at `update_table`, on the stamped append arm and the stamp-only door. Harness pins 2 and 3 are un-ignored and the crash gate reads 5 passed ([MB-2c ledger](../../ledgers/staging/mb-2c-ledger.md) C-008 to C-010). Nothing in RePark waits on this card any more. When the fork lands it, RP-N repins and the wrapper is deleted; the exact items are in the ledger's C-011 and in [`write/map.md`](../../../crates/repark-iceberg/src/write/map.md) under "Retirement". The sections "Consumer", "Severity" and "What RePark does once it merges" below are kept as filed and are read under this line.

**Target:** the owned `iceberg-rust` fork, at the pin `076d5f98` (`Cargo.toml:167`). A fix lands with red-first pins, and RP-N repins RePark. This is the owner's fork work, and no RePark lane implements it.

**Consumer:** MB-2c's closing slice, which covers:
- step 3, the append fence;
- un-ignoring harness pins 2 and 3;
- the 5-passed crash gate.

See [the MB-2c order](../../wo/microbatch/mb-2c-replay-reconcile.md) and the sketch §2 Q8.

**Severity:** P2. NS-5 and CC-9 stand on it on the append door: two drivers on one sink, and a stale re-delivery, must lose at the base the epoch check read. Today the append re-bases and duplicates the batch.

## What MB-2c needs

The epoch check (MB-2c step 1, landed) reads the sink head `H` from the table the commit starts from. The check is sound only if the commit cannot land on a base other than `H`. The copy-on-write and merge-on-read arms already fail on a moved base, but only under `write.merge.isolation-level=serializable` (the default): they add `validate_no_conflicting_data` only at that level. Harness pin 3's copy-on-write variant is green with the epoch check in. Under `snapshot` isolation a stamped copy-on-write MERGE re-bases past run A and commits epoch 1 twice; MBE-15 (`MergeIsolationRefused`) is the RePark-side refusal for that, tracked by [the ledger](../../ledgers/staging/mb-2c-ledger.md) C-007, and it is not part of this card. The append arm does not fail:
- `commit_append_with_summary` builds a `merge_append`;
- the stamp-only door builds an empty `merge_append`;
- the fork's retry re-bases either one past a concurrent append and lands it.

## What is already there (read at the pin `076d5f98`)

- **The re-base.** `Transaction::do_commit` reloads, re-bases a stale transaction and re-applies its actions (`crates/iceberg/src/transaction/mod.rs:506`–`:515`). `FastAppendAction::commit` (`append.rs:108`–`:131`) and `MergeAppendAction` validate only their added files. Neither carries `validate_from_snapshot` nor a concurrent-data check.
- **The validation shape to copy.** `OverwriteFilesAction` has `validate_from_snapshot(i64)` (`overwrite_files.rs:270`) and `validate_no_conflicting_data()` (`:242`), and runs them in `validate` (`:443`) before the re-apply.

## Why the validation must sit on the append action itself (DM-6, C-001 (d), and A′)

DM-6 measured branch A on the pin: an empty `overwrite_files()` with `validate_from_snapshot(H)` added beside the stamped append. The measurement has three findings:
- It fails the raced commit with `DataInvalid`.
- It refuses every quiet commit with `PreconditionFailed`, because the snapshot is empty.
- With `allow_empty_commit()` added, it never commits. Each snapshot producer in the transaction asserts `main` at the transaction's base, the first moves it, and the second fails `CatalogCommitConflicts` through every retry.

A second snapshot-producing action cannot fence the append.

**A′, measured and rejected (the verifier, 2026-10-07).** A single add-only `overwrite_files()` carrying the files, the stamp summary, `validate_no_conflicting_data()` and `validate_from_snapshot(H)` replaces the append. It fences the files-carrying arm: the quiet commit lands one `append` snapshot, and a racing append fails it with `DataInvalid`. It is rejected for two reasons:
- **It cannot fence the stamp-only door.** A racer with no data files is not seen (an empty stamp-only racer lets it land), and with no added files the operation classifies as `overwrite`, not `append`.
- **It shares the MB-2a conflict.** It fails on any concurrent data, so it refuses unrelated writers and other queries (the four MB-2a pins under "The request").

So no action available at the pin both fences the stamp-only door and keeps MB-2a's tolerance. The check goes on the append action's own `validate`, keyed to this query's stamp.

## The request

`FastAppendAction` and `MergeAppendAction` gain `validate_from_snapshot(i64)` and `validate_no_concurrent_snapshot_with_summary(key, value)`, both off by default. RePark sets the key to `repark.cdc.query-id` and the value to the committing query's id.
- **The rule.** With the summary check set, `validate` fails if the refreshed base holds a snapshot on the target ref, newer than the from-snapshot, whose summary carries `key = value`. Only this query's own concurrent commits fail it.
- **MB-2a's tolerance is kept.** A newer snapshot that does not carry the pair never fails the check, whatever its operation: an unrelated writer's append, another query's stamped commit, a compaction `Replace`. Sketch §6 ("the concurrent-append pin keeps its shape") holds. Four MB-2a pins measure this, and each stays green: `a_concurrent_unrelated_append_still_commits_both_halves_exactly_once`, `a_racing_stamp_of_another_query_keeps_both_records`, `a_foreign_writer_inside_the_scope_commits_as_on_main_and_leaves_the_claim` and `three_racing_appends_still_stamp_exactly_once_on_every_arm`. A check on every newer `Append`, `Overwrite` or `Delete`, as the first draft asked, turns all four red.
- **No from-snapshot.** Without `validate_from_snapshot`, the check uses the transaction's starting snapshot, as `OverwriteFilesAction` computes `effective_start`. A `None` start (the transaction began on an empty table) treats every snapshot on the ref as concurrent. That is harness pin 2's case: the stale view it re-delivers against has no snapshot, so `ClaimedStamp::base` is `None`.
- **A pinned id that is no longer an ancestor.** If the from-snapshot is not in the refreshed ref's ancestry (a rollback, or a ref reset past it), `validate` fails. Nothing can be proven about what landed after it.
- **The operation stays `Append`.** No second snapshot is produced, and an empty merge append (the stamp-only door) is fenced the same way.
- **Every failure is non-retryable,** so the transaction's retry loop cannot re-base past it. It names the from-snapshot, the newer snapshot and the key.

It is the narrow form of silver-s0's `F-SILVER-PIN-BASE` candidate (`task/ledgers/staging/silver-s0-ledger.md` C-008).

## Measured against RePark (MB-2c fold 1, 2026-10-07)

The revised rule was emulated without the fork, as a temporary test-only catalog shim. It wraps the catalog for a stamped commit on the append arm and on the stamp-only door, and refuses `update_table` (non-retryable `DataInvalid`) when `main` above the pinned base carries this query's stamp, or when the base is not on `main`. The fork re-bases before each `update_table`, so the shim sees the same refreshed state a `validate` would. Results, with `--include-ignored`, were:
- harness pins 2 and 3 green, so the crash gate reads 5 passed;
- the four MB-2a pins above green;
- `cargo test -p repark-iceberg --lib`: 940 passed, 0 failed.

The shim was reverted, and none of it is committed. The record is the [MB-2c ledger](../../ledgers/staging/mb-2c-ledger.md) C-006, under D-12.

## Pins (fork side)

- **The race pin.** A racer's commit that carries the same `key = value` lands between load and commit, and the pinned commit then fails with nothing landed. It is run on `fast_append`, on `merge_append` with files, and on an empty `merge_append` with a property update in the same transaction (the stamp-only shape).
- **The tolerance pin.** A racer's append without the pair, and a racer's append that carries the key with another value, each land between load and commit. The pinned commit still lands one `append` snapshot over them.
- **The replace pin.** An unrelated `replace` between load and commit does not fail the pinned commit, which lands one `append` snapshot.
- **The empty-start pin.** With no from-snapshot, on a transaction begun on an empty table, a racer that carries the pair fails the commit.
- **The non-ancestor pin.** A from-snapshot that a rollback took off the ref fails the commit.
- **The quiet pin.** With no race, the pinned commit lands exactly one `append` snapshot, and so does the empty merge append. This is the case branch A failed.
- **The default pin.** With the options unset, today's re-base still happens.

## Mutation

- Dropping the check from the append's `validate` turns the race pin red.
- Dropping the summary filter turns the tolerance pin red.

## What RePark does once it merges (MB-2c closing slice, not this card)

1. RP-N repins the fork.
2. `ClaimedStamp::stamp_transaction`'s callers pin the append to `ClaimedStamp::base`.
3. The `#[ignore]` lines on harness pins 2 and 3 are deleted, and the crash gate reads 5 passed.
4. The DM-6 measurements in `crates/repark-iceberg/src/write/sink_offsets_fence_tests.rs` are re-read against the new pin.

Up: [map.md](map.md).
