# F-APPEND-PIN-BASE-1: an append that fails, instead of re-basing, when its pinned base moved (fork card)

**Filed:** 2026-10-07. This is the micro-batch sink's third fork request. It is the conditional ask of [the design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §7. OQ-4 ("filed only if DM-6 fails") was ratified with the sketch (#971, 2026-10-06). DM-6 failed on 2026-10-07 ([MB-2c ledger](../../ledgers/staging/mb-2c-ledger.md) C-001), and the orchestrator's ruling Q1 of 2026-10-07 files it.

**Target:** the owned `iceberg-rust` fork, at the pin `076d5f98` (`Cargo.toml:167`). A fix lands with red-first pins, and RP-N repins RePark. This is the owner's fork work, and no RePark lane implements it.

**Consumer:** MB-2c's closing slice, which covers:
- step 3, the append fence;
- un-ignoring harness pins 2 and 3;
- the 5-passed crash gate.

See [the MB-2c order](../../wo/microbatch/mb-2c-replay-reconcile.md) and the sketch §2 Q8.

**Severity:** P2. NS-5 and CC-9 stand on it on the append door: two drivers on one sink, and a stale re-delivery, must lose at the base the epoch check read. Today the append re-bases and duplicates the batch.

## What MB-2c needs

The epoch check (MB-2c step 1, landed) reads the sink head `H` from the table the commit starts from. The check is sound only if the commit cannot land on a base other than `H`. The copy-on-write and merge-on-read arms already fail on a moved base: harness pin 3's copy-on-write variant is green with the epoch check in. The append arm does not fail:
- `commit_append_with_summary` builds a `merge_append`;
- the stamp-only door builds an empty `merge_append`;
- the fork's retry re-bases either one past a concurrent append and lands it.

## What is already there (read at the pin `076d5f98`)

- **The re-base.** `Transaction::do_commit` reloads, re-bases a stale transaction and re-applies its actions (`crates/iceberg/src/transaction/mod.rs:506`–`:515`). `FastAppendAction::commit` (`append.rs:108`–`:131`) and `MergeAppendAction` validate only their added files. Neither carries `validate_from_snapshot` nor a concurrent-data check.
- **The validation shape to copy.** `OverwriteFilesAction` has `validate_from_snapshot(i64)` (`overwrite_files.rs:270`) and `validate_no_conflicting_data()` (`:242`), and runs them in `validate` (`:443`) before the re-apply.

## Why the validation must sit on the append action itself (DM-6, C-001 (d))

DM-6 measured branch A on the pin: an empty `overwrite_files()` with `validate_from_snapshot(H)` added beside the stamped append. The measurement has three findings:
- It fails the raced commit with `DataInvalid`.
- It refuses every quiet commit with `PreconditionFailed`, because the snapshot is empty.
- With `allow_empty_commit()` added, it never commits. Each snapshot producer in the transaction asserts `main` at the transaction's base, the first moves it, and the second fails `CatalogCommitConflicts` through every retry.

A second snapshot-producing action cannot fence the append. The check has to live in the append's own `validate`.

## The request

`FastAppendAction` and `MergeAppendAction` gain `validate_from_snapshot(i64)` and `validate_no_concurrent_data()`, both off by default.
- **When both are set,** `validate` fails non-retryably if the refreshed base holds an `Append`, `Overwrite` or `Delete` snapshot on the target ref newer than the pinned id.
- **`Replace` is excluded.** A compaction does not change the rows, as on the overwrite arm.
- **The operation stays `Append`.** No second snapshot is produced, and an empty merge append (the stamp-only door) is fenced the same way.
- **The failure is non-retryable,** so the transaction's retry loop cannot re-base past it. It names the pinned id and the newer snapshot.

It is the narrow form of silver-s0's `F-SILVER-PIN-BASE` candidate (`task/ledgers/staging/silver-s0-ledger.md` C-008).

## Pins (fork side)

- **The race pin.** A racer's append between load and commit fails the pinned commit, and nothing lands. It is run on `fast_append`, on `merge_append` with files, and on an empty `merge_append` with a property update in the same transaction (the stamp-only shape).
- **The replace pin.** An unrelated `replace` between load and commit does not fail the pinned commit, which lands one `append` snapshot.
- **The quiet pin.** With no race, the pinned commit lands exactly one `append` snapshot, and so does the empty merge append. This is the case branch A failed.
- **The default pin.** With the option unset, today's re-base still happens.

## Mutation

Dropping the check from the append's `validate` turns the race pin red.

## What RePark does once it merges (MB-2c closing slice, not this card)

1. RP-N repins the fork.
2. `ClaimedStamp::stamp_transaction`'s callers pin the append to `ClaimedStamp::base`.
3. The `#[ignore]` lines on harness pins 2 and 3 are deleted, and the crash gate reads 5 passed.
4. The DM-6 measurements in `crates/repark-iceberg/src/write/sink_offsets_fence_tests.rs` are re-read against the new pin.

Up: [map.md](map.md).
