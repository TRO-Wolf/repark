# F-COMMIT-OFFSET-PROPERTY-1: a table-property update committed with a row delta or overwrite survives the commit-retry path (fork card)

**Filed:** 2026-10-05. This is the micro-batch sink's second fork request (owner, 2026-10-05: "Fork requests file today").

**Target:** the owned `iceberg-rust` fork. If the pin goes red, a fix lands and RP-N repins; if it is green, only a test lands.

**Consumer:** MB-2a, the sink and offsets, and MB-2c, replay and reconcile. See [the plan](../epic-term/microbatch-cdc-sink-plan-2026-10-04.md), D-1 and D-4.

**Severity:** P2. MB-2a's exactly-once claim stands on it.

## What D-1 needs

A micro-batch commit into Silver does two things in the same catalog commit:
1. It stamps the query id, the batch epoch and the source snapshot vector into the snapshot summary.
2. It writes the same offset vector as a table property, so resume is one read.

## What is already there (read at the pin `e1d74bef` and on RePark `main`)

- **Summary properties on both MERGE arms.** `RowDeltaAction::set_snapshot_properties` (`crates/iceberg/src/transaction/row_delta.rs:249`) and `OverwriteFilesAction::set_snapshot_properties` (`overwrite_files.rs:233`) exist. RePark's MERGE already passes its summary extras on the copy-on-write arm (`crates/repark-iceberg/src/write/merge/snapshot_commit.rs:181`, `:201`) and on the merge-on-read arm (`:415`). The summary half of D-1 needs no fork change.
- **Several actions in one commit.** A `Transaction` holds several actions (`crates/iceberg/src/transaction/mod.rs:145`), and `update_table_properties()` (`:216`) is one of them. So one commit can, in principle, carry the row delta (or the overwrite) together with the property update.

## The request: prove the combined commit, fix it only if it fails

The request is a fork-side, red-first test of the combined commit under concurrency. Under the transaction's retry, the commit re-bases on a catalog refresh and re-applies its actions (`mod.rs:398`, `:513`). The test must show three things.

1. **One commit.** A transaction of `row_delta()` (or `overwrite_files()`) plus `update_table_properties()` produces one new metadata version, carrying both the snapshot with its summary properties and the property.
2. **The retry and rebase path.** A concurrent writer commits an unrelated append between the load and the commit, so the transaction retries. The retried commit still carries the summary properties and the property, both exactly once.
3. **No silent overwrite.** If the concurrent writer changed the same property key, the retried commit must not clobber it without notice. Either the transaction refuses (preferred: a typed conflict error naming the key), or the card records that the fork has no property-level requirement. In the second case MB-2c's generation fencing (CC-9) does the check in RePark, before commit.

If (1) or (2) fails, the fix lands with the test. (3) is a measurement that MB-2c's design reads.

## Pins (fork side)

- The combined-commit pin.
- The retry pin.
- The same-key race pin.

Each records the metadata versions it observed. A mutation that drops the property action from the re-applied list turns the retry pin red.

Up: [map.md](map.md).
