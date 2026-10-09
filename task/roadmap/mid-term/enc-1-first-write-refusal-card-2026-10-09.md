# Card ENC-1-FIRST-WRITE-REFUSAL-1: a table that carries `encryption.key-id` refuses its first write

**Date:** 2026-10-09. **Filed by:** Claude (Haiku 5.5), docs lane, from the readiness review of main `40fc916f`.

**Status:** open. Not scheduled. The owner's ruling is made (2026-10-01). This card is the product card the ruling asks for.

**Grade:** clerk, guided by Muse. **Verifier:** Opus.

**Retires:** when the refusal lands on every door in the checklist, the pin named below flips on purpose, and registry row `ENC-1` moves from DECLARED to the refusal.

## Why

The owner ruled on 2026-10-01 (item ES-3): the first write to a table that carries `encryption.key-id` refuses, as Spark does without a KMS. `CREATE` keeps succeeding, as in Spark. The ruling is recorded in two files:

- [contracts-ahead-of-code-2026-10-01.md](../epic-term/contracts-ahead-of-code-2026-10-01.md) line 98 (row ES-3).
- [release-roadmap-2026-08-29.md](../epic-term/release-roadmap-2026-08-29.md) line 365 (the 2026-10-01 row, item 3).

Today the property is stored and never applied. `INSERT` writes ordinary Parquet with no error. The second test in the probe shows the write succeeds (see [the R-007 card](ice-tsns-merge-wall-1-card-2026-10-09.md), Evidence).

The pin `v3_create_with_encryption_key_id_still_scans_without_a_kms` in [crates/repark-spark/src/tests/v3_cow.rs](../../../crates/repark-spark/src/tests/v3_cow.rs) (line 604) encodes the old behavior. It creates the table with the property, runs an `INSERT`, and asserts that the rows stay readable. The refusal makes that assertion false. Flip it on purpose, in the same change as the refusal.

## Registry row

Registry row `ENC-1` is at [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) line 1684, under the heading that starts with `ENC-1` and says that Iceberg table encryption keys are stored, never applied. It reads DECLARED (owner, 2026-08-24). Its last paragraph already records the 2026-10-01 ruling.

When this card lands, the row moves from DECLARED to the refusal. The heading, the repark bullet, the rationale and the pin name change with it. The fork item stays. Envelope encryption is fork work (GAP_MATRIX R130), and the refusal does not change it.

The same status appears in two north-star rows that the landing change also trues up: [v1-0-iceberg-v3-northstar.md](../epic-term/v1-0-iceberg-v3-northstar.md) lines 63 and 95.

## Current behavior

The review's probe has a second test, `encrypted_table_write_must_refuse_without_encryption_support`. It runs `CREATE TABLE` with `encryption.key-id`, then `INSERT`. The write succeeds and the test fails with "write unexpectedly succeeded with encryption requested and no KMS" (the review's log, lines 89-92).

## Refusal shape

Lean, for the executor to confirm: one guard in the shared write entry point, so that a door cannot skip it, plus one pin per door in the checklist. The refusal is a typed error that names `encryption.key-id`. It fires before any file is written, so a refused write leaves the snapshot and the file list unchanged.

## Coverage checklist

Every write and maintenance door named in the tree's maps. Each item needs a pin that fails on main and passes after the refusal.

Spark SQL door (the "Live:" list in [crates/repark-spark/map.md](../../../crates/repark-spark/map.md)):

- [ ] `INSERT INTO ... VALUES`
- [ ] `INSERT INTO ... SELECT`
- [ ] `INSERT OVERWRITE`, whole table (stage-then-swap; [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) line 15)
- [ ] `INSERT OVERWRITE PARTITION`, the dynamic and the static arms (row ICE-V3-WRITE-DEFAULT-1-OVERWRITE-PART in the registry)
- [ ] `MERGE INTO`, the insert and the update arms
- [ ] `UPDATE`, and `DELETE`: the identity path in `predicate_dml`, and the ordinary path wired to the fork's `TableProvider` ([crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) lines 55-56)
- [ ] `CTAS`. It writes the first data file. Lean: refuse, because the ruling's "first write" covers a CTAS data write. `CREATE` alone keeps succeeding.
- [ ] `CREATE OR REPLACE TABLE` and RTAS. Replacing writes data.
- [ ] `TRUNCATE TABLE`. Lean: refuse, because it commits a snapshot.
- [ ] The DML passthrough named in the "Live:" list.
- [ ] Writes to a branch or a tag, and WAP stage-only writes (`writeTo(...).option("branch", ...)` and `spark.wap.branch`), per the branch and tag entry in the same map.

DataFrame and native doors:

- [ ] `writeTo(...).append()`, `writeTo(...).overwrite...`, `saveAsTable`, `insertInto`, and path writes ([python/repark/map.md](../../../python/repark/map.md) line 52)
- [ ] The bulk `append` and the stage-then-swap overwrite in the Rust crate ([crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) lines 14-15)

Maintenance doors, the `CALL` procedures named in [crates/repark-spark/src/call/params.rs](../../../crates/repark-spark/src/call/params.rs) (lines 475-490 and 548-563):

- [ ] `rewrite_data_files`. It writes new data files, so it is the door the review named.
- [ ] `rewrite_position_delete_files`
- [ ] `rewrite_manifests`. Spark encrypts manifests too (the registry row's Spark bullet).
- [ ] `rewrite_table_path`. It writes metadata files.
- [ ] `add_files`. It writes manifests over existing files.
- [ ] `expire_snapshots`. Lean: refuse, because it commits a snapshot.
- [ ] `remove_orphan_files`. It deletes only. Lean: refuse, so that no door on the table writes or deletes under the property. The owner may carve out delete-only doors.
- [ ] `run_maintenance` ([crates/repark-spark/src/call/run_maintenance_apply.rs](../../../crates/repark-spark/src/call/run_maintenance_apply.rs)), the maintenance policy door
- [ ] The routed plan and apply paths: `rewrite_data_files`, `rewrite_manifests` and `expire_snapshots` in [crates/repark-spark/src/call/apply_partitioning.rs](../../../crates/repark-spark/src/call/apply_partitioning.rs) (lines 149-151) and [crates/repark-spark/src/call/plan_partitioning.rs](../../../crates/repark-spark/src/call/plan_partitioning.rs) (lines 530-532)

## Gates

- Each checklist door has a red pin on main and a green pin after the refusal. The pin reads the typed error and the unchanged snapshot and file list.
- The flipped pin `v3_create_with_encryption_key_id_still_scans_without_a_kms` now asserts the refusal, with a new name.
- `CREATE` with the property still succeeds, and that is pinned.
- The probe's second test passes.
- Each door with a native and a SQL form has a pin on each entry point, on the Arrow path (AGENTS.md, "Hard rules": the entry-point matrix).

## Done when

- Registry row `ENC-1` reads as the refusal, with its pin.
- No door writes plain Parquet into a table that carries the property.
- Every line of the coverage checklist is pinned, or the owner has carved it out in a dated ruling.

## Pointers

- The ruling: [contracts-ahead-of-code-2026-10-01.md](../epic-term/contracts-ahead-of-code-2026-10-01.md) line 98; [release-roadmap-2026-08-29.md](../epic-term/release-roadmap-2026-08-29.md) line 365.
- The registry row: [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) line 1684.
- The pin: [v3_cow.rs](../../../crates/repark-spark/src/tests/v3_cow.rs) line 604. Its original ledger: [2026-08-24-v3e-1-2-cow-oracle-ledger.md](../../ledgers/archive/2026-08/2026-08-24-v3e-1-2-cow-oracle-ledger.md) (C-009).
- The sibling card for the probe that shows the write succeeds: [ice-tsns-merge-wall-1-card-2026-10-09.md](ice-tsns-merge-wall-1-card-2026-10-09.md).
