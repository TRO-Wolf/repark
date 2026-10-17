# Card ENC-CTAS-REPLACE-1: a key landing mid-statement leaves an orphan unencrypted file

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 98 (a).

**Round 2 (2026-10-15, owner ruling 108), verbatim:** "108: (a). ENC-CTAS-REPLACE-1 names both doors. execute_staged_create and publish_staged_with_summary go on the census exception list under that card, the census commits, round 2 is complete."

**Status:** open. Not scheduled. Needs an owner call: close, or carve out. One defect, two spellings. The Spark SQL door is `crates/repark-spark/src/ctas.rs::finish_ctas_staged_commit`. The ANSI door is `crates/repark-sql/src/create_table.rs::execute_staged_create` and `publish_staged_with_summary`. The census excepts all three under this card. Since owner ruling 121 (2026-10-17) the census is a type the compiler checks, and the exception is `#[expect(clippy::disallowed_methods, reason = "ENC-CTAS-REPLACE-1")]` on each of the three functions: they still add staged files and publish outside `crates/repark-iceberg/src/write/staged_commit.rs`.

**Retires:** when the staged files are removed on that failure, or the publish refuses before staging, with a pin that a key landing mid-statement leaves no unencrypted file.

**Registry row:** [ENC-CTAS-REPLACE-1](../../../docs/spark-sql-iceberg-parity.md) (dated row 2026-10-15, owner ruling 98 (a); ruling 108 names both doors).

## Risk

An orphan unencrypted file under a keyed table, never served.

## Sequence

`CREATE OR REPLACE TABLE … AS SELECT` on the Spark SQL door: `begin_replace` (`crates/repark-spark/src/ctas.rs:304`) loads the table before the key exists, so the staged table still carries the old unkeyed properties and a plain `FileIO`. Staging then writes an unencrypted Parquet file (`finish_ctas_staged_commit`, `crates/repark-spark/src/ctas.rs:328`; fast path line 339, override path lines 353–373; applied locally line 395). The guard's `refuse_keyed_publish` (`crates/repark-iceberg/src/catalog/encryption_guard.rs:133`, called at line 449) checks the staged (unkeyed) table and passes. The inner `publish_replace_table` (called at `ctas.rs:403–408`) then fails on the moved base metadata location (a `CommitFailed`, not a keyed refusal). Nothing removes the staged file. Result: an orphan unencrypted file under the keyed table's `data/`, referenced by no snapshot, so never served. Main behaves the same. (Line numbers read on main `27c56f74`; the brief's "341 and 379–395" are corrected here.)

## ANSI door

The same defect under the other spelling. `execute_staged_create` stages files on a staged create or replace (`add_data_files`, then `commit`, or `publish_staged_with_summary`). `publish_staged_with_summary` stages (`add_files` or `add_data_files`) and then calls `publish_create_table` or `publish_replace_table`. Nothing sweeps those files. A key that lands between the staged table's load and that publish leaves the same orphan: an unencrypted file under the keyed table's `data/`, referenced by no snapshot, so never served.

## Note

Read in the code, not run. Spark-side behaviour not measured.
