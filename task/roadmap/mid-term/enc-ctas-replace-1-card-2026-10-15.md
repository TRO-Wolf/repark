# Card ENC-CTAS-REPLACE-1: a key landing mid-statement leaves an orphan unencrypted file

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 98 (a).

**Status:** open. Not scheduled. Needs an owner call: close, or carve out.

**Retires:** when the staged files are removed on that failure, or the publish refuses before staging, with a pin that a key landing mid-statement leaves no unencrypted file.

**Registry row:** [ENC-CTAS-REPLACE-1](../../../docs/spark-sql-iceberg-parity.md) (dated row 2026-10-15, owner ruling 98 (a)).

## Risk

An orphan unencrypted file under a keyed table, never served.

## Sequence

`CREATE OR REPLACE TABLE … AS SELECT` on the Spark SQL door: `begin_replace` (`crates/repark-spark/src/ctas.rs:304`) loads the table before the key exists, so the staged table still carries the old unkeyed properties and a plain `FileIO`. Staging then writes an unencrypted Parquet file (`finish_ctas_staged_commit`, `crates/repark-spark/src/ctas.rs:328`; fast path line 339, override path lines 353–373; applied locally line 395). The guard's `refuse_keyed_publish` (`crates/repark-iceberg/src/catalog/encryption_guard.rs:133`, called at line 449) checks the staged (unkeyed) table and passes. The inner `publish_replace_table` (called at `ctas.rs:403–408`) then fails on the moved base metadata location (a `CommitFailed`, not a keyed refusal). Nothing removes the staged file. Result: an orphan unencrypted file under the keyed table's `data/`, referenced by no snapshot, so never served. Main behaves the same. (Line numbers read on main `27c56f74`; the brief's "341 and 379–395" are corrected here.)

## Note

Read in the code, not run. Spark-side behaviour not measured.
