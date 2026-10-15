# Card LOCAL-FS-WRITE-ROOT-1: one DataFrame write widens the SQL policy

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 105 (a).

**Status:** open. Not scheduled.

**Retires:** when a DataFrame write no longer widens the SQL allow-list, or the widening is an explicit, documented opt-in.

**Registry row:** [LOCAL-FS-WRITE-ROOT-1](../../../docs/cutover/production-audit-2026-10-11.md) (dated row 2026-10-15, owner ruling 105 (a)).

## Risk

One DataFrame write to a path makes later SQL writes to that path allowed.

## Sequence

`ReparkSession::note_local_write_root` (`crates/repark-core/src/session/path_write.rs:28`) registers every DataFrame write path as a local warehouse root, so one write to a path makes later SQL writes to that path allowed. It is reached from the binding at `crates/repark-python/src/session.rs:559` and from the DataFrame write path at `python/repark/src/repark/spark/dataframe/writer_readwriter.py:416`. See also the BF-CC2-PYBIND-001 note in `crates/repark-python/src/map.md`. Both cited line numbers were read on main `5462c1b2` and are exact.

## Note

Read in the code, not run. Spark-side behaviour not measured.
