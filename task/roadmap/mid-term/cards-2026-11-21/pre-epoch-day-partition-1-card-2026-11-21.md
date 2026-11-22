# PRE-EPOCH-DAY-PARTITION-1 — a pre-epoch `day` partition value differs from Spark's, and RePark's reader misses a range over its own file

**Rating: proposed S1 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-fork-2`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21. The replay measured the read impact that the audit did not.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S1 (proposed).** Wrong partition metadata, plus a measured silent miss in RePark's own reader.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Spark.** The partition for the row is `ts_day=1969-12-30`.
- **[M] RePark.** The partition is `ts_day=1969-12-31`. A cold Spark read of `.files` confirms it.
- **[M] Controls.** Equality and an unfiltered read find the row on both engines, as the audit said.
- **[M] Spark reading RePark's file.** Spark finds the row for `<`, `>=`, `BETWEEN` and CAST-to-DATE. Java widens its pre-epoch projection.
- **[M] RePark reading its own file.** Through `register_table`, RePark returns `[]` for `ts BETWEEN '1969-12-30 00:00' AND '1969-12-30 23:59:59.999999'`. The same read on Spark's file returns `[1]`.

## Why S1

The partition metadata is wrong, and RePark's own reader silently drops the row for a range over the true day. The audit measured only equality and cold Spark reads, so it reported no read impact; the replay measured one.

## Scope

The pre-epoch day transform on write, and RePark's read of a pre-epoch day partition it wrote. The replay's day-read probe files (basenames) are `day_prune.py`, `day_prune_adopt.py`, `day_prune_repark.py` and `cold_read.py`; the verdict does not map each probe to a cell.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
