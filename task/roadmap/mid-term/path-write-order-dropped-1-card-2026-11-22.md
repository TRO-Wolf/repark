# Card PATH-WRITE-ORDER-DROPPED-1: a sorted DataFrame is written to a plain path in its source order

**Date:** 2026-11-22. **Filed by:** the orchestrator (`claude-opus-5-5`), not dispatched. **Source:** an independent
workload measurement against Spark 4.1.2 (a `sortWithinPartitions` before a single-part Parquet write); widened
here on main `c5370be6`. **Severity:** S3 (owner, 2026-11-22), rated on the `sortWithinPartitions` witness. The
measured scope below is wider (`orderBy`, CSV and JSON), so the rating is asked again.

## The defect

`collect()` of a sorted DataFrame returns the requested order on both engines. Written to a plain path, Spark stores
the rows in that order; RePark stores them in the order they were read, as if the sort were not there. Values and
row counts are equal, so a reader that sorts, or a comparison that sorts first, sees no difference. A reader that
uses the file's order (pandas, Polars, a person opening a CSV export) gets the unsorted rows.

## Measured (twelve rows stored in descending `k`, sorted ascending before the write, one output file)

| Shape | Spark stored | RePark `c5370be6` stored |
|---|---|---|
| `df.sortWithinPartitions("k").write.parquet(p)` | `1, 2, …, 12` | `12, 11, …, 1` |
| `df.orderBy("k").write.parquet(p)` | `1, 2, …, 12` | `12, 11, …, 1` |
| `df.orderBy("k").coalesce(1).write.parquet(p)` | `1, 2, …, 12` | `12, 11, …, 1` |
| `df.orderBy("k").coalesce(1).write.csv(p)` | `1, 2, …` | `12, 11, …` |
| `df.orderBy("k").coalesce(1).write.json(p)` | `1, 2, …` | `12, 11, …` |
| `df.orderBy("k").collect()` | `1, 2, …, 12` | equal |

The independent measurement also found the same on a one-partition frame with duplicate keys and NULLs, for
ascending, descending with NULLs last, and ascending with NULLs last.

Not measured: Iceberg table writes, `saveAsTable`, the SQL door's `INSERT OVERWRITE DIRECTORY`, and inputs read from
several files.

## Fix direction (seat not located)

The sort is applied on the collect path and lost on the write path; the first step is to find whether the planner
removes a sort under a sink that declares no required order, or whether the writer reads the unsorted input. Pins:
each row above, plus multiple output parts (each part sorted, as Spark writes it).
