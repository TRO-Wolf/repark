# Card WRITE-ORDER-DROPPED-1: a sorted DataFrame is written in its source order

**Date:** 2026-11-22. **Filed by:** the orchestrator (`claude-opus-5-5`), not dispatched. **Source:** an independent
workload measurement against Spark 4.1.2 (a `sortWithinPartitions` before a single-part Parquet write), widened on
main `c5370be6` by the orchestrator and by a second measurement (Fable). Filed in #1406 as PATH-WRITE-ORDER-DROPPED-1;
renamed because Iceberg writes are affected too. **Severity:** S2 (owner, 2026-11-22, re-rated from S3 on the wider
scope). Not S1: values and row counts are equal, and neither Spark nor Parquet promises a reader the row order.

## The defect

`collect()` of a sorted DataFrame returns the requested order on both engines. Written out, Spark stores the rows in
that order; RePark drops the sort and stores them in the order they were read. A reader that sorts sees no
difference. A reader that uses the file's order (pandas, Polars, a person opening a CSV export) gets unsorted rows,
and a sort meant to cluster files so that readers can skip row groups by their statistics clusters nothing.

## Measured (both ANSI modes, same answers)

Twelve rows stored in descending `k`, sorted ascending before the write, one output file:

| Shape | Spark stored | RePark `c5370be6` stored |
|---|---|---|
| `df.sortWithinPartitions("k").write.parquet(p)` | `1, 2, …, 12` | `12, 11, …, 1` |
| `df.orderBy("k").write.parquet(p)`, also with `.coalesce(1)` | `1, 2, …, 12` | `12, 11, …, 1` |
| `df.orderBy("k").coalesce(1).write.csv(p)`, `.json(p)` | `1, 2, …` | `12, 11, …` |
| `df.orderBy("k").collect()` | `1, 2, …, 12` | equal |

The second measurement found the sort dropped in nine of nine sorted plain Parquet writes (`orderBy` ascending and
descending, `coalesce(1)`, `sortWithinPartitions`, SQL `ORDER BY`, and a sort after an aggregate, a join, a window
and a union), and in two Iceberg writes: `INSERT INTO t SELECT … ORDER BY k` and `writeTo(t).create()` of a sorted
frame. The first measurement found the same on a one-partition frame with duplicate keys and NULLs, ascending,
descending with NULLs last, and ascending with NULLs last.

**Equal on both:** an unsorted write; `orderBy` followed by `partitionBy`, where Spark drops the sort as well; and a
plain `INSERT` into an Iceberg table with a declared write order (`WRITE ORDERED BY`), which RePark sorts by that
order (ICE-SORTED-INSERT-1 and -2, 20 offline pins passing on `c5370be6`).

**Side difference:** the sort after a union was written as two files where Spark wrote one.

Not measured: `saveAsTable`, the SQL door's `INSERT OVERWRITE DIRECTORY`, and several output parts (Spark sorts each).

## Fix direction (seat not located)

The sort survives the collect path and is lost on every write path measured, plain and Iceberg, except the declared
write order, which adds its own sort. The first step is to find whether the planner removes a sort under a sink that
declares no required input order. Pins: each row above, the Iceberg pair, the union's file count, and several output
parts.
