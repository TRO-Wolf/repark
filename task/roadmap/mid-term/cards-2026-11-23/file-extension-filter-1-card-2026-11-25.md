# Card FILE-EXTENSION-FILTER-1: Parquet directory reads skip files whose names do not end in `.parquet`

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from finding C2 of the first verifier of
PARQUET-PARTITION-DISCOVERY-1 (PR #1430), re-measured here. **Severity:** S1 (owner, 2026-11-25): rows go missing
silently. **Status:** filed, not dispatched.

## Measured

Measured on live Spark 4.1.2 and on a build of main `14d2404a6` (#1429's tested head), in both ANSI modes, with the
same answers in each. Every file is a valid Parquet file with one `id bigint` column.

| Directory | Spark 4.1.2 | Main |
|---|---|---|
| `flat/a.parquet` and `flat/b` | ids `10, 11`; `count()` 2 | **id `10`; `count()` 1** |
| `p=1/part-00000`, `p=2/part-00000.parquet`, `p=3/part-00000.snappy.parquet`, `p=4/data.bin` | ids `1, 2, 3, 4`, with `p int` | **ids `2, 3`**, without `p` |

Spark lists every file except those whose names start with `_` or `.`, whatever the extension. The verifier also
measured an upper-case `.PARQUET` skipped on main where Spark reads it. The missing `p` column is
PARQUET-PARTITION-DISCOVERY-1 (#1430), which keeps the same extension filter.

## Seat

- `crates/repark-core/src/spark_nullable.rs:48,60`: `ParquetReadOptions::default()`, whose file extension is
  `.parquet`, so the listing drops every other name.
- #1430's partition seat filters by the same suffix; its verifier names `partition.rs:755-757` on that branch.

## First step

Measure the extension rule over a directory, a glob and a list of paths, with compressed names, upper-case names, no
extension, and hidden names, on the Spark door and the native door. Merge after #1430, which changes the same listing.

Up: [map.md](map.md).
