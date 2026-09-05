# map — docs/perf

## Purpose

Committed performance baselines. A number whose environment was not recorded
is not a baseline (H-3). This directory is evidence plus the machine/profile
header; it is not a second measurement convention.

This file closes when the H-3 campaign archives to `docs/history/`.

## Contents

- [engine-iceberg-analysis-2026-09-04.md](engine-iceberg-analysis-2026-09-04.md) — **PERF-ANALYSIS
  (2026-09-04, Fable 5.1 session):** the query-engine + Iceberg-integration performance analysis on
  a release module at 8-thread parity — eleven measured candidates ranked by isolated cost over the
  family floor (facade `collect()` 4,939 ms at 1e6×7; cubic `withColumn` chains 2,376 ms at depth
  100; Iceberg `count(*)` decoding every column 68/357 ms; the fanout splitter, cooperative
  writers, single-partition scans, per-statement manifest reads, `createDataFrame(tuples)`, the
  `avg` UDAF, RANGE frames, catalog round trips), the candidates measured and CLOSED, the
  unmeasured hypotheses, a nine-unit slate in build order, and every command. Raw per-iteration
  timings with start/end load:
  [engine-iceberg-analysis-2026-09-04-numbers.json](engine-iceberg-analysis-2026-09-04-numbers.json).
  Filed with two plural spellings normalised for the typos gate. Units cite a cell of this report and re-run its §6 command before and after.
- [dynamic-flatten-baseline.md](dynamic-flatten-baseline.md) — **PERF-DYNFLATTEN-1
  (2026-09-04):** 1e5 and 1e6 per-fixture wall / RSS / walks, Spark explode wall,
  ratio, row-set equality, and the three H-3 candidate rankings. Release profile
  only: a debug module inverts the ranking, so debug numbers are not a baseline.
  **PERF-DYNFLATTEN-2 (2026-09-04)** appends an "after" section: its own before/after
  pair measured back to back on one quieter host, never overwriting the earlier tables.
  Two runs from different hours are not one table — each carries its own noise floor and
  its own 1-minute load, and a cost is read against the floor of the run it came from.
  pins: perf-dynflatten-1-measure/C-003, C-004

- [iceberg-write-baseline.md](iceberg-write-baseline.md) — **PERF-ICE-WRITEPATH-1
  (2026-09-05):** the `iceberg_write/1000000/{ctas,ctas_partitioned8,df_write_parquet_zstd}`
  cells before and after. §1 names four builds and where each may be quoted: the registry carries
  the SHIPPED pair (base against the branch, both on the pinned fork), and the two builds that
  carry the never-committed fork path override are quoted only in the pending fork row. Carries the build matrix, the fixture, the load at each cell and the commands, plus
  the isolated splitter measurement taken in the fork lane where no RePark rebuild is involved.
  Round 3 adds §7's determinism table — three attempts at the same claim, two refuted — and
  moves the probes into the tracked bench tree.
  pins: perf-ice-writepath-1/C-009, C-010

- [facade-boundary-baseline.md](facade-boundary-baseline.md) — **PERF-FACADE-1
  (2026-09-04):** the `collect()` and `withColumn`-chain cells, produced by the tracked runner
  [python/repark-parity/bench/facade/](../../python/repark-parity/bench/facade/map.md)
  (`make facade-bench`). There is no stale before column: the pre-unit code path is
  reconstructed in the same process on the same release module and timed beside the shipped
  one, so the only variable in each pair is which code runs. The floor is re-measured every run
  as the spread of five repeated medians of one cell, because it is a property of the box that
  hour. Section 2 records the measured ceiling of the projection collapse this unit defers
  (65.04 ms at depth 100 — under the bar, so the deferral rests on correctness, not on the size
  of the prize) and says plainly that the first draft's 140.46 ms was measuring the wrong loop.
  pins: perf-facade-1/C-001, C-006, C-007, C-009

- [spill-matrix-baseline.md](spill-matrix-baseline.md) — **H3-SPILL-1 (2026-09-05):** the Round 3: the concurrency row names lane E (the ten `to_pandas` cells re-run alone).
  Never-OOM truth table. 18 operators x 5 pool sizes (unbounded / 8 GiB / 1 GiB / 256 MiB /
  64 MiB) x 2 scales (1e6 / 1e7 wide rows, so 1e7 exceeds 1 GiB) = **180 cells, each in a fresh
  subprocess on a release module**, classified `ok` / `spilled` / `degraded` / `clean_error` /
  `internal_error`, with peak RSS polled from `/proc`, wall, and the 1-minute load beside each.
  **No cell aborted the process and no cell returned a wrong answer** — 115 of the 144 bounded
  cells carry a content digest and every one equals the unbounded run (163 run digests once the
  repeats are counted, because a repeated cell keeps a digest per run); the 29 without are 28
  refusals plus one probe that exhausted the same pool it was probing. §1.1 names the digest kind
  per operator, because a row count is not an answer check. Three Apache Spark cells on
  the same fixture at `spark.driver.memory=1g` sit beside it, including the one where Spark is
  the worse citizen (`collect_list` dies with a Java OOM that takes the SparkContext down where
  repark refuses with a typed exception, quoted from the JVM's own captured stderr). Section 2
  reads DataFusion 54.1's spill support out of
  the vendored source rather than inferring it, because that table is the ceiling on any
  Never-OOM claim: windows, `Unnest`, the Iceberg scan and the facade boundary take no
  reservation at all, so the pool cannot bound them — though each still returns the identical
  digest at every pool, so what the pool does not bound it also does not corrupt. Per-cell
  evidence including every repeat and the JVM stderr lines:
  [spill-matrix-baseline-cells.json](spill-matrix-baseline-cells.json).
  pins: h3-spill-1/C-002, C-003, C-004, C-005

## Pointers

- Up: [../map.md](../map.md)
- Harness: [../../python/repark-parity/bench/dynflatten/map.md](../../python/repark-parity/bench/dynflatten/map.md)
