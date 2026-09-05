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

- [iceberg-catalog-io-baseline.md](iceberg-catalog-io-baseline.md) — **PERF-ICE-CATALOG-IO-1
  (2026-09-05):** the `strace -f -e trace=openat` census per statement (analysis §7.6 reproduced
  exactly as the before column) and the `t_many` / `t_many_merged` cells, before and after the
  session metadata-location cache. `metadata.json` READS fall from 2 (SELECT) and 3–6 (DML) to
  **0 on every statement**; the one remaining open per DML is the commit writing its own pointer,
  which no cache removes. The manifest cells do not move (120.4 → 120.0 ms) and were never going
  to: that cost is 192 manifests re-read through a fresh `ObjectCache` per `Table`, which is
  fork-gated part 3. §3 names the three fork asks (`F-CATIO-A`, `F-CATIO-B`, `F-CATIO-AWS`), what
  each measures, and why the Glue column of the AWS table is argued by the census method rather
  than measured. Both timing columns are the same release module in back-to-back runs with their
  own re-measured floor and recorded load — not a quiet box.
  pins: perf-ice-catalog-io-1/C-001, C-005, C-006

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

## Pointers

- Up: [../map.md](../map.md)
- Harness: [../../python/repark-parity/bench/dynflatten/map.md](../../python/repark-parity/bench/dynflatten/map.md)
