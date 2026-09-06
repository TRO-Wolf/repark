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
- [iceberg-catalog-io-baseline.md](iceberg-catalog-io-baseline.md) — **PERF-ICE-CATALOG-IO-1
  (2026-09-05):** the `strace -f -e trace=openat` census per statement, measured on both knob
  settings, and the `t_many` / `t_many_merged` cells. `metadata.json` READS fall from 2 (SELECT)
  and 3–6 (DML) to **0 on every statement that reads an existing table**; `CREATE TABLE` and CTAS
  read 1 with the cache on AND off, because the catalog reads back the document it just wrote —
  creation is not cacheable and the note says so. §7.6 of the analysis reports TOTAL opens; this
  note splits reads from the commit's own write, and reads + writes reproduce §7.6 exactly. The
  AWS table reads **unchanged today** in both columns: only the memory catalog is wired, and the
  note names the two separate asks that stand between it and a zero (`F-CATIO-AWS` for the S3 GET,
  `F-CATIO-A` for the `GetTable` count no cache can touch). The manifest cells do not move
  (120.4 → 120.0 ms) and were never going to: that cost is 192 manifests re-read through a fresh
  `ObjectCache` per `Table`, which is fork-gated part 3, measured at 11.33 ms through a temporary
  path override. §3 names all four fork asks and what each measures. Both timing columns are the
  same release module in back-to-back runs with their own re-measured floor and recorded load —
  not a quiet box.
  pins: perf-ice-catalog-io-1/C-001, C-005, C-006
  **PERF-ICE-CATALOG-IO-2 (2026-09-05)** appended §5, re-measuring part 3 on the real pin
  (`79119643`, no override) with the manifest knob as the only variable (`0`, the default,
  vs `33554432`, set explicitly): `t_many/count_id/stmt2`
  115.81 → 10.95 ms, repeated reads opening no manifest at all, and the DML scope explained —
  the fork's scan path consults the cache but its transaction/maintenance/inspect paths load
  straight from `FileIO`, so DML saves read-side repeats only (filed `F-CATIO-COMMIT`).
  Earlier tables untouched.
  pins: perf-ice-catalog-io-2/C-006
  **PERF-ICE-CATALOG-IO-3 (2026-09-05)** appended §6, re-measuring on the default
  session (no knob) against explicit `0` on one release module: `t_many/count_id/stmt2`
  123.47 → 11.27 ms (target ≤ 20, within 0.4 ms of IO-2's explicit-knob column on every
  row), the census reproduced cell for cell with no knob set, and 500 small tables at
  332.2 vs 323.9 MB peak RSS (delta 8.3 MB, bar 64 MB). Earlier tables untouched.
  Round 2 (same unit) extended §6.3 with the 2,000/8,000-table peak and VmRSS-growth
  rows (the 32 MiB budget binds estimated weight, not resident bytes — a ~7.5×
  under-count, registry `PERF-CATALOG-CACHE-WEIGHT-1`), added §6.4 with the token-budget
  second-pass cells, and moved Commands to §6.5.
  pins: perf-ice-catalog-io-3/C-006

- [iceberg-scan-baseline.md](iceberg-scan-baseline.md) — **PERF-ICE-SCAN-1
  (2026-09-05):** the `count_star`, `count_id`, `sum_all` and `string_len` cells at 1e6 and
  1e7 before (pinned fork) and after (temporary F-27 path override), each against its own
  re-measured parquet floor. `count(*)` folds: 86.5 → 2.0 ms at 1e6 (parquet 1.8 ms),
  686 → 2.5 ms at 1e7. Full scans go N=1 → N=8 but MISS the 1.5×-of-parquet target
  honestly (1.8×/2.2× at 1e6, 2.4×/3.6× at 1e7); §3 decomposes the residue into ~1 ms
  planning, ~10 ms fixed per query, ~2× per-byte, and a `count(col)` statistics-pushdown
  gap. The DV leg stays unfolded and answers 990,000 in 4.6 ms.
  pins: perf-ice-scan-1/C-008, C-011

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
- [aggregate-baseline.md](aggregate-baseline.md) — **PERF-AGG-AVG-1
  (2026-09-05):** the grouped-`avg` cells before/after the `GroupsAccumulator`
  (`avg`/`sum` by `l_partkey` 4.45× → 1.10–1.28×, TPC-H Q17 13.8–18.3× → 3.6–8.3×
  DuckDB with the ≤ 3× bar missed and the sum-floor unreachability proof), floors,
  machine/profile header, and a reproduce block ending in the committed cost probe.
  Round 2 narrows its `try_avg`-overflow sentence to the 2×-MAX shape, points the
  sum-wrap shape at BACKLOG row `AVG-DEC-SUMWRAP-1`, and discloses the grouped-float
  bit change (`FLOAT-AGG-3`).
  Note this baseline's deviation from the facade precedent: the by-partkey cells run
  from a throwaway script, not a tracked runner — only the Q17 leg (the tracked TPC-H
  runner) and the committed probe re-derive mechanically.
  **PERF-FACADE-CDF-1 (2026-09-05)** appended §4, turning the §3 create controls into a
  before/after pair (1,656.62 → 70.30 ms at 1e5 tuples); earlier tables untouched.
  pins: perf-facade-cdf-1/C-001, C-005

- [spill-matrix-baseline.md](spill-matrix-baseline.md) — **H3-SPILL-1 (2026-09-05):** the Round 3: the concurrency row names lane E (the ten `to_pandas` cells re-run alone).
  **H3-SPILL-RESIDUE-1 (2026-09-06)** rewrote §6 and §7 and annotated §1/§2/§3: both defects
  are fixed, the single `internal_error` cell is `clean_error` 3/3, the 8 MiB column was
  re-run before and after to show the other 17 operators cell-for-cell identical, and the
  `collect` happy path carries five facade-bench medians per module rather than one.
  The 180-cell tables are NOT re-run — they stay the 2026-09-05 measurement, with the one
  moved cell called out where it appears. pins: h3-spill-residue-1/C-001, C-002, C-003
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
