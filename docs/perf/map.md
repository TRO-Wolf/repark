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
  **DYNFLATTEN-LISTNULL-1 (2026-09-06)** appends a third note: the live `read.parquet`
  path now matches Spark on the void-list shapes; the bench `createDataFrame` path and
  the numbered tables are untouched.
  pins: perf-dynflatten-1-measure/C-003, C-004
  pins: dynflatten-listnull-1/C-005

- [iceberg-write-baseline.md](iceberg-write-baseline.md) — **PERF-ICE-WRITEPATH-1
  (2026-09-05):** the `iceberg_write/1000000/{ctas,ctas_partitioned8,df_write_parquet_zstd}`
  cells before and after. §1 names four builds and where each may be quoted: the registry carries
  the SHIPPED pair (base against the branch, both on the pinned fork), and the two builds that
  carry the never-committed fork path override are quoted only in the pending fork row. Carries the build matrix, the fixture, the load at each cell and the commands, plus
  the isolated splitter measurement taken in the fork lane where no RePark rebuild is involved.
  Round 3 adds §7's determinism table — three attempts at the same claim, two refuted — and
  moves the probes into the tracked bench tree. **WRITE-DISTRIBUTION-1 (2026-09-06)** appends
  §8: the hash distribution rule's before/after pair on one box with the same native swapped, the
  partitioned cell 64 → 8 files and 3.44× → 1.96× of the control, its RSS peak, and the live
  Spark layout; §6 gains the shipped row.
  **WRITE-DISTRIBUTION-2 (2026-09-06)** appends §9: the stream-paths pair (overwrite 32 → 8
  files, 8.59× → 7.38× of the control; merge 32 → 8; INSERT INTO and append stay at 64, fork),
  the live Spark 8-file bed for all four statements, and the F-4 `_row_id`-map counts in §8;
  §6 gains the stream rows.
  pins: perf-ice-writepath-1/C-009, C-010
  pins: write-distribution-1/C-007
  pins: write-distribution-2/C-003
  **WRITE-ORDER-DIST-1 (2026-09-06)** appends §10: the distribution-mode gate and the per-writer
  sort cost — no overhead on the unordered pair (control ratios overlap), +71 ms best-median on
  the 1e6 ordered overwrite, the new `insert_overwrite` / `insert_overwrite_ordered` cells, and
  the live Spark row set per partition value. The pair predates the WRITE-DISTRIBUTION-2 merge;
  the merged tree routes the stream cells to 8 files (§9).
  pins: write-order-dist-1/C-008, C-011
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
  rows (the 32 MiB budget then bound estimated weight, not resident bytes — a ~7.5×
  under-count, registry `PERF-CATALOG-CACHE-WEIGHT-1`), added §6.4 with the token-budget
  second-pass cells, and moved Commands to §6.5.
  **RP-16 (2026-09-11):** §6.3 closes `PERF-CATALOG-CACHE-WEIGHT-1` FIXED at pin
  `090bc821` (fork `#274`): charged object-graph weight, measured retaining
  1,071,000 / evicting 1,070,000, retain pin 1,250,000, old 280000 now evicts.
  pins: perf-ice-catalog-io-3/C-006
  pins: rp-16/C-002, C-003

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
  moved cell called out where it appears. **Round 2 (2026-09-06)** added three honest limits §6
  and §1 did not carry: the containment's fourth gate and why three were not enough, that the
  refusal log is session-scoped rather than per-stream (and cannot be), the four stderr panic
  blocks a contained refusal still prints, and — outside the 180 cells — `toPandas()` under a
  64 MiB address-space headroom aborting the process 3/3 where `collect()` raises `MemoryError`
  3/3 at the identical ceiling. pins: h3-spill-residue-1/C-001, C-002, C-003
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
- [spill-coverage-matrix-2026-09-11.md](spill-coverage-matrix-2026-09-11.md) — **NEVEROOM-1
  steps 2–3 (2026-09-11):** the full-tier D-1 × D-3 run — 9 operators × 2×/4×/8× of a
  1 GB FairSpillPool × 3 repetitions, each cell a subprocess under
  `RLIMIT_AS = VmSize_at_apply + 3 × limit` (S2-8), `target_partitions=4`,
  `batch_size=8192`. 24 of 27 cells fold to a stable three-outcome state;
  `hash_join-4x` is `KILLED` 3/3 (an un-accounted ~35 MB allocation aborts
  under the cap — the no-spill-path boundary, upstream epic
  datafusion#24768), `hash_aggregate-2x` and `window_unbounded-2x` are
  `UNSTABLE` (refuse↔spill and refuse↔kill races; #22758 named on the
  `hash_aggregate` 2× CSV/`md` notes in step 3). Both H3-SPILL fixed
  outcomes reproduce: `nested_loop_join` refuses naming
  `NestedLoopJoinLoad` with the contained panic in stderr, `collect`
  refuses as `MemoryError`. `## Never-OOM at v1.3` records S2-18: the
  claim is the matrix as measured; a DataFusion bump that closes #24768
  or #22758 re-runs `matrix_run.py --reps 3` as its pin. Machine CSV:
  [spill-coverage-matrix-2026-09-11.csv](spill-coverage-matrix-2026-09-11.csv).
  Harness: [../../python/repark-parity/tests/spill/map.md](../../python/repark-parity/tests/spill/map.md).
  pins: neveroom-1/C-005, C-006, C-007, C-008, C-009, C-010
- [approx-percentile-baseline.md](approx-percentile-baseline.md) — **PERF-APPROXPCT-1
  (2026-09-05):** the `percentile_approx` cells before/after the Greenwald-Khanna
  sketch (1e7 wall 2.95 → 0.14 s, peak 2507.8 → 752.9 MB against a 188.6 MB
  `count(id)` floor; warm-1e6 wall 0.03 s under the committed 1.0 s bar), the
  sublinear residual attributed to transient batches (inferred), state sizes
  (952656/4776/72 B at acc 10000/100/2), and the accuracy-knob cells pointed at
  the unit ledger §4, not restated. **Round 2 (2026-09-06):** the after column is
  re-derived through the tracked
  [approxpct harness](../../python/repark-parity/bench/approxpct/map.md); the before
  column stands recorded (no second release build of the pre-unit tree).
  **Round 3 (2026-09-06):** the 1e7 attributable 462 → 564 MB is the deferred
  canonical fold's cost, not a re-derivation of the same kernel.
  pins: perf-approxpct-1/C-004
  **DFCORE-5 (2026-09-07)** appends §"DataFrame.approxQuantile": the
  `approxQuantile` before/after pair (2x3 collects 6 → 1, medians 0.155 →
  0.052 s; 4x5 collects 20 → 1, medians 0.572 → 0.087 s; values identical),
  recorded on one release module with per-run loads, wall not gated.
  pins: dfcore-5/C-005
- [csv-infer-baseline.md](csv-infer-baseline.md) — **CSV-INFER-PERF-1 (2026-09-06):**
  local CSV `inferSchema` before/after on a 300k × 8 file (True 2.339 s → 0.155 s,
  plan-time `to_arrow` 34 → 1, True/False **2.01×**). Leftover numeric grammar at
  every width; `multiLine` re-read is infer-free. `nullValue` keeps one `try_cast`
  aggregation.
  pins: csv-infer-perf-1/C-001, C-005, C-006
- [eager-preview-baseline.md](eager-preview-baseline.md) — **PERF-EAGER-PREVIEW-1
  (2026-09-07):** the repr/HTML/vertical-show before/after pair on a 1e6-row
  parquet read and a `mapInArrow`-backed frame (`count()` 1 → 0 on every plain
  preview door; bridged eager doors 2,000,000 → 65,536 computed UDF rows and
  0.821/0.845 → 0.031/0.031 s; bridged vertical show unchanged as the
  control), recorded on one release module with per-run loads, wall not gated.
  pins: dfcore-6/C-005

- [profiles-1-passthrough-probe-2026-09-09.md](profiles-1-passthrough-probe-2026-09-09.md) — **PROFILES-1
  step 0 (2026-09-09):** the `.config()` pass-through probe over the card's twenty
  candidate knobs (twelve read, eight write) — one row per key with set / read-back /
  engine-signal / verdict columns, plan and file-fact fragments quoted from the captured
  output, the gating answer (yes: 11 PASSES THROUGH, 9 ACCEPTED BUT UNREAD, 0 REFUSED, 0
  NOT MEASURED), and the reproduce commands. Runnable method kept beside it in
  [profiles-1-probe/](profiles-1-probe/map.md). Step 0's record only; the unit's ledger
  is born in step 1.
  **REVIEW-FIX-8 (2026-09-11)** makes the probe re-runnable (unique temporary directory
  per run, `REPARK_CONFIG=""`, the work prefix scrubbed to `<work>`, two outputs byte
  for byte identical) and trues the table: `VALIDATED` state for the three `repark.*`
  session keys, the two `write.*` rows re-described as table properties with this
  round's measurements, counts re-derived as 13 PASSES THROUGH, 3 VALIDATED,
  4 ACCEPTED BUT UNREAD, 0 REFUSED, 0 NOT MEASURED. The four remaining UNREAD rows are
  CONF-UNREAD-1's.
  pins: review-fix-8/C-001, C-002, C-003, C-004, C-005
  **CONF-UNREAD-1 (2026-09-11)** resolves the four UNREAD rows: both tables gain
  an `after CONF-UNREAD-1` column — `enable_page_index`, `bloom_filter_on_read`
  and `write_batch_size` are pinned reaching the scan-source / writer options
  (PASSES THROUGH), `coalesce_batches` is REFUSED loud at build and at runtime
  `conf.set` (DataFusion 54.1.0 defines the option but no engine path reads it)
  — and §5's counts re-derive to 16 PASSES THROUGH, 3 VALIDATED, 0 ACCEPTED BUT
  UNREAD, 1 REFUSED, 0 NOT MEASURED.
  pins: conf-unread-1/C-006
- [config-profiles-2026-09-12.md](config-profiles-2026-09-12.md) — **PROFILES-1 step 2
  (2026-09-12):** the full-scale knob sweep on a release module — baseline plus one
  table per knob (19 swept, `coalesce_batches` refused loud so not read, R-16),
  value × query → median of three repetitions with ratio to `@default`, the argmax
  row per knob, the affected-cells reading of the card's "its query" and the
  control-spelling rule, and the "no effect measured" list (5 knobs flat within
  5 % on the cells they can reach — `bloom_filter_on_write`,
  `max_row_group_size`, `write_batch_size`, `scan.concurrency_limit`,
  `target-file-size-bytes`). Every number recomputes from the committed CSVs in
  [config-profiles-2026-09-12/](config-profiles-2026-09-12/map.md) — the C-008
  pin parses the tables and re-derives each cell.
  pins: profiles-1/C-006, C-007, C-008
- [ap-0-partition-candidates-2026-09-10.md](ap-0-partition-candidates-2026-09-10.md) —
  **AP-0 (measure, 2026-09-10):** the ADAPT-PART candidate measurement — three local
  Iceberg beds (the futures frame CTAS unpartitioned, two generated 400k-row beds at
  uniform and skewed group shares, 206 files each) scored from `files`/`partitions`
  manifest bounds at a 512 KiB target: one ranked P-2/P-3 table per bed, the sampled
  columns, and the plausibility read. Runnable method kept beside it in
  [../../python/repark-parity/bench/adaptpart/map.md](../../python/repark-parity/bench/adaptpart/map.md).
  The 20 percent prediction check is open (orchestrator O-run).
  pins: ap-0/C-001, C-002, C-003, C-004
- [torture-1-2026-09-11.md](torture-1-2026-09-11.md) — **TORTURE-1 step 5
  (2026-09-11):** the full-tier results table — eight families at 1 000 000 rows
  (`v3_dv`: 800 000 live after the `id % 5 = 2` delete, generated live under
  `REPARK_PARITY_LIVE=1` on zulu-17) × both doors on a release module, one row per
  family × door with rows, harness wall seconds and outcome; the same 11-xfail
  inventory as CI tier and no new registry rows. Also carries the S2-12
  measurement: per-`datasets/` coverage tables and consumer lists — zero
  retirements (`nested`/`secrets` covered but consumed outside their own tests;
  `schema_inference`/`extreme_types`/`smartcsv` partly covered).
  pins: torture-1/C-031, C-032, C-033, C-034, C-036
- [adapt-part-ap1-2026-09-11.md](adapt-part-ap1-2026-09-11.md) — **AP-1 step 2
  (measure, 2026-09-11):** `CALL plan_partitioning` run against the same three beds
  on the release module — per-bed `byte_ratio` measured from parquet footers and
  independently recomputed (futures 0.462675 on zstd CTAS files; uniform and skewed
  1.0 on uncompressed INSERT files — the write-path codec split is named), the top
  three frame rows verbatim per bed, and the projection re-read against AP-0's O-run
  actuals: still +76.2 % / +88.0 % on the synthetic beds, filed as residue
  AP-1-R-001 with its mechanism (the footer ratio reads the input codec; the rewrite
  changed codec).
  pins: ap-1/C-012
- [adapt-part-ap1-remeasure-2026-09-11.md](adapt-part-ap1-remeasure-2026-09-11.md) —
  **AP-1 RP-16 re-measure (2026-09-11):** the three AP-0 beds rebuilt on the
  release module at fork pin `090bc821`. INSERT data files on uniform and skewed
  now carry zstd (independent footer recompute: codec ZSTD, `byte_ratio`
  0.376076 vs AP-1 step 2's uncompressed 1.000000). A same-session
  `ADD PARTITION FIELD identity(grp)` plus `rewrite_data_files` on fresh copies
  measured live actuals 7 928 680 / 7 672 169 (20 files, UNCOMPRESSED). The
  honest 20 percent check is −85.4 % / −84.9 % against those actuals (stale
  AP-0 CTAS actuals −73.8 % / −72.0 % kept for continuity); residue AP-1-R-001
  stays OPEN. No Rust or Python source changed.
  pins: ap-1-remeasure/C-001, C-002, C-003, C-004
- [adapt-part-ap1-remeasure-2-2026-09-12.md](adapt-part-ap1-remeasure-2-2026-09-12.md) —
  **AP-1 RP-17 re-measure (2026-09-12):** the third re-measure, on fork pin
  `41e25ba2` (`#278` F-WRITE-COMPRESS-2 — the maintenance / COW / MoR rewrite and
  position-delete writers honour `write.parquet.compression-codec`). Same three
  beds, same `identity(grp)` candidate, live `rewrite_data_files` actuals whose
  output footers are now zstd; both projections (stored-bytes × ratio and
  uncompressed-sum × ratio) sit beside the actual so the orchestrator can rule
  deferred question Q-1; the 20 % check answers AP-1-R-001 as measured. No Rust
  or Python source changed.
  pins: rp-17/C-002, C-003

## Pointers

- Up: [../map.md](../map.md)
- Harness: [../../python/repark-parity/bench/dynflatten/map.md](../../python/repark-parity/bench/dynflatten/map.md)
