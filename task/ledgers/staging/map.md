# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [cfg-1-ledger.md](cfg-1-ledger.md) —
  **CFG-1 step 1 (2026-09-09), in flight:** `repark.toml` discovery, profile merge and
  `${VAR}` interpolation — `discovery.rs` (`$REPARK_CONFIG` → `./repark.toml` →
  `~/.config/repark/repark.toml`, empty disables, named-but-missing refuses), `profile.rs`
  (`[default]` + `[<profile>]` deep merge, unknown keys refuse with the key path),
  `interpolate.rs` (missing variable refuses naming path and variable). All six clauses
  PROVEN, 24 pins in `config_file/tests.rs`; `sources.rs` / `redact.rs` stay placeholders
  for step 2. `risk_tier: standard`. Branch `feat/cfg-1`.
  pins: cfg-1/C-001, C-002, C-003, C-004, C-005, C-006
- [ex-29-class-remainder-ledger.md](ex-29-class-remainder-ledger.md) —
  **EX-29 (2026-09-11), in flight:** the v1.1 example backfill's class-surface
  remainder — the 29 non-`F.*` backlog names at base `a10062b8`. Re-measured on
  live PySpark 4.1.2 (ANSI on, UTC, zulu-17): zero names coverable, 23 stay with
  their existing §7 rows (EX-DF-1/2/3/4/7/8/17/19, EX-CAT-1/2, EX-W2-1,
  EX-IO-7), the six `Column.*` plumbing names stay pending an owner ruling on
  inventory narrowing. Pin gaps filled: `get_database` / `list_databases` snake
  legs, the `describe` string-column raise, the `colRegex` multi-match arm.
  `risk_tier: standard`. Branch `docs/ex-29-class-remainder`.
  pins: ex-29-class-remainder/C-001, C-002, C-003, C-004, C-005, C-006
- [ex-30-functions-remainder-ledger.md](ex-30-functions-remainder-ledger.md) —
  **EX-30 (2026-09-11), in flight:** the v1.1 example backfill's `F.*` remainder —
  the 99 `F.*` backlog names at base `f413241b`. Measured on live PySpark 4.1.2
  (ANSI on, UTC, zulu-17): 9 covered by two new `docs/examples/functions/` scripts
  (`bitmap.py`, `udf.py`), 89 stay with their §7 rows (87 pre-existing plus
  `from_xml` / `schema_of_xml` under the new EX-FN-22), `F.PythonUDFColumn` stays
  pending an owner ruling on inventory narrowing. New rows EX-FN-22 (XML E1 stubs)
  and EX-FN-23 (the `udf` / `pandas_udf` factory return-type arm, BACKLOG ARM on
  covered names); pins in `test_examples_functions_b.py`. Backlog 128 → 119.
  `risk_tier: standard`. Branch `docs/ex-30-functions-remainder`.
  pins: ex-30-functions-remainder/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
- [sepmo-e0-e1-ledger.md](sepmo-e0-e1-ledger.md) —
  **SEPMO-E0E1 (2026-09-06), in flight, round 3:** telemetry inventory (E-0) and usage
  collector (E-1). Minority truncated JSONL and exit-without-terminal are degraded
  records; majority-bad still fails. Muse tokens come from the session store
  (`runs.tsv` join, `.msp-view-v1` pinned); cost is still absent. Grok live keys
  include `cache_read_input_tokens` and `modelUsage`. OpenCode sqlite has token
  and cost columns. Claude transcripts are not accessible. Collector is
  `scripts/sepmo_usage.py`. `risk_tier: standard`. Branch
  `sepmo/e0-e1-usage-collector`.
  pins: sepmo-e0-e1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [sepmo-e2-ledger.md](sepmo-e2-ledger.md) —
  **SEPMO-E2 (2026-09-06), in flight, round 3:** compact role packets. Packet
  format v1 (eight field groups, stable prefix then dynamic, source identity,
  version), assembler `scripts/sepmo_packet.py` plus
  `scripts/sepmo_packet_extract.py` (`build` / `check` / `diff`), three
  converted campaign briefs as fixtures plus two prefix-only briefs,
  constraint-preservation tests (sidecar `STABLE_RULES` equality, trailer,
  re-render, `bash -n` through `build`/`check`, unbackticked boundary paths
  through `build`, prefix-negating phrases), and a baseline table against E-0
  cached/uncached ratios with no token-savings claim. Adoption proposal names
  `--brief` / `--followup`. `risk_tier: standard`. Branch
  `sepmo/e2-compact-packets`.
  pins: sepmo-e2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [v3-0-charter-ledger.md](v3-0-charter-ledger.md) —
  **V3-0 (2026-08-21):** the format-v3 scope audit, and the defect it found. Intended as a
  charter with no product change and it does not close that way. **Read §3 first**:
  `rewrite_data_files` had no format-version check and reassigned every row's lineage on a v3
  table while returning the correct rows, where Spark carries lineage through unchanged. It is
  reachable on a v3 table that was already in the catalog, which is the drop-in case, so the
  guard shipped with the audit (`V3-LINEAGE-1`). §2 is the other half of the news, and it is
  good: v3 reads and v3 appends are already correct, round-tripped through Spark, including the
  row lineage the format mandates. §4 answers A12's stated first question — adoption, through
  `register_table`, whose Spark signature is measured there.
  Critic r3 PASS (2026-09-07): padded outer-join side disclosed in `FNP8-NULLABILITY`; counts trued up.
- [write-distribution-2-ledger.md](write-distribution-2-ledger.md) —
  **WRITE-DISTRIBUTION-2 (2026-09-06), in flight:** the hash distribution rule on the
  partitioned stream write paths — the funnel dispatcher routes each batch by hash of the
  writer's partition values, so one value lands in one writer: `INSERT OVERWRITE` and MERGE
  inserts go 32 → 8 data files (Spark's count) at 1e6. Plain `INSERT INTO` and
  `saveAsTable(append)` stay at 64 — fork-owned, the halted question. Closes the WD1 review
  gaps F-1 (mutation-proven cast pin), F-2 (partitioned abort pin) and F-4 (§8 counts). No
  dependency, no spawn. `risk_tier: standard`. Branch `perf/write-distribution-2`.
  pins: write-distribution-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008

## Pointers
- Up: [../map.md](../map.md)
- [perf-scan-1-plan-once-ledger.md](../completed/perf-scan-1-plan-once-ledger.md) —
  **PERF-SCAN-1 (2026-09-03 / r2 2026-09-04), in flight:** `TargetScanStream` caches
  `FileScanTask`s across concurrent `StreamingTable` re-executes (hardening). Registry
  `PERF-SCAN-3PASS-1` stays BACKLOG: production identity DELETE is 1 + 0 + 1 opens, not
  3 × N at scan. `risk_tier: standard`. Branch `perf/scan-1-plan-once`.
  pins: perf-scan-1-plan-once/C-001, C-002, C-003, C-004
- [sql-harden-1-cutover-shapes-ledger.md](../completed/sql-harden-1-cutover-shapes-ledger.md) —
  **SQL-HARDEN-1 (2026-09-04), in flight:** the cutover pipeline cutover Iceberg SQL shapes S1–S7
  measured against live Spark on the memory catalog; Glue + S3 Tables legs. Four registry
  rows filed, `V3-COV-7` cited, 0 FIXED. `risk_tier: standard`. Branch
  `feat/sql-harden-1-cutover-shapes`. pins: sql-harden-1-cutover-shapes/C-001
- [sql-harden-2-cow-shapes-ledger.md](../completed/sql-harden-2-cow-shapes-ledger.md) —
  **SQL-HARDEN-2 (2026-09-04), in flight:** S1/S2/S4 at v2 and v3 copy-on-write (S8/S9).
  `delete_files` empty both engines; data-file count 1 after the second MERGE; remaining
  DIVERGES are `CUTOVER-CTAS-REQ-1` / `V3-COV-7`. No `CUTOVER-COW-*` row. Glue + S3 Tables
  PASS. `risk_tier: standard`. Branch `feat/sql-harden-2-cow-shapes`.
  pins: sql-harden-2-cow-shapes/C-001, C-002, C-003, C-004
- [rp-10-repin-f25-ledger.md](../completed/rp-10-repin-f25-ledger.md) — **RP-10 (2026-09-04), in flight:**
  the fork repin `594bdbe5` → `85a4aaf0` (F-25). `validate_fresh_dvs_only` stops once every
  `added_dvs` key is found; `PERF-DVCLOSE-STMT-1` closes. `risk_tier: standard`. Branch
  `feat/rp-10-repin-f25`.
- [rp-16-ledger.md](rp-16-ledger.md) — **RP-16 (2026-09-11), in flight:** consume fork pin
  `090bc821` (F-WRITE-COMPRESS-1 `#276` plus riders `#273`–`#275`, `#277`). Consumer
  fix: `PERF-CATALOG-CACHE-WEIGHT-1` FIXED — charged object-graph weight, measured
  retain 1,071,000 / evict 1,070,000, retain pin 1,250,000, old 280000 now evicts. Do not re-measure
  AP-1. `risk_tier: standard`. Branch `chore/repin-rp-16`.
  pins: rp-16/C-001, C-002, C-003, C-004
- [date-fn-1-spark-date-spelling-ledger.md](../completed/date-fn-1-spark-date-spelling-ledger.md) —
  **DATE-FN-1 (2026-09-04), in flight:** Spark SQL `date()` spelling and `unix_timestamp`;
  `CUTOVER-DATE-1` FIXED; S6 gold rows Spark-equal, program still DIVERGES on `V3-COV-7`.
  `risk_tier: standard`. Branch `fix/date-fn-1-spark-date-spelling`.
  pins: date-fn-1-spark-date-spelling/C-004
- [ex-15-dataframe-a-ledger.md](../completed/ex-15-dataframe-a-ledger.md) —
  **EX-15 (2026-09-04), in flight:** the v1.1 example backfill's first `DataFrame.*` batch —
  36 roster names at base `c70a306`; 28 covered by eight `docs/examples/dataframe/` files
  (backlog 578 → 550), 8 measured divergences stay with §7 rows `EX-DF-1`…`EX-DF-6` and pins in
  `python/repark/tests/test_examples_dataframe_a.py`. `risk_tier: standard`. Branch
  `docs/ex-15-dataframe-a`. pins: ex-15-dataframe-a/C-001
- [ex-16-dataframe-b-ledger.md](../completed/ex-16-dataframe-b-ledger.md) —
  **EX-16 (2026-09-04), in flight:** the v1.1 example backfill's second `DataFrame.*` batch —
  36 roster names at base `f3968aa`; 32 covered by eight `docs/examples/dataframe/` files
  (backlog 550 → 518); `intersectAll`/`intersect_all` and `groupingSets`/`grouping_sets` stay
  with §7 rows `EX-DF-7`/`EX-DF-8`, and the narrow `mergeInto`/`printSchema` arms are recorded as
  §7 rows `EX-DF-9`/`EX-DF-10`, pins in `python/repark/tests/test_examples_dataframe_b.py`.
  `risk_tier: standard`. Branch `docs/ex-16-dataframe-b`. pins: ex-16-dataframe-b/C-001
- [ex-18-dataframe-c-ledger.md](../completed/ex-18-dataframe-c-ledger.md) —
  **EX-18 (2026-09-04), in flight:** the v1.1 example backfill's third `DataFrame.*` batch —
  36 roster names at base `e3600a1`; 35 covered by eleven `docs/examples/dataframe/` files (backlog 484 →
  449 through the EX-16/EX-17 merges), `toJSON` stays (R-DF-BATCH2), §7 `EX-DF-11`…`EX-DF-17`, pins in `python/repark/tests/test_examples_dataframe_c.py`. `risk_tier: standard`. Branch `docs/ex-18-dataframe-c`. pins: ex-18-dataframe-c/C-001
- [ex-17-column-a-ledger.md](../completed/ex-17-column-a-ledger.md) —
  **EX-17 (2026-09-04, r2), in flight:** the v1.1 example backfill's `Column.*` (a) batch —
  40 roster names at base `e3600a1`; 34 covered by ten `docs/examples/column/` files
  (backlog 550 → 516 at base; 484 after the EX-16 merge), 6 engine-plumbing rows stay (no PySpark analog), the two
  measured divergent bare-name arms are §7 rows `EX-COL-1`/`EX-COL-2` with pins in
  `python/repark/tests/test_examples_column_a.py`. `risk_tier: standard`. Branch
  `docs/ex-17-column-a`. pins: ex-17-column-a/C-001
- [df-printschema-1-trailing-newline-ledger.md](../completed/df-printschema-1-trailing-newline-ledger.md) —
  **DF-PRINTSCHEMA-1 (2026-09-04), in flight:** `printSchema` stdout byte-identical to
  Spark's (flat, nested, array, `level=1` exact captures); `EX-DF-10` flipped to FIXED in
  the merge commit `68e408d`. `risk_tier: standard`. Branch
  `fix/df-printschema-1-trailing-newline`.
  pins: df-printschema-1-trailing-newline/C-004
- [ex-20-window-catalog-ledger.md](../completed/ex-20-window-catalog-ledger.md) —
  **EX-20 (2026-09-04), in flight:** the v1.1 example backfill's `Window`/`WindowSpec` +
  first `Catalog.*` batch — 40 roster names at base `3484f8d7`; 37 covered by eight files
  under `docs/examples/window/` and `docs/examples/catalog/` (backlog 411 → 374 shipped;
  449 → 412 at the dispatch base), 3 stay
  (`getDatabase`/`get_database`, `listDatabases`) with §7 rows `EX-CAT-1`/`EX-CAT-2`, the
  `functionExists` dbName arm is `EX-CAT-3`, and the DataFrame-door tied-key default frame
  is `EX-WIN-1`, pins in `python/repark/tests/test_examples_window_catalog.py`.
  `risk_tier: standard`. Branch `docs/ex-20-window-catalog`. pins: ex-20-window-catalog/C-001
- [ex-19-dataframe-d-window-ledger.md](../completed/ex-19-dataframe-d-window-ledger.md) —
  **EX-19 (2026-09-04, r3), in flight:** the v1.1 example backfill's fourth `DataFrame.*` batch —
  the 39-name DataFrame remainder plus GroupedData, Row, na, and stat surfaces at base `7496049`;
  38 covered by ten `docs/examples/dataframe/` files (backlog 449 → 411 shipped after the EX-18
  merge; 518 → 480 at the dispatch base), `stat.freqItems` stays
  with §7 `EX-DF-19`, the `withColumnsRenamed` duplicate-name arm is §7 `EX-DF-18`, the struct
  `Row` field arm is §7 `EX-ROW-1`, pins in
  `python/repark/tests/test_examples_dataframe_d.py`. `risk_tier: standard`. Branch
  `docs/ex-19-dataframe-d-window`. pins: ex-19-dataframe-d-window/C-001
- [perf-dynflatten-2-null-mask-ledger.md](../completed/perf-dynflatten-2-null-mask-ledger.md) —
  **PERF-DYNFLATTEN-2 (2026-09-04), in flight:** the one candidate PERF-DYNFLATTEN-1 queued,
  built. A scalar UDF unions the parent struct's validity into the child array instead of a
  per-leaf `CASE WHEN parent IS NULL`; `struct_d6`'s isolated null cost 64.83 ms → 0.01 ms
  (0.1x its run's floor), every bed row set, schema and ordered-row digest identical against a
  rebuilt pre-extractor module, `DYNFLATTEN-QUALNAME-1` FIXED as a side effect and re-pinned as
  an answer pin. `risk_tier: standard`. Branch `perf/dynflatten-2-null-mask`.
  pins: perf-dynflatten-2-null-mask/C-001, C-002, C-003, C-004, C-005
- [ex-22-types-writerv2-ledger.md](../completed/ex-22-types-writerv2-ledger.md) —
  **EX-22 (2026-09-04), in flight:** the v1.1 example backfill's `types` + `DataFrameWriterV2`
  batch — all 43 roster names at base `b5827be6`; 42 covered by eleven files under
  `docs/examples/types/` (new) and `docs/examples/io/` (backlog 340 → 298 shipped; 374 → 332
  at the dispatch base), the flagged
  `VariantType`/`TimeType`/`CharType`/`VarcharType` measured Spark-equal, the Arrow helpers and
  four snake_case spellings covered as repark extensions; `DataFrameWriterV2.overwrite` stays
  with §7 `EX-W2-1`, the empty-source `overwritePartitions` arm is §7 `EX-W2-2`, the
  `option`/`options` branch-tag arm is §7 `EX-W2-3`, and the round-2 unpartitioned-table
  `overwritePartitions` parser leak is §7 `EX-W2-4` (OPEN, fix unit
  `WRITERV2-OVERWRITE-UNPART-1`), pins in
  `python/repark/tests/test_examples_window_catalog.py`. `risk_tier: standard`. Branch
  `docs/ex-22-types-writerv2`. pins: ex-22-types-writerv2/C-001, C-002, C-003, C-004, C-005
- [perf-dynflatten-1-measure-ledger.md](../completed/perf-dynflatten-1-measure-ledger.md) —
  **PERF-DYNFLATTEN-1 (2026-09-04), in flight:** measure `dynamicFlatten` on the
  nested bed; rank the three H-3 intake candidates. `risk_tier: standard`.
  Branch `perf/dynflatten-1-measure`.
  pins: perf-dynflatten-1-measure/C-001, C-002, C-003, C-004
- [profiles-1-ledger.md](profiles-1-ledger.md) —
  **PROFILES-1 step 1 (2026-09-10), in flight:** the measurement bed step 2 sweeps:
  three D-2 datasets, five reads + three writes, knob × value CSV harness, one-JVM
  guard, `--smoke` proof mode. No sweep, no timings as results. `risk_tier: standard`.
  Branch `feat/profiles-1`.
  pins: profiles-1/C-001, C-002, C-003, C-004, C-005
- [perf-facade-1-ledger.md](../completed/perf-facade-1-ledger.md) —
  **PERF-FACADE-1 (2026-09-04), in flight:** slate items 1 and 2 of PERF-ANALYSIS-1, the two
  biggest measured user-visible walls. `collect()` row materialization moves into
  `repark-python` (`collect_rows.rs` emits value tuples; the facade builds every `Row` from one
  shared names tuple per batch with the collector suspended): 1e6 x 7 **4,908 -> 956 ms**
  (5.14x), from 1.37x slower than Spark to 3.79x faster. `DataFrame.columns` answers from the
  plan's logical schema (`logical_names.rs`) and `with_columns` reads it once per call instead
  of once per existing column: depth-100 chain build **2,385 -> 367 ms** (6.50x, 5,750 analyzer
  passes -> 0), from 3.19x slower than Spark to 2.04x faster. The 150 ms chain target is NOT
  met and is reported as missed: the residue is DataFusion's own per-expression projection
  validation, and the collapse that would close it measures **65.04 ms** — under the bar, so
  `PERF-FACADE-CHAIN-2` is deferred on correctness (plan lineage, `_origin_plan_id`,
  `MISSING_ATTRIBUTES`) and not because the prize is small. Mutation 8 of 8 red; an independent
  critic reproduced the unit on its own clone and reds 7 of its own. Round 2 replaced the
  baseline with a tracked runner (`python/repark-parity/bench/facade/`, `make facade-bench`) and
  re-measured every number with it, because the first baseline's probes were untracked.
  `risk_tier: standard`. Branch `perf/facade-1`.
  pins: perf-facade-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [cutover-schema-1-ledger.md](../completed/cutover-schema-1-ledger.md) —
  **CUTOVER-SCHEMA-1 (2026-09-04), in flight:** nullability derived the way Spark
  derives it — reader relax, CTAS all-optional on both doors, decimal-cast analyzer
  rule, export-boundary Utf8 coercion. Closes `CUTOVER-CTAS-REQ-1` and
  `CUTOVER-DEDUP-SCHEMA-1`, the nullability half of `V3-COV-8`, converges
  `DYNFLATTEN-READNULL-1`. `risk_tier: standard`. Branch
  `fix/cutover-schema-1`.
  pins: cutover-schema-1/C-001, C-002, C-003, C-004, C-005, C-006
- [nullability-2-ledger.md](../archive/2026-09/2026-09-06-nullability-2-ledger.md) —
  **NULLABILITY-2 (2026-09-05), complete:** the analyzer's remaining nullability
  and cast residues, Spark-equal — generalized cast nullability, boolean→decimal,
  null-safe equal non-null, reader relax at every depth, tz-naive dtype mapping.
  Closes or narrows `CAST-NULL-1`, `CAST-BOOL-DEC-1`, `DEC-9` (remainder),
  `G6-4`, `G12-1`, `G12-2`, `CUTOVER-NULLDEPTH-1`, `READ-TSNTZ-DTYPE-1`.
  `risk_tier: elevated`. Branch `fix/nullability-2`.
  pins: nullability-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [perf-facade-cdf-1-ledger.md](../completed/perf-facade-cdf-1-ledger.md) —
  **PERF-FACADE-CDF-1 (2026-09-05), in flight:** PERF-ANALYSIS-1 candidate 2 —
  `createDataFrame(list of tuples)` stops normalizing every cell in Python five times and
  infers + converts column-wise, with nested columns delegated to the unchanged per-cell
  path. Target `create/100000/tuples_count` ≤ 100 ms. `risk_tier: standard`. Branch
  `perf/facade-cdf-1`.
  pins: perf-facade-cdf-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [dfcore-2-ledger.md](../completed/dfcore-2-ledger.md) —
  **DFCORE-2 (2026-09-07), in flight:** the four UDF select rewrites out of `core.py` —
  scalar and classic to `udf_projection.py`, the window variants to
  `udf_window_projection.py`. Move-only: `core.py` 5954 → 5263, class loses exactly
  the four methods, package and core gain exactly the two modules, 6101 collected IDs
  preserved plus one pin test, four mutations red existing pins. `risk_tier: standard`.
  Branch `refactor/dfcore-2`.
  pins: dfcore-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-3-ledger.md](../completed/dfcore-3-ledger.md) —
  **DFCORE-3 (2026-09-07), in flight:** the statistics family out of `core.py` —
  six bodies plus the `freqItems` refusal to `statistics.py`. Move-only: `core.py`
  5263 → 5060, `writer_readwriter.py` 1113 → 1111, class dir frozen, package and
  core gain exactly `statistics`, 6102 collected IDs preserved plus one pin test,
  seven mutations red existing pins, collect count 6 == 6. `risk_tier: standard`.
  Branch `refactor/dfcore-3`.
  pins: dfcore-3/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-4a-ledger.md](../completed/dfcore-4a-ledger.md) —
  **DFCORE-4a (2026-09-07), in flight:** sampling out of `core.py` — the three
  sampling bodies plus argument normalization and seed coercion to `sampling.py`.
  Move-only: `core.py` 5060 → 4819, class loses exactly `_prepare_sample_args`,
  package and core gain exactly `sampling`, 6103 collected IDs preserved plus five
  pin tests, five mutations red existing pins, same-seed determinism pinned on a
  fixed thousand-row frame. `risk_tier: standard`.
  Branch `refactor/dfcore-4a`.
  pins: dfcore-4a/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-4b-ledger.md](../completed/dfcore-4b-ledger.md) —
  **DFCORE-4b (2026-09-07), in flight:** display out of `core.py` — the ten
  show/repr/HTML/eager bodies to `display.py`. Move-only: `core.py` 4819 →
  4539, class loses exactly the six display leavers, package and core gain
  exactly `display`, 6108 collected IDs preserved plus fourteen pin tests, ten
  mutations red existing pins, show/repr/HTML goldens byte-identical with
  `count()` tallies as the DFCORE-6 baseline. `risk_tier: standard`.
  Branch `refactor/dfcore-4b`.
  pins: dfcore-4b/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  Critic r1 FAIL on the moved warning's `stacklevel`; served (stacklevel 3, filename pin).
- [dfcore-5-ledger.md](../completed/dfcore-5-ledger.md) —
  **DFCORE-5 (2026-09-07), in flight:** `approxQuantile` in one collect per
  frame — the per-probability loop becomes one aggregation over the list form
  of `percentile_approx` (2x3 collects 6 → 1, 4x5 20 → 1, values identical),
  validation, shapes, NaN rules and the ignored `relativeError` preserved on
  both doors, audit values equal to live Spark, the all-NULL/empty divergence
  pinned on both sides, and the three DFCORE-3/DFCORE-4a gap pins closed.
  `risk_tier: standard`.
  Branch `perf/dfcore-5`.
  pins: dfcore-5/C-001, C-002, C-003, C-004, C-005
- [dfcore-6-ledger.md](../completed/dfcore-6-ledger.md) —
  **DFCORE-6 (2026-09-07), in flight:** eager previews fetch N+1 and never
  `count()` — repr, HTML, and vertical show read the footer from the extra
  row (1 → 0 counts per door), bridged eager doors peek at most
  `maxNumRows + 1` UDF rows (1e6-row preview 2,000,000 → 65,536 computed
  rows, 0.821/0.845 → 0.031/0.031 s), footers and cap-edge shapes preserved,
  every DFCORE-4b golden byte-identical, DFCORE-4b F2 closed by a non-golden
  vertical pin. `risk_tier: standard`.
  Branch `perf/dfcore-6`.
  pins: dfcore-6/C-001, C-002, C-003, C-004, C-005
  Critic r1 PASS (2026-09-07); the row-count assertion added to the repr footer pin.
- [sql-describe-1-ledger.md](../completed/sql-describe-1-ledger.md) —
  **SQL-DESCRIBE-1 (2026-09-09), in flight:** `DESCRIBE [TABLE] [EXTENDED|FORMATTED]`
  on Iceberg tables — step 1 measured the live PySpark 4.1.2 oracle on one session:
  six captures plus schemas in the ledger's Oracle capture section, FORMATTED
  byte-identical to EXTENDED, plain identical to `DESCRIBE TABLE`, missing table raises
  `AnalysisException` with `[TABLE_OR_VIEW_NOT_FOUND]`. Step 3 (2026-09-09) landed the
  facade pins, the live leg (19 of 22 rows byte-identical; `Name`, `Location`, `Table
  Properties` engine defaults differ by measurement), the DESC-1 registry row, and the
  DBT-DESC-1 retirement; all clauses PROVEN with four residue rows. `risk_tier: standard`.
  Branch `feat/sql-describe-1`.
  pins: sql-describe-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [maint-policy-1-ledger.md](../completed/maint-policy-1-ledger.md) —
  **MAINT-POLICY-1 (2026-09-10), complete:** the declarative maintenance policy — the typed
  `[<profile>.maintenance]` table with per-table overrides and the D-2 duration parser
  (`config_file/maintenance.rs`), `CALL <catalog>.system.run_maintenance(...)` with its
  dry-run plan, the D-4 step order behind the delete-ratio gate, the apply path
  (`ran` / `failed` / `skipped`, the chain stopping on failure), the session-build stamp
  that makes `repark.toml` reach the procedure, and the `session.run_maintenance` facade
  wrapper with its guide. `adaptive_partitioning` reserved for ADAPT-PART.
  `risk_tier: standard`. Branch `feat/maint-policy-1`.
  pins: maint-policy-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-024, C-025, C-026, C-027, C-028, C-029, C-030
- [facade-audit-0-ledger.md](../completed/facade-audit-0-ledger.md) —
  **FACADE-AUDIT-0 steps 1–2 (2026-09-10), in flight:** Half A of the Rust-backed facade
  audit — one measured row per module under `python/repark/src/repark/` (106 files,
  51,930 lines; binding sites, pyarrow references, delegate/logic/pyarrow class), the
  IPC crossing sites, and every place a `Column` renders SQL text — plus Half B
  (weighing, freeze constraints, confirmed sequence with pins, open questions), in
  [task/roadmap/epic-term/facade-audit-2026-09-10.md](../../roadmap/epic-term/facade-audit-2026-09-10.md).
  Base `2fad8135`; no source touched; READING path under R-10.
  `risk_tier: standard`. Branch `docs/facade-audit-0`.
  pins: facade-audit-0/C-001, C-002, C-003, C-004, C-006, C-007, C-008
- [ap-1-ledger.md](../completed/ap-1-ledger.md) —
  **AP-1 step 1 (2026-09-10), in flight:** `CALL
  <catalog>.system.plan_partitioning(table => …, target_file_size_bytes => …)` — the P-2
  candidates scored with exactly P-3 over the `files` metadata table, the D-1 frame, the
  AP-0-R-001 caveat on every row, P-5 branch refusal plus the multi-spec note. Step 2 (GLM:
  release run over the AP-0 tables, perf document, guide section) is out of scope.
  `risk_tier: standard`. Branch `feat/ap-1`.
  pins: ap-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [review-fix-4-ledger.md](../completed/review-fix-4-ledger.md) —
  **REVIEW-FIX-4 (2026-09-10), in flight:** the eager frame's checkpoint paths —
  `lazy()` with no `_cache_view` interpolation, pending-checkpoint discharge in
  `count()` and the styled row count with no count query, and the `none`-view
  unreachable pin. Closes Q-12, Q-13, Q-50. `risk_tier: standard`. Branch
  `fix/review-fix-4`.
  pins: review-fix-4/C-001, C-002, C-003
- [review-fix-5-ledger.md](../completed/review-fix-5-ledger.md) —
  **REVIEW-FIX-5 step 1 (2026-09-10), in flight:** DESCRIBE takes one- to three-part names
  with session-default completion (D-3), never filters a three-part table named like a
  metadata table (D-1), resolves `Owner` from the session-built `DescribeOwnerConfig`
  extension (D-2), and redacts Table Properties through `prop_key_is_secret` with the Spark
  `s3.access-key-id` delta recorded as deliberate (D-4). `risk_tier: standard`.
  Branch `fix/review-fix-5`.
  pins: review-fix-5/C-001, C-002, C-003, C-004, C-005, C-006
- [review-fix-1-ledger.md](../completed/review-fix-1-ledger.md) —
  **REVIEW-FIX-1 (2026-09-10), in flight:** the CFG-1 mirror agrees with the loader —
  ASCII-only knob values refusing as `ValidationError`, duplicate names refused across
  every catalog and every database source naming both key paths, and an empty overlay
  profile surviving the `to_toml()` round trip. Closes Q-3, Q-4, Q-5, Q-6.
  `risk_tier: standard`. Branch `fix/review-fix-1-2`.
  pins: review-fix-1/C-001, C-002, C-003
- [review-fix-2-ledger.md](../completed/review-fix-2-ledger.md) —
  **REVIEW-FIX-2 (2026-09-10), in flight:** CFG-1's own pins stop reading the
  developer's `HOME` — the seed no-file pin drives stubbed discovery with `home: None`
  and the C-023 control session builds from a forced empty file. Closes Q-1.
  `risk_tier: standard`. Branch `fix/review-fix-1-2`.
  pins: review-fix-2/C-001, C-002
- [review-fix-6-ledger.md](../completed/review-fix-6-ledger.md) —
  **REVIEW-FIX-6 step 1 (2026-09-10), in flight:** `explain()` refuses the both-set
  shape — `extended` and `mode` together raise `PySparkValueError`
  `CANNOT_SET_TOGETHER` (the `Row(1, a=2)` shape), while a string `extended` with no
  `mode` keeps working. Closes Q-17. `risk_tier: standard`. Branch
  `fix/review-fix-6-11`. All clauses PROVEN (both-set pin red-first on base, green
  with the guard).
  pins: review-fix-6/C-001, C-002, C-003, C-004
- [review-fix-11-ledger.md](../completed/review-fix-11-ledger.md) —
  **REVIEW-FIX-11 step 1 (2026-09-10), in flight:** every DF-EXPLAIN-1 D-2 row gets a
  pin — cost, codegen, `ANALYZE`, simple-mode physical-only, and the blank-line
  separation. Landed with REVIEW-FIX-6. Closes Q-32 (with Q-17). `risk_tier: standard`.
  Branch `fix/review-fix-6-11`. All clauses PROVEN (mode pins green-before-green on
  base, confirming the D-2 rows; green on the landed tree).
  pins: review-fix-11/C-001, C-002, C-003, C-004, C-005, C-006
- [display-lazy-1-ledger.md](../completed/display-lazy-1-ledger.md) —
  **DISPLAY-LAZY-1 step 1 (2026-09-10), in flight:** a lazy frame's `repr` shows
  the schema, not the data (R-22 supersedes R-2) — the D-1 header box over the
  `lazy: N columns` first line, D-2 materialised classification, D-3 eagerEval
  rows with one plan run, D-4 unchanged doors plus the lazy bridge header, D-5
  transformation-laziness chains, and RF-5's duckdb probe with its re-pin. Step 2
  adds the plain-checkpoint marker (C-007) and the docs round. `risk_tier: standard`.
  Branch `feat/display-lazy-1`.
  pins: display-lazy-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [review-fix-8-ledger.md](../completed/review-fix-8-ledger.md) —
  **REVIEW-FIX-8 (2026-09-11), in flight:** the PROFILES-1 probe is re-runnable and its
  table is true — a unique temporary directory per run, `REPARK_CONFIG=""` in every
  session, the `VALIDATED` state for the three `repark.*` session keys, the two
  `write.*` rows re-described as accepted-at-session / applied-at-table with per-key
  table measurements, and re-derived counts (13 / 3 / 4 / 0 / 0). Closes Q-21, Q-22,
  Q-23, Q-40, Q-41, Q-55, Q-56. `risk_tier: standard`. Branch
  `fix/review-fix-8-conf-unread-1`.
  pins: review-fix-8/C-001, C-002, C-003, C-004, C-005
- [review-fix-3-ledger.md](../completed/review-fix-3-ledger.md) —
  **REVIEW-FIX-3 step 1 (2026-09-10), in flight:** the polars keep-set honours every
  legal `max_rows` — `min(n, max_rows)` split `(keep + 1) // 2` head /
  `keep - head` tail (Q-9), `max_rows = 1` earns its `1, …` without reviving the
  C8-Q-001 bare-ellipsis keep-set, the `truncate=True` → `str_len` remap gains a
  mutation pin (Q-10), and `show`'s docstring states the probe-first count (Q-11).
  `risk_tier: standard`. Branch `fix/review-fix-3-9-14`.
  pins: review-fix-3/C-001, C-002, C-003, C-004
- [review-fix-9-ledger.md](../completed/review-fix-9-ledger.md) —
  **REVIEW-FIX-9 step 1 (2026-09-10), in flight:** the polars renderer matches live
  polars 1.43.2 at its boundaries — list cells elide at four items
  (`[0, 1, … 3]`, Q-27) and `_polars_float_text` is re-derived from `fmt_float`
  so `±999999.0` spells `±9.99999e5` (Q-28), with live-polars oracle pins on
  list lengths 3/4/5 and the float switch either side (D-3); the shared
  `max_rows` pin is worked once under REVIEW-FIX-3 (Q-26).
  `risk_tier: standard`. Branch `fix/review-fix-3-9-14`.
  pins: review-fix-9/C-001, C-002, C-003
- [review-fix-14-ledger.md](../completed/review-fix-14-ledger.md) —
  **REVIEW-FIX-14 step 1 (2026-09-10), in flight:** the display configuration keeps
  its promises — `conf.get` on an unset `repark.display.style` serves the
  alive-token snapshot instead of re-reading `REPARK_DISPLAY_STYLE` (Q-52,
  ADR-0004), and `repark.display.max_rows` refuses above the delegated ceiling
  `_DISPLAY_MAX_ROWS_CEILING = 10_000` so the styled probe cannot fetch unbounded
  (Q-51). `risk_tier: standard`. Branch `fix/review-fix-3-9-14`.
  pins: review-fix-14/C-001, C-002
