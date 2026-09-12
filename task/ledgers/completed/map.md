# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [ap-0-ledger.md](ap-0-ledger.md) —
  **AP-0 (measure, 2026-09-10), in flight:** ADAPT-PART partition-candidate
  measurement — three local Iceberg beds (futures CTAS unpartitioned, generated
  uniform/skewed 400k-row beds, 206 files each) scored from `files`/`partitions`
  manifest bounds at a 512 KiB target, one ranked P-2/P-3 table per bed in
  [../../../docs/perf/ap-0-partition-candidates-2026-09-10.md](../../../docs/perf/ap-0-partition-candidates-2026-09-10.md),
  runnable method in
  [../../../python/repark-parity/bench/adaptpart/map.md](../../../python/repark-parity/bench/adaptpart/map.md).
  4 PROVEN, 1 OPEN (C-005, the 20 percent prediction check needs the orchestrator
  O-run rewrite). `risk_tier: standard`. Branch `feat/ap-0`.
  pins: ap-0/C-001, C-002, C-003, C-004
- [ap-1-ledger.md](ap-1-ledger.md) — Unit ledger — AP-1 steps 1–2 · `CALL plan_partitioning()`; step 2 (2026-09-11) projects from the footer byte ratio and files residue AP-1-R-001. **RP-17 re-measure (2026-09-12)** errata at the top (above the RP-16 one): fork `#278` makes `rewrite_data_files` write zstd; live actuals 4 474 081 / 4 136 632 ZSTD; the 20 percent check reads −74.2 % / −72.0 % against stored×ratio; AP-1-R-001 still OPEN; both projections recorded for the orchestrator's Q-1 ruling. Clause verdicts untouched.
- [ap-2-ledger.md](ap-2-ledger.md) — Unit ledger — AP-2 step 1 (apply) · `CALL apply_partitioning()`
- [ballista-audit-0-ledger.md](ballista-audit-0-ledger.md) —
  **BALLISTA-AUDIT-0 steps 1–3 (2026-09-08), in flight:** Half A facts plus Half B
  judgement for the Ballista audit at upstream tag `54.1.0` (`f4e66525`) —
  clauses C-001…C-012 PROVEN on document evidence under the grammar gate's
  reading-unit rule (R-10; LEDGER-READING-1 step 2, 2026-09-09), §26 A–H +
  §27 + §28 gate filled, appendix A6 closes the python/tooling gap; the ADR
  disposition and the brief's Milestone 0 row landed in #426.
  Branch `docs/ballista-audit-0`.
- [ballista-m1-a-ledger.md](ballista-m1-a-ledger.md) —
  **BALLISTA-M1-A step 1 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): `DistributedExecutor` + `LocalDataFusionExecutor` in
  `crates/repark-distributed`; range-sum, status, and cancel pins green; `cluster`
  feature builds with no cluster code. `risk_tier: standard`.
  Branch `feat/ballista-m1-a`.
  pins: ballista-m1-a/C-001, C-002, C-003, C-004
- [ballista-m1-b-ledger.md](ballista-m1-b-ledger.md) —
  **BALLISTA-M1-B step 2 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): UDF-on-executor pin
  (`repark_times_ten` through `ReparkSessionProvider`; vanilla session fails to resolve);
  cancel mid-flight (`Cancelled`, no running tasks within 5 s); codec install pin
  (`repark_ballista_codec()` is Ballista's defaults; two-executor shuffle completes).
  C-004 round-trip serde stays OPEN / PARKED (needs `datafusion-proto`). `risk_tier: standard`.
  Branch `feat/ballista-m1-b`.
  pins: ballista-m1-b/C-001, C-002, C-003, C-005, C-006
- [ballista-m1-c-ledger.md](ballista-m1-c-ledger.md) —
  **BALLISTA-M1-C step 2 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): D-1 three shapes PROVEN; D-3
  `Completed { stages, retried_stages }` shuffle bytes > 0 on the two-stage hash aggregate;
  D-4 session spill dir has no shuffle files after complete or cancel. D-2 retry stays
  OPEN (ChaosExec is not fail-once; stopping an executor needs `arrow_flight`).
  `risk_tier: standard`. Branch `feat/ballista-m1-c`.
  pins: ballista-m1-c/C-001, C-003, C-004
- [ballista-m1-d-ledger.md](ballista-m1-d-ledger.md) —
  **BALLISTA-M1-D step 2 (2026-09-10), merged** as part of Ballista Milestone 1
  (#460/#466/#469/#470): Iceberg provider codec and
  two-executor scan pins (step 1) plus the design-doc Iceberg section and success-list
  line 17. Runtime abstraction is in place; Iceberg writes and the commit coordinator
  are Milestone 3 (ADR-0004). Residues: S3/Glue credentials; `IcebergTableScan` rewrite
  to parquet file groups (`datafusion-proto` wall, third time). `risk_tier: standard`.
  Branch `feat/ballista-m1-d`.
  pins: ballista-m1-d/C-001, C-002, C-003
- [ballista-m2-a-ledger.md](ballista-m2-a-ledger.md) —
  **BALLISTA-M2-A steps 1–2 (2026-09-11):** the delegating physical extension codec —
  `ReparkPhysicalExtensionCodec` implements `PhysicalExtensionCodec`, delegates every node it
  does not own to `BallistaPhysicalExtensionCodec`, owns `IcebergTableScan` via the `RPIC`
  `IcebergScanSpec` wire format, and rebuilds the scan from the session catalog carried inside
  the codec (never ambient authority). Re-proves BALLISTA-M1-B C-004. Step 2 owns the design
  doc. `risk_tier: standard`. Branch `feat/ballista-m2-a`.
  pins: ballista-m2-a/C-001, C-002, C-003, C-004
- [ballista-m2-b-ledger.md](ballista-m2-b-ledger.md) —
  **BALLISTA-M2-B (2026-09-11), in flight:** typed `IcebergTableScan` encode — downcast
  plus `table().identifier()`, `resolved_snapshot_id()`, `projection()`, `predicates()`;
  predicates travel as `datafusion-proto` `Expr`s; string literals and bracketed names
  travel; encode-time rebuild-and-compare guard retired; BALLISTA-M2-A-R-001 closed.
  Wire `RPIC` v2. `risk_tier: standard`. Branch `feat/ballista-m2-b`.
  pins: ballista-m2-b/C-001, C-002, C-003, C-004, C-005
- [conf-unread-1-ledger.md](conf-unread-1-ledger.md) —
  **CONF-UNREAD-1 steps 1–2 (2026-09-11), in flight:** the wiring/refusals for
  the four accepted-but-unread `datafusion.*` keys — `coalesce_batches` refuses
  loud at build and at runtime `SET` (no DataFusion 54.1.0 engine path reads
  it); `enable_page_index`, `bloom_filter_on_read` and `write_batch_size` are
  wired (values reach the scan-source / writer options; the write subject moves
  file bytes); the probe re-run carries nineteen accepted keys, one refusal,
  and nine validation refusals. Step 2: the probe tables gained the
  `after CONF-UNREAD-1` column (`ACCEPTED BUT UNREAD` zero), §5's counts were
  re-derived, and the guide gained the `datafusion.*` forwarding/refusal
  paragraph. `risk_tier: standard`. Branch `feat/conf-unread-1`.
  pins: conf-unread-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [csv-infer-perf-1-ledger.md](csv-infer-perf-1-ledger.md) —
  **CSV-INFER-PERF-1 (2026-09-06), in flight:** local CSV `inferSchema` no longer
  materializes the frame per candidate cast. Native DataFusion inference plus
  Utf8-only timestamp columns; `nullValue` keeps one `try_cast` aggregation.
  300k × 8 True 2.339 s → 0.079 s (0.95× of False). `risk_tier: standard`.
  Branch `perf/csv-infer-perf-1`.
  pins: csv-infer-perf-1/C-001, C-002, C-003, C-004, C-005, C-006
- [cutover-schema-1-ledger.md](cutover-schema-1-ledger.md) — Unit ledger — CUTOVER-SCHEMA-1 · nullability derived the way Spark derives it
- [date-fn-1-spark-date-spelling-ledger.md](date-fn-1-spark-date-spelling-ledger.md) — Unit ledger — DATE-FN-1 · Spark SQL `date()` spelling
- [dbt-1-adapter-ledger.md](dbt-1-adapter-ledger.md) — **DBT-1 (2026-09-04), in flight:** a dbt
  path for RePark, so cutover step C6 can move gold off Spark/Glue
  ([../../../docs/cutover/inventory.md](../../../docs/cutover/inventory.md) ruling 2). Design
  first: every statement shape dbt emits for the two gold models and their ten test blocks was
  run through `repark.sql()` on a memory catalog, and the seventeen refusals are §3.2. They are
  all in the **statement surface**, not the transport, so a Spark-Thrift endpoint was rejected on
  measurement — the route is an in-process `dbt-repark` adapter subclassing `dbt-spark`'s
  `SparkAdapter`, so `file_format='iceberg'` keeps one reading rather than two.
  `dbt run` + `dbt test` build both models and pass all ten blocks on the S6 answers (59 passed,
  1 skipped, via `make py-test-dbt` in `preflight`); the Glue leg is written and skipped for the
  orchestrator. Ten registry rows: §2.5 `DBT-VIEW-1`, `DBT-TEMPVIEW-1`, `DBT-DESC-1`,
  `DBT-TBLPROPS-1` (extended in round 2 to cover `SHOW TABLE EXTENDED`, whose message it shares
  verbatim), `DBT-CREATENS-1`; §7 `B-TZ-5` (promoted from the awaiting-pins queue when this unit
  pinned it), `DBT-CTASCLAUSE-1`, `DBT-RELCOMMENT-1`, `DBT-COLCOMMENT-1`, `DBT-QUALIFY-1`.
  **Round 2 (Opus critic, FAIL on 7 S2 + 3 S3) is §10**; read §6 for the mutation table, which
  now carries a zero-red control and states which mutations are true no-ops rather than gaps.
  `risk_tier: standard`.   Branch `feat/dbt-1`.
  pins: dbt-1-adapter/C-001, C-002, C-003, C-004, C-005
- [df-eager-1-ledger.md](df-eager-1-ledger.md) —
  **DF-EAGER-1 step 1 (2026-09-09), in flight:** `.eager()` / `.compute()` / `.lazy()` on the
  facade DataFrame. Step 1 only: the red-first pins in
  [../../../python/repark/tests/test_df_eager_1.py](../../../python/repark/tests/test_df_eager_1.py)
  — seven `xfail(strict=True)` pins red on the base tree (marker-less run recorded in the
  ledger) plus two keep-green guards; step 2 builds D-1..D-6 in `core.py` + `polars.py`.
  Clauses C-001…C-006 OPEN. Branch `feat/df-eager-1`.
  pins: df-eager-1/C-001, C-002, C-003, C-004, C-005, C-006
- [df-explain-1-ledger.md](df-explain-1-ledger.md) —
  **DF-EXPLAIN-1 (2026-09-08), in flight:** `DataFrame.explain()` prints plan text, not
  `Row(...)` reprs. Step 1 done: the seven red-first pins in
  `python/repark/tests/test_df_explain_1.py` run RED on base `f00ed9ea` (run recorded in the
  ledger); D-4 measured — `EXPLAIN FORMAT TREE` passes the Spark door unchanged. Step 2 open:
  implement D-1..D-3 in `core.py` (`_explain_text` + the method body) plus the guide
  paragraph. `risk_tier: standard`. Branch `feat/df-explain-1`.
  pins: df-explain-1/C-001, C-002, C-003, C-004, C-005, C-006
- [df-printschema-1-trailing-newline-ledger.md](df-printschema-1-trailing-newline-ledger.md) — Unit ledger — DF-PRINTSCHEMA-1 · printSchema prints Spark's trailing blank line
- [dfcore-1-ledger.md](dfcore-1-ledger.md) —
  **DFCORE-1 (2026-09-07), in flight:** leaf helpers out of `core.py` — Arrow cell
  conversion to `rows_export.py`, export-error mapping to new `export_errors.py`,
  mapInArrow schema checks to new `udf_schema.py`, grouped-UDF assembly to new
  `grouped_udf.py`. Move-only: `core.py` 6302 → 5954, `joins_columns.py` 1239 → 1238,
  export surfaces pinned identical, 6093 pre-existing collected IDs unchanged, 6 added. `risk_tier: standard`.
  Branch `refactor/dfcore-1`.
  pins: dfcore-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-2-ledger.md](dfcore-2-ledger.md) — Unit ledger — DFCORE-2 · the four UDF projection rewrites out of `core.py`
- [dfcore-3-ledger.md](dfcore-3-ledger.md) — Unit ledger — DFCORE-3 · the statistics family out of `core.py`
- [dfcore-4a-ledger.md](dfcore-4a-ledger.md) — Unit ledger — DFCORE-4a · sampling out of `core.py`
- [dfcore-4b-ledger.md](dfcore-4b-ledger.md) — Unit ledger — DFCORE-4b · display out of `core.py`
- [dfcore-5-ledger.md](dfcore-5-ledger.md) — Charter ledger — DFCORE-5 · `approxQuantile` in one collect per frame
- [dfcore-6-ledger.md](dfcore-6-ledger.md) — Charter ledger — DFCORE-6 · eager previews fetch N+1, never `count()`
- [display-bridge-1-ledger.md](display-bridge-1-ledger.md) —
  **DISPLAY-BRIDGE-1 step 1 (2026-09-09), in flight:** a bridged frame's `show()` follows
  the display style (R-13) — `_show` resolves the style before the bridge peek; under
  `polars` / `duckdb` the peeked table renders through `_render_styled_show` (short peek →
  exact shape, no count; full peek → one count), under `spark` the grid is byte-identical.
  Closes DISPLAY-POLARS-1-S3-Q-001 (flipped FIXED in the completed display-polars-1
  ledger). Three clauses, pins red-first in
  [../../../python/repark/tests/test_display_bridge_1.py](../../../python/repark/tests/test_display_bridge_1.py).
  `risk_tier: standard`. Branch `feat/display-bridge-1`.
  pins: display-bridge-1/C-001, C-002, C-003
- [display-lazy-1-ledger.md](display-lazy-1-ledger.md) — Unit ledger — DISPLAY-LAZY-1 step 1 · a lazy frame's `repr` shows the schema, not the data (R-22)
- [display-polars-1-ledger.md](display-polars-1-ledger.md) —
  **DISPLAY-POLARS-1 step 1 (2026-09-09), in flight:** polars-style rendering becomes the
  default. Step 1 only (D-1 + D-2): `_DEFAULT_DISPLAY_STYLE` flips to `polars`, resolved
  through new `default_display_style()` (env `REPARK_DISPLAY_STYLE` first, refuse-loud via
  `normalize_display_style`), and the facade suite pins `spark` via conftest so no existing
  expectation moves. Clauses C-001…C-002 green. Branch `feat/display-polars-1`.
  pins: display-polars-1/C-001, C-002
- [docs-links-1-ledger.md](docs-links-1-ledger.md) —
  **DOCS-LINKS-1 step 1 (2026-09-09), in flight:** the gate LEDGER-READING-1 D-3 assumed —
  `make check-docs-links` checks every tracked `*.md`'s relative links, GitHub-style anchors
  and `docs:` evidence cells; the measured 10-link baseline is the allowlist residue
  (`scripts/docs_links_allowlist.txt`). Clauses C-001…C-003 green. Branch `feat/docs-links-1`.
- [dynflatten-listnull-1-ledger.md](dynflatten-listnull-1-ledger.md) —
  **DYNFLATTEN-LISTNULL-1 (2026-09-06), in flight:** Spark's parquet reader infers
  `optional int32 element (Null)` as `array<int>`; repark kept `List(Null)` and
  `drop_null_lists=True` dropped `user_properties`. FIX: `promote_parquet_null_types`
  in `read_parquet_nullable` maps Arrow `Null` to `Int32` after the nullability relax.
  Default `drop_null_lists` stays True; SQL `make_array()` still drops. Live
  `read.parquet` + `dynamicFlatten` matches Spark including `user_properties` int32
  NULLs. `risk_tier: standard`. Branch `fix/dynflatten-listnull-1`.
  pins: dynflatten-listnull-1/C-001, C-002, C-003, C-004, C-005, C-006
- [ex-15-dataframe-a-ledger.md](ex-15-dataframe-a-ledger.md) — Unit ledger — EX-15 · v1.1 example backfill, `DataFrame.*` (a)
- [ex-16-dataframe-b-ledger.md](ex-16-dataframe-b-ledger.md) — Unit ledger — EX-16 · v1.1 example backfill, `DataFrame.*` (b)
- [ex-17-column-a-ledger.md](ex-17-column-a-ledger.md) — Unit ledger — EX-17 · v1.1 example backfill, `Column.*` (a)
- [ex-18-dataframe-c-ledger.md](ex-18-dataframe-c-ledger.md) — Unit ledger — EX-18 · v1.1 example backfill, `DataFrame.*` (c)
- [ex-19-dataframe-d-window-ledger.md](ex-19-dataframe-d-window-ledger.md) — Unit ledger — EX-19 · v1.1 example backfill, `DataFrame.*` remainder, `GroupedData`, `Row`, na/stat functions (d)
- [ex-20-window-catalog-ledger.md](ex-20-window-catalog-ledger.md) — Unit ledger — EX-20 · v1.1 example backfill, `Window` / `WindowSpec` and the first `Catalog.*` names
- [ex-21-catalog-session-ledger.md](ex-21-catalog-session-ledger.md) —
  **EX-21 (2026-09-04, r2), in flight:** the v1.1 example backfill's `Catalog.*` remainder +
  `SparkSession` surface (a) batch — 35 roster names at base `b5b17f0`; 34 covered by sixteen
  `docs/examples/catalog/` and `docs/examples/session/` files (backlog 411 → 377; 374 → 340
  after the EX-20 merge), `list_databases` stays (same function object as the divergent
  `listDatabases`, §7 `EX-CAT-2`), the `registerFunction` return, `newSession` promotion,
  empty `create_dataframe`, unset-key `conf.get`, and missing-path reader arms are §7
  `EX-SES-1`..`EX-SES-5`, pins in
  `python/repark/tests/test_examples_window_catalog.py`. `risk_tier: standard`. Branch
  `docs/ex-21-catalog-session`. pins: ex-21-catalog-session/C-001
- [ex-22-types-writerv2-ledger.md](ex-22-types-writerv2-ledger.md) — Unit ledger — EX-22 · v1.1 example backfill, the `types` surface and `DataFrameWriterV2`
- [ex-23-ta-a-ledger.md](ex-23-ta-a-ledger.md) —
  **EX-23 (2026-09-04), in flight:** the v1.1 example backfill's TA-kernels (a) batch — the
  first 40 `ta.*` backlog names at the dispatch base `671a7144` (shipped on `bfef4a62`); all 40 covered by eight
  `docs/examples/ta/` files (backlog 298 → 258 shipped; 340 → 300 at dispatch) measured against the recorded C TA-Lib 0.4.0
  goldens on the 5000-row OHLCV fixture (Spark has no TA kernels — the goldens are the family's
  oracle, the same `.bin` files `test_ta.py`/`test_ta_volume.py` pin bit-identically); all 40
  bit-identical, zero divergences, no §7 row, no new pin file. Round 2 (critic): the examples'
  durable control is now full-array bit-exact (`expect_bit_exact` over all 5000 rows — the
  tail-only 1e-9 control was blind to the NaN prefix), the 24 helper docstrings are stripped to
  house form, and the red-first re-run (four mutations) all exit 1. `risk_tier: standard`. Branch
  `docs/ex-23-ta-a`. pins: ex-23-ta-a/C-001, C-002, C-003, C-004
- [ex-24-ta-b-ledger.md](ex-24-ta-b-ledger.md) —
  **EX-24 (2026-09-04), in flight:** the v1.1 example backfill's TA-kernels (b) batch — the
  remaining 45 `ta.*` backlog names at base `188499a6` (= `origin/main` at dispatch); all 45
  covered by twelve `docs/examples/ta/` files (backlog 258 → 213; `BACKLOG_BASELINE` 258 → 213)
  measured against the recorded C TA-Lib 0.4.0 goldens on the 5000-row OHLCV fixture (Spark has
  no TA kernels — the goldens are the family's oracle, the same `.bin` files
  `test_ta.py`/`test_ta_volume.py` pin bit-identically); all 45 bit-identical, zero divergences,
  no §7 row, no new pin file; the `over_columns`/`with_indicators` composition helpers are
  covered through fused examples whose every produced column is asserted bit-exact. Red-first:
  45 has-no-example findings with the files held out (exit 1), and the bit-exact control named
  kernel, row and both values on a bulk overwrite (exit 1). `risk_tier: standard`. Branch
  `docs/ex-24-ta-b`. pins: ex-24-ta-b/C-001, C-002, C-003, C-004
- [ex-25-functions-a-ledger.md](ex-25-functions-a-ledger.md) —
  **EX-25 (2026-09-05), in flight:** the v1.1 example backfill's `F.*` long-tail (a) batch —
  the 45-name roster at base `bc7c76cc` (= `origin/main` at dispatch); 20 names covered by five
  new `docs/examples/functions/` files plus the `F.hours` arm in `partition_transforms.py`
  (backlog 213 → 193; `BACKLOG_BASELINE` 213 → 193), every asserted value measured on live
  PySpark 4.1.2 (ANSI on, UTC); the other 25 stay with nineteen new §7 rows (EX-FN-1..19;
  `F.base64` keeps BL-17), pinned by twenty tests in `test_examples_functions_a.py`. No
  `csv_json.py`: all four CSV/JSON names refuse. Red-first: 20 has-no-example findings with
  the files held out (exit 1), and the wrong-median control in `stats.py` failed the execute
  leg by name (exit 1). `risk_tier: standard`. Branch `docs/ex-25-functions-a`.
  pins: ex-25-functions-a/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [ex-26-io-session-ledger.md](ex-26-io-session-ledger.md) —
  **EX-26 (2026-09-06), in flight:** the v1.1 example backfill's reader/writer/session long-tail
  batch — the 50-name roster at base `24932dee` (= `origin/main` at dispatch); 29 names covered
  by twelve new `docs/examples/{io,session,dataframe}/` files (backlog 193 → 164;
  `BACKLOG_BASELINE` 193 → 164), every asserted value measured on live PySpark 4.1.2 (ANSI on,
  UTC) or on repark's documented answer for the repark-only names; seventeen roster names keep
  their prior stays rows and the four excel names stay with the new §7 EX-IO-7 row, while eleven
  new rows (EX-IO-1..10, EX-SES-6) pin the diverged arms of covered names, with thirteen
  tests in `test_examples_io_session.py` (plus a dated EX-SES-1 Spark-half correction). Red-first: 29
  has-no-example findings with the files held out (exit 1), and the wrong-bytes control in
  `writer_csv.py` failed the execute leg by name (exit 1). `risk_tier: standard`. Branch
  `docs/ex-26-io-session`.
  pins: ex-26-io-session/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015
- [ex-27-ml-ledger.md](ex-27-ml-ledger.md) —
  **EX-27 (2026-09-05, round 2 2026-09-06), in flight:** the v1.1 example
  backfill's `ml.*` family — the 28-name roster at base `282607f5`; all 28 names
  covered by five `docs/examples/ml/` files (backlog 164 → 136). Round 2
  re-measured every oracle cell on live PySpark 4.1.2, including the
  session-level cells round 1 printed as "equal" without collecting. Nine §7
  rows (EX-ML-1..9) pin the diverged arms, with nine tests in
  `test_examples_ml.py`. Mixins are taught only through concrete stages.
  `risk_tier: standard`. Branch `docs/ex-27-ml`.
  pins: ex-27-ml/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [ex-28-scalar-remainder-ledger.md](ex-28-scalar-remainder-ledger.md) —
  **EX-28 (2026-09-06), in flight:** the v1.1 example backfill's `F.*` scalar
  remainder — the 34-name roster at base `57f21b9b`; seven names covered by
  extending three `docs/examples/functions/` files (backlog 136 → 129).
  Twenty-seven stay with existing EX-FN / BL-17 / FNP-15 / FNP-16 rows; two
  new §7 rows (EX-FN-20, EX-FN-21) pin `try_to_timestamp` and the
  `unix_timestamp` format arm. Every asserted value measured on live
  PySpark 4.1.2 (ANSI on, UTC). `risk_tier: standard`. Branch
  `docs/ex-28-scalar-remainder`.
  pins: ex-28-scalar-remainder/C-001, C-002, C-003, C-004, C-005, C-006
- [facade-audit-0-ledger.md](facade-audit-0-ledger.md) — Unit ledger — FACADE-AUDIT-0 step 1 · Half A: the Rust-backed facade facts
- [fn-fix-2-ctrl-1-controls-ledger.md](fn-fix-2-ctrl-1-controls-ledger.md) — **FN-FIX-2-CTRL-1
  (2026-09-04), in flight:** the seven incidental controls FN-FIX-2's critic found
  missing, measured on live PySpark 4.1.2 (both ANSI modes) and pinned; controls 2–7
  Spark-equal, control 1 (`regexp_extract`) refusal pinned on both doors
  (FINDING F-FN-FIX-2-CTRL-1-1, ACCEPTED_FLAGGED round-3; Spark `'alpha'`/`''`;
  flag superseded by FN-REGEXP-EXTRACT-1 — answer pin since merge `60ad77b0`);
  round-3 adds NULL `ltrim`/`rtrim` pins, the SQL `RLIKE`-keyword refusal pin
  (§7 FN-RLIKE-KEYWORD-1), and reversible ANSI legs.
  `risk_tier: standard`. Branch
  `fix/fn-fix-2-ctrl-1-controls`. pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [fn-fix-2-string-rows-ledger.md](fn-fix-2-string-rows-ledger.md) — **FN-FIX-2 (2026-09-04):**
  six silent string rows become Spark-equal (`FN-INITCAP-1`, `FN-CHR-1`,
  `FN-TRIM-CHARS-1`, `FN-ELT-1`, `FN-REGEX-POSIX-1`, `FN-LIKE-ESCEND-1`).
  pins: fn-fix-2-string-rows/C-001, C-002, C-003, C-004
- [fn-regexp-extract-1-ledger.md](fn-regexp-extract-1-ledger.md) — **FN-REGEXP-EXTRACT-1
  (2026-09-04):** Spark `regexp_extract(str, regexp[, idx])` on both doors (the last
  regexp kernel; closes the R-FN-BATCH1 gap the FN-FIX-2-CTRL-1 control exposed).
  Round 2: idx validated only inside the match arm; §7 `FN-REGEX-LOOKAROUND-1` filed;
  facade 2-arg widening disclosed.
  pins: fn-regexp-extract-1/C-001, C-002, C-003, C-004
- [fnp-8-review-ledger.md](fnp-8-review-ledger.md) —
  **FNP-8-REVIEW (2026-09-07), in flight:** remediation round 1 for FNP-8 (PR #412,
  merged unreviewed) — the round-1 critic's F1–F7, each red-first against live
  PySpark 4.1.2, plus the no-regression held set. `risk_tier: standard`. Branch
  `review/fnp-8-review`.
- [fnp-9-collections-json-ledger.md](fnp-9-collections-json-ledger.md) — **FNP-9/10
  (2026-09-05), in flight:** the collections and JSON function families. Ten names built
  Spark-equal on both Spark-facade doors (`get_json_object`, `json_array_length`,
  `json_object_keys`, `to_json`, `from_json`, `schema_of_json`, `create_map`, `map_concat`,
  `array_insert`, `arrays_zip`); seven §7 rows file what the unit measured and did not build,
  each with a pin that reds when the seam closes. Six unbuilt names share ONE seam — Spark's
  multi-column generators (`posexplode`, `posexplode_outer`, `inline`, `inline_outer`, `stack`,
  facade `json_tuple`) need a plan shape the facade select path does not have, so the seam is
  filed once instead of six one-column impostors. The JSON reader is hand-written so
  `Cargo.lock` stays untouched, and it is the better fit anyway — Spark keeps an integer token
  verbatim and re-renders the rest through `Double.toString`.
  Branch `feat/fnp-9-collections-json`.
  pins: fnp-9-collections-json/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009
- [h3-spill-1-ledger.md](h3-spill-1-ledger.md) — Round 3: C-004 counts 22 pins.
  **H3-SPILL-1 (2026-09-05), in flight:** the Never-OOM truth table. 180 cells (18 operators ×
  5 pool sizes × 2 scales), each a fresh subprocess on a release module under a resident-memory
  watchdog: **zero aborts, zero wrong answers**, and 115 of the 144 bounded cells carrying a
  disclosed content digest that equals the unbounded run (163 run digests once repeats are
  counted). Pins only — no product code changed. Round 2 answered eight critic findings, all
  about what the matrix checked and claimed rather than what it measured — a row count is not an
  answer digest, a repeat's digest must not be discarded, and a published error string must be
  one that was recorded. Two failure-shape defects filed as §7 BACKLOG rows
  with pins that red when fixed: `H3-SPILL-NLJ-1` (a nested-loop join at a tight pool answers
  with a caught Rust panic from DataFusion's `RepartitionExec`) and `H3-SPILL-COLLECT-1`
  (`collect()` under an address-space limit panics on a null `PyObject` instead of raising
  `MemoryError`). The document's honest limit is §2: windows, `Unnest`, the Iceberg scan and the
  facade boundary take no pool reservation at all, so no pool bounds them. `risk_tier: standard`.
  Branch `harden/h3-spill-1`.
  pins: h3-spill-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [h3-spill-residue-1-ledger.md](h3-spill-residue-1-ledger.md) —
  **H3-SPILL-RESIDUE-1 (2026-09-06), in flight:** the two Never-OOM failure shapes H3-SPILL-1
  filed and did not fix. `collect()` under an `RLIMIT_AS` ceiling now raises `MemoryError`:
  every CPython allocation on the row fast path goes through `Bound::from_owned_ptr_or_err`,
  because pyo3's safe constructors reach `assume_owned` and panic on NULL **even where the
  signature returns `PyResult`** — and that panic consumes the `MemoryError` on its way out, so
  catching it later cannot recover it. A nested-loop join at a bounded pool now refuses with the
  same typed exception every other operator gives: a bounded session's `FairSpillPool` is wrapped
  in `RefusalRecordingPool`, and the Arrow reader reports a fenced panic that a recorded refusal
  caused as that refusal. The DataFusion defect behind it is **upstream and still open** — 54.1's
  `NestedLoopJoinExec` re-executes partition 0 of its build child on the OOM fallback path — and
  the issue text is in the ledger; no dependency changed. Measured before and after on release
  modules: the matrix's only `internal_error` cell is `clean_error` 3/3, the other 17 operators
  at 8 MiB are identical cell for cell, and the `collect` happy path's two five-run distributions
  overlap. **Round 2 (2026-09-06)** answered five critic findings, one of them S1: the containment
  rule was unbounded — an injected `index out of bounds` panic after one refusal came back as a
  pool refusal — so a fourth gate now requires the payload to be one DataFusion 54.1 can reach on
  its refusal and spill-fallback paths, cited line by line. The scope claim was corrected rather
  than the code: the refusal log is session-scoped, not per-stream, and cannot be per-stream. Two
  more honest-limits disclosures landed: a contained refusal still prints 4 panic blocks to
  stderr, and `toPandas()` under a 64 MiB address-space headroom aborts the process where
  `collect()` raises `MemoryError`. Seven mutations, seven kills. `risk_tier: elevated`.
  Branch `harden/h3-spill-residue-1`, PR #401.
  pins: h3-spill-residue-1/C-001, C-002, C-003, C-004, C-005
- [ledger-reading-1-ledger.md](ledger-reading-1-ledger.md) —
  **LEDGER-READING-1 step 1 (2026-09-09), in flight:**   reading units may prove clauses on
  document evidence (R-10). A staging ledger whose first 40 lines carry the READING value of
  the `Path` header field is exempt from the grammar gate's rule B; rules A and C stay armed,
  the reading evidence shape is `docs: <path>#<heading-anchor>` (a convention, not a gate
  check), and `EXCEPTIONS` is untouched. Step 2 (2026-09-09) flipped BALLISTA-AUDIT-0's
  twelve clauses to `PROVEN` on `docs:` cells. Branch `feat/ledger-reading-1`.
  pins: ledger-reading-1/C-001, C-002, C-003, C-004
- [maint-policy-1-ledger.md](maint-policy-1-ledger.md) — Unit ledger — MAINT-POLICY-1 step 1 · typed `[<profile>.maintenance]` policy
- [neveroom-1-ledger.md](neveroom-1-ledger.md) —
  **NEVEROOM-1 steps 1–3 (2026-09-10/11), in flight:** the spill-coverage matrix
  harness, the full 27-cell run, and the CI golden. Step 3 (S2-18) pins
  `ci_golden.csv` and `test_ci_tier_matches_golden` (C-008), the committed
  27-cell CSV pin with the three R-1 cells (C-009), and the `docs/testing.md`
  plus `## Never-OOM at v1.3` pointers (C-010). The three non-outcome cells
  stay as measured; the ledger does not move to `completed/` in this step.
  `risk_tier: standard`. Branch `feat/neveroom-1-step-3`.
  pins: neveroom-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [nightly-live-1-ledger.md](nightly-live-1-ledger.md) —
  **NIGHTLY-LIVE-1 (2026-09-11):** the parity-live nightly red since 2026-09-05 — a test
  calling `.stop()` on a PySpark session kills the one JVM `SparkContext` under the shared
  oracle. The conftest `_shared_oracle_context_guard` fails the stopper by name (red-first:
  `test_group_agg.py::test_cross_engine_collect_and_multi_count_distinct_vs_pyspark`); every
  PySpark teardown stop removed, `ReparkSession.stop()` kept; shared-catalog sites given
  private catalog names and the avg-overflow leg pins `leafNodeDefaultParallelism`. Full
  live suite: 6298 passed, 0 failed on this box. `risk_tier: standard`. Branch
  `fix/nightly-live-1`.
  pins: nightly-live-1/C-001, C-002, C-003, C-004
- [perf-agg-avg-1-ledger.md](perf-agg-avg-1-ledger.md) —
  **PERF-AGG-AVG-1 (2026-09-05), in flight:** the `GroupsAccumulator` for the Spark
  `avg` / `try_avg` UDAF (PERF-ANALYSIS-1 slate item 8, candidate 10) — Float64 and
  Decimal32/64/128/256 grouped paths with Spark's result rules and `try_avg`
  overflow → NULL on the 2×-MAX shape (`AVG-DEC-SUMWRAP-1` files the wrap shape),
  the retract path untouched for window frames. 5 PROVEN, 0 OPEN,
  1 REJECTED (Q17 ≤ 3× missed with the sum-floor proof; avg/sum ≤ 1.3× met); gates
  green, attestation filed. `risk_tier: standard`. Branch `perf/agg-avg-1`.
- [perf-dynflatten-1-measure-ledger.md](perf-dynflatten-1-measure-ledger.md) — Charter ledger — PERF-DYNFLATTEN-1 · measure `dynamicFlatten`
- [perf-dynflatten-2-null-mask-ledger.md](perf-dynflatten-2-null-mask-ledger.md) — Charter ledger — PERF-DYNFLATTEN-2 · the null-mask struct extractor
- [perf-facade-1-ledger.md](perf-facade-1-ledger.md) — Unit ledger — PERF-FACADE-1 · `collect()` rows in the binding, `withColumn` chains made linear
- [perf-facade-cdf-1-ledger.md](perf-facade-cdf-1-ledger.md) — Unit ledger — PERF-FACADE-CDF-1 · `createDataFrame(list of tuples)` goes column-wise
- [perf-ice-catalog-io-1-ledger.md](perf-ice-catalog-io-1-ledger.md) —
  **PERF-ICE-CATALOG-IO-1 (2026-09-05), in flight:** the catalog-IO unit at base `6eaccd5e`.
  Shipped: a session-scoped Iceberg metadata cache keyed by metadata-file **location**, built once
  per session and handed to every **memory** catalog it builds, behind `repark.iceberg.metadataCache`
  (default on) and `repark.iceberg.metadataCacheEntries` (default 512, a high-water clear at the
  statement door). `metadata.json` READS fall from 2 (SELECT) and 3–6 (DML) to **0 on every
  statement that reads an existing table** — the analysis' §7.6 TOTALS split into reads and the
  commit's own write, reads + writes reproducing §7.6 exactly. `CREATE TABLE` and CTAS still read
  1 with the knob on and off: creation is not cacheable. `PERF-CATALOG-CALLS-1` FIXED **narrowly**
  — the metadata document is fetched once per location; the count of catalog round trips per
  statement is UNCHANGED, and Glue / S3 Tables are NOT wired at all.
  Fork-gated and NOT shipped, one registry row each: one load per planning round
  (`PERF-CATALOG-LOADS-1` / `F-CATIO-A`), the shared path-keyed manifest cache
  (`PERF-ICE-MANIFEST-1` / `F-CATIO-B`), the Glue and S3 Tables metadata cache
  (`PERF-CATALOG-AWS-CACHE-1` / `F-CATIO-AWS`) and a bounded LRU inside the fork's cache
  (`PERF-CATALOG-CACHE-BOUND-1` / `F-CATIO-BOUND`). A and B are implemented and test-green in
  `$HOME/repark-lanes/lanes/catio-fork` and measured through a temporary, never-committed path
  override: `t_many` second-statement `count_id` **120.01 → 11.33 ms** (target ≤ 20) with a
  repeated read opening no manifest at all. Three pins SKIP naming their ask;
  `git diff origin/main -- Cargo.toml Cargo.lock` empty.
  Risk-first: **twelve** Rust pins on two doors over ONE catalog plus **thirteen** always-run
  Python legs, green before and after, with a **six**-mutation score (two of them escapes that
  were closed). **Round 2** (Opus critic) reproduced the engine independently and found no stale
  read or lost write; its eleven findings were all claim, scope and filing, and each is dispositioned
  in the ledger's "Round 2 — review gaps" table. `risk_tier: elevated`. Branch
  `perf/ice-catalog-io-1`.
  pins: perf-ice-catalog-io-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [perf-ice-catalog-io-2-ledger.md](perf-ice-catalog-io-2-ledger.md) —
  **PERF-ICE-CATALOG-IO-2 (2026-09-05), landed default-OFF per the round-2 ruling:** the
  RePark side of CATALOG-IO part 3 at fork pin `79119643` (RP-12, already on the base). A
  session key `repark.iceberg.manifestCacheBytes`
  (alias `repark.iceberg.manifest_cache_bytes`, default `0` = off, set bytes to opt in)
  sizes the fork's shared manifest `ObjectCache` for the memory catalog; every table the
  catalog materializes carries the one cache. Shipped: the part-3 pin un-skipped and green
  with the knob set explicitly (`t_many/count_id/stmt2` **115.81 → 10.95 ms**, target ≤ 20;
  repeated reads open no manifest at all); the six IO-1 staleness pins re-run green with
  the cache off (the default) plus new explicit-knob Python legs per cell (MERGE, DROP +
  re-CREATE, `register_table`, rewrite + expire, time-travel, branch); the funnel pinned
  by manifest deletion; correctness pinned under a 512-byte budget over eight tables; two
  lineage detector pins holding `PERF-CATALOG-LINEAGE-CACHE-1`'s shape (knob-on NULLs,
  knob-off assigned). `PERF-ICE-MANIFEST-1` BACKLOG with before/after;
  `PERF-CATALOG-CACHE-BOUND-1` NARROWED to the metadata cache;
  `PERF-CATALOG-COMMIT-CACHE-1` / `F-CATIO-COMMIT` filed BACKLOG — the
  census showed DML saving read-side repeats only, because the fork's transaction paths never
  consult the cache (0 vs 166+ direct loads), which re-reads but never serves stale. Glue and
  S3 Tables are NOT wired (their builders have no such method at the pin). `git diff
  origin/main -- Cargo.toml Cargo.lock` empty.
  Risk-first: **five** Rust parse pins plus **four** Rust delete-manifest pins plus **thirteen**
  Python legs, with a **seven**-mutation score (one escape closed: the knob-off control parsed
  no string until it was strengthened to). The in-lane critic pass found two claim-scope
  overstatements (the map's "every table shares", the unrecorded `memory_catalog()` behaviour
  change) and both are remediated in the ledger's critic table.
  **FINDING S1-1 (C-004 REJECTED), resolved by the round-2 ruling:** the unit HALTED mid-flight
  when the facade suite redded 4 upgrade-lineage tests — the fork's `(path, schema)` manifest
  key does not carry the list-entry lineage range, so a v2-context parse poisons v3 reads of
  the same path (`_row_id` NULL). The ruling landed option (b): default OFF, so the four tests
  are green by default and the fix (`F-CATIO-KEY`, separate fork unit) plus a later default-ON
  flip close the remainder. Filed `PERF-CATALOG-LINEAGE-CACHE-1` / `F-CATIO-KEY` (fork-side,
  no RePark fix exists). `risk_tier: standard`. Branch `perf/ice-catalog-io-2`.
  pins: perf-ice-catalog-io-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [perf-ice-catalog-io-3-ledger.md](perf-ice-catalog-io-3-ledger.md) —
  **PERF-ICE-CATALOG-IO-3 (2026-09-05), landed default-ON:** the flip IO-2's round-2
  ruling named as the follow-up, on the fixed pin `2ed39cb0` (RP-13, `F-CATIO-KEY`).
  `DEFAULT_MANIFEST_CACHE_BYTES` is 32 MiB; the four HALT tests, the staleness
  battery and the lineage pins run on default sessions and are green; a two-session
  concurrency leg and a 500-table subprocess RSS comparison (332.2 vs 323.9 MB, delta
  8.3, bar 64) prove the fork-fix contract and the bound.
  `t_many/count_id/stmt2` **123.47 → 11.27 ms** on the default session (target ≤ 20);
  `PERF-ICE-MANIFEST-1` FIXED with the default-session number;
  `PERF-CATALOG-CACHE-BOUND-1` narrowed with the measured RSS. Charter committed
  red-first (the default pins redded on the base, then green after). 7 PROVEN, 0 OPEN.
  `risk_tier: elevated`. Branch `perf/ice-catalog-io-3`.
  pins: perf-ice-catalog-io-3/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [perf-ice-writepath-1-ledger.md](perf-ice-writepath-1-ledger.md) —
  **PERF-ICE-WRITEPATH-1 (2026-09-05), in flight:** the two write-path defects PERF-ANALYSIS-1
  ranked together, because both are read off the same CTAS pair. Fork half **F-28**: the
  partition splitter groups a batch with Arrow kernels and materializes one `Literal::Struct`
  per group instead of one per row, keeping the row-wise path where Arrow total-order equality
  is not Iceberg `Struct` equality. RePark half: `IcebergPartitionWriteExec`, a CTAS write node
  with one output partition per writer, so the parquet encode and zstd run on the executor's
  threads instead of sharing one task — no `tokio::spawn`, no new dependency. The commit is an
  ordering, not a layout: the manifest ascends by content and `_row_id` tiles it contiguously,
  while the layout and a row's `_row_id` vary with the scan's file grouping
  (`WRITE-GROUPING-CTAS-1`); a failed write into a fresh table deletes every data file it made. `risk_tier: elevated`. Branch `perf/ice-writepath-1`.
  pins: perf-ice-writepath-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011
- [perf-scan-1-plan-once-ledger.md](perf-scan-1-plan-once-ledger.md) — Charter ledger — PERF-SCAN-1 · plan the identity DELETE / MERGE target scan once
- [preflight-parity-1-ledger.md](preflight-parity-1-ledger.md) —
  **PREFLIGHT-PARITY-1 (2026-09-09), in flight:** the CAP-1 source-file mirror joins
  `make preflight` as `make py-test-parity-cap` — the mirror file alone
  (`test_cap_1_source_file_line_cap.py`), `py-test`'s isolated interpreter recipe verbatim,
  measured 1.428 s against the 60 s budget, seated after `py-test-facade` before `audit`;
  `verify` untouched. A size-gate ratchet that updates only `scripts/check_lib_py.py` now reds
  the pre-PR gate locally instead of failing CI's Python job (#427). `risk_tier: standard`.
  Branch `feat/preflight-parity-1`.
  pins: preflight-parity-1/C-001, C-002, C-003, C-004, C-005
- [profiles-1-ledger.md](profiles-1-ledger.md) — Unit ledger — PROFILES-1 steps 1–3 · the measurement bed, the knob sweep, and the two profiles
- [rdf-schema-evo-1-ledger.md](rdf-schema-evo-1-ledger.md) — §10 records the RP-15 bump and the RePark-side critic PASS.
  **RDF-SCHEMA-EVO-1 (2026-09-06), in flight:** `rewrite_data_files` after schema evolution —
  the owner's 7v8 refusal, reproduced on the pinned fork for add (+spec), add-only, drop,
  rename, INT→BIGINT promotion and v3-with-DV, each with no later write, and measured on
  live PySpark 4.1.2. Eleven facade pins red on `8bc325a3`, green on fork
  `fix/rdf-schema-evo-1` (`8ef7ef5b`); the registry row flips BACKLOG → FIXED when the
  orchestrator bumps the pin. No RePark production code, no dependency move.
  `risk_tier: standard`. Branch `fix/rdf-schema-evo-1`.
  pins: rdf-schema-evo-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [review-fix-1-ledger.md](review-fix-1-ledger.md) — Unit ledger — REVIEW-FIX-1 · the CFG-1 mirror agrees with the loader
- [review-fix-10-ledger.md](review-fix-10-ledger.md) —
  **REVIEW-FIX-10 (2026-09-10), in flight:** `is_reading` parses the `Path` header field —
  the marker matches anywhere in the first 40 lines except inside backtick code spans, the
  value is the leading identifier run compared to `READING` exactly (D-1 as amended by
  R10-A/R10-B, recorded as D-1a). Four scratch-ledger pins, red first. `risk_tier: standard`.
  Branch `fix/review-fix-10`.
  pins: review-fix-10/C-001
- [review-fix-11-ledger.md](review-fix-11-ledger.md) — Unit ledger — REVIEW-FIX-11 step 1 · the unpinned `explain()` rows (Q-32, with Q-17)
- [review-fix-12-ledger.md](review-fix-12-ledger.md) —
  **REVIEW-FIX-12 step 1 (2026-09-10), in flight:** the docs-links gate measures what it
  claims — rendered-text slugs, final-slug duplicates, table-cell-only `docs:` cells, the
  unclosed-fence finding, the absolute-target skip, same-file anchors (which caught the
  stale V3-COV-8 fragment at `docs/spark-sql-iceberg-parity.md:3053`, repaired in the same
  commit), and the three-space fence rule. `risk_tier: standard`. Branch
  `fix/review-fix-12`.
  pins: review-fix-12/C-001, C-002, C-003, C-004, C-005, C-006
- [review-fix-13-ledger.md](review-fix-13-ledger.md) —
  **REVIEW-FIX-13 (2026-09-11), in flight:** the Ballista audit is trued up —
  `state/` 16427 split 7491 + 8936 with the producing commands, the per-session
  seat renamed `SessionBuilder` with its tag signature, R-7 split into default
  (`axum`, AWS credential stack) versus optional (`prometheus-metrics`,
  `graphviz-support`, `keda-scaler`) with `cargo tree` proof, the A1
  placeholder replaced by a runnable counter (23249 test-only lines), and the
  step-3 map line landed. Closes Q-42, Q-43, Q-44, Q-45, Q-53, Q-54.
  `risk_tier: standard`. Branch `fix/review-fix-13-15`.
  pins: review-fix-13/C-001, C-002, C-003, C-004, C-005
- [review-fix-14-ledger.md](review-fix-14-ledger.md) — Unit ledger — REVIEW-FIX-14 step 1 · the display configuration keeps its promises
- [review-fix-15-ledger.md](review-fix-15-ledger.md) —
  **REVIEW-FIX-15 (2026-09-11):** the reading fixture's Verdict/Evidence cells
  swap to the grammar-described order; its first-cut `_pins` bindings left the
  code under S2-14 (REVIEW-FIX-15b), so every citation is map-navigated.
  Closes Q-48, Q-49. `risk_tier: standard`. Branch `fix/review-fix-13-15`.
  pins: review-fix-15/C-001, C-002
- [review-fix-15b-ledger.md](review-fix-15b-ledger.md) —
  **REVIEW-FIX-15B (2026-09-11):** ruling S2-14 — REVIEW-FIX-15's four
  `_pins = "pins: …"` code-as-data bindings in `test_dl_2_ledger_grammar.py` are the
  comment ban routed through a variable, so they are deleted with no replacement
  construct; the citations they carried (`ledger-reading-1/C-001..C-003`,
  `review-fix-15/C-001, C-002`) and this unit's own move to the DL-2 bullet of
  `python/repark-parity/tests/map.md`. `risk_tier: standard`. Branch
  `fix/review-fix-15b`.
  pins: review-fix-15b/C-001, C-002
- [review-fix-2-ledger.md](review-fix-2-ledger.md) — Unit ledger — REVIEW-FIX-2 · CFG-1's own pins stop reading the developer's HOME
- [review-fix-3-ledger.md](review-fix-3-ledger.md) — Unit ledger — REVIEW-FIX-3 step 1 · the polars display honours every legal `max_rows`
- [review-fix-4-ledger.md](review-fix-4-ledger.md) — Unit ledger — REVIEW-FIX-4 · the eager frame's checkpoint paths (Q-12, Q-13)
- [review-fix-5-ledger.md](review-fix-5-ledger.md) — Unit ledger — REVIEW-FIX-5 step 1 · DESCRIBE metadata-name intercept, Owner, short names, redaction
- [review-fix-6-ledger.md](review-fix-6-ledger.md) — Unit ledger — REVIEW-FIX-6 step 1 · `explain()` refuses the both-set shape (Q-17)
- [review-fix-7-ledger.md](review-fix-7-ledger.md) —
  **REVIEW-FIX-7 step 1 (2026-09-10), in flight:** the sweep's security-shaped fixes —
  header-injection close in the Python mirror (refuse + quote), secret-free TOML parse
  errors in the engine, and the RF-3 one-warning disclosure for ambiently discovered
  cloud catalogs. 3 PROVEN, 0 OPEN. `risk_tier: standard`. Branch `fix/review-fix-7`.
  pins: review-fix-7/C-001, C-002, C-003
- [review-fix-8-ledger.md](review-fix-8-ledger.md) — Unit ledger — REVIEW-FIX-8 · the PROFILES-1 probe is re-runnable and its table is true
- [review-fix-9-ledger.md](review-fix-9-ledger.md) — Unit ledger — REVIEW-FIX-9 step 1 · the polars renderer matches polars at its boundaries
- [rp-10-repin-f25-ledger.md](rp-10-repin-f25-ledger.md) — Charter ledger — RP-10 · fork repin 594bdbe5 → 85a4aaf0 (consume F-25; close PERF-DVCLOSE-STMT-1)
- [sql-describe-1-ledger.md](sql-describe-1-ledger.md) — Unit ledger — SQL-DESCRIBE-1 · `DESCRIBE [TABLE] [EXTENDED|FORMATTED]` on Iceberg tables (step 1: measurement)
- [sql-harden-1-cutover-shapes-ledger.md](sql-harden-1-cutover-shapes-ledger.md) — Unit ledger — SQL-HARDEN-1 · the cutover pipeline cutover Iceberg SQL shapes
- [sql-harden-2-cow-shapes-ledger.md](sql-harden-2-cow-shapes-ledger.md) — Unit ledger — SQL-HARDEN-2 · copy-on-write cutover shapes
- [torture-1-ledger.md](torture-1-ledger.md) —
  **TORTURE-1 steps 1–5 (2026-09-11):** the torture-test dataset suite —
  step 1: the `repark_parity.torture` generator package (checkout-only `__path__` graft,
  `generate` CLI writing Parquet + CSV, ci/full tiers, one `Family` protocol), the `nested`
  and `inference` families, the both-door suite skeleton, the tiered `make py-test-torture`
  target, and registry row `CSV-INFER-INT32-WIDTH`; step 2: the `extreme_types`,
  `smartcsv`, `temporal` and `decimal_overflow` families with both-door cells, registry
  rows `CSV-INFER-HEADER-CASE`, `SUM-DEC-I128WRAP-1`, `DATE-INTERVAL-NSBOUND-1`, and the
  no-live-Spark labeling; step 3: the `secrets` family (13 credential-shaped columns from
  the `prop_key_is_secret` needle set beside `bucket_key` and three ordinary controls)
  and the `flag_secret_columns = off|warn|refuse` read option honored on the csv/json
  readers and refused loud elsewhere, pinned on both doors; step 4: the `v3_dv` family —
  a live-Spark-only generator writing a many-file format-v3 merge-on-read Iceberg table
  with Puffin deletion vectors under `REPARK_PARITY_LIVE=1`, the ≤ 1 MB committed fixture
  under `fixtures/torture/data/v3_dv/` with its `truth.json`, and both-door cells pinning
  the true post-delete count on repark (96 of 120, no registry divergence — repark reads
  the DVs correctly). Step 5 pending (the full-tier results document).
  `risk_tier: standard`. Branches `feat/torture-1`, `feat/torture-1-s2`,
  `feat/torture-1-step-3`, `feat/torture-1-step-4`.
  pins: torture-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022,
  C-023, C-024, C-025, C-026, C-027, C-028, C-029, C-030
- [win-slide-1-ledger.md](win-slide-1-ledger.md) — **WIN-SLIDE-1 (2026-09-04), in flight:** the
  thirteen aggregates that refused over a sliding frame now answer Spark-equal on both doors.
  One mechanism, not thirteen: a `sliding_frame_rescan` analyzer rule on every core session
  re-evaluates the frame per row into a fresh accumulator when DataFusion's sliding accumulator
  cannot retract — by capability, so a future aggregate never refuses. The physical `WindowExpr`
  route is closed in DF 54.1 (`WindowFn` is unexported) and §7.2 names the gap. Two door bugs
  found and fixed on the way (`WIN-RANGE-DF-1`, `WIN-COLLECT-DOOR-1`) and the frame case of the
  `percentile_approx` accuracy divergence filed (`WIN-SLIDE-PCT-ACC-1`). `risk_tier: standard`.
  Branch `feat/win-slide-1`.
  pins: win-slide-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [write-distribution-1-ledger.md](write-distribution-1-ledger.md) —
  **WRITE-DISTRIBUTION-1 (2026-09-06), in flight:** the hash distribution rule before a
  partitioned Iceberg write — Spark's `write.distribution-mode = hash`. A `RepartitionExec` under
  the CTAS write node, `Partitioning::Hash` over one `PartitionTransformExpr` per partition field
  (the fork's transform over the cast source column), so one partition value lands in one writer:
  the partitioned 1e6 CTAS goes 64 → 8 data files (Spark's count) and 3.44× → 1.96× of the
  parquet-sink control; the unpartitioned CTAS is untouched by decision. No dependency, no spawn.
  `risk_tier: standard`. Branch `perf/write-distribution-1`.
  pins: write-distribution-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [write-order-dist-1-ledger.md](write-order-dist-1-ledger.md) —
  **WRITE-ORDER-DIST-1 (2026-09-06), in flight:** `ALTER TABLE … WRITE ORDERED BY` /
  `WRITE DISTRIBUTED BY` and the write properties they set. A pre-parse DDL module plus
  a one-transaction sort-order/property primitive over the fork's `replace_sort_order`;
  `write.distribution-mode` gating in `hash_distribution`; per-writer sorting in the two
  funnel entries. `risk_tier: standard`. Branch `feat/write-order-dist-1`.
  pins: write-order-dist-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
