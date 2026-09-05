# map — python/repark/tests

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).
CC-2 close: S3 Tables location-guard phrase kept contiguous in `test_aws_acceptance.py`.

## Purpose

Facade tests for the `repark` wheel — they require the compiled native module and exercise the
real boundary end to end. See [../map.md](../map.md). Q1 re-home (2026-08-14): imports target
`repark.spark.*`; `test_sql_alias.py` pins failing `import repark.sql` + the ANSI callable.
S-1's `test_decimal128_parity.py` / `test_sql_passthrough_parity.py` /
`_record_decimal128_goldens.py` keep old import lines (SQM union).

**This suite IS the full-extras facade census cohort** (`docs/design/python-facade.md` §6.3): the
recorded acceptance run installs the built **wheel** by explicit file path into a bare interpreter
outside the workspace, with `numpy`/`pandas`/`polars`/`ml-ext` present, `pyspark` and `duckdb`
absent, no JVM on `PATH`, and every `REPARK_*` gate variable unset by name. A `maturin develop`
run is the fast local loop, never the acceptance artifact (design §8, the real-artifact rule).

**Ported minus the declared EC-4 deferral list** —
[../../../task/port/deferred-python-tests.txt](../../../task/port/deferred-python-tests.txt) (12
node ids: the whole `test_excel_reader.py` file plus one node each in `test_pg_catalog.py` and
`test_pg_jdbc_options.py`). Each entry is annotated in place below. A test that is missing and
NOT in that file is a defect, not a decision.


Docstrings here are one line each: `check_docstring_presence` (D101/D102/D103/D105/D107)
requires one, and nothing may say more. Reasons live in this map, not in the source.

## Contents

CC-2 slice complete: every module's comments and docstrings audited; oracle discriminators,
mutation payloads, pins, and safety contracts kept, narration and round history deleted.

- [test_win_slide_1.py](test_win_slide_1.py) — **WIN-SLIDE-1 (2026-09-04):** the sliding-frame
  corpus. One eight-row typed seed (`id` INT so the `range_frame` shape also pins `WIN-RANGE-DF-1`,
  `g` for the partition boundary, NULLs in `v` / `v2` / `vi` / `b`, all-NULL `vn` / `vin` / `bn`,
  and `ov` at `Long.MaxValue` for the `try_sum` overflow leg) drives thirteen aggregates over five
  frame shapes — `rows_both`, `range_frame`, `all_null`, `empty_frame`, `partitioned` — on the SQL
  door and the DataFrame door, against `SPARK_GOLDEN`, recorded from live PySpark 4.1.2 on
  2026-09-04. Spark's two doors agreed on every one of the 65 cells, so one golden serves both.
  `collect_set` is compared as a sorted multiset (Spark leaves the order unspecified); floats
  compare to a 1e-9 relative tolerance, which the re-scan meets exactly because it never retracts.
  The `repark_engine` fixture is function-scoped on purpose: the suite's autouse
  `_isolate_active_session` stops the active session between tests, so a module-scoped one is dead
  by the second case. The two live legs re-derive every pin on Spark under
  `REPARK_PARITY_LIVE=1`. **Round 2 (2026-09-04)** adds three rows the critic found unpinned:
  the catastrophic-cancellation fixture `v = [1e16, 1.0, 1.0, 1e16, 1.0, 1.0]`, which separates a
  RETRACTING sliding `sum` / `avg` from Spark's re-scan (`WIN-SLIDE-FLOAT-1`) and whose controls
  are split three ways on measurement — bit-identical, one-ulp drift, and not-comparable (Spark
  refuses `median` over a frame and its ANSI `corr(v, v)` divides by zero); the DATE / TIMESTAMP
  RANGE order-key rows (`WIN-RANGE-ERRCLASS-1` for the DataFrame door's error class,
  `WIN-RANGE-DF-1`'s scope for the SQL door); and the two sketch goldens, which
  `WIN-SLIDE-PCT-ACC-1` published under a live-oracle line that no live test had touched. The
  accuracy-knob test's last assertion was two constants and is now repark's own answer against
  Spark's. pins: win-slide-1/C-001, C-002, C-003, C-004, C-007
- **Round 4:** the bed loader in these tests uses `package.__dict__["__path__"]`, not an
  attribute assignment needing a `# type:` pragma.
- [test_dynflatten_bed_gate.py](test_dynflatten_bed_gate.py) — **PERF-DYNFLATTEN-1
  (2026-09-04):** gate-scale bed parquet flattens on repark (struct_d3 /
  cartesian / null_typed_list). pins: perf-dynflatten-1-measure/C-001, C-002
- [test_dynflatten_null_mask.py](test_dynflatten_null_mask.py) — **PERF-DYNFLATTEN-2
  (2026-09-04):** the before/after correctness pin, one row per bed shape. `ROWS` holds,
  per shape, the row count, the Arrow schema string (names, types, nullability) and the
  SHA-256 over the ordered `to_pylist()` rows of the gate-scale flattened table, captured
  from a module rebuilt without the extractor. The raw IPC bytes were rejected as the pin
  because they differ under cleared validity bits on five shapes (ledger §8): the payload
  under a null is don't-care, the rows and schema are not. `test_every_bed_shape_is_covered`
  refuses a shape the bed grows later without a row. pins: perf-dynflatten-2-null-mask/C-001, C-003
- [test_dynamic_flatten_divergences.py](test_dynamic_flatten_divergences.py) —
  **PERF-DYNFLATTEN-1:** `DYNFLATTEN-QUALNAME-1`, measured over keep ∈ {none, `id`, `k`} ×
  depth 1–4. Split out of `test_dynamic_flatten.py` to hold that file at its 1618-line ceiling.
  **PERF-DYNFLATTEN-2 (2026-09-04) turned it into an answer pin.** The extractor closed the
  divergence as a side effect — the leaf-projection rule was choking on the `get_field` inside
  the per-leaf CASE, and there is no longer a `get_field` to hoist — so the two refusal pins
  and their control became one pin over the same 12 cells, asserting the columns and the row
  values live PySpark 4.1.2 returns for each. The file keeps its name and its module docstring:
  the row it cites is the same row, now FIXED, and renaming would break the registry citation.

  | pin | holds |
  |---|---|
  | `test_keep_column_beside_any_struct_depth_collects_the_spark_row` | all 12 cells collect; `[keep, Payload_L…_Val]` and `{keep: 1, leaf: 9}` |

  Mutation: `null_mask_extractable` → `false` restores the pre-extractor CASE, and that build
  was measured byte-identical to `main` on all eleven bed shapes; the 6 cells with a keep column
  at depth ≥ 2 are exactly the ones `main` raised on (the two refusal pins were green there), so
  the mutation reds **6 of 12** and the other 6 stay green.
  pins: perf-dynflatten-1-measure/C-003
  pins: perf-dynflatten-2-null-mask/C-002
- [test_parity_live_dynflatten.py](test_parity_live_dynflatten.py) — **PERF-DYNFLATTEN-1:**
  the live dynamicFlatten legs, split out of `test_parity_live.py` when main's growth pushed
  that module past its 1000-line ceiling (ceilings only move down).
  `test_live_dynflatten_matches_spark_explode` hands BOTH engines `read.parquet` of one file;
  that symmetry surfaced `DYNFLATTEN-READNULL-1` (repark keeps a parquet `required` column
  non-nullable, Spark widens it). Co-collects with the disclosure legs on the shared
  session-scoped `spark_engine`, which moved to `conftest.py` so one JVM serves both modules.
  **PERF-DYNFLATTEN-2** keeps this leg as its live gate: both engines `read.parquet`, so the
  null-mask extractor is exercised on the path where leaf projection pushdown lives, not only on
  the `createDataFrame` path the bench measures. 17 passed / 105 deselected, unchanged.
  **CUTOVER-SCHEMA-1 (2026-09-04):** the reader relax converged the row — repark `id` now
  nullable like Spark, so the pin asserts `True` on both sides. DYNFLATTEN-READNULL-1 FIXED.
  pins: perf-dynflatten-1-measure/C-002, C-003
  pins: perf-dynflatten-2-null-mask/C-004
  pins: perf-dynflatten-1-measure/C-002, C-003
  pins: cutover-schema-1/C-001
- [test_ctas_view_typed.py](test_ctas_view_typed.py) — **CTAS-VIEW-1 (2026-09-03):** parquet
  file → `read.format('parquet')` → `createOrReplaceTempView` → unpartitioned
  `CREATE TABLE … USING iceberg AS SELECT *` into the memory catalog; read-back equals
  the source (value). pins: ctas-view-1-conform-stream/C-001, C-004
- `test_pr_245_revalidation.py` — PR #245 public-door revalidation for Spark string literals,
  binary casts, parser limits, and facade controls.
- [test_bl15_bl16_math_divergences.py](test_bl15_bl16_math_divergences.py) — **BL-15 FIXED
  (LOG1P-1, 2026-09-02):** `F.expm1` is the precise kernel (`math.expm1`); BL-16 hypot
  still overflows to `inf` at extreme magnitude. pins: log1p-1-precise-kernels/C-005
- [test_fn_arrays_divergence.py](test_fn_arrays_divergence.py) — **FN-FIX-1 (2026-09-03):** (module docstring is the forced one-liner; the EX-8 and FN-FIX-1 pins are cited on this row, not in the file)
  Spark-equal array pins — FN-ARRAYPOS-1 not-found `0`, FN-ARRAYSORT-1 NULLs last,
  FN-ARRAYSOVERLAP-1 three-valued, FN-FLATTEN-1 NULL sub-array → NULL row.
  pins: fn-fix-1-registry-rows/C-003
  pins: ex-8-functions-arrays/C-001
- [test_fn_elt_out_of_range.py](test_fn_elt_out_of_range.py) — **FN-FIX-2 (2026-09-04):**
  FN-ELT-1. Out-of-range `elt` (index 3, 0, −1) raises `INVALID_ARRAY_INDEX`; NULL
  `n` is NULL; in-range 1/2 agree.
  pins: fn-fix-2-string-rows/C-001, C-003, C-004
  **FN-FIX-2-CTRL-1 (2026-09-04):** ANSI-off out-of-range and NULL `n` answer NULL.
  pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [test_fn_regex_posix_class.py](test_fn_regex_posix_class.py) — **FN-FIX-2 (2026-09-04):**
  FN-REGEX-POSIX-1. `regexp_count` / `rlike` / `regexp_replace` of `[[:alpha:]]` match
  Spark's Java union bracket (`[1, 0, 4]` / `[True, False, True]` / `'##bb##'`).
  pins: fn-fix-2-string-rows/C-003
  **FN-FIX-2-CTRL-1 (2026-09-04):** `[[:alpha:]x]` matches `'x'` and `'fox'` via
  `rlike` / `regexp_like` / SQL `regexp_like`; `regexp_extract` answer pinned (after FN-REGEXP-EXTRACT-1) on
  both doors (FINDING F-FN-FIX-2-CTRL-1-1, ACCEPTED_FLAGGED; Spark: `'alpha'`/`''`).
  Round 3: SQL `RLIKE` keyword refusal pinned (`test_sql_rlike_keyword_refuses`;
  §7 FN-RLIKE-KEYWORD-1). Round 4 (FN-REGEXP-EXTRACT-1): the extract answer pin
  cites `pins: fn-regexp-extract-1/C-002`.
  pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [test_fn_like_escape_end.py](test_fn_like_escape_end.py) — **FN-FIX-2 (2026-09-04):**
  FN-LIKE-ESCEND-1. A LIKE pattern ending in the escape char raises
  `INVALID_FORMAT.ESC_AT_THE_END` SQLSTATE 42601. Control `like('a\\b', 'a\\\\b')`
  is True. pins: fn-fix-2-string-rows/C-003
  **FN-FIX-2-CTRL-1 (2026-09-04):** the refusal holds ANSI-off and for the
  explicit-`ESCAPE` spelling.
  pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [test_log1p_1.py](test_log1p_1.py) — **LOG1P-1 (2026-09-02):** three-door `log1p` /
  `expm1` pins (Spark SQL, ANSI `repark.sql()`, facade), tiny-arg vs composed form,
  SEM-1 incidentals. Live Spark cell lives in `test_parity_live.py` on the
  session-scoped `spark_engine`. Oracle live PySpark 4.1.2.
  pins: log1p-1-precise-kernels/C-001, C-002, C-004
- [test_bl17_base64_padding.py](test_bl17_base64_padding.py) — **BL-17 (2026-09-03):**
  codifies today's unpadded `F.base64` (`'Spark'` → `U3Bhcms`, `'A'` → `QQ`) so a
  padded kernel reds the pin; Spark 4.1.2 is `U3Bhcms=` / `QQ==`. Measured by EX-4.
  pins: ex-4-functions-strings-a/C-001
- [test_fn_initcap_divergence.py](test_fn_initcap_divergence.py) — **FN-FIX-2 (2026-09-04):**
  FN-INITCAP-1. `initcap` starts a word only after SPACE (`'a-b'` → `'A-b'`).
  pins: fn-fix-2-string-rows/C-003
  **FN-FIX-2-CTRL-1 (2026-09-04):** `'ünï_9 ab'` → `'Ünï_9 Ab'`; `''` and NULL keep.
  pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [test_fn_chr_divergence.py](test_fn_chr_divergence.py) — **FN-FIX-2 (2026-09-04):**
  FN-CHR-1. `chr`/`char` are `n % 256`; negatives are `''`.
  pins: fn-fix-2-string-rows/C-003
  **FN-FIX-2-CTRL-1 (2026-09-04):** `chr(0/256/65536/1114112)` wrap; negatives `''`; NULL keeps.
  pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [test_fn_trim_chars.py](test_fn_trim_chars.py) — **FN-FIX-2 (2026-09-04):**
  FN-TRIM-CHARS-1. `F.trim`/`ltrim`/`rtrim` two-arg charset; one-arg whitespace kept.
  pins: fn-fix-2-string-rows/C-003
  **FN-FIX-2-CTRL-1 (2026-09-04):** empty trim set is a no-op; NULL trim set is NULL.
  Round 3: NULL `ltrim` / `rtrim` pinned on both doors
  (`test_fn_trim_null_charset_is_null`).
  pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [test_examples_window_catalog.py](test_examples_window_catalog.py) — **EX-21 (2026-09-04, r2):**
  EX-22 (2026-09-04): the module docstring names all three batches after the merge of main; imports sorted.
  the five divergence pins for the catalog/session example batch — `registerFunction` answers
  the UDF object where Spark's deprecated alias returns the original callable (EX-SES-1), an
  action on a `newSession()` result promotes it process-active where Spark keeps the caller
  (EX-SES-2, Spark column re-measured in round 2), `create_dataframe([], ['a'])` answers an
  empty string-typed frame where Spark raises `CANNOT_INFER_EMPTY_SCHEMA` (EX-SES-3), `conf.get`
  on an unset key raises a bare `Exception` where Spark raises `SparkNoSuchElementException`
  (EX-SES-4), and a missing file raises `AnalysisException` 'No files found' through the readers
  where Spark raises `PATH_NOT_FOUND` (EX-SES-5). EX-20's window/catalog pins share this file
  since the EX-20 merge.
  pins: ex-21-catalog-session/C-001
- [test_examples_dataframe_b.py](test_examples_dataframe_b.py) — **EX-16 (2026-09-04):**
  DF-PRINTSCHEMA-1 (2026-09-04): the printSchema pin is `test_print_schema_stdout_matches_spark` and asserts Spark's tail.
  the four divergence pins for the DataFrame-b example batch — `intersectAll`/`intersect_all`
  refusal with Spark's multiset answer recorded (EX-DF-7), `groupingSets`'s one-set-per-column
  answer plus the refused Spark documented shape (EX-DF-8), `mergeInto`'s bare-key sugar and
  `target.`/`source.` qualifier arms that answer Spark's merged rows (EX-DF-9), and
  `printSchema`'s stdout ending one newline short of Spark's capture (EX-DF-10; FIXED by DF-PRINTSCHEMA-1, the pin now asserts Spark's tail). The module
  docstring names the row span `EX-DF-7`…`EX-DF-10`.
  pins: ex-16-dataframe-b/C-001
- [test_examples_dataframe_a.py](test_examples_dataframe_a.py) — **EX-15 (2026-09-04):**
  the six divergence pins for the DataFrame-a example batch — `colRegex`/`col_regex`
  raw-string compilation (EX-DF-1), the three global-temp-view refusals (EX-DF-2),
  `exceptAll`/`except_all` refusal (EX-DF-3), `describe`'s unordered rows with
  Spark's cells pinned order-independently (EX-DF-4), the `corr`/`cov` NULL-pair
  arm under an explicit all-nullable DoubleType schema (EX-DF-5), and the silent
  `createTempView`/`create_temp_view` replace of an existing name (EX-DF-6).
  pins: ex-15-dataframe-a/C-001
- [test_examples_window_catalog.py](test_examples_window_catalog.py) — **EX-20 (2026-09-04):**
  the four divergence pins for the window/catalog example batch — the DataFrame-door tied-key
  ordered default frame running per-row where Spark shares peer sums (EX-WIN-1, the G5
  default-frame class), `getDatabase('default')` answering None description/locationUri where
  Spark fills both (EX-CAT-1), `listDatabases` re-measuring the FA-2 field shape on 4.1.2
  (EX-CAT-2), and `functionExists(name, dbName)` answering True where Spark scopes the check
  (EX-CAT-3). **EX-22 (2026-09-04)** adds the four WriterV2 pins — `overwrite(condition)`
  refusing where Spark performs the conditional overwrite (EX-W2-1), empty-source
  `overwritePartitions` refusing where Spark no-ops (EX-W2-2), `option`/`options` with a
  branch/tag key refusing where Spark silently writes the default branch (EX-W2-3), and
  `overwritePartitions` on an unpartitioned table leaking a `ParseException` where Spark
  replaces the whole table (EX-W2-4) — via a
  `spark_v2` memory-catalog fixture. The module docstring carries the batch pins line.
  pins: ex-20-window-catalog/C-001
  pins: ex-22-types-writerv2/C-003, C-005
- [test_examples_dataframe_c.py](test_examples_dataframe_c.py) — **EX-18 (2026-09-04):**
  the seven divergence pins for the DataFrame-c example batch — the `sameSemantics`
  alias arm answers handle identity where Spark answers plan equality (EX-DF-11),
  `replace` without subset casts or raises where Spark replaces typed cells (EX-DF-12),
  `sample`'s stable seeded set where Spark's keyword-seed spelling drops the seed and
  the seeded sets differ (EX-DF-13), `sampleBy`'s seeded 0.5/0.5 fractions keeping three
  rows where Spark keeps two (EX-DF-14), `summary`'s unordered multi-stat rows,
  string-column raise, and bare-call refusal with the count row pinned (EX-DF-15),
  `show`'s rendering without Spark's truncation trailer (EX-DF-16), and the `toJSON`
  refusal (EX-DF-17).
  pins: ex-18-dataframe-c/C-001
- [test_examples_column_a.py](test_examples_column_a.py) — **EX-17 (2026-09-04):** imports
  `repark.spark.functions` (importing the `repark.functions` shim rebinds the package attribute
  and hides the private SSOT names `test_qi1_idents.py` pins; imports sorted);
  the two divergence pins for the Column-a example batch —
  `test_col_cast_qualified_projection_name`: a bare `F.col("v").cast("double")`
  select names the CDF-qualified column where Spark answers `v` (EX-COL-1), and
  `test_get_field_bare_projection_name`: an unaliased `getField` projects `r['a']`
  where Spark answers `r.a` (EX-COL-2).
  pins: ex-17-column-a/C-001
- [test_examples_dataframe_d.py](test_examples_dataframe_d.py) — **EX-19 (2026-09-04):**
  the three divergence pins for the DataFrame-d example batch — `withColumnsRenamed`
  refusing duplicate final names where Spark answers the duplicate-named frame
  (EX-DF-18), `stat.freqItems` refusing loudly where Spark answers the frequent-item
  table (EX-DF-19), and the struct-valued `Row` field answering a dict where Spark
  keeps the nested `Row` (EX-ROW-1). The module docstring names the row span.
  pins: ex-19-dataframe-d-window/C-001
- [test_fnp7_try_inversions.py](test_fnp7_try_inversions.py) — **FNP-7a/7b:** twelve `try_*`
  inversions. Spark 4.1.2 cells (value and Arrow type) on the two reachable doors (Spark SQL
  + facade Column API). Native ANSI `repark.sql()` does not load SparkExtension: the twelve
  names are unresolved (`Invalid function`). Interval `try_avg` refuses `[FNP-11]` (2026-08-31).
  DATE + HOUR (24 HOUR included) promotes to timestamp; INTERVAL day-time Duration-max
  overflow is NULL on both signs of the bound; DATE + 0 HOUR stays date (BL-14, recorded).
  pins: fnp-7-try-inversions/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011, C-012, C-013, C-014, C-015, C-017, C-018, C-019
  `test_try_to_number_non_foldable_divergence_is_pinned` holds FN-TRYTONUMBER-1 at the 100-column rule.
  pins: ex-11-functions-hash-url-random/C-001
- [test_integer_overflow_parity.py](test_integer_overflow_parity.py) — **F-Y10-1:** integer
  `+` / `-` / `*` overflow shared-raise under default ANSI and Int32/Int64 wrap when
  `ansi=false`, SQL and facade expression (`F` import uses the PySpark N812 noqa).
  Untyped `1 + 1` / `2147483647 + 1` stay int64 (literal-width split).
  Unaliased planner-hit SQL keeps BinaryExpr column names.
  Native ``repark.sql()`` overflow raise is pinned. Facade i32 sub/mul wrap and
  i64 CAST+lit wrap cells are pinned (ASCII docstring operators for RUF002;
  ruff format on long CAST SQL).
  pins: f-y10-1-int-overflow/C-001, C-002, C-003.
- `test_production_file_size.py` — frozen parent-symbol inventory, integrated AST body hashes,
  responsibility ownership, `_funcs` compatibility namespace, isolated source/wheel import-cycle
  smoke, default source ceiling, and retired exception pins for the production/file-size refactor.
  PERF-FACADE-CDF-1 joined the inventory: `create_dataframe_columns.py`, the six new router
  bindings with their owners and hashes, and 76 cross-owner edges (the rows→columns dispatcher
  edge pins the new router binding).
- [test_sqp_1_string_literals.py](test_sqp_1_string_literals.py) — **SQP-1:** facade string values
  use the shared Spark literal helper across SQL, createDataFrame, unpivot, and ML paths.
- [test_dml_c_truncate.py](test_dml_c_truncate.py) — **DML-C:** facade `.sql()` TRUNCATE
  wipes rows, stamps `operation=delete`, time-travels to the pre-truncate snapshot;
  missing table is `TABLE_OR_VIEW_NOT_FOUND`; a view is `EXPECT_TABLE_NOT_VIEW`;
  native `repark.sql()` plans a missing table as `table '…' not found`.
  pins: dml-c-truncate/C-004, C-006, C-007
- [test_mw9_delete_granularity.py](test_mw9_delete_granularity.py) — **MW-9:** facade Spark
  `.sql()` unset `write.delete.granularity` writes one position-delete file per data file.
- [test_rp3_c009_write_default.py](test_rp3_c009_write_default.py) — **RP-3 C-009:** no engine
  caller sets `write_default`; Iceberg fixture bytes stay flat vs `origin/main` when that
  ref exists (skips on a shallow checkout). pins: rp-3-fork-repin/C-009, C-010, C-011
- [test_rp4_c004_to_branch.py](test_rp4_c004_to_branch.py) — **RP-4 C-004 / RP-5 C-004:** engine
  callers now set `to_branch` (RP-5 consumes F-6).
  pins: rp-5-fork-repin/C-004
  invokes `to_branch`; Iceberg fixture bytes stay flat vs `origin/main`. pins: rp-4-fork-repin/C-004
- [test_v3_cow_dml.py](test_v3_cow_dml.py) — **V3-7 lift:** facade Spark `.sql()` UPDATE,
  sequential COW DELETE, and MERGE matched-update keep `_row_id`; created-v3 UPDATE and
  MERGE Spark-equal; **V3-8:** subquery-`WHERE` UPDATE and DELETE keep `_row_id` at
  next-row-id 6 / 5 and the outside-the-hole `NOT IN` UPDATE still refuses `G3-E8` with
  rows unchanged; MOR first DELETE commits a Puffin DV and the second merges. RP-4
  six-file `rewrite_data_files` keeps `_row_id` / seq on `to_arrow`. **V3-9:** the facade MoR
  subquery-`WHERE` twin — `DELETE … IN` at next-row-id 3 / added 0 and `UPDATE … IN` at
  next-row-id 4 / added 1, each with `delete_files.file_format` = `PUFFIN`; the UPDATE
  statement is split across two source lines to hold the 100-column ruff ceiling.
  pins: v3-9-mor-predicate-dml-dv/C-003; v3-8-subquery-where-lineage/C-002;
  v3-7-merge-lineage/C-002; rp-6-fork-repin/C-002, C-003; rp-4-fork-repin/C-003
- [test_ref_branch_tag_wap.py](test_ref_branch_tag_wap.py) — **REF:** the facade rows for
  branch/tag retention and the refused doors — both `WITH SNAPSHOT RETENTION` halves at the
  oracle's values, the reversed order refusing, write-to-branch landing on the named branch
  (RP-5 / REF-1 FIXED), write-to-tag refusing Spark-shaped, and WAP declared
  (`fast_forward` / `publish_changes` / `cherrypick_snapshot` and the `spark.wap.*` confs all
  fail closed). The `branch_`/`tag_` READ selectors resolve the ref here too — standalone and on
  a DML statement's read side (`INSERT … SELECT`, `MERGE … USING`, a `DELETE` predicate
  subquery). A tag-named write TARGET refuses Spark-shaped; a missing branch refuses
  naming it and does not create the branch.
  pins: ref-branch-tag-wap/C-002, C-003, C-004, C-005, C-007
  pins: rp-5-fork-repin/C-004
- [test_v3e4_refs_time_travel.py](test_v3e4_refs_time_travel.py) — **V3E-4:** facade
  branch/tag, `VERSION AS OF` over DVs, rollback, expire dual-probe, orphan
  24h floor on the partitioned-DV fixture after a RePark append; live-DV UPDATE
  still refuses `V3-COW-1` (RP-3 C-004; DELETE on live DVs is lifted).
- [test_v3_lineage_columns.py](test_v3_lineage_columns.py) — **V3-4:** facade SQL serves
  Spark-equal `_row_id` / `_last_updated_sequence_number` on the V3E-3 fixtures; `SELECT *,
  _row_id` expands user columns only; qualified/aliased forms; unquoted case-fold;
  JOIN/CTE/subquery/`VERSION AS OF` refuse `V3-ROWID-2`; v2 is `No field named _row_id`.
  pins: v3-4-serve-lineage-columns/C-004, C-005, C-007, C-008, C-011, C-012, C-013, C-014,
  C-015, C-016, C-018, C-020
- [test_v3e3_fixtures.py](test_v3e3_fixtures.py) — **V3E-3 (2026-08-24):** facade adopt of
  the Spark-written partitioned v3 DV fixture and the equality-delete + DV fixture;
  live rows, partition prune, `.delete_files` content 1/2; RP-3 C-007 CALL still refuses
  live DVs. pins: rp-3-fork-repin/C-007, C-011
- [test_v3_live_file_order.py](test_v3_live_file_order.py) — **V3-11 (2026-09-02):** the live
  cell for same-commit data-file order. Its repark half runs JVM-free (a partitioned v3 CTAS
  over three partition values and a MoR MERGE that updates one partition and inserts into two
  more, each at Spark's exact `_row_id` map); under `REPARK_PARITY_LIVE=1` it replays both
  statements on PySpark 4.1.2 + Iceberg 1.11.0 at the matched layout and asserts the same two
  maps. It is a file of its own rather than a section of `test_v3_live_oracle.py` because that
  file is 977 lines against the 1,000-line source ceiling. **Remediation (same day):** it must
  not tear down a session it did not create — `test_parity_live.py` builds a session-scoped one
  and collects earlier by filename — so it records `SparkSession.getActiveSession() is None`
  before `getOrCreate()` and stops only what it started; its catalog is `v3_11_file_order`, not
  the shared `local` of `_live_parity.LIFECYCLE_SPARK_CATALOG` whose warehouse it would
  otherwise repoint and delete; and it keeps its warehouse under pytest's `tmp_path` with no
  private Ivy cache (a cold session would re-fetch ~96 MB from Maven).
  pins: v3-11-row-id-determinism/C-004
- [test_v3_live_oracle.py](test_v3_live_oracle.py) — **V3E-5 (2026-08-27):** nightly live oracle for the two V3E-3 fixtures — `REPARK_PARITY_LIVE=1` repark == Spark on partitioned-DV prune and equality-delete alongside DV, plus `.delete_files` kinds. RP-6: `test_partitioned_dv_update_commits_and_rewrite_returns_zeros` pins Spark-equal `(id, _row_id, seq)` after live-DV UPDATE; `rewrite_position_delete_files` returns zeros with rows and fixture bytes unchanged after the UPDATE. JVM-free twins stay in `test_v3e3_fixtures.py`. Critic remediation (2026-08-27): prune1 on Spark, combined DirLock, exact content sets, mirrored format, GAV full equality, version sort, COW, `py-format` single-line, meta-pin now asserts archive/dual-wire/diff allowlist. Formal CCC + cargo-deny/wheel remediation (2026-08-28): `chacha20` yanked and `thiserror` duplicate `skip`. PLAN-1 makes the ledger lookup lifecycle-aware across staging, completed, and archive, and checks the landed #253 commit instead of the current branch. **Nightly fix (2026-09-01):** the three live helpers now qualify `CALL <catalog>.system.register_table` through `LIFECYCLE_SPARK_CATALOG`; unqualified, Spark resolved it against `spark_catalog` and the CI leg had been red since its first run (2026-08-28). The north-star meta-pin now checks the row cites V3E-5 and the oracle version regardless of its status glyph, so an honest ⚠ does not red it. V3-7: `test_v3_merge_matched_update_live_cow_and_mor` cites the V3-7 ledger transcript (not `/tmp`) and live-gates COW/MoR matched-UPDATE MERGE. **V3-10 (2026-09-02):** `test_v3_upgrade_v2_to_v3_live_matches_spark` skips FIRST when the tier is off — its repark half duplicates `test_v3_upgrade.py::test_alter_upgrade_with_the_opt_in_serves_v3_lineage` and cost 0.32 s of call time on every JVM-free run (test wall 0.52 s → 0.20 s). Its Spark helper reuses the default Ivy cache like `_live_parity.build_spark_iceberg_engine` instead of a per-call `mkdtemp`, and picks the newest Hadoop pointer by PARSED version like `_materialize`, not by lexicographic `sorted(glob)` (which would pick `v9` over `v10`). It is not folded into `_live_subquery_where_dml_measurement`: that helper memoizes ONE session's cells behind a module-level dict and returns early on the second call, so adding upgrade statements to it would couple two units' measurements to one session's ordering. **V3-9 (2026-09-02):** `test_v3_mor_subquery_where_dml_live` cites the V3-9 transcript, runs the repark half JVM-free and live-gates the MoR subquery-`WHERE` DELETE / UPDATE lineage and `PUFFIN` delete-file format against Spark (pins: v3-9-mor-predicate-dml-dv/C-002, C-003, C-005). The Spark leg is one session for both modes: `_live_subquery_where_dml_measurement` measures the COW and MoR cells once and each test asserts its own pinned values against that measurement, so the file's live wall clock fell from 24.07 / 24.05 s to 23.39 / 22.74 s (pins: v3-9-mor-predicate-dml-dv/C-008). **RDF-1 (2026-09-02):** it read `completed/` by absolute path and reded the moment the archive ritual moved that ledger; both ledger reads now share `_ledger_text`, the staging/completed/archive lookup PLAN-1 already used for the V3E-5 meta-pin. **RP-7 (2026-09-02):** the two sibling live helpers dropped their per-call `spark.jars.ivy` `mkdtemp` + `rmtree`; on a runner with no local Iceberg jar that forced a full Maven resolve twice per nightly, and the default Ivy cache is what `_live_parity.build_spark_iceberg_engine` and the V3-10 helper already use.
  pins: rp-7-f18-repin/C-006 **RP-7 (2026-09-02):** the shared-Puffin container-close cell went to its own module rather than here — this file was 23 lines under the 1000-line cap.
  pins: rdf-1-position-delete-bounds/C-004
- [test_date_fn_1.py](test_date_fn_1.py) — **DATE-FN-1 (2026-09-04):** Spark SQL `date()`
  and `unix_timestamp` unit pins (timestamp / string / date / NULL; invalid string ANSI on
  and off; zero-arg `FROM range(3)` is three identical BIGINT rows on SQL and the facade).
  Live co-collect `test_parity_live.py::test_live_date_fn_1_date_and_unix_timestamp`.
  pins: date-fn-1-spark-date-spelling/C-001, C-003, C-004
- [test_sql_harden_cutover.py](test_sql_harden_cutover.py) — **SQL-HARDEN-1 (2026-09-04):** (DATE-FN-1 flipped the date/unix_timestamp refusal pin to an answer pin)
  the cutover pipeline cutover shapes S1–S7 (9 programs). **SQL-HARDEN-2 (2026-09-04):**
  S8/S9 = S1/S2/S4 at v2 and v3 copy-on-write (6 programs; 15 total). Always-run repark
  half against `_sql_harden_cutover_repark.py`; live Spark half against
  `_sql_harden_cutover_spark.py` behind `REPARK_PARITY_LIVE=1`. Catalog `sqlh1`. Inventory
  `_sql_harden_cutover_programs.py` (`cow_properties` beside `mor_properties`), runners
  `_sql_harden_cutover_run.py`, verdicts `_sql_harden_cutover_golden.py`. AWS legs in
  `test_aws_acceptance.py`: Glue and S3 Tables replay all 15 rows into
  `testing_repark_acceptance` and assert the CoW MERGE cells (`delete_files` 0, data-file
  count equal to the memory half) plus the S6 gold namespace, which `date()` now answers into.
  Namespace pin: rendered SQL uses only the passed namespace.
  CUTOVER-DATE-1 controls: `to_date` / `CAST AS DATE` / `date` / `unix_timestamp` answer.
  MoR MERGE delete-file golden pins kinds (PARQUET vs PUFFIN), not count; count is
  host-dependent (3 on a 64-core box) and the always-run pin is `count >= Spark's 2`.
  CoW MERGE: `delete_files` empty (a delete file is a defect); data-file count after the
  second pass is 1 on both engines.
  **CUTOVER-SCHEMA-1 (2026-09-04):** REPARK halves re-measured to the Spark-equal
  nullability derivation (79 schema-triple cells); s3 verdict EQUAL
  (`CUTOVER-DEDUP-SCHEMA-1` FIXED), CTAS-REQ programs still DIVERGE only on `V3-COV-7`
  (`CUTOVER-CTAS-REQ-1` FIXED).
  pins: sql-harden-1-cutover-shapes/C-001, C-002, C-003, C-004
  pins: sql-harden-2-cow-shapes/C-001, C-002, C-003, C-004
- [test_cutover_schema_1.py](test_cutover_schema_1.py) — **CUTOVER-SCHEMA-1 (2026-09-04):**
  nullability derived the way Spark derives it on the cutover shapes. Always-run:
  `read.parquet` of a required-field file reports every field nullable (flat and nested),
  csv/json likewise; Spark-door CTAS stores every column optional including
  `SELECT coalesce(x, 0)` (at repark's `long` width — the V3-COV-8 width half stays
  BACKLOG, only nullability moves); the S3 dedup Arrow schema equals Spark field for field;
  `CAST(1 AS DECIMAL)` is nullable while `CAST(1 AS INT/STRING)` keeps the non-null
  literal; the ANSI-door cast fence and the SE-1 tighten-derived CTAS refusal control
  stay put. Live legs re-derive the read/cast/dedup cells from PySpark 4.1.2 on the
  shared `spark_engine`. Round 3 (2026-09-05) pins four pre-existing divergences as
  BACKLOG rows with current-answer pins: `CAST-NULL-1` (non-decimal cast targets),
  `CAST-BOOL-DEC-1` (boolean-to-decimal refusal), `CUTOVER-NULLDEPTH-1` (relax stops
  at depth 32), `READ-TSNTZ-DTYPE-1` (tz-naive timestamp reads `string` via `dtypes`).
  The module docstring is the pins-only one-liner; this row is the reason.
  pins: cutover-schema-1/C-001, C-002, C-003, C-004, C-005, C-006
- [test_v3_statement_coverage.py](test_v3_statement_coverage.py) — **V3-COV (2026-09-03):** the v3
  statement-coverage matrix — 81 `_Program` rows (a v3 seed, the statement(s) under test, the
  probes compared) over every served statement class and all seven `CALL system.*` procedures.
  `test_v3_statement_row_reproduces_the_measured_repark_answer` always runs against the committed
  golden; `test_v3_statement_row_matches_the_live_spark_oracle` runs the same program on the live
  oracle behind `REPARK_PARITY_LIVE=1` and re-asserts the verdict. Seeds are single-file per
  partition on both engines so a file-shape probe is comparable under the shared `local[2]`
  session, and the module-private catalog is `v3cov` (live-cell rules 1–7). A create row compares
  the table it created through a `META` probe — format version, current schema, partition fields
  and the `write.*` properties, read from the table's own metadata JSON — which is what found
  `V3-COV-7` and `V3-COV-8`; `_agrees` exempts ONLY a mutual refusal from the value comparison, so
  a new cell kind cannot be added and silently never checked. `_latest_metadata` sorts on
  `(st_mtime, name)`, not `st_mtime` alone, so two pointers written inside one filesystem mtime
  tick still resolve to the same one on every run. **The `V3-COV-8` reading was published
  backwards and corrected 2026-09-03 (ledger ERRATA 2 / E-7):** the golden here is the measured
  one — repark's CTAS derives `id: long, required`, Spark's `id: int, optional`. The live cell compares
  `REPARK[name]` rather than re-running the repark half (81 sessions the always-run sibling
  already pays), and `NEEDS_SNAPSHOT_MARKS` / `NEEDS_METADATA_PATH` — computed once from the
  program text at import — keep the snapshot scan and the metadata-pointer glob off the programs
  that interpolate neither. `REFUSED` is a verdict
  about the STATEMENT: `drop-table` reads its table back and both engines refuse that read, which
  is the agreement the row exists for, so the row stays `EQUAL`. Partitioned rows pinned
  `_last_updated_sequence_number` and **not** `_row_id` while `V3-COV-3` was open, because the
  delegated partitioned INSERT's `_row_id` mapping was unstable and pinning an unstable value is
  the false green this matrix exists to prevent. **RP-8 (2026-09-03):** fork F-20 (`#261`) drains
  `FanoutWriter::close` ascending, the mapping is Spark's in 12 of 12 runs, so `_P_LINEAGE` is
  back on every partitioned program — nine goldens re-measured on both engines — and the
  instability cell became `test_v3_partitioned_insert_row_id_mapping_is_stable_and_spark_ordered`
  beside the CTAS control that was always stable. Matrix and totals:
  [../../../docs/design/v3-statement-coverage.md](../../../docs/design/v3-statement-coverage.md).
  pins: v3-cov-statement-coverage/C-002, C-003, C-004, C-006
  pins: rp-8-repin-f21-f22/C-007
- [_v3_statement_coverage_programs.py](_v3_statement_coverage_programs.py) — **V3-COV Its module docstring counts the 81 rows.
  (2026-09-03):** the inventory — `_Seed`, `_SEEDS`, `_Program` and the 81 `_PROGRAMS` rows with
  the probes each compares. Split out of the test module for the `check_lib_py` ceiling; the seam
  is inventory / runner. pins: v3-cov-statement-coverage/C-001
- [_v3_statement_coverage_golden.py](_v3_statement_coverage_golden.py) — **V3-COV (2026-09-03):**
  `VERDICTS` per program, and the join of the two engine halves it re-exports.
  pins: v3-cov-statement-coverage/C-003
- [_v3_statement_coverage_repark.py](_v3_statement_coverage_repark.py) and
  [_v3_statement_coverage_spark.py](_v3_statement_coverage_spark.py) — **V3-COV (2026-09-03):**
  the measured halves, one entry per program, recorded 2026-09-03 against live PySpark 4.1.2 +
  Iceberg 1.11.0. One module per engine, so neither grows past the ceiling and a re-measurement
  diff reads as one side moving. All three are `_`-prefixed, so pytest never collects them.
  **CUTOVER-SCHEMA-1 (2026-09-04):** `ctas-v3` re-measured to optional (the V3-COV-8
  nullability half); verdict stays DIVERGES on width.
  pins: v3-cov-statement-coverage/C-003
  pins: cutover-schema-1/C-002
- [test_v3_legacy_delete_merge.py](test_v3_legacy_delete_merge.py) — **V3-12 (2026-09-02):** the
  facade door's cell for a v3 merge-on-read write over an upgraded table's legacy parquet
  position delete. `_repark_legacy_merge_shape` and `_spark_legacy_merge_shape` run the SAME five
  statements and are compared as one dict — the delete files before and after (format,
  `record_count`, whether the entry names a data file) plus the surviving lineage — so nothing
  in the pin depends on a generated file name. The Spark half runs `local[1]` with
  `shuffle.partitions` and `default.parallelism` at 1: at `local[2]` the four-row `INSERT` splits
  into TWO data files, the legacy delete lands on one and the new DV on the other, and no merge
  is exercised at all. It borrows `_LIVE` / `_LIVE_SKIP` / `_ALLOW_CREATE_V3_KEY` /
  `_v37_iceberg_runtime_jar` from `test_v3_live_oracle.py`. The second test is the incidental
  control: a table that stays v2 keeps writing parquet position deletes and this engine leaves
  TWO of them live for one data file where Spark rewrites one. The module also carries the facade
  twins of the two cells V3-12 filed as refusals (plain-`WHERE`, and a delete covering two data
  files), so each entry point has its own row. **RP-8 (2026-09-03):** both flipped to merges at
  pin `c1d6c9de` and each gained a live Spark twin — `test_plain_where_mor_delete_over_a_legacy_parquet_delete_matches_spark`
  compares the A2 shape, and `test_partition_scoped_legacy_delete_matches_spark` compares the
  whole §12 P1–P4 sequence on a table partitioned by `part = 7` with two data files, which is the
  layout the oracle measured. Both live halves seed one file per append with
  `coalesce(1)` and obey the same session discipline as the first cell. **Session discipline:** `_live_session` reuses
  `SparkSession.getActiveSession()` when one is alive and stops only a session it built —
  `test_parity_live.py` sorts first and holds a session-scoped `local[2]` session, and an
  unguarded `getOrCreate` borrowed it (dropping master and jars, splitting the seed into two data
  files so no merge was exercised, turning the pin red) and then `stop()`ped it out from under every
  later live test. The seed is `createDataFrame(...).coalesce(1).writeTo(...).append()` so the
  cell holds at ONE data file under any master rather than needing its own context. The catalog
  name is module-private (`v312legacy`), NOT `_live_parity.LIFECYCLE_SPARK_CATALOG`'s `local`
  whose warehouse this module would otherwise repoint and `rmtree`, and nothing pops
  `PYSPARK_SUBMIT_ARGS`, which would permanently disarm `_live_parity`'s Iceberg arming. The
  repark-only assertions live in their own always-run test so real work is not reported as
  skipped.
  pins: v3-12-legacy-delete-merge/C-003, C-004
  pins: rp-8-repin-f21-f22/C-003
- [test_v3_dv_container_close.py](test_v3_dv_container_close.py) — **RP-7 (2026-09-02):**
  `test_v3_shared_puffin_container_close_live` runs the shared-Puffin close on both engines from
  the same partitioned v3 MoR seed and compares an engine-independent SHAPE (`_dv_close_shape`)
  rather than file names — which entry moved, at what offset, whether the sibling's
  `(container, offset, record_count)` tuple survived, and the surviving rows. It keys touched vs
  sibling by record count, not by sorted position: the partition writers do not always emit
  `part = 0` first, and an ordered pin was green by luck on one run and red on the next. The
  seed is ONE `INSERT` on both engines so the two data files (and therefore the one shared
  Puffin) match; six single-row `INSERT`s give six data files and no shared container at all.
  It borrows `_LIVE` / `_LIVE_SKIP` / `_ALLOW_CREATE_V3_KEY` / `_v37_iceberg_runtime_jar` from
  `test_v3_live_oracle.py` instead of copying the live-tier gate.
  Remediation (2026-09-02): the four `SELECT *` reads project only the four columns
  `_dv_entries` reads.
  pins: rp-7-f18-repin/C-003, C-005, C-006
- [test_v3_acceptance_local.py](test_v3_acceptance_local.py) — **LIVE-v3 (2026-09-02):** the
  local proof of the live v3 leg body. Runs `run_v3_acceptance` against the memory catalog and
  asserts the same `assert_v3_acceptance_outcome` the Glue and S3 Tables legs assert, so the
  first live run is a measurement and not a debugging session; also pins the `S3T-V3-1`
  classifier edges (a service refusal classifies, the engine's own opt-in message does not, a
  denial does not, `format-version 2` does not) and that the recorded disposition masks the
  account id. Mutation-proof 19 red of 19 across `_acceptance_v3` expected counts (including
  `V3_FILES_PER_PARTITION`), the DV content/format constants, the delete predicate, the
  `adopt_with` argument, and the five structural leg mutations. `_second_session` registers the
  memory catalog AND creates the namespace because `newSession` replays builder config only, not
  a runtime `register_memory_catalog` — the live legs pass a bare `spark.newSession`. pins: live-v3-aws-legs/C-002
- [test_v3_dv_compaction.py](test_v3_dv_compaction.py) — **V3-5 / B-MOR-3:** facade six-file
  v3 MOR compact drops six Puffin DVs (`removed_delete_files_count = 6`,
  Arrow int32); `rewrite_position_delete_files` returns zeros on those six DVs
  (`B-MOR-3` FIXED 2026-09-03) and converts five upgraded parquet deletes to PUFFIN;
  live ids and `_row_id` / seq stay.
  pins: v3-5-dv-compaction/C-002, C-003, C-004
  pins: b-mor-3-rewrite-position-deletes-v3/C-003
- [test_v3_upgrade.py](test_v3_upgrade.py) — **V3-10 (2026-09-02):** the facade door's in-place
  v2 → v3 upgrade — the refusal without `repark.sql.allowCreateFormatVersion3` (and the proof that
  `_row_id` stays unresolvable on the still-v2 table), the opt-in upgrade with NULL lineage on
  pre-upgrade rows and Spark's `(1,2,1),(2,3,1),(3,4,1),(4,0,2),(5,1,2)` after one append, and the
  downgrade / `'-1'` / `'4'` / `'x'` / `'3.0'` refusals leaving the table where the upgrade left
  it, the same-version request writing no new metadata file, and the proof that the opt-in
  refusal carries no CREATE-door phrasing.
  pins: v3-10-upgrade-v2-to-v3/C-003, C-004
- [test_v3_create_opt_in.py](test_v3_create_opt_in.py) — **V3-2 (2026-08-24):** facade CREATE/CTAS
  `format-version = 3` refuses unless `repark.sql.allowCreateFormatVersion3` is true, and
  **since V3-9 (2026-09-02)** the refusal no longer claims v3 cannot do merge-on-read
  row-level writes, asserted here (pins: v3-9-mor-predicate-dml-dv/C-006); opt-in
  CREATE is readable and a six-file `rewrite_data_files` keeps `_row_id` / seq on `to_arrow`
  (`V3-LINEAGE-1` FIXED). Also the V3R-1
  (2026-08-25) type pin `test_v3_geometry_geography_variant_columns_refuse_naming_the_type`:
  `GEOMETRY` / `GEOGRAPHY` / `VARIANT` columns refuse at CREATE, no table left (registry
  `V3-GEO-1`). **V3-6:** `test_opt_in_v3_create_timestamp_ns_schema_round_trips` facade
  CREATE + `to_arrow` ns schema (empty table — no facade ns write surface), and
  `test_v3_unknown_column_refuses_naming_the_type`
  (`UNKNOWN` refuses at CREATE, no table left). pins: v3-6-v3-types/C-003, C-004
- [test_rewrite_data_files_options.py](test_rewrite_data_files_options.py) —
  **rewrite_data_files options:** facade `where` keeps the **part=1** pre-image byte-identical
  and rewrites part=0 away; unknown strategy and bad where use Spark's text; `sort` and
  `sort_order` refuse.
  pins: maint-rewrite-data-files-options/C-003, C-004, C-005, C-006, C-007
- [test_mw8_runbook.py](test_mw8_runbook.py) — **MW-8 (2026-08-24; RP-5 2026-09-01):** the
  maintenance cycle `docs/guide/iceberg-guide.md` "The maintenance runbook" documents, run end
  to end on a local catalog at gate scale (6,000 rows, 2 partitions, six MERGEs). F-16r
  rewrites this fixture's in-band delete-laden seed files (`test_delete_laden_seed_files_are_rewritten_by_the_runbook`).
  The MW-7 2,500-row pin still holds; it flipped to the reclaim on 2026-09-02 and this module
  stays green either way (`RDF-1` FIXED for a delete file naming one data file).
  pins: rp-5-fork-repin/C-005; rdf-1-position-delete-bounds/C-003
  **C-010 (Critic remediation, F-MW8-1/F-MW8-3)** parses the guide's `MAINTENANCE_CYCLE`
  out of its python block and compares procedure, order, argument names and literal argument
  values (placeholders skipped) against `measure.maintenance_sequence`'s, so printed SQL that drifts from the measured
  cycle reds — it is what catches an `expire_snapshots` call with no `older_than`. C-009 is the
  narrower companion: it checks that every home the section relies on is LINKED, and it does not
  detect an uncited number.
- [test_mw7_scale_smoke.py](test_mw7_scale_smoke.py) — **MW-7 (2026-08-23):** the
  scale-measurement driver (`python/repark-parity/bench/mw7/`) run at gate scale, pinning
  the MACHINERY behind the 1e7-row numbers: the census equals an independent count over
  `files` / `manifests` / `snapshots` and a `Path.stat` of the manifest list; merge-on-read
  delete files grow exactly `partitions x merges` (one per `(spec, partition)` per commit —
  the fixture sets `write.delete.granularity = 'partition'`) while `COUNT(*)` holds; the
  copy-on-write leg writes zero delete files,
  so it is a valid control; `rewrite_position_delete_files` folds the deletes to one per
  partition and `rewrite_data_files` cuts the data files; `rewrite_manifests` drops the
  manifest count on both legs; the maintenance sequence is the charter's five procedures in
  order with orphan cleanup last and dry-run; every timing carries the answer it was
  measured on and the answer does not move across maintenance; the generator is
  deterministic. Wall-clock is recorded in the ledger, never asserted.
  **C-011 (2026-08-24, Critic remediation; flipped 2026-09-02 by RDF-1):**
  `test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies` pins registry row
  `RDF-1` — a 2,500-row v2 merge-on-read table written as ONE data file inside Java's bin-pack
  band, then a MERGE deleting every one of its rows. The pin now asserts the reclaim: the
  delete file's `file_path` bounds are exact and equal to the seeded path (field `2147483546`,
  read from the manifest, not inferred from a count), `rewrite_data_files` reports
  `removed_delete_files_count = 1`, the seeded path leaves the live set, the sequence ends at
  zero delete files and zero delete records, and `COUNT(*)` is still 2,500 — the reclaimed rows
  do not resurrect. Its predecessor asserted the opposite (the file survives) because RePark's
  own writer truncated those bounds away.
  **SCALE-v3 (2026-09-02):** the v3 twins of the same shapes, from a second `v3_smoke_run`
  fixture the `--format-version 3` knob drives. What they hold, and why each differs from its
  v2 twin:

  | v3 pin | Claim | v2 twin |
  |---|---|---|
  | `test_the_cli_defaults_to_format_version_two` | driver default 2, CLI default 2, `4` refused; fixture-free, so a parser regression reds in milliseconds | the default is the whole v2 behaviour |
  | `test_each_leg_records_the_format_version_it_was_built_at` | both fixtures report the version their legs were built at | — |
  | `test_v3_mor_delete_files_are_one_per_seeded_data_file` | delete files hold at the seeded data-file count while delete records grow `merges x rows_per_merge` | v2 grows `partitions x merges` files |
  | `test_v3_mor_delete_files_are_file_scoped_deletion_vectors` | every delete file is content 1, `PUFFIN`, and names exactly one LIVE data file | v2 writes Parquet position deletes at `partition` granularity |
  | `test_v3_cow_leg_keeps_row_lineage` | zero delete files and `_row_id` readable and distinct on touched and untouched rows | v2 has no lineage columns |
  | `test_v3_position_delete_compaction_returns_zeros_and_the_sequence_continues` | `rewrite_position_delete_files` returns zeros on live DVs (`B-MOR-3` FIXED), and the remaining four procedures run | v2 folds 12 delete files to 2 |
  | `test_v3_delete_file_layout_matches_live_spark` (`REPARK_PARITY_LIVE=1`) | at a matched 4,000-row layout repark and Spark agree exactly: `[(1, PUFFIN, 120), (1, PUFFIN, 120)]`, 8 data files, 4,000 rows | no v2 live twin here |
  | `test_a_refusal_is_recorded_only_when_the_step_is_armed` | `run_maintenance_step` records a refusing CALL only when armed; unarmed, the same CALL raises | the driver's one new failure path, driven on a v2 table |
  | `test_started_at_records_the_start_of_the_run_not_its_end` | `run_scale_measurement` takes an injected `clock`; the pin hands it a backdated, strictly increasing fake and requires `started_at` to format its FIRST reading (F-SCALE-V3-1). No wall-clock assumption, so the runner's speed cannot decide it — the first shape of this pin read a 3-second run and went red on a runner that finished in 1.38 s | the field was wrong on both formats |

  The v3 fixture runs at `reps=1` where the v2 one runs at 3: no v3 pin reads a timing, and the
  scan battery's ordering and warm-up claims are already held on the v2 fixture (C-007).

  pins: rdf-1-position-delete-bounds/C-003
  pins: scale-v3-mw7/C-001
- [test_mw5_baseline_delta.py](test_mw5_baseline_delta.py) — **MW-5 (2026-08-23):**
  the MW-0 growth demo re-run: 1,000-row v2 merge-on-read, ten MERGEs of the same
  200 ids, position-delete files 1→10 then compact 10→1 and data files →1
  (**MW-9:** the fixture sets `write.delete.granularity = 'partition'` so that
  1-per-MERGE count survives the Spark-default `file` flip),
  `COUNT(*)` 1,000 `int64` throughout. After the MERGEs, `VERSION AS OF` the CTAS
  snapshot still returns seed names `n{id}`; live rows after the MERGEs are
  `m{id}` for ids 1..200 and `n{id}` otherwise (oracled, not only contrasted).
  After expire the CTAS snapshot is the unknown-snapshot needle. Wall-clock is
  logged, not asserted.
- [test_a13_ctas_fallback.py](test_a13_ctas_fallback.py) — **A13 (2026-08-23):**
  location-less Spark CTAS after `register_memory_catalog` writes under the warehouse,
  not `<temp>/repark_ctas`; two sessions with different warehouses and the same
  `mem.ns.events` names do not share a directory.
- [test_lrs4_door_domain.py](test_lrs4_door_domain.py) / [test_polars_core.py](test_polars_core.py) —
  **PYC-5:** nested helpers `door` and `guarded` gained return annotations
  (typed signatures; ruff classifies them as ANN202, which stays ignored).
  The ANN201 ignore drop is the isolated public-function count of 0.
- [test_pyc_3_dataclasses.py](test_pyc_3_dataclasses.py) — **PYC-3 (2026-08-22):**
  accepted-input set of `merge._Clause` and the four `_csv_smart` records
  (including empty `PreparedCsv`); extra/strict/frozen pins; `to_dict` key set;
  pydantic wheel-dep token; the two DATACLASS_EXCEPTIONS rows deleted not zeroed.
  **PYC-4:** remaining-key pin is dual-wire only (parity dataclass rows converted).
  Behaviour stays on `test_merge_into.py` / `test_t4_csv_smart.py`.
- [test_pyc_2_nested_defs.py](test_pyc_2_nested_defs.py) — **PYC-2 (2026-08-22):**
  AST pin that the eight lifted shipped modules have zero nested `def`s (ancestor
  walk, including `try` / `if`); `types.py` `verifier` and `udtf._build` stay
  pragma-sanctioned; the ten EXCEPTIONS rows are deleted not zeroed; CDF/ext
  finalize uses extra-args. Behaviour stays on existing applyInPandas / row /
  range / UDTF / lit / verifier pins.
- [test_pyc_1_nested_defs.py](test_pyc_1_nested_defs.py) — **PYC-1 (2026-08-22):**
  AST pin that `core.py`, `plan_collapse.py`, and `udf_bridge.py` have zero nested
  `def`s. Behaviour stays on the existing show / UDF / filter / sample pins.
- [test_timestamp_type.py](test_timestamp_type.py) — **Q10:** facade
  `spark.sql.timestampType` — default get, get/set round-trip, invalid set +
  builder refusal naming both tokens, NTZ opt-in SQL literal/CAST +
  `selectExpr` + `createDataFrame` inference on Arrow (value AND type),
  `to_timestamp` stays LTZ. Ledger: `task/q10-timestamptype-ledger.md`.
- [test_functions_split_identity.py](test_functions_split_identity.py) — FN-SPLIT
  (2026-08-15): `__all__` before==after pin. **FN-C moved the pin** to 261
  names (253 after FN-A + FN-B + 8 aggregate additions) + every name resolves.
  (2026-08-15): `__all__` before==after pin. **FN-D moved the pin** 253→264
  (11 datetime additions on the freeze inventory; independent of FN-C) + every
  name resolves. **FNP-15/16:** the pre-split 360 names stay the prefix; 62
  declared-absent names append. pins: fnp-15-16/C-016
  (2026-08-15): `__all__` before==after pin. **FN-E moved the pin** to 262 names
  (freeze 253 + 9 collection additions) + every name resolves.
  (2026-08-15): `__all__` before==after pin. **FN-F moved the pin** to 263
  names (253 FN-A+FN-B + 10 session/bitwise additions) + every name resolves.
  (2026-08-15): **FN-W moved the pin** 291→296 (5 window additions:
  `lag`/`lead`/`nth_value`/`percent_rank`/`cume_dist`).
  (2026-08-17): **FN-GT1 moved the pin** 296→315 (18 leftover thin-wires +
  `getbit` alias of `bit_get`).
  (2026-08-17): **FN-GT2 moved the pin** 315→333 (18 datetime/collections/url/bitmap).
- [test_functions_gt2.py](test_functions_gt2.py) — FN-GT2 (2026-08-17): leftover
  THIN-WIRE datetime/collections/url/bitmap through Arrow (value AND type).
  ``datediff`` stub stays; ``element_at`` pins 1-based + zero-index refuse +
  string-key map extraction; ``shuffle`` pins type+length; ``array_compact``
  drops NULLs only. Rework: exact interval/bitmap/unix_micros values, regex
  ``str_to_map``, NULL rows, non-UTC session pins, docstring-example execute.
  Honesty: W2 MonthDayNano; ``date_diff`` int32; bitmap 0/−1; unix_micros LA
  column.
  **X-round (2026-08-18), repair round** — 30 tests. New pins: ``shuffle(NULL array)`` is NULL
  on the Spark door AND the facade (X1, was an arrow-data panic) with the ANSI
  door's ``Invalid function 'shuffle'`` recorded as the not-reachable matrix row;
  seeded ``shuffle`` agrees across doors (X2); ``parse_url``/``try_parse_url``/
  ``get``/``url_*`` column-name direction (X3/X4/X5/X12); ``str_to_map`` ``\s``
  is ASCII so NBSP does not split (X6); ``map_from_entries`` duplicate key raises
  (X7); the seven ``java.net.URI`` vs ``url::Url`` recipes, both doors, each row
  naming the normalized answer it used to give (X8); ``element_at`` OOB and
  ``make_date`` invalid-Y-M-D are NULL while ``1 / 0`` still raises (X9);
  ``try_parse_url`` / ``try_url_decode`` success paths (X11); ``unix_micros(DATE)``
  LA 28.8e9 (X13). ``parse_url`` schemeless now RAISES ``INVALID_URL`` and the
  QUERY key is a Java regex — both previously-recorded residuals, now closed.
  ``test_functions_e.py::test_get_map_by_key`` updated in lockstep: map lookup
  through ``get`` needs ``F.lit(key)`` now that a bare ``str`` is a column name.
  The repair round re-derived every X8 expectation against a live
  ``java.net.URI`` probe (MEASURED-JVM, OpenJDK 11.0.31) driven through the
  disassembled ``ParseUrlEvaluator$`` getter map, and added
  ``test_parse_url_hostile_urls_split_like_java_net_uri`` (15 hostile rows —
  IPv6/IPv4 hosts, registry-based fallback where HOST is NULL but AUTHORITY is
  not, empty query/ref/authority, malformed escape) plus the compile-ORDER block
  in ``test_parse_url_query_key_is_a_java_regex``. Two Spark divergences the
  probe exposed are now pinned rather than documented: an uncompilable QUERY key
  raises on ``try_parse_url`` too (``TryParseUrl`` is
  ``ParseUrl(failOnError=False)``, not ``TryEval``), and a 3-arg call with a
  non-``QUERY`` part is NULL before the URL is parsed at all. The X8 regex-key
  work also **introduced** a residual, now pinned rather than left silent:
  ``test_parse_url_query_key_regex_dialect_residual`` fixes both halves of the
  ``java.util.regex`` vs ``regex``-crate gap — twelve keys that agree
  (``\p{Alpha}``, ``a++``, ``[a-z&&[^b]]``, ``(?<n>a)``, …) and five that do not
  (lookahead, lookbehind, backreference, atomic group, ``\Q…\E``), which raise
  here under both UDFs.
- [test_functions_gt1.py](test_functions_gt1.py) — FN-GT1 (2026-08-17) leftover
  THIN-WIRE math/string/bitwise/utf8 through `ReparkSession` Arrow `to_arrow()`
  (value AND type). **GT1-FIX (2026-08-18):** ColumnOrName direction pins,
  NULL-input rows (19 names; `regexp_count` NULL is NULL not 0), exact Arrow
  types, unsigned-shift negative, invalid UTF-8 via unhex, `getbit` projection
  name, `regexp_instr` idx (NULL-propagates; value ignored — live Spark
  4.1.2), keyword forms (`numBits`/`numBucket`/`partNum`), docstring examples
  execute. **Round-2 (2026-08-19):** Spark SQL door `regexp_count` NULL /
  `regexp_instr` idx-ignore matrix, ARRAY refuse, decimal 5/40, STRING
  `partNum`, `getbit` SQL name, omitted-idx display `, 0`, UTF-16 empty-count
  / ASCII ``\\d``, partNum 0 fail-loud, Java find-loop `[0-9]*` = 6, named
  Infinity stringify residual. **Round-4 (2026-08-19):** start-anchor
  mid-surrogate skip (`🐈`/`^` = 1 both doors; `🐈\\n🐈`/`(?m)^` = 2 via
  F.*). Ledger: `task/fn-gt1-ledger.md`.
- [test_functions_w.py](test_functions_w.py) — FN-W (2026-08-15): window
  wrappers through `ReparkSession` Arrow `to_arrow()` (value AND type).
  `lag`/`lead` default first/last-row NULL + explicit default + NULL-source
  row; `nth_value` 1-based; `percent_rank`/`cume_dist` Float64. `ignoreNulls`
  is an honest cut (TypeError). **FN-LAST-1 FIXED 2026-09-03 (FN-FIX-1):**
  `test_last_ignorenulls_window_skips_trailing_null`.
  Live co-collect `test_live_fn_fix_1_last_and_approx_percentile`.
  pins: fn-fix-1-registry-rows/C-003
- [test_functions_a.py](test_functions_a.py) — FN-A (2026-08-15): ordering / null /
  math wrappers through `ReparkSession` Arrow `to_arrow()` (value AND type). Alias
  names resolve + one behavior case. `cbrt` pins the negative-root hazard.
- [test_functions_b.py](test_functions_b.py) — FN-B (2026-08-15): string wrappers
  through `ReparkSession` Arrow `to_arrow()` (value AND type). `replace` pins
  literal vs `regexp_replace`. `printf` aliases the existing `format_string` UOE.
- [test_functions_c.py](test_functions_c.py) — FN-C (2026-08-15): aggregate
  aliases/shims through `ReparkSession` Arrow `to_arrow()` (value AND type).
  Alias names resolve + one behavior case vs canonical. `count_if` pins
  true-only counting; `bool_and`/`bool_or` pin vs `min`/`max` on booleans.
- [test_functions_d.py](test_functions_d.py) — FN-D (2026-08-15): datetime wrappers
  through `ReparkSession` Arrow `to_arrow()` (value AND type). Alias names resolve
  + one behavior case. `unix_seconds` pins toward-zero vs TZ-5 CAST floor.
  `current_timezone` pins Session zone, not `$TZ`.
- [test_functions_e.py](test_functions_e.py) — FN-E (2026-08-15): collection
  wrappers through `ReparkSession` Arrow `to_arrow()` (value AND type). `get`
  pins 0-based vs SQL `element_at` 1-based (index 0 raises).
  **FN-FIX-1:** `arrays_overlap` nulls-only is NULL. pins: fn-fix-1-registry-rows/C-003
- [test_functions_f.py](test_functions_f.py) — FN-F (2026-08-15): try / session /
  bitwise wrappers through `ReparkSession` Arrow `to_arrow()` (value AND type).
  `uuid` pins type + uniqueness; `version` is the repark string. FN-GT1 later
  shipped ``bit_count`` / ``getbit`` / snake-case shifts; camelCase shift
  aliases and charter try_* stay absent.
- [test_version_ssot.py](test_version_ssot.py) — version SSOT pins (release PR): `__version__` == distribution version, PEP 440 release shape, past the 0.0.1 name-reservation era. Guards the `dynamic = ["version"]` maturin wiring.
- `test_partition_value_audit.py` + `_record_partition_value_goldens.py` — **V-4
  (2026-08-13):** write-path partition-key VALUE audit vs live Spark 4.1.2 + Iceberg.
  **AD-2 (2026-08-15):** F-V4-2 `+00:00`→`UTC` equality; F-V4-1 timestamptz projection
  unlocked (fork #192/#193). ruff-format lockstep on `test_metadata_tables.py`.
  Carry-check (identity int/string/date/timestamp, bucket, truncate, Iceberg
  years/months/days/hours UTC-epoch) + load-bearing SQL `year(ts)` /
  `date_format` identity under non-UTC sessions + TZ-8 CAST/to_date equality (R-4) +
  refusal-class pins. Ledger: `task/v4-partition-values-ledger.md`,
  `task/r4-tz8-ledger.md`.

- `test_collation_refuse.py` — **G15 (2026-08-12):** loud collation refuse. createDataFrame
  (`UNICODE_CI` / `UTF8_LCASE` / DDL / Spark `__COLLATIONS` fromJson), `cast`/`try_cast`,
  Spark SQL `COLLATE` / `ORDER BY COLLATE` / `CAST AS STRING COLLATE` / `SET`/`RESET`,
  `F.expr` / `filter` SQL-string, session/builder conf keys + getOrCreate reuse fold;
  default (non-COLLATE) distinct-count untouched; constructor + `simpleString` stay;
  `F.collate` / `F.collation` / `Column.collate` proven absent. Ledger:
  `task/y7-collation-refuse-ledger.md`.

- `test_t0_df_regions_import_freeze.py` — r27 T0 Q7 import freeze pins (r27 T0 overload)

- `test_declare_sorted.py` — **SE-1 PR-B:** the `declareSorted` door. Results bit-identical
  declared vs undeclared; the plan pin (tp=1 session) that the window `SortExec` really goes
  1 → 0 and the scan advertises `output_ordering=… ASC NULLS LAST`; the five loud refusals
  (unsorted data — and the view still answers afterwards; transformed frame; cached/persisted
  handle — cache intact after the refusal, declare-then-cache pinned as the sanctioned order;
  unknown name, listing the available ones; no keys); case-insensitive display resolution; snake/camel are
  one function; declaring twice is idempotent. Disclosed in the module docstring: the window
  ordering in the plan pin is spelled `ASC NULLS LAST` because Spark's `ASC` default is
  NULLS FIRST while the engine declares NULLS LAST. Ledger:
  `task/se1-declared-sorted-ledger.md`. **PR-D1 does not edit this file** (hint-mode
  nodes stay byte-identical).
- `test_declare_sorted_tighten.py` — **SE-1 PR-D1:** `tightenNulls=True` value-identical
  to hint with key fields non-nullable on `to_arrow()` **and** `df.schema` (SQM F4);
  refuse-on-nulls (names the flag); hint-after-tighten restores; both spellings share the
  keyword; `saveAsTable` create and `writeTo().create()` refuse a tightened frame and a
  tighten-derived frame (SQM F3). Round-3: delete-the-facade-layer mutant pin; right-side
  combinator marker (R-C); cache remint refuse (R-A); all-nullable CREATE + INSERT
  allowed and literal-over-tight refused (R-D); `df.schema` type-exactness; export
  metadata stripped; doctest examples execute; SQL-derived write + lazy-view
  CREATE refuse (Q-001); polars join marker (R-C); facade R-D Array/Map
  element-nullability helper (C1-Q-003). Serving-shape elision is the
  Rust Spark-door pin.
  Round-4: `CREATE VIEW <catalog>.<ns>.v` and `SELECT … INTO <catalog>.<ns>.t` over a
  tightened source refuse (Y-3 / Y-4 — both leaked on BASE, measured on the pre-fix native
  module), session-scoped names stay allowed, and the analyzed-schema export carries no
  `repark.tighten_nulls` tag (Y-6 — the node that kills the Rust strip mutant; the
  `to_arrow()` metadata assertions do NOT, and say so). Two `Kills:` claims were re-measured
  and honestly relabelled: the literal-over-tightened node is belt-and-suspenders, not a
  facade discriminator (Y-1), and the cache node's `saveAsTable` half is guarded by the facade
  marker while its SQL half is the R-A discriminator (Y-7 / verifier P-3).
  Round-6 (R6-1): `createOrReplaceTempView` is not a catalog-write door — a QUALIFIED name
  refuses (`AnalysisException`, `tableExists` false for both the 3-part and 2-part spellings),
  a one-part name stays SESSION-LOCAL under `SET datafusion.catalog.default_catalog`
  (`tableExists` false on the catalog name, true on the bare one), and the ordinary one-part
  registration still reads back (the allowed side). Measured on BASE: the lazy/empty body
  PERSISTED a `required: true` Iceberg table through this API, with no statement planned and so
  no guard in the path.
  Round-6 second pass (critic S1):
  `test_a_catalog_over_the_build_time_default_is_not_a_temp_view_home` — pinning the home to the
  configured default-catalog NAME was not enough, because `datafusion.catalog.default_catalog` is
  a supported BUILD-time conf: MEASURED on that fix, `createDataFrame([])` +
  `createOrReplaceTempView("v_leak")` both returned Ok and `tableExists("ice.sales.v_leak")` was
  **True**. The home now carries the schema PROVIDER, so such a session has no session-local home
  and every temp-view mint refuses loud.

- `test_t4_csv_smart.py` — r26 T2 decimal-union + sampling pins

- ~~`test_excel_reader.py`~~ — **NOT PORTED (phase-3 EC-4 deferral, whole file).** All ten
  r25 T5 `session.read.excel` / `sheet_names` pins fail against the binding's refuse-arms
  (`repark-excel` is post-milestone-one), so every test in the file defers by node id:
  [../../../task/port/deferred-python-tests.txt](../../../task/port/deferred-python-tests.txt).
  It returns with the connector crate, unchanged.

- `test_a3_cast_vocab.py` — **r24 A3 QUAL-03:** parametrized cast/try_cast over every
  (octo C4-Q-001: try_cast byte/short overflow → NULL; strict cast fail-loud);
  `types.py` primitive that claims cast + aliases; native residual → `AnalysisException`.
- `test_a3_secrets_redaction.py` — **r24 A3 SEC-04:** `_secrets.prop_key_is_secret` needles
  (conformance inventory covers every Rust arm + `bucket`/`arn` `_key` exclusions; octo C1-SEC-001);
  getAll isolation pin (octo C2-Q-002);
  `getAll` redacts; `get(explicit)` unchanged.
- `test_dynamic_flatten.py` — **r24 DF1** `DataFrame.dynamicFlatten` / `dynamic_flatten`
  (planner is native `repark_core::dynamic_flatten`; this file is the facade contract):
  nested struct-in-struct; null parent/mid struct → NULL not zero
  (createDataFrame-door Python `None` / clean children; CASE-drop is the engine
  dirty-child pin, not these fixtures); list-of-struct;
  **U-DF-1:** capitalized `Legs` list-of-struct + sibling struct (`Legs_leg_id`);
  multi-list serial explode order; list explode in-place column order;
  struct-in-list-in-struct; null-typed list drop;
  **DF-2:** default `empty_as_null=True` keeps NULL+EMPTY lists as a null-element row
  (`False` keeps NULL / drops EMPTY); in-test GA4 fixture pins both flag states
  (page_view empty / purchase full / session_start NULL);
  `drop_null_lists=False` void column + void-sibling SKU pin (SQM #176 V-2),
  the kept void column also pins `.schema`/`.dtypes` as `NullType`/`void`
  (SQM #176 W-1 Debug-spelling fail-open);
  **G3b (2026-08-18):** the GA4 fixture now carries the REAL `items[].item_params[]`
  (array-of-struct inside an array-element struct — the shape whose absence let the
  postfix-`[]` spelling defect ship), plus a standalone minimal repro of that shape on both
  doors (flatten is native Unnest; `_sql_array_of` postfix mutant only kills
  `explode_outer`), a scalar-inner nested-array guard, a map-element still-refuses-loud rider, and
  `test_create_dataframe_honors_requested_void` (D-5: explicit `NullType()` /
  `ArrayType(NullType())` is honored end to end instead of silently becoming string);
  max_depth LOUD refuse (`[DYNAMIC_FLATTEN_MAX_DEPTH]` token);
  bool flag type gates (incl. `empty_as_null`);
  name-collision prefix + same-pass + cross-pass + list→unnest refuse
  (`[DYNAMIC_FLATTEN_NAME_COLLISION]` token); empty-struct-only refuse
  (`[DYNAMIC_FLATTEN_EMPTY_STRUCT]`); interleaved
  in-place column order;
  idempotence on already-flat; H1 already-flat join preserves display overlay /
  origin binds (`test_already_flat_h1_join_preserves_display_overlay`); expanding
  one-field-struct H1 frame drops overlay (prefixed leaves, not parent display
  names — `test_expanding_h1_flatten_drops_stale_overlay`); **DF1 MIA `_plan()`
  pin:** uncached mapInArrow then `dynamicFlatten` materializes the bridge
  (`test_mapinarrow_dynamic_flatten_materializes_bridge` already-flat doubling;
  sibling `test_mapinarrow_nested_dynamic_flatten_materializes_bridge`
  `payload STRUCT<x: BIGINT>` → `payload_x`); revert to `_inner.dynamic_flatten`
  is 0 rows while createDataFrame flatten stays green; both method names;
  custom separator; explode_lists=False;
  schema-walk + plan-build collect/count/to_arrow spy pin (C1-Q-003);
  native-kernel
  docstring pin; dotted-separator list Unnest pin. Arrow value+type pins.
  (octo: trailing newline W292.) **C1-Q-002:** map-element refuse pins
  `dynamicFlatten` (`[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`) **and**
  `explode_outer` CAST spelling.
  **DEFECT-2 (2026-08-18):** projection over a multi-pass flatten — the full 15-subset matrix
  value-checked against the whole-frame export in BOTH explode orders, `count()`/`agg`, the GA4
  real shape flatten-then-project (coverage, not a reproduction: measured green on BASE), and
  `cache()` re-pinned as a plain pattern now that it is no longer the workaround.
  Guard: DataFusion's `push_down_leaf_projections` wrapped so it declines on the
  `Unnest`-carrying plans it miscompiles — `crates/repark-core/src/session/df_guards.rs`, NOT the
  `enable_leaf_expression_pushdown` flag (which stays at DataFusion's default);
  ledger `task/c25-bugfix-ledger.md`.
- `test_qi1_idents.py` — **r23 QI1 / CQ-006/007:** `_idents` SSOT pins (always-quote vs
  `functions_mod` is `repark.spark.functions` itself (EX-17, 2026-09-04): the shim re-exports only `__all__`.
  quote-if-needed classes; injection-probe battery with independent oracle equality / under-escape
  mutation pin; path-escape + probe-table freeze lockstep with Rust probes;
  re-export identity for session/dataframe/catalog/column/functions/merge).
- `test_catalog_staleness.py` — **T6 / CQ-008 / BUG-007:** list-on-access `listTables` sees
  out-of-band Catalog-API create; OOB drop absent not phantom (never-in-DF **and** DF-known
  product CREATE → OOB drop — F-T6-PIN-DROP-A / F-T6-PHANTOM-A); temps still appended after
  Iceberg OOB clean (F-T6-TEMP-A); `refresh_catalog_provider` escape hatch for free-SQL residual.
- `test_c4_expand2_facade.py` — **C4 expand2:** `PySparkAssertionError` under repark tree
  (check_error isinstance + `__all__` export pin); `repartition`/`repartitionByRange`/
  `repartitionById` Spark errorClass validation + single-node no-op (incl. sole-arg list/bool
  `NOT_COLUMN_OR_STR`); fillna list/subset/None/tuple errorClass pins.
- `test_c5_census_r7.py` — **C5 / r22 census-r7:** `Column.getItem`/`getField`/`__getattr__`
  nested access (array/map/struct) Arrow value+type; `try_cast` display + null-on-fail +
  LongType Arrow int64; `cast`/`try_cast` `NOT_DATATYPE_OR_STR`; `transform` chain +
  `NOT_CALLABLE`/`NOT_COLUMN` gates; `F.when` + chained `.when` str→`NOT_COLUMN`;
  `DataFrame.sameSemantics`/`same_semantics` non-DF→`NOT_DATAFRAME` + handle-identity pins.
  (**octo C1:** type gates + missing pins + sameSemantics honesty; **octo C2:** getItem
  Arrow int/string type pins.)
- `test_udf.py` / `test_udf_oracle.py` / `udf_oracle_funcs.py` — **U8 classic scalar Python
  udf**: `F.udf` / `@udf` / `spark.udf.register` / SELECT-list SQL rewrite; per-row cost pin;
  null + returnType coercion; composition refuse; Java register loud;
  live 4.1.2 oracle (importorskip without JVM). **octo C1:** string/comment registry-scan
  ignore pins; WHERE-string false-positive pin; register name simple-ident refuse.
  **octo C2:** qualified-col SQL arg; nested-subquery message; decimal float coerce.
  **octo C3:** case-insensitive register overwrite; sql()-time UDF snapshot pin.
  **octo C4:** comment-between-name-paren SQL; join/union after udf.
  **octo C5:** UDF+engine-func sibling without AS; VALUES FROM.
  **octo C6:** multi-arg null propagation; pure mask/strip helper unit pins.
  **U9:** expression-wrap (`+1`, CAST, abs), nested `f(g(x))`, WITH/CTE body, DISTINCT,
  ORDER BY alias (no `__repark_sql_udf_*` leak), decorator
  `returnType=` / `useArrow=` / duck-typed DataType, `registerJavaUDAF` loud pin.
  **U9 octo C1:** unaliased wrap/nested anti-leak schema pins; CTE non-pollution of
  pre-existing temp views (snapshot/restore); star (`*`) + UDF clean refuse.
  **U9 octo C2:** `count` UDF does not break engine `count(*)`; set-op refuse message.
  **U9 octo C3:** user UDF raise surfaces PySparkException (not rewrite UOE); fresh CTE
  name not left in catalog after WITH.
  **U9 octo C4:** ORDER BY NULLS FIRST/LAST after UDF refuses loud (no silent ignore).
  **U9 octo C5:** Spark optional-AS ``udf(col) alias``; multi-arg UDF + literal SQL pin.
  **U9 octo C6:** CTE column-list rename ``WITH c(z) AS``; SORT/DISTRIBUTE/CLUSTER BY refuse.
  **U9 octo C7:** no-FROM ``SELECT udf(lit)``; QUALIFY refuse with set of post-SELECT clauses.
  **U9 octo C8:** regression battery clean (S3-only); ruff format pass.
  **U10 / r21 T7:** WHERE UDF rewrite; GROUP BY alias/matching expr (keys-only);
  HAVING on alias; SELECT+WHERE UDF; GROUP BY+aggregate refuse-loud no leak;
  JOIN ON refuse-loud no leak. **octo C1:** compound WHERE base-column residual
  pin; aggregate HAVING refuse-loud no leak (no plan-garbage).
  **octo C2:** GROUP BY/HAVING UDF expr whitespace match pin.
  **octo C3:** WHERE UDF + subquery residual refuse-loud pin
  (`test_sql_udf_where_subquery_refuses_loud_no_leak`).
  **octo C5:** register reserved `__repark_sql_udf*` prefix refuse pin.
  **octo C6:** WHERE CAST residual type-token pin.
  **EXTRA F-E1-1:** IS DISTINCT FROM / trim(BOTH…FROM) / substring(FROM…FOR) /
  extract(YEAR FROM) + UDF WHERE pins (no bare keyword identity-project).
  **EXTRA F-E1-2:** compound WHERE columns named date/double/string/end + quoted
  `"from"` project; no `__repark_sql_udf_*` leak.
  **r22 U11 residual poles:** INTERVAL `'1' DAY` residual pin; INTERVAL
  `DAY TO SECOND` / `YEAR TO MONTH` multi-unit pin (octo U11 C1); typed
  `DATE`/`TIMESTAMP` literal residual pin (octo U11 C2); quoted `"and"` /
  `"or"` filter-boolean-steal pins; bare `when` pin-refuse (quoted works; no leak).
- `test_udtf.py` — **r23 C6 / U12 UDTF scalar-arg phase-2 core:** `@udtf` /
  lit-call multi-row expand (Arrow value+type); `spark.udtf.register` +
  `SELECT * FROM name(lit_args)` rewrite; column-subset SELECT; LATERAL /
  non-literal / table-arg refuse-loud; validation errorClasses held
  (`INVALID_UDTF_*` / `CANNOT_REGISTER_UDTF`); reserved name refuse;
  `functions.udtf` export; empty-eval empty schema; half-wired scalar guard.
  **octo C1 pins:** name-in-string/comment no-hijack; JOIN table-factor refuse;
  unclosed SQL string refuse; eval arity mismatch; multi-arg/NULL/TRUE/FALSE/case
  SQL; zero-arg + bad register name.
  **octo C2 pins:** SQL `1e2` scientific; trailing-comma refuse; start() failure
  still calls terminate(); bare yield None refuse.
  **octo C5 pins:** UDTF name colliding with SQL `max`/`abs` must not break
  SELECT-list calls; comma multi-FROM still refuse-loud.
- `test_c6_census_r8.py` — **r23 C6 census-r8:** `Catalog.registerFunction` /
  `register_function` → SQL + DF callable; `functionExists` registry probe
  (multipart + dbName signature); `UserDefinedFunction.deterministic` default
  True + `asNondeterministic` False + register preserves flag.
- `test_t7_census_r6.py` — **r21 T7 census:** `isStreaming` always False;
  `Column.substr` (int + Column args); `F.array_contains` values.
- `test_alter_table.py` — I6 / R-ALTER-TABLE: ADD/DROP/RENAME COLUMN schema-eq + read-after
  (added→NULL, rename data intact), ADD COLUMNS plural + FIRST, TYPE widen + narrow-refuse twin
  (int→long + float→double + decimal — octo C3), case-insensitive DROP (octo C5), DROP NOT NULL,
  loud refuse REPLACE COLUMNS / partition evolution / ADD NOT NULL, and **V3-6 C-005**
  Spark-equal DEFAULT DDL refuse (CREATE / ADD COLUMN / SET DEFAULT)
  (pins: v3-6-v3-types/C-005). FQ `mem.ns.table` only (no
  bare-name dependency).
- `test_alter_table.py` — I6 / R-ALTER-TABLE + I7 partition evolution: ADD/DROP/RENAME COLUMN
  schema-eq + read-after (added→NULL, rename data intact), ADD COLUMNS plural + FIRST, TYPE
  widen + narrow-refuse twin (int→long + float→double + decimal — octo C3), case-insensitive
  DROP (octo C5), DROP NOT NULL; I7 ADD/DROP PARTITION FIELD + write-after-evo + VERSION AS OF
  pre-evo pin (octo I7-C5) + case-insensitive DROP name, REPLACE PARTITION FIELD, REPLACE
  COLUMNS promote + identity-trap twin; residual refuse ADD NOT NULL.
  FQ `mem.ns.table` only (no bare-name dependency).
- `test_ml_feature_oracle.py` — **U2:** NaN-mix SQL fixtures CAST float literals to DOUBLE;
  CountVectorizer `1.0` SQL now yields decimal128 vectors (values still sum). R-ML-FEATURE (M2) + Q1 R-ML-QUANTILE: VectorAssembler, StringIndexer/IndexToString,
  OHE sparse, Standard/MinMax/MaxAbs scalers, Bucketizer, Imputer mean/mode/**median**,
  Tokenizer, **RegexTokenizer**, StopWordsRemover, SQLTransformer, Binarizer, PolynomialExpansion,
  **RobustScaler**, **QuantileDiscretizer**, **CountVectorizer+IDF**, pipeline e2e, live label oracle;
  STOP pins replaced (no dual-pin); octo c5 OHE all-null; octo c2–c4: StopWordsRemover unnest, Pipeline save/load, outputCol collision, foreign fit refuse; octo c1: SQLTransformer SELECT-only refuse + StandardScaler n=1 std pin.
- `test_e1_errorclass.py` — E1 R-CENSUS-ERRORCLASS: PySparkRuntimeError hierarchy, interval
  constructors, facade errorClass pre-checks, alias(metadata=), Column.__getitem__/__iter__,
  native surface shim; **octo C1 Fixer:** int element extract (not slice/fail-open), str
  field native eval, free-SQL quoted hostile idents; **octo C2 Fixer:** Column-key /
  non-int/non-str getitem not parent (`test_column_getitem_column_key_not_parent`,
  `test_column_getitem_non_int_non_str_not_parent`); **octo C3 Fixer:** open-bound slice
  no invented substr defaults (`test_column_getitem_open_bound_slice_no_invented_defaults`)
  + step → `SLICE_WITH_STEP`; **octo C4 Fixer:** map str getitem value pin
  (`test_column_getitem_map_str_key_extracts_value` — createDataFrame map Row `d["k"]`
  → value; does **not** invent `test_field_accessor` PASS); **octo C7 Fixer:** YearMonth
  invalid-constructor pins mirror DayTime (`123` / `(YEAR, 321)` — C7-Q-001);
  getitem closed-slice Spark substr value pin
  (`test_column_getitem_slice_substr_spark_semantics` — C7-L-001).
- `test_f1_sql_expander.py` — F1 R-CENSUS-R3-EC + **G1 UPDATE/DELETE:** free-SQL bare-name
  expander Path A (INSERT/SELECT/CTAS/MERGE + UPDATE/DELETE statement forms + e2e bare
  SELECT/INSERT/CTAS/UPDATE/DELETE; temp-view prefer on FROM; VIEW/TEMP TABLE non-rewrite;
  auto-memory spark_catalog qualify; EXTRACT FROM non-table; CTE name non-expand;
  **octo C1–C6** as before; **G1:** UPDATE/DELETE identifier scan, WHERE-subquery FROM,
  SET body never regexed, leading trivia; **octo C1:** table name ending in `set`, refuse
  eating SET keyword as table when target missing).
- `test_g2_window_rand_sampleby.py` — G2 R-CENSUS-R5: Window rowsBetween/rangeBetween +
  ranks; XORShift rand/randn; sampleBy seed-0 **exact XORShift key set** (not band);
  eagerEval repr/html + **HTML escape XSS pins**; RANGE non-numeric refuse + without
  ORDER BY; finite float frame bounds refuse; randn(0)/rand(0) sequence pins
  (**octo C1**); multi-ORDER RANGE refuse + ranking requires ORDER BY (**octo C2**);
  inverted frame start>end refuse (**octo C3**); ruff format/E501 XORShift helper
  wrap (octo gates).
- `test_g1_stat_and_expander.py` — G1 R-CENSUS-R4: `DataFrame.stat` property +
  corr/cov/crosstab/sampleBy/approxQuantile value+type pins; freqItems loud residual;
  Group H self-join attempt; **H1 r20:** condition-join / self-join_II–IV / AMBIGUOUS_REFERENCE
  / drop-by-Column / select parent Columns both sides / select_join_keys all how (STOP pin
  flipped to green); **octo H1-C1:** select-star multi-name, chained condition join,
  filter/orderBy parent Columns (value pin desc), select cast parent Column; **octo H1-C2:**
  withColumnRenamed / dropna / fillna / when on multi-name joins; **octo H1-C3:**
  toDF/alias/union/sample multi-name identity; **octo H1-C4:** withColumns/describe/dropDuplicates/intersect; **octo H1-C5/C6:** rename map/replace/randomSplit/dtypes overlay;
  **octo H1-C7:** selectExpr(\"*\"); bare
  UPDATE/DELETE e2e;
  **octo C1:** sampleBy fraction [0,1]+NaN; approxQuantile relativeError; join no-alias
  when column sets disjoint; **octo C2:** relativeError NaN; probability domain ValueError;
  **octo C3:** join(on=[]) crossJoin gate.
- `test_h2_group_h2.py` — **H2 r22** Group H long tail: non-origin dup projection multi-name
  map (cast/year/`sum,sum` display overlay); same-object self-join equi sugar + multi-token
  arm loud refuse + alias workaround; `Column.round` / wrap-display collapse;
  `spark.app.name==repark` bare getOrCreate verify pin (critic-octo C1 pins).
- `test_f1_errorclass.py` — F1 true-EC residual: array.array unsupported →
  CANNOT_INFER_TYPE_FOR_FIELD; make_interval collect → PySparkNotImplementedError;
  `_merge_type` / `_make_type_verifier` class+param keys.
- `test_f2_fail_value.py` — F2 R-CENSUS-R3-VALUE: nested tuple struct + name pad + map
  collect dict; scalar DoubleType CDF; csc/sec Inf at 0 (Arrow float64) + bare div NULL
  pin; overlay display -1; regexp_replace global; mixed lit list string coerce;
  `str(df)` / dtypes non-ascii; `printSchema(level)` tree depth (value+type pins);
  **octo C1:** Apache map null-before-int order; overlay `-1` value==omit + SQL; nested
  array-of-maps collect dicts; lit int+float float promote; **octo C2:** empty DoubleType
  keeps double + csc empty; overlay float pos type error; **octo C3:** mutation-proof
  combo (map+empty scalar+overlay+F1 nested WITH); **octo C4/C5:** lit numpy Integral/Real
  + homogeneous np.int64 list normalize; **octo C8:** ruff format pin asserts.
- `test_e2_readwriter.py` — E2 R-CENSUS-READWRITER: bare-name resolution
  (`resolve_table_name` / saveAsTable / table / writeTo / insertInto / MERGE /
  DROP TABLE SQL expander), `spark.sql.defaultNamespace` seed, parquet save/load +
  loud unsupported formats, ndarray lit dtypes + uint refuse + COLUMN_IN_LIST;
  **F1:** SELECT/INSERT expander pins now expect qualification (no longer residual);
  **octo C1 Fixer:** `spark_catalog` alias on `tableExists`/`databaseExists`,
  action-time writeTo/MERGE re-resolve after `setCurrentDatabase`, DROP expander
  non-rewrite pins (SELECT/INSERT/DROP VIEW/script), bare insertInto + MERGE e2e;
  **octo C2 Fixer:** quoted-dotted segment rejoin pin (C2-SEC-001),
  `listTables("spark_catalog.default")` alias (C2-Q-002), `table()` temp-view prefer
  e2e over catalog shadow (C2-Q-003), bare `read.option(snapshot-id).table` resolve
  (C2-Q-001);
  **octo C3 Fixer:** `test_save_unsupported_format_loud` requires
  `DATA_SOURCE_NOT_FOUND` (not format-name-only OR) — R1 retargeted to `orc`;
  **octo C4 Fixer:** `test_format_iceberg_load_does_not_prefer_temp_view` (C4-L-001),
  `test_write_csv_json_*` (C4-Q-002) **superseded by R1 round-trip pins**, ndarray dtype pins also
  assert Arrow `to_pylist` values (C4-Q-001);
- `test_t4_csv_smart.py` — **r25 T4** smartCsv + Q1 inference protocol: pure rung pins,
  messy preamble/BOM/ragged fixtures, value+type Arrow path (bool/int32/int64/decimal/date/
  timestamp/float64/string), `describe_ingest` diagnostics, opt-in header case normalize,
  default `.csv` r20-R1 regression guards. **B4 (round 4):** detect pins origin/main
  agreement-first (DS-4 known-limit elects the rival; headed TSV/`;`/quoted-pipe
  keep origin/main winners) plus D2 refuse (empty / multi-char / newline / CR /
  quote; `option("sep","")` does not fall through; a present `option("sep", ",")`
  on a file whose auto-detect elects `;` still parses as comma). Parse pins are
  labeled non-discriminating origin/main `csv.reader` regression guards.
- `test_r1_read_formats.py` — R1 CSV/JSON read+write: header/inferSchema/schema/sep/nullValue/
  multiLine readers; format().load; write→read Arrow value+type (flat + nested JSON); empty
  overwrite; unsupported parse options + orc DATA_SOURCE_NOT_FOUND; **octo:** numeric nullValue,
  default `_cN`, empty+sep, gzip RT, multiLine object loud / empty-array ok, bool loud,
  **partitionBy path wires hive dirs (R2; was refuse-loud)**, JSON schema null-fill, semantic
  options on `.csv()` shorthand.
- `test_r2_read_formats2.py` — R2 writer option matrix / path modes / partitionBy: quoteAll /
  escapeQuotes wired; dateFormat/timestampFormat refuse-loud; parquet compression; path
  mode overwrite/append/error/ignore; partitionBy hive layout + multi-col + append merge +
  unknown-col loud; **octo fix half:** root `read.parquet(partitioned)` no null-fill /
  no empty root part (C3-001/C6-001), duplicate partitionBy loud (C3-002), append col-set +
  type mismatch refuse (C2-001/C6-002/C7-002), append-onto-file + overwrite-symlink
  AnalysisException (C2-002/C2-003); residual read encoding/timestampFormat pins
  (divergences.md = D3).
  **octo C5 Fixer:** `test_spark_catalog_alias_writer_paths` e2e saveAsTable/writeTo/
  insertInto/MERGE with `spark_catalog.*` (C5-Q-001); DROP expander positive pins
  exact rewritten SQL + multi-name sole-target (C5-Q-002);
  **octo C6 Fixer:** `test_lit_object_ndarray_unsupported` +
  `test_lit_bytes_ndarray_unsupported` refuse with `UNSUPPORTED_NUMPY_ARRAY_SCALAR`
  (C6-Q-001 — no object/|S → array<string>).
- `test_pyspark_compat_smoke.py` — C2/X1 / R-PYSPARK-COMPAT: pinned Apache PASS tests via
  `repark-parity/compat` harness + meta-pins (redirect, no JVM, known-FAIL classified);
  X1 grows pin list to all tip PASSes (functions+column + still-green types/dataframe
  incl. `test_range`; octo C4: +hour/minute/second → 40 pins); **E1 +13 → 92 pins**;
  **E2 ndarray +4 → 96 pins**; meta known-fail = `test_field_accessor` (F2 moved wall off
  `test_lit_list` mixed-cast PASS; pin list / exact-count untouched).
  Unit pins for classify/fetch live in
  [`../../repark-parity/tests/test_compat_harness.py`](../../repark-parity/tests/test_compat_harness.py).
- `test_session_range.py` — X1: `SparkSession.range` Apache count pins + step/float/numPartitions;
  octo C1: empty/neg-step multisets, bool reject, numPartitions < 1;
  octo C2: float step int() truncation + Arrow int64 physical / facade dtypes int;
  octo C3: range after stop raises.
- `test_column_x1_census.py` — X1: Column between/pow/string/bitwise/eqNullSafe/lit temporal + trig;
  octo C1: bitwiseOR/XOR values, lit(time)/lit(list)/empty array, hypot 3-4-5, dayname(date);
  octo C2: eqNullSafe(None), between inclusive/inverted;
  octo C3: hour/minute/second on Time + Timestamp, date_add/add_months str count col, LongType API;
  octo C5: __ror__ values, F.array column-name strings;
  octo C6: hypot/pow Apache lit-second forms;
  octo C7: Enum→tuple/list lit, acos/asin domain pins.
- `test_types_x2_census.py` — X2: Row + createDataFrame nested/LongType census pins.
- `test_dataframe_x3_census.py` — X3 + octo X3: error-class seed (immutable params), drop(Column),
  join/conf + builder gate, sample overload/seed mix, randomSplit seed, count star + struct
  field names, explain extended hang budget, table, show, StorageLevel, toDF, dropDuplicates [].
- `test_ml_boost_oracle.py` — M4 R-ML-BOOST + **M5 booster-bytes** + **M6 CV parallelism** + **M8 every-ext save/load-or-pin-refuse**: `repark.ml.ext` bare import + ImportError pin, XGBoostRegressor E2E + lib-direct parity, **XGBoostRegressorModel + ClassifierModel save/load predict-parity** (M1 envelope + `booster.raw` via `save_raw`; atomic M7 overwrite; library-major version guard; octo M5 C1/C2 path/params pins), **LightGBM* model_to_string save/load**, **sklearn RF* pin-refuse** exact `pickle forbidden (arbitrary code execution on load)` (save/write/**load/read** — octo M8 C1), matrix completeness pin, no-pickle hygiene grep, **octo M8 C1** classifier-flag mismatch + `num_features<=0` refuse pins, ParamGridBuilder + CrossValidator (LR + XGB grid; best-map selection pins) + **parallelism=1 vs 4 avgMetrics determinism** (M6), OHE plural inputCols/outputCols, PipelineModel.save STOP-loud for ext (no hollow ext publish), training-row re-hold pins, ext MemTable GC ownership, sparse+dense densify 4096 cap, multiclass f1 loud refuse, stretch Classifier/LightGBM/RF lib-direct, numpy not at repark.ml top-level grep-gate; octo C2–C7 pins retained.
- `test_ml_estimators_oracle.py` — **U2:** intercept-only fixture labels `CAST(1.0 AS DOUBLE)`
  (bare `1.0` is DECIMAL and `fit` refuses). R-ML-ESTIMATORS (M3): LinearRegression OLS (perfect line,
  multi-feature, no-intercept, singular/elastic/standardization loud), RegressionEvaluator RMSE,
  BinaryClassification accuracy + **areaUnderROC rank-sum** (M5; ties midrank; non-binary refuse octo M5 C1; raw prefers over prediction octo M5 C3) + **areaUnderPR score-group AP (M6; ties order-independent octo M6 C3)** + **dense list rawPrediction extract (M6)** + short-vector loud refuse (octo M6 C4) + **M7 sparse VectorUDT rawPrediction extract** (missing index → 0.0; size&lt;2 refuse; inverted+both-index mutation pins; null-cell not densify-to-0; non-vector Map refuse; native sparse densify disclosure), LogisticRegression IRLS, KMeans default-init refuse +
  random init, save-path no training rows, live pyspark 1e-6 rel parity, numpy import grep-gate (native only; M4 ext carve-out);
  octo C2: model `copy()` isolation (LR/logistic/k-means), `maxIter=0` cold-start/init-only + num_rows;
  octo C3: transform width-mismatch loud; empty-feature intercept-only mean;
  octo C4: num_features/coefficients desync refuse; octo C5: empty evaluator refuse;
  octo C6: predictionCol collision refuse on transform; octo C7: MSE/MAE/R2 hand pins.
- `test_ml_feature_oracle.py` — **U2:** see the Q1 row above (NaN-mix CAST + CountVectorizer
  decimal vectors). R-ML-FEATURE (M2): VectorAssembler (+ **M7 sparseOutput**), StringIndexer/IndexToString,
  OHE sparse, **M7 SI keep × OHE dropLast matrix**, Standard/MinMax/MaxAbs scalers, Bucketizer, Imputer mean/mode + median STOP,
  Tokenizer, StopWordsRemover, SQLTransformer, Binarizer, PolynomialExpansion, pipeline e2e, live label oracle;
  quantile STOP stubs; octo c5 OHE all-null; octo c2–c4: StopWordsRemover unnest, Pipeline save/load, outputCol collision, foreign fit refuse; octo c1: SQLTransformer SELECT-only refuse + StandardScaler n=1 std pin.
- `test_ml_skeleton_oracle.py` — R-ML-SKELETON (M1): Param/explainParams, Pipeline fit/transform
  ordering, uid shape, repark-ml v1 persistence (+ no training rows) + **M7 atomic overwrite** + race aside cleanup + file-target overwrite, dense FixedSizeList +
  sparse struct createDataFrame round-trip, mixed-width AnalysisException, fit/transform
  foreign-frame refuse, live pyspark uid/explain importorskip.
- `test_select_global_agg.py` — R-SELECT-GLOBAL-AGG pins (select≡agg, MISSING_GROUP_BY, empty
  count, sticky `_is_aggregate` cast/binary/null/when/coalesce/abs, `sum+1`/`cast(sum)`/
  `abs(sum)`/`round(sum)` 1-row, `sum+lit` allowed, composed-agg+bare + nested free
  (`sum+id`, `coalesce(sum,id)`, `when(id,sum)`) → MISSING_GROUP_BY, hostile
  `lit("count(Int64(1))")` uncorrupted; C3: `sum(x+1)+lit`, `current_timestamp` companion,
  alias-then-compose/cast, quoted hostile count, case-preserved sum+lit, structural
  sql_expr mutation pins; **C4:** case-preserved sum.alias / alias+lit rebind, batch-4
  structural sql_expr + case-preserved stddev, asc/desc sql_expr, first(ignorenulls)
  SQL path, collect_list null/empty SQL, isnull+date_* sticky MISSING_GROUP_BY;
  **C5:** case-preserved pure rebind for first/last/collect_*/count_distinct/corr/covar,
  last IGNORE NULLS value pin, collect_set null-exclude+empty [], multi count_distinct
  SQL null-if-any pack ≡ native; **C6:** free-OR `_scalar`/`concat`/`greatest` select
  boundary + metadata, `sum+row_number().over` → MISSING_GROUP_BY (pure_global
  aggregate|foldable), case-preserved pure `sum(col+1)` ≡ SQL, pure collect_set nulls,
  polars `_sort_key` sql_expr; **C7:** `select(sum, rand)` / nested `sum+rand` →
  MISSING_GROUP_BY (nullary non-foldable + ungroupable; current_date still foldable),
  nested `sum+over` / `coalesce(sum,window)` / `when(sum).otherwise(window)` →
  MISSING_GROUP_BY via sticky `_has_ungroupable`; octo C1–C7; **combine C1:**
  select(explode, sum) → MISSING_GROUP_BY before generator short-circuit;
  **combine C3:** rebind sort sticky sql_expr/AF/generator + cube(`order`.asc);
  withColumns/withColumn sticky aggregate → `[INVALID_USAGE_OF_AGGREGATE]`;
  **combine C4:** `_grouping_col_sql` always `_quote_ident` + hostile-quote cube keys;
  **combine C5:** cube/rollup agg AS alias names + MIA plan-stable cube pin;
  **combine C6:** polars `_sort_key` generator sticky + orderBy/pl.sort refuse;
  non-finite float lit CAST embeds on select-global-agg).
- `test_pivot.py` — **U2:** NaN-key fixture CASTs `10.0`/`1.0`/`20.0` to DOUBLE (bare
  literals UNION NaN cannot cast to decimal). R-PIVOT pins (values/inferred/multi values/null IS NULL/count/
  alias/cap/limit-then-sort/cube refuse/countDistinct refuse/avg-min-max values/
  non-simple refuse/first-last ignorenulls partitions=1; c3: values order,
  distinct-before-limit, REPEATED_CLAUSE, bool true/false, cast values, NaN;
  c4: BIGINT key outside int32; digit-named measure count non-null not row-count;
  c5: count(\"1\") non-null not row-count; pivotMaxValues equality boundary;
  c6: count(cast/abs/coalesce) refuse not row-count; sum/avg/min/max/first/last
  lit(1) refuse on digit-named measure frame;
  c7: first/last .alias Arrow values; count(distinct_id)/count(distinct) non-null;
  c8: values-list ignores pivotMaxValues (list len>max succeeds; inferred still overflows)).

- `test_select_global_agg.py` — R-SELECT-GLOBAL-AGG pins (select≡agg, MISSING_GROUP_BY, empty count).
- `test_iceberg_hygiene.py` — I5 R-ICEBERG-HYGIENE: column-def CREATE schema-eq vs CTAS twin
  (Arrow names+types); PARTITIONED BY + TBLPROPERTIES; CTAS+cols rejection pin (Spark message +
  no orphan); CREATE/DROP BRANCH|TAG via SQL DDL + VERSION AS OF time-travel pins; default AS OF
  = current; DROP main / kind mismatch refuse; DEFAULT column option refuse; trailing AS OF
  misspelled RETENTION refuse; **r25 T2** `test_ref_ddl_replace_and_retain` (CREATE OR REPLACE
  lands + misspelled RETENTION still loud); octo C8 py-lint line wrap on NOT NULL create. No AWS.
- `test_stream_ipc_ingest.py` — I4 R-STREAM-IPC-INGEST named oracle: native
  `register_arrow_stream_as_temp_view` round-trip values/types + empty schema-only + non-exporter
  TypeError; bare `arrow_array_stream` PyCapsule path; exporter raise preserves exception type;
  mid-stream C-stream fail → no partial view + session usable; nested repark-stream re-entry
  must not process-abort (octo C1-SAF-001); mapInArrow C-stream primary path; C-stream vs IPC
  fallback row/schema multiset equivalence; mid-stream user exception → PySparkException +
  session usable; structural prefer-cstream-not-ipc pin. Companion: untouched-green
  `test_mapinarrow.py`.
- `test_mapinarrow.py` — U-SPIKE-MAPINARROW pins (values, empty, schema mismatch name/type detail, re-run, cache, unpersist re-run, SMALLINT/TINYINT/FLOAT widths, incremental IPC, upstream close-on-fail, MIA hide/GC, peek isEmpty/take/show, mapInPandas; C2: identity no-ops, selectExpr/alias/sample/union/crossJoin/summary, parquet write, cache+filter+unpersist, traceback both halves; C3: write then collect/write call-counter + temp-view keeps bridge; register track-before-sql / drop-on-fail; mapInPandas None loud; C4: parent re-run after filter/select, show peek max_output_rows, set-op/crossJoin left-staging drop on right fail; C5: upstream input pull-order (not collect-all), groupBy/agg multiset, plan-child MIA view reuse bound; C6: mapInPandas empty wrong-name/type loud + empty correct schema ok; C7: unpersist clears plan-ready so post-unpersist plan children rematerialize; unpersist+action then plan child / na._type_keys / groupBy not dangling; C8: mapInPandas yield-before-consume + empty-prefix-then-consume preserve input multiset; **combine C1:** mapInArrow→select(explode)/withColumn(explode) materializes bridge values not empty placeholder; **combine C2:** mapInArrow→select(sum,lit)/cast(sum)/sum+1 single prepare + Arrow sum pins ≡ pure AF; **combine C3:** mapInArrow→groupBy.pivot.sum Arrow values + call-count pin (F2xS1); **combine C4:** selectExpr `_plan()` post-prepare call-count + value agreement with select/filter; **combine C5:** alias/sample/randomSplit/summary/set-ops/crossJoin/unpivot + cube.agg plan-stable post-prepare call-count/value pins; cube AS alias column names; **combine C6:** mapInArrow→select(explode)/withColumn(explode) non-idempotent calls[n]==1 + tag values; **combine C7:** identity no-ops copy `_mia_plan_ready` post-prepare call-count/value pins; polars.join `_plan()` post-prepare vs DataFrame.join (C7-Q-001 / C7-Q-002)).
- `test_mapinarrow_oracle.py (+ mapinarrow_oracle_funcs.py picklable helpers)` — live PySpark 4.1.2 mapInArrow oracle (named deliverable).
- `test_applyinpandas.py` — U6 R-APPLYINPANDAS pins: single/multi-key values, null keys, empty input, global groupBy, StructType schema, schema name/type mismatch loud, empty wrong/partial/extra columns loud + zero-column empty ok (octo C1), user raise + traceback + cause + KeyboardInterrupt not wrapped, None return, non-callable, lazy until action + re-run, cache pins once, expression group key refused, cube/pivot refused, boundary-stitch multi-batch + null-type promote + empty-batch mid-stitch, engine orderBy key-contiguous stream seam, e2e multi-batch group calls-once (batch_size pinned to 8192 — the session default is 65536), schema cast overflow names column, empty group result schema ok, snake_alias, string schema types, multi-key null+empty-string.
- `test_applyinpandas_oracle.py (+ applyinpandas_oracle_funcs.py picklable helpers)` — live PySpark 4.1.2 applyInPandas oracle (named deliverable): values, multi-key, null keys, empty input, global groupBy, schema-mismatch class, empty-wrong-columns class; skips cleanly without JVM.
- `test_pandas_udf.py` — U7 + **M5/M6** `@pandas_udf` pins: SCALAR select/withColumn + multi-UDF one-pass + octo C1–C8 harden pins retained; **SCALAR_ITER** basic/multi-arg/pass-through/wrong-batch-count + dual-UDF streams (octo M5 C5); **pure GROUPED_AGG** mean/global/multi-key+multi-arg + large-group stitch (octo M5 C7); **M6 mixed UDF+builtin** order-independent + global crossJoin + **null group-key null-safe join** (octo M6 C1); cube/rollup refuse (octo M5 C6) + hostile returnType refuse (octo M5 C1) + GROUPED_AGG-in-select refuse + SCALAR-in-agg refuse; **M6 windowed GROUPED_AGG** unbounded `partitionBy` + **null partition keys** + **select alias overwrite** last-wins (octo M6 C1/C2); **M7** ordered default frame (UNBOUNDED PRECEDING→CURRENT ROW running agg) + duck-typed `_frame_start`/`_frame_end` rowsBetween; GROUPED_MAP/WINDOW functionType tag still loud; PandasUDFType ints match PySpark 4.1.2 (200/201/202/204).
- `test_pandas_udf_oracle.py (+ pandas_udf_oracle_funcs.py picklable helpers)` — live PySpark 4.1.2 pandas_udf oracle (named deliverable): SCALAR values/nulls/coercion/multi-arg/string/error/withColumn + **M5 SCALAR_ITER + pure GROUPED_AGG**; skips cleanly without JVM. Not Apache `test_pandas_udf*` census.
- `test_explode_rewrite.py` — R-EXPLODE-REWRITE pins (null/empty, one-generator, posexplode*
  STOP, str ColumnOrName, cast sticky, withColumn unnest, pre-aliased AS strip, multi-array
  exact type bind; **DF-2:** `explode_outer` on `array<struct>` + nested `web_info` struct
  keeps null/empty rows; void `array<Null>` keeps via `make_array(NULL)` and
  reports `NullType`/`void` from `.schema`/`.dtypes` (SQM #176 W-1);
  map element still refuses (non-discriminating regression guard);
  struct/map/void mapper unit pin;
  **G3b (2026-08-18):** the mapper unit pin now demands the **angle** nested-array spelling
  (`array<inner>`, never postfix `inner[]`), and
  `test_nested_array_cast_spelling_round_trips_in_engine` asserts the emitted spelling parses
  back to the same type through `make_array(CAST(NULL AS …))`;
  **U-DF-1:** string-form / `F.col` / getitem / casefold explode of
  createDataFrame `Legs`, `explode_outer('Legs')` null/empty keep, absent name still loud;
  octo c2: pre-aliased sibling, Timestamp outer type, reserved/mixed-case
  idents, hostile name quote, asc/desc sticky, alone-select outer; octo c3: compound
  mixed-case sibling, nested-list outer type, fn-call/subquery ColumnOrName not SQL inject,
  array-of-struct explode, coalesce outer type, size sibling; octo c4: sql.functions
  __all__/identity + posexplode STOP path, nested generator refuse, hostile cast reject;
  octo c5: F.size/coalesce/when/str refuse generator, nested explode refuse, chained cast
  compose, generator select dup-name preflight; octo c6: aggregate wrappers refuse,
  filter/orderBy/groupBy/agg refuse, nested array_length empty guards; octo c7: date
  wrappers + `.dt` refuse, Window.partitionBy/orderBy refuse, cube/rollup/groupingSets
  + SQL agg path refuse; **combine C1:** select(explode,sum/count) → MISSING_GROUP_BY
  before unnest; generator-only / generator+id still unnest; **combine C4:**
  explode(collect_list)/explode(array_repeat(sum)) + synthetic generator+agg select →
  MISSING_GROUP_BY; **combine C5:** generator alias/cast keep sticky aggregate bits so
  select(synth.alias/cast) still MISSING_GROUP_BY;
  **DEFECT-2 (2026-08-18):** `test_two_pass_explode_chain_survives_a_narrowing_projection` —
  the hand-written two-pass explode + struct-extract chain (no `dynamicFlatten`) reaches the same
  Unnest-over-Unnest shape, and a projection dropping an inner pass's column is green; this is the
  pin that proves the defect was the plan shape, not the flatten helper).
- `test_datasets_facade.py` — **conductor-18 DS-4 (2026-08-16):** facade pins for the five
  torture-dataset families generated by `python/repark-parity/datasets/<family>` at seeded
  `small()` scale (64 rows / seed 42 — never the 1M CLI default), written to `tmp_path` and
  read back through the facade; the generator table is the oracle, assertions are Arrow
  value+type. **nested** (the held DS-1 pins, landed on #154): parquet keeps the capitalized
  nested schema incl. `array<void>`; string-form / casefold / `F.col` / getitem
  `explode('Legs')`; `explode_outer` keeps null+empty rows on the scalar-element lists
  (`Tags`/`Scores`) **and** on `array<struct>` `Legs` (DF-2 flipped the refuse pin
  in place); `dynamicFlatten` struct unnest with parent-path prefixes and full-depth
  in-place column order (13 columns; row count is the outer-explode cartesian — see
  `_nested_full_flatten_rows`) with the null-typed list dropped; the BUG-CANDIDATE that
  pinned `count()` failing inside `push_down_leaf_projections` on that full-depth plan is
  FLIPPED IN PLACE (DEFECT-2, 2026-08-18): `test_nested_dynamic_flatten_count_action_is_green`
  now pins `count() == to_arrow().num_rows` plus a narrowing `select`. **schema_inference** POLICY:
  an under-sampled read misses the int32→int64 widening past `samplingRows` (full-scan
  control resolves int64) **and** the under-widened cast then refuses LOUD, never silently
  truncating; labeled-class resolutions incl. zero-padded ids reading as `int32`.
  **extreme_types**: decimal128(24,21) parquet round trip + POLICY p>38 → float64 demotion.
  **secrets**: reads are unredacted — standing detector if data-column flagging ever lands
  silently. **smartcsv**: delimiter zoo (declared `sep`) diagnostics, duplicate-header
  dedupe, ragged pad + synthesized `_c12` overflow column, null-token / bool-spelling /
  decimal-width classes vs typed truth. Three BUG-CANDIDATE pins across the file report
  engine behavior without changing it: delimiter AUTO-detect picks a rival delimiter on
  the embedded-delimiter corpus (B4 left this as a known-limit; declare `sep=`;
  European-locale files use `sep=';'`); a euro-comma column infers `decimal128` and
  then refuses the cast on the raw comma text (both corpora that carry the class).
  (The third — `count()` on the full-depth flatten plan — is retired: fixed by DEFECT-2.)
  Ledger: `task/c18-datasets-ledger.md`; B4/DF-2 record: `task/c25-bugfix-ledger.md`.

- **R-TPCH-V3 (W1):** SF10 disk gate / DIED exit 6 / subprocess signal→DIED /
  iceberg_wall vs parquet-not-Iceberg column pins; query_result JSON round-trip;
  octo C1/C2 lint: DIED outranks TIMEOUT in exit_code pin.

- **extra-octo E7:** private default cache root; refuse dir symlink data_root.

- **extra-octo E2:** timeout-then-ERROR priority; exit_code_for_board pins 0/3/4/5; cache symlink/zero refuse pins.

- **extra-octo E1:** private SF0.01 cache; bool≠int; first-timeout drain; multi-payload WRONG; exit codes.

- **octo C5:** TPC-H mutable-box SIGALRM keep + repark mid-repeat TIMEOUT→WRONG-RESULT pin.

- `test_catalog_hygiene.py` — #100 fast-follow: bare-session listTables lists temps,
  never SCHEMA_NOT_FOUND (explicit missing names still raise).
- `test_tpch_smoke.py` — **R-TPCH-HARNESS**: SF0.01 DuckDB-diff pins for all 22 TPC-H queries
  against `python/repark-parity/bench/tpch/sf1_status_ledger.json` (OK → value match;
  WRONG-RESULT → still disagrees; ERROR → EXPECTED-ERROR class (requires error_class); TIMEOUT still SF0.01 DuckDB-diff OK;
  silent fix/regression both red). Q1 is **WRONG-RESULT** after Z-3 U1 (Spark-typed
  `avg(l_discount)` vs DuckDB float). `importorskip("duckdb")`; tpch extension INSTALL try →
  skip if unreachable.
  Duckdb hard-provisioned in root `dev` group (scoreboard guard; polars/pandas skip precedent
  unchanged for *their* tests).
- `test_tpch_compare_unit.py` — TPC-H compare kernel + **V3** unit pins (int/float/Decimal
  off-by-one; mid-repeat; SF10 disk gate skip FINDING; DIED exit 6; subprocess SIGKILL→DIED;
  iceberg_wall column; parquet-not-Iceberg must not flip wall header; CLI SF>100 refuse) +
  **r24 G10:** baseline-ratios gate (`check_baseline_ratios` within/over ceiling, WRONG-RESULT,
  committed 22-query ceilings file; critic-octo empty/partial/zero-ok_checked fail-closed;
  full-22 under/over against committed baseline; NaN/inf ratio refuse) +
  **B1** pins (120s→300s Slow/hung timeout retry; merge_three_way; worse_status; CLI
  --engine/--timeout-retry; sail_engine import without pysail; octo B1-C1..C7: subject_label,
  sail unavailable skipped, no double-run, status coerce, kill-group, SF10 hard wall,
  Sail original_sql, per-engine DIED, gRPC disclose only when Sail ran, default engine repark).
- `test_tpcds_smoke.py` — **R-TPCDS-HARNESS** (D1) + **D2** pins: SF0.01 DuckDB-diff against
  `bench/tpcds/sf1_status_ledger.json` (OK → value match; ERROR → EXPECTED-ERROR class;
  TIMEOUT/DIED still SF0.01 correctness). Curated list includes Q5/Q80/Q84 (D2
  `SparkConcat` Utf8 fix, ledger OK);
  `test_curated_smoke_pins_d2_concat_fixed_queries` membership pin. Full 99 behind
  `REPARK_TPCDS_FULL=1`. `importorskip("duckdb")` only for missing duckdb/extension.
- `test_tpcds_compare_unit.py` — TPC-DS compare + runner unit pins (ordered vs multiset;
  120s→300s Slow vs hung; SF1 disk gate; exit codes 0/3/4/5/6; CLI SF>100 refuse;
  ROLLUP classify; gap census; **octo:** Exception≠EXCEPT classify; greylight hard wall;
  unknown status exit 4; InvalidPayload walls; ORDER BY strip literals/comments;
  t300_wall vs t300_budget; ledger expect 99; empty --queries refuse).
- `test_fuzz_smoke.py` — **R-SQL-FUZZER** (D3): seed-42 / 200-query always-on differential
  smoke vs DuckDB (`bench/fuzz/`); determinism + null-density + compare (Decimal exact,
  float tol) unit pins; aggregate `ord_tie` total-order; minimizer leftmost ORDER BY drop
  (mutation-proof max_steps=2 vs rightmost counterfactual) + heal-reject; bank JSON ROW
  fixture round-trip + pin replay; banked repro xfail pins (empty corpus OK); long-pass
  generator n=5000 determinism; negative seed reject; multiline compare sanitize; bank
  sequence continue + no-overwrite; corpus index full scan; minimizer join-drop clears
  WHERE; bank `has_order_by` header; JSON seed artifact pin; budget &lt;60s. Ledger:
  `task/d3-sql-fuzzer-ledger.md`.
- `test_write_bench_unit.py` — **R-WRITE-BENCH (W1 + r22 extension)**: pure helper pins
  for `bench/write/` (file-size parse, verdict classes NO_DATA / NO_K_BENEFIT /
  NO_STALL / PARTIAL_SCALING_PLATEAU, release-build probe, expected 2x rows helper,
  markdown scale/local-fs/INSERT-K disclosure, CLI empty-K + K=0 usage,
  datagen unknown-table refuse; **r22:** merge source-plan / expected-rows, synthetic
  narrow/wide parquet (**write I/O pin `importorskip("polars")`**; width/rows validated
  before polars import so bad-width/nonpositive pins green without optional polars),
  MERGE/OW markdown disclosures + rule-10 pin constants, CLI extension width/K guards).
  No SF1/1M wall in CI.
- ruff format lockstep (W4 functions.py).
- `test_df_batch2.py` — R-DF-BATCH2 cube/rollup/unpivot/explain + loud census;
  **combine C5:** cube/rollup AS alias column `c` + values; unpivot hostile quote pins;
  **combine C6:** cube first(lit('count(Int64(1))')) uncorrupted + GroupedData.count
  structural shortcut (C6-SAF-001).
- ruff format lockstep (W7 gate).

- `test_facade_hygiene.py` — R-FACADE-HYGIENE (W7) cdf hide/GC, fillna, dropDuplicates, OOS.

- `test_df_batch2.py (lint: functions_api not F; N802 sampleBy)` — R-DF-BATCH2 cube/rollup/unpivot/explain + loud census + C5 unpivot quote / cube alias.
- `test_polars_ns.py` — R-POLARS-NS str/dt/fill_null + differential. (rider: dt test sorts client-side — UNION ALL order flake)
- `test_polars_ns.py (skeptic fix: real-path starts_with/slice + quote pin; full census)` — R-POLARS-NS str/dt/fill_null + differential.
- `test_pg_jdbc_options.py` — PG2 offline option pins (jdbc overloads, format aliases, XOR/caps).
  Ported minus **one** node (EC-4): `test_jdbc_num_partitions_above_cap_is_unsupported` — the
  `read_postgres` refuse-arm pre-empts the engine's cap error. The other offline pins raise their
  `IllegalArgumentException` **facade-side**, before any native reader, and port green.
- `test_pg_jdbc_oracle.py` — PG2 named oracle (SKIP-LOUD without `REPARK_PG_DSN`; DuckDB skip-loud).

- octo-extra C5: concat sql_expr null-propagation guard

- octo-extra C4: cast/concat MERGE embed pins

- **octo-extra C3: MERGE lit embed pins for != / CASE**

- **octo-extra C2: cache transform pin; bare summary refuse**

- **octo-extra C1 (2026-07-30):** pins for take materialize; multiset *All refuse; schema evo loud

- `test_polars_core.py` — R-POLARS-CORE (importorskip polars; pl accessor; reject real Expr).
- `test_polars_differential.py` — R-POLARS-CORE rider: differential pins vs REAL polars
  (aligned pipelines byte-equal; divergences pinned: all-NULL sum NULL-vs-0, join collision
  loud-not-suffixed; repo-ruff strict: zip strict=, raw match patterns, pinned-ruff 0.15.22 format).

- `test_df_easy.py` — **R-DF-EASY**: selectExpr/toDF/dtypes/printSchema/set-ops/crossJoin/offset/alias/describe/summary/replace/sample/randomSplit/colRegex/no-ops.
- `test_df_printschema.py` — **DF-PRINTSCHEMA-1**: `printSchema` stdout byte-identical to
  Spark (flat / nested struct / array / `level=1` exact captures) plus the live leg;
  red-first 4 red of 4.
  pins: df-printschema-1-trailing-newline/C-002, C-003, C-004
- `test_cache_persist.py` — **R-PERF-CACHE** + **r23 CACHE1**: cache/persist self + is_cached + storageLevel;
  second action after cache cheap; derived after materialize; unpersist; localCheckpoint;
  clearCache real drop (live + orphan GC path + leaves `__repark_ckpt_*`); StorageLevel cosmetic
  warn-once; `repark.cache.max_bytes` refuse / zero-off / invalid+>u64 conf / builder.config path /
  conf.unset tomb (no builder resurrect); cache entry-point vs VALUES temp-view branch pin;
  localCheckpoint-after-cache truncates lineage; child-plan cache sharing OUT pin;
  object-identity only; type error on bad level.
- `test_merge_into.py` — **R-MERGEINTO**: builder upsert equals SQL-MERGE (COW + MoR); delete /
  partial update / insert dict; Column condition; temp-view cleanup (success + failure);
  no-clause `[NO_MERGE_ACTION_SPECIFIED]`; `whenNotMatchedBySource().delete()` and
  `.update()` execute (Arrow types); type errors;
  `withSchemaEvolution` refuses loud; equi-join sugar unit pin. Arrow path for row sets.
- `test_merge_scan_prune_semantics.py` — **MG-1 (2026-08-15):** MERGE residual-probe
  hardening pins (r1/M1 Utf8→INT 2-row upsert; r2/M6 BIGINT 3e9 vs INT no-abort;
  r3/M5 `t.city = 'Zürich'` battery shape + backtick `` t.`Zürich` `` column;
  r11/M7
  mixed-case ON ≡ pruning-off). Memory catalog, Arrow `to_arrow()` value AND type.
  Does **not** edit `test_merge_semantics_audit.py`. Ledger:
  `task/mg1-scanprune-hardening-ledger.md`.
- `test_merge_insert_scope.py` — **audit M4**: NOT MATCHED conditions/VALUES resolve against the
  SOURCE only — target-column reference is a loud analysis error (was: silent LEFT-JOIN NULL);
  source-qualified and bare names resolve to the source. Arrow path.
- `test_merge_store_assign.py` — **audit M9 / BL-4**: MERGE INSERT **and**
  `WHEN MATCHED UPDATE SET` ANSI store-assignment gate — boolean→int /
  timestamp→bigint / string→numeric refuse at analysis (`not ANSI-store-assignable`);
  numeric widening, NULL-fill and atomic→string still insert/update. Arrow path.
  UPDATE twins need a matching key so the SET path fires.
- `test_insert_store_assign.py` — **WI-1**: the NON-MERGE write-path store-assignment gate.
  `INSERT OVERWRITE` (all-columns AND column-list arms, plus the
  `write.mode('overwrite').insertInto` facade door) refuses `date→int` / `date→bigint` /
  `boolean→int` / `timestamp→bigint` / `string→numeric` with the shared
  `not ANSI-store-assignable` needle and the `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`
  sub-class, naming the column and both types; a refused overwrite leaves the prior snapshot
  intact. Positives: widening, narrowing, atomic→string, date→timestamp, NULL-fill and identity
  all still write. **WI-2 (2026-08-15) — the `=== WI-2` section:** the four plain-INSERT doors
  are refusals now, not a named gap. `INSERT INTO … SELECT`, `writeTo().append()` and
  `write.insertInto()` refuse with the `INSERT INTO` path label and the same needle, driven by
  the `InsertStoreAssignment` `AnalyzerRule` (`crates/repark-iceberg/src/write/insert_gate.rs`) —
  plus the G6-5 reverse pair (`INT → DATE`), a positive control that an **explicit user CAST**
  still writes (Spark treats it as the user's intent), and the honest residual: a literal
  `INSERT INTO … VALUES` row conforms inside the `Values` node where the synthesized and explicit
  casts are byte-identical, so `VALUES (true)` into an `INT` column still writes `1` while
  `VALUES (DATE '…')` is refused by the G6-3 CAST gate instead. Arrow path.
- `test_merge_semantics_audit.py` — **MERGE-audit corpus** (2026-08-14 audit gap-map rows
  c/d/g/n/o): null-safe `<=>` / `eqNullSafe` ON matches NULL keys (both doors); builder-door
  `=` NULL keys do not match; self-merge (target as source) updates once per row; join-key
  UPDATE on an unpartitioned target; partial INSERT NULL-fills omitted nullable columns.
  Arrow path, value + type.
- **octo-extra C3: format= refuse surface**

- **octo-extra C2: __all__ / export coverage**

- **octo-extra C1 (2026-07-30):** test_fn_batch1 pins ln/log10 + from_unixtime string type

- `test_fn_batch1.py` — R-FN-BATCH1 scalar wrappers (value+type+null; unsupported loud).
  **FN-FIX-1:** `test_isnan_null_is_false_non_nullable`, `test_create_dataframe_stores_nan_not_null`.
  pins: fn-fix-1-registry-rows/C-003
  pins: ex-10-functions-null-cond-misc/C-001
- `test_fn_batch4.py` — R-FN-BATCH4 aggregates/stats/hash census; **U2 (2026-08-13):**
  `test_stats_aggregates` VALUES `(1.0),(2.0),(3.0)` are DECIMAL(2,1) — compares go through
  `float()`; **FN-FIX-1** discrete `percentile_approx`/`approx_percentile` (column type, not
  t-digest interpolation) + SQL alias pins;
  octo c1 adds bool reject, SQL centroids 3-arg pin, Imputer NaN/missingValue,
  RegexTokenizer order, CV fractional minTF, fit temp-view cleanup;
  octo c2 UNION ALL token/id association pin + Imputer same-col in-place;
  octo c3 MinMax/MaxAbs NaN-tolerant fit pin;
  octo c7 StringIndexer fit temp-view cleanup pin
  (+ array-percentage STOP seed).
  `test_sha2_facade_hex_string_matches_spark` holds FN-SHA2-1.
  pins: fn-fix-1-registry-rows/C-003
  **FN-APPROXPCT-1 FIXED 2026-09-03 (FN-FIX-1):**
  `test_approx_percentile_discrete_bigint_matches_spark`.
  **FN-APPROXPCT-ACC-1:** `test_percentile_approx_sql_third_arg_does_not_change_discrete_p50`
  (repark `100.0` at accuracy 2; Spark `1.0`).
  pins: fn-fix-1-registry-rows/C-003
- `test_fn_batch3.py` — R-FN-BATCH3 datetime + Chrono≠Java + loud census.
- `test_fn_batch2.py` (octo C1: exact overlay/slice pins)` — **R-FN-BATCH2**: strings/collection value+type+null pins; loud census
  (soundex/sentences/arrays_zip/map_from_arrays/locate pos / array_join null_replacement).

- `test_repark_log_subscriber.py` — R-TRACE-SUBSCRIBER: subprocess MoR MERGE with
  `REPARK_LOG=info` asserts all five `merge.*` phase names + CLOSE timings on stderr; without
  env asserts none (subscriber inert). Process isolation required (global `try_init`).
- `test_types_simple_string.py` — R-PARITY-NITS / X2 simpleString/typeName/json/fromDDL/
  StructType.add / ArrayType / MapType / collation / toInternal pins.
- `test_types_x2_census.py` — X2 census: Row empty/unnamed repr + factory arity;
  createDataFrame LongType schema, nested list/struct/map, variable int arrays;
  **octo:** explicit nested StructType/MapType/ArrayType(String) Arrow values (not
  stringify), DDL nested array, map int keys, tuple/dict→struct, sparse exact keys.
- `test_sliding_avg_parity.py` — R-RETRACT-SHIM rider: live-oracle sliding-avg pins (NULL
  frames, collect values + to_arrow types; oracle verbatim in the X2 ledger).
  **U2:** `1.0`/`3.0`/`6.0` CAST to DOUBLE so the column stays Float64 (bare literals
  mixed with `CAST(NULL AS DOUBLE)` became decimal avg).
- `conftest.py` — autouse fixture clearing the process-wide `getOrCreate` active-session
  registry around every test (WU-4 isolation), and the session-scoped `spark_engine` live
  oracle. That fixture lives here rather than in a test module so every live module shares ONE
  JVM: a fixture imported into a second module would register a second definition and build a
  second session. `spark_iceberg_engine` stays in `test_parity_live.py` (single consumer).
  pins: perf-dynflatten-1-measure/C-002
- `test_create_dataframe_materialize.py` — R-PERF-VALUES / R-PERF-ARROW-CDF / **P1a** /
  **P2a**: createDataFrame materializes once (list/Row/pandas/polars/schema=int32; second
  action cheap; count correct); structural pins prefer `register_arrow_stream_as_temp_view`
  over IPC for tuples+pandas+polars+empty typed; IPC fallback when C-stream symbol absent;
  SAF-001 pins drop orphan MemTable when sql-after-register fails on C-stream, IPC, and
  untyped VALUES materialize paths; C-stream runtime error does not IPC-fallback
  (shared `_NativeRegisterProxy`). **P2a:** native-path spies assert pandas uses
  `_arrow_table_from_pandas` (not `_rows_from_pandas`) and polars uses
  `_arrow_table_from_polars` (not `_rows_from_polars`). **critic-octo C1:** typed
  StructType Double/Float refuse ±inf on native pandas/polars; native Decimal envelope
  raises PySparkValueError (parity with list path, not bare ArrowInvalid).
  **critic-octo C2:** duplicate pandas column names → PySparkValueError; object all-NaT
  + DoubleType schema → PySparkTypeError (not raw ArrowNotImplementedError).
  **critic-octo C4:** empty pandas/polars + StructType keeps declared Arrow types (0-row).
- `test_t1_cdf_ingest.py` — **r21 T1** createDataFrame ingestion parity: dict key-union
  order (3 oracle cases) + empty-first dict null-fill (octo C4) + type widening + synthetic
  Orders-shaped (Legs list<map> type honesty — octo C5) + residual int+str refuse +
  name-list length-bind pins (octo C6) + StructType; lint E501 wrap (octo C8)
  null-fill/drop extras; nested ArrayType(StructType) schema value+type (octo C3); Row
  mismatch refuse retained; Boolean/Long/Double/Decimal/Date/Timestamp pairwise merge
  refuse (scalar + list-of-scalar + map values — critic-octo C1/C2 + EXTRA XC1-L1..L4 +
  XC2-L1..L3); polars List(Struct)/Struct + pandas ArrowDtype list-struct
  collect/to_arrow value+type; Binary/Time refuse retained; wrapped `{"Orders":[...]}`
  via `json.load` + dict path chain.
- `test_n1_nested_dict_struct.py` — **r23b N1** `inferNestedDictAsStruct.enabled`: conf
  default (TRUE since the FA-4 owner flip, 2026-08-16 — divergence registry) /builder/set
  entry points + the default-unset struct pin; Q8 conf-false(explicit) map byte-identity + sparse-vector
  conf-invariant; conf-true list-of-dict / unnested cell / dict-in-dict / ragged
  null-fill / field-order / non-string keys; row-dict key-union conf-invariant (Q6);
  explicit schema wins (Q11); Orders shape Legs/ConditionalOrders array<struct> under
  conf true (Q10). Value+type on collect/to_arrow both conf states.
  **octo C1:** non-sparse struct with `indices` field keeps all fields; sparse super-set
  keeps `extra`; null-only key / all-empty / long+string / long+double refuse; nested
  list multi-row union; None key refuse.
  **octo C2:** list<list<dict>> field union (single + multi-row); conf strip truthiness;
  bool+long CANNOT_MERGE pin.
  **octo C3:** empty-list field then list-of-dict keeps array<struct>; string+struct
  CANNOT_MERGE pin.
- `test_select_naming.py` — **Group H** select/projection display naming vs live PySpark 4.1.2:
  mutation leak accepts `Int32(1)` as well as `Int64(1)` (F-Y10-1 Python lit width);
  full matrix (`(x + 1)`, cast-of-attr → child name, cast-of-compound → `CAST(...)`,
  cast-into-binary dual-slot, `negative(x)`, CASE/coalesce/concat/lit/date fns, alias wins,
  `<=`/`>=`/`isNotNull`, withColumn unaffected); value+Arrow pins (arith/cast/neg/when/
  coalesce/mod); mutation proofs (drop `for_select` → Int64/`t.` leak only; unstable cast →
  CAST text); agg CAST embed; string lit unquoted; **H2 multi-name map** for non-origin
  dup projection names (`select(x, x.cast)` / lit+s) + AMBIGUOUS getitem + `.alias`
  disambiguation; bare `select("X")` / `F.col("X")` keep requested spelling `X` (not schema
  collapse); `select("X","x")` dual names; CI getitem composition is NamedExpression not Alias
  (`df["X"]+1` → `(X + 1)`, not `x AS X`; octo r2 C3-L-005); H2 wrap collapses user
  `.alias("z")+1` → `(z + 1)` while agg keeps `sum(x AS y)`; requested-spelling projection
  is re-selectable (`select("X").select("X")` / `F.col` / getitem + mixed-case `alias("Total")`
  — octo r3 C3-L-007); residual sinks after `select("X")` — filter/where SQL, fillna/na.drop,
  dropDuplicates, withColumnRenamed/withColumnsRenamed, F.sum("X")/shortcuts/dict agg
  (octo r4 C3-L-008); `select("*", expr)` expands star; all 13 date projection names;
  `F.expr("1 + 1")` → `(1 + 1)`; `current_timestamp()` display.
- `test_session.py` — **U2:** `test_to_numpy_numeric_matrix` VALUES `(1.5, 2.5)` are
  DECIMAL → object-of-Decimal (no longer float64). Import smoke (`import repark`, `from repark import ReparkSession` — plus the
  `SparkSession` drop-in and `ReParkSession` pre-rename aliases, identity-asserted); the
  builder chain (`builder…getOrCreate()`, both snake_case and camelCase, fresh-per-access);
  **C3:** `getActiveSession`/`active`/`newSession`/context-manager/`getAll`/`isModifiable`/
  conf get-without-default raise / set(None) refuse / soft conf fold on getOrCreate reuse /
  createDataFrame promotes active; **octo C3 C1:** newSession BaseException restore pin +
  static conf set refuse + getAll copy isolation; **octo C3 C2:** foreign-active newSession
  pin + getOrCreate reuse skips static conf + type-error createDataFrame still promotes;
  **octo C3 C3:** conf.unset clears builder fallback (tombstone);
  **octo C3 C4:** soft-fold after unset + static unapplied-silence pin;
  **octo C3 C6:** SparkSession alias getActiveSession/active pin;
  **octo C3 C7:** CM enter does not promote active; ruff-clean noqa+format;
  **WU-4:** `getOrCreate` returns the identical object twice; differing builder config warns and
  returns the active session; `stop()` then `getOrCreate` builds fresh; stopped-session ops raise
  a named `RuntimeError`; engine-knob dual-spelling policy (identical values OK; conflicting ints
  raise naming both keys; unparsable int raises); a `sql("SELECT 1 AS a, 'x' AS b")` round-trip
  to `to_arrow` / `collect` / Polars with correct values; `count`; `pyarrow.table(df)` consuming
  the Arrow PyCapsule directly; `show` logging; that `repark.__version__` is exposed (the attribute
  the wheel import-smoke prints); `to_pandas` + the `toPandas` alias (values, column order, alias
  identity); and `to_numpy` (numeric matrix dtype/values, null→NaN, mixed-type object promotion,
  zero-row shape); WU-4 fidelity: show stdout/truncate, collect→Row, columns/schema
  (metadata-no-execution pin: an un-runnable `CAST('abc' AS INT)` plan resolves its schema so
  `columns`/`schema` succeed while `collect` on the same df raises — N4), `limit`+show row cap
  (the rendered grid is parsed: exactly two data rows, rows 3..10 absent — N3),
  select("*"), createDataFrame, read.parquet, isNull/isNotNull/when, F.expr substr-0 parity.
- `test_getorcreate_catalogs.py` — **R-GETORCREATE (dogfood 2026-07-28)**: `getOrCreate` on a
  LIVE session registers newly-configured catalogs (silent when the whole delta applied),
  repeat same-builder calls don't re-warn, same-name-different-config warns naming the kept
  catalog while the ORIGINAL registration keeps serving, malformed late blocks raise like
  the build path.
- `test_dataframe_actions.py` — **U2:** NaN/None collect fixture CASTs `1.5`/`2.0` to
  DOUBLE. **R-TAIL** DataFrame action surface vs live PySpark 4.1.2
  (zulu-17 oracle 2026-07-28; ruff line-length/format clean): `take`/`head`/`first`/`tail`/
  `isEmpty`/`toLocalIterator`
  return types (`list[Row]` / `Row|None` / `bool` / iterator), `n=0` / oversize / empty-frame
  matrix, negative-`n` (`take`/`head` → `AnalysisException`
  `[INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE]`; `tail(-1)` → `[]` — live Spark does not
  raise), non-int `PySparkTypeError` (incl. **bool domain** — `take`/`head`/`tail` reject
  both `True` and `False` (C8-Q-001; `False` alone would silent-`[]` under truthy-only
  bool guards), plus EDGE `None`/`float` — `take(None)`/`tail(None)`/`take(1.5)`/
  `head(1.5)`/`tail(1.5)` raise matching `num` (C8-Q-002); `head(None)` is **OK** → first
  `Row` / empty→`None`, not a reject), `head(1)` is `list[Row]` not `Row` (polymorphism pin
  vs no-arg `head()`), tail≠limit mutation guard, stopped-session pin for `tail(0)`/`tail(-1)`
  (must raise, not silent `[]`), **and** `take(0)`/`head(0)` after stop (C5-Q-001 —
  zero short-circuit before limit/collect would silent-`[]` while live-frame zero and
  tail-stop pins stay green), **and** `isEmpty`/`toLocalIterator`/`first`/bare `head()`
  after stop (C6-Q-002 — lifecycle parity; those entry points were unpinned while take/
  head(n)/tail stop pins stayed green), Arrow value+type on the take/limit path. **P2b:**
  streaming `toLocalIterator` value+type vs collect, partial-consume iterator pin,
  `to_arrow_batches`/`toArrowBatches` concat≡`to_arrow`, empty-frame **schema-bearing**
  zero-row batch (octo C1), multi-batch orderBy concat≡`to_arrow` + stream≡collect,
  maps/nulls/nested array-map stream≡collect, partial-abandon then full action,
  mid-stream `to_arrow_batches` → `PySparkException` (parity with `to_arrow`), repeated
  schema/columns stability post-stream (SchemaRef cache); **octo C2:** collect batch-wise ≡
  stream under orderBy, decimal/struct/date/ts/null-struct collect≡stream, dual interleaved
  iterators, `range(0)` empty schema batch; **octo C3:** cache+stream/collect/batches +
  unpersist re-run, collect mid-stream `PySparkException` parity with stream; **octo C4:**
  empty nested (array/struct/decimal/ts/map) + wide-empty schema-bearing batch.
  **r22 P5:** primitive fast-path collect ≡ to_arrow value+type; map/array-map convert;
  duplicate display-name positional `_rows_from_arrow_table`; empty/zero-col bulk assembly;
  schema classifiers (identity vs map-convert vs calendar-interval);
  **octo C1:** nested empty map value → `{}`; NaN/None identity pin; nested MonthDayNano
  refuse (list/struct/map); zero-col n-row live collect.
  Never only `show`.
- `test_builder_config_map.py` — **R-TAIL** `Builder.config(map=…)` PySpark 3.4+ form:
  multi-key map, int→str coerce, Spark `to_str` bool/None on **map, kv, and conf** arms
  (`True`→`"true"`, `None` stays `None` — not bare `str()`; conf arm load-bearing
  `True` → `IllegalArgumentException` so `int(True)==1` cannot silently build), empty map,
  map↔kv same-key overwrite (retained pins), **sequential map/conf update-merge** into
  existing `builder._config` (C4-Q-001: disjoint-key kv→map / map→map / kv→conf keep+add;
  empty `map={}` and empty conf `getAll()` must not clear prior keys — wholesale
  `self._config={…}` replace fails), **sequential conf same-key overwrite** (C7-Q-001:
  kv→conf / map→conf / conf→conf assign — `setdefault` / insert-if-missing on conf keeps
  prior values while C4 disjoint merge stays green),
  map+key together (map wins, no error; exclusive apply pinned with
  **non-overlapping** keys so merge-then-overwrite fails), non-mapping `map` →
  `AttributeError` on `.items()`, **conf missing `getAll`** → `AttributeError` matching
  `getAll` (C8-Q-003: `conf=object()` and same-call `map={…}, conf=object()` must raise,
  not soft-empty / fall-through), duck-typed `conf.getAll()`, conf≻map **and** conf≻kv
  precedence,
  **empty conf + map same-call** exclusive conf≻map (C5-Q-002: `getAll()==[]` still
  ignores map; non-empty conf≻map alone is a hollow pin for fall-through-to-map),
  **empty map={} + kv** and **empty conf + kv** same-call exclusive (C6-Q-001: empty
  container still excludes key/value; non-empty map≻kv / conf≻kv alone are hollow pins for
  fall-through-to-kv),
  load-bearing shuffle=0 via map still raises `IllegalArgumentException`,
  `**dict` unpacking is NOT the API (`TypeError`). Positional kv regression kept green.
- `test_t3_ux_polish.py` — **r21 T3** (2026-08-03): display_style conf.set→show + property/conf
  lockstep + module `repark.display_style` refuse-loud; **F-T3-001** conf.unset resets
  live style + conf.get to default `spark` (show spark-like; no split-brain); default
  `spark.app.name`=`repark`; `Column.round` + windowed TA chain; H1 bare-join export naming
  overlay on collect/to_arrow/to_polars/to_pandas (display names, dups positional, no
  `__repark_*` leak); **F-T3-002** multi-name Row pickle round-trip via `from_ordered_fields`.
- `test_display_styles.py` — **R-DISPLAY** (2026-07-28): opt-in `DataFrame.show()` styles via
  Combine note (R-TAIL x R-DISPLAY): the pre-combine `test_no_public_dataframe_tail`
  ownership pin is superseded by `test_public_tail_and_preview_tail_coexist_and_agree`
  — public `tail` (PySpark-parity full-collect) and `_preview_tail_rows` (bounded
  `limit_with_skip` display path) coexist by design and must agree.
  `repark.display.style` builder config + runtime `session.display_style` (`spark` default /
  `polars` / `duckdb`). Pins: default spark grid byte-identical golden; each style's exact
  rendering on a fixed 12-row ordered fixture (polars head5+tail5 with `…`; duckdb box + type
  row + `(N shown)` footer); NaN/null/truncation cells; empty + 1-row frames; invalid style
  refuses with `IllegalArgumentException`; private `_preview_tail_rows` returns last n via
  engine skip+fetch (no full collect); spark path does not call `count()`, styled paths do
  (extra scan disclosed). MUTATION: force default to polars → default golden reds.
- `test_display_styles.py` — **R-DISPLAY** (2026-07-28; harden cycle-1..8 2026-07-28): opt-in
  `DataFrame.show()` styles via `repark.display.style` builder config + runtime
  `session.display_style` (`spark` default / `polars` / `duckdb`). Pins: default spark grid
  byte-identical golden; each style's exact rendering on a fixed 12-row ordered fixture
  (polars head5+tail5 with `…`; duckdb box + type row + `(N shown)` footer); NaN/null/truncation
  cells; empty + 1-row frames; invalid style refuses with `IllegalArgumentException`; private
  `_preview_tail_rows` returns last n via engine skip+fetch; spark path does not call `count()`,
  styled paths do (extra scan disclosed); **`show(n)` keep-set** (polars `show(0)`/`show(1)`
  must not over-show; duckdb `show(1)` keeps first row not last-only and forbids body `·` when
  `tail_n=0` — C8-Q-001; duckdb `show(0)` footers `(0 shown)`); **`show` rejects bool `n`**
  (`PySparkTypeError` — C8-L-001; ORDER BY fixture so `show(1)` first-row fence is
  deterministic); **no public `DataFrame.tail`** (R-TAIL ownership); **partial-
  collect discipline** (`limit_with_skip(skip=total-fetch)` + no
  `collect`/`to_polars` + no root facade **or native** `__arrow_c_stream__`/`to_arrow` unlimited
  export + lws return must be the streamed plan + per-`to_arrow` row caps); boolean cells
  lowercase `true`/`false`; dtype row uses precise Arrow widths (TINYINT→i8/int8,
  SMALLINT→i16/int16, FLOAT→f32/float); `show(0)` logs `show(0 rows)`; **getOrCreate reuse**
  applies explicit `repark.display.style` (no false "may not apply" on pure style delta;
  case-insensitive key on build+reuse) and leaves style alone when the key is absent;
  **`truncate<=0`** shows full cells (Spark parity);
  **dual-cased last-wins** (C7-Q-001/C7-L-001: later mixed-case override + invalid last refuses;
  resolve-time last-wins when dual aliases present); **`total <= fetch`** tail short-circuit
  (C7-Q-002: no `limit_with_skip` / no negative skip).
  MUTATION: force default to polars → default golden reds; full collect+slice / root
  `pa.table(self)` / `pa.table(self._inner)` / decoy `limit_with_skip` → no-full-collect +
  `limit_with_skip` pins red; duckdb show(0) without `(0 shown)` → zero-footer pin red; duckdb
  show(1) middle `·` with empty tail → C8-Q-001 pin red; drop bool-`n` guard → C8-L-001 pin red;
  public `DataFrame.tail` → no-public-tail pin red; drop reuse apply → reuse pin reds; pure-style
  reuse still warns / no `_builder_config` sync → C6-Q-001 pin red; drop key `.lower()` →
  C6-Q-002 pins red; exact/first CI wins dual-case → C7-Q-001 pins red; drop `total <= fetch`
  short-circuit → C7-Q-002 pin red; `cap=int(truncate)` for `0` → C6-L-001 pin red;
  `str(bool)` → boolean pin reds; collapsed logical types → narrow-type pin reds.
- `test_session_config_knobs.py` — **audit G3 (SAF-006 / SAF-007)**: engine-knob `.config(...)`
  range validation pinned at the REAL user entry point
  (`ReparkSession.builder.config(k, v).getOrCreate()` — the Rust builder and `PyReparkSession::new`
  pins are not the user surface), for all three key families and **both spellings each**.
  Per-key policy, oracle = live PySpark 4.1.2 (zulu-17, re-run during the G3 remediation pass) +
  the `SQLConf` shipped in
  `spark-catalyst_2.13-4.1.2.jar`: `spark.sql.execution.arrow.maxRecordsPerBatch` has NO
  `checkValue` and documents "If set to zero or negative there is no limit" (live: `getOrCreate`
  OK, `conf.get` → `'0'`, query runs; also does not raise on the reuse path), so `0`/`-1` are
  ACCEPTED — the session builds and runs on
  the `to_arrow` path (value + Arrow type) with a warn-once disclosure that repark cannot emit
  unbounded batches; `spark.sql.shuffle.partitions` declares `checkValue(_ > 0, …)`, so `0`/`-1`
  raise `IllegalArgumentException` here with live 4.1.2's message VERBATIM, asserted by string
  EQUALITY plus the
  `PySparkException`/`RuntimeError` parents and the not-`ValueError`/not-`OverflowError` negatives;
  `repark.memory.limit.gb` keeps `0` as the bounded-pool opt-out (still builds + runs) and refuses
  negatives. **Recorded message deltas vs live Spark 4.1.2** (captured verbatim in the module
  docstring — `[INVALID_CONF_VALUE.REQUIREMENT] The value '0' in the config
  "spark.sql.shuffle.partitions" is invalid. The value of spark.sql.shuffle.partitions must be
  positive SQLSTATE: 22022`): repark drops the `SQLSTATE: 22022` suffix (no repark error carries
  SQLSTATE), the repark-native key spellings have no Spark counterpart so the same shape is
  emitted with the repark key substituted, and on the reuse path repark validates but does not
  *apply* the knob (PySpark's `getOrCreate` really applies builder options — captured 200 → 7).
  **Both `getOrCreate` paths are pinned per key family**: the fresh build AND the `_active_session`
  REUSE path (a session is established first — the autouse `conftest` fixture otherwise clears the
  registry and hides that path), including that a LEGAL value on reuse still returns the same
  object and that the batch-sentinel disclosure is not swallowed there. Timing is a disclosed
  divergence: repark raises eagerly at `getOrCreate` where a FRESH PySpark process returns OK and
  raises at the first `sessionState` touch (live-verified). Also pins the layer boundary — BOTH
  `_native.PyReparkSession(batch_size=0)` and `_native.PyReparkSession(target_partitions=0)` still
  refuse (the sentinel is a FACADE translation, not an engine relaxation) — and that
  `memory_limit_gb=1`,
  the smallest non-zero budget this entry point can express, is far above the engine's 1 MiB floor
  (SAF-007 is unreachable from Python; the arithmetic is pinned Rust-side by
  `memory_limit_gb_never_lands_below_the_floor`).
- `test_t2_sort_memory.py` — **r21 T2 sort-memory:** measure-first FairSpillPool pressure
  diagnosis (synthetic OHLCV + 17 float cols; ExternalSorter *or* SortPreservingMergeExec);
  `spark.conf.set("datafusion.*")` get/set round-trip + SHOW ALL engine pin;
  unknown/malformed/non-canonical (mixed-case, padded, trailing-newline) key refuse-loud +
  no store (+ no engine mutation for trailing-`\n` twin — extra-octo T2 E1-1);
  value quote-escape / injection fail-closed pin; builder `datafusion.runtime.memory_limit`
  alone; dual `repark.memory.limit.gb` + `datafusion.runtime.memory_limit` refuse; runtime
  `conf.set("repark.memory.limit.gb")` refuse (build-time only); OOM `to_arrow`/`collect` →
  `PySparkException` with DF message + REPARK conf hint (no pyarrow dynamic-source wrapper);
  unit pin for `_export_engine_error` noise strip; reverse-sort succeeds after pool raise
  via conf. **S-1 R1:** pool-type pins at the three `pool_size`-only sites now require
  `fair(` and forbid `greedy(` (A3: those were false-green under DF's greedy SET).
  **S-1 R2:** runtime `temp_directory` `conf.set` / SQL `SET` refuse loud (names
  `TMPDIR`, no store-only twin); builder key creates a `datafusion-*` DiskManager
  workdir. **S-1 R3:** module docstring default is RAM-relative (cap 8 GiB).
- `test_t2_spill_reach.py` — **S-1:** recon §3 battery. Small FairSpillPool (64 MiB SET,
  2 partitions): sort / hash_agg / distinct / grouping_sets / SMJ assert
  `EXPLAIN ANALYZE` `spill_count > 0`. hash_join + `array_agg` pinned AS failures
  (`Resources exhausted` + `HashJoin` / `array_agg`; `fair(` required, `greedy(`
  forbidden). Runtime-SET pool-type pin. Grouping sets over `md5`; SMJ on `md5`
  (range is pre-sorted); hash_join/array_agg use a 16 MiB pool + payload.
  ruff-format lockstep.
- `test_describe_namespace.py` — Group Z: `DESCRIBE NAMESPACE [EXTENDED]` + the
  `DATABASE`/`SCHEMA`/`DESC` synonyms through the facade. Pins the Arrow schema (`info_name`
  NOT NULL / `info_value` nullable, both `string`) AND values from `to_arrow()`, the v2 row set
  (absent property → omitted row, never `''`), the `EXTENDED` `Properties` rendering
  `((Amid,vm), (k1,v1), (k2,v2))`, the 14-row **redaction truth table** (Spark's `Utils.redact`
  matches the key OR the value against BOTH default patterns —
  `(?i)secret|password|token|access[.]?key` and `(?i)url` — so `{"innocent": "my password is
  hunter2"}` redacts on its VALUE, while `access_key`/`ACCESS-KEY` are shown by both engines),
  `.show()` rendering, the missing-namespace
  `AnalysisException` **class identity** (live pyspark 4.0.0 `SCHEMA_NOT_FOUND`), and the Z6
  regression that `DESCRIBE <table>` — including a view literally named `namespace` — is not
  shadowed.
- `test_show_namespaces.py` — Group AB: `SHOW NAMESPACES` + the `SCHEMAS`/`DATABASES` synonyms and
  the `FROM` spelling through the facade. Pins the Arrow schema (ONE column `namespace`, `string`,
  **NOT NULL**, no field metadata — the live v2 oracle's verbatim shape) AND values from
  `to_arrow()`, the `NamespaceHelper.quoted` row rendering (`ab space` → `` `ab space` ``), that
  `LIKE` is Spark's `StringUtils.filterPattern` and NOT SQL `LIKE` (full match not substring,
  case-insensitive, `|` alternation, `%`/`_` literal, the `LIKE` keyword optional, the pattern
  matched against the QUOTED row), `.show()` rendering, the unknown-catalog `AnalysisException`
  **class identity** (live pyspark 4.0.0 `SCHEMA_NOT_FOUND` / 42704), the two registry-rowed
  refusals
  ([NS-1](../../../docs/spark-sql-iceberg-parity.md#ns-1--show-namespaces-without-in-from-requires-an-explicit-catalog) /
  [NS-2](../../../docs/spark-sql-iceberg-parity.md#ns-2--nested-show-namespaces-in-catalognamespace-is-refused))
  failing LOUD, and that a
  relation named `namespaces`/`schemas` is not shadowed (Spark has no `SHOW <relation>` form).
- `test_perf_facade_collect_rows.py` — **PERF-FACADE-COLLECT-1** (2026-09-04): the binding row
  fast path against the pre-existing Python converter, kept callable as
  `rows_export.rows_from_arrow_table_python`. Both converters run on the same batch and every
  cell is compared by `repr` as well as by value, so a Decimal that lost its scale or an int
  returned as a float is red where `==` alone would pass. Matrix: every natively converted type
  at its extremes with a null in each column (int8..uint64 bounds, `f32` widening, ±inf,
  non-ASCII / empty / NUL / emoji strings across the three UTF-8 layouts, the three binary
  layouts, an all-null column), then the declined types (three decimal scales, date32, time64,
  timestamp, list, struct), then map / tz-aware timestamp through the supplied-column route,
  the calendar-interval refusal, duplicate display names, zero rows, zero columns, and the
  guard that the collector is re-enabled after collect.
  It also carries the answer pin for `COLLECT-STRUCT-ROW-1`, a divergence the round-2 review
  found and this unit did not cause: a `StructType` cell collects as a `dict` where live
  PySpark returns a nested `Row`, identically on both converters, so the equality pin beside it
  is what proves the divergence is pre-existing.
  pins: perf-facade-1/C-002, C-003, C-006, C-007
- `test_perf_facade_logical_names.py` — **PERF-FACADE-WITHCOLUMN-1** (2026-09-04): 17 planned
  statements plus a 12-deep `withColumn` chain and eight DataFrame transforms assert
  `_native.logical_column_names` is byte-equal to the analyzer-backed `column_names` — the
  invariant `DataFrame.columns` now depends on, since every repark analyzer rule rewrites
  through `NamePreserver`. Unaliased arithmetic, decimal and mixed-width integer coercion,
  wildcards, joins, unions, windows and case-preserved aliases are the cells that would move
  first if a rule stopped preserving names.
  pins: perf-facade-1/C-004, C-008
- `test_perf_facade_cdf_1.py` — **PERF-FACADE-CDF-1** (2026-09-05): the column-wise
  `createDataFrame` path against the legacy row-wise path, kept callable as
  `create_dataframe_rows._arrow_table_from_raw_tuples_legacy`. Both dispatchers run on the
  same input and every case compares Arrow field types, Arrow values and `collect()` by
  `(type name, repr)` as well as by value, so a retyped cell is red where `==` alone would
  pass. Arrow values compare by repr-per-row and collected rows by signature only, so NaN
  cells compare instead of never matching. Matrix: every scalar Python type with Nones in
  every column and whole-None columns,
  all merge-kind refusals with exact text, decimal envelope and int64-overflow refusals,
  tuples/lists/namedtuples/dicts/Rows/scalars, every schema form, empty frames, NaN/NaT
  witnesses, nested columns under both struct/map and legacy-coerce confs, ML vectors,
  `array.array` typecodes, 1e4 rows, and a live leg against PySpark 4.1.2 `createDataFrame`.
  The conf halves assert their own effect (struct vs map, first-only vs merged fields, UTC
  vs naive timestamps) so a session-reuse regression cannot make them vacuous.
  pins: perf-facade-cdf-1/C-002, C-003, C-004, C-007, C-009
- `test_row.py` — **G-ROW** (2026-07-27): pure-Python + collect pins for `repark.row.Row` vs
  live PySpark 4.1.2 (zulu-17 oracle first). Construction (keyword order, positional,
  `from_mapping`, mixed args+kwargs → `PySparkValueError` `[CANNOT_SET_TOGETHER]`;
  single list/tuple arg kept as one value — octo C1-L-002; user fields `_fields`/`_values`
  attr access returns column values not internal storage — C1-L-001);
  `__getitem__` int/negative/OOB `IndexError`/str/slice→`tuple`; E8 classes — missing str
  and wrong-typed key → `PySparkValueError` (not `KeyError`/`TypeError`); `__contains__`
  field names only; `__fields__` list copy; iteration values; `asDict` flat + recursive
  nested Row/list/dict; value-only equality (incl. vs tuple) + hash equal to plain
  tuple (`hash(row)==hash((…))`, set/dict interop — octo C2-Q-001); missing attr →
  `PySparkAttributeError` `[ATTRIBUTE_NOT_SUPPORTED]`; collect surface end-to-end. Out of
  charter: pickling, `Row("name","age")` factory. Select-display residue (ROW-003) is
  already closed on main by Group H (`test_select_naming.py`) — verified, no invented work.
- `test_errors.py` — the WG-3/U4 error-taxonomy matrix, end to end through the public facade: the
  subclass tree (`ParseException` ⊂ `AnalysisException` ⊂ `PySparkException` ⊂ `RuntimeError`,
  `UnsupportedOperationException` ⊂ `PySparkException`; Group S reparents Parse under Analysis for
  PySpark parity — the other leaves stay distinct) + re-export identity
  (`repark.errors.X is repark._native.X`); then, per entry
  point, {parse → `ParseException`, analysis → `AnalysisException`, execution → base} pinned on
  `spark.sql` (syntax / unknown-table with the table name preserved in `str` / an un-runnable
  `CAST('abc' AS INT)` execution error → base, not analysis), `F.expr` (syntax / unresolved-column),
  and DataFrame ops (`filter("a +")` syntax / `select("no_col")` analysis / a doomed `cast` at
  `collect` → base); the U4 pins (audit CQ-002/CQ-015, OTH-009) — the MoR-mode scope gate raises
  `UnsupportedOperationException` (message preserved, still `PySparkException`; the former
  partitioned-MERGE gate was RETIRED by A4 — `test_merge_partitioned_target_gate_retired_now_runs`
  pins that it now runs. **Group T NARROWED the MoR probe** and **Group Y moved it again**:
  merge-on-read `MERGE INTO` now runs on transform-partitioned tables too, so the surviving
  `NotImplemented` on `write.merge.mode` is an UNRECOGNISED VALUE — both probes here
  (`test_merge_mor_mode_gate_raises_unsupported_operation_exception` and the `RuntimeError`
  near-drop-in pin) set `TBLPROPERTIES ('write.merge.mode' = 'merge-on-write')`, a permanent gate
  rather than a scope boundary a later group will retire), a duplicate `CREATE NAMESPACE` and a `DROP TABLE` on a missing table raise
  `AnalysisException` with the iceberg kind (`NamespaceAlreadyExists`/`TableNotFound`) visible in
  `str(exc)` (Spark parity: those exception families extend `AnalysisException`); the
  near-drop-in pin — `except RuntimeError` still catches the typed
  exceptions, the MoR gate included; and the **Group X leaf-type pins** (derived from a LIVE
  pyspark 4.0.0 JVM oracle, not from memory) — an invalid `spark.sql.catalog.<n>.type` raises
  `IllegalArgumentException` with key+value preserved in `str(exc)`, the engine and facade config
  paths raise the SAME class, `df.select/filter/drop(123)` + `F.sum(123)` raise
  `PySparkTypeError`, `df.sort()`/`dropna(how=…)`/`createDataFrame([])` raise `PySparkValueError`,
  `df.nosuchattr` raises `PySparkAttributeError` (with `hasattr` still working), each asserting
  BOTH PySpark parents; plus
  `test_python_arg_errors_runtime_error_divergence_is_deliberate`, which pins registry
  [FA-3](../../../docs/spark-sql-iceberg-parity.md#fa-3--python-argument-wrappers-subclass-runtimeerror);
  **G-ROW**
  `test_row_missing_key_and_bad_index_raise_pyspark_value_error` closes the Group X Row
  residuals (`row["zz"]` / `row[object()]` → `PySparkValueError`, missing attr →
  `PySparkAttributeError`, mixed ctor → `PySparkValueError`) with existing leaves only
  (`PySparkKeyError` still deferred — no reachable malformed-Row raise). (`test_columns.py`'s `test_expr_referencing_a_column_raises`
  moved from `ValueError`
  to `AnalysisException` in the same change — the PySpark-faithful type.) The two execution-error
  CAST pins (`test_sql_execution_error_raises_base_exception` +
  `test_dataframe_collect_execution_error_raises_base_exception`) carry a **KNOWN-DIVERGENCE**
  cross-reference (F-BR-6) to the parity backlog — registry
  [`../../../docs/spark-sql-iceberg-parity.md`](../../../docs/spark-sql-iceberg-parity.md) §7 row
  **BL-1**, which holds the semantics and the intent-to-fix; a future CAST-parity unit **updates**
  these pins rather than obeying them.
- `test_catalog_flow.py` — the source publish job's path end to end from Python (the
  acceptance kernel): memory catalog + namespace, temp view → `tableExists` gate → CTAS
  (`USING iceberg` + `TBLPROPERTIES` incl. `format-version`) → `MERGE … UPDATE SET * / INSERT *`
  → `dropTempView`/`clearCache` → row oracle; the U1 facade pin
  `test_ctas_partitioned_by_end_to_end` (audit P0-1: `spark.sql` CTAS with `PARTITIONED BY` →
  value AND Arrow type via `to_arrow`, a partition-filtered read, and
  read-back-after-reregister — previously the clause was silently dropped); plus temp-view
  replace/drop semantics,
  `tableExists` semantics (absent table/namespace → False, unregistered catalog → error,
  one-part = temp views), and camelCase↔snake_case alias identity. WG2: the config-driven publish
  flow — the measured source publish job's `spark.sql.catalog.glue_alt.*` block (`type = memory`
  AWS-free use, `io-impl` present-and-dropped, no `register_memory_catalog` call) registers the
  catalog at `getOrCreate` and drives namespace → CTAS → MERGE round-trip; plus a malformed catalog
  block raising at `getOrCreate`. NR-1 (2026-07-12): a `repark.sql.catalog.<name>.*`-prefixed block
  registers identically (CTAS round-trip), and the same property under both spellings with
  different values raises the fail-loud conflict (error names both keys; raw values absent). WG-2
  (ADV-1): `spark.create_namespace(catalog, namespace, location=…)` places a CTAS's data under the
  set `location` (proving the facade → PyO3 → session → catalog path threads the property — the
  rglob is empty if the location is dropped). WG-5 (ADV-2 residual):
  `test_sql_create_namespace_location_places_ctas_data_there` — SQL `CREATE NAMESPACE … LOCATION`
  through `spark.sql` now sets the location too (previously only the programmatic call could); a CTAS
  lands under it, value + Arrow type checked on `to_arrow` + `.parquet` placement (empty rglob ⟺ the
  SQL LOCATION was dropped). U2 (audit BUG-001):
  `test_ctas_into_location_uri_only_namespace_places_data_there` — a `location_uri`-ONLY namespace
  (the pre-existing real-Glue-DB shape, built via `WITH DBPROPERTIES`) resolves for CTAS through
  the facade: data lands under it (empty rglob ⟺ the fallback read is gone and the memory catalog
  silently fell back to $TMPDIR), value + Arrow type on `to_arrow`.
- `test_namespace_location_guard.py` — **R-6 / G-6 Q1 (2026-08-14):** one facade
  pin, memory catalog, `repark.sql`-era imports. `spark.create_namespace` four
  shapes: create-new, same-location idempotent, contradictory location raises
  `AnalysisException` naming both paths, no-location idempotent.
- (combine 2026-07-29: match= patterns raw-stringed, RUF043)
- `test_interchange_parity.py` — **G-INT** interchange battery (oracle = live PySpark 4.1.2 /
  **FN-FIX-1:** list/tuple/object `float('nan')` stays DOUBLE NaN. pins: fn-fix-1-registry-rows/C-003
  zulu-17 / UTC / arrow.pyspark.enabled=true, measured 2026-07-27). **TZ-4 PR-1:** pandas
  timestamp cells compare wall-clock (same helper as Arrow) so UTC annotation is not a
  naive-`Timestamp` equality red. **INT-001** `to_pandas` /
  `toPandas` + `to_arrow`: full value AND type matrix for int32/int64/float/decimal/string/bool/
  date/timestamp with and without nulls (nulls → float64/object promotion, matching live Spark;
  every column's Arrow + pandas cells pinned on all three rows — C1-Q-004). **INT-002**
  `createDataFrame` from dicts / tuples / Rows / namedtuple+NamedTuple `_fields` (C3-Q-002) +
  schema reorder-by-name + partial-overlap fail (C6-L-001) /
  pandas (Int64+NaN→null; Timestamp/datetime64 tz+naive — C2-Q-001; typed all-null Int64/float/
  bool/string/ts preserve Arrow types — C3-Q-001; Int32/16/8 all-null==non-null int64 width —
  C4-Q-001; ArrowDtype ts/date/double all-null — C4-L-002; date/decimal all-null — C4-Q-003) /
  polars (typed all-null + Date/Datetime/Decimal + Int32/16/8 width-stable — C3-Q-001/C4-*) /
  list/dict/Row all-NaN→float64 + all-NaT→timestamp (C4-L-001) /
  `numpy.datetime64[ns]`→TIMESTAMP not int / calendar-unit `D|W|M|Y` all-null+non-null DATE
  occupancy-stable (C3-Q-001) / date+timestamp+Decimal literals —
  value AND Arrow type via `to_arrow` (never only `show`); **r21 T1:** dict key-union null-fill
  (not missing/extra refuse); Row still fail-loud; polars Binary/Time refuse retained
  (nested List/Struct accepted — see `test_t1_cdf_ingest.py`); pandas Arrow time/binary
  refuse retained; str-as-row, schema=str / set / dict / non-str names (C3-Q-003/C3-SAF-001),
  ragged widths, empty pandas/polars CANNOT_INFER_EMPTY_SCHEMA (polars +schema too), inf float /
  Timedelta + all-null timedelta/Duration (C4-Q-002) / `numpy.timedelta64` refuse (C3-L-001) /
  pandas IntervalDtype refuse (C3-L-002) / polars Binary|Time all-null refuse (C3-L-003; r21 T1:
  List|Struct|Array accepted via Arrow — see `test_t1_cdf_ingest.py`) / pandas PeriodDtype+Period
  refuse (C4-Q-002) / categorical int|str null-occupancy
  stable (C4-Q-003) / datetime64[ms|us|ns|s] all-null TIMESTAMP pin (C4-Q-001) / pandas
  ArrowDtype time|binary all-null refuse (C4-Q-004; nested list/struct accepted r21 T1) /
  datetime64 minute ``m`` ≠
  month ``M`` case-sensitive unit pin (C5-Q-001/C5-L-001) / complex64|128 refuse all-null+cells
  (C5-Q-002) / Sparse[int64]|Sparse[bool]|Sparse[object] null-occupancy stable
  (C5-Q-003/C5-SAF-002/C6-Q-001) / object-dtype NaN→DOUBLE + NaT→TIMESTAMP witnesses + pure-None
  VARCHAR (C5-SAF-001); schema pure rename positional + pure reorder by-name across
  pandas/dict/Row/polars/namedtuple/NamedTuple (C2-L-001/C6-L-001, no value swap);
  schema subset/partial-overlap fail loud; empty list+names / pure-None all-null columns pin
  non-Null Arrow string types (C2-L-003); tz-aware→UTC; Decimal scientific fixed-point + refuse
  under-scale/over-magnitude (C2-L-002); quote/escape (multi-quote).
  **INT-003** `to_polars` value+dtype matrix **with and without nulls** (C1-Q-005); round-trip
  `to_polars` → `createDataFrame` value identity for int64/string/float/bool/date; registry
  [TY-4](../../../docs/spark-sql-iceberg-parity.md#ty-4--createdataframe-widens-arrow-int32-to-int64) /
  [TY-5](../../../docs/spark-sql-iceberg-parity.md#ty-5--createdataframe-widens-decimal-precision-and-scale)
  pins for int32→int64 and Decimal(10,2)→Decimal(38,18) on the VALUES path (dtype asserted, not
  just value).
- `test_catalog_surface.py` — **R-CURCAT-FACADE** (closes G-INT INT-004 follow-up). Pins
  `tableExists` (3-part + **2-part under currentCatalog** + 1-part under currentDatabase + temps),
  `currentCatalog`/`setCurrentCatalog`/`currentDatabase`/`setCurrentDatabase`,
  `listCatalogs`/`listDatabases`/`listTables`/`databaseExists`/`getDatabase` (+ snake_case),
  namedtuple field shapes (`Database`/`Table`/`CatalogMetadata`), `spark.sql.defaultCatalog` seed,
  CATALOG_NOT_FOUND / SCHEMA_NOT_FOUND raises, listTables filterPattern (`*ent*` / `entity|other`),
  multi-catalog isolation, non-str → PySparkTypeError. **Y-3:** `getDatabase` value/shape
  (bare + qualified + `spark_catalog` alias), real `locationUri`/`description` when set,
  missing-namespace `SCHEMA_NOT_FOUND` **equals DESCRIBE sibling** (no SHOW precheck;
  AST forbids `_namespace_exists` on `get_database`), `locationUri` equals
  `probe_namespace_location_via_describe` on one memory session, FA-2 `listDatabases`
  still None.
  Remaining divergences rowed as
  [ST-1](../../../docs/spark-sql-iceberg-parity.md#st-1--show-tables-in-is-unimplemented) /
  [FA-2](../../../docs/spark-sql-iceberg-parity.md#fa-2--listdatabases-leaves-description-and-locationuri-as-none).
  SQL sibling smoke: `SHOW NAMESPACES IN` (full pin in `test_show_namespaces.py`).
- `test_parity3.py` — **R-PARITY3**: `createDataFrame(schema=StructType|DDL)` preserves int32;
  `show(vertical=True)` real `-RECORD` layout + only-showing-top-n. Row factory/pickle pins in
  `test_row.py`.
- `test_sql_alias.py` — **Q1 re-home**. `import repark.sql` fails; `repark.sql("SELECT 1")`
  is the ANSI-door callable (Arrow path, INT/INT truncates). `repark.spark.sql` alias
  package: `is` identity vs canonical `repark.spark.*`, loud gaps, sed
  `pyspark`→`repark.spark` smoke, top-level shim identity.
- `test_catalog_surface.py` — **G-INT INT-004** (historical bullet; current surface is the
  R-CURCAT entry above). Pins that still matter: `tableExists` / camelCase aliases /
  `clearCache`/`dropTempView`. Rowed listing refusals:
  [ST-1](../../../docs/spark-sql-iceberg-parity.md#st-1--show-tables-in-is-unimplemented) /
  [FA-2](../../../docs/spark-sql-iceberg-parity.md#fa-2--listdatabases-leaves-description-and-locationuri-as-none).
  SQL sibling smoke: `SHOW NAMESPACES IN` (full pin in `test_show_namespaces.py`).
- `test_dml_b_partition_overwrite.py` — **DML-B:** facade `spark.sql` `INSERT OVERWRITE …
  PARTITION` static/dynamic pins (values, Arrow types, snapshot operation, empty-dynamic
  refuse). pins: dml-b-insert-overwrite/C-001, C-002, C-004, C-005
- `test_sql_dml_eager.py` — WG-1 (F-BR-2): bare `spark.sql` DML executes **eagerly**, PySpark
  parity. A bare `INSERT`/`DELETE`/`UPDATE` whose returned DataFrame is never collected still
  applies the write (pre-fix: a silent no-op); collecting the returned DataFrame does not re-apply
  (exactly-once); a runtime DML failure surfaces at `sql()` time as the base `PySparkException`
  (NOT Analysis/Parse — WG-3). Every value check is on the `to_arrow` export path with the Arrow
  **type** pinned too (never `show`). **r25 T2:** `test_bare_sql_branch_tag_replace_round_trip`
  (CREATE OR REPLACE / bare REPLACE BRANCH|TAG success on facade; supersedes refuse-loud pin).
- `test_case_insensitive_conform.py` — WG-4 (BUG-007): case-insensitive by-name column conform
  through the real facade. A `MERGE … UPDATE SET * / INSERT *` whose source frame spells its columns
  in a different case than the target conforms by name (value AND Arrow type via `to_arrow`); two
  source columns colliding on one target raise a loud ambiguous error naming both. (The `ON`
  predicate/explicit references are DataFusion-resolved — a disclosed follow-up; the source column is
  named explicitly in `ON` here so the test pins the CONFORM, not that resolution.)
- `test_filter_predicate_rewrite.py` — **audit G2**: the SQL-string filter-predicate identifier
  rewriter (`DataFrame._quote_filter_sql_identifiers`), pinned through BOTH entry points
  (`.filter` and `.where`, parametrized) on the `to_arrow` path, value AND Arrow type. Three
  behaviours + their discriminators: (1) a casefold collision (`id`/`ID`) refuses **at the
  reference** — `filter("other > 0")` on that frame still runs (the over-refusal regression),
  every spelling of the colliding name raises `AnalysisException` naming both candidates (never
  last-write-wins, P4C5-Q-001), and the name inside a single-quoted literal is data, not a
  reference; (2) a token followed by `(` is a function call — `year(ts)` plans on a frame with a
  `year` column, and the **case-differing** shape (column `YEAR`, call `year(ts)`) is the true
  discriminator since DataFusion resolves function names case-sensitively (`"YEAR"(ts)` →
  `Invalid function`), while bare `year`/`YEAR` on the same frame still rewrites (P5C5-Q-001);
  (3) **all three** members of `_SQL_LITERAL_KEYWORDS` keep their grammar meaning against a frame
  that actually carries a column of that name — `["true","b"]`, `["false","b"]`, `["null","b"]` —
  each with the suppressed rewrite asserted to fail (`"true"` / `"false"` → non-boolean predicate;
  `b IS NOT "null"` → `ParseException`). Plus the pin for registry
  [ID-3](../../../docs/spark-sql-iceberg-parity.md#id-3--exact-duplicate-column-names-are-refused-at-construction)
  (exact-duplicate output names refused at construction on both paths — also the upstream guard
  behind `_by_name_casefold_map`'s defensive branch). Every behaviour is mutation-proven (each of
  the four rewriter rules reverted → this module reds; dropping any single keyword from the set
  reds it too).
  **Error shape:** the refusal is Spark's verbatim `[AMBIGUOUS_REFERENCE] Reference \`id\` is
  ambiguous, could be: [\`id\`, \`ID\`].`, asserted by string EQUALITY. Two recorded, deliberate
  differences from live Spark 4.1.2: repark lists the *actual* colliding columns where Spark
  echoes the reference spelling once per candidate, and repark omits the `SQLSTATE: 42704` suffix
  (no repark error carries SQLSTATE).
  **Oracle basis:** every golden was derived from live PySpark 4.1.2 during audit G2. Two recipes
  carry standing live legs in `_live_parity.py` (`filter_unambiguous_on_case_colliding_frame`,
  `filter_keyword_literal_false_column`) and all three disclosed divergences carry live
  `DISCLOSURES` legs; the remaining goldens (function-call skip, `null` keyword, mixed-case
  survival) are **hand-derived from that same oracle session with no standing live leg**.
  **Disclosed divergences characterized here** (behaviour fixes are out of charter) — the
  semantics live in the divergence registry, this map links:
  [`../../../docs/spark-sql-iceberg-parity.md`](../../../docs/spark-sql-iceberg-parity.md) §3
  **ID-2** (the two spellings that bypass the case-collision refusal), §7 **BL-2**
  (backtick-quoted idents are not a protected span), and §3
  [ID-3](../../../docs/spark-sql-iceberg-parity.md#id-3--exact-duplicate-column-names-are-refused-at-construction)
  (exact-duplicate output names refused at construction).
- `test_dropin_disclosure.py` — (+ Group F review 2026-07-21: withColumns lateral-alias divergence pin; + SEC-008 2026-07-24: `show()` logs a row-count breadcrumb at INFO but NOT row data — full render is DEBUG-only; + r23 CACHE1: `clearCache` is a real MemTable drop — no-warn disclosure pin only, behavior in `test_cache_persist.py`) WG-4 Clause 2 (OTH-010): the drop-in no-op / accepted-ignored
  surface. `clearCache()` no longer a silent no-op (CACHE1 Q11); `show(vertical=True)` and
  `master(url)` warn once per process (warn-once re-armed per test via the modules'
  `_reset_dropin_warnings_for_tests`). Group F adds `setLogLevel` silent no-op + `spark.version`
  repark-prefix disclosure pins. Rationale table: `docs/spark-sql-iceberg-parity.md` §8.
- `test_dogfood_gaps.py` — Group F (2026-07-21 dogfood): F1 `current_timestamp` µs/UTC Arrow +
  Iceberg v2 CTAS regression; **TZ-4 PR-1:** SQL / `F.expr` `current_timestamp` ns residuals
  flipped to µs+UTC; SQL / expr CTAS reject pins flipped to v2 success. F2/F3 `sparkContext`/`version`; F4 `withColumns` atomic +
  `withColumnsRenamed` (+ duplicate-name fail-loud); F5 `transform` signature/error class; F6
  DIVERGENCE-1 timestamp-LTZ collect passthrough disclosure (JVM-free). Oracles from live
  PySpark 4.1.2.
- `test_column_access.py` — (+ 2026-07-21 review pins: getitem requested-spelling naming, copy no-recursion) **Group G1** column-access sugar (2026-07-21; octo R1 Half B + R2
  Half B S1 CI getitem; **Group H octo r2** NamedExpression display): `df.x` / `__getattr__`
  (success, missing `[ATTRIBUTE_NOT_SUPPORTED]` AttributeError naming the column, method
  precedence over a column named `count`, **case-sensitive** attr, underscore column,
  existing-type dunder resolution + missing dunder → ATTRIBUTE_NOT_SUPPORTED, `repr`/`str`
  on getattr/getitem entry); `df["x"]` / `__getitem__` (str → Column; **case-insensitive**
  str resolve so `df["X"]` works when col is `x` (C2-L-001/002) with display/`repr`/`+1`
  NamedExpression `X` not `x AS X`; CI ambiguity → AnalysisException; int positional
  ±IndexError, Column → filter with schema equality, list|tuple → select with schema
  equality (C2-Q-002), missing str → eager `AnalysisException` with type-identity +
  `RuntimeError` hierarchy, bad type → TypeError); held-DF stop pins (`df.x` / `df["x"]` /
  `df[0]` / Column-filter / list-select / `df[1.5]` all prefer-stop `RuntimeError`);
  `Column.__neg__` (int64+nullable values, NULL rows, float64 values+type+nullable+null
  rows (C2-Q-003), `select` columns `negative(x)`, `str`/`repr` `Column<'negative(x)'>`,
  `F.sum(-df.x)` → `sum(negative(x))`, double `negative(negative(x))`, nested
  `sum(negative((x + 1)))` display **and** values). JVM-free pins from live PySpark 4.1.2.
- `test_columns.py` — **U2:** `SELECT 7.0 AS a` is DECIMAL(2,1); Column `/` stays float64
  3.5 (`test_division_is_float`); **R-2 A7:** SQL `SELECT 7.0 / 2.0` is decimal128(8,6)
  (`test_sql_float_literal_division_is_decimal`). The Column / expression surface (WG1): the seven `types` objects → engine
  strings; `col`/`lit`/`expr` construction (incl. the column-referencing `expr` boundary raising);
  arithmetic/comparison/logical operators; the Python-boolean misuse guards (`bool()`, `and`/`or`,
  `if`, and `in` on a Column raise PySpark's ValueError instead of silently dropping predicates);
  `alias`; `cast` (each type object + string spec +
  decimal precision/scale + the `long`/`bigint`→Int64 spellings with float-truncation, R2 enabler);
  `coalesce`/`concat`/`current_timestamp`; **D2** SQL-path `concat(coalesce(NULL,''),…)`
  Utf8 (not string_view) + SQL any-NULL → NULL + multi-row SQL null + non-string stringify
  pins (TPC-DS Q5/Q80/Q84 class); the DataFrame ops
  `withColumn`, `filter`/`where` (Column + SQL string), `select`, `drop`, `orderBy`/`sort`
  (asc/desc/nulls-ordering/`ascending`), `join` (inner/left, name/list/Column condition); an
  end-to-end chain; and `test_parity_*` cases run through the real `repark_parity` differential core
  against goldens recorded from **live PySpark 4.1.2** (zulu-17). Parity inputs are built with
  `createDataFrame` so BOTH engines infer the pinned type + nullability — an inline SQL `VALUES`
  fixture would pin repark's own int64/nullable/double shape as "Spark" (cycle-2 C1; `end_to_end_chain`
  registers its two frames as temp views so the by-name join gets distinct qualifiers).
- `test_sql_passthrough_parity.py` — the AR-WG-SQL adversarial corpus (C-AR-005): raw
  `spark.sql()` strings — no DataFrame-API mediation — pinning the audit's live-proven
  divergence classes on their exact inputs: integer `/` always-double (the S0 `5/2`),
  divide/modulo-by-zero **raises under default ANSI ON** (U5 / Q10=A) and is NULL when
  `.config("spark.sql.ansi.enabled", "false")` (literal and column divisors; **U2:**
  `1.0/0.0` and `5.0%0.0` are decimal; **R-2 U4b:** `1.0/0.0` is `(8,6)`), decimal ÷ decimal
  is Spark `(23,13)` (`test_decimal_division_stays_decimal`), ORDER BY null-placement defaults (asc/desc/explicit override/the LIMIT
  row-changing case/window `OVER (ORDER BY …)`), 0-based `[]` subscript + 1-based `element_at`
  (arrays incl. the index-0 error, maps), and the `substr`/`substring` position-edge matrix.
  Ends with the **divergence corpus × entry-point matrix** (added 2026-07-13 after the F.expr
  bit-reinterpretation regression): a seven-class corpus (division, div/mod-by-zero, substr
  edges, element_at, 0-based subscript) pinned through BOTH `spark.sql` and `F.expr` on the
  Arrow path, value and type — new expression entry points must join the matrix.
  Hand-computed Spark goldens through `assert_frames_equal` (real pyspark still not runnable
  here — tracked in `task/todo.md`). The module docstring flags the one non-ANSI CAST divergence
  NOT in the corpus — registry
  [`../../../docs/spark-sql-iceberg-parity.md`](../../../docs/spark-sql-iceberg-parity.md) §7 row
  **BL-1** (F-BR-6) — as green-only-excluded rather than a corpus row.
- `test_functions_dates.py` — WG2: `Window`/`row_number` (order, partition restart, Int32 type,
  over-on-non-window error, spec immutability), the `%` operator, and the 13 date functions
  (extractors incl. the `dayofweek` 1=Sunday trap, `last_day`/`add_months` month-end clamp AND the
  FN-ADDMONTHS-1 month-end divergence pin (2015-02-28 +1 → repark 2015-03-31, Spark 2015-03-28,
  `test_add_months_month_end_divergence_is_pinned`, renamed from
  `test_add_months_preserves_month_end_into_longer_months` whose comment mislabelled repark's
  clamp values as Spark's)/`date_add`,
  `date_format` Java patterns + unsupported-letter raise, `trunc`/`date_trunc` granularities + the
  `'Q'`→NULL / format-first cases); `test_parity_*` through the differential core (goldens recorded
  from live PySpark 4.1.2 — pin column name + Arrow type + **field nullability** + bit-exact values;
  nullability is part of the parity contract; the `row_number` non-null pin is a live assertion). The
  parity date goldens carry `nullable=True`, so `_date_spine` builds a **nullable** `calendar_date`
  via `createDataFrame` + `cast(DateType())` (both engines agree; an inline non-null `VALUES (DATE …)`
  spine would pin repark's nullable date-function outputs as "Spark" — cycle-2 C1). Ends with the
  acceptance kernel reproducing the `silver_dim_jobs.py` dim-dates transform shape with exact rows.
  pins: ex-6-functions-datetime-a/C-001
- `test_metadata_tables.py` — **I2 / R-METADATA-TABLES** named oracle: Spark
  `cat.ns.tbl.snapshots` (+ history/files/manifests/partitions/refs/entries/
  metadata_log_entries/all_* family) + `spark.table("…files")`; schema pins from fork
  inspect sources; row sanity on ≥3-snapshot fixture; real table named `files` wins; DML
  + AS OF composition loud; unpartitioned files/partitions drop the empty `partition`
  column (fork #194; declared rename of
  `test_unpartitioned_partition_column_divergence` →
  `test_unpartitioned_files_have_no_partition_column`) + readable_metrics-by-name
  pins (R142). Octo C1: FQ column
  named `files` not rewritten; UPDATE/CTAS
  DML refuse; paren AS OF refuse; metadata of real `files` table; tight readable_metrics
  interior pin (no hollow `len>=0`). Octo C2: JOIN metadata; TRUNCATE refuse; real
  `snapshots` wins; all_files ≥ files row bound. Octo C3: TIMESTAMP/SYSTEM_* AS OF refuse.
  Octo C5: CREATE VIEW meta refuse. Octo C6: DROP/ALTER meta refuse.
  Octo C8: ruff-format final; OCTO-CONVERGED. **H-1c (2026-08-10,
  [ADR-0006](../../../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md)):** the
  facade half of the enumeration decision — `SHOW TABLES` and `information_schema.tables` list the
  base table only (`test_metadata_tables_are_hidden_from_enumeration_at_the_facade`), and both
  spellings of a hidden name still return the same rows
  (`test_a_hidden_metadata_table_is_still_queryable_at_the_facade`). Hidden from the listing,
  never removed from the engine.
- `test_time_travel.py` — **I1 / R-TIME-TRAVEL** named oracle: multi-snapshot fixture (CTAS +
  append + MERGE) + tag/branch via `_testing_create_ref`; SQL `VERSION AS OF` /
  `TIMESTAMP AS OF` / `FOR SYSTEM_*` (incl. latest-`<=` at s2/s3_ts + mid-interval — octo
  C1-Q-001/L-001/L-002); reader options `snapshot-id` / `as-of-timestamp` /
  `branch` / `tag` (all mutex pairs + residual incremental denylist); filter/projection
  composition; current-read unaffected; write-to-branch/tag loud; `__repark_tt_*` hidden from
  listTables (rewritten in H-1b with the ephemeral-view leak fix: the SQL rewrite now RELEASES
  its pins, so the non-vacuity half of that pin comes from the reader-options registration,
  which still survives by design — it backs the returned frame; the filter step also asserts
  POSITIVE membership of the real table first, so an empty listing cannot green it);
  two-part AS OF fail-loud; unary-minus snapshot id named in error; multi-table
  JOIN dual VERSION AS OF (octo C2); RFC3339 Zulu TIMESTAMP; direct read_iceberg_table
  mutex kwargs; empty branch/tag loud (octo C3); schema-at-snapshot vs current after RTAS
  widen (static provider, not post-hoc filter — octo C4); SYSTEM_VERSION string ref;
  parquet+TT option loud; INSERT…SELECT AS OF; subquery AS OF; SNAPSHOT-ID case;
  branch option trims whitespace (octo C5); CTAS/MERGE USING AS OF source (octo C6);
  CTE AS OF; snapshot-id i64 overflow → AnalysisException (octo C7); triple mutex pin
  (octo C8). Arrow multiset **and** schema pins via
  `to_arrow`. Fork cites in module docstring (pin `4723104b`).
- `test_facade_polish.py` — aggregate **compound display naming** (live-recorded PySpark 4.1.2
  matrix: `sum((x + 1))`, `sum(CAST(x AS DOUBLE))`, `sum(abs(x))` **incl. negatives**, `sum(x AS y)`,
  reflected-op commuting `2 * x` → `sum((x * 2))` + float-literal `2.0` (2026-07-21 review pins;
  ruff: `match=` patterns carrying `|` must be raw strings — RUF043),
  user `.alias` wins; comparison/logical: `sum(CAST((x > 0) AS INT))`, `NOT (x = n)`, `AND`/`OR`,
  `IS NULL`/`IS NOT NULL`, multi-arm `CASE WHEN` values+names, `coalesce`/`concat` arity,
  when-after-otherwise reject, residual semantic-option denylist pins; I1: time-travel options
  no longer denylisted on iceberg, rejected on parquet) +
  `spark.read` expansion (quote-aware `.table` ids, SQL-fragment reject, `.format`/`.load`,
  `.option("path")` case-insensitive last-wins, semantic options fail loud on load/parquet/table,
  format case-insensitivity, load-arg beats option
  path, unknown-key tolerate, missing format/path → `AnalysisException`, `.schema` disclosed
  `UnsupportedOperationException` (C1-Q-007)). JVM-free pins; mutation-proof.
- `test_group_agg.py` — **U2:** signed-zero collect_set fixture uses `createDataFrame`
  (SQL `-0.0` is DECIMAL 0, no IEEE sign bit). **Group E (E1/E2/E7) + Group J**: the aggregation family, pinned to real
  (2026-07-22 review: ruff-formatted — the unit left the format gate red at tip)
  PySpark 4.1.2 (run locally under Java 17 — `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`). `groupBy`/`agg`
  (Column + dict form), the shortcuts (`count`/`sum`/`avg`/`min`/`max`), and the aggregate functions;
  output-name parity (`count`, `count(1)`, `sum(x)`, `avg(x)`, `count(DISTINCT x)`); and the mandatory
  edge pins through `assert_frames_equal` (value + Arrow type + nullability): sum/count/avg skip NULLs,
  `count(*)` counts rows vs `count(col)` skips (non-nullable bigint), `sum(int32)`→long widening,
  `first`/`last` with `ignorenulls` (deterministic under a single partition + an order-independent
  unique-non-null pin), empty-group→0-rows vs empty-df global→1-NULL-row, and an unresolvable-column
  `AnalysisException` (E8). **Group J:** `collect_list`/`collect_set` (NULL exclusion, empty→`[]`,
  sorted-contents pins — order nondeterministic; string empty-vs-NULL pin; dict form with **value**
  pins for list/set and multi-key dict; global `df.agg` list+set values; signed-zero collect_set
  + multi-col countDistinct divergence pin vs Spark; dict reducer names case-insensitive
  (`COLLECT_LIST`/`Collect_Set` — camelCase still rejected); empty-string multi-cd pin); multi-col
  `countDistinct` 2-col/3-col
  (name `count(DISTINCT a, b)`, LongType non-null, any-NULL-row exclusion; 3-col uses a
  within-group-varying third column — not the grouping key; empty-frame → 0); cross-engine e2e vs
  live PySpark (forces zulu-17 when present; skips on JVM gateway failure — never hard-fails
  routine suite); mutation proofs (set≠list routing; multi-col≠first-col-only; null-if-any struct
  pack; third-col-matters). **R3 (remediation):**
  the zero-arg shortcuts (`groupBy(g).sum()` /
  `.min()` full frame-equal goldens + avg/mean/max naming + the string-column exclusion) aggregate
  every numeric column including the grouping key (`[g, sum(g), sum(x), sum(y)]`, oracle-verified).
- `test_union_distinct.py` — **Group E (E3/E4)**: `union`/`unionAll` (UNION ALL by position, keeps
  left names); int+double→double type coercion as a GENUINE parity golden — inputs built with
  `createDataFrame` so both engines infer type+nullability identically (F1 remediation: the prior
  fixture used inline SQL `VALUES (1)`/`VALUES (2.5)` and pinned repark's own `double`/nullable output
  as the "Spark" golden, but Spark parses `2.5` as DECIMAL(2,1) → `decimal128(11,1)` non-null, so the
  pin was repark-vs-repark); that inline-decimal-literal divergence is now DISCLOSED and pinned in
  `test_union_inline_decimal_literal_diverges_from_spark` (**U2 / TY-3 dated 2026-08-13:**
  repark `decimal128(21,1)` nullable vs Spark `decimal128(11,1)` non-null. **U3 dated
  2026-08-13:** still DECLARED — U3 `fromLiteral` is `+ - *` only; UNION uses Spark
  `forType(INT)=(10,0)`, not digits. **R-2 dated 2026-08-14:** still DECLARED — hook is
  `TypeCoercion` / `coerce_union` (Int64→DECIMAL(20,0)), not a `decimal_precision` arm). Count-mismatch raises; `unionByName`
  (by name, reorders), missing-column raises by default + `allowMissingColumns=True` fills NULL (parity
  golden); `distinct`, `dropDuplicates()` (= distinct) and `dropDuplicates(subset)` with a
  deterministic-survivor pin (key set / identical non-key values, never an accident). **R4
  (remediation):** the `allowMissingColumns` + `dropDuplicates(subset)` parity goldens now build
  their inputs with `createDataFrame` (not inline `VALUES`) so both engines infer int64/nullable
  identically — GENUINE parity, re-recorded from PySpark 4.1.2 (the inline-`VALUES` fixtures pinned
  repark's own int64/nullable as "Spark" where live Spark is int32/non-null). **R5 (disclosure):**
  `test_union_int_string_coerces_to_string_diverges_from_ansi_spark` — repark coerces `int UNION
  string` → string (lossless), ANSI Spark 4 raises `CAST_INVALID_INPUT`; load-bearing. **R6:**
  `dropDuplicates` subset accepts list/tuple, rejects a bare `str` (PySpark-shaped `TypeError`).
- `test_na_rename.py` — **Group E (E5)**: `withColumnRenamed` (present + missing-is-noop); `fillna`
  dict (parity golden) + dict-miss→`AnalysisException` (mandatory edge), scalar numeric (value+type;
  the filled-column nullability is a disclosed Spark-inconsistency divergence, not pinned), `na.fill`
  string (parity golden), subset, type-family separation (bool/numeric/string); `dropna`
  `any`/`all`/`thresh`/`subset` and `na.drop`. Null-string fixtures use `createDataFrame` so the
  Arrow type is `string` (not `string_view`) and nullability matches Spark. **R2 (remediation):**
  `fillna(float)` into an integer column keeps the INTEGER dtype and fills the TRUNCATED value
  (`2.5`→`2`, scalar + dict + int32-width-preservation + the float-into-double control; value+type
  pins, oracle-verified). **R6:** `fillna`/`dropna` subset accept a `str`/tuple (not char-iterated),
  wrong-type → PySpark-shaped `TypeError`.
- (2026-07-23 review: RUF043 raw-string + format debt cleared) `test_writer.py` — **Group E (E6)**: `DataFrame.write` end to end against the in-memory Iceberg
  catalog. `saveAsTable` create (CTAS) / append / overwrite / error(+errorifexists, the default) /
  ignore; position-based `insertInto` (+ overwrite); identity `partitionBy` (partition-filtered
  read-back); and `format`/`mode` validation (non-iceberg + bad mode reject loudly — **Group X**:
  `test_mode_rejects_invalid` now pins `AnalysisException` + `[INVALID_SAVE_MODE]`, flipped from
  `ValueError` per the live pyspark 4.0.0 oracle). **C1-SEC-001:**
  malicious table names (`t; DROP`) raise `AnalysisException` on `saveAsTable` / `insertInto` /
  `writeTo`; path-escape segments (`".."` / `"a/b"`) reject at `_sql_table_ref` (O3-C4-SEC-001);
  valid multipart names still round-trip. Routes only
  through CTAS / `INSERT INTO` / `INSERT OVERWRITE` — no new commit machinery. **R1 (remediation):**
  `saveAsTable` into an existing table resolves columns BY NAME — a reordered same-typed append
  lands correctly (parity readback value+type), an extra/missing source column raises
  `AnalysisException`, and the insertInto-positional-vs-saveAsTable-by-name discriminator pins the
  two writers genuinely diverge on a reordered frame (oracle-verified on PySpark 4.1.2).
- `test_ctas_division_writeback.py` — **Group L-write**: CTAS integer-division type-derivation at
  the facade boundary. `ReparkSession.sql` CTAS into an in-memory Iceberg catalog, then read the
  written table back on the Arrow path (`to_arrow`), value + Arrow type: the load-bearing
  union-of-division (`SELECT 5/2 AS q UNION ALL SELECT 7/2` → double `{2.5, 3.5}`, the shape that
  failed at the parquet writer pre-fix), a bare division control, and a zero-divisor (`5/0` → NULL
  double, non-ANSI). Oracle: live PySpark 4.1.2 (non-ANSI), re-derived for the unit.
- `test_session_timezone_parity.py` — the **session-timezone differential corpus** (gap G1) plus
  the temporal-edge rows (gap G16), landed by H-1a split A. 20 recorded recipes — 18 over scalar
  literals plus the 2 `column_extract_*` rows over a real tz-aware TIMESTAMP **column** (a two-row
  in-memory frame registered as a temp view, the brief's own recipe) — recorded in record mode
  against live PySpark 4.1.2 (zulu-17, `local[2]`, ANSI on, `spark.sql.shuffle.
  partitions=2`) with `spark.sql.session.timeZone` set to the row's own zone — `America/New_York`
  and `Asia/Tokyo`, one either side of UTC so a sign error cannot pass both — plus the
  `current_timestamp` row, whose value is nondeterministic and so is pinned by Arrow TYPE.
  **Since H-1a split B (2026-08-10) most rows are EQUALITY rows:** the extraction fix landed, so
  thirteen of the recorded disclosures now assert `repark == Spark` (`repark=None`) — and that
  flip IS the fix's revert-red evidence, because undoing the fix reds every one of them.
  **TZ-4 PR-1 (2026-08-13):** instant-producer TYPE rows flipped to equality (`to_timestamp` Z,
  `date_trunc` return, DataFrame-API `date_trunc` column). `current_timestamp` type pin is equality.
  **TZ-4 PR-2 (2026-08-13):** zoneless LTZ input localizes in the session zone; `TimestampType`
  is µs+UTC; NTZ stays naive. Flipped to equality: CAST-str round-trip, two zoneless
  spellings, NTZ distinction. Residue: `zoneless_timestamp_literal_*` is VALUE-converged;
  extractor columns stay nullable (Spark non-null) — not the TZ-7 class.
  **TZ-8 (2026-08-14):** five CAST/`to_date` equality rows (NY, UTC, Tokyo forward, NTZ,
  epoch) plus a DataFrame-API pin (post-#95 `repark.spark.sql` import era). `datediff` stays
  residual. Ledger: `task/r4-tz8-ledger.md`.
  **Its 2026-08-10 rework grew the corpus from 20 rows to 29** (a size pin moved because an
  adversarial panel measured wrong-answer families the original rows were structurally blind to —
  every one of them hands the engine a `…Z`-suffixed string, i.e. only the shapes where reading a
  TIMESTAMP as a UTC instant is RIGHT). The nine added rows are: three ZONELESS-input disclosures
  (a `TIMESTAMP '…'` literal, a zoneless `to_timestamp` / `CAST(str AS TIMESTAMP)`, and a
  naive-`datetime` COLUMN — registry row TZ-7, now localized in PR-2);
  one `TimestampNTZType`-vs-`TimestampType` row that gives registry row TZ-6 a **recorded**
  basis (now an equality row); two `date_trunc`-COMPOSITION equality rows (a `DATE`
  and a string truncated and then read back — the single-hop `DATE` control row cannot see a
  whole-day error there); one DST fall-back `date_trunc` row; and two **DataFrame-API** rows
  (`entry_point="dataframe_api"` → `dataframe_api_extraction`, i.e. `df.select(F.year(...), ...)`,
  the facade's OTHER user entry point, previously pinned only by a Rust proxy).
  One row is still a disclosure (`zoneless_timestamp_literal_*`, extractor nullability only).
  (A twelfth WAS the `CAST(TIMESTAMP AS BIGINT)` unit bug — registry row
  **TZ-5**, which converged on 2026-08-12 when the timestamp-cast epoch-seconds fix landed;
  `pre_1970_timestamp_cast_to_bigint` is now an equality row and the equality count moved 17 → 18.
  That class's own per-entry-point corpus is `test_timestamp_cast_parity.py`.)
  `test_the_extraction_class_converged_and_the_residue_is_named` pins that residue by name, so a
  new disclosure cannot be smuggled back into the extraction class.
  A disclosure row pins BOTH halves (repark's actual output AND the recorded Spark output), and a
  failed one is CLASSIFIED before it raises: if repark's live output now equals the recorded Spark
  golden the message says CONVERGED and says flip-don't-delete; if it matches neither half the
  message says regression and sends you to record mode. Two control rows assert plain EQUALITY
  (DATE extraction and leap-day DATE arithmetic are session-zone independent on both engines) and
  are UNCHANGED by the fix — which is the half of the claim an all-disclosure corpus could never
  make. Also carries the conf-surface pins: the `UTC` default, the builder round trip, the
  accepted-but-neither-validated-nor-applied runtime `conf.set`/`unset` disclosure, the reuse
  path's deliberate laxness (an invalid zone on a second `getOrCreate` warns, never raises), the
  whitespace normalization that keeps `conf.get` on the engine's trimmed zone, and the engine's
  build-time refusal of an unknown or blank zone. Rows go through the facade `sql()` door or (for
  the two `dataframe_api_extract_*` rows) `df.select(F...)`; together they are the **facade** cell
  of the four-entry-point matrix, at BOTH of its user spellings. The other three cells are pinned in
  Rust against the same instants — `crates/repark-spark/tests/session_timezone.rs` (native
  DataFrame API + Spark door) and `crates/repark-sql/tests/session_timezone_ansi_door.rs`
  (ANSI door).
- `_record_session_timezone_goldens.py` — the **record driver** for the corpus above (NOT a
  `test_` module; never collected). Imports `ROWS` from the committed test module and re-runs each
  row's own `run_row` recipe on live PySpark under the row's own zone, so the recorded golden and
  the asserted recipe are one recipe, not two copies. Exit 0 = every recorded half still
  reproduces (schema name/type/nullability then values); non-zero prints the live values to paste
  back after deciding the move is deliberate. It never edits the corpus. Needs a JVM + `pyspark`
  (`uv sync --extra record`); invocation is in its module docstring and in `docs/history/hardening-h1/h1a-ledger.md`.
- `test_timestamp_cast_parity.py` — the **timestamp-cast differential corpus** (registry row
  **TZ-5**, landed 2026-08-12; **B-TZ-4** string-cast rows landed 2026-08-13), the facade cell of
  the `CAST(TIMESTAMP AS <numeric>)` epoch-seconds class **and** `CAST(TIMESTAMP AS STRING)`. repark returned epoch NANOSECONDS where Spark returns epoch SECONDS — a
  10⁹ factor, correctly signed, on the one shape a migrated job writes to get an epoch. 19 rows
  recorded against live PySpark 4.1.2 on the same basis as the timezone corpus, across **three
  facade spellings**: `sql` (16 rows), `dataframe_api` (2 — `F.col("ts").cast("long"/"int"/
  "double")` over a real tz-aware COLUMN, which crosses PyO3 as a bare `Expr::Cast` with no SQL
  string, i.e. the cell a SQL-only fix would leave wrong) and `expr` (1 — `F.expr`). The rows that
  carry the claim are the **negative FRACTIONAL** seconds: Spark uses `Math.floorDiv`, so
  `-0.5 s → -1` and `-1.25 s → -2` where truncation toward zero says `0` and `-1`. Truncation
  agrees with Spark on every positive instant and every whole negative second, so those two rows
  are the only things separating the real fix from the plausible one; the positive fractional rows
  are the other half of that fence. Also pins the same-path siblings (`INT`/`SMALLINT` — refused
  outright before the fix; `DOUBLE`/`FLOAT`/`DECIMAL`, which keep the fraction), NULL, and
  zone-independence over three zones. **TZ-4 PR-1** flipped the last disclosure
  (`bigint_to_timestamp_reads_seconds`) to equality — VALUE already agreed; the Arrow type is
  now `timestamp[us, tz=UTC]`. **B-TZ-4 (2026-08-13):** 12 equality rows for
  `CAST(TIMESTAMP AS STRING)` — LTZ under NY/Tokyo/UTC, trailing-zero fraction, half-second,
  microsecond, epoch, pre-epoch, NULL, DataFrame LTZ, DataFrame NTZ, `F.expr`.
  Year-shape (0001 / −0001 / +10000) is a Rust kernel pin (`to_timestamp` year 1 is
  outside the ns intermediate).
  Recorded Spark 4.1.2 strings in `task/v3-btz4-ledger.md` §2. `test_the_class_is_covered_per_entry_point_and_per_edge`
  pins the corpus SHAPE (all three numeric spellings, both signs of the floor edge, every named
  numeric target, the zone matrix, B-TZ-4 STRING/NTZ/expr doors, and zero disclosures) so the
  class cannot decay into "one representative case". The numeric class is zone-INdependent; the
  string class is zone-sensitive. Engine cells: `crates/repark-spark/tests/timestamp_cast_seconds.rs`
  and `crates/repark-sql/tests/timestamp_cast_ansi_door.rs` (numeric); string kernel pins live in
  `crates/repark-functions/src/timestamp_cast.rs`. Ledgers:
  `../../../task/tz5-cast-seconds-ledger.md`, `../../../task/v3-btz4-ledger.md`.
- `_record_timestamp_cast_goldens.py` — the **record driver** for the corpus above (NOT a `test_`
  module; never collected), the same shape as `_record_session_timezone_goldens.py`: it imports
  `ROWS` from the committed module and re-runs each row's own `run_row` on live PySpark under the
  row's own zone, so the golden and the asserted recipe cannot drift apart. It never edits the
  corpus. Needs a JVM + `pyspark` (`uv sync --extra record`); invocation is in its module
  docstring.
- `test_merge_differential_parity.py` — the **MERGE INTO differential corpus** (H-2 gap G3,
  record-side). 11 rows (budget 8-11): basic upsert control, duplicate source keys (error-class
  `MERGE_CARDINALITY_VIOLATION` on both engines + insert-only that commits both rows),
  `WHEN MATCHED AND` arm ordering / threshold first-match-wins, NULL merge keys (NULL=NULL does
  not match), insert-only and delete arms, conditional matched update by target predicate, and
  the `WHEN NOT MATCHED BY SOURCE` content row (DML-A; engines agree on values and types)
  unmatched target), and — **audit M11 (2026-08-15)** — `dup_source_keys_unconditional_delete`, now
  a **content** equality row: the M11 exemption landed, so repark matches the recorded Spark
  survivor table (id=1 / name='a') for duplicate source keys against a SINGLE unconditional
  `WHEN MATCHED THEN DELETE`. The Spark half was RECORDED live; do not hand-edit the golden.
  The budget ceiling moved 10 → 11 for it (commented at the gate).
  Every content row runs create → seed → MERGE → read back on a real Iceberg
  table and asserts post-MERGE contents on the Arrow path (value AND type AND nullability) via
  the parity comparator; error/split rows pin the error token. Lifecycle helper (cleanup on
  success and failure — no stray warehouse tables) lives in this module beside the recipe SSOT.
  Recorded against live PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` (exact Spark-minor
  match, **derived** from the pinned pyspark version in repark-parity's record extra — CP-8 /
  N-2b; jar is record-time only via `spark.jars.packages`). Split-path convergence is CLASSIFIED
  (CONVERGED → flip to content equality; commit-but-mismatch → regression) when repark stops
  refusing — not a bare "expected raise". **N-2b (2026-08-11):** GAV + pyspark-version helpers
  live in `_oracle_pins.py` (one importable home; this module re-exports them). Dead
  `spark_needs_cow_props` row knob removed; re-derive recipe quotes the full parity-live sync
  line. **4 Rust pins** in `crates/repark-spark/src/tests/merge.rs`. **Items 2+3** (lifecycle
  live + 13 timezone live scenarios) land in the second N-2b PR — see
  `task/n2b-merge-followup-ledger.md`. Archive: `docs/history/hardening-h1/n2-merge-ledger.md`.
- `_oracle_pins.py` — **one importable home** for the Iceberg Spark-runtime GAV + pyspark-version
  derive helpers (CP-8 / N-2b). Exports `ICEBERG_SPARK_RUNTIME_GAV`, `ICEBERG_SPARK_RUNTIME_NOTE`,
  `ICEBERG_SPARK_SCALA_BINARY`, `ICEBERG_RUNTIME_VERSION`, `_pinned_pyspark_version`,
  `_spark_major_minor`. Consumed by the MERGE differential (re-export), the MERGE record driver
  (GAV only — never from a `test_` module), and `_live_parity.build_spark_iceberg_engine`.
- `_record_merge_differential_goldens.py` — the **record driver** for the MERGE corpus (NOT a
  `test_` module; never collected). Provisions Spark with the pinned Iceberg GAV (imported from
  `_oracle_pins`, never from the test module) + a local Hadoop warehouse catalog, imports `ROWS`
  + lifecycle helpers from the committed test module, and re-derives every Spark half (content /
  error needle / split success). Exit 0 = bit-for-bit reproduce; never edits the corpus. Needs
  zulu-17 + the full parity-live sync line
  (`uv sync --locked --extra record --extra numpy --extra pandas --extra polars --extra ml-ext
  --no-install-package repark`) + network on first Ivy resolve. Invocation in its docstring and
  `task/n2b-merge-followup-ledger.md` re-derive block.
- `test_dml_subquery_parity.py` — the **DELETE/UPDATE subquery-predicate corpus** (defect
  **G3-E8**). Budget 20-32: residual **split** rows (repark refuses with the
  G3-E8 valve's own needle `subquery predicates are silently mis-executed`; the Spark half is the
  recorded post-DML table) covering UPDATE `NOT IN` with a NULL — plus **content** rows
  (2 non-subquery controls + `DELETE … IN` / `NOT IN` + NULL trap + `[NOT] EXISTS` ±
  correlation + correlated IN + identity `UPDATE … IN` including multi-column SET, scalar
  expression, and empty subquery). Recorded against live Spark 4.1.2 2026-08-14. Every row
  runs create -> seed -> create key table -> seed -> DML -> read
  back on a real Iceberg table (explicit DDL + INSERT, never CTAS) and asserts on the Arrow path
  (value AND type AND nullability) through the parity comparator. Split-path convergence
  is CLASSIFIED (CONVERGED -> flip to content equality; commit-but-mismatch -> regression), and
  the classifier is proven reachable in both arms. Residual splits stay refused. See
  `task/r1-g3e8-pr4-ledger.md`.
- `_record_dml_subquery_goldens.py` — the **record driver** for the G3-E8 corpus (NOT a `test_`
  module; never collected). Provisions Spark with the pinned Iceberg GAV + a local Hadoop
  warehouse catalog, imports `ROWS` + the lifecycle helper from the committed test module, and
  re-derives every Spark half. Exit 0 = bit-for-bit reproduce; never edits the corpus. Needs
  zulu-17 + `uv sync --extra record` + network on the first Ivy resolve. Only ONE local Spark
  driver at a time — check `pgrep -af 'pyspark|SparkSubmit'` (ignoring a standing container
  cluster) before running. Invocation in its docstring and `task/g3e8-guard-ledger.md`.
- `test_decimal128_parity.py` — the **decimal128 differential corpus** (gap G2) plus expression-
  level arithmetic overflow (gap G13), landed by G-7 (Python half), U5-updated, **R-2
  flipped**. 24 G2 rows (4 `/` rows now equality at Spark `(p,s)`) and 10 G13 rows
  (shared-raise `/0` + overflow; ANSI-OFF `/0` and overflow NULL at Spark types;
  DEC-8 `(38,20)*(38,20)` equality at `(38,6)`; DEC-9 nullability residue kept).
  Recorded against live PySpark 4.1.2. DEC-6 overflow raise is **LANDED** (checked `+`
  UDF). Budget pin: G2 20-26, G13 6-12. Ledger: `task/r2-dec-close-ledger.md`.
  **CUTOVER-SCHEMA-1 (2026-09-04):** `int_times_decimal_promotes_wider_in_repark` plus the
  three G13 nullability cells flip to equality (overflow-exposed operand casts); the two
  money CTAS rows split the post-write shape into `expected_written` (nullable — Spark's
  CTAS-optional derivation) while the SELECT half stays non-null with the recorded oracle.
  pins: cutover-schema-1/C-002, C-003
- `_record_decimal128_goldens.py` — the **record driver** for the decimal128 corpus (NOT a
  `test_` module; never collected). Imports `ROWS` / `CTAS_ROWS` from the committed test module
  and re-runs each row's own `run_row` recipe on live PySpark; raise-class rows re-check the
  exception class still matches. Exit 0 = every recorded half still reproduces; never edits the
  corpus. Needs a JVM + `pyspark` (`uv sync --extra record`); invocation in its module docstring
  and in `docs/history/hardening-h1/g7-decimal-ledger.md`.
- `test_join_parity.py` — the **joins differential corpus** (H-2 gap G4), landed by W-3, widened
  by **G4b**. 30 rows (budget 20–30): NULL join keys on every join type (inner/left/right/full —
  NULL never matches NULL) + null-safe `<=>`; duplicate-key m×n fan-out (order-insensitive);
  SQL CROSS / LEFT SEMI / LEFT ANTI content equalities; type-mismatched keys
  (int/string/decimal + malformed cast error); outer-join schema nullability flips (name-gated
  `*nullable*`); facade `sql()` primary + 9 DataFrame-API `df.join` content rows (CP-11)
  including `eqNullSafe`. Every content row asserts value AND Arrow type AND nullability on the
  `to_arrow` path via `repark_parity.assert_frames_equal` — never `show`. Budget pin +
  name-gated family coverage so a control cannot satisfy NULL-key / nullability / type-mismatch
  pins. **G4b:** the two DF `leftsemi`/`leftanti` refuse **splits** CONVERGED when the DataFrame
  semi binding landed — they are now content equalities with their recorded Spark halves
  unchanged. **Y-4 (2026-08-12):** declared rename landed; current pins are
  `df_left_semi_on_name` / `df_left_anti_on_name` (was `df_left_semi_unsupported` /
  `df_left_anti_unsupported`). Four rows joined them so the claim spans the whole
  DF surface, not one shape: `on='k'` / `on=['k']` / `left.k == right.k` (a different engine
  path — the H1 SQL rewrite) and the NULL-key edge on both semi and anti. The corpus now holds
  NO splits, so the split classifier's two arms are proven against the explicit
  `_CLASSIFIER_PROBE_SPLIT` harness row rather than a live corpus row — the machinery stays
  guarded for the next lane's disclosure. Split-path convergence is still CLASSIFIED (CONVERGED
  → flip to content equality; commit-but-mismatch → regression). **Out of scope (declared):**
  fixing divergences; registry file; windows (W-4). Ledgers: `task/w3-joins-ledger.md`,
  `task/g4b-join-widening-ledger.md`, `task/y4-rename-ledger.md` (§6 holds paste-true
  registry citation updates; registry file not edited by Y-4).
- `test_g4b_semi_join.py` — the **non-differential** half of the G4b DataFrame semi/anti
  widening: the parts with no Spark golden to compare against. Every accepted spelling
  (`semi` / `left_semi` / `leftsemi` / `LeftSemi` / `LEFT_SEMI` and the anti family — each is its
  own alias-map key, and `LeftSemi` is reachable only through the case fold), semi + anti as
  complements on one fixture, the semi result staying a usable frame (project / filter / count),
  the refusal-message contents, and the declared **conditionless divergence**: `on=None` /
  `on=[]` with a semi `how` refuses loud rather than falling through to the facade's Cartesian
  path, which would answer an m×n cross join. A guard test pins that the refusal did NOT widen
  into `how='inner'`. **G4b-R2 / Y-5:** right-parent `select`/`filter`/`withColumn` after
  semi/anti raise Spark 4.1.2 `MISSING_ATTRIBUTES` (same-name
  `RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`; distinct-name
  `RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT`); `drop(right[…])` is the probed Spark no-op;
  left refs still resolve; inner-join origin resolution is a regression guard;
  Q-001 emitting-join subtract (`semi.join(right, …, "inner")`); Q-002 `_spawn`
  descendant refuse; Q-003 self-semi exclusive-set. **Z-4 / Y-5 SAF-001:**
  `F.abs(right[…])` after semi/anti raises the same `MISSING_ATTRIBUTES` classes
  (select/filter/withColumn); left-abs and inner-abs still resolve; `_scalar`
  ride-along (`F.lower`). **W-4 / A6 Q-002:** `F.sum` / `F.count` / `F.avg` /
  `F.min` / `F.max` / `F.count_distinct` / `F.first` / `F.last` after semi raise
  the same classes (`test_right_ref_agg_*`, left / inner / distinct-name /
  `count_distinct` left-then-right). Ledger: `task/y5-origin-map-ledger.md`,
  `task/z4-residuals-ledger.md`, `task/w4-z-residuals-ledger.md`. Live-Spark
  behaviour for the conditionless divergence is recorded in
  `task/g4b-join-widening-ledger.md`.
- `_record_join_goldens.py` — the **record driver** for the joins corpus (NOT a `test_` module;
  never collected). Imports `ROWS` + lifecycle helpers from the committed test module and
  re-derives every Spark half (content / error needle / split success) under order-insensitive
  compare. Exit 0 = every recorded half still reproduces; never edits the corpus. Needs zulu-17
  + `uv sync --extra record`. Invocation in its docstring and `task/w3-joins-ledger.md`.
  Serialize with other JVM recorders via `/tmp/grok-jvm-record.lock`.
- `test_window_parity.py` — the **window-function differential corpus** (H-2 gap G5). 42 rows
  (budget 20-45; W-4's 27 plus G5b's 15-row `temporal_range` family): default-frame trap with ties (RANGE peers — name-gated `default_frame_*` ≥3),
  explicit ROWS vs RANGE / sliding / unbounded / value-offset frames, ranking family with ties
  (`rank`/`dense_rank`/`row_number`/`ntile`/`percent_rank`), lag/lead default + explicit default
  value + NULL payload, partitioned vs unpartitioned, ORDER BY NULLS FIRST/LAST, and ≥2
  DataFrame-API `Window.partitionBy` rows (CP-11). Seed via `createDataFrame` + temp view so the
  corpus measures WINDOW behaviour, not VALUES literal-type noise. 31 equalities (value+type match
  on frames/offsets/default-frame trap + temporal working path) + 11 disclosures (SQL-door ranking
  returns Arrow `uint64` vs Spark `int32`; R4 residual; R1/R5 flipped by W-4). Every row
  asserts on the Arrow path via `repark_parity.assert_frames_equal`; disclosure failures are
  CLASSIFIED CONVERGED (flip-don't-delete) vs regression. Determinism: total ORDER BY or
  peer-determined columns. Ledger: `task/w4-windows-ledger.md`.
  **G5b temporal-`RANGE` family (2026-08-11, appended — no W-4 row edited):** 15 rows, family
  `temporal_range`, name-gated in the budget test. 12 equalities pin the interval-bounded path
  over datetime order keys — ascending, descending, ties (zero-width interval == peer group),
  NULL order keys, `DATE` key, partitioned, centred (both bounds intervals), `HOUR`≠`DAY`, the
  G5b evidence row (a unit-less offset over a `DATE` key means **days**, not Arrow's months),
  and Y-1's three flips. **Y-1 (2026-08-12) flipped three residual rows to equality:** `DAY TO SECOND` (R2)
  and both negative-offset rows (R3 `sum` / `count(*)`). **Half-B** pins same-kind magnitude
  invert as a shared refuse (`test_temporal_range_negative_both_preceding_refuses_like_spark`
  — wrapping `-1` is gone on the Spark door after this fix; ANSI still wraps — named residual).
  Three disclosures remain: unquoted `INTERVAL 1 DAY` (R1, deferred — needs `spark_ast.rs`),
  both-bounds-`FOLLOWING` off-by-one (R4, 120 vs Spark's 90), and an interval bound over a
  numeric key (R5, raw Arrow cast). Family disclosure floor is now ≥3. Module-level
  tests: `test_temporal_range_bare_offset_over_timestamp_refuses`,
  `test_temporal_range_bare_offset_over_date_key_is_days_not_months`,
  `test_temporal_range_negative_both_preceding_refuses_like_spark` (Q-001), and
  `test_temporal_range_mixed_negative_timestamp_and_numeric_bare_refuses` (Q-003). Entry point
  is SQL only: `Window.rangeBetween` takes numeric offsets in PySpark and in the facade, so a
  temporal frame is unreachable from the DataFrame API in either engine. Engine half:
  `crates/repark-spark/src/window_range.rs`; Spark-door pins:
  `crates/repark-spark/src/tests/window_temporal_range.rs`. Ledgers:
  `task/g5b-temporal-range-ledger.md`, `task/g5br-range-residuals-ledger.md`.
- `_record_window_goldens.py` — the **record driver** for the window corpus (NOT a `test_` module;
  never collected). Imports `ROWS` + `run_row` from the committed test module; re-derives every
  Spark half on live PySpark 4.1.2 (`local[2]`, ANSI on, shuffle=2). `--emit` prints paste-ready
  `_table(...)` snippets. Exit 0 = bit-for-bit reproduce; never edits the corpus. Needs zulu-17 +
  `uv sync --extra record`. Invocation in its docstring and `task/w4-windows-ledger.md`.
- `test_nested_container_parity.py` — the **nested-container differential corpus** (H-2 gap G18),
  unlocked by the nested order-insensitive comparator. 6 rows (budget 4-6): struct + map
  createDataFrame equalities (value AND type AND nullability), SQL-door struct select equality,
  and TYPE disclosures for array / collect_list / array-of-struct (`list<item:…>` vs Spark
  `list<element:…>` [not null]). Outer rows order-insensitive (G18 enabler). Budget pin +
  CONVERGED/regression classifier. Does **not** edit the registry. Ledger:
  `task/x5-nested-comparator-ledger.md`.
- `_record_nested_container_goldens.py` — the **record driver** for the nested corpus (NOT a
  `test_` module; never collected). Imports `ROWS` + `run_row`; re-derives every Spark half;
  `--emit` pastes Spark + divergent repark halves. Hold `/tmp/grok-jvm-record.lock`.
- `test_boundary_shapes_parity.py` — **Y-6 / H-2 gap G10** facade-boundary container-shape
  corpus (sibling of `test_interchange_parity.py`; does **not** duplicate X-5
  `test_nested_container_parity.py` VALUES families). 10 rows (budget 8–10). Coverage
  floors are **semantics-gated**: typed-Map (recipe `out_map` / Arrow map, plus
  `map_topandas_*` disclosure — a `map_` prefix on a struct-inference equality does not
  count); `*struct_*` both directions; `*binary_*` both directions; `*array_*` ≥2 **and**
  the item-vs-element ingest disclosure; `*pandas_timestamp_unit_*` **and** the inbound
  us disclosure (ns equality cannot satisfy); inbound glob `*_from_pandas_*` matches
  every inbound row. Value AND dtype/shape AND (Arrow surface) nullability. 6 equalities
  (binary bytes both directions, array ndarray cells, ArrowDtype list field name, inbound
  object-dict→struct, inbound datetime64[ns]) and 4 disclosures (map toPandas dict vs
  list-of-pairs; struct Long mixed `10`/`20.0` vs int 20; inbound object-list `element`
  vs `item`; inbound datetime64[us] → pandas ns vs us). Map pair order is
  order-insensitive (X-5 key-sort). CONVERGED/regression classifier arms committed.
  Census cohorts NOT extended (A11). Ledger: `task/y6-boundary-shapes-ledger.md`.
- `_record_boundary_shapes_goldens.py` — the **record driver** for the G10 corpus (NOT a
  `test_` module; never collected). Imports `ROWS` + `run_row`; re-derives every Spark half
  on live PySpark 4.1.2 (`local[2]`, ANSI on, shuffle=2, `session.timeZone=UTC`,
  `arrow.pyspark.enabled=true`). Pyspark coordinate derived from the project's `record`
  extra (CP-8). `--emit` pastes Spark + divergent repark halves. Hold
  `/tmp/grok-jvm-record.lock`.
- `test_cast_failure_parity.py` — the **cast-failure semantics differential corpus** (H-2 gap G6),
  landed by X-1. 15 rows (budget 8–15) recorded against live PySpark 4.1.2 ANSI ON (`local[2]`,
  shuffle=2, `session.timeZone=UTC`): 10 shared-raise **error** equalities (malformed string→int /
  date, INT→TINYINT overflow, decimal narrowing overflow, DF `Column.cast` twin, and the five
  DATE↔INT doors below), 2 **try_cast** NULL equalities (twins of the failing casts), 1
  well-formed control equality, **no remaining split**
  and **1 nullability-only content disclosure** (TIMESTAMP→INT — was a repark-raises split until
  the TZ-5 cast unit un-refused it, 2026-08-12: value/type now match Spark's unix-seconds int32;
  repark propagates the literal's non-null where Spark types the CAST nullable; content-disclosure
  classifier arms proven on this row, split arms kept proven via a synthetic exemplar).
  **G6-3 / G6-5 (2026-08-15):** the corpus's last `split` flipped —
  `date_to_int_spark_refuses_repark_days` is a shared-raise **error** equality now, both engines
  raising `DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION`. Four doors the design measured as
  unpinned divergences were recorded in the same diff (`date_to_bigint_both_refuse`,
  `try_cast_date_to_int_both_refuse`, `df_cast_date_to_int_both_refuse`,
  `df_try_cast_date_to_int_both_refuse`) plus the reverse `int_to_date_both_refuse` (G6-5), which
  is why `G6_BUDGET_MAX` ratcheted 10 → 15 with its reason on the constant. The spark-raises split
  classifier arm keeps its exemplar the way the repark-raises arm did: a synthetic `CastRow` that
  never joins `ROWS`. The live-tier disclosure `cast_date_to_int_spark_refuses` retired with the
  row (`_live_parity.py`, the `test_parity_live.py` exact set, and the registry §6 heading all move
  in the same diff or the mirror gate reds in both directions).
  **Y-4 (2026-08-12):** current pin name `timestamp_to_int_nullability` (was
  `timestamp_to_int_spark_seconds_repark_raises`; live-mirror token
  `cast_timestamp_to_int_nullability` is unchanged).
  §0 re-verified that the slate
  "non-ANSI NULL" premise narrowed under ANSI ON — fewer than 4 real divergences, not manufactured.
  Every content row asserts value AND Arrow type AND nullability via
  `repark_parity.assert_frames_equal`; error rows pin raise class + error needle (A7). Split
  classifiers (CONVERGED vs regression) proven by monkeypatch (CP-1). **Does not edit**
  `_live_parity.py` / live size pins / registry (A3 — §6 paste-true BOTH halves in the ledger).
  Ledgers: `task/x1-cast-failure-ledger.md`, `task/y4-rename-ledger.md` (Y-4 name only).
- `_record_cast_failure_goldens.py` — the **record driver** for the cast-failure corpus (NOT a
  `test_` module; never collected). Imports `ROWS` + lifecycle helpers from the committed test
  module; re-derives every Spark half (content / error needle / split success-or-raise) under
  ANSI ON + UTC. Exit 0 = every recorded half still reproduces; never edits the corpus. Needs
  zulu-17 + `uv sync --extra record`. Invocation in its docstring and
  `task/x1-cast-failure-ledger.md`. Serialize via `/tmp/grok-jvm-record.lock`.
- `test_three_valued_logic_parity.py` — the **three-valued logic differential corpus** (H-2 gap
  G12), landed by X-2. 12 rows (budget 10–12): six load-bearing AND/OR/NOT truth-table combos
  (name-gated `and_*`/`or_*`/`not_*`); `NULL = NULL` vs `NULL <=> NULL`; `IS [NOT] NULL` vs
  `= NULL`; `CASE WHEN <null-predicate>`; one SELECT-level `IN (…, NULL)` (DML NOT-IN family is
  **PR #54 in flight** / G3-E8 — not duplicated); facade `sql()` primary + ≥2 DataFrame-API
  `eqNullSafe` / `&|~` rows (CP-11, name-gated `df_*`). 10 equalities + 2 disclosures (null-safe
  equal result nullability: Spark non-null bool vs repark nullable bool — value agrees). Every
  content row asserts value AND Arrow type AND nullability on the `to_arrow` path; disclosure
  failures CLASSIFIED CONVERGED vs regression; both classifier arms proven by monkeypatch.
  Ledger: `task/x2-tvl-ledger.md` (§6 paste-true registry rows; registry file not edited).
- `_record_tvl_goldens.py` — the **record driver** for the TVL corpus (NOT a `test_` module; never
  collected). Imports `ROWS` + lifecycle helpers from the committed test module; re-derives every
  Spark half under the parity comparator; `--emit` prints paste-ready `_table`/`_one_row`
  snippets. Exit 0 = bit-for-bit reproduce; never edits the corpus. Needs zulu-17 +
  `uv sync --extra record`. Serialize via `/tmp/grok-jvm-record.lock`. Invocation in its docstring
  and `task/x2-tvl-ledger.md`.
- `test_float_agg_parity.py` — the **float aggregation differential corpus** (H-2 gap G7), landed
  by X-3. Exactly 2 rows (budget 2): `sum` / `avg` of the catastrophic-cancellation fixture
  (large ±1e16 interleaved with small addends — same VALUES as the Rust pins in
  `crates/repark-spark/src/tests/float_agg.rs`). Both rows are **disclosures**: Spark lands 2.25 /
  0.28125; repark lands 3.75 / 0.46875 (float64 nullable both sides). Recorded under
  `local[2]` / shuffle=2 / ANSI on; repark uses `spark.sql.shuffle.partitions=2` (→
  `target_partitions`). Arrow-path asserts via `repark_parity.assert_frames_equal`; disclosure
  failures CLASSIFIED CONVERGED vs regression. In-module ULP-tolerance path exists but is unused
  (distance is not last-ulp). Live-tier DISCLOSURE handoff is §6 paste-true only (lane never
  edits `_live_parity.py` — conductor A4). Ledger: `task/x3-float-agg-ledger.md`.
- `_record_float_agg_goldens.py` — the **record driver** for the float-agg corpus (NOT a `test_`
  module; never collected). Imports `ROWS` + `run_row` from the committed test module; re-derives
  every Spark half on live PySpark 4.1.2. Exit 0 = bit-for-bit reproduce; never edits the corpus.
  Needs zulu-17 + `uv sync --extra record`. Hold `/tmp/grok-jvm-record.lock` (conductor B4).
- `_live_parity.py` — the **live oracle tier** shared registry (NOT a `test_` module — a helper,
  never collected). Two recipe kinds:
  1. **Single-shot** (`Scenario` / `SCENARIOS`, **42** goldens): Group E group-agg/na/union +
     columns + dates + the two Group L-write division goldens `division_union` / `division_bare`
     + the two audit-G2 filter-rewriter goldens + the two H-1a non-UTC-oracle DATE controls + the
     **13 G1/G16 extraction-class timezone live rows** (N-2b item 3; size pin 29 → 42). Because
     repark is a near-drop-in for PySpark, ONE recipe runs on both engines (`Engine` abstraction).
  2. **Lifecycle** (`LifecycleScenario` / `LIFECYCLE_SCENARIOS`, **2** MERGE rows — N-2b item 2):
     multi-statement `create → seed → [merge_src] → act → read` with always-cleanup.
     `live_merge_basic_upsert` (control equality) + `live_merge_matched_arm_order` (arm-order
     first-match-wins — not the builder upsert twin). `build_spark_iceberg_engine` is a sibling of
     `build_spark_engine` (option A): GAV from `_oracle_pins`, Hadoop catalog, ANSI on. The
     default live session still has **no Iceberg catalog**; under `REPARK_PARITY_LIVE=1` this
     module arms `PYSPARK_SUBMIT_ARGS` with the GAV so the process's first SparkContext can
     resolve `SparkCatalog` for later lifecycle tests (L-1). repark path: `build_repark_engine` +
     `register_memory_catalog` + `with_cow_props=True`.
  `DISCLOSURES` = the load-bearing recorded divergences (exact-set pin in
  `test_parity_live.py`; 14 names after the 2026-08-12 L-1 landing-truth sweep — the original
  four plus G6/G12/G7/G18/G4b live-mirrors). `live_enabled()` is the `REPARK_PARITY_LIVE` gate;
  `build_spark_engine()` / `build_spark_iceberg_engine()` import pyspark **lazily**.
  **Per-scenario session-conf override (H-1a):** `Scenario.session_conf` (and lifecycle) carries
  conf pairs for one scenario only — oracle via `spark_session_conf`, repark via BUILD.
- `test_parity_live.py` — the **live oracle tier** (L1) + its flag detector (L6a). Routine (every
  PR, JVM-free): `test_scenario_recipe_matches_golden_on_repark` +
  `test_lifecycle_scenario_matches_golden_on_repark` run each recipe on repark and assert
  `repark == golden`. Live (`REPARK_PARITY_LIVE=1`, `parity-live.yml` / `make parity-live`): one
  shared session-scoped SparkSession + a separate session-scoped Iceberg-provisioned engine for
  lifecycle rows; `test_live_scenario_matches_repark_golden_and_spark` /
  `test_live_lifecycle_scenario_matches_repark_golden_and_spark` re-derive each golden from live
  Spark and assert **repark == pinned golden == live Spark**;
  `test_live_disclosure_still_diverges` re-asserts each recorded divergence STILL holds on both
  engines (silent convergence → RED).
  **B-MOR-3:** `test_live_rewrite_position_delete_files_upgraded_parquet_matches_spark` (takes the shared `spark_iceberg_engine` fixture; the three catalog keys are set for the leg and unset in `finally`) is the
  cell-B live co-collected leg (five upgraded parquet deletes → five PUFFIN, catalog `bmor3live`).
  pins: b-mor-3-rewrite-position-deletes-v3/C-001, C-003
  **RP-11:** `test_live_rewrite_position_delete_files_below_floor_matches_spark` is the
  cell-B2 live co-collected leg (two upgraded parquet deletes stay parquet, four zeros,
  catalog `bmor3floor`).
  pins: rp-11-repin-f24/C-002
  **LOG1P-1:** `test_live_log1p_expm1_tiny_args_and_domain` uses the shared `spark_engine`
  (one `SELECT` of nine aliases, no `stop`, no per-cell Ivy).
  pins: log1p-1-precise-kernels/C-001, C-004
  **FN-FIX-1:** four live legs on the same `spark_engine` —
  `test_live_fn_fix_1_isnan_sha2_try_to_number_add_months`,
  `test_live_fn_fix_1_last_and_approx_percentile`, `test_live_fn_fix_1_arrays`,
  `test_live_fn_fix_1_nan_ingest`.
  pins: fn-fix-1-registry-rows/C-001, C-002, C-003, C-004
  **FN-FIX-2:** `test_live_fn_fix_2_strings` and `test_live_fn_fix_2_regex_like` on the
  same `spark_engine`; `test_live_disclosure_still_diverges` still collected.
  pins: fn-fix-2-string-rows/C-001, C-003, C-004
  **FN-FIX-2-CTRL-1:** the same two legs co-collect the control cells (initcap/chr/trim
  controls, ANSI-off elt, ANSI-off LIKE, `[[:alpha:]x]` bracket, `regexp_extract` Spark
  oracle); ANSI conf applied via reversible `lp.spark_session_conf`. Round 3: NULL
  `ltrim` / `rtrim` cells and the `RLIKE`-keyword Spark oracle cells.
  pins: fn-fix-2-ctrl-1-controls/C-002
  **FN-REGEXP-EXTRACT-1:** `test_live_fn_regexp_extract` on the same `spark_engine`
  (oracle row plus repark co-collect on both doors; round 2: ANSI-off cells via
  `lp.spark_session_conf` — non-match `''` triple plus matching-input error cells).
  pins: fn-regexp-extract-1/C-002, C-004
  **PERF-DYNFLATTEN-1:** `test_live_dynflatten_matches_spark_explode` co-collects
  beside `test_live_disclosure_still_diverges` on the shared `spark_engine`
  (struct_d3 / list_struct_1 / cartesian_two_lists at 16 rows).
  pins: perf-dynflatten-1-measure/C-002, C-003
  Size pin `test_registry_covers_the_mandated_golden_family`
  is **42** (was 29); lifecycle budget pin is **2**. Flag unset → every live test SKIPs with a
  visible reason. Catches golden drift + oracle drift the JVM-free suite cannot see.
  **The registry mirror (H-1d, 2026-08-10):** `test_disclosures_mirror_the_registry` is always-on
  (JVM-free) and checks `_live_parity.DISCLOSURES` against the divergence registry
  `docs/spark-sql-iceberg-parity.md` in **both** directions — a registry row that opts in with a
  `` `live-mirror: <name>` `` bullet must have a `Disclosure` of that name, and every `Disclosure`
  must be claimed by a row. The registry is the SSOT for divergence *semantics*; this list is the
  checked mirror (registry §6). A RED means one side moved without the other; fix the wrong side,
  never the assertion. **Fail-closed on a near-miss:** any `-` bullet mentioning `live-mirror` that
  does not match the exact spelling reds loud naming the line, rather than reading as zero matches
  — otherwise a row could advertise a drift detector nobody checks. Registry §6 documents that
  spelling for row authors; the regex and the doc move together.
  FN-REGEXP-EXTRACT-1 (2026-09-04): `test_live_fn_regexp_extract` co-collects with the printSchema and
  FN-FIX-2 legs after the merge of main; it keeps the `skipif(not lp.LIVE)` guard like every live leg.
- `test_ta_volume.py` — **TA-4 (2026-08-15):** volume-family facade (`ad`/`adosc`/`obv`/`mfi`)
  through the DataFrame door. The 5000-row OHLC + `fixture_volume` golden is written to Parquet
  and `read_parquet`-ed; each indicator `.over(orderBy(ts))` is `to_bits`-identical to the
  TA-3-recorded C-TA-Lib golden. Call-site Column-returns + polars_talib keyword spellings
  (`fastperiod`/`slowperiod`/`timeperiod`). Does **not** edit `test_ta.py`.
- `test_ta.py` — T1b + T2 batches 1–2 + WG2–WG5 + T3 (+ the 2026-08-17 `__all__`-completeness
  pin: every public `ta` def must be exported — closes the silent-`wma`-omission class): the
  `repark.ta` DataFrame route. The 5000-row
  OHLC golden fixture (`crates/repark-ta/tests/goldens/*.bin`, columns
  `ts`/`open`/`high`/`low`/`close`/`periods`)
  is written to Parquet and `read_parquet`-ed, then each indicator (`ta.ema(...).over(...)`, plus
  `sma`/`rsi`/`adx` multi-input/`stddev` nbdev/`correl` two-series/`linearreg_angle`/`min`/`max`/
  `sum`, the WG1 overlap-MA family `wma`/`dema`/`tema`/`trima` odd+even/`kama`/`t3` two vfactors/
  `midpoint`/`midprice` two-series/the three split `bbands`, the WG2 batch — the ROC family,
  `willr`/`cci`/`cmo`, `bop` four-series, `apo`/`ppo` at matype 0 **and 7**, `ma`/`macdext` matype 7,
  split `aroon_down`/`aroon_up` +
  `aroonosc`, `trix`, `ultosc`, and the WG3 directional family `dx`/`adxr`/`plus_di`/`minus_di`/
  `plus_dm`/`minus_dm`, the split `macd`/`macdfix`/`macdext` outputs, the `ma` selector at
  matype 0/1, the WG4 split stochastics `stoch_slowk`/`_slowd`/`stochf_fastk`/`_fastd`/
  `stochrsi_fastk`/`_fastd` **and matype-7 all 8 golden bins** via the kwargs that route MAMA
  (`stoch_type7_{slowk,slowd}` all-MAMA on both facades; `stoch_mixed_7_0_{slowk,slowd}`;
  `stochf_type7_{fastk,fastd}` with `fastd_matype=7`; `stochrsi_type7_{fastk,fastd}` with
  `fastd_matype=7`), the WG5 sweep-up `natr`/`beta` + the O/H/L/C price transforms
  `avgprice`/`medprice`/`typprice`/`wclprice`, and the T3 parked four — the split `mama`/`fama`,
  `sar`/`sarext` H/L, and `mavp` over the `periods` column at matype 0 + 1) is asserted
  `to_bits`-identical to the recorded C-TA-Lib golden = the kernel output (C TA-Lib is the parity
  oracle here; no goldens re-recorded). Plus call-site checks (Column-returns, string-vs-Column-form
  agree, `MIN`/`MAX`/`SUM` alias identity, unknown-name error).
  **G-NAN:** `null_lookback` opt-in pins — default `False` (and omitted kwarg) keeps lookback
  prefix as **valid kernel NaN** slots (`is_valid`, `isnan`, `null_count==0`) *and* bit-exact
  vs goldens (C5-Q-001: Arrow path, not numpy-only — so always-wrap cannot hide as NaN↔NaN);
  with `True`, ema/rsi/bbands_upper null-prefix matches the polars_talib null pattern (lookback
  lengths 20/14/19) and dense suffix stays bit-exact; mid-series NaN (injected past EMA
  lookback) stays a valid NaN slot (never SQL NULL); keyword-only enforced via TypeError.
  **r21 T4 ta-etl:** `over_columns` type guards; `withColumns(over_columns(...))` → one
  `WindowAggExec` + Arrow bit-exact vs sequential `withColumn`; **r23b N2:** sequential same-spec
  independent `withColumn` also merges to one `WindowAggExec` (was N-stack anti-pattern pin).
- `test_ta_with_indicators.py` — **conductor-13 TA-2:** `ta.with_indicators` serving helper.
  Arrow value+type vs hand-built `over_columns`; required keyword-only `partition`/`order`
  (TypeError on omit; empty partition refuses); cross-symbol RSI leak vs unpartitioned
  `orderBy("ts")`; `last_row` row-count + last-bar values; one `WindowAggExec` via the N2
  mechanic (function-name tokens so DCE cannot fake fusion); `null_lookback` threads through
  `_NullLookbackColumn`. Does **not** edit `test_ta.py` (A12). Ledger:
  `task/ta2-with-indicators-ledger.md`.
- `test_n2_plan_collapse.py` — **r23b N2** plan-collapse pins: stage (a) logical alias-chain squash
  (no `ts AS ts AS ts`); stage (b) adjacent same-spec withColumns/withColumn merge → 1
  `WindowAggExec` + Arrow bit-exact vs single fused call; dependent `tr`→`etr5` keeps stacking;
  filter / drop / select-subset / cache intervene (Q15 + octo C2 cache mark); `.round(4)` /
  `.alias` same-layer wrap merges; overwrite-base-name then re-read stacks; different WindowSpec
  no-merge; operator 4-chain → 1. Synthetic OHLCV only. **octo C1:** drop/select/alias/overwrite
  pins. **octo C2:** cache blocks merge.
  **r25 T3:** double-`.alias` repeated-alias peel (`… AS name AS name` absent); rename double-alias
  → single; distinct rename chain `alias("a").alias("b")` → `… AS b` (octo C1-Q-006); operator-shaped
  17-TA chain ≤1 `WindowAggExec` + Arrow to_bits parity vs fused `withColumns`.
- `_acceptance.py` — WG4 shared helpers for the real-AWS acceptance harness (NOT a `test_` module,
  so pytest never collects it): the source-publish-job-shaped constants (bronze bucket/prefix,
  `glue_catalog`, `s3://` warehouse, scratch namespace `testing_repark_acceptance`, the real
  `format-version 2` + copy-on-write + target-file-size `TBLPROPERTIES`) and pure builders
  (`bronze_path` s3a, `glue_catalog_config`, `fq_table`, `ctas_sql`, `merge_sql`,
  `acceptance_namespace_location` = `<warehouse>/<namespace>` — the ADV-1 programmatic namespace
  location) + the `deduplicate` row_number transform. **G-6 location-mismatch guard (Glue):**
  `normalize_location_uri`, `assert_namespace_location_matches` (exact equality after trailing-slash
  strip; match / mismatch / no-location all fail-loud with both values + the operator fix),
  `location_from_describe_rows`, `probe_namespace_location_via_describe` (DESCRIBE-row
  unit helper; retired as the live path), and `assert_glue_scratch_namespace_location`
  (**Y-3:** reads `spark.catalog.getDatabase(…).locationUri`). **A2 second bullet:** `s3tables_catalog_config`
  (S3TablesCatalog impl, ARN as `warehouse` → RePark's `table_bucket_arn`) + the non-secret
  `S3TABLES_CATALOG` name — the table-bucket ARN is a RUNTIME arg from `TABLE_BUCKET_ARN`, never a
  committed literal. **MW-4:** `MOR_ICEBERG_TABLE_PROPERTIES`, `mor_ctas_sql`,
  `run_mor_merge_compact_expire` / `assert_mor_maintenance_outcome` /
  `require_snapshot_readable` / `require_snapshot_expired` (shared by the memory analog
  and the Glue live leg). **RP-1:** `MOR_MIN_POSITION_DELETE_FILES = 5` (F-1 floor);
  five MERGEs plus the idempotent replay; expected-row pin follows `MOR_UPDATED_ID_COUNT`.
  **MW-5:** `require_snapshot_readable` takes `expected_rows`
  (default `MOR_SEED_ROW_COUNT`; the 1,000-row demo passes 1000).
  **MW-10:** `retry_on_commit_conflict` (default 3; `CatalogCommitConflicts` /
  `CommitFailed` requirement mismatch / `validate_data_files_exist`); MERGE and each
  maintenance CALL are wrapped; `MorMaintenanceOutcome` records `retry_count`,
  `max_call_retries`, `service_commits` (union of both expire logs minus engine ids),
  snapshot log before/after expire, whether current after expire matches the engine, and
  `ambiguous_engine_windows`. Denial signatures win over conflict signatures.
  `assert_retry_counts` caps per-call retries, not the sum. `assert_engine_expire_removed_ctas`
  requires the CTAS id in the before-expire log. (pins: mw-10-s3tables-mor/C-001, C-003, C-004).
- `_acceptance_v3.py` — **LIVE-v3 (2026-09-02):** the format-version-3 leg body, split out of
  `_acceptance.py` so both stay under the CAP-1 1,000-line ceiling; also a non-`test_` module.
  `V3_ICEBERG_TABLE_PROPERTIES` (`format-version 3` + merge-on-read delete/update/merge),
  `v3_seed_select_sql` (one CTAS row) + `v3_insert_batches` (single-row appends, so each identity
  `part` reaches the five-file `min-input-files` floor and `_row_id` assignment stays sequential —
  a two-row CTAS and two-row appends both shuffled survivor ids between runs), `v3_ctas_sql`
  (`PARTITIONED BY (part)`, no IF NOT EXISTS), `v3_row_delete_sql`, `v3_merge_source_sql`,
  `delete_file_rows`, `v3_lineage_rows`, `v3_ordered_rows`, `v3_data_files_per_partition`
  (arms `V3_FILES_PER_PARTITION`: the appends must land 5+5, exactly AT Spark's
  `min-input-files` floor, or step 6 is a no-op — asserted, not assumed),
  `v3_rows_and_lineage` (ONE ordered scan for the post-MERGE and post-rewrite pairs, which used
  to open the same snapshot twice: 45 → 22 object opens per pair, 178.8 → 92.4 ms and
  168.6 → 70.3 ms, pinned values unchanged), `current_metadata_location`
  (`metadata_log_entries` tail = the `register_table` argument), `run_v3_acceptance`, and
  `assert_v3_acceptance_outcome` / `assert_v3_lineage` / `assert_v3_row_ids_are_stable` /
  `assert_deletion_vectors`. `v3_row_delete_sql` is the ONLY `DELETE FROM` in the harness and is
  always single-key: the DV step needs a row delete and never-teardown bans everything else.
  Measured local answers, 2026-09-02: a DV is `content = 1` + `file_format = 'PUFFIN'` (not
  content 2); 1 DV after DELETE, 2 after the MoR MERGE, `rewrite_data_files` 12 rewritten /
  2 added / 2 removed leaving 0 DVs, 14 snapshots before expire and 1 after. Survivor `_row_id`
  is exact and stable across MERGE and rewrite. **V3-11 (2026-09-02):** the NOT MATCHED
  insert's `_row_id` is deterministic now — `V3_EXPECTED_INSERTED_ROW_ID = 11`, Spark's value,
  replaces the fresh-unused-id invariant the flapping forced (`V3-ROWID-3` FIXED). With the
  commit's file ordering removed the leg fails 2 of 6 runs; with it, 5 of 5 green.
  Pairing the appends into five commits would cut ~46 % of the wall time and was DECLINED: it
  puts two partitions in one commit, the shape that made survivor `_row_id` order-dependent.
  `exact_commit_counts=False` relaxes sequence and snapshot counts for S3 Tables' own commits and
  nothing else. S3 Tables decision table: `classify_v3_create_outcome` /
  `is_format_version_3_refusal` / `format_v3_refusal_record` (`S3T-V3-1`; the engine's own opt-in
  message never classifies). pins: live-v3-aws-legs/C-001
- `test_acceptance_v3_helpers.py` — **LIVE-v3 (2026-09-02):** AWS-free structural pins for
  `_acceptance_v3` and the two live legs. The never-teardown guard over that module (no DROP,
  exactly one `DELETE FROM`, AST-pinned inside `v3_row_delete_sql` with its `WHERE`);
  `register_table` reachable only through the optional `adopt_with` factory; and
  `test_v3_legs_are_twins_of_the_mor_legs` — helper + asserter called, `table_name` built from
  `ACCEPTANCE_TABLE_PREFIX` + a `v3_dv_` stem + `uuid4` read off the AST (an earlier
  source-substring form was satisfied by a docstring token — the vacuity this file exists to
  avoid), `V3_ALLOW_CREATE_KEY` passed to `.config`, Glue runs the location guard and
  `adopt_with`, S3 Tables runs neither and passes `exact_commit_counts=False`, its
  `create_namespace` takes no `location`, and the denial path is
  `pytest.fail(format_denial_failure(...))`. pins: live-v3-aws-legs/C-003
- `test_acceptance_helpers.py` — WG4 AWS-free unit tests for `_acceptance` that run **everywhere**
  (no gate): the builder outputs (s3a bronze path, the measured glue config block, CTAS/MERGE SQL
  shape keyed on the id column, the real TBLPROPERTIES block, and `acceptance_namespace_location`
  under the Glue warehouse without doubling a trailing slash — ADV-1), the scratch-≠-production
  namespace guard, a structural guard that `test_aws_acceptance.py` carries no `DROP TABLE`/`DELETE
  FROM`/`DROP NAMESPACE`, the `deduplicate` transform against a memory session (newest row per id),
  and the G-6 location-mismatch guard's pure comparison edges (match, mismatch naming both values,
  no-location, DESCRIBE-row extraction). **Y-3:** Glue wrapper stub drives `getDatabase`;
  AST pin that the wrapper calls `getDatabase` (not DESCRIBE). **MW-4 (2026-08-23):** MOR
  TBLPROPERTIES is merge-on-read not copy-on-write (COW block unchanged), CALL SQL shape, expected
  row oracle, always-run memory analog of `run_mor_merge_compact_expire`, DROP/DELETE scan of
  `_acceptance.py`, Glue MOR AST pin (location guard; `table_name` is `mw4_mor_` plus
  `uuid4`, not a docstring token), S3 Tables must not call the MOR helper, identical-MERGE
  source pin `[updates[-1]]`, dual-probe AST (`require_snapshot_readable` in the runner,
  `require_snapshot_expired(outcome.first_snapshot_id)` in the asserter), live test must
  call the asserter with `table_name` as the helper's fourth argument. Fake-session pins:
  id-echo / generic `snapshot` AnalysisException is not expire; the engine needle is;
  a successful VERSION AS OF is expire-no-op.
  **MW-10:** memory-analog retry pins — conflict twice then success (`retry_count == 2`);
  exhausted budget raises after exactly `attempts` with the count in the message; a
  non-conflict error is re-raised on the first call; each named conflict signature
  retries. Per-call budget: sum 4 with max 2 passes; max 4 fails. Service-commit union
  includes a pre-expire id that vanishes; a two-id engine window is ambiguous.
  Injection pin: first MERGE and first CALL each conflict once → `retry_count == 2`,
  `max_call_retries == 1`. Denial wins over `CommitFailed`+requirement. CTAS missing
  from the before-expire log names automatic expiry. AST: `retry_on_commit_conflict`
  inside MERGE and the runner; `create_namespace` has no `location` keyword; denial
  path is `pytest.fail(format_denial_failure(...))` (pins: mw-10-s3tables-mor/C-001,
  C-002, C-003, C-004).
- `test_aws_acceptance.py` — WG4 the env-gated real-AWS acceptance harness: a **module-level** **S6 leak guard (2026-09-04):** the "not leaked into `cut`" half reads the leak namespace through `_leaked_table_is_reachable`, which treats the CI role's Glue `AccessDenied` on `database/cut` as the stronger proof (the role cannot reach that database at all, so nothing could have been created there); run 33916856419 failed on the raw `tableExists` call.
  `pytest.mark.skipif` on `REPARK_AWS_ACCEPTANCE != "1"` skips the whole module by default (CI
  stays AWS-free; the single sanctioned real-AWS run is the Fable audit's). Gated in, it mirrors
  the source publish job: a Glue-`catalog-impl` session via `.config(...)`, bronze `s3a://` read
  (entity/ds/id-col from `REPARK_ACCEPT_ENTITY`/`_DS`/`_ID_COL`), the dedup transform, then
  namespace-create (programmatic `spark.create_namespace(..., location=…)` — ADV-1, since SQL
  `CREATE NAMESPACE` without `LOCATION` would omit the `location` a real Glue catalog requires (SQL `LOCATION` works too since WG-5); idempotent on an
  "already exists") → **G-6: `assert_glue_scratch_namespace_location`** (`getDatabase.locationUri`
  + exact match to the intended warehouse path; fail loud on stale LocationUri) →
  `tableExists`→CTAS-or-MERGE → idempotent second MERGE into `testing_repark_acceptance`.
  Oracles: bronze rows > 0, published == deduped (fresh CTAS), second pass count unchanged.
  No DROP TABLE / DROP NAMESPACE / DELETE FROM — tables accumulate; MW-4 CALL expire may
  remove expired snapshot files under the scratch prefix. **A2 second bullet
  (`test_process_silver_acceptance_against_s3tables`):** the same shared publish path
  (`_bronze_dedup_publish_idempotent`) against an **S3 Tables** catalog — additionally gated on
  `TABLE_BUCKET_ARN` (SKIP, not fail, when absent, so a Glue-only run is unaffected); ARN read from
  env, never logged; namespace created WITHOUT a `location` (S3 Tables namespaces carry no location
  by design — nothing to compare; the Glue location-mismatch guard is intentionally not called).
  Runbook: `REPARK_AWS_ACCEPTANCE=1 TABLE_BUCKET_ARN=<us-east-2 ARN> AWS_REGION=us-east-2`.
  **MW-4 (2026-08-23):** `test_mor_merge_compact_expire_against_glue` — a unique
  `testing_mw4_mor_*` table per run; CTAS merge-on-read → MERGEs that strand position-delete
  files → identical MERGE → `rewrite_position_delete_files` + `rewrite_data_files` +
  `expire_snapshots` → Arrow row parity and VERSION AS OF of the CTAS snapshot fails.
  **MW-10:** `test_mor_merge_compact_expire_against_s3tables` — the Glue leg's twin against
  `S3TABLES_CATALOG` (namespace without `location`; skip when `TABLE_BUCKET_ARN` is absent;
  `testing_mw10_mor_*`; dual probe via the shared helper; a table-storage denial fails
  loud with action, resource, and masked account). No DROP TABLE. The first owner dispatch
  (2026-08-30, run 33333274383) ran it green — the measured **allow** filled the tier2-aws,
  north-star, and guide slots and no denial registry row was filed
  (pins: mw-10-s3tables-mor/C-005); that green dispatch on merged `main`, with preflight and
  the parity harness, is the unit's whole-surface gate (pins: mw-10-s3tables-mor/C-006).
  **LIVE-v3 (2026-09-02):** `test_v3_dv_dml_maintenance_against_glue` and
  `test_v3_dv_dml_maintenance_against_s3tables` — twins of the MW-4 / MW-10 legs over
  `run_v3_acceptance`, `testing_v3_dv_<uuid4>` per run, `repark.sql.allowCreateFormatVersion3`
  on the builder, no DROP path. Glue also adopts the final metadata location on
  `spark.newSession()`; S3 Tables does not, because `register_table` there is the dated gap
  `S3T-1` / fork R126. S3 Tables decision table: supported → the full leg with
  `exact_commit_counts=False`; a classified `format-version 3` CREATE refusal → the leg asserts
  no table was left behind, records the masked refusal text as `S3T-V3-1` through
  `warnings.warn`, and passes; anything else → raised (a storage-delete denial still fails loud
  first). Neither leg has run yet — the first measurement is the nightly or a dispatch on merged
  `main` (pins: live-v3-aws-legs/C-003).

- `test_two_door_kernel_parity.py` — **FNP-1 (2026-08-20):** charter clause C-012 at the facade
  layer. Pins that a name reachable from both doors returns the same Arrow **type and value**
  whether it is called as `F.f(x)` or `spark.sql("SELECT f(x)")`. Written because the whole facade
  suite passed unchanged across a fix that moved `F.to_timestamp` from `timestamp[ns]` to
  `timestamp[us, tz=UTC]` — no row compared the two doors, so the divergence was invisible. The
  Rust half of the same clause is `crates/repark-python/src/column/door_parity_tests.rs`.

- `test_fnp2_free_names.py` — **FNP-2 (2026-08-20):** all four null-ordering corners pinned by
  observed ROW ORDER (not by which method they delegate to), the same four through `Window.orderBy`,
  the plan-collapse non-merge of two specs differing only in null placement, the `ascending=`
  override superseding a per-column marker, and the `column`/`negate`/`session_user` aliases on the
  Arrow path.

- `test_fnp3_destubbed.py` — **FNP-3 (2026-08-20):** the eleven names whose kernel the engine
  already shipped but the facade refused. Every row pinned on the Arrow path AND cross-checked
  against the SQL door for value and type. `crc32`/`sha1` check against `zlib`/`hashlib` rather
  than against RePark's own output; `xxhash64` has no oracle available and pins determinism,
  distinctness and return type only, and says so.

- `test_fnp4c_higher_order.py` — **FNP-4c (2026-08-31):** ten Spark higher-order names on
  the facade Column API, values and Arrow types vs the live PySpark 4.1.2 oracle, plus
  per-name `NUM_ARGS_MISMATCH` expects/got text, raw Arrow map-entry order, zip_with
  right-side nullability, and mixed-width `aggregate` Int64 merge-output (SQL-door
  VALUES + `F.lit(0)`). pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004,
  C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-015
- `test_fnp4_lambda_seam.py` — **FNP-4a (2026-08-20):** a Python lambda reaching the engine.
  `exists` through the Column API, Spark's three-valued null semantics, the empty-array and
  null-array edges, an outer column captured in the body, loud refusals for wrong arity and a
  non-Column return, and the four DataFrame entry points that resolve lambda variables. `join_on`
  is wired but deliberately unpinned — it resolves against the LEFT schema only, which the test
  docstring says rather than implies.

- `test_fnp5_aggregates.py` — **FNP-5 (2026-08-20):** the thirteen aggregates the facade could
  not reach. The nine `regr_*` are pinned against an EXACT fit (`y = 2x + 1`), so slope 2,
  intercept 1, r-squared 1, sxx 5, syy 20, sxy 10 are closed-form rather than repark agreeing with
  itself, and each is cross-checked against the SQL door. `approx_count_distinct`'s `rsd` argument
  is pinned as accepted-and-ignored (Spark uses HLL++, DataFusion HLL) — a signature contract, not
  a claim the estimate matches Spark. **Critic round 2 (2026-08-20):** the two-door check gained
  `DOOR_RETURNS_UNSIGNED`, a RATCHET holding the one name whose doors reach the same kernel but
  hand back different types — the facade casts `regr_count` to Spark's signed bigint, the SQL door
  still returns the engine's `UInt64`. Fixing the door turns this test red on purpose.

- `test_fnp6_regexp.py` — **FNP-6a (2026-08-20):** `regexp_extract_all` / `regexp_substr`
  against Python's `re` as an independent oracle, the three no-match conventions Spark keeps
  apart, door agreement, and a pin tying `regexp_count` to `size(regexp_extract_all(...))` on an
  empty-matching pattern so the shared `Matcher.find()` walk cannot drift between them.

- `test_fn_regexp_extract.py` — **FN-REGEXP-EXTRACT-1 (2026-09-04):** `regexp_extract`
  on both doors against the live PySpark 4.1.2 oracle table (groups, default/whole-match idx,
  `''` on no match, NULL-in NULL-out, `REGEX_GROUP_INDEX` naming `regexp_extract`, POSIX union,
  `\p{L}`, non-ASCII/empty edges, lookbehind refusal; round 2: non-matching input answers
  `''` for any idx on both doors). pins: fn-regexp-extract-1/C-002, C-003

- `test_fnp6_random.py` — **FNP-6b (2026-08-20):** `randstr` / `uniform` pinned on the properties
  Spark's docs state — length, character pool, range, the integer-vs-double return rule,
  determinism per seed — and deliberately NOT on generated values, since no live Spark runs here
  and a value pin would read as parity evidence while being repark agreeing with itself. Also pins
  that a NaN bound refuses distinctly from an inverted range.

- `test_fnp6_validate.py` — **FNP-6c (2026-08-20):** the UTF-8 pair exercised on BINARY input,
  which is the only place they can fail in repark (an Arrow `Utf8` array cannot hold invalid
  UTF-8, while Spark's `UTF8String` can); plus `assert_true` raising on NULL as well as false,
  and honouring a caller-supplied message.

- `test_fnp15_16_declared_refuse.py` — **FNP-15/16 (2026-08-30):** unreachable and deferred-by-cost
  Spark function refusals. Facade, Spark SQL, ANSI SQL, `sql.functions` re-export, and
  `F.expr` over every family. The C-012 "unsupported" strip-check covers all four
  FNP-16 family sections. C-013 member sets are asserted against an independent
  census literal in the test, not against `functions_declared` tuples. pins: fnp-15-16/C-001,
  C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-016, C-017
- FNP-15/16 C-015 — gates live in the execution-record table of
  [fnp-15-16-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-30-fnp-15-16-ledger.md), not in the
  refuse tests. pins: fnp-15-16/C-015
- `test_fnp_critic_remediation.py` — **Critic round 1 (2026-08-20):** regression pins for the
  findings two independent adversarial passes raised on this branch, including the S0 (nested
  higher-order functions returned an inverted boolean), the `ascending=` override matrix, the
  empty-pattern count-vs-collect agreement, `groupBy`/`agg` over a higher-order column, `randstr`
  refusing an enormous length instead of aborting the process, and a structural check that no
  working function still documents itself as unsupported. **Critic round 2 (2026-08-20):** pins for
  the three S1 defects that remediation itself introduced — every dispatched aggregate run through
  `.over()` (a CAST must not hide an aggregate from the window path), `regr_count` as a signed
  bigint through arithmetic (the unsigned fix was keyed on a name and missed its sibling), and an
  unaliased higher-order column keeping the same output name on every build (the uniqueness
  counter leaked into the schema).

- `test_lrs1_higher_order_refusals.py` — **LRS-1 (2026-08-20):** the four paths that leaked a
  DataFusion internal error for a higher-order column now refuse. Two of the nine pins exist only
  to BOUND the change (ordinary columns still pass those paths; higher-order columns still work
  where they worked), because a refusal that over-fires is worse than the error it replaced. One
  pin runs the workaround the messages name.

- `test_lrs2_argument_contracts.py` — **LRS-2 (2026-08-20):** `xxhash64()` refuses by its own
  name with Spark's `WRONG_NUM_ARGS`, and the lambda parameter-kind gate uses Spark's allowlist.
  Three of the seven pins hold behaviour the Critic round wanted CHANGED and the oracle said was
  already right — a pin is how a refuted suggestion stops coming back.

- `test_lrs7_unordered_window.py` — **LRS-7 (2026-08-20):** the unordered-window default frame.
  Eleven window functions measured on both sides; every expected value is Spark's own answer. One
  pin guards the interaction with the round-2 unsigned-count CAST, which `over` peels and re-applies
  around the code this unit changed.

- `test_lrs3_registered_divergences.py` — **LRS-3 (2026-08-20):** the pins that let registry rows
  `RAND-1` and `BL-8` land (§6: a row lands with its pin or it does not land), plus the SQL door's
  new `approx_count_distinct` spelling. The `BL-8` pin is a ratchet — it asserts the door is STILL
  unsigned, so closing the row reds it on purpose.

- `test_sem6_substr_zero_width_null.py` — **SEM-6 (2026-08-21):** `regexp_substr` returns NULL for
  a zero-width match, closing registry row `RE-3`. Seven divergent cases and six controls; the
  controls are the point, because `a*` on `'ab'` (first match non-empty) is what distinguishes the
  correct rule from "empty pattern → NULL", which would pass every divergent case and still be
  wrong.

- `test_sem3_string_idx.py` — **SEM-3 (2026-08-21):** `regexp_extract_all` accepts a string `idx`
  as a literal group index, which Spark, repark's own SQL door and repark's own `regexp_instr` all
  already did. A regression from F-FNP6A-1, which removed `lit_indices` entirely when only position
  1 was wrong. The bare-string-`regexp` and `Column`-`idx` assertions are the controls that stop
  the narrowing from swinging back the other way.

- `test_sem1_extract_all_group_default.py` — **SEM-1 (2026-08-21):** the two-argument
  `regexp_extract_all` defaults to capture group 1, as Spark does — the first change since the port
  that makes a working query return a different value, taken on the owner's dated ruling. Closes
  registry row `RE-1`. The zero-group cases pin the consequence: with a default of 1 and no group
  to take, the call now RAISES, which is what turned two unrelated tests red as runtime errors.

- `test_sem4_regex_group_index_message.py` — **SEM-4 (2026-08-21):** the group-index refusal now
  raises Spark's own `[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX]`, one condition covering both a
  negative and an over-large index where repark had two messages of its own; and the four regexp
  kernels name themselves in their planning errors instead of borrowing a hard-coded
  `regexp_count` / `regexp_instr`. Message-only — the two legal-index assertions are the control
  that proves the accepting path is untouched.

- `test_lrs6_regexp_divergences.py` — **LRS-6 (2026-08-20):** pins that CODIFY today's behavior
  for registry rows `RE-1` and `RE-2`, so the unit that fixes either turns its pin red on purpose.
  `RE-1` is the sweep's highest-value find: the two-argument `regexp_extract_all` returns group 0
  where Spark returns group 1. **SEM-1 (2026-08-21):** `RE-1` closed, so its two pins left this
  file — `test_sem1_extract_all_group_default.py` owns those assertions now. **SEM-5
  (2026-08-21):** the `regexp_substr` pin is now `RE-3`, its own row, measured on plain ASCII; the
  BMP-bound test's claim that both RE-2 divergences are confined to supplementary-plane text was
  false for the substr half and is corrected.

- `test_lrs4_door_domain.py` — **LRS-4 (2026-08-20):** pins for `LOG-1` and `UNIX-1`.
  **SEM-1 (2026-08-31):** `LOG-1` pins flip to Spark: `log(8)` is the natural log on both
  Spark doors; both arities return NULL on the six non-positive edges. `UNIX-1` is unchanged.
  pins: sem-1-spark-answer-parity/C-004, C-008
- `test_sem1_spark_log.py` — **SEM-1 (2026-08-31):** Spark-door `log` kernel, `F.log` two-arg,
  native ANSI base-10 control, `log2`/`log1p`/`ln` incidentals. Oracle live PySpark 4.1.2.
  pins: sem-1-spark-answer-parity/C-004, C-006, C-007, C-010

## I want to...

| ...do this | go to |
|---|---|
| Add a `DataFrame.mergeInto` builder pin (R-MERGEINTO) | `test_merge_into.py` |
| Add a MERGE scan-prune / residual-probe pin (M1/M5/M6/M7) | `test_merge_scan_prune_semantics.py` |
| Add a cache/persist pin (R-PERF-CACHE) | `test_cache_persist.py` |
| Add easy DataFrame lowering pins (R-DF-EASY) | `test_df_easy.py` |
| Add a facade behavior test | `test_session.py` |
| Add smartCsv / inference protocol pins (r25 T4) | `test_t4_csv_smart.py` |
| Add an engine-knob `.config(...)` range/validation test (batch size / partitions / memory) | `test_session_config_knobs.py` (per-key Spark rules — SAF-006/SAF-007) |
| Add an error-taxonomy / exception-type test | `test_errors.py` |
| Add a `Row` API / collect-row parity test (G-ROW) | `test_row.py` |
| Add a collect row-materialization equality pin (PERF-FACADE-COLLECT-1) | `test_perf_facade_collect_rows.py` |
| Add a logical-vs-analyzed column-name pin (PERF-FACADE-WITHCOLUMN-1) | `test_perf_facade_logical_names.py` |
| Add a select/projection display-naming test (Group H) | `test_select_naming.py` |
| Add H2 Group H long-tail / wrap-display / same-object self-join pins | `test_h2_group_h2.py` |
| Add a catalog / publish-path test | `test_catalog_flow.py` |
| Add interchange (`toPandas` / `createDataFrame` / `to_polars`) parity | `test_interchange_parity.py` (G-INT) |
| Add a Catalog API surface / missing-method divergence pin | `test_catalog_surface.py` (G-INT; Y-3 `getDatabase`) |
| Add a `DESCRIBE NAMESPACE` / namespace-metadata-readback test | `test_describe_namespace.py` |
| Add a `SHOW NAMESPACES` / namespace-listing / `LIKE`-pattern test | `test_show_namespaces.py` |
| Add a bare-`spark.sql` eager-DML (INSERT/DELETE/UPDATE/empty OW wipe/CALL refuse) test | `test_sql_dml_eager.py` (C3-Q-002 empty OW facade pin; C3-L-001 residual unknown-CALL refuse; C5-Q-001 incompatible empty OW must not wipe; r25 T2 CREATE OR REPLACE / REPLACE BRANCH|TAG round-trip pin) |
| Pin rewrite_data_files `where` / strategy / sort_order | `test_rewrite_data_files_options.py` — filtered rewrite byte-identity, Spark unknown-strategy and bad-where text, `sort_order` refuse (`RDF-SORT-1`) |
| Add a maintenance `CALL system.*` oracle (I3) | `test_maintenance_call.py` — expire/rewrite/rollback + tag **and** branch dual probe (s1 kept, s2 expired) + positional sort refuse + previous_snapshot_id + unknown/orphan refuse. **MW-1:** expire pins Spark's full six-column result, all bigint and all nullable, after the content-file funnel was split into data / position-delete / equality-delete. **MW-2:** rewrite pins Spark's fifth column `removed_delete_files_count`, non-nullable and 0 — Java's `remove-dangling-deletes` defaults off and the options map refuses, so the zero is a real count. *(MW-7, 2026-08-24: the zero is real, but do not read it as "delete files therefore survive compaction" — on Spark they do not, because its planner rewrites delete-laden files outright. Registry `RDF-1`.)* **MW-3:** the pre-MW-3 orphan refuse pin is retired and replaced by three — `older_than` required (`ORPHAN-1`), dry-run default with Spark's one-column result shape (`ORPHAN-2`), the 24-hour floor measured across its boundary (parity, not strictness), and the shared-CTAS-root refusal pinned on the very fixture that surfaced it — a dry run there listed 139,179 leftover files. **V3-1:** `register_table` adopts an engine-written table and returns Spark's three nullable BIGINT columns (`pa.int64()`); unknown-proc pin is fail-closed on `register_table`. **MW-6:** `rewrite_manifests` pins Spark's two non-nullable `int32` columns and its counts (5 manifests → 1, `5, 1`), the no-op zeros with no new snapshot, and the argument surface — `spec_id` refuses, `use_caching` is accepted and changes nothing (`MANIFEST-2`) |
| Pin the MW-7 scale-measurement machinery | `test_mw7_scale_smoke.py` — the bench driver at gate scale: census vs an independent count, delete files `partitions x merges` then folded to one per partition, COW zero-delete control (a control, not a clean delete-cost isolate — MOR-minus-COW bundles delete reads with MOR's data-file fan-out), manifest drop across `rewrite_manifests`, the five-procedure order, timings that carry their answer |
| Pin the W-0 window-shape bench at gate scale | `test_w0_window_bench_smoke.py` — Iceberg lead/lag cell, memory_limit outcome class, the sliding-refuse set (**EMPTY since WIN-SLIDE-1, 2026-09-04** — the same pin, now the guard against a refusal returning), remaining absents fail at planning. pins: w-0-window-bench/C-002, C-005, C-006, C-009; win-slide-1/C-008 |
| Pin an aggregate over a SLIDING window frame on both doors | `test_win_slide_1.py` — the thirteen once-refusing aggregates x five frame shapes x two doors against the recorded Spark 4.1.2 column, plus `collect_list` frame order, the `collect_set` multiset, `try_sum` BIGINT overflow inside a frame, `CURRENT ROW … UNBOUNDED FOLLOWING`, and the `percentile_approx` accuracy divergence. pins: win-slide-1/C-001, C-002, C-003, C-004, C-007 |
| Pin `RDF-1` (a 100 %-dead in-band data file IS compacted, and its delete file dies with it) | `test_mw7_scale_smoke.py::test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies` — exact equal `file_path` bounds, `removed_delete_files_count` 1, zero delete files, 2,500 rows. pins: rdf-1-position-delete-bounds/C-003 |
| Re-measure the MW-0 MOR growth demo (MW-5) | `test_mw5_baseline_delta.py` — 1,000 rows, ten MERGEs of 200 ids, delete files 1→10 then compact+expire 10→1, Arrow `COUNT(*)` 1,000 `int64`, expire mutation-proof. Wall-clock logged, not asserted |
| Add a case-insensitive column-conform (MERGE star) facade test | `test_case_insensitive_conform.py` |
| Add a drop-in no-op / accepted-ignored disclosure test (OTH-010) | `test_dropin_disclosure.py` |
| Add a Column / functions / types / DataFrame-op test | `test_columns.py` |
| Add a `filter`/`where` SQL-string predicate rewriting / ambiguity test | `test_filter_predicate_rewrite.py` |
| Add a groupBy / agg / aggregate-function test | `test_group_agg.py` |
| Add a union / distinct / dropDuplicates test | `test_union_distinct.py` |
| Add a withColumnRenamed / na (fillna/dropna) test | `test_na_rename.py` |
| Add a `DataFrame.write` (saveAsTable/insertInto) test | `test_writer.py` |
| Add a CTAS write-path type-derivation test (division/write-schema) | `test_ctas_division_writeback.py` |
| Add a Group I `writeTo` / path parquet / `sortWithinPartitions` / `F.weekday` test | `test_writer_v2.py` (octo r1–r4 + 2026-07-22 review: empty stage-swap, sticky transforms incl. Window.partitionBy, same-session path read after overwrite; **DML-B** `overwritePartitions` replaces source partitions, empty input refuses, snapshot `overwrite` (pins: dml-b-insert-overwrite/C-003, C-004); C1-Q-005 option warn-once; C3-SEC-001 transform identity quoting pin (now incl. `bucket`); O3-C1-Q-003 `insertInto` empty overwrite wipe pin; Group P: `test_bucket_partitioned_by_round_trips_e2e` + `test_years_partitioned_by_round_trips_e2e` — non-identity transform CTAS works end-to-end (replaced the old transform-gate rejects)) |
| Add a facade SQL `INSERT OVERWRITE … PARTITION` pin (DML-B) | `test_dml_b_partition_overwrite.py` — static nonempty/empty + Hive arity + two-key AND/incomplete + string/NULL + dynamic keep-siblings + empty-dynamic refuse (pins: dml-b-insert-overwrite/C-001, C-002, C-004, C-005) |
| Add a Window / date-function / row_number test | `test_functions_dates.py` |
| Add a `declareSorted` / sort-elimination plan or refusal test | `test_declare_sorted.py` |
| Add a `tightenNulls` facade pin | `test_declare_sorted_tighten.py` |
| Add an FN-A ordering / null / math function test | `test_functions_a.py` |
| Add an FN-B string-function test | `test_functions_b.py` |
| Add an FN-C aggregate / window-alias function test | `test_functions_c.py` |
| Add an FN-D datetime-function test | `test_functions_d.py` |
| Add a `repark.ta` indicator test | `test_ta.py` |
| Add a `ta.with_indicators` serving-helper pin | `test_ta_with_indicators.py` (do not edit `test_ta.py`) |
| Add a `repark.ta` indicator test | `test_ta.py` (volume family: `test_ta_volume.py`) |
| Add / extend a live-oracle golden (repark == pin == live Spark) | `_live_parity.py` (`SCENARIOS`) + `test_parity_live.py` |
| Record a divergence from Apache Spark (behavior, pin, rationale) | `../../../docs/spark-sql-iceberg-parity.md` — the registry; add the `Disclosure` too when the live tier can express it (§6) |
| Run a live-oracle scenario under a NON-UTC session zone | `Scenario(..., session_conf=((lp.SESSION_TIME_ZONE_KEY, lp.ZONE_TOKYO),))` — and move the size + uniqueness pins in the same diff |
| Add a session-timezone / temporal-edge differential row | `test_session_timezone_parity.py` (`G1_ROWS` / `G16_ROWS`; record the Spark half with `_record_session_timezone_goldens.py`, never by hand) |
| Re-derive the recorded Spark halves (record mode) | `JAVA_HOME=… PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_session_timezone_goldens.py` |
| Add a MERGE INTO differential row (gap G3) | `test_merge_differential_parity.py` (`ROWS`; record Spark half with `_record_merge_differential_goldens.py`, never by hand) |
| Re-derive the MERGE differential Spark halves (record mode) | First the parity-live sync line (`uv sync --locked --extra record --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark`), then `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_merge_differential_goldens.py` |
| Add a DELETE/UPDATE subquery-predicate row (defect G3-E8) | `test_dml_subquery_parity.py` (`ROWS`; record the Spark half with `_record_dml_subquery_goldens.py`, never by hand) |
| Re-derive the G3-E8 Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_dml_subquery_goldens.py` |
| Add a decimal128 / overflow differential row | `test_decimal128_parity.py` (`G2_ROWS` / `G13_ROWS` / `CTAS_ROWS`; record the Spark half with `_record_decimal128_goldens.py`, never by hand) |
| Re-derive the decimal128 Spark halves (record mode) | `JAVA_HOME=… PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_decimal128_goldens.py` |
| Add a joins differential row (gap G4) | `test_join_parity.py` (`ROWS`; record Spark half with `_record_join_goldens.py`, never by hand) |
| Change / extend the DataFrame `leftsemi` / `leftanti` surface | `test_g4b_semi_join.py` for spellings + refusals + G4b-R2 origin-map pins; `test_join_parity.py` for a recorded Spark equality; `crates/repark-python/tests/bindings.rs` for the engine-level pin |
| Pin semi/anti right-origin refuse / drop no-op | `test_g4b_semi_join.py` (`test_right_ref_*`, `test_left_refs_*`, `test_inner_join_right_ref_*`, `test_semi_then_inner_join_emits_the_same_right`, `test_spawn_descendant_still_refuses_unemitted_right`, `test_self_semi_exclusive_set_resolves_df_column`, `test_distinct_name_*`, `test_right_ref_abs_*`, `test_left_abs_*`, `test_inner_join_abs_*`, `test_distinct_name_abs_*`, `test_right_ref_lower_*`, `test_coalesce_left_then_right_*`, `test_abs_string_name_*`, `test_right_ref_agg_*`, `test_left_agg_*`, `test_inner_join_sum_*`, `test_distinct_name_sum_*`, `test_count_distinct_left_then_right_*`, `test_sum_string_name_*`,
  `test_inner_join_abs_keeps_the_abs_on_a_negative_key`) |
| Re-derive the joins Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_join_goldens.py` (hold `/tmp/grok-jvm-record.lock`) |
| Add a timestamp-cast differential row (registry TZ-5 / B-TZ-4) | `test_timestamp_cast_parity.py` (`ROWS`; record Spark half with `_record_timestamp_cast_goldens.py`, never by hand — and keep the SHAPE pin in `test_the_class_is_covered_per_entry_point_and_per_edge` honest in the same diff) |
| Re-derive the timestamp-cast Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_timestamp_cast_goldens.py` (hold `/tmp/grok-jvm-record.lock`) |
| Add a window-function differential row (gap G5) | `test_window_parity.py` (`ROWS`; record Spark half with `_record_window_goldens.py`, never by hand) |
| Re-derive the window Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_window_goldens.py` (hold `/tmp/grok-jvm-record.lock`) |
| Add a nested-container differential row (gap G18) | `test_nested_container_parity.py` (`ROWS`; record Spark half with `_record_nested_container_goldens.py`, never by hand) |
| Re-derive the nested-container Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_nested_container_goldens.py` (hold `/tmp/grok-jvm-record.lock`) |
| Add a facade-boundary container-shape row (gap G10) | `test_boundary_shapes_parity.py` (`ROWS`; record Spark half with `_record_boundary_shapes_goldens.py`, never by hand). Do not extend X-5 VALUES families or census allowlists. |
| Re-derive the G10 boundary-shape Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_boundary_shapes_goldens.py` (hold `/tmp/grok-jvm-record.lock`; marker `y6-g10-fix` after the cycle-1 pin fix) |
| Add a cast-failure differential row (gap G6) | `test_cast_failure_parity.py` (`ROWS`; record Spark half with `_record_cast_failure_goldens.py`, never by hand) |
| Re-derive the cast-failure Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_cast_failure_goldens.py` (hold `/tmp/grok-jvm-record.lock`) |
| Add a three-valued-logic differential row (gap G12) | `test_three_valued_logic_parity.py` (`ROWS`; record Spark half with `_record_tvl_goldens.py`, never by hand) |
| Re-derive the TVL Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_tvl_goldens.py` (hold `/tmp/grok-jvm-record.lock`) |
| Add a float-agg differential row (gap G7) | `test_float_agg_parity.py` (`ROWS`; record Spark half with `_record_float_agg_goldens.py`, never by hand) |
| Re-derive the float-agg Spark halves (record mode) | `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=python/repark-parity/src .venv/bin/python python/repark/tests/_record_float_agg_goldens.py` (hold `/tmp/grok-jvm-record.lock`) |
| Run the live oracle tier (needs a JVM) | `make parity-live` (or `REPARK_PARITY_LIVE=1 … pytest`) |
| Add an acceptance-harness helper (path/config/SQL builder) + its AWS-free unit | `_acceptance.py` + `test_acceptance_helpers.py` |
| Change the real-AWS acceptance run | `test_aws_acceptance.py` (gated on `REPARK_AWS_ACCEPTANCE=1`; never run it AWS-free) |
| Measure a the cutover pipeline cutover Iceberg SQL shape | `test_sql_harden_cutover.py` + `_sql_harden_cutover_programs.py` (memory Spark oracle; S8/S9 CoW); AWS legs `test_sql_harden_cutover_against_glue` / `_s3tables` |
| Run the suite | `uv run maturin develop` then `uv run pytest python/repark/tests` |

## Pointers

- Up: [../map.md](../map.md)
- Code under test: [../src/repark/map.md](../src/repark/map.md).

### PR-6 tier-2 AWS security guard (2026-08-08)
- `_acceptance.py` gained `assert_real_buckets_configured()` + env-overridable BRONZE_BUCKET /
  GLUE_WAREHOUSE (default to synthetic placeholders): a real-AWS run refuses the committed
  placeholders. Two always-run pins in `test_acceptance_helpers.py`
  (`test_placeholder_buckets_refuse_a_real_aws_run`, `test_operator_buckets_pass_the_guard`) are
  declared v2-only facade-census ADDITIONS (`task/port/added-python-tests.txt`); the gated
  `test_aws_acceptance.py` calls the guard before any AWS touch.

## Debug

- `test_sqp_1_string_literals.py` is byte-frozen (sha256) by
  `python/repark-parity/tests/test_pr_245_revalidation_record.py`, and `test_functions_gt1.py`'s
  residual docstring phrases are pinned by `test_sqp_1_record.py` — a rewrap can red either.
- ruff format lockstep (octo C8).


(2026-07-31 R-EXPLODE-REWRITE octo c1 pins in `test_explode_rewrite.py`: str ColumnOrName,
cast sticky, withColumn unnest, pre-aliased AS strip, multi-array exact type, posexplode_outer.)
(2026-07-31 R-EXPLODE-REWRITE octo c2 pins: pre-aliased sibling no double-AS, Timestamp outer,
reserved/mixed-case idents, hostile ColumnOrName quote, asc/desc sticky, alone outer.)
(2026-07-31 R-EXPLODE-REWRITE octo c3 pins: compound mixed-case, nested-list outer, hostile
ColumnOrName, array-of-struct, coalesce outer type, size sibling.)
(2026-07-31 R-EXPLODE-REWRITE octo c4 pins: sql.functions export, nested generator refuse,
hostile cast reject.)
(2026-07-31 R-EXPLODE-REWRITE octo c5 pins: F.size/coalesce/when/str refuse generator, nested
explode refuse, chained cast compose, generator select dup-name preflight.)
(2026-07-31 R-EXPLODE-REWRITE octo c6 pins: F.count/sum/avg refuse generator; filter/orderBy/
groupBy/agg refuse; nested `[[]]` top-level array_length not cardinality product.)
(2026-07-31 R-EXPLODE-REWRITE octo c7 pins: F.year/date_*/.dt refuse generator;
Window.partitionBy/orderBy refuse; cube/rollup/groupingSets + SQL agg bare explode refuse.)

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: repark._native` | Run `uv run maturin develop` first |
| `test_to_polars` skipped | expected unless the `polars` extra is installed (`importorskip`) |
| `test_parity_live.py` live tests SKIPPED | expected unless `REPARK_PARITY_LIVE=1` + a JVM (`make parity-live`); routine CI is JVM-free by design |
| `test_disclosures_mirror_the_registry` RED | `DISCLOSURES` and `docs/spark-sql-iceberg-parity.md` disagree. The failure names which side is orphaned: a registry-only name means the `Disclosure` was deleted (the row lost its drift detector); a disclosure-only name means the row was never written. Fix the side that moved |
| live tier RED | triage per docs/testing.md: golden leg only → golden drift (fix pin); live-Spark diverges from an unchanged pin → oracle drift; a disclosure reds → engines converged |
| a `test_session_timezone_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark output: do NOT delete the row — flip it to `repark=None` (equality) and record the convergence. Thirteen rows were flipped exactly this way when the extraction fix landed, and that flip is its revert-red evidence. |
| an EQUALITY row in that module reds after an engine change | The extraction fix regressed. Check `crates/repark-functions/src/datetime.rs` (the coercion path) and `SparkExtension::configure` (the carrier install) before touching the row — the Rust pins in `crates/repark-spark/tests/session_timezone.rs` localize it faster than the facade does. |
| a row reds saying "moved OFF its pinned disclosure ... regression" | repark matches neither half: re-derive both in record mode (`_record_session_timezone_goldens.py`) before touching the pin. |
| a `test_timestamp_cast_parity.py` row reds with a value 10⁹ too large | the analyzer's `Expr::Cast` arm is not firing. It ships with the Spark door's `SessionExtension`, so check the session was built with it; a bare session legitimately keeps DataFusion's raw tick (pinned in `crates/repark-sql/tests/timestamp_cast_ansi_door.rs`). |
| a `test_timestamp_cast_parity.py` row reds by exactly ONE before 1970 | truncation toward zero crept back in. Spark FLOORS: check `seconds_floor_from_ticks` still uses `div_euclid` — an arrow `Timestamp(Second)` cast hop is the plausible "simplification" that reintroduces this, and only the negative FRACTIONAL rows catch it. |
| `test_range_between_moving_average` reds with every window equal | its ORDER BY key is `date.cast("timestamp").cast("long")` — Spark's own spelling, and epoch SECONDS since the TZ-5 fix. It deliberately carries NO `/1e6` scale workaround any more; re-adding one reds it, which is the point (`task/tz5-cast-seconds-ledger.md` §9). |
| a value-offset `rangeBetween` refuses a CAST order key | the guard resolves the key by NAME and a cast keeps its BASE column's projection name; `Column.over` treats a key as bare only when its spark display equals that name. Both sides are pinned in `test_g2_window_rand_sampleby.py` — do not widen one without the other. |
| a `test_decimal128_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark output (or raises the same ANSI class): do NOT delete — flip to equality / shared-raise and record the convergence. |
| a decimal128 row reds saying regression | re-derive both halves with `_record_decimal128_goldens.py` before touching the pin. |
| a `temporal_range` row reds | check WHICH half moved: an equality row means the interval-bounded path (or Y-1's R2/R3 / Half-B invert fix) regressed (re-derive both halves); a disclosure means a residual class changed — flip it, do not delete it. `task/g5br-range-residuals-ledger.md` §6 names the remaining classes |
| a `test_window_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark output: do NOT delete — flip to `repark=None` (equality) and record the convergence. |
| a window row reds saying regression | re-derive both halves with `_record_window_goldens.py` before touching the pin. |
| a `test_nested_container_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark list/struct/map output: do NOT delete — flip to `repark=None` (equality) and record the convergence. |
| a nested-container row reds saying regression | re-derive both halves with `_record_nested_container_goldens.py` before touching the pin. |
| a `test_boundary_shapes_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark pandas/Arrow boundary shape: do NOT delete — flip to `repark=None` (equality) and record the convergence. |
| a G10 boundary-shape row reds saying regression | re-derive both halves with `_record_boundary_shapes_goldens.py` before touching the pin. |
| G10 budget pin reds | G10 must stay 8–10 rows, ≥1 equality, ≥3 disclosures, a **typed-Map** disclosure (`out_map` / `map_topandas_*`, not a `map_`-prefixed struct), struct+binary both directions, ≥2 `*array_*` **and** `array_from_pandas_object` disclosure, timestamp-unit **us disclosure** plus inbound ns twin, inbound glob `*_from_pandas_*` matching every inbound row; restore families rather than greening with controls. |
| nested budget pin reds | G18 must stay 4–6 rows, ≥2 equalities, ≥2 disclosures, ≥1 `*struct*`, ≥1 `*map*`, ≥2 `*array*`/`*collect_list*`; restore families rather than greening with controls. |
| a `test_three_valued_logic_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark output (incl. null-safe-eq nullability): do NOT delete — flip to equality (`repark=None`) and record the convergence. |
| a TVL row reds saying regression | re-derive both halves with `_record_tvl_goldens.py` before touching the pin. |
| window budget pin reds | G5 must stay 20-28 rows, min 6 equalities, max 22 disclosures, ≥3 `default_frame_*`, ROWS-vs-RANGE, ranking/offset/nulls families, ≥2 `dataframe_api` rows; restore controls rather than deleting families. |
| decimal128 budget pin reds | G2 must stay 20-26, G13 6-8, CTAS exactly 3, min 8 equalities, max 20 disclosures, and ≥3 `*clamps_scale_in_spark` rows; restore the control equalities / clamp family rather than converting them to disclosures or deleting them behind a non-clamp `DECIMAL(38,…)` control. |
| a `test_join_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark output (or DF semi/anti starts succeeding): do NOT delete — flip to content equality and record the convergence. |
| a joins row reds saying regression | re-derive both halves with `_record_join_goldens.py` before touching the pin. |
| joins budget pin reds | G4 must stay 20–30 rows, min 14 equalities, max 8 disclosures/splits, ≥4 `*null_keys_*` (every join type), ≥2 `*duplicate_keys_*`, ≥2 `*type_mismatch_*`, ≥2 `*nullable*`, ≥6 DF content rows, and (G4b) the DF semi family on both the name/list-key and Column-condition paths plus both NULL-key edges; restore the name-gated families rather than greening them with controls. |
| a `df_left_semi_*` / `df_left_anti_*` row reds | the G4b DataFrame semi binding regressed. Localize in Rust first (`crates/repark-python/tests/bindings.rs` `join_on_names_left_semi_*` / `_left_anti_*` / `_semi_family_never_merges_a_key_column`), then the facade alias map + `_join_on_condition_h1` left-only projection in `python/repark/src/repark/dataframe/core.py`. Re-splitting the row to green it is a laundered regression, and `test_join_row_set_covers_g4_budget` reds on it. |
| `test_g4b_semi_join.py` conditionless test reds | the semi/anti `on=None` / `on=[]` guard stopped firing, so a conditionless semi join now falls through to the Cartesian path and answers an m×n cross join instead of Spark's rows. Restore the `_SEMI_JOIN_HOWS` guard in `DataFrame.join`; do not relax the test. |
| `test_right_ref_select_*` reds with left `k` values | the G4b-R2 origin map lost join-type awareness — `select(right["k"])` name-fell-back to the left column. Restore `_remember_unemitted_right_origins` on both the name-key and H1 condition paths; do not special-case `select` alone. |
| `test_semi_then_inner_join_emits_the_same_right` reds | `_spawn` copied `_origin_not_emitted` onto the inner-join child and the emitting path did not subtract. Restore `left_only=False` on non-semi `_remember_unemitted_right_origins`. |
| `test_spawn_descendant_still_refuses_unemitted_right` reds with left `k` | the `_spawn` copy line was deleted; filter/select children name-fall-back. Restore `child._origin_not_emitted = self._origin_not_emitted`. |
| `test_self_semi_exclusive_set_resolves_df_column` reds | exclusive-set remember started recording the shared self plan id. Keep `right.ids - left.ids`. |
| `test_right_ref_drop_is_spark_noop` reds by dropping `k` | `drop(right["k"])` fell through to name-drop of the left column. The unemitted-origin branch must `continue` (Spark 4.1.2 no-op), not raise and not name-drop. |
| a `test_cast_failure_parity.py` row reds saying CONVERGED | repark now matches Spark (shared raise, or success golden): do NOT delete — flip to content/error equality and record the convergence. |
| a cast-failure row reds saying regression | re-derive both halves with `_record_cast_failure_goldens.py` before touching the pin. |
| cast-failure budget pin reds | G6 must stay 8–10 rows, min 3 equality-class, min 3 shared-raise errors, ≥2 `try_cast_*`, ≥1 DF `Column.cast` row, name-gated malformed-numeric / malformed-temporal / overflow families; do not invent divergences under ANSI ON. |
| a `test_float_agg_parity.py` row reds saying CONVERGED | repark now produces the recorded Spark output: do NOT delete — flip to `repark=None` (equality) and record the convergence. |
| a float-agg row reds saying regression | re-derive both halves with `_record_float_agg_goldens.py` before touching the pin. |
| float-agg budget pin reds | G7 must stay exactly 2 rows (sum + avg of the catastrophic-cancellation fixture). |
| a live scenario reds only under a non-UTC `session_conf` | the override reached the oracle but not repark (or vice versa): repark takes it at session BUILD, Spark at `conf.set`. Check `build_repark_engine` stopped the previous active session. |

First checks: `uv run maturin develop` then `uv run pytest`. Escalate to: [../map.md#debug](../map.md).

<!-- 2026-07-14: lint-pass doc touch for staged CTAS / metadata schema -->

- ACC remediation: F6 tick-identity LTZ pin (Q-001); withColumns TypeError before alias (Q-002).

- Octo C1: SQL current_timestamp residual pin; CTAS unit==us; rename chain; default master.
- Octo C2–C4: CTAS tz pin; not-null current_timestamp; empty withColumns; transform *args.
- Octo C5–C8: stop() blocks sparkContext/version; CTAS tz UTC pin.

- Octo r2 C1: config spark.master OTH-010 warn; held SparkContext stop; withColumns str keys; F1 near-now + F.expr ns residual pins.
- Octo r2 C2–C5: master warn on getOrCreate reuse; SQL CTAS ns residual fail pin; empty rename map.
- Octo r2 C6–C8: dual-spell withColumns identity pin; F4 atomicity re-attested.

- Octo r3 C1: DF liveness token on stop; empty col names rejected; case-insensitive spark.master warn; cast(TimestampType) tz-strip pin.
- Octo r3 C2–C5: insertInto/stop gate; singular empty names; CTAS near-now value; columns/schema/transform after stop.
- Octo r3 C6–C8: held saveAsTable after stop pin; mutation re-attestation CLEAN@S1.

- `test_pg_catalog.py` — PG3 catalog config + skip-loud live. Ported minus **one** node (EC-4):
  `test_postgres_catalog_config_redacts_in_engine_errors` — it is the only case here that reaches
  repark-core's `CatalogKind::Postgres` `NotImplemented` registration, which refuses the session at
  native construction. The parse-level pins (`…requires_url_at_build`) port green.

- `test_pg_acceptance.py` — PG4 battery + report SKIP-LOUD/live.

- **skeptic fix:** dbtable-from-props pin; scale no silent cap; catalog.schema.table live pin.

- **octo-extra c1:** bad partition int → IllegalArgumentException pin.
<!--(combine rider: dash normalization, pinned ruff) 2026-08-01 -->

- `test_scalar_subquery_sort_pin.py` — DF54.1 Sort-loss regression pins (fuzz-42-1/2 class);
  re-enable done-signal for the physical scalar-subquery flag.

- 2026-08-01 style rider: `test_ml_feature_oracle.py` reformatted (ruff format at tip).

- 2026-08-01 rider: `test_pyspark_compat_smoke.py` is now a subprocess WRAPPER over
  `repark-parity/compat/smoke_suite.py` (union-run JVM/namespace isolation).

<!-- 2026-08-02: r16 combine rider — stale pins updated to X2/X3 surfaces: bigint dtypes, error-class regexes, lit(list) now legal -->

<!-- 2026-08-02: r16 rider — compat-smoke wrapper importorskips pyspark itself (wheel-smoke CI has no pyspark; inner-suite skip → pytest exit 5 misread as failure) -->

<!-- 2026-08-03: R-AUTO-MEMCAT — test_auto_memory_catalog.py: bare round trip, suppression (config/knob/foreign default), stop() warehouse cleanup, :memory: isolation across sessions -->

<!-- 2026-08-03: R-AUTO-MEMCAT style rider — RUF043 raw regex + doc-line wrap in test_auto_memory_catalog.py -->
- M7 format/lint gate clean (ruff format + py-lint).

<!-- 2026-08-03 (r21 combine rider 3): test_t1_cdf_ingest.py + two-mode legacy-conf coerce/refuse pin. -->

<!-- 2026-08-03 (r21 combine): annotation fix in legacy-conf pin (ReparkSession, ruff F821). -->

<!-- 2026-08-03 (r21 combine rider): T2 OOM tests made box-deterministic — pin datafusion.execution.target_partitions=128 so spill reservations exceed the pool regardless of core count (CI wheel-smoke DID-NOT-RAISE fix); verified under taskset 2-CPU. -->

<!-- 2026-08-04 (r23 combine rider): catalog surface pin += registerFunction/functionExists (+snake twins) — C6 additions. -->
<!-- (pin content landed this commit) -->

- `test_sec_boundaries.py` — **r24 SB1:** SEC-01 facade+free-SQL `maxArrayElements` ceilings; SEC-02 `allowLocalFilesystemDDL` COPY TO refuse/allow pins.

<!-- 2026-08-04 (r24 combine rider): cast-refusal pins renamed with their assertion (rule 11) —
  test_cast_unknown_type_raises_parse_exception_at_facade_allowlist; four hostile-token pins in
  test_explode_rewrite.py moved to ParseException with the allowlist untouched. -->

<!-- 2026-08-04 (r24 combine rider): test_sec_boundaries.py += Python-side grandfather pin
  (COPY TO under a registered memory-catalog warehouse allowed; sibling path refused) and the
  fail-closed pin (runtime conf.set cannot loosen the gate in either spelling). Readback-lie on
  conf.set is a documented r25 seed in task/sb1-boundaries-ledger.md. -->

<!-- 2026-08-04 (r24 combine rider): test_sec_boundaries.py += typed-writer regression pin —
  df.write works with the gate at default while free SQL to a SIBLING path still refuses
  (guards against re-widening the grant to destination.parent). -->
- r25 morning critic pins: facade metadata-table `.count()`/styled `.show()`/partial
  projection (test_metadata_tables.py); `CREATE OR REPLACE TAG … IN <table>` spelling kept
  in the round-trip loop (test_sql_dml_eager.py).

- octo C1: option-map samplingRows pin

- octo C2: samplingRows empty/non-integral refuse pins
- octo C3 commit 2026-08-05T23:29:11Z: order-independent decimal union
- octo C4: t2 ledger reader.py SSOT; bool samplingRows pin

## I want to... → go to

| I want to... | go to |
|---|---|
| Find the test for a facade surface | grep this file for the surface name, then the annotated bullet |
| Understand why a test is absent | [../../../task/port/deferred-python-tests.txt](../../../task/port/deferred-python-tests.txt) + [../../../task/port/deferred-tests.md](../../../task/port/deferred-tests.md) |
| Reproduce the recorded cohort run | `docs/design/python-facade.md` §6.3 (environment clauses) |
| See the port record for this suite | [p3e-facade-ledger.md](../../../docs/history/port-v2/p3e-facade-ledger.md) |

## Pointers

- Up: [../map.md](../map.md)
- The package under test: [../src/repark/map.md](../src/repark/map.md).
- The parity harness nine of these files import:
  [../../repark-parity/tests/test_compat_harness.py](../../repark-parity/tests/test_compat_harness.py).

## Debug

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: repark._native` | The wheel is not installed in the running interpreter |
| `ModuleNotFoundError: repark_parity` | Install `python/repark-parity` by path — a bare interpreter outside the uv workspace does not resolve it implicitly |
| A pyspark/duckdb-gated module runs instead of skipping | The cohort requires both ABSENT; an unstated venv changes the denominator |
| A gated AWS/PG test runs | A `REPARK_*` gate variable leaked into the environment — the cohort requires every one unset by name |
| Outcome moved vs `task/census/baseline-fc3f48102/facade/` | Do not "fix" it — attribute it. Unattributed movement fails the phase (design §6.4) |

First checks: rebuild the wheel and reinstall by path. Escalate to: [../map.md#debug](../map.md).

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `_acceptance.py` (`TEMP_VIEW` is now
  `staging_view`; docstrings), `test_acceptance_helpers.py` (the example entity strings are
  now `entity_a`/`entity_b` and the example key column `entity_a_id`, with their assertions),
  `test_catalog_flow.py`, `test_aws_acceptance.py`, `test_columns.py`, `test_dogfood_gaps.py`,
  `test_functions_dates.py`, `test_merge_into.py`, `test_sql_alias.py` (docstrings/comments).
  **Test NAMES were deliberately left alone** — a test rename is a declared-rename unit that
  ships alone (docs/testing.md "Relocation discipline") and would move the facade census's
  collected-name multiset.
**SQM round 7 (R7-1):** `test_declare_sorted_tighten.py` gains
`test_named_read_paths_find_a_temp_view_under_set_default_catalog` (under a `SET` to another
catalog, `tableExists` / `spark.table` / free SQL / cache / persist / checkpoint /
`createDataFrame` / `selectExpr` / `alias` all agree on the view's rows — every one of them
MEASURED red on the round-7 BASE `3910ac7`),
`test_no_set_leaves_every_named_read_path_byte_identical` (the no-SET leg: same rows, same
columns, `list_temp_view_names` still ONE-part — the home spelling is a SQL reference, never a
rename) and `test_a_catalog_over_the_home_refuses_the_read_spelling_too`. Tests that compare a
facade-minted view NAME (`test_cache_persist.py`, `test_create_dataframe_materialize.py`,
`test_ml_boost_oracle.py`) now go through `repark.spark._temp_views.local_view_name`, because the
handles carry the home-qualified spelling; `test_e2_readwriter.py::test_resolve_prefer_temp_view`
pins the new resolver contract (home segments in, home-qualified name out, `None` → catalog
qualification unchanged). The under-`SET` pin's docstring states its SCOPE: it covers facade
spellings only, NOT the engine crates' own bare scratch registrations (`repark-iceberg` MERGE /
identity DML, `__repark_tt_*`), which stay red under the same `SET` on BASE and on this tree
alike — a disclosed round-8 residual, deliberately unpinned.
