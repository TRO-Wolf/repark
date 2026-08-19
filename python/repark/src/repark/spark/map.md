# map — python/repark/src/repark/spark

## Purpose

The PySpark facade package (`repark.spark`) after the Q1 re-home (2026-08-14).
`from repark.spark import ReparkSession`; `SparkSession` is a drop-in alias. The
`pyspark` → `repark.spark` sed-swap alias is `repark.spark.sql`. A thin, typed Python
shell over the compiled `repark._native` module; all compute runs in Rust, rows cross as Arrow
(zero-copy). See [../map.md](../map.md).

## Contents

- **r26 T1 packages:** `dataframe/` (was `dataframe.py`) and `session/` (was `session.py`); public import paths frozen. `DataFrameReader.smartCsv` lives in `session/reader.py`.

- `_csv_smart.py (r26 T2: decimal union; samplingRows/10k cap)` — **r25 T4 csv-smart** (`# === r25 T4: csv-smart ===`): Q1 inference
  PROTOCOL (bool→int32→int64→decimal128→float64→date→timestamp→string; fail falls back;
  terminal string; deterministic) + messy-CSV prep (BOM, preamble skip, delimiter detect
  (origin/main ``csv.reader`` agreement-first; known-limit on embedded-rival files —
  declare ``sep``; B4 salvaged single-char refuse; L3 comments restored),
  header detect, ragged null-pad). Consumed by `DataFrameReader.smartCsv`. Not shared Rust
  (greylit Q1(b)); claim-board copy for T5/T6. Pins: `tests/test_t4_csv_smart.py`.
  **Q10:** timestamp rung follows `spark.sql.timestampType` via
  `session/timestamp_type.default_timestamp_data_type`.
- `session.py` — **r25 T4** `DataFrameReader.smartCsv(...)` repark extension (Q5 method
  contract); default `.csv` frozen (r20-R1). `dataframe.py` — `describe_ingest()` + sticky
  `_ingest_report` slot (diagnostics; no silent magic).
- `_idents.py` — **r23 QI1 / CQ-006/007:** single-source SQL identifier quoting + path-escape
  needles. Always-quote (`quote_ident`) vs quote-if-needed (`quote_ident_if_needed`) call-site
  classes; `reject_path_escape_segment` (O3-C4-SEC-001); injection/path-escape probe tables
  lockstep with `crates/repark-write/src/idents.rs`. Call sites: session / dataframe / catalog /
  column / functions / merge / polars / ml* (incl. base+tuning) re-export or import; no dual-sourced
  defs (**octo C1** remigrated residual inlines). Injection assert uses independent oracle equality.
  **octo C2:** docstring refs name `_idents.quote_ident` (not session local).
  **octo C3:** session parse + merge ON bare check use `is_plain_ident` SSOT (no local plain-ident regex);
  merge keeps `AnalysisException` import for empty-clause gate.
- `dataframe.py` / `column.py` / `functions.py` / `polars.py` — `select()` global-agg
  (R-SELECT-GLOBAL-AGG): pure simple-name AggregateFunction (`_is_aggregate_function`, no
  nested `sql_expr` parens) → `group_by().agg`; composed / scalar / foldable companions
  (`lit`, `current_timestamp`) / compound AF args → SQL global-agg path; sticky
  `_is_aggregate` + free-attribute bit (`_has_free_attribute`) so nested bare attrs
  (`sum+id`, `coalesce(sum,id)`, `upper(id)` beside sum, `greatest(sum,id)`) raise
  `[MISSING_GROUP_BY]`; pure_global = each col is (aggregate|foldable) and not free and
  not ungroupable (window `.over` companions rejected — octo C6-L-001; sticky
  `_has_ungroupable` for nested sum∘window / coalesce / when — octo C7-L-002; `F.rand`
  non-foldable nullary — octo C7-L-001); AF builders carry structural
  quoted `sql_expr`; `alias` does not embed `AS` into `sql_expr`; SQL path rebinds
  case-preserved simple-name AFs (octo C1–C3). **octo C4:** rebind after `.alias` via
  structural `sql_expr` (agg_name cleared); batch-4 AFs (`stddev`/`variance`/`median`/
  `corr`/`covar`/`bit_*`) structural sql_expr + rebind allowlist; `asc`/`desc` keep
  sql_expr; `first`/`last(ignorenulls)` emit `IGNORE NULLS`; `collect_list` SQL =
  `coalesce(array_agg … IGNORE NULLS, make_array())`; `isnull` + `date_add`/`add_months`/
  `date_format`/`trunc`/`date_trunc` sticky free/agg. **octo C5:** rebind allowlist also
  covers `first`/`last` (via `first_value`/`last_value` + `IGNORE NULLS`),
  `collect_list`/`set`, `corr`/`covar_*`, single+multi `count_distinct`; `collect_set`
  SQL = `array_distinct(array_agg … IGNORE NULLS)` (DF DISTINCT keeps NULL); multi
  `count_distinct` SQL = null-if-any `struct` pack (not bare multi-arg COUNT DISTINCT).
  **octo C6:** free-OR pins on `_scalar`/`concat`/`greatest` at select boundary; pure_global
  requires aggregate|foldable; compound pure AF → SQL; `polars._sort_key` preserves
  sql_expr + generator/generator_cast (combine C6-Q-002); pure `collect_set` null-exclude pin.
  **octo C7:** `_scalar` nullary not vacuous-foldable (`rand` non-foldable + ungroupable;
  `current_date` explicit `foldable=True`); sticky `_has_ungroupable` from `.over` /
  `rand` OR-propagated across binary/when/coalesce/wrappers; pure_global rejects
  ungroupable.
- `dataframe.py` — **r24 DF1** (`# === r24 DF1: dynamicFlatten ===`): repark-extra
  `DataFrame.dynamicFlatten` / `dynamic_flatten` — recursive struct unnest with parent-path
  prefix (default `separator="_"`), null-safe `CASE WHEN parent IS NULL` field projection,
  in-place unnest column order (polars select+unnest), optional list explode
  (`explode_lists=True`, in-place column position), drop `array<void>`
  (`drop_null_lists=True`), `empty_as_null=True` (keep NULL and EMPTY lists as one
  null-element row; `False` is the polars ≥2.0 default — EMPTY drops, including
  EMPTY `array<void>` siblings that carry typed lists; NULL void lists are kept),
  `max_depth=100`
  LOUD refuse (never silent truncate); schema-only walks (no forced collect); prefixed name
  collisions refuse LOUD (Q25). Planner is native (`repark_core::dynamic_flatten`);
  the facade method is the type-gate + `_spawn`. Pins: `tests/test_dynamic_flatten.py`.
  Registry row is orchestrator-side.
  Region: type-gate + `_spawn` only (Python helper deleted); H1/H2 identity,
  collect, writer frozen.
- `dataframe.py` — GroupedData.pivot two-phase CASE aggregates (R-PIVOT; octo c1–c8:
  bare count, alias recovery, distinct.limit then sort ≤max, cube/rollup refuse,
  simple-name-only inputs, first/last ignorenulls=True, avg/min/max pins;
  REPEATED_CLAUSE, bool true/false names, Cast lit to pivot type, isnan for NaN;
  c4: cast via logical long not schema IntegerType; count(10) exact not startswith;
  c5: count(\"1\") vs count(*) disambiguated via native display; pivotMaxValues
  equality boundary pin len>max not >=;
  c6: count(CAST/abs) refuse not row-count Ident(...); sum/avg/min/max/first/last
  lit(1) refuse via typed-scalar native display — no digit-column fail-open;
  c7: first_value/last_value after .alias; countDistinct match requires space after
  DISTINCT — not count(distinct_id)/count(distinct);
  c8: values-list form uncapped by pivotMaxValues — cap only on inferred discover).

- `dataframe.py` — `select()` all-aggregate list routes to global `agg` (R-SELECT-GLOBAL-AGG).
- `column.py` / `dataframe.py` / `functions.py` — **r22 C5 census-r7** (`# === r22 C5: census-r7 ===`):
  `Column.getItem` / `getField` thin wrappers over `__getitem__`; `Column.__getattr__` struct
  field access; `Column.try_cast` (native `TryCast` + `TRY_CAST` display); `Column.transform`
  (Column→Column chain); `F.when` / chained `.when` `NOT_COLUMN` type gate; `DataFrame.sameSemantics`
  `NOT_DATAFRAME` + best-effort native-handle identity (not plan-text / not Catalyst).
  `cast`/`try_cast` share `NOT_DATATYPE_OR_STR` via `_engine_type_from_cast_arg` (octo C1).
  Outside P5/H2/R2 collect/join/writer bands. Pins: `tests/test_c5_census_r7.py`.
- `_secrets.py` — **r24 A3 SEC-04:** mirror of Rust `prop_key_is_secret` needles
  (`catalog_config.rs:126`); `RuntimeConfig.getAll` redacts values to `***`; `get(key)`
  of an explicit secret key is unchanged (ledger both-ways rec).
- `column.py` — **r24 A3 QUAL-03:** cast map lockstep (`tinyint`/`smallint` aliases) with
  native `parse_data_type`. Pins: `tests/test_a3_cast_vocab.py`,
  `tests/test_a3_secrets_redaction.py`.
  **G15:** `cast` / `try_cast` refuse a non-binary `StringType` / `string collate NAME`
  token at first evaluation (not construction). Pins: `tests/test_collation_refuse.py`.
  **U-DF-1:** `_bound_generator_array` rebinds a single-ident explode source through
  the frame schema at select mid-project (unquoted native `col` folds case). Pins:
  `tests/test_explode_rewrite.py`, `tests/test_dynamic_flatten.py`.
- `functions.py` — **r24 A3 octo C1-Q-001:** `posexplode` STOP message has no embedded
  DataFusion major (was stale "52.x" while pin is 54.1); pin in `test_explode_rewrite`.
  **G15:** `collate` / `collation` are **not** stubbed — absence is already `AttributeError`
  (A5); documented in `tests/test_collation_refuse.py`.
- `functions.py` / `dataframe.py` / `column.py` — explode/explode_outer guarded unnest
  (R-EXPLODE-REWRITE); posexplode STOP. Octo c1: str→col, sticky generator+cast;
  octo c2: quoted idents (no double-AS), Timestamp/nested element types, asc/desc sticky,
  withColumn→select rewrite, exact element-type bind, strip AS on pre-aliased array;
  octo c3: two-phase native siblings + SQL unnest (compound mixed-case, size/cardinality,
  ColumnOrName not free SQL); element type only for explode_outer / explode_keep_null
  (DF-2: struct elements spell via CAST(NULL AS struct<…>); void uses
  untyped `make_array(NULL)`; map still refuses);
  no BIGINT fail-open;
  generator cast keeps array native.
  octo c4: explode*/posexplode* in `functions.__all__` (sql.functions sed-swap);
  nested generator ops refuse loud (UNSUPPORTED_GENERATOR); cast type allowlist (no
  fail-open into generator CAST SQL).
  octo c5: F.* `_scalar`/coalesce/concat/when + explode* input reject nested generators
  (no silent non-unnest / kind overwrite); chained `.cast().cast()` composes cast tokens;
  generator select runs duplicate-name preflight; Decimal(p,s) from Arrow debug.
  octo c6: `_aggregate_argument` + filter/orderBy/groupBy/agg refuse generators;
  empty guards use top-level `array_length` (not multi-dim `cardinality`).
  octo c7: `_date_fn`/date_* + `.dt` refuse generators; Window.partitionBy/orderBy
  refuse; cube/rollup/groupingSets + `_agg_via_sql_group` refuse (parity with groupBy/agg).
  **U-DF-1:** `_select_with_generator` mid-project uses `_bound_generator_array`
  so string-form / `F.col` explode of mixed-case createDataFrame fields (`Legs`)
  resolve; Column-form `df['Legs']` and compound inners unchanged.
  **combine octo C1:** `_select_with_generator` mid-projects via `_plan()` (not raw
  `_inner`) so mapInArrowxexplode/withColumn(explode) materializes the UDF bridge;
  sticky-aggregate classification runs *before* the generator short-circuit so
  `select(explode, sum)` hits `[MISSING_GROUP_BY]` (not unnest mid-project).
  **combine octo C2:** `_select_global_aggregate_sql` registers one `_plan()` snapshot
  (not action `create_or_replace_temp_view` + second `group_by()` prepare) so
  mapInArrowx`select(sum, lit)` / `cast(sum)` / `sum+1` share pure-AF plan-stable rows.
  **combine octo C3:** `_rebind_stable_name_column` sort-marker branch preserves
  `sql_expr` / `is_aggregate_function` / `generator` / `generator_cast` (parity with
  `Column.asc`; reserved cube SELECT quotes); `withColumn`/`withColumns` refuse sticky
  `_is_aggregate` (`[INVALID_USAGE_OF_AGGREGATE]`) so pure_global cannot collapse N→1.
  **combine octo C4:** `explode`/`explode_outer` refuse sticky-aggregate args
  (`explode(collect_list)` / `explode(array_repeat(sum))`) + select generator+agg early
  gate → `[MISSING_GROUP_BY]` (not unnest); `_grouping_col_sql` always `_quote_ident` on
  str CUBE/ROLLUP/GROUPING SETS keys (embedded `"` doubled — C4-SEC-001); `selectExpr`
  registers `_plan()` (not `_native_for_registration`) so mapInArrow post-prepare agrees
  with `select`/`filter` (C4-L-001).
  **combine octo C5:** SQL-lowering plan children (alias/sample/randomSplit/summary/
  set-ops/crossJoin/unpivot) + `_agg_via_sql_group` register `_plan()` (C5-Q-001 /
  C5-L-001); generator `alias`/`cast` keep sticky aggregate/free/ungroupable/AF
  (C5-Q-002); unpivot `_quote_ident` + `_sql_string_literal` (C5-SEC-001); cube/rollup
  agg `AS` Spark default / `.alias` names (C5-L-002).
  **combine octo C6:** `polars._sort_key` keeps generator/generator_cast (parity with
  asc/desc — C6-Q-002); `_agg_via_sql_group` no substring `count(Int64(1))` rewrite —
  structural `count(*)` on `F.count("*")` + `GroupedData.count` (C6-SAF-001);
  `_lit_sql_expr` embeds non-finite floats as `CAST('NaN'|'Infinity'|'-Infinity' AS
  DOUBLE)` not bare identifiers (C6-SAF-002); MIAxexplode pins use non-idempotent
  call-count (C6-Q-001).

- ruff format lockstep (W4 functions.py).
- **F2 R-CENSUS-R3-VALUE (2026-08-02):** FAIL-VALUE harvest — nested createDataFrame
  tuple→`struct<_1,_2>` + short-name pad `_2` + empty-map type merge + map collect→dict;
  scalar `DataType` schema → `value` column; `printSchema(level)` via `treeString`;
  `DataFrame.__str__`/`__repr__` → `DataFrame[name: type, …]`; overlay display default
  `-1`; `regexp_replace` global `g` + Column pattern/replacement; mixed `lit([...])`
  string coerce; `sec`/`csc` Inf at exact zero (CASE; global div-by-zero NULL unchanged).
  Group H self-join **H1-shipped** (condition/self-join + AMBIGUOUS + drop-by-Column);
  **H2 (r22):** non-origin duplicate projection names (cast/year/lit) via multi-name map;
  same-object self-join equi sugar; wrap-display collapses ``.alias("v")`` in outer exprs.

  **octo C1:** map null-before-int witness; overlay `-1`==omit value; nested array-of-map
  collect dicts; lit int+float numeric promote (not string).
  **octo C2:** empty typed createDataFrame keeps engine_types (not string); overlay float
  pos/len → NOT_COLUMN_OR_INT_OR_STR.
  **octo C4:** lit list treats numpy Integral/Real as int/float (no faked string).
  **octo C5:** homogeneous numpy int/float lists normalize to Python builtins for lit().
  **octo C8:** ruff wrap/format on map-collect, overlay, lit coerce, F2 pins.
- **X1 R-CENSUS-FUNCTIONS (2026-08-01):** `session.range` (generate_series exclusive-end;
  octo C2: doc discloses BIGINT/Arrow int64 vs facade Int64→IntegerType collapse);
  octo C3: `_integer_argument` accepts column-name str (date_add/add_months/date_sub);
  hour/minute/second on Time via repark DatePartUdf;
  Column `between` / pow / string predicates / eqNullSafe / bitwise*; functions trig +
  dayname/monthname; `lit` date/datetime/time/Enum/list; `array()` via make_array;
  `types.LongType`. Scoreboard: functions+column 9→19 actor PASS; octo +hour/minute/second
  (≥22 functions+column engine PASSes verified).
- **R-DF-BATCH2**: cube/rollup/unpivot/explain/createTempView; loud toJSON;
  **G1:** `DataFrame.stat` property + corr/cov/crosstab/sampleBy/approxQuantile live
  (freqItems still loud); pivot is real R-PIVOT — not loud-unsupported.
  Group H / **H1 (r20):** per-DataFrame `_plan_id`; schema-bound Columns carry
  `origin_plan_id`/`origin_field` + `join_sql_part` QCOL tokens; condition joins rewrite
  ON to relation-qualified SQL with unique engine names when display names collide
  (ordinal in `__repark_{l|r}_{plan_id}_{n}_{display}` — chained multi-name joins,
  octo H1-C1-001), `_display_names`/`_engine_names`/`_origin_map` for Spark multi-name
  output, `AMBIGUOUS_REFERENCE` on bare ambiguous getitem, `drop(Column)` /
  `select(parent Col)` via origin map; `select("*")` expands via engine/display binds
  (H1-C1-002); filter compounds rewrite QCOL via `_rewrite_qcol_tokens_local`+`filter_sql`;
  `_column_of`/`withColumn`/asc|desc preserve origin; cast/isNull keep `join_sql_expr`;
  `_iter_bound_columns` for rename/na.fill/na.drop; when/CASE join_sql; multi-name select
  rewrite preserves `join_sql_expr` (fillna coalesce); toDF/alias/union/sample/withColumns/describe/dropDuplicates/intersect/replace/randomSplit/dtypes/selectExpr-star preserve
  multi-name display maps; name equi-join still SubqueryAlias only on clash/self
  (octo C1-Q-003).
  **H2 (r22):** non-origin select dups (`select(x, x.cast)`, `year,year`, `sum,sum`) use
  synthetic `__repark_sel_h2_{n}_{k}` engines + display map (global-agg early-return attaches
  overlay — critic-octo C1-002); same-object `df.join(df, cond)` alternates QCOL token sides
  for simple leaf comparisons (equi cardinality); multi-token arms refuse loud with
  `df.alias("l").join(df.alias("r"), …)` (critic-octo C1-001).

  sampleBy fractions ∈ [0,1] incl. NaN (C1-Q-001);
  approxQuantile relativeError non-negative number (C1-Q-002).
- **R-FACADE-HYGIENE (W7):** listTables hides __repark_cdf_*; weakref finalize; fillna one-proj; dropDuplicates row_number; OOS named errors.
- `dataframe.py` — includes `DataFrame.mapInArrow` / `mapInPandas` (U-SPIKE-MAPINARROW facade streaming bridge; re-runs on action unless cached; unpersist restores re-run and clears `_mia_plan_ready` so plan children rematerialize — C7-Q-001 / C7-L-001; uncached action rebinds `_inner` when no plan-stable snapshot; schema via `_sql_type_to_arrow`; upstream close-on-fail; MIA view finalize on Python DF; peek-bounded isEmpty/take/show; `_native_for_registration` is action-like (keeps `_map_bridge` so write/temp-view does not one-shot pin — C3); `_prepare_for_plan` plan snapshots keep parent `_map_bridge` + **reuse one plan-stable** MIA view (C4 + C5-SAF-001); `GroupedData.agg` re-prepares so empty placeholder cannot silently aggregate (C5-Q-002); set-ops/crossJoin dual register under try/finally (C4-SAF-001); `_identity_child` for no-ops copies bridge + `_mia_plan_ready` (combine C7-Q-001); `polars.join` `_plan()` register (combine C7-Q-002); register-then-track / drop-on-sql-fail (C3-SAF-001); mapInPandas None is loud like mapInArrow; empty mapInPandas frames emit a 0-row RecordBatch so schema stays loud — C6-L-001; mapInPandas never rebinds the closed-over input batch name on yield — C8-L-001; **I4:** `_execute_map_in_arrow_bridge` prefers `register_arrow_stream_as_temp_view` over a `RecordBatchReader` of `_iter_map_in_arrow_output` (captures mid-stream `PySparkException` past C-stream ArrowInvalid wrap); IPC path is `_execute_map_in_arrow_bridge_ipc` version-skew fallback when the native symbol is absent).
- `dataframe.py` — **U6 / R-APPLYINPANDAS:** `GroupedData.applyInPandas(func, schema)` / `apply_in_pandas` over the production mapInArrow bridge. Plan: engine-side `orderBy(*group_keys)` for key-contiguous Arrow stream (single-node `repartition` is a no-op; full sort is enough) → mapInArrow streams batches → single-pass group boundary scan with boundary-stitch buffering (`_iter_apply_in_pandas_group_tables`, O(largest group + batch); full-stream re-sort/re-group in facade is forbidden) → per-group `pandas.DataFrame` → `func` → re-ingest via C-stream path. Contracts: lazy (schema/columns without running func); re-run on action unless cache; schema DDL/StructType with pandas→Arrow cast to declared schema then mapInArrow validation; user exceptions → PySparkException + traceback; empty input → empty output (no func calls); global `groupBy()` = one group (honestly O(dataset)); cube/rollup/grouping-sets + pivot refused; non-NamedExpression group keys refused (project first). **M5:** pure `GROUPED_AGG` `pandas_udf` in `groupBy().agg` routes over this machinery (`_agg_via_pandas_udfs`); mixed UDF+builtin agg is loud M6 seed.
- `functions.py` / `dataframe.py` / `session.py` — **U8 classic scalar Python `udf`:**
  `@udf` / `udf(fn, returnType)` / `UserDefinedFunction` / `PythonUDFColumn` in
  `functions.py` (decorator/export only for the marker). Bridge:
  `DataFrame._select_with_python_udfs` — **additive** mapInArrow path (does **not** modify
  the U7/M5 pandas_udf bridge; M6 owns that region). Per batch: Arrow → Python scalars
  **per row** → user func → Arrow re-ingest. **Honest cost:** O(rows) Python calls
  (slower than `pandas_udf` by design — Spark parity). Null = Python `None`; returnType
  coercion via declared Arrow type. Composition mid-expression refused. Mix with
  pandas_udf / generators / aggregates refused loud. **`session.udf`**
  (`UDFRegistration`): `register` returns callable; `registerJavaFunction` /
  `registerJavaUDAF` loud no-JVM. **SQL:** registry-name-only structural scan at
  `spark.sql` (never generic `ident(`); SELECT-list simple `udf(col|lit)` rewrite via
  DF bridge (lazy); WHERE/GROUP BY/HAVING/ORDER BY/JOIN/nested → REFUSE LOUD.
  **octo C1:** `_sql_mask_strings_and_comments` (length-preserving) so registry scan +
  clause-keyword bounds + rewrite pass-through checks ignore UDF-name text inside
  string literals / `--` / `/* */` comments; `register` name must be simple SQL
  identifier `[A-Za-z_][A-Za-z0-9_]*`.
  **octo C2:** SQL simple-arg form accepts qualified cols (`t.a` / alias.col);
  nested-subquery refuse message ordered before "outside SELECT list"; decimal
  returnType coerces int/float → Decimal at re-ingest.
  **octo C3:** register case-insensitive overwrite (one key per lower-name);
  SQL hit spans deduped; plan binds UDF at `sql()` time.
  **octo C4:** SELECT-list parse strips comments (preserve strings) so
  `udf /*c*/ (col)` rewrites; join/union-after-udf composition pins.
  **octo C5:** complex pass-through without AS uses expression text as
  column name (UDF + `abs(a)` siblings); VALUES FROM sources.
  **octo C6:** multi-arg null pin; pure `_sql_mask_*` / strip helper unit tests.
  **U9 SQL composition** (`# === r20 U9: sql-udf-rewrite ===`): SELECT-list
  expression-wrapped UDFs (`udf(x)+1`, `CAST(udf(x) AS …)`, nested `f(g(x))`) —
  multi-stage materialize + residual `selectExpr`; true nested subquery =
  `(SELECT|WITH …)` only (not CAST/abs parens); WITH/CTE body rewrite via
  temp views with query-scoped snapshot/restore (no session pollution);
  DISTINCT post-materialization; ORDER BY select-aliases via
  `DataFrame.orderBy` (never leak `__repark_sql_udf_*`, including unaliased
  wrap/nested default names); star (`*`) + UDF refuse-loud; registry scan skips
  `name(*)` engine aggregates (count UDF must not break `count(*)`); set-op +
  UDF refuse-loud; user UDF/analysis errors preserve PySparkException taxonomy
  (not rewrite UOE); ORDER BY NULLS FIRST/LAST refuses loud (DF sort nulls not
  wired end-to-end); Spark optional-AS ``expr alias`` (no AS keyword) on
  SELECT-list items; WITH CTE column-list rename via toDF;
  SORT/DISTRIBUTE/CLUSTER BY/QUALIFY refuse-loud; no-FROM ``SELECT udf(lit)``
  materialization; `udf(returnType=…, useArrow=…)` + duck-typed DataType.
  Octo C1–C8 complete (early_stop=false).
  **U10 / r21 T7** (`# === r21 T7: census-r6 ===`): scalar registered UDF in
  WHERE / GROUP BY (keys-only) / HAVING — peel + materialize + residual
  filter/groupBy; never leak `__repark_sql_udf_*`; JOIN ON / nested subquery /
  ORDER BY expression / GROUP BY+aggregate still refuse-loud. Census-adjacent:
  `DataFrame.isStreaming` always False; `Column.substr`; `F.array_contains` via
  engine `array_has`. **octo C1:** compound WHERE residual base-column identity
  projection (`_sql_where_residual_base_projections`); aggregate HAVING clean
  refuse (no engine plan garbage). **octo C2:** SELECT↔GROUP BY/HAVING UDF call
  match is case+whitespace normalized (`_sql_udf_call_match_key`).
  **octo C3:** WHERE residual with nested SELECT/EXISTS refuses loud
  (`_sql_residual_has_subquery` wired after residual base projection) — no
  engine ParseException. **octo C5:** `spark.udf.register` refuses names
  containing reserved `__repark_sql_udf` materialization prefix.
  **octo C6:** WHERE residual CAST type tokens not identity-projected
  (AS-type skip; type tokens not in static reserved so real columns named
  date/double/string still project — F-E1-2). **EXTRA F-E1-1/F-E1-2:** syntax
  keywords FROM/BOTH/FOR reserved; extract fields before FROM skipped; CASE END
  nesting; type-token/end columns quote-projected; quoted residual idents project.
  **r22 U11 residual poles:** INTERVAL unit tokens after `INTERVAL` literal not
  projected (incl. multi-unit `DAY TO SECOND` / `YEAR TO MONTH` trailing unit
  after `TO` — octo U11 C1); typed `DATE '…'` / `TIMESTAMP '…'` / `TIME '…'`
  constructors not projected (octo U11 C2); columns named `and`/`or`/`not`
  projected under `__repark_sql_udf_wcol_*` temps so `DataFrame.filter` cannot
  case-steal boolean keywords (quoted residual rewritten; never leak temps to
  user schema).

- `udtf.py` / `functions.py` / `session.py` / `catalog.py` — **r23 C6 / U12 UDTF
  scalar-arg phase-2 core** (`# === r23 C6: udtf-phase2-core ===`): `@udtf` /
  `udtf(Handler, returnType=…)` / `UserDefinedTableFunction` / `spark.udtf`
  (`UDTFRegistration`). Handler validation still uses Spark `INVALID_UDTF_*` /
  `CANNOT_REGISTER_UDTF`. **Call** with foldable lit/Python scalars builds a
  DataFrame via synthetic one-row arg frame + `mapInArrow` expansion of
  `eval` iterators. **register** stores on session `_udtf_registry`; SQL
  `SELECT * FROM name(lit_args)` rewrites via `try_sql_registered_udtf` at
  `session.sql` (before scalar-UDF rewrite). Table-factor scan scoped to the
  FROM-clause region (not SELECT-list `, name(` — octo C5-SEC-001); strings/
  comments do not hijack (octo C1-SEC-001). Unclosed SQL string args refuse;
  eval yield width must match returnType (octo C1-L-003). SQL numeric `1e2`
  accepted; trailing commas refuse; `terminate` always runs after construct
  including `start` failure; bare `yield None` refuses (octo C2). **LATERAL** /
  non-literal Column / table-arg / analyze-only stay blocked (U11 seed). Never
  leak `__repark_sql_udf_*`. `functions.udtf` re-export for sed-swap.
  **Census cluster** (`# === r23 C6: census-catalog-udf ===`):
  `Catalog.registerFunction` → `spark.udf.register`; `Catalog.functionExists`
  probes session UDF registry only; `UserDefinedFunction.deterministic` +
  `asNondeterministic` surface flag (default True). Outside QI1/OV1/CACHE1
  regions (catalog `clear_cache` band untouched). Pins: `tests/test_udtf.py`,
  `tests/test_c6_census_r8.py`.
  **octo U11 C1 half-wired guard held:** `F.udf` / `spark.udf.register` /
  `UserDefinedFunction` refuse `UserDefinedTableFunction` at construction.

- `functions.py` / `dataframe.py` — **U7 + M5 `pandas_udf`:** `@pandas_udf(returnType[, functionType])` / `PandasUDFType` export in `functions.py` (decorator + marker; int values match PySpark 4.1.2: SCALAR=200, GROUPED_MAP=201, GROUPED_AGG=202, SCALAR_ITER=204). Bridge: `DataFrame._select_with_pandas_udfs` — intermediate engine `select` of non-UDF projections + UDF inputs (lazy) → deferred mapInArrow. **SCALAR** Series→Series one-pass per batch; **SCALAR_ITER** batch-iterator adapter (`Iterator[Series]→Iterator[Series]`, multi-arg tuple form). **GROUPED_AGG** Series→scalar in `groupBy().agg` (pure form). Composition mid-expression refused. **GROUPED_MAP / window** loud M6 seed. Lazy schema/columns; re-run unless cache; requires `repark[pandas]` at action.
- `dataframe.py` — **U6 / R-APPLYINPANDAS:** `GroupedData.applyInPandas(func, schema)` / `apply_in_pandas` over the production mapInArrow bridge. Plan: engine-side `orderBy(*group_keys)` for key-contiguous Arrow stream (single-node `repartition` is a no-op; full sort is enough) → mapInArrow streams batches → single-pass group boundary scan with boundary-stitch buffering (`_iter_apply_in_pandas_group_tables`, O(largest group + batch); full-stream re-sort/re-group in facade is forbidden) → per-group `pandas.DataFrame` → `func` → re-ingest via C-stream path. Contracts: lazy (schema/columns without running func); re-run on action unless cache; schema DDL/StructType with pandas→Arrow cast to declared schema then mapInArrow validation; user exceptions → PySparkException + traceback; empty input → empty output (no func calls); global `groupBy()` = one group (honestly O(dataset)); cube/rollup/grouping-sets + pivot refused; non-NamedExpression group keys refused (project first). **M5/M6:** `GROUPED_AGG` `pandas_udf` in `groupBy().agg` routes over this machinery (`_agg_via_pandas_udfs`); **mixed** UDF+builtin is M6 two-pass plan-built join (UDF pass + native aggregate + engine join on keys / crossJoin for global).
- `functions.py` / `dataframe.py` — **U7 + M5 + M6 `pandas_udf`:** `@pandas_udf(returnType[, functionType])` / `PandasUDFType` export in `functions.py` (decorator + marker; int values match PySpark 4.1.2: SCALAR=200, GROUPED_MAP=201, GROUPED_AGG=202, SCALAR_ITER=204). Bridge: `DataFrame._select_with_pandas_udfs` — intermediate engine `select` of non-UDF projections + UDF inputs (lazy) → deferred mapInArrow. **SCALAR** Series→Series one-pass per batch; **SCALAR_ITER** batch-iterator adapter (`Iterator[Series]→Iterator[Series]`, multi-arg tuple form). **GROUPED_AGG** Series→scalar in `groupBy().agg` (pure + mixed). **M6/M7 windowed GROUPED_AGG:** `PandasUDFColumn.over(Window.partitionBy(...))` unbounded whole-partition (`_select_with_window_pandas_udfs` — groupBy.agg + join back); **M7** `orderBy` default ROWS UNBOUNDED PRECEDING→CURRENT ROW + duck-typed `_frame_start`/`_frame_end` (G2 owns facade `rowsBetween`; does not edit `window.py`); `functionType=WINDOW` tag still loud. Composition mid-expression refused. **GROUPED_MAP** loud. Lazy schema/columns; re-run unless cache; requires `repark[pandas]` at action.
  **octo harden C1:** `PandasUDFColumn.__bool__` refuse (no and/or/if fail-open);
  dual datatype positionals (`@pandas_udf("long","double")`) loud; returnType validates
  Arrow mapping (no variant/interval/time→string fail-open) + field-list DDL struct refuse;
  bridge uses nullable pandas dtypes for null ints (no float demotion); pass-through schema
  from intermediate analyzed Arrow (not collapsed logical type_keys); generator/aggregate
  UDF inputs refused.
  **octo harden C2:** partition-transform UDF inputs (`years`/`months`/`bucket`/…) refuse
  (no all-null Series fail-open); nested returnType leaves (`array<variant>` /
  `map<string,time>` / `array<struct<a:variant>>`) refuse string fail-open; pass-through
  mapInArrow expected schema keeps intermediate physical Arrow types (timestamp tz);
  mutation pins for aggregate-input / mix-with-aggregate / mix-with-explode refuses.
  **octo harden C3:** null bool/float Series use BooleanDtype/Float*Dtype (C3-Q-001);
  user func `return None` loud `got None` (C3-Q-002); `PandasUDFColumn.__init__` + bridge
  revalidate `return_type_sql` via `_normalize_pandas_udf_return_type_sql` /
  `_pandas_udf_arrow_type_for_return` — hostile constructor / post-build mutation of
  `variant`/`array<variant>` cannot fail-open to string (C3-SEC-001).
  **octo harden C4:** `_normalize_pandas_udf_return_type_sql` stores
  `DataType.simpleString()` (logical identity) not `_data_type_to_sql_type` engine tokens;
  `_select_with_pandas_udfs` patches `_map_bridge["schema"]` with the pre-coerce
  `result_schema` so mapInArrow `struct_type_from_arrow` cannot collapse
  `timestamp_ntz` / `varchar(n)` / `char(n)` on `DataFrame.schema` (C4-Q-001).
  **octo harden C5:** `PandasUDFColumn` Column-parity composition surface complete —
  `__neg__`/`__pow__`/`__rpow__`/`__rmod__`/`__rand__`/`__ror__` refuse UOE (not TypeError);
  composition pin covers mul/mod/over/unary/power/reflected/logical (C5-Q-001 / C5-Q-002).
  **octo harden C6:** pass-through types via `_analyzed_arrow_schema` (native analysis-only
  Arrow C capsule — no `limit(0).to_arrow()` action at select/withColumn); pandas import
  deferred into mapInArrow callback (at action time). Lazy pin tracks `to_arrow` call count
  at plan time + source pin for deferred import (C6-Q-001).
  **octo harden C7:** non-Series UDF returns refuse (no `pd.Series(result)` coerce —
  str/dict char-split silent wrong multiset; C7-Q-001); `PandasUDFColumn` Column methods
  `isNull`/`between`/`when`/`asc`/`__contains__`/string preds/bitwise refuse UOE not
  AttributeError (C7-Q-002); decorator functionType-first + returnType routes through
  `_normalize_pandas_udf_function_type` so GROUPED_* / SCALAR_ITER cannot fail-open to
  SCALAR (C7-L-001).
  **octo harden C8:** dual-datatype refuse excludes functionType-like first positionals
  (`not _is_pandas_udf_function_type(f)`) so string tags `@pandas_udf("GROUPED_AGG","long")`
  / `@pandas_udf("SCALAR","long")` hit the C7 FT-first route (UOE M5 / build) instead of
  misleading PySparkTypeError dual-returnType (C8-Q-001).
  **octo M5 C1–C8:** GROUPED_AGG revalidates returnType (no variant fail-open); mixed
  UDF+builtin refuse order-independent (M5; **M6 ships** two-pass join); WINDOW string
  recognized as functionType (still UOE — use GROUPED_AGG + `.over`); dual SCALAR_ITER
  independent streams; large-group GROUPED_AGG stitch; cube/rollup refuse.
  **M6:** mixed UDF+builtin plan-built join + windowed GROUPED_AGG unbounded partitionBy
  via `_null_safe_equi_join_sql` (`IS NOT DISTINCT FROM` — null keys; octo M6 C1) + select
  alias last-wins / prefer window outs (octo M6 C2); `areaUnderPR` score-group AP (ties
  order-independent, octo M6 C3) + dense-list rawPrediction extract + short-vector refuse
  (octo M6 C4); CrossValidator `parallelism` thread-pool (ctor `<1` refuse).
- **octo U6 applyInPandas (2026-08-01):** `_validate_apply_in_pandas_result_columns` — Spark RESULT_COLUMN_NAMES_MISMATCH class (empty wrong/partial/extra loud; zero-column empty ok; extras not silently dropped) (C1); `_apply_in_pandas_table_from_segments` promotes null→concrete schemas across batch edges (C2); schema-cast overflow/conversion errors surface column-named messages before untyped fallthrough (C3); e2e multi-batch group call-once pin + KeyboardInterrupt not wrapped (C4–C6); multi-key null/empty-string + empty-batch stitch pins (C7–C8).
- `merge.py` — MERGE source registration uses `_native_for_registration` (same empty-placeholder fix).
- **R-DF-BATCH2**: cube/rollup/unpivot/explain/createTempView; loud toJSON/pivot/stat.
- **R-FACADE-HYGIENE (W7):** listTables hides __repark_cdf_* / __repark_mia_*; weakref finalize; fillna one-proj; dropDuplicates row_number; OOS named errors.
- ruff format lockstep (W7 gate).

- **R-FACADE-HYGIENE (W7, lint-clean):** listTables hides __repark_cdf_* / __repark_mia_*; weakref finalize; fillna one-proj; dropDuplicates row_number; OOS named errors.

- **octo mapInArrow C8 (2026-07-31):** mapInPandas `_arrow_func` uses distinct `input_batches` / `output_batches` names so yield-before-consume UDFs still walk real input (C8-L-001).
- **octo mapInArrow C7 (2026-07-31):** `unpersist` clears `_mia_plan_ready`; uncached action rebinds `_inner` when no plan-stable snapshot so post-unpersist plan children cannot reuse a dropped action-ephemeral (C7-Q-001 / C7-L-001).
- **octo mapInArrow C6 (2026-07-31):** mapInPandas empty `to_batches()→[]` synthesizes 0-row RecordBatch so wrong-name/type empty yields stay loud (C6-L-001).
- **octo mapInArrow C5 (2026-07-31):** `_mia_plan_ready` reuses one plan-stable MemTable across plan children (C5-SAF-001); groupBy/agg prepare + value pin (C5-Q-002); upstream input pull-order pin vs collect-all-then-func (C5-Q-001).
- **octo mapInArrow C4 (2026-07-31):** plan-child snapshot keeps parent bridge (filter/select re-run); action vs plan-stable MIA view tracking; show peek pin; set-op/crossJoin try/finally dual register.
- **octo mapInArrow C3 (2026-07-31):** `_native_for_registration` → `_action_inner` (write/temp-view re-run); track-before-sql / drop-on-fail registration; mapInPandas None loud; full-output IPC multi-buffer vs `memory_limit_gb` DEFERRED (Rust streaming register = v2).
- **octo mapInArrow C2 (2026-07-31):** writers/MERGE/selectExpr/alias/sample/set-ops/crossJoin materialize MIA; identity no-ops propagate bridge; cache+prepare keeps bridge for unpersist; hollow traceback pin tightened.
- **combine octo C4 selectExpr (2026-07-31):** `selectExpr` uses `_plan()` (plan-stable) not `_native_for_registration` (action re-run) so post-prepare mapInArrow agrees with `select`/`filter` (C4-L-001).
- **combine octo C5 (2026-07-31):** plan-stable `_plan()` for alias/sample/randomSplit/summary/set-ops/crossJoin/unpivot + cube SQL agg (C5-Q-001/L-001); generator alias/cast sticky aggregate bits (C5-Q-002); unpivot free-SQL quoting (C5-SEC-001); cube/rollup `AS` agg names (C5-L-002).
- **combine octo C6 (2026-07-31):** MIAxexplode non-idempotent call-count (C6-Q-001); polars `_sort_key` generator sticky (C6-Q-002); cube free-SQL no count(Int64(1)) substring rewrite + GroupedData.count structural count(*) (C6-SAF-001); lit NaN/Inf CAST embeds (C6-SAF-002).
- **combine octo C7 (2026-07-31):** `_identity_child` copies `_mia_plan_ready` with bridge so post-prepare repartition/coalesce/hint/offset(0)/toDF peers do not re-snapshot non-idempotent mapInArrow (C7-Q-001); `polars.join` registers `_plan()` (not action `create_or_replace_temp_view`) so post-prepare pl.join agrees with DataFrame.join (C7-Q-002).
  **SE-1 R-C:** `PolarsFrame.join` now `_spawn(planned, right)` so `_tighten_derived`
  ORs the right parent (same as `DataFrame.join`).
- **octo mapInArrow C1 (2026-07-31):** SMALLINT/TINYINT/FLOAT schema widths; upstream close; cache/unpersist re-run; MIA finalize+hide; peek isEmpty/take/show; incremental/mapInPandas pins.

- **R-DF-BATCH2 (lint-clean cov/sampleBy)**: cube/rollup/unpivot/explain/createTempView; loud toJSON/stat
  (pivot is real R-PIVOT — not loud-unsupported).
- **R-POLARS-NS (W6):** Column.str/dt namespaces; fill_null(value); forward/backward OUT.
- **R-POLARS-NS (starts_with/ends_with/contains/substr via call_scalar) (W6):** Column.str/dt namespaces; fill_null(value); forward/backward OUT.

- octo-extra C5 format reflow

- octo-extra C5: concat sql_expr null-propagation guard

- octo-extra C4: cast/concat sql_expr for MERGE

- **octo-extra C3: sql_expr on !=/when/is_null; writer materialize**

- octo-extra C2b: summary bare refuse (E501 docstring reflow)

- **octo-extra C2: summary loud bare; merge Column-only assigns; cache docstring**

- **octo-extra C1 (2026-07-30):** cache take/isEmpty; MERGE table ref; set-ops loud; polars join quote/drop

- **R-POLARS-CORE** (`polars.py` + `DataFrame.pl`): polars-style API; collect lazy-imports real polars. (rider 2026-07-31: sort honors polars null placement via interleaved null-indicator keys — was silently Spark-coupled; zip strict=; combine C7-Q-002: `join` registers `_plan()` not action temp-view)
- **octo-extra C3: to_date/to_timestamp format= refused**

- **octo-extra C2: __all__ exports batch funcs**

- **octo-extra C1 (2026-07-30):** log=ln; from_unixtime string; to_date ColumnOrName; log10 export

- **R-FN-BATCH1**: string/math/null/date scalar wrappers + loud gaps (split/datediff/…).
- **- **R-FN-BATCH4**: stddev/var/corr/covar/bit_*/sha2/rand + loud gaps.

- **R-FN-BATCH3****: datetime/extract/timestamp_* + Chrono≠Java format refusal; loud format_number/utc/make_timestamp.

- **R-FN-BATCH2**: strings/collection census (reverse/repeat/array_*/map_*/size/slice/sequence/…)
  via expanded `call_scalar`; soundex/sentences/arrays_zip/map_from_arrays loud-unsupported.

- R-PERF-VALUES / R-PERF-ARROW-CDF / **P1a C-stream** (2026-08-02): createDataFrame
  materializes once via MemTable; non-empty + typed-empty path builds `pyarrow.Table`
  then `register_arrow_stream_as_temp_view` (no IPC encode/`to_vec`; IPC is version-skew
  fallback only). Mid-path sql-after-register failure drops the orphan `__repark_cdf_*`
  view via `BaseException` (octo C1 SAF-001 / C3; docstring notes interrupt path);
  C-stream runtime errors do not fall back to IPC.
- **P2a CDF extractor** (2026-08-03, region `# === r20 P2a: cdf-extractor ===`): pandas →
  `pa.Table.from_pandas` / polars → `.to_arrow()` with dtype refuse + object-null witnesses
  + int→int64 cast rules, then the P1a C-stream materialize. List/Row/dict still use the
  row-tuple builder. Pins in `tests/test_create_dataframe_materialize.py` (native-path spies).
  **critic-octo C1:** inf refuse + decimal envelope run *before* engine_type cast (typed
  schema no longer skip is_inf; Decimal refuse is PySparkValueError not ArrowInvalid).
  **critic-octo C2:** refuse duplicate pandas column labels before dtype/name lookup;
  positional `iloc` series access; object-null × schema cast failures → PySparkTypeError.
  **critic-octo C4:** empty pandas/polars + typed StructType/DDL → 0-row typed Arrow table
  (name-only empty still CANNOT_INFER_EMPTY_SCHEMA).
- **r21 T1 cdf-ingest** (2026-08-03, region `# === r21 T1: cdf-ingest ===`): dict-list
  **Spark key-union** (`_spark_dict_key_union_order` + `_bind_named_row(allow_missing=True)` —
  sorted first-row keys then append newly seen; null-fill missing; StructType null-fill +
  drop extras). Row lists stay fail-loud. Inferred Boolean/Long/Double/Decimal/Date/
  Timestamp pairwise mix on same column (scalar **or** list-of-scalar elements; map
  values via prepare) → `CANNOT_MERGE_TYPE` refuse (no silent float→int / Decimal→int /
  bool→1.0 / epoch-from-int; critic-octo C1/C2; EXTRA XC1-L1..L4 + XC2-L1..L3; E501 style
  C8). Polars nested
  `List`/`Struct`/`Array` + pandas ArrowDtype list/struct accepted via Arrow path;
  Binary/Time/Duration/dictionary refuse retained. Wrapped `{"Orders":[...]}` guidance on
  `createDataFrame` + multiLine JSON error. Pins: `tests/test_t1_cdf_ingest.py` + updated
  interchange refuse/union pins.
- **r23b N1 nested-dict-struct** (2026-08-04, `# === r23b N1: … ===`): conf
  `spark.sql.pyspark.inferNestedDictAsStruct.enabled` — default flipped to `"true"` in
  `_SQLCONF_DEFAULTS` (owner decision 2026-08-16; DECLARED divergence from PySpark's
  `false`, registry row in the divergence registry; `"false"` restores byte-identical
  PySpark behavior); contextvar `_INFER_NESTED_DICT_AS_STRUCT` (template after
  `_LEGACY_FIRST_ELEMENT_COERCE`) set per `createDataFrame`. Conf true: dict-valued
  *cells* → StructType with multi-row / list-element field union + null-fill (Spark
  SPARK-35929); conf false: MapType byte-identical. Sparse `{size,indices,values}` path
  conf-invariant. Row-dicts (key-union) never consult the conf. **octo C1:** sparse
  reshape exact three-field set only (not any struct with `indices`); None field names
  refuse. **octo C2:** list element type merge under conf true (nested
  `list<list<dict>>` field union); conf truthiness `.strip()`. **octo C3:**
  empty-list → list<null> under conf true; string-wins only over atomics (not
  nested). Pins: `tests/test_n1_nested_dict_struct.py`.
- **R-DF-EASY** (selectExpr/toDF/set-ops/describe/sample/no-ops) + **R-PERF-CACHE** (`storage.py` + `DataFrame.cache`/`persist`/`unpersist`/`localCheckpoint`):
  lazy object-identity MemTable materialize; **r23 CACHE1** cache path uses
  `materialize_as_cache_view` (caller branch; VALUES keeps `materialize_as_temp_view`);
  `clearCache` real drop; StorageLevel cosmetic warn-once; `repark.cache.max_bytes`
  (u64-capped named IAE; builder + conf.set; conf.unset tomb honored);
  localCheckpoint-after-cache reclassifies to `__repark_ckpt_*`; pins in
  `tests/test_cache_persist.py`.
- **R-MERGEINTO** (`merge.py` + `DataFrame.mergeInto`): PySpark 4.0+ builder lowers to SQL
  `MERGE INTO` via a generated `__repark_merge_src_<uuid>` temp view; pins in
  `tests/test_merge_into.py`.

- `types.py` — **E1 (2026-08-05):** `DayTimeIntervalType` / `YearMonthIntervalType` with
  `INVALID_INTERVAL_CASTING` → `PySparkRuntimeError` (both start_field **and** end_field
  membership — pins include bare invalid ints + bad end; octo C7-Q-001); `ClassVar` field
  maps; treeString prints interval ranges.
  **F1 (2026-08-06):** `_merge_type` / `_make_type_verifier` private helpers (Apache
  test_types; class+param keys `CANNOT_MERGE_TYPE` / `FIELD_NOT_NULLABLE_WITH_NAME` /
  `FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME`); compat bootstrap overlays the underscore names.
  **X2 R-CENSUS-TYPES (2026-08-01):** full type constructor surface —
  **G15 (2026-08-12):** `refuse_evaluated_collation` / `refuse_collation_session_key` /
  `collation_refusal_message` — first evaluation of a non-binary `StringType` (and any
  session key containing `collation`) refuses; constructor + `simpleString` stay.
  `StructField.fromJson` applies Spark `metadata.__COLLATIONS` (does not pop-and-forget)
  so createDataFrame cannot silently wrong-count (Q-003 / SEC-001).
  `StringType(collation)`, `ArrayType`/`MapType`/`NullType`/`BinaryType`/`ByteType`/
  `ShortType`/`FloatType`/`CharType`/`VarcharType`/`TimeType`/`TimestampNTZType`/
  `CalendarIntervalType`/`VariantType`; `StructType.add`/`fieldNames`/`toDDL`/
  `treeString`/`fromJson`/`__getitem__`; `DataType.fromDDL`/`json`/`toInternal`/
  `fromInternal`; atomic `jsonValue` is the type-name string (Spark 4); bare
  `varchar` fromDDL → StringType (octo X2 C1 nested engine markers). Prior
  R-PARITY-NITS simpleString/typeName retained.
- `__init__.py` — re-exports `ReparkSession` (primary), `SparkSession` / `ReParkSession` aliases,
  `DataFrame`, `Catalog`, `Column`, `Window`/`WindowSpec`, and the `errors` + `functions` + `ta` +
  `types` modules (the public API) and sets `__version__` (from the installed distribution metadata,
  with a `0.0.0` source-tree fallback; the wheel CI import-smoke prints it).
- `sql/` — **R-SQLALIAS** alias subpackage (`repark.sql` / `repark.sql.types` /
  `repark.sql.functions` / `repark.sql.window`) so `sed 's/pyspark/repark/'` works on multi-import
  scripts. Aliases only (`is` identity to canonical modules); absent pyspark.sql names raise loud.
  See [sql/map.md](sql/map.md).
- `errors.py` — **C4 expand2 (2026-08-03):** `PySparkAssertionError(PySparkException,
  AssertionError)` so Apache `assertDataFrameEqual` / `check_error` isinstance lands after
  the errors overlay (hour-0 FAIL-ERROR-CLASS x5 in `test_utils`). **messageParameters**
  preserve bare `None` values (assert null actual/expected — `str(None)` would break
  check_error equality; typed `dict[str, str | None]`).
- `dataframe.py` — **C4 expand2:** `repartition` first-arg validation (`NOT_COLUMN_OR_STR`
  for list/bool/non int|str|Column — including **sole-arg** list; octo C1-S1-002);
  `repartitionByRange` / `repartitionById` surfaces (Spark errorClass + single-node no-op;
  multi-partition routing + `spark_partition_id` remain seeds); `na.fill` refuses list
  value with `NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR`.
- `errors.py` — **E1 R-CENSUS-ERRORCLASS (2026-08-05):** adds `PySparkRuntimeError` /
  `PySparkNotImplementedError` under the repark `PySparkException` tree (G16); native engine
  leaves get a surface shim (`getCondition`/`getErrorClass`→None, `getMessageParameters`→None,
  empty `getQueryContext`) so `check_error` never AttributeErrors (G4-a). Full native errorClass
  wiring remains OUT.
  **X3 R-CENSUS-DATAFRAME (2026-08-01):** `PySparkTypeError` /
  `PySparkValueError` / `PySparkAttributeError` accept Apache-style `errorClass` /
  `messageParameters` kwargs and expose `getCondition` / `getMessageParameters` /
  `getQueryContext()` (empty contexts seed — deep QueryContext still charter seed).
  **octo X3:** defensive copy + str-coerce of `messageParameters` (C2/C5).
  the PySpark-shaped exception taxonomy (WG-3; U4 added the unsupported class;
  Group X added `IllegalArgumentException` + the Python-argument wrappers),
  mirroring `pyspark.errors`: `PySparkException` (base, subclass of `RuntimeError`) ⊃
  `AnalysisException` (unresolved table / column, plan/type error, iceberg not-found /
  already-exists kinds) ⊃ `ParseException` (SQL / expression syntax error — Group S reparents it
  under `AnalysisException` for PySpark parity, so `except AnalysisException` catches parse errors),
  and `UnsupportedOperationException` (the deterministic scope gates — an unrecognised
  `write.merge.mode`, merge-on-read MERGE on a non-V2 table, a non-Parquet write format; Group R
  retired the transform-partitioned COW MERGE gate and Group Y the merge-on-read one — and
  unsupported iceberg features;
  the PySpark class for a JVM
  `UnsupportedOperationException`), and — **Group X** — `IllegalArgumentException` (an invalid
  `.config(...)` key/value, engine-side `Error::Config` AND the facade's own `_lookup_int`;
  what live pyspark 4.0.0 raises for a bad `SQLConf` value). Iceberg commit/data errors stay the
  base type with the kind
  name leading `str(exc)` (`"CatalogCommitConflicts => …"`). Those five are re-exported **by
  identity** from `repark._native` (an engine-raised error IS the caught class), with `__module__`
  re-homed to `repark.errors`. All subclass `RuntimeError`, so `except RuntimeError` on engine
  failures keeps working (near-drop-in). The engine→type mapping lives in Rust
  (`repark_core::Error::exception_class` → `repark-python` `to_py_err`).
  **Group X also DEFINES (in Python, not the native module) the three Python-argument wrappers**
  `PySparkValueError(PySparkException, ValueError)` / `PySparkTypeError(PySparkException,
  TypeError)` / `PySparkAttributeError(PySparkException, AttributeError)` — exact
  `pyspark.errors` shapes, raised by the facade for a bad argument type/value
  (`df.select(123)`, `df.sort()`, `df.nosuchattr`). They need MULTIPLE bases, which
  `pyo3::create_exception!` cannot express, and no Rust code raises them — hence Python-side.
  Widening only: `except TypeError`/`ValueError`/`AttributeError` still catches, and a migrated
  `except PySparkException` now catches too. Exception-class hierarchy vs Spark is registry
  [FA-3](../../../../docs/spark-sql-iceberg-parity.md#fa-3--python-argument-wrappers-subclass-runtimeerror).
  A leaf type ships only with ≥1 reachable raise (the Group S no-stubs rule).
- `session.py` — **getOrCreate reuse path (R-GETORCREATE, 2026-07-28):** a later builder's
  Combine note: `Builder.config` routes conf/map/kv uniformly through
  `_set_config_entry` (display-key case-insensitive last-wins on every form).
  NEW `spark.sql.catalog.*` names register onto the LIVE session (native
  `register_late_catalogs`; PySpark parity — catalogs instantiate lazily per name);
  already-registered names keep their registration and the warning names them + the
  unapplied keys; a builder whose whole delta was ADDED catalogs does not warn (the block
  folds into the recorded builder config). Pins: `tests/test_getorcreate_catalogs.py`.
  `ReparkSession` (the primary class, 2026-07-12 casing; `SparkSession` stays the
  byte-identical drop-in alias and `ReParkSession` the pre-rename back-compat alias) + the
  `builder…getOrCreate()` chain (with PySpark camelCase aliases `appName` / `getOrCreate`).
  **WU-4:** `getOrCreate` is true get-or-create — a module-level active-session registry returns
  the live session when one exists (warns via `UserWarning` if builder config differs; does not
  rebuild); `stop()` clears the registry and marks the handle stopped (subsequent ops raise
  `RuntimeError` naming the stopped state). `_reset_active_session_for_tests` isolates the
  suite. Also: `sql`, `read_parquet`, **`read_csv` / `read_json`** (R1), `table` /
  `spark.read.table` (multipart identifier only —
  quote-aware segments + double-quoted SQL; backticks accepted; SQL fragments rejected), expanded `DataFrameReader`
  (`.parquet`, **`.csv` / `.json`** (R1), **`.smartCsv`** (r25 T4 repark-extra), `.table`, `.format`/`.load` for parquet+csv+json+iceberg,
  `.option`/`.options` —
  case-insensitive last-wins; `path` applied when load path omitted (load arg beats option);
  empty format is fail-loud (not Spark default-parquet); semantic keys like `pathGlobFilter`
  fail loud on `load`/`parquet`/`table`; **I1 time-travel** reader options `snapshot-id` /
  `as-of-timestamp` / `branch` / `tag` are supported on format('iceberg') (mutual exclusion;
  i64-domain gate before PyO3 — octo C7-Q-001;
  residual denylist: `start-snapshot-id`/`end-snapshot-id` naming incremental read); format names
  case-insensitive; unknown keys tolerated; **R1** `.schema(StructType|DDL)` stores and applies
  on csv/json (name-based when fields match; positional for CSV no-header); **octo R1:** `_cN`
  rename regardless of inferSchema; invalid bools loud; nullValue Utf8-force + promote;
  multiLine JSON mismatch loud (array-only; empty `[]` ok); semantic options on `.csv()`;
  partial schema null-fill; parquet rejects set schema at load; `_testing_create_ref` /
  `_testing_list_snapshots` are **test-support only** (ManageSnapshots fixture seam — product
  SQL CREATE/DROP BRANCH|TAG is I5; seam stays); **T6** `list_iceberg_table_names` /
  `list_temp_view_names` / `list_df_schema_table_names` / `refresh_catalog_provider` +
  `_testing_oob_create_table` / `_testing_oob_drop_table`),
  `createDataFrame` (VALUES-backed; **G-INT**: accepts list of tuples/lists/dicts/`Row`/
  namedtuple/`NamedTuple`, pandas DataFrame, polars DataFrame; cell types
  None/bool/int/float/str/date/datetime/Decimal (pandas `Timestamp`/`datetime64`/`NaT` and
  `numpy.datetime64` — incl. `[ns]` → TIMESTAMP, not epoch int; calendar units `D`/`W`/`M`/`Y`
  (incl. all-null NaT) → DATE — C3-Q-001); **X2:** nested list/dict/Row → Arrow
  list/map/struct; LongType/ArrayType/MapType/StructType schema fields; StringType
  stringifies non-string cells; float-only FixedSizeList ML path kept for DenseVector;
  **octo X2 C1–C5:** nested engine markers use STRING (not VARCHAR) so
  Array/Map/Struct schemas with string fields stay nested Arrow (no silent stringify);
  `_sql_type_to_arrow` fails loud on nested parse; DDL field lists accept nested
  types via `DataType.fromDDL`; map key type from sample keys; tuple→struct positional;
  sparse ML dict exact key set + shapes only;
  never `if not data`
  on a DataFrame — that raised pandas "truth value is ambiguous"; **dict** schema=None uses
  Spark key-union null-fill (r21 T1); **Row** still fail-loud on missing/extra keys;
  `schema=` = list/tuple of str, **StructType**,
  or DDL string `"a INT, b STRING"` (R-PARITY3 — IntegerType/INT → int32, closes G-INT int32
  widening when explicit); refuse non-DDL bare str character-iteration, set/dict (C3-Q-003);
  `schema=[names]` on named sources = pure **reorder by name** or pure **positional rename**
  (disjoint names), length mismatch / partial overlap fail loud — no cross-entry-point value
  swap (C2-L-001); plain tuples positional; namedtuple/`NamedTuple` use `_fields` as source
  names (`schema=None` keeps them; `schema=` reorder-by-name like dict/Row — C3-Q-002 /
  C6-L-001); empty pandas/polars → CANNOT_INFER_EMPTY_SCHEMA; empty list+names /
  untyped pure-`None` all-null → `CAST(NULL AS VARCHAR)` (stable Arrow string, not bare Null —
  C2-L-003); list/dict/Row all-NaN/all-NaT keep DOUBLE/TIMESTAMP/DATE from pre-normalize witnesses
  (C4-L-001 / C3-Q-001 date units); pandas/polars **typed** all-null keep dtype-matched CAST (all integer widths
  → BIGINT so null occupancy cannot flip int32↔int64 — prior C3-Q-001/C4-Q-001; ArrowDtype
  timestamp/date/double recognized — C4-L-002; date/decimal/Datetime arms pinned — C4-Q-003);
  timedelta/Duration/`numpy.timedelta64` refuse even when all-null (C4-Q-002 / C3-L-001);
  pandas Interval refuse before int/float soft-map (C3-L-002); pandas PeriodDtype / Period cells
  refuse (period[D] would soft DATE via endswith `[d]`; period[M] was VARCHAR — C4-Q-002);
  categorical all-null maps via `categories.dtype` so int categories stay BIGINT not VARCHAR
  (C4-Q-003); datetime64[ms|us|ns|s] all-null stay TIMESTAMP (closed-bracket unit match —
  C4-Q-001 pin); datetime64 unit match is **case-sensitive** (`M` month→DATE, `m` minute→
  TIMESTAMP — C5-Q-001 / C5-L-001); complex64/128 dtypes + Python/numpy complex cells refuse
  (C5-Q-002); SparseDtype unwraps to `subtype` so Sparse[int64]/Sparse[bool] all-null stay
  BIGINT/BOOLEAN not VARCHAR (C5-Q-003 / C5-SAF-002); Sparse[object] all-null runs the same
  object-cell NaN/NaT witness as dense object so occupancy cannot flip VARCHAR↔DOUBLE
  (C6-Q-001); object-dtype all-null witnesses raw NaN→DOUBLE / NaT→TIMESTAMP like the list
  path (C5-SAF-001; pure None stays VARCHAR);
  pandas ArrowDtype time/binary/list/struct all-null refuse (C4-Q-004 / C4-L-003);
  polars List/Struct/Binary/Time/Array refuse all-null rather than VARCHAR fail-open (C3-L-003);
  tz-aware datetime/Timestamp → UTC then naive; Decimal fixed-point `format(..., 'f')` into
  DECIMAL(38,18) with envelope refuse for scale>18 or |v|≥10^20 (C2-L-002); tuple path refuses
  str character-iteration / ragged widths; inf float + pandas Timedelta fail loud), the
  `catalog` property,
  `register_memory_catalog`. `.builder` is a descriptor returning a *fresh* builder per access
  (no cross-chain config leakage); `.master(url)` is recorded but ignored (single-node) and warns
  once per process (OTH-010 disclosure). **Group F:** `sparkContext` (minimal `SparkContext` —
  `setLogLevel` silent no-op / `applicationId` stable per-session / `master` from builder;
  other attrs raise `AttributeError` naming the gap) and `version` (`repark-<dist>`, not Spark
  `"4.1.2"` — disclosed).
  `.config(...)` collects into `_config` with the live PySpark 4.1.2 signature
  `config(key=None, value=None, conf=None, *, map=None)` (**R-TAIL**): single kv pair,
  `map={...}` multi-key (PySpark 3.4+; `**dict` unpacking is **not** the API), or duck-typed
  `conf` with `.getAll()`; precedence `conf` > `map` > kv (map+key **and** conf+key together
  do **not** error — conf wins over both; map wins over kv via **exclusive** apply of the
  winning branch only — not merge; **empty** `getAll()` still excludes map **and** kv in the
  same call — C5-Q-002 / C6-Q-001 pins; **empty** `map={}` still excludes kv in the same
  call — C6-Q-001 pin; non-mapping `map` →
  `AttributeError` on `.items()`). Across **sequential** `.config(...)` calls, map and conf
  arms **update-merge** into the existing `_config` (per-key assign; empty map/conf must not
  clear prior keys — C4-Q-001 pin; conf-arm **same-key** overwrite from prior kv/map/conf — C7-Q-001 pin, so `setdefault`/insert-if-missing cannot keep prior values). Map, kv, **and** duck-typed conf values all use Spark
  `to_str` (`_to_str`: bool → `"true"`/`"false"`, `None` stays `None`, else `str(...)`;
  conf arm is load-bearing for duck-typed bool/None — bare raw store would let
  `int(True)==1` pass shuffle validation); native `HashMap<String,String>`
  strips `None` values at the FFI boundary. The whole map then
  flows to the native constructor:
  engine knobs are extracted facade-side (`_lookup_int` → `(key, value)`, then a **per-key-family**
  range check — audit SAF-006, oracle = live PySpark 4.1.2 + the shipped `SQLConf`:
  `_resolve_batch_size` honours Spark's documented "no limit" sentinel
  (`spark.sql.execution.arrow.maxRecordsPerBatch` / `repark.batch.size` `<= 0` is LEGAL — Spark
  declares no `checkValue` — so repark accepts it, leaves the engine knob unset, and warns ONCE per
  process that DataFusion cannot emit unbounded batches);
  `_resolve_shuffle_partitions` mirrors Spark's `checkValue(_ > 0)` on
  `spark.sql.shuffle.partitions` / `repark.target.partitions` — `<= 0` raises
  `IllegalArgumentException` carrying live Spark 4.1.2's message VERBATIM via `_config_value_error`
  (`[INVALID_CONF_VALUE.REQUIREMENT] The value '0' in the config "<key>" is invalid. The value of
  <key> must be positive`). **Recorded deltas vs live 4.1.2:** repark drops the trailing
  `SQLSTATE: 22022` (no repark error carries SQLSTATE), and the repark-native spellings have no
  Spark counterpart so the same shape is emitted with the repark key substituted;
  `_resolve_memory_limit_gb` keeps `0` as the bounded-pool opt-out and refuses negatives (same
  shape) before they reach the native `Option<usize>` (which would raise a bare `OverflowError`).
  All three families resolve **before** the `get_or_create` `_active_session` short-circuit, so an
  out-of-range knob raises — and the batch-sentinel disclosure fires — on the REUSE path too, as
  live PySpark does (`getOrCreate` applies builder options via `setConfString`); the recorded
  divergence is that repark validates but cannot *apply* an engine knob to a live session, so it
  warns "some configuration may not apply" instead. Timing divergence (eager at `getOrCreate` vs
  Spark's lazy first-`sessionState` raise on a fresh process) is disclosed on the user-readable
  `Builder.config` docstring. Pinned end to
  end in `../../tests/test_session_config_knobs.py`), and
  `spark.sql.catalog.<name>.*` / `repark.sql.catalog.<name>.*` blocks (both spellings accepted)
  register the catalog at `getOrCreate` (a malformed block raises there); **R-DISPLAY** facade-only
  `repark.display.style` (`spark` default / `polars` / `duckdb`; key lookup case-insensitive
  **last-write-wins** — C7-Q-001/C7-L-001: `Builder.config` collapses case aliases onto the
  canonical key; resolve scans insertion-order last alias + validates)
  is validated at build, applied on reuse (runtime-mutable; pure style delta does **not** emit
  the engine-knob "may not apply" warning — C6-Q-001; snapshot `_builder_config` synced), and
  exposed as `session.display_style` get/set — kept under the `repark.` prefix so no PySpark
  `spark.*` key collides; **r21 T3:** `conf.set/get("repark.display.style")` drives the live
  session style (no conf-only absorption); **F-T3-001** `conf.unset` tombs the key, resets
  `alive_token` display_style to default `spark`, and `get` honors the tomb (property + show
  lockstep); module `repark.display_style = …` refuses loud;
  default `spark.app.name` is `repark` when WE control the default. All other keys stay tolerated. `create_namespace(catalog, namespace, location=None)` creates a namespace with an
  optional warehouse `location` — settable via SQL `CREATE NAMESPACE … LOCATION`/`WITH DBPROPERTIES` (WG-5) or this programmatic path, so a namespace
  bound for a Glue (RequireExplicitLocation) catalog is created here with its location (ADV-1);
  the engine stores it under BOTH `location` and `location_uri` (the Glue `locationUri` key) and
  reads with a `location_uri` fallback, so pre-existing Glue databases work too (U2 / BUG-001).
- `catalog.py` — `Catalog` (`spark.catalog`): **R-CURCAT-FACADE** current-catalog concept (fast-follow #100: bare-session listTables falls through to temps, never raises; guard flattened SIM102)
  (facade-only; dies with `stop()`; no engine `USE` / bare `SHOW NAMESPACES`). Methods:
  `table_exists`/`tableExists` (3-part with **`spark_catalog` alias** matching
  `resolve_table_name`; **2-part under currentCatalog**; 1-part temp then current db),
  `drop_temp_view`/`dropTempView`, `clear_cache`/`clearCache` (r23 CACHE1: real drop of
  `__repark_cache_*` MemTables + unpersist live handles + orphan prefix drop; fail-loud; Q11),
  `drop_temp_view`/`dropTempView`, `clear_cache`/`clearCache` (no-op; OTH-010;
  CACHE1 sole-writer band — C6 does not edit),
  **r23 C6:** `registerFunction`/`register_function` (alias of `spark.udf.register`)
  + `functionExists`/`function_exists` (session UDF registry probe only),
  `currentCatalog`/`setCurrentCatalog`, `currentDatabase`/`setCurrentDatabase`,
  `listCatalogs`/`listDatabases`/`listTables`/`databaseExists`/`getDatabase` (+ `spark_catalog`
  alias on two-part form for **listTables / databaseExists / tableExists / getDatabase**;
  snake_case aliases). Return shapes: namedtuple `Database`/`Table`/`CatalogMetadata` (live
  4.1.2 field names). **Y-3:** `getDatabase` fills `locationUri`/`description` via
  `DESCRIBE NAMESPACE` (existence + location; no SHOW `_namespace_exists` precheck —
  that walk swallows catalog/IO as absence). FA-2 `listDatabases` still None. Built over
  `SHOW NAMESPACES IN` + **T6 live Iceberg `list_iceberg_table_names`** (list-on-access;
  CQ-008/BUG-007 — not the DF provider snapshot) + **native `list_temp_view_names`** (default
  schema directory; never global `information_schema.tables` — F-T6-PHANTOM-A) + DF schema
  `list_df_schema_table_names` for non-Iceberg permanents. **listTables** globally hides
  engine-private prefixes `__repark_cdf_*` / `__repark_mia_*` / `__repark_tt_*` (I1 time-travel
  static pins — octo C1-Q-002); two-part `spark_catalog.db` aliases like `tableExists` (octo C2-Q-002).
  Since **H-1b (2026-08-11)** the `__repark_tt_*` filter's live subject is the **reader-options**
  registration only (`spark.read.option("snapshot-id", …)`, which keeps its view because that view
  backs the returned frame): the SQL `VERSION AS OF` rewrite releases its pins once the statement
  is planned, so it no longer produces anything for this filter to hide. The filter stays — dropping
  it would expose the reader-options pin — and the test now sources its non-vacuity from that path.
  That sourcing is only sound because the two paths can no longer collide: until the same unit's
  fix pass the engine had TWO minters of the prefix, both counting from 1, so a `VERSION AS OF`
  statement could deregister the very reader-options view this filter hides (engine pin:
  `repark-spark`'s `time_travel_statement_pins_never_collide_with_a_reader_options_view`).
  Optional list* `pattern` uses Spark filterPattern (`*` / `|`;
  Python `re` `\A…\Z` anchors — not Rust `\z`). Non-str args → `PySparkTypeError`. Listing
  divergences rowed as
  [FA-2](../../../../docs/spark-sql-iceberg-parity.md#fa-2--listdatabases-leaves-description-and-locationuri-as-none) /
  [ST-1](../../../../docs/spark-sql-iceberg-parity.md#st-1--show-tables-in-is-unimplemented).
  Pins in `../../tests/test_catalog_surface.py` (incl. isolation + pattern + type pins from
  critic-octo) + `test_time_travel.py` (tt hide).
- `row.py` — PySpark-compatible `Row` for `DataFrame.collect` (**G-ROW**, live PySpark 4.1.2;
  **r21 T3** `from_ordered_fields` preserves Spark-legal duplicate display names on collect;
  **F-T3-002** `__reduce__` pickles via `from_ordered_fields` so multi-name Rows do not drop
  dup values)
  + pattern + type pins from critic-octo) + `test_catalog_staleness.py` (T6 OOB create/drop) +
  `test_time_travel.py` (tt hide).
- `row.py` — PySpark-compatible `Row` for `DataFrame.collect` (**G-ROW**, live PySpark 4.1.2
  oracle 2026-07-27): attribute access (`row.col` → `PySparkAttributeError`
  `[ATTRIBUTE_NOT_SUPPORTED]` on miss); `__getitem__` int (incl. negative; OOB bare
  `IndexError`) / str field / slice → plain `tuple` of values; missing str **and**
  wrong-typed key → `PySparkValueError` (same `__fields__.index` funnel as live
  `pyspark.sql.types.Row` — closes the Group X KeyError/TypeError residuals; no new
  leaves); `__contains__` is field-name membership only; `__fields__` → `list` of names;
  iteration yields values; `asDict(recursive=)` converts nested `Row`/list/dict when
  recursive; value-only equality (incl. vs plain tuples — live `Row` is a `tuple`
  subclass; repark is not, by design); `repr` `Row(a=1, b=2)`; mixed args+kwargs →
  `PySparkValueError` `[CANNOT_SET_TOGETHER]`. Storage uses name-mangled slots
  (`__field_names`/`__field_values`) so user fields `_fields`/`_values` do not shadow
  attr access (octo C1-L-001); a single list/tuple positional arg is one value, not
  unpacked (C1-L-002). **R-PARITY3:** `Row("name","age")` all-str factory form (callable,
  repr `<Row(…)>`, picklable) + value-row pickling. **X2:** empty `Row()` is a factory;
  unnamed value rows + nested empty use angle-bracket repr (`<Row('Alice', 11)>`,
  `Row(a=<Row()>)`); factory arity raises `ValueError`. Pins in `tests/test_row.py` +
  `tests/test_errors.py::test_row_missing_key_and_bad_index_raise_pyspark_value_error` +
  `tests/test_parity3.py` + `tests/test_types_x2_census.py`.
- `storage.py` — **R-PERF-CACHE** `StorageLevel` (PySpark flags + repr; disk/off-heap recorded only).
  **X3:** duck-type `__eq__` so Apache suite `pyspark.storagelevel.StorageLevel` compares equal.
  **r23 CACHE1:** disk/off-heap/replication cosmetic → session-once `UserWarning` on `persist`;
  loud MemTable memory contract + optional `repark.cache.max_bytes` size guard.
  **octo X3 C2:** `__eq__` also swallows TypeError/ValueError → NotImplemented.
- `session.py` — **C3 census expand (additive):** `getActiveSession` / `active` /
  `newSession` (does not steal active; **octo C1** try/finally restores active on
  BaseException) / `__enter__`/`__exit__` / `_sc` alias; `createDataFrame`+`sql`
  promote active; `RuntimeConfig.getAll` / `isModifiable` / static-key `set` refuse /
  `get` without default raises / `set(None)` refuse; getOrCreate reuse folds soft conf
  keys into live RuntimeConfig (skips static keys — octo C2; engine knobs still warn);
  **octo C3:** `conf.unset` tombstones builder-fallback keys; **octo C4:** static keys
  excluded from reuse unapplied warn. **G1 (2026-08-08):** expander region adds
  `UPDATE` / `DELETE FROM` bare targets (sole-writer G1); octo C1: `_update_rest_has_set_clause`
  refuses missing-table SET-keyword eat; residual seeds:
  `_jvm` / `parallelize` / catalog bare-namespace / `sql(args=)` / `test_udf` → U8.
- `session.py` — **X3:** `RuntimeConfig` / `spark.conf` set/get/unset (facade-local; backs
  `sql_conf` + `spark.sql.crossJoin.enabled` join gate). `table(None)` → NOT_STR errorClass.
- `session.py` — **r21 T2 sort-memory (`# === r21 T2: sort-memory ===`):** `RuntimeConfig.set`
  forwards the `datafusion.*` allow-list to the live engine via SQL `SET` (malformed /
  unknown / non-canonical mixed-case, whitespace-padded, or trailing-newline lookalikes →
  `IllegalArgumentException` — never silent store-only; key regex anchors with `\Z` not `$`
  so Python `$`/final-newline cannot accept a twin key — extra-octo T2 E1-1); builder +
  getOrCreate reuse apply the same forward. **One truth for the FairSpillPool:**
  `repark.memory.limit.gb` (build-time only; default 8 GiB; 0 = unbounded; runtime
  `conf.set` refuses loud) vs `datafusion.runtime.memory_limit` (runtime twin) — dual-set
  on the same builder refuses loud. Helpers: `_forward_datafusion_conf`,
  `_looks_like_datafusion_conf_key`, `_refuse_dual_memory_pool_knobs`,
  `_refuse_runtime_memory_limit_gb`, `_apply_builder_datafusion_conf`.
- `merge.py` — **R-MERGEINTO** `MergeIntoWriter` (PySpark 4.0+): clause accumulation
  (`whenMatched` / `whenNotMatched` / `whenNotMatchedBySource` → `updateAll`/`update`/`delete`/
  `insertAll`/`insert`), bare-name equi-join sugar for condition str, `withSchemaEvolution`
  accept-and-record (no engine flag). `merge()` → register temp view → `session.sql` → drop view
  (returns `None`). `whenNotMatchedBySource` renders SQL; engine rejects (disclosed). Column
  assignments use `Column.sql_expr_part()` so `lit("x")` embeds as a SQL string literal.
- `dataframe.py` — `DataFrame` (carries the native session handle that minted it):
  **X3 + octo X3:** `drop(Column)`; `join(on=None)` + `_cross_join_enabled` (runtime conf then
  builder; default true); `_prepare_sample_args` PySpark overload parity + seed-mixed LCG
  (sample + randomSplit); `dropDuplicates([])` → full distinct; explain `extended=True` is
  print-only (not ANALYZE); `df["*"]` star token for `count`.
  **`mergeInto` / `merge_into`** → `MergeIntoWriter` (R-MERGEINTO);
  **R-DF-EASY** methods (selectExpr, toDF, dtypes, printSchema, set ops, crossJoin, offset, alias, describe/summary, replace, sample/randomSplit, colRegex, repartition/coalesce/hint no-ops); **`cache` / `persist` / `unpersist` / `localCheckpoint` / `is_cached` / `storageLevel`** (MIA: unpersist clears `_mia_plan_ready`; C7)
  (R-PERF-CACHE — lazy MemTable; object-identity only);
  `count`, `limit(n)`, `show(n,truncate,vertical)` to **stdout** (default **spark** style =
  PySpark-parity ASCII grid via engine-side limit before collect — byte-stable; opt-in
  `repark.display.style` / `session.display_style` ∈ {`spark`,`polars`,`duckdb`}: polars =
  `shape` + dtype row + first 5/last 5 with `…` (`n` caps keep-set, never enlarges edges past 5);
  duckdb = box-drawing + type row + row-count footer (truncated head+tail; `show(1)` keeps first
  row only with no middle `·` when `tail_n=0` — C8-Q-001; `show(0)` empty keep-set still
  footers `(0 shown)` when total > 0); head+tail uses
  `count()` (extra scan, disclosed) + `limit` head + private
  `_preview_tail_rows` via native `limit_with_skip` when `total > fetch` (short-circuit
  `limit(total)` when `total <= fetch` so skip never goes negative — C7-Q-002) — no full-table
  collect; dtype row from
  precise Arrow field types on the head table (not collapsed `logical_schema_fields`); cells
  spell lowercase `true`/`false`; `truncate` int cap only when `>0` (`0`/negatives = full cell,
  Spark parity — C6-L-001); `show` rejects bool `n` (`PySparkTypeError` — C8-L-001; `bool` is
  an `int` subclass); `show(0)` logs keep-set size 0;
  `vertical=True` renders live Spark `-RECORD i-` layout under spark style (R-PARITY3; OTH-010
  warn-only closed); polars/duckdb styles stay horizontal with a one-shot warn),
  `collect` → `list[Row]`,
  **R-TAIL actions** (live PySpark 4.1.2 oracle 2026-07-28; pins in
  `../../tests/test_dataframe_actions.py`; docstrings ruff-E501/format-clean): `take(n)` →
  `list[Row]` (`limit`+`collect`;
  `n=0`→`[]`; negative → `AnalysisException` `[INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE]`,
  minus SQLSTATE + plan dump; stopped sessions fail loud on `take(0)`/`head(0)` too —
  C5-Q-001, no zero short-circuit before limit/collect); `head()` → `Row|None`, `head(n)` →
  `list[Row]` incl. `head(1)`
  (same negative raise; `bool` rejected like take); `first()` → `head()`; `tail(n)` → last
  `n` rows via full Arrow/`collect` then slice (engine `limit` is head-only — not used;
  PySpark OOM caveat restated verbatim; `_ensure_alive` before the `n<=0`→`[]` short-circuit so
  stopped sessions fail loud on `tail(0)`/`tail(-1)` too; live frame `n<=0`→`[]` including
  negative — Spark does **not** raise on `tail(-1)`); `isEmpty`/`is_empty` →
  `bool` (`limit(1).count()==0`); `collect` builds the Row list via the same batch-wise
  C-stream conversion as `toLocalIterator` (P2b octo C2 — no dual full-Table+list peak;
  **r22 P5:** bulk per-batch `_rows_from_arrow_table` list extend; schema-once identity
  columns skip per-cell map conversion + calendar refuse; columnar `to_pylist`+`zip` →
  `Row.from_ordered_fields`; maps/nested-map still convert; dups positional;
  **octo C1:** schema-aware map item convert so nested empty maps stay `{}` (not `[]`);
  recursive calendar-interval refuse into list/struct/map containers);
  `toLocalIterator`/`to_local_iterator`
  (`prefetchPartitions` accepted, ignored) yields `Row`s via honest O(batch) C-stream
  pull (**P2b** — no longer full `collect` then yield; peak *Arrow* memory O(batch);
  full `list(iterator)` still O(rows) Row objects — octo C1 doc honesty); `to_arrow_batches`/
  `toArrowBatches` (repark extension, disclosed) yields `RecordBatch`es over the same stream
  (empty → one zero-row schema-bearing batch so `Table.from_batches` preserves types —
  octo C1-001); stopped sessions fail loud on
  `isEmpty`/`toLocalIterator`/`first`/bare `head()` too (C6-Q-002 — lifecycle parity with
  take/head/tail stop pins),
  `columns`/`schema` (**metadata-only** via native analyzed schema — no
  `to_arrow`), **Group G1** column-access sugar: `__getattr__` (`df.x` → `Column`; missing →
  `AttributeError` with Spark `[ATTRIBUTE_NOT_SUPPORTED]` text; method precedence /
  **case-sensitive** like PySpark attr (`df.X` fails when col is `x`); existing type dunders
  resolve on type, missing dunder falls through; live PySpark 4.1.2) and `__getitem__`
  (`df["x"]` → Column; **case-insensitive** str resolution keeping the requested spelling
  as a NamedExpression (same display identity as `F.col("X")` — not `x AS X` Alias text
  so compounds/`abs`/agg embeds stay clean; octo r2 C3-L-005), like the Spark analyzer under
  `spark.sql.caseSensitive=false` — exact match first, else single CI match; native bind is
  **quoted** on the canonical field so mixed-case projections stay re-selectable (octo r3
  C3-L-007); zero → eager `AnalysisException`, multiple → ambiguity AnalysisException;
  `df[i]` positional, `df[Column]` → filter, `df[list|tuple]` → select; both dunders
  first-line `_ensure_alive` so held DF after `stop()` prefer-stop),
  `select("*")` (and `"*"` among other args expands), temp-view registration, Arrow interchange `to_arrow` / `to_polars` /
  `to_pandas`/`toPandas` / `to_numpy`, `__arrow_c_stream__`, and transforms: `with_column`/
  `withColumn`, **Group F** plural `with_columns`/`withColumns` (atomic — exprs over existing
  names see the original frame; NEW-name lateral refs raise where Spark resolves them
  order-sensitively, disclosed 2026-07-21 review; live PySpark 4.1.2), `with_columns_renamed`/`withColumnsRenamed` (sequential
  name-list rewrite; duplicate final names raise `AnalysisException` — Spark allows dups,
  disclosed), `transform(func, *args, **kwargs)` (non-DataFrame return → `AssertionError` Spark
  message shape), `filter`/`where` (SQL-string predicates run through
  `_quote_filter_sql_identifiers`: schema idents → double-quoted canonical form; single-quoted
  literals and already-quoted spans untouched; a token followed by `(` is a function call, not a
  column — P5C5-Q-001; `true`/`false`/`null` keep their grammar meaning even against a same-named
  column (`_SQL_LITERAL_KEYWORDS`, all three members pinned); a casefold collision in the frame
  refuses **at the reference** with `AnalysisException` carrying Spark's verbatim
  `[AMBIGUOUS_REFERENCE] Reference \`id\` is ambiguous, could be: [\`id\`, \`ID\`].` message — so an
  unambiguous predicate on the same frame still runs, and the ident is never
  last-write-wins-rebound, P4C5-Q-001 / audit G2. **Recorded message deltas vs live Spark 4.1.2:**
  repark lists the *actual* colliding columns where Spark echoes the reference spelling once per
  candidate, and omits Spark's `SQLSTATE: 42704` suffix (no repark error carries SQLSTATE).
  **Disclosed divergences — the refusal covers the bare SQL-string form ONLY, and the
  protected-span list is NOT exhaustive.** Both are registry rows, not restated here:
  [`docs/spark-sql-iceberg-parity.md`](../../../../docs/spark-sql-iceberg-parity.md) §3 **ID-2**
  (the spellings that bypass the refusal) and §7 **BL-2** (backticks are not a protected span).
  Each row holds repark's behavior, Spark's, its pin in
  `tests/test_filter_predicate_rewrite.py`, and the rationale; both are re-derived nightly by the
  `_live_parity.py` `filter_case_collision_bypasses` / `filter_backtick_identifier`
  disclosures),
  `select` (**Group H**: applies each column's
  `_projection_name` via `Column.for_select()` so compound projections match live PySpark
  names — `(x + 1)`, `negative(x)`, `CASE WHEN …` — never DataFusion `Int64(1)` text; plain
  cast of a named attribute keeps the child name; explicit `.alias` wins; bare
  `select("X")` / `F.col("X")` keep the **requested** spelling (not schema-canonical collapse);
  schema string/`F.col` binds use quoted native ids so `select("X").select("X")` still works
  (octo r3 C3-L-007); residual name sinks after requested-spelling projections also bind/quote
  — `filter`/`where` SQL schema idents, `fillna`/`na.drop`, `dropDuplicates(subset)` via
  groupBy+first, `withColumnRenamed`/`withColumnsRenamed` via bind+select, `GroupedData`
  string/`F.sum("X")` aggs (octo r4 C3-L-008); **duplicate projection names fail loud** with
  `AnalysisException` + `.alias` workaround — DIVERGENCE vs Spark which allows dups), `drop`,
  `order_by`/`orderBy`/`sort`, `join`.
  **Group E** aggregation & set-ops: `group_by`/`groupBy`/`groupby` → `GroupedData` (`.agg` in
  Column-expression AND dict form — dict keys include Group J `collect_list`/`collect_set`;
  reducer names are case-insensitive per live PySpark 4.1.2 (`COLLECT_LIST` ≡ `collect_list`) —
  plus the `.count`/`.sum`/`.avg`/`.mean`/`.min`/`.max` shortcuts;
  each aggregate is aliased to its PySpark output name — `sum(x)`, `sum((x + 1))`,
  `sum(CAST(x AS DOUBLE))`, `sum(x AS y)`, `count`, `avg(x)`, `collect_list(x)`,
  `count(DISTINCT a, b)` — via the Column's `_agg_name` +
  facade `_spark_display` (compound exprs never leak DataFusion's `Int64(1)` rendering); **R3**
  the zero-arg shortcuts (`groupBy(g).sum()` with no column names)
  aggregate EVERY numeric column in schema order INCLUDING the grouping key → `[g, sum(g), sum(x),
  …]` (oracle-verified; string columns excluded), not a `ValueError`), `agg` (global aggregate =
  `groupBy().agg`), `union`/`unionAll` (by position)
  + `union_by_name`/`unionByName(allowMissingColumns)` (by name; a missing-column mismatch raises
  `AnalysisException` unless allowed), `distinct` + `drop_duplicates`/`dropDuplicates(subset)`
  (**R6** subset is a list/tuple — a bare `str` raises a PySpark-shaped `TypeError`, never
  char-iterated; subset path uses groupBy+quoted bind for mixed-case fields),
  `with_column_renamed`/`withColumnRenamed` (bind+select, CI old name), the `na` property
  (`DataFrameNaFunctions`) with `fillna`/`dropna` aliases, and the `write` property
  (`DataFrameWriter`). `GroupedData`,
  `DataFrameNaFunctions`, and `DataFrameWriter` are defined here. `DataFrameNaFunctions.fill` fills
  by type-family (numeric/boolean/string, matched to the value; a dict fills named columns and
  raises `AnalysisException` on an unknown key); **R2** a numeric value filled into an integer column
  keeps that column's exact width and fills the TRUNCATED value (`fillna(2.5)` into a bigint → `2`,
  still bigint — the fill literal is cast to the column's type, not widened to double). `drop` filters
  on non-NULL counts (`how='any'|'all'`, `thresh`, `subset`); `fill`/`drop` `subset` accept a `str`
  (wrapped, not char-iterated), list, or tuple (**R6**). `DataFrameWriter` routes through the engine's
  **existing** SQL paths — CTAS / `INSERT INTO` / `INSERT OVERWRITE` via a throwaway temp view — with
  no new commit machinery: `.format('iceberg')` for table writes (rejects others at `saveAsTable`),
  `.mode(...)` (an unrecognized mode raises `AnalysisException` with Spark's `[INVALID_SAVE_MODE]`
  tag — Group X live oracle: Spark rejects this JVM-side, so it is NOT one of the Python-arg
  `PySpark*Error` wrappers; matches the sibling path-write mode check),
  `.partitionBy(...)` (identity; partition cols `_quote_ident`'d in CTAS),
  `.saveAsTable` / `.insertInto` / empty-overwrite provider wipe all route table names through
  `_sql_table_ref` (C1-SEC-001 — SQL-fragment names raise `AnalysisException`; O3-C4-SEC-001 —
  path-escape `..`/`/`/`\` segments also reject at the identity boundary),
  `.saveAsTable` (**R1** into an existing table; case-insensitive by-name conform via the
  module-level `_by_name_casefold_map` (write surfaces ONLY — every name in a conformed column list
  is a reference, so a casefold collision there refuses for the whole list; the filter rewriter
  deliberately does not use it) — Spark `caseSensitive=false` / audit BUG-007; pin
  `test_parity_save_as_table_append_case_insensitive_by_name`)
  resolves columns BY NAME — projects the source in the target's column order; an extra/missing
  column raises `AnalysisException`, never a silent transpose/drop — unlike positional
  `.insertInto`). **Group I:** `DataFrame.writeTo(table)` → `DataFrameWriterV2` (`using` iceberg
  only; table validated/quoted at `writeTo` via `_sql_table_ref`; `tableProperty` → TBLPROPERTIES;
  `partitionedBy` identity strings/cols `_quote_ident`'d + transform markers
  (`bucket`/`years`/`months`/`days`/`hours`) → CTAS `PARTITIONED BY` end-to-end (Group P);
  `option`/`options` process-once `UserWarning` then ignored (C1-Q-005);
  `create` / `createOrReplace` / `replace`, `append` by-name, `overwritePartitions` = LOUD UnsupportedOperationException (dynamic partition overwrite unavailable — 2026-07-22 review; was static
  whole-table INSERT OVERWRITE disclosed vs Spark dynamic (empty source routes through engine
  `INSERT OVERWRITE` so repark-sql can schema-validate then provider-wipe — C1-Q-001 / C5-Q-001;
  facade no longer short-circuits to bare DELETE),
  `overwrite(condition)` loud
  UnsupportedOperationException); path writes `df.write.parquet|csv|json(path)` /
  `format("parquet"|"csv"|"json").save(path)` via shared **`_apply_path_write`** →
  `COPY TO … STORED AS …` (R1+R2; modes overwrite/error/errorifexists/**append**/**ignore** —
  stage-then-swap for overwrite; pure-Python no-op ignore; append merges staged trees with
  unique part-append names **after column-set + type schema validation** (refuse silent
  null-fill / type-incompatible merge — octo C2-001/C7-002); append onto plain file and
  overwrite of symlink dest → `AnalysisException` not raw OSError (C2-002/C2-003); staging
  must exist before dest removal; empty COPY materializes schema-carrying part so empty
  overwrite cannot wipe-then-fail (**octo:** empty CSV header uses sep; **parquet empty
  materialize uses `rglob` + skips root empty part when hive/`partitionBy` children exist** —
  F-R2-C3-001 no silent null partition keys on `read.parquet(root)`); **R2 partitionBy** →
  DataFusion `COPY … PARTITIONED BY` hive-style dirs (partition cols omitted from data files —
  Spark shape; **duplicate partitionBy cols refuse-loud** C3-002; hive partition discovery on
  *read* residual); csv OPTIONS: header/sep/quote/escape/nullValue/**quoteAll**/`escapeQuotes`/
  compression; **dateFormat/timestampFormat refuse-loud** (strftime vs SimpleDateFormat);
  parquet **compression** wired (snappy/gzip/zstd/lz4/none); json compression + date/ts refuse;
  orc/other still `DATA_SOURCE_NOT_FOUND`; directory shape differs from
  Spark `part-*`/`_SUCCESS` — disclosed); writer SQL binds the temp view by callable (never
  `str.format` over user path/property text — braces/`{view}` are literal);
  `sortWithinPartitions` = orderBy path (single-node = one partition). The Arrow
  export is **streaming** (native `__arrow_c_stream__` pulls batches lazily, O(one batch)); `to_arrow`
  is the single funnel all eager materializers route through, and it re-raises a mid-stream engine
  execution error (surfaced by pyarrow as an `ArrowException`) as the base `repark.errors.PySparkException`
  (a `RuntimeError`) so the near-drop-in error contract holds — plan-time parse/analysis errors surface
  earlier, already classified, and pass through unchanged. **P2b** adds `to_arrow_batches` (lazy
  batch iterator; empty → one zero-row schema batch) + honest streaming `toLocalIterator`
  (Arrow peak O(batch); full Row list still O(rows)); region banner
  `# === r20 P2b: action/export ===`. **r21 T2 export ERROR path** (`# === r21 T2: sort-memory ===`):
  `_export_engine_error` / `_export_error_message` re-raise mid-stream `ArrowException` as
  clean `PySparkException` with the DataFusion message (strip pyarrow "dynamically evaluated
  source" noise) + REPARK conf hint naming `repark.memory.limit.gb` /
  `datafusion.runtime.memory_limit` on FairSpillPool pressure (ExternalSorter /
  SortPreservingMergeExec / Resources exhausted — marker set includes both operators).
  T3 owns export NAMING on the same surface — different helpers.
  `# === r20 P2b: action/export ===`. **r21 T3 export NAMING:** H1 multi-name display overlay at
  the Arrow boundary (`_apply_export_display_names` on `to_arrow` / `to_arrow_batches`; collect
  builds Rows via positional `Row.from_ordered_fields` so Spark-legal dup display names survive);
  banner `# === r21 T3: ux-polish ===`.
- `column.py` — `Column`: wraps a native `PyColumn`; **H2** `spark_wrap_display_part` collapses
  user `.alias("v")` (`… AS v`) when embedded in outer displays so `round`/`abs`/binary/cast
  show `round(v, 2)` not `round((…) AS v, 2)`; aggregate args still use `spark_display_part`
  (`sum(x AS y)`). Operators (`+ - * / %`, `== != < > <=
  >=`, `& | ~`, plus reflected arithmetic), **Group G1** unary `__neg__` (`-col` → value via
  `lit(0)-self`, display `negative(x)`, native expr aliased so `select(-df.x).columns ==
  ['negative(x)']` and `F.sum(-df.x)` → `sum(negative(x))`; double → `negative(negative(x))`)
  and PySpark-style `__repr__` (`Column<'…'>`), the PySpark misuse guards (`__bool__` and
  `__contains__` raise — `and`/`or`/`not`/`if`/`in` on a Column must fail loudly, never silently
  drop a predicate), **E1** `alias(*names, metadata=)` (`ONLY_ALLOWED_FOR_SINGLE_COLUMN` when
  multi-name + metadata), `__getitem__` (slice step → `PySparkValueError`/`SLICE_WITH_STEP`;
  open-bound slices raise classic `substr` type errors — no invented start=1/length=start
  defaults — octo C3-L-001/L-002; closed int/Column bounds → `call_scalar("substr")`
  embedding owned Spark `substring_udf` (pos 0 ≡ 1; not DF built-in — octo C7-L-001);
  int → 0-based
  `array_element`/`__repark_array_get__` element extract — no fail-open parent; str →
  native `get_field` + double-quoted free-SQL ident (struct fields **and** map[str] keys
  — pin `test_column_getitem_map_str_key_extracts_value`, octo C4-Q-001) — octo
  C1-L-001/L-002/Q-002/SEC-001; Column/other keys → polymorphic
  `getitem`/`__repark_get_item__` (array 0-based or map-by-key) — never parent `_inner` —
  octo C2-L-001),
  and `__iter__` → `NOT_ITERABLE` (so getitem does not make Columns iterable),
  `cast` (accepts a `types` object or a type
  string), the ordering markers `asc`/`desc` (read by `orderBy`), `over` (apply a
  `WindowSpec` to a window-function column), and **r21 T3** `Column.round(scale)` repark-extra
  (delegates to `F.round`; D2 catalog row #31). `Scalar` = the auto-`lit` RHS union. The `_agg_name`
  slot (Group E) carries an aggregate column's PySpark default output name (`sum(x)`, `count`), which
  `GroupedData.agg` applies as the alias; any other operator (including `.alias`) resets it to `None`
  so an explicit `.alias(...)` overrides the default. **R-SELECT-GLOBAL-AGG / octo C1:** sticky
  `_is_aggregate` (OR on binary; preserve on `alias`/`cast`/unary/null/`when`) so composed
  aggregates still classify as global-agg; `_is_foldable` on `F.lit` and pure-literal ops so
  `select(sum(x), lit(1))` is not `[MISSING_GROUP_BY]`. The `_spark_display` slot tracks PySpark-style
  expression fragments for compound aggregate args (`(x + 1)`, `CAST(x AS DOUBLE)`, `x AS y`,
  `(x > 0)`, `(NOT (x = 10))`, `((a) AND (b))` / `OR`, `(x IS NULL)` / `IS NOT NULL`,
  `coalesce(...)`, `CASE WHEN … END`, `negative(x)`). **R-MERGEINTO** adds optional `_sql_expr` /
  `sql_expr_part()` so SQL-embedding surfaces (MERGE assignments) quote string literals correctly
  while display names stay unquoted (Spark projection parity). **Group H** adds `_projection_name` (select
  boundary alias text) and `_stable_name` (bare col / user alias — cast keeps the child name in
  select while agg still embeds `CAST(...)`); `Column.for_select()` always applies the projection
  alias when set (bare refs keep requested spelling — live Spark; octo C3-L-001/002).
  String `lit` display is unquoted (live Spark: `first(z)`, `concat(s, z)`). Reflected `+`/`*`
  commute in the display (`2 * x` → `(x * 2)`, per live PySpark; `-`/`/` keep operand order);
  float literals keep the point (`2.0`, never `2`) — both from the 2026-07-21 octo review, which
  also cleared the unit's ruff debt (`__all__` sort incl. `abs`). Group I: optional
  `_partition_transform` marker on `F.years`/`months`/`days`/`hours` Columns (sticky across
  `.alias`, binary/unary ops, null checks, `cast`, `asc`/`desc`, `when`/`otherwise` arms —
  rejected in `select`/`filter`/`orderBy`/`withColumn`/`groupBy`/`join`/`GroupedData.agg`).
- `functions.py` — **E1 error-class pre-checks:** `bucket` `NOT_COLUMN_OR_INT`; `greatest`/`least`
  `WRONG_NUM_COLUMNS`; type-check stubs for `from_csv`/`from_xml`/`schema_of_*`/`json_tuple`/
  `raise_error` (happy path loud-unsupported). **R-FN-BATCH1** + **R-FN-BATCH2** scalar/collection
  wrappers via `call_scalar`
  (+ loud unsupported for engine gaps; ledger `task/fn-batch2-ledger.md`); **Q1**
  `percentile_approx` / `approx_percentile` ship over `approx_percentile_cont` (facade
  accuracy accepted-and-ignored — fact lives in the `percentile_approx` docstring; free-SQL
  3rd arg = t-digest centroids not GK accuracy; array percentages STOP seed; bool percentage
  rejected).
  **X3:** `struct(...)` via native `make_struct` + `count` star Column forms (`"*"`,
  `col("*")`, `df["*"]`); **octo X3 C4** field names via named_struct path.
- `functions.py` — the `pyspark.sql.functions` surface (FN-SPLIT 2026-08-15: constructors +
  first-half agg/date stay here; `functions_udf.py` holds pandas/python UDF;
  `functions.py` re-exports UDF helpers the DataFrame/session bridges import
  (`_build_python_udf`, `_normalize_pandas_udf_*`); `functions_expr.py` holds
  `_scalar` wrappers). **FN-A (2026-08-15):** 25 ordering/null/math names in
  `functions_expr.py` (sign/ifnull/nvl/asc/desc/asc_nulls_first/desc_nulls_last/e/pi/
  negative/positive/pmod/expm1/ln/log2/log1p/degrees/radians/nvl2/nullif/equal_null/
  zeroifnull/nullifzero/isnotnull/cbrt). Deferred: typeof/bround/conv (charter);
  asc_nulls_last/desc_nulls_first (`_sort_specs` couples nulls to ascending).
  **FN-GT1 shipped** rint/factorial/bin/hex/unhex.
  Pins: `tests/test_functions_a.py`. **Z-4 / Y-5 SAF-001:**
  `_scalar` wrappers). **FN-B (2026-08-15):** 21 string names in
  `functions_expr.py` (lcase/ucase/char/char_length/character_length/substring/substr/
  left/right/contains/like/ilike/regexp_like/rlike/regexp/btrim/startswith/endswith/
  printf/replace/quote). Deferred: regexp_extract_all/regexp_substr (charter);
  to_char/to_varchar (`call_scalar` allow-list — no crates/ edit).
  **FN-GT1 shipped** split_part/regexp_count/regexp_instr/bit_length/octet_length.
  Pins: `tests/test_functions_b.py`.
- `functions_window.py` — **FN-W (2026-08-15):** 5 window names (`lag` /
  `lead` / `nth_value` / `percent_rank` / `cume_dist`) over DF 54.1 UDWFs via
  `PyColumn.window_udwf` (no IntegerType cast). Python owns PySpark signatures;
  `ignoreNulls` is an honest cut. Pins: `tests/test_functions_w.py`.
- `functions_agg.py` — **FN-C (2026-08-15):** 8 aggregate aliases/shims
  (`first_value`/`last_value`/`std`/`count_if`/`bool_and`/`every`/`bool_or`/`some`).
  Deferred: `lag`/`lead`/`nth_value`/`percent_rank`/`cume_dist` (A8: PyColumn
  window surface is closed — **FN-W ships these**); `sum_distinct`/`sumDistinct` /
  `approx_count_distinct`/`approxCountDistinct` (no native distinct-on-sum /
  approx-distinct arm; `count_aggregate` distinct is count-only); charter
  ENGINE-WORK `any_value`/`max_by`/`min_by`/`product`/`grouping`/`grouping_id`/
  `percentile`/`window`/`window_time`/`session_window`. Pins:
  `tests/test_functions_c.py`.
  **FN-D (2026-08-15):** 11 datetime names in `functions_datetime.py` (day/curdate/now/
  dateadd/datepart/to_unix_timestamp/unix_date/unix_seconds/unix_millis/
  date_from_unix_date/current_timezone). Deferred at FN-D write (GT2 shipped
  make_date/make_interval/make_dt_interval/unix_micros/date_diff):
  localtimestamp/to_timestamp_ntz; charter ENGINE-WORK
  make_timestamp_ltz/ntz, make_ym_interval, to_timestamp_ltz, convert_timezone,
  timestamp_add/diff. Pins: `tests/test_functions_d.py`.
  **G6-3 rider (2026-08-15):** `unix_date` now builds the ENGINE's `unix_date`
  (`_scalar("unix_date", …)` over the new `call_scalar` arm) instead of the
  `.cast("date").cast("int")` chain it used to spell. Spark refuses `CAST(DATE AS INT)` at
  analysis and repark does too now (registry row G6-3), and the refusal's own message names
  `UNIX_DATE` as the remedy — so the remedy must not be spelled as the refused cast. The leading
  `.cast("date")` stays: it is what lets a string / timestamp column reach a function whose
  signature is an exact DATE.
  **FN-E (2026-08-15):** 9 collection names in `functions_collections.py`
  (cardinality/array_size/array_agg/named_struct/map_contains_key/array_append/
  array_prepend/arrays_overlap/get). Deferred at FN-E write (GT2 shipped
  map_from_entries/shuffle/array_compact/element_at): create_map
  (`call_scalar` allow-list); charter higher-order/JSON/
  generators. Pins: `tests/test_functions_e.py`.
  **FN-GT2 (2026-08-17):** leftover THIN-WIRE datetime/collections/url/bitmap
  — 18 names. Wrappers in `functions_datetime.py` / `functions_collections.py`
  / `functions_url.py`. Binder arms in `column/function_dispatch.rs`.
  **X-round (2026-08-18):** ColumnOrName parity — `parse_url` / `try_parse_url`
  dropped the force-lit on `partToExtract` / `key` (a bare `str` is a COLUMN
  NAME, PySpark 4.1.2), `get`'s `index` likewise (only a bare `int` is wrapped),
  and `url_encode` / `url_decode` / `try_url_decode` renamed the parameter
  `col` → `str` (PySpark's spelling; positional calls unaffected).
  `shuffle(col, seed)` wires the Spark 4.0 seed. `element_at` / `make_date`
  docstrings now STATE the ANSI-class NULL divergence. Ledger:
  `task/fn-gt2-ledger.md` (X-round).
  **Rework (2026-08-17):** W1 `element_at` treats a `str` extraction as a
  literal key; W2 interval
  `str` parts are column names; W3 `unix_micros` casts timestamp first; W4
  regex `str_to_map`; W5 non-UTC pins. `bitmap_*` moved to
  `functions_bitwise.py`.
  `datediff` DISPOSED-STUB untouched. `element_at` is 1-based (contrast `get`).
  **FN-GT1 (2026-08-17):** leftover THIN-WIRE math/string/bitwise/utf8 — 18
  names + `getbit` alias. Math wrappers in new `functions_math.py`; bitwise
  leftovers in `functions_bitwise.py`; string/utf8 leftovers in
  `functions_expr.py`. Binder arms in `column/function_dispatch.rs`.
  **GT1-FIX (2026-08-18):** ColumnOrName wiring (G1–G8), oracle pins (P1–P5),
  docstring examples (F3), `lit_indices` sweep. **Round-2 (2026-08-19):**
  door-side `regexp_*` / `split_part` kernels; omitted `regexp_instr` idx
  projects `, 0`; UTF-16 empty-count / ASCII ``\\d``; partNum 0 fail-loud.
  Ledger: `task/fn-gt1-ledger.md`.
  **FN-F (2026-08-15):** 10 try/session/bitwise names in `functions_bitwise.py` +
  `functions_session.py` (bitwise_not/bitwiseNOT, broadcast, current_user/user,
  current_catalog/current_database/current_schema, version, uuid). Deferred:
  remaining try_* / to_number / to_binary (charter); assert_true (`raise_error` is
  construction-time UOE). **FN-GT1 shipped** bit_count/getbit/shift*. Pins:
  `tests/test_functions_f.py`.
  **Z-4 / Y-5 SAF-001:**
  `_thread_origin` copies `_origin_plan_id` / `_origin_field` (and wrappers set
  `join_sql_expr`) through `abs`, `_scalar`, `_date_fn`, `coalesce`, `concat`,
  `add_months`, `date_add` so `F.abs(right["k"])` after a semi join raises Spark's
  `MISSING_ATTRIBUTES` instead of binding left. **W-4 / A6 Q-002:** the same thread
  on `sum`, `count` (non-star), `avg`, `min`, `max`, `count_distinct`, `first`,
  `last`. `column.py` is closed. WG1 in-use set: `col` (stable_name),
  `lit`, `expr` (column-free SQL only; infix fragments parenthesize for projection like Spark
  `(1 + 1)`), `coalesce`/`concat` (with `_spark_display` + projection; carry Group I
  `_partition_transform` when any arg is marked),
  `current_timestamp` (+ `currentTimestamp`; Group F: Arrow `timestamp[us, tz=UTC]`; projection
  name `current_timestamp()`, not DataFusion `now()`). WG2
  added `row_number` and the 13 date functions (`year`, `month`, `quarter`, `weekofyear`,
  `dayofweek` (1=Sun), `dayofmonth`, `dayofyear`, `last_day`, `add_months`, `date_add`,
  `date_format`, `trunc`, `date_trunc` (format-first)) — each coerces a name→`col` / an int→`lit`
  and (**Group H**) carries Spark projection display (`year(d)`, `add_months(d, 1)`, …), plus
  Group I `_partition_transform` from the date arg (octo r2).
  **Group I / Group P:** `weekday` (0=Monday..6=Sunday) plus partition transforms `years`/
  `months`/`days`/`hours` and `bucket(numBuckets, col)` (valid **only** inside
  `writeTo(...).partitionedBy`; elsewhere → `PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY`).
  Group P: these now work end-to-end — the fragment renders into CTAS `PARTITIONED BY (bucket(4,
  "id"), years("d"), …)` and the engine builds the real Iceberg transform (computed-mode fork
  splitter); the former "CTAS transform gate remains loud" disclosure is retired. `bucket` requires
  a positive int `numBuckets` (loud `TypeError` otherwise; the engine also rejects `<= 0` at parse
  time). Transform identity args are `_quote_ident`'d inside the fragment (`years("col")` /
  `bucket(4, "col")` — C3-SEC-001 residual of C1-SEC-001 identity quoting).
  **Group E** aggregate functions (shadowing the builtins `sum`/`min`/`max`/`count`, as PySpark
  does): `sum`, `count` (`count("*")`→`count(1)` counts rows, `count(col)` skips NULLs),
  `count_distinct`/`countDistinct` (multi-col form supported end-to-end — Group J), `avg`,
  `mean` (= `avg`), `min`, `max`, `first`/`last` (with PySpark's `ignorenulls`), and **Group J**
  `collect_list`/`collect_set` (NULL elements excluded; empty group → `[]`; order nondeterministic —
  pin sorted contents; snake_case only, no camelCase aliases per live PySpark 4.1.2 inspect) —
  each returns a Column carrying its Spark output name in `_agg_name` (verified against real
  PySpark 4.1.2) and carries `_partition_transform` from the input arg (Group I). Facade `abs`
  (CASE-based) for nested-name pins like `sum(abs(x))` (also carries the marker).
- `window.py` — `Window` / `WindowSpec` (`partitionBy`/`orderBy`, immutable), consumed by
  `Column.over`; WG2's one window pattern (`row_number().over(Window.partitionBy(...).orderBy(...))`).
  Group I: rejects `F.years`/`months`/`days`/`hours` in partition/order keys (octo r3 — same
  `PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY` as DataFrame entry points).
- `ml/` — `repark.ml` pipeline skeleton + feature transformers + native estimators +
  M4 tuning (`ParamGridBuilder`/`CrossValidator`) + delegated `ml/ext/` backends
  behind optional `repark[ml-ext]` (R-ML-SKELETON M1 / R-ML-FEATURE M2 /
  R-ML-ESTIMATORS M3 / R-ML-BOOST M4). See [ml/map.md](ml/map.md). Feature `fit` = session
  queries; estimator `fit` = multi-pass Rust Arrow stream (params only); `transform` = plan
  only; no `pyspark.ml` alias shim tonight. Vectors: dense FixedSizeList / sparse struct
  (layout home: `ml/linalg.py`); persistence repark-ml v1 (`pipeline.py` format constants).
- `ta.py` — the `repark.ta` technical-analysis surface (T1b + T2 batches 1–2 + WG2–WG5 + T3 +
  TA-4 volume +
  **G-NAN**; module doc truth-up 2026-08-15: plan-fusion claim corrected to post-N2 behavior;
  `__all__` gap closed 2026-08-17 — `wma` was defined but unexported, F-4 finding, now listed
  and pinned by a completeness test): the
  full 81
  indicator entry points (`ema`/`sma`/`rsi`/`adx`/`atr`/`trange`/`var`/`stddev`/`linearreg`+`_slope`/
  `_intercept`/`_angle`/`tsf`/`correl`/`min`/`max`/`sum`, the WG1 overlap-MA family `wma`/`dema`/
  `tema`/`trima`/`kama`/`t3`/`midpoint`/`midprice`, the split `bbands_upper`/`_middle`/`_lower`,
  the WG2 simple-momentum batch `mom`/`roc`/`rocp`/`rocr`/`rocr100`/`willr`/`cci`/`cmo`/`bop`/
  `apo`/`ppo`/`aroon_down`/`aroon_up`/`aroonosc`/`trix`/`ultosc`, the WG3 directional + MACD
  families `dx`/`adxr`/`plus_di`/`minus_di`/`plus_dm`/`minus_dm`/split `macd`+`_signal`+`_hist`/
  `macdfix`+`_signal`+`_hist`/`macdext`+`_signal`+`_hist`/the `ma` selector, the WG4 split
  stochastics `stoch_slowk`/`_slowd`/`stochf_fastk`/`_fastd`/`stochrsi_fastk`/`_fastd`, and the WG5
  sweep-up `natr` (H/L/C) / `beta` (two-series) + the no-period O/H/L/C price transforms
  `avgprice`/`medprice`/`typprice`/`wclprice`, and the T3 parked four — the split `mama`/`fama`
  (real-valued `fastlimit`/`slowlimit`), `sar`/`sarext` (H/L, real-valued accelerations; SAREXT's
  short-side output is negative), and `mavp` whose second argument is a per-row `periods` **series**
  — column-or-name — clamped to `[minperiod, maxperiod]`, and the TA-4 volume four `ad`/`adosc`
  (H/L/C/V; `fastperiod`/`slowperiod` slots) / `obv` (close+volume; first output is first volume) /
  `mfi` (H/L/C/V, `timeperiod`)) over the
  `repark-ta` kernels — `min`/`max`/`sum` carry uppercase `MIN`/`MAX`/`SUM` TA-Lib-name aliases.
  Each returns a
  **window-function** `Column` (series column-or-name first, TA-Lib-named kwargs
  `timeperiod`/`nbdev`/`nbdevup`/`nbdevdn`/`vfactor`/`fastperiod`/`slowperiod`/`signalperiod`/
  `matype`/`fastmatype`/`slowmatype`/`signalmatype`/`timeperiod1..3`/`fastk_period`/`slowk_period`/
  `slowk_matype`/`slowd_period`/`slowd_matype`/`fastd_period`/`fastd_matype` with TA-Lib defaults;
  `bop` is four-series O/H/L/C, `willr`/`cci`/`ultosc`/`dx`/`adxr`/`plus_di`/`minus_di` and the
  `stoch`/`stochf` splits are H/L/C, `aroon_*`/`aroonosc`/`plus_dm`/`minus_dm` are H/L,
  `apo`/`ppo`/`ma`/`macdext*` and the stochastic smoothing legs carry MA-type codes
  **0..=8 incl. MAMA (7)** — all six `stoch_*`/`stochf_*`/`stochrsi_*` facades document that
  domain in their docstrings); the caller supplies ordering
  with `.over(Window.orderBy(...))`. Built on the native
  `PyColumn.ta_window`, so SQL and DataFrame routes are one kernel.
  **G-NAN (`null_lookback`):** every wrapper accepts keyword-only `null_lookback: bool = False`.
  Default leaves the kernel NaN lookback prefix (existing `to_bits` goldens byte-unchanged). With
  `True`, a `_NullLookbackColumn` wraps `.over(w)` as
  `when(row_number().over(w) > lookback, ta_result.over(w))` so only the deterministic prefix
  (length from the kernel's TA-Lib lookback formula — never blanket `isnan`) becomes SQL NULL;
  mid-series NaN is preserved. Python-side projection only; Rust kernels still emit NaN.
  Lookback formulas match the kernels including MACD period-swap
  (`_macd_lookback` = `(max(fast,slow)-1)+(signal-1)`), ULTOSC `max(t1,t2,t3)`, and MA-type
  table / compound stoch sums via `_ma_lookback`.
  **r21 T4 ta-etl:** `ta.over_columns(window, {name: bare_ta_col, …})` → dict for a single
  `DataFrame.withColumns` (same-spec windows fuse to one DataFusion `WindowAggExec`). Measurement
  WIN — kernel work dominates; see the private v1 repository's `t4-ta-etl-ledger.md` (v1-era ledger, never ported). Region banner `# === r21 T4: ta-etl ===`.
  **r23b N2 plan-collapse:** adjacent independent same-spec `withColumn`/`withColumns` chains now
  merge into one `WindowAggExec` (sticky layer meta + structural WindowSpec equality; dep on a
  prior-layer name keeps stacking; cache/persist marks block merge — octo C2). Alias-chain squash
  in `select` projection build collapses identity `x AS x AS x` re-aliases. Pins live with
  `python/repark/tests/test_n2_plan_collapse.py` (no unit ledger was filed for this surface).
  **r25 T3 plan-hygiene:** extends `_collapse_identity_projection_alias` only (Q7 — no second path)
  to peel nested native `Alias` chains via `PyColumn.collapse_identity_aliases` before the N2
  for_select gate; operator 17-TA chain plan/value-parity pins. See
  `task/t3-plan-hygiene-ledger.md`.
  **conductor-13 TA-2:** `ta.with_indicators(df, *, partition, order, columns,
  null_lookback=False, last_row=False)` — serving helper. `partition` and `order` are required
  keyword-only (no guessed column names). A missing `partitionBy` is the silent cross-symbol RSI
  footgun (one global series across symbols that share timestamps); the helper exists so ETL
  cannot forget it. Builds the fused `over_columns` window from existing plan pieces only
  (window + `row_number`/`max`); `last_row=True` keeps the last TA-window bar per partition;
  `null_lookback` threads through `_NullLookbackColumn`. Pins:
  `tests/test_ta_with_indicators.py`. Ledger: `task/ta2-with-indicators-ledger.md`.
- `types.py` — the seven Spark cast type objects (`StringType`, `IntegerType`, `DoubleType`,
  `BooleanType`, `DateType`, `TimestampType`, `DecimalType(p, s)`) → canonical engine type strings
  the native `cast` parses. Plain classes (not Pydantic) to keep PySpark's positional constructors.
- `py.typed` — marks the package as typed.

## I want to...

| ...do this | go to |
|---|---|
| Quote a SQL identifier / reject path-escape segments | `_idents.py` (r23 QI1 SSOT; never local copies) |
| Add a session entry point | `session.py` (`ReparkSession` / `Builder`) |
| Smart CSV / inference protocol (r25 T4) | `_csv_smart.py` + `DataFrameReader.smartCsv` + `DataFrame.describe_ingest` |
| Add / change Excel reader (`read.excel` / `sheet_names`) | `session.py` `DataFrameReader.excel` + `read_excel` (r25 T5) |
| Add / change `DataFrame.mergeInto` builder | `merge.py` + `dataframe.py` (`mergeInto`) |
| Add / change cache/persist/StorageLevel | `storage.py` + `dataframe.py` (R-PERF-CACHE) |
| Add a DataFrame action, interchange, or transform | `dataframe.py` |
| Add `df.x` / `df["x"]` column-access sugar (Group G1) | `dataframe.py` (`__getattr__` / `__getitem__`) |
| Add a groupBy/agg, union, distinct, na, or write method | `dataframe.py` (`GroupedData` / `DataFrameNaFunctions` / `DataFrameWriter`) |
| Add a column operator / `__neg__` / `alias` / `__getitem__` / `cast` / `over` | `column.py` |
| Add a `functions` (`col`/`lit`/date/window/aggregate) function | `functions.py` (re-export + `__all__`) / `functions_expr.py` / `functions_agg.py` / `functions_window.py` / `functions_udf.py` |
| Add a `functions` (`col`/`lit`/date/window/aggregate) function | `functions.py` (re-export + `__all__`) / `functions_expr.py` / `functions_udf.py` / `functions_datetime.py` (FN-D) |
| Add a `functions` (`col`/`lit`/date/window/aggregate) function | `functions.py` (re-export + `__all__`) / `functions_expr.py` / `functions_udf.py` / `functions_collections.py` |
| Add a `functions` (`col`/`lit`/date/window/aggregate) function | `functions.py` (re-export + `__all__`) / `functions_expr.py` / `functions_bitwise.py` / `functions_math.py` / `functions_datetime.py` / `functions_collections.py` / `functions_url.py` / `functions_session.py` / `functions_udf.py` |
| Add a window builder (`Window`/`WindowSpec`) method | `window.py` |
| Add a TA indicator (`repark.ta`) | `ta.py` (+ the kernel + UDF in `repark-ta`) |
| Add / change the TA serving helper (`with_indicators`) | `ta.py` (TA-2; required `partition`/`order`) |
| Add ML pipeline / feature / estimator (`repark.ml`) | [ml/map.md](ml/map.md) + [docs/design/python-facade.md](../../../../docs/design/python-facade.md) §4 Q3 + `crates/repark-ml` |
| Add a Spark cast type object | `types.py` |
| Add / change an exception type (the error taxonomy) | `errors.py` (+ the `create_exception!` block and `to_py_err` in `crates/repark-python/src/lib.rs`) |
| Change `Row` (collect result / field access / asDict) | `row.py` (G-ROW pins in `tests/test_row.py`) |
| Add a metadata (`spark.catalog`) operation | `catalog.py` |

## Pointers

- Up: [../../map.md](../../map.md)
- Native module: [../../../../crates/repark-python/map.md](../../../../crates/repark-python/map.md).

## Debug

- **TA-2 `with_indicators`:** omitting `partition` is a `TypeError` (keyword-only, no
  default). Empty `partition=[]` is `PySparkTypeError` naming the cross-symbol RSI
  footgun. Do not add a guessed `"symbol"` / `"ts"` default. `null_lookback` rewrites
  through `_NullLookbackColumn` only (lookback-by-length, never `isnan`).

- **r25 T4 smartCsv:** diagnostics live on `df.describe_ingest()` (sticky `_ingest_report`);
  plain `.csv` returns `{}`. Inference is pure-Python protocol (`_csv_smart`); casts go through
  engine `Column.cast`. Default `.csv` path must stay r20-R1 identical — never route default
  through smart.

- Empty path-overwrite reads back `[]` (not an error) because `_apply_path_write` writes a
  schema-only `part-00000.{parquet,csv,json}` when COPY produced no files (directory staging
  only — single-file COPY targets are left untouched) — DF54's COPY writes nothing
  for zero rows and its reader refuses empty directories (DF52 tolerated both); Spark's
  contract is a schema-carrying part file.

- Holistic UDF slate combine (`review/grok-udf-slate-combine`): select routes generators → global-agg → projection; generator checks before with_column/filter returns.

- `mapInArrow` schema mismatch / user-func exceptions → `PySparkException` with field/type + traceback text; bridge re-runs each action unless `.cache()`; after `unpersist` re-runs again and clears `_mia_plan_ready` so plan children rematerialize (C7-Q-001 / C7-L-001); writers/SQL registration use `_native_for_registration` (empty placeholder is never written); identity no-ops keep `_map_bridge` + `_mia_plan_ready` (combine C7-Q-001); `polars.join` plan-stable (combine C7-Q-002); `listTables` hides `__repark_mia_*`; plan children reuse one plan-stable snapshot (`_mia_plan_ready`); groupBy/agg prepare guards empty placeholder; mapInPandas empty wrong-schema yields must raise (not silent `[]` — C6-L-001).
(2026-07-31 R-EXPLODE-REWRITE octo c1: `explode`/`explode_outer` use `_column_argument` (str→col);
`Column.cast` keeps `_generator` + `_generator_cast` for element CAST after unnest;
`withColumn` routes generators via `withColumns`→select rewrite; `_array_element_sql_type`
exact field bind only; `sql_expr_without_alias` strips pre-aliased AS from unnest SQL.)
(2026-07-31 R-EXPLODE-REWRITE octo c2: `_sql_embed_expr_fragment` quotes array/sibling idents;
siblings use `sql_expr_without_alias` (no double-AS); element type parse Timestamp/nested +
casefold field bind (no BIGINT fail-open on bound unknown); `asc`/`desc` sticky generator.)
(2026-07-31 R-EXPLODE-REWRITE octo c3: `_select_with_generator` two-phase — native project
siblings+array temp then SQL unnest by quoted idents only; `_array_element_sql_type` only on
explode_outer and fail-loud (no BIGINT); `Column.cast` on generator keeps array `_inner`.)
(2026-07-31 R-EXPLODE-REWRITE octo c4: explode*/posexplode* on `functions.__all__`;
`_reject_nested_generator` on Column ops; cast type allowlist + phase-2 re-validate.)
(2026-07-31 R-EXPLODE-REWRITE octo c5: `_scalar`/coalesce/concat/when + explode* input
reject generators; chained generator cast composes; dup-name preflight before generator
branch; Decimal(p,s) parse in `_arrow_debug_type_to_sql`.)
(2026-07-31 R-EXPLODE-REWRITE octo c6: `_aggregate_argument` rejects generators; filter /
`_sort_specs` / groupBy / GroupedData.agg reject generators; empty guards use
`array_length` not multi-dim `cardinality`.)
(2026-07-31 R-EXPLODE-REWRITE octo c7: `_date_fn`/add_months/date_add/date_format/trunc/
date_trunc + `.dt` reject generators; `_window_column` rejects on Window.partitionBy/
orderBy; `_grouping_sets_grouped` + `_agg_via_sql_group` reject generators.)

(2026-07-23 review: ruff-format reflow in dataframe.py/session.py alongside the audit-fix gate cleanup.)
(2026-07-24 SEC-008: `DataFrame.show()` logs only a row-count breadcrumb at INFO; the full rendered
table — which can carry row data / PII — moved to DEBUG.)
(2026-07-28 R-DISPLAY: opt-in `repark.display.style` / `session.display_style` for polars/duckdb
`show()` renderings; default spark path byte-unchanged. Pins in `tests/test_display_styles.py`.)
(2026-07-28 R-DISPLAY harden C8: duckdb/polars `use_ellipsis = tail_n > 0` so `show(1)` has no
bare middle dots; `show(n)` rejects bool via `PySparkTypeError`.)


| Symptom | First check |
|---|---|
| `ModuleNotFoundError: repark._native` | Build the native module: `uv run maturin develop` |
| `ImportError` on `to_polars` | install the `polars` extra (`pip install 'repark[polars]'`) |
| `ImportError` on `to_pandas` / `toPandas` | install the `pandas` extra (`pip install 'repark[pandas]'`) |
| `ImportError` on `to_numpy` | install the `numpy` extra (`pip install 'repark[numpy]'`) — pyarrow>=18 no longer bundles numpy |

First checks: `import repark` after `maturin develop`. Escalate to: [../../map.md#debug](../../map.md).

<!-- 2026-07-14: lint-pass doc touch for staged CTAS / metadata schema -->

<!-- dogfood Group F: withColumns/transform/sparkContext surface 2026-07-21 -->

- Group F dogfood gaps: `withColumns`/`withColumnsRenamed`/`transform`, session `sparkContext`/`version` — pins in `tests/test_dogfood_gaps.py`.

- ACC remediation: F6 tick-identity LTZ pin (Q-001); withColumns TypeError before alias (Q-002).

- Octo r2 C1: config spark.master OTH-010 warn; held SparkContext stop; withColumns str keys; F1 near-now + F.expr ns residual pins.
- Octo r2 C2–C5: master warn on getOrCreate reuse; SQL CTAS ns residual fail pin; empty rename map.

- Octo r3 C1: DF liveness token on stop; empty col names rejected; case-insensitive spark.master warn; cast(TimestampType) keeps µs+UTC (TZ-4 PR-2).
- Octo r3 C2–C5: insertInto/stop gate; singular empty names; CTAS near-now value; columns/schema/transform after stop.

- Group G1 (2026-07-21): `DataFrame.__getattr__` / `__getitem__`, `Column.__neg__` + `__repr__` —
  pins in `tests/test_column_access.py` (live PySpark 4.1.2 oracle). R2 Half B: getitem str
  case-insensitive (Spark analyzer); getattr stays case-sensitive.

- **PG2:** `DataFrameReader.jdbc` + `format(postgres|jdbc|postgresql)` / `read_postgres`.
- **r25 T5 excel:** `DataFrameReader.excel` / `sheet_names` + `ReparkSession.read_excel` /
  `excel_sheet_names` (disclosed extension; pure-Rust calamine via `_native`; single-sheet v1).
  Semantic option refuse on load (octo C4). Pins: `tests/test_excel_reader.py` (10 cases).
  Ledger: `task/t5-excel-ledger.md`.

- **r22 A2 octo C4:** `session.py` jdbc docstring — SEC-001 default `prefer` + warn fallback
  (purge r21 `prefer≡disable` lie).

- **skeptic fix:** jdbc dbtable-from-props; PG4 registered-catalog MERGE USING pg.*

- **octo c1:** drop secret-theater redaction on jdbc/load.

- **octo c7:** remove no-op except re-raise on jdbc/load_postgres.

- **octo c8:** indent fix after except cleanup.

- **octo-extra c1:** _parse_jdbc_int_option for partition bounds.
<!--(pg combine rider: pinned-ruff format pass) 2026-08-01 -->

- 2026-08-01 style rider: `session.py` reformatted (ruff format at tip).

<!-- 2026-08-02: r16 combine rider — fillna numeric family widened to Byte/Short/Long/Float (X2 LongType split had silently skipped bigint columns); _normalize_subset error_class per-surface (dropna NOT_LIST_OR_STR_OR_TUPLE per 4.1.2 oracle); functions __all__ completed (+27 names — the overlay __all__ change had let median et al fall through to real pyspark) -->

- **E2 / R-CENSUS-READWRITER (2026-08-02):** bare-name resolution layer —
  `resolve_table_name` / `_sql_table_ref_resolved` / `spark.sql.defaultNamespace` seed;
  `session.sql` expands bare `DROP TABLE`; writers (`saveAsTable` / `insertInto` /
  `writeTo` / MERGE) + `table()` qualify under current catalog/NS; `spark_catalog`
  alias (including `catalog.tableExists` / `databaseExists` / `listTables`); memory catalog does
  **not** auto-create current NS (callers / compat harness seed it). `writeTo` /
  MERGE re-resolve bare names at action time (not frozen at construction).
  `F.lit(ndarray)` typed arrays + `UNSUPPORTED_NUMPY_ARRAY_SCALAR` / `COLUMN_IN_LIST`;
  path `save` unsupported → `DATA_SOURCE_NOT_FOUND`. Native `arrow_type_key` List →
  `array<element>` for dtypes.
  **octo C1 Fixer:** tableExists/databaseExists `spark_catalog` alias; action-time
  writeTo/MERGE resolve; DROP non-rewrite + insertInto/MERGE e2e pins.
  **octo C2 Fixer:** `_join_table_identifier_segments` quote-aware rejoin (C2-SEC-001 —
  dotted quoted segments survive resolve→`_sql_table_ref`); `read_iceberg_table` +
  TT `DataFrameReader.table` bare/spark_catalog resolve (C2-Q-001); `listTables`
  two-part `spark_catalog` alias (C2-Q-002); e2e temp-view prefer via `table()`
  (C2-Q-003).
  **octo C3 Fixer:** path-save unsupported pin requires `DATA_SOURCE_NOT_FOUND`
  (C3-Q-001; not format-name-only OR); native `arrow_type_key` List recursion
  depth-bounded (C3-CRATE-001 — see `crates/repark-python/src`).
  **octo C4 Fixer:** `format("iceberg").load` catalog-only via `read_iceberg_table`
  (`prefer_temp_view=False` — C4-L-001; `spark.table` still prefers temps);
  Writer `.csv`/`.json` raise `DATA_SOURCE_NOT_FOUND` like `format().save` (C4-Q-002) —
  **superseded by R1** (csv/json wired; orc keeps the shape).
  **octo C6 Fixer:** `F.lit(ndarray)` refuses object / `|S` (bytes) with
  `UNSUPPORTED_NUMPY_ARRAY_SCALAR` (C6-Q-001 — no fail-open `array<string>`).

<!-- 2026-08-03: R-AUTO-MEMCAT — bare getOrCreate auto-registers session-scoped spark_catalog memory catalog + default ns (duckdb :memory: analogue); opt-out repark.sql.autoMemoryCatalog=false; temp warehouse dies on stop(); auto never blocks user intent (register flip + spark_catalog alias treat auto-only as absent, sticky flag) -->

<!-- 2026-08-06: F1 R-CENSUS-R3-EC — free-SQL expander Path A (DF default_catalog probe abandoned:
  temp views need datafusion.public; Iceberg schema refuses MemTable with data).
  `_expand_bare_table_names_in_sql` now also rewrites INSERT / CREATE TABLE / MERGE INTO /
  SELECT|WITH FROM+JOIN via `resolve_table_name` SSOT (statement-prefix / structural scan;
  no body regex). Auto-memory sticky flag unchanged.
  Hardening: paren-aware FROM (skip EXTRACT(YEAR FROM col)); CTE names not qualified;
  CREATE/INSERT identifier scan (bare_ctas); two-part TT expands under current catalog.
  True-EC: array.array typecodes; MonthDayNano collect → PySparkNotImplementedError.
  **octo C1:** nested WITH CTE recollect; comma-join FROM lists; INSERT INTO TABLE +
  DIRECTORY path-insert skip; SQL comment + leading-trivia awareness.
  **octo C2:** FROM (subq), bare sibling lists; MERGE USING subquery body expand;
  TABLESAMPLE before comma.
  **octo C3:** FROM ONLY prefix; IntegerType verifier rejects bool/float/str.
  **octo C4:** non-recursive WITH body scope (prior CTEs only; RECURSIVE self bare).
  **octo C5:** comma-list skips comments; CREATE VIEW AS body FROM expand.
  **octo C6:** TABLESAMPLE BERNOULLI/SYSTEM before comma. -->

<!-- 2026-08-08: G1 R-CENSUS-R4 — UPDATE/DELETE free-SQL bare targets via same expander SSOT
  (`_try_expand_update_sql` / `_try_expand_delete_sql`; identifier scan; WHERE-subquery FROM
  via existing walker; never-regex SET body). DataFrame.stat is a **property**; corr/cov/
  crosstab/sampleBy/approxQuantile implement TOP FAIL-MISSING family (Apache corr/cov/
  crosstab/approxQuantile PASS; sampleBy seed-count residual). Group H attempt: join
  SubqueryAlias both sides when name-clash/self (equi-join + self on=name); residual
  condition join needs relation-qualified Expr::Column lineage (DataFusion DFSchema +
  Join resolution).
  **octo C1:** sampleBy fraction [0,1]+NaN refuse; approxQuantile relativeError type/range;
  join alias only on name intersect/self; UPDATE table `*_set` pin; refuse eating SET
  keyword as table (`_update_rest_has_set_clause`).
  **octo C2:** relativeError NaN refuse; probability domain → PySparkValueError.
  **octo C3:** join(on=[]) uses crossJoin gate (no silent cartesian). -->

<!-- 2026-08-03: R-AUTO-MEMCAT style rider — SIM102 if-combine (session.py knob scan), formatter pass on session/catalog -->

<!-- 2026-08-08: r19 combine rider — sql docstring deduped after U8/G1 keep-both -->

<!-- 2026-08-08: r19 combine rider — formatter pass on dataframe.py (keep-both spacing) -->

<!-- 2026-08-09: r20 G2 R-CENSUS-R5 — Window.rowsBetween/rangeBetween + Column.over frames;
  F.rank/dense_rank/ntile; F.rand/randn Spark XORShift (seeded); sampleBy seed counts MATCH;
  eagerEval `__repr__`/`_repr_html_` (showString packing + conf truncate/maxNumRows).
  Region banners `# === r20 G2: window/rand/sampleBy ===` in window.py / functions.py /
  dataframe sampleBy+eagerEval. (Residual CLOSED 2026-08-12 by registry row TZ-5:
  `timestamp.cast("long")` was the raw DataFusion tick — µs for a `createDataFrame` column, ns
  for a `to_timestamp` literal — and is now Spark's epoch SECONDS. The rangeBetween
  moving-average pin no longer divides by 1e6; it spells Spark's own expression.
  See `task/tz5-cast-seconds-ledger.md`.)
  **octo C1:** `_repr_html_` html.escape cells+headers (XSS); RANGE value-offset non-numeric
  ORDER BY → SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE; RANGE without ORDER BY →
  RANGE_FRAME_WITHOUT_ORDER; finite float frame bounds refuse; sampleBy exact XORShift key
  pin (not band); randn(0) first-value pin; unseeded rand seed-0 honesty; seed bool refuse.
  **octo C2:** RANGE value-offset multi-ORDER BY → RANGE_FRAME_MULTI_ORDER; ranking
  functions without ORDER BY loud (not DF Internal); row_number spark_display.
  **octo C3:** inverted frame bounds (start>end) refuse at rowsBetween/rangeBetween.
  **octo gates:** ruff SIM103/C416 on RANGE dtype helper; XORShift test line wrap.
  **TZ-5 follow-on (2026-08-12):** the non-numeric ORDER BY guard resolves the key by NAME, and a
  CAST chain keeps its BASE column's projection name (`col("d").cast("long")` still projects as
  `d`) — so it read the SOURCE dtype and refused a numeric key Spark accepts. `Column.over` now
  treats a key as "bare" only when its spark display EQUALS its projection name; an expression
  falls to the display branch, matches no schema field, and the engine stays the authority on its
  type. Unreachable until the TZ-5 cast fix let the moving-average pin drop the `/1e6` wrapper
  that was hiding it. Pinned both ways in `test_g2_window_rand_sampleby.py`
  (`..._refuses_non_numeric_order` / `..._accepts_a_cast_numeric_order_key`). -->
- M7 format/lint gate clean (ruff format + py-lint).

<!-- 2026-08-03 (r20 combine): dataframe.py select() projection reconstructed after H1×G2 keep-both — G2 range-order validation in the input loop + H1 deferred-for_select rebind projection. -->

<!-- 2026-08-03 (r20 combine rider 2): M7 windowed pandas_udf duck-typing aligned to G2's landed WindowSpec — frame presence = value-not-None (attrs now always declared), RANGE refuse keys on _frame_units, JVM-long unbounded sentinels map back to unbounded. functions.py + dataframe.py. -->

<!-- 2026-08-03 (r21 combine rider 3): T1 numeric-merge refuse is now conf-aware — spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled=true restores Spark's legacy first-element truncate-coerce (contextvar through the nested cell walk); default stays CANNOT_MERGE_TYPE refuse. -->

<!-- 2026-08-03 (r21 combine): unused Row import removed from rider wrapper. -->
<!-- (formatter pass same rider) -->

<!-- 2026-08-03 (r22 combine rider): generator (explode) selects keep the loud duplicate-name refuse — H2 multi-name overlay does not reach the unnest SQL rewrite (r23 seed); pin message updated. -->

<!-- 2026-08-04 (r24 combine rider): unknown cast tokens raise ParseException (an
  AnalysisException) instead of bare ValueError — live PySpark 4.1.2 raises ParseException and
  errors.py already documents that mapping, so `except AnalysisException` now catches a bad cast
  as it does on Spark. The allowlist (injection control) is unchanged; only the class moved. -->

<!-- 2026-08-04 (r24 combine rider): DataFrameWriter path writes register their COPY staging
  target as a trusted local write root (SEC-02 gate is free-SQL-scoped) — fixed 31 writer-test
  failures the unit gates could not see. -->
- r25 morning critic fixes: native alias-peel handler in dataframe.py narrowed
  (AttributeError = unavailable; engine failures debug-logged, behavior preserved); dead
  `cleaned_text` second copy removed from `_csv_smart.py`.

- r26 octo C1: smartCsv samplingRows also via DataFrameReader.option map
- octo C3 commit 2026-08-05T23:29:11Z: order-independent decimal union
- **F-3 (2026-08-17) docstring backfill:** every public `def`/`class` in this directory now
  carries a docstring — `functions_udf.py` (37: the `PandasUDFColumn` / `PythonUDFColumn`
  composition-refuse stubs, each naming the surface it refuses), `polars.py` (30), `merge.py`
  (3: the `WhenNotMatchedBySource` terminals), plus singles in `functions.py`, `row.py` and
  `types.py`. The `polars.py` `.str` / `.dt` texts state the repark-specific contract measured
  against the built engine, not polars': `replace` replaces EVERY match (so it equals
  `replace_all`), `zfill` is a plain `lpad` with no sign handling, `weekday` uses Spark's
  0=Monday..6=Sunday, and `truncate` takes a Spark `date_trunc` granularity (not `"1mo"`).
  These are polars-only differences, so they stay docstring-local — the divergence registry
  takes rows only for pinned Spark differences. Docstring-only; no signature or logic moved.
- `_temp_views.py` — **SQM round 7 (R7-1):** the temp-view SPELLING seam. `scratch_view_name`
  mints an INTERNAL scratch-view name already spelled against the session's temp-view home
  (always quoted — call sites must not re-quote it), so mint / every `FROM`-and-qualifier use /
  drop all carry one home-pinned string; `home_view_ref` gives the home spelling of an EXISTING
  user-named one-part view (`DataFrame.alias`); `local_view_name` goes back to the one-part name
  for prefix checks and name-only APIs. Reason: a bare reference inside a SQL body is resolved by
  DataFusion against the LIVE `datafusion.catalog.default_catalog`, so under a raw `SET` every
  facade path that minted a view then scanned it by bare name missed while
  `catalog.tableExists` (which asks the home) said True. Callers updated across `dataframe/`,
  `ml/`, `merge.py`, `polars.py`, `session/`. **Boundary (round-8 residual, disclosed):** this
  seam spells only the FACADE's own views. The ENGINE crates register their own scratch relations
  under bare names — `repark-iceberg`'s MERGE and identity-`UPDATE`/`DELETE` tables and the
  `__repark_tt_*` time-travel view — so `DataFrame.mergeInto(...).merge()`, `UPDATE`/`DELETE …
  IN (SELECT …)` and `VERSION AS OF` stay RED under such a `SET`. MEASURED equally red on the
  round-7 BASE; see `task/se1-declared-sorted-ledger.md` (round 7, "NOT-RUN").
