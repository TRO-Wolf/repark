# map — repark-python/src

## Purpose

Source for `repark-python` — the PyO3 cdylib (`_native` module). The only crate with `unsafe`. See
[../map.md](../map.md). Q1 re-home (2026-08-14): `PyReparkSession.native()` builds a
non-Spark (DataFusion dialect) session for the Python `repark.sql()` ANSI callable.

## Contents
- `allocator.rs` — conductor-19 AL-1a: `#[global_allocator]` mimalloc static, compiled
  only under `allocator-mimalloc` (default off). Funding / default-off rustdoc lives
  here. `lib.rs` carries a two-line `#[cfg]` `mod` hook so the 190-line root ceiling
  stands.
- `tests.rs` — binder unit tests (r26 LR1 hoist). **EC-1 type-identity guard:** four `const _`
  coercions plus `repark_core_error_is_the_repark_common_error_type` pin that `repark_core::Error`
  / `ErrorClass` ARE `repark_common::Error` / `ErrorClass` (re-exported, not redefined) — the
  binding's textually-unchanged `repark_core::Error` lines resolve through a different crate than
  at the port pin, so the identity is mechanized rather than remembered (design §3 EC-1).

- `ml.rs` — M3 native estimator fit binder (`fit_linear_regression` / `fit_logistic_regression` /
  `fit_kmeans`): streams `execute_stream` batches into `repark-ml` accumulators; params-only results.
  KMeans init indexes **non-null** feature rows only (null feature rows skipped, not PRNG-addressable).
  `discover_feature_width_and_count` for logistic; `max_iter==0` still reports `num_rows` (octo C2).
  Empty list features → width-0 dense row (intercept-only OLS; octo C3).
  **M7 octo C2 densify disclosure:** sparse VectorUDT `{size,indices,values}` Struct features refuse
  with message naming densify / `sparseOutput=False` (native path does not densify).

- **R-POLARS-NS:** call_scalar starts_with/ends_with/contains/substr for .str namespace.

- **octo-extra C1 (2026-07-30):** call_scalar log→ln; from_unixtime→to_char

- **R-FN-BATCH4:** aggregate stddev/var/median/corr/covar/bit_* + sha256/random.
- **r20 G2:** `Column.over` accepts optional ROWS/RANGE frame + AggregateFunction→window;
  `rank`/`dense_rank`/`ntile` (Int32 cast); `call_scalar` `rand`/`randn` → repark-functions
  XORShift UDFs (seeded).
- **FN-W (2026-08-15):** `column/window.rs` `window_udwf` (no IntegerType cast) +
  `lag`/`lead`/`nth_value`/`percent_rank`/`cume_dist` over DF 54.1 UDWFs.
  `percent_rank`/`cume_dist` stay Float64; offset windows preserve input type.
  `ignoreNulls` not wired. **COLX (2026-08-15):** `column.rs` → `column/`
  (`mod.rs` pymethods; helpers in `window.rs` / `expr_build.rs`).

- **Q1 R-ML-QUANTILE:** `PyColumn::approx_percentile_cont(percentile: f64)` — native
  AggregateUDF call for facade `percentile_approx` / `approx_percentile` (groupBy/df.agg).

- **R-FN-BATCH3:** next_day/hour/minute/second/date_part/timestamp_*.

- **R-FN-BATCH2:** call_scalar strings/collections (reverse, array_*, map_*, size, slice,
  sequence, elt 1-based→0-based, …) via functions + functions_nested expr_fn.
- **r24 SB1 / SEC-01:** `call_scalar` `array_repeat` / `sequence` / `repeat` refuse planner-visible
  literal expansions over `repark.sql.maxArrayElements` (default 10_000_000) via
  `repark_functions::cardinality::refuse_facade_literal_expansion` → `AnalysisException`.
- **r21 T7 census-r6:** call_scalar `array_contains`/`array_has` → `nested_fn::array_has`
  (Spark `F.array_contains` facade).
- **E1 octo C1 Fixer:** call_scalar `array_element` → owned `__repark_array_get__` (Spark
  0-based Column[i]); `get_field` → DF core get_field (struct field extract).
- **E1 octo C2 Fixer:** call_scalar `getitem` → `__repark_get_item__` polymorphic array
  0-based / map-by-key (Column/other keys never fail-open to parent).
- **E1 octo C7 Fixer:** call_scalar `substr`/`substring` → owned
  `repark_functions::string::substring_udf()` (not DF `expr_fn::substr`/`substring`);
  closes Column.__getitem__ slice / 3-arg pos-0/`negative` divergence (C7-L-001).
- **r22 C5 census-r7:** `PyColumn::try_cast` → DataFusion `Expr::TryCast` (null on cast
  failure; facade `Column.try_cast` / SQL `TRY_CAST` display). Cargo.toml freeze held.
- **r24 A3 QUAL-03:** `column/expr_build.rs` `parse_data_type` full facade cast vocab (`float`/`byte`/
  `short`/`binary` + `tinyint`/`smallint`); residual → `AnalysisException` (not `ValueError`);
  test renamed `parse_data_type_maps_facade_primitive_cast_vocabulary` (rule 11).
  **TZ-4 PR-2:** `"timestamp"` → `Timestamp(µs, UTC)`; `"timestamp_ntz"` → naive µs.
  `dataframe.rs` `simpleString` distinguishes `timestamp` vs `timestamp_ntz`.
- **r25 T3 plan-hygiene:** `column/expr_build.rs` `collapse_identity_alias_chain` +
  `PyColumn.collapse_identity_aliases` peels nested `Alias` chains to one outer rename (facade
  N2 collapse path only — greylit Q7); unit `collapse_identity_alias_chain_peels_same_name_stack`.
- **E2:** `arrow_type_key` List/LargeList/FixedSizeList → Spark `array<element>`
  simpleString (nested tinyint/smallint/float/bigint) so `dtypes` matches ndarray lit;
  Map → `map<k,v>`. **octo C3 Fixer (C3-CRATE-001):** depth-bounded recursion
  (`ARROW_TYPE_KEY_MAX_DEPTH=32` + `...` fallback) so adversarial nested List cannot
  stack-overflow via `logical_schema_fields`/`dtypes`; unit pins
  `arrow_type_key_list_element_simple_string_matches_spark` +
  `arrow_type_key_deep_list_nesting_is_depth_bounded`.
- **F2:** `arrow_type_key` Struct → `struct<field:type,…>` (not Debug format) for facade
  schema/dtypes/printSchema on nested createDataFrame; `call_scalar` `regexp_replace`
  default flags `"g"`; `sec`/`csc` CASE Inf at exact zero; **octo C1:** `overlay` drops
  literal `-1` 4th arg (Spark replace-length).

- `column/` — [column/map.md](column/map.md). **X3 + octo X3 C4:** `PyColumn.make_struct(fields)` → DataFusion
  `named_struct(lit(name), expr, …)` extracting outer Alias names so Spark field names
  are preserved (bare `struct(args…)` always emits `c0`/`c1`).
- `column/mod.rs` — `PyColumn.call_scalar` thin wrapper (R-FN-BATCH1/2 table lives in
  `column/function_dispatch.rs`, FN-GX) + Q1 `approx_percentile_cont`. **TZ-8:** `to_date`
  embeds `repark_functions::expr_fn::to_date` (session-zone Date32), not DataFusion's built-in.

- `exceptions.rs` — the five-member `create_exception!` taxonomy in its own file-backed module
  (the `check_lib_rs` RATCHET fired at PR-3): carries the module-scoped
  `#![expect(clippy::disallowed_methods)]` escape so the panic/spawn ban stays LIVE for the rest
  of the crate (p3c ledger P-4/P-5); re-exported at the crate root, so `crate::…Exception` paths
  are unchanged.
- `lib.rs` — the `#[pymodule] _native` entry point; **R-TRACE-SUBSCRIBER** `try_init_repark_tracing`
  (env-gated `tracing_subscriber::fmt` on import — `REPARK_LOG` preferred, else `RUST_LOG`;
  `FmtSpan::CLOSE` for phase timings; `try_init` never panics); registers `PyReparkSession` + `PyDataFrame` +
  `PyColumn` and re-exports the WG-3/U4 error taxonomy from `exceptions.rs` — base `PySparkException`
  (subclass of `RuntimeError`) ⊃ `AnalysisException` ⊃ `ParseException` (Group S: PySpark parity —
  `pyspark.errors` defines `ParseException(AnalysisException)`, so `except AnalysisException`
  catches parse errors), `UnsupportedOperationException`, and — **Group X** —
  `IllegalArgumentException`, the PySpark names, registered in
  `_native` and re-exported by `repark.errors`). `to_py_err` maps a
  `repark_core::Error` to the matching exception via the exhaustive `Error::exception_class()` →
  `ErrorClass` (Parse → `ParseException`, Analysis → `AnalysisException`, Unsupported →
  `UnsupportedOperationException` — the deterministic scope gates + unsupported iceberg features
  (U4 / audit CQ-002; the exception's docstring names live examples, and Group Y de-staled them: the
  partitioned-MERGE and merge-on-read-MERGE gates it used to cite are both RETIRED, so it now cites
  an unrecognised `write.merge.mode`, merge-on-read on a non-V2 table, and a non-Parquet write
  format); IllegalArgument → `IllegalArgumentException` — an invalid `.config(...)`
  key/value (`Error::Config`), what live pyspark 4.0.0 raises for a bad `SQLConf` value
  (Group X); Base → `PySparkException`, including the iceberg residual whose message
  leads with the kind name);
  the `ErrorClass` match is itself exhaustive (no `_`), so both hops are compile-time-checked — no
  silent default. `datafusion_to_py_err` classifies a raw `DataFusionError` via
  `repark_core::engine_err` then `to_py_err` (the boundary for the DataFrame-op / `F.expr`
  surface). Message preserved verbatim in `str(exc)`. Unit test:
  `to_py_err_routes_to_typed_exceptions_subclassing_runtime_error`.
  **A leaf type lands here ONLY with ≥1 reachable engine raise** (the Group S no-stubs rule) —
  PySpark's `ArithmeticException` / `NumberFormatException` / `DateTimeException` /
  `ArrayIndexOutOfBoundsException` / `SparkRuntimeException` are deliberately ABSENT (Group X
  enumerated-and-deferred; see the ledger in `task/todo.md`). The Python-argument leaves
  (`PySparkValueError`/`PySparkTypeError`/`PySparkAttributeError`) live in
  `python/repark/src/repark/errors.py` instead (**that package lands phase-3 PR-5**; not in the
  tree yet) — they need MULTIPLE bases, which
  `pyo3::create_exception!` cannot express, and no Rust code raises them.
- `session.rs` — `PyReparkSession`; includes `declare_temp_view_sorted` (SE-1: verified
  sortedness declaration → window `SortExec` elision; GIL released for the scan);
  `materialize_as_temp_view` (VALUES) +
  `materialize_as_cache_view` (r23 CACHE1 cache path, optional max_bytes); R-PERF-ARROW-CDF
  `register_ipc_stream_as_temp_view` (IPC ingest; **P1a:** createDataFrame facade prefers
  C-stream and only falls back here on version-skew); **I4 R-STREAM-IPC-INGEST**
  `register_arrow_stream_as_temp_view(name, obj)` — consume any `__arrow_c_stream__` exporter
  / `arrow_array_stream` capsule (`FFI_ArrowArrayStream::from_raw` → `ArrowArrayStreamReader`,
  same import contract as `dataframe.rs` `import_capsule_stream` ~769 / `bindings.rs`
  `import_stream` ~806), drain under GIL into `register_record_batches_as_temp_view` (no IPC
  encode/decode; no `repark-core` change). **P1a:** CDF non-empty path uses this seam
  (pa.Table exporter). Drain wraps `with_stream_poll_no_detach` so nested
  repark-stream re-entry cannot process-abort (octo C1-SAF-001).
- `fence.rs` — the shared **SAF-007 panic fence** over the PyO3 boundary. `fence(op, || PyResult<T>)`
  (+ the `fenced!("Type.method", { … })` macro) wraps EVERY `#[pymethods]` body across `session.rs`,
  `dataframe.rs`, and `column/mod.rs`. **OBS1:** hang-localizing families use `fenced_span!(family, op, …)`
  → `py.entry` span (`family` + `operation` static labels only — never user secrets). Families:
  `py.session`/`py.sql`/`py.read`/`py.action`/`py.catalog` (incl. `table_exists` /
  `list_temp_view_names` / `list_df_schema_table_names`). Column plan-builders stay plain `fenced!`.
  Pins: `fence_with_span_emits_py_entry_family_and_passes_through`,
  `fence_with_span_fields_are_static_labels_only` (runtime field capture),
  `entry_point_families_emit_py_entry_spans` (all five families; runtime field capture for
  static-label secret pin). A Rust panic is caught (`catch_unwind` +
  `AssertUnwindSafe`) and re-raised as the base `PySparkException` (a `RuntimeError`, near-drop-in —
  `except RuntimeError` catches it) with the panic text preserved under an "internal error"
  framing, NOT PyO3's `PanicException` (a `BaseException` — O-1: `pyo3-0.29.0/src/panic.rs`).
  `fence_stream_poll` is the second shape, for `dataframe::StreamingBatchReader::next` (the Arrow
  C-stream `extern "C"` `get_next` callback, which PyO3's trampoline does NOT cover — an escaping
  panic there ABORTS the process; O-2: `arrow-array-57.3.1/src/ffi_stream.rs`): a poll panic becomes
  a terminal `Err(ArrowError)` and the facade `to_arrow` maps it to `PySparkException`. Pins:
  `fence.rs` unit tests (helper contract),
  `session.rs::fenced_panic_surfaces_as_pyspark_exception_and_leaves_session_usable` (through real
  Python dispatch + session-usable-after),
  `dataframe.rs::arrow_stream_poll_panic_is_fenced_not_aborting_subprocess_isolated`
  (subprocess-isolated — the fence-removed mutation ABORTS the child, parent survives to see RED).
  Poison/double-panic (E3): per O-3 no std lock is held across the engine execution where a panic
  originates, so a caught panic leaves the session's `catalogs`/`registered_s3_buckets` locks and the
  set-once `SHARED_RUNTIME` un-poisoned — the session stays usable.
- `session.rs` — `PyReparkSession`: routes every `block_on` through a **process-wide** Tokio
  `Runtime` (`OnceLock` + `Arc`, shared across sequential sessions — not one runtime per
  constructor). **EC-5:** the `OnceLock` holds a `repark_core::EngineRuntime` — the TYPE is engine
  API (additive, `repark-core`); the INSTANCE stays here, same lifetime/behavior/pin test
  (`sequential_sessions_share_one_tokio_runtime`). **EC-2 (design §5 F3):** the constructor
  installs the SPARK DOOR before `build()` —
  `.with_sql_dialect(Arc::new(repark_spark::SparkDialect))` +
  `.with_extension(Arc::new(repark_spark::SparkExtension))`. A bare builder is stock DataFusion
  here (phase 1 inverted both seams), so omitting either silently yields a non-Spark session; pin
  `spark_doored_session_resolves_spark_function_and_routes_spark_statement` covers both halves.
  Constructor (builder knobs `memory_limit_gb` / `batch_size` /
  `target_partitions` refuse `0` with `Error::Config` — audit SAF-006 / octo P3C1-Q-002; these are
  ENGINE knobs, so Spark's `maxRecordsPerBatch <= 0` "no limit" sentinel is translated to `None` by
  the Python facade before it gets here and only a direct `_native` call trips the refusal;
  a non-zero `memory_limit_gb` is always >= 1 GiB, so it can never trip the engine's 1 MiB
  floor (SAF-007);
  `target_partitions` + the facade's full `config` dict — parsed for `spark.sql.catalog.<name>.*`
  blocks and registered via `register_configured_catalogs` on the shared runtime with the GIL
  released; `register_late_catalogs(config)` is the post-hoc variant for the facade
  getOrCreate reuse path (same GIL-released pattern, returns `(added, skipped)`)
  released via `py.detach`, matching other entry points; a malformed block raises here;
  `memory_limit_gb=None` → engine default 8 GiB pool, `Some(0)` → unbounded opt-out,
  `Some(n>0)` → n GiB — C2-Q-002), `sql`,
  `read_parquet`, **`read_csv` / `read_json`** (R1 — option map → session native readers),
  **T5 (EC-3 REFUSE-ARM)** `read_excel` / `excel_sheet_names` — port-pin name/arity/defaults kept,
  body raises `UnsupportedOperationException` naming the surface + "post-milestone-one" + the
  `task/todo.md` backlog row (`repark-excel` is not in this build; design §3 EC-3). Pins:
  `read_excel_refuses_with_named_unsupported_operation`,
  `excel_sheet_names_refuses_with_named_unsupported_operation`,
  `read_iceberg_table` (I1 time-travel pins), and the catalog/temp-view surface —
  `create_or_replace_temp_view(name, frame)` (lazy plan registration), `drop_temp_view`,
  `table_exists`, `register_memory_catalog` (the direct AWS-free convenience — Glue / S3 Tables
  have no dedicated method; they register through the config path above),
  `testing_create_ref` / `testing_list_snapshots` (I1 **test-support only**), **T6**
  `list_iceberg_table_names` / `list_temp_view_names` / `list_df_schema_table_names` /
  `refresh_catalog_provider` / `testing_oob_create_table` /
  `testing_oob_drop_table`, `create_namespace`
  (optional `location` property, threaded into the namespace's `location` — SQL `CREATE NAMESPACE`
  can set it via `LOCATION` since WG-5, and this programmatic path also sets it — a Glue-bound namespace is created here with its warehouse path; ADV-1. The
  session seam mirrors `location` onto `location_uri` — the U2 / audit BUG-001 dual-write, pinned
  at `repark-core` (v1 `repark-session`, re-homed); this crate only builds the single-key map).
  Unit test: sequential sessions Arc-share the runtime.
- `dataframe.rs` — `PyDataFrame`: wraps a DataFusion `DataFrame`; `count`; `column_names` /
  `logical_schema_fields` (post-`analyze_eagerly` metadata, **no execution**);
  **`analyzed_arrow_schema`** (analysis-only Arrow C schema `PyCapsule` — physical field types for
  plan-only consumers; U7 pandas_udf pass-through / octo C6-Q-001; **P2b:** `OnceLock<SchemaRef>`
  cache on the plan handle — first `analyzed_arrow_schema_native` pays analysis, later
  `columns`/`schema`/stream-open reuse the same `SchemaRef`, never invalidate); `limit(n)`;
  **`limit_with_skip(skip, fetch)`** (DataFusion `Limit` with non-zero skip — R-DISPLAY facade
  `_preview_tail_rows` tail preview; shareable with a later public `DataFrame.tail`);
  `show(n)` (engine-side limit then collect; returns the rendered table string); `__arrow_c_stream__`
  — zero-copy **streaming** Arrow PyCapsule: opens `DataFrame::execute_stream` (once, under
  `py.detach`) and hands the `SendableRecordBatchStream` to the crate-local `StreamingBatchReader`,
  which pulls ONE batch per `next()` (GIL released per poll unless `with_stream_poll_no_detach` —
  I4/octo C1-SAF-001 nested ingest; `block_on` on the consumer's thread, so no
  `OnceLock`-runtime re-entry) — peak memory O(one batch), not O(result) (audit SAF-003 /
  finding #14). Declares the analyzed LOGICAL schema (`analyze_eagerly`: right types + Spark-style
  `nullable = true`), not the physical `stream.schema()`. `inner()`; transforms:
  `with_column`, `filter` / `filter_sql` (**G15:** `filter_sql` calls
  `repark_spark::refuse_collation_in_sql` so a SQL-string COLLATE never hits DataFusion's
  unsupported-AST path), `select`, `drop`, `sort`, `join_on_names` /
  `join_on_condition` (**H1 r20:** `join_type_from_str` accepts inner/left/right/full —
  Apache self-join / select-join-keys battery; **G4b:** widened with the semi family
  `semi`/`left_semi`/`leftsemi` → `JoinType::LeftSemi` and `anti`/`left_anti`/`leftanti` →
  `JoinType::LeftAnti`. `join_keeps_only_left_columns` gates the Spark key-merge projection OFF
  for those two: a semi/anti output is the LEFT input's schema, so there is no duplicate
  right-hand key to merge and `spark_join_projection` must not run); **Group E** set/aggregate routing:
  `aggregate(group_by, aggregates)`
  (PySpark `groupBy().agg`; empty `group_by` = global aggregate — one NULL row over an empty input,
  vs zero rows grouped), `union(other, by_name)` (`by_name=false` = positional keeping left names +
  type-coercing; `by_name=true` = `union_by_name`, filling missing columns with NULL — the facade
  rejects a mismatch when `allowMissingColumns=False`), `distinct()` and `distinct_on(subset)`
  (dropDuplicates all-cols / subset), `with_column_renamed(old, new)` (no-op on a missing column,
  Spark parity). Plan-time DataFusion errors (transforms, and the `execute_stream` build)
  route through `crate::datafusion_to_py_err` → the WG-3 taxonomy, so `filter("bad sql")` raises
  `ParseException` and `select("no_col")` raises `AnalysisException`. A mid-stream **execution**
  error (surfacing while the consumer pulls batches) rides the Arrow C stream's error channel as an
  `ArrowError` (engine text preserved) — the facade `to_arrow` re-raises it as the base
  `PySparkException`. Streaming test layers (`#[cfg(test)]`): **reader-level** pins over hand-scripted
  streams — multi-batch value+type, error surfacing, and (over a *sequential* stream)
  batch-1-before-a-later-error (a sequential-stream *ordering* property); and the **end-to-end**
  `arrow_c_stream_export_is_lazy_and_does_not_materialize_up_front` pin, which drives the real dunder
  over a single-partition plan whose source counts batches produced (`produced == 0` at export — red
  on a collect-then-wrap revert, F-BR-4). The export contract is O(one batch) **peak memory**, NOT
  batch/error ordering (F-BR-5: a parallel plan may surface a later batch's error before batch 1).
  **r23 PG2 / OTH-009 / Q15:** Ctrl-C during a long poll is deferred until `block_on` returns;
  `check_signals` between batches was evaluated and **not shipped** (would launder
  `KeyboardInterrupt` → `ArrowError` → facade `PySparkException`). Rationale lives in the rustdoc
  on `StreamingBatchReader` (v1's `task/pg2-pg-runtime-ledger.md` has no counterpart here).
- `column/mod.rs` — `PyColumn`: wraps a DataFusion `Expr` (`from_py_object`-opt-in so it extracts by
  value as a method arg). Constructors `column`/`literal`/`sql` (**G15:** `sql` calls
  `repark_spark::refuse_collation_in_sql` so `F.expr("… COLLATE …")` refuses at parse altitude)
  + `coalesce`/`concat`/
  `current_timestamp` (Group F: casts DataFusion `now()` ns → `timestamp[us, tz=UTC]` to match
  live PySpark 4.1.2 Arrow and Iceberg v2 — `timestamp_ns` is rejected until v3); operators
  `add`/`sub`/`mul`/`div`/`modulo`, `eq`/`ne`/`lt`/`gt`/`le`/`ge`,
  `and_`/`or_`/`not_`; `alias`; `cast` (canonical type-string parser → Arrow `DataType`; accepts the
  seven `types`-object strings plus `long` / `bigint` → `Int64` for the PySpark `cast("long")`
  spelling and the facade na-fill width-preserving path, which `IntegerType`=Int32 cannot name).
  **X1 call_scalar:** trig/hyperbolic/inverse (`cos`…`atanh`), `hypot`/`sec`/`csc`/`cot`,
  bitwise_and/or/xor, `eq_null_safe`, like/ilike/rlike, `array`/`make_array` (for `lit([…])`
  + `F.array`).
  Two Spark-semantics guards over the raw DataFusion ops: `div` casts both operands to `Float64`
  (Spark `/` is always true/double division, never integer-truncating) and `concat` wraps the
  DataFusion `concat` in an any-arg-`IS NULL` → NULL `CASE` (Spark propagates NULL; DataFusion
  skips nulls as empty strings). Known divergence: zero-arg `concat()` fails at plan time where
  Spark returns `''` (fail-loud, no real caller — tracked in task/todo.md). SQL-path
  `current_timestamp()` (DataFusion bare `now()`) is still ns until a SQL-shim unit.
  `expr`/`sql` plans on a throwaway context with `register_all`+`analyzer_rules`, then analyzes
  eagerly (`repark_functions::analyze_eagerly`) BEFORE extracting the projection expr — the
  handoff carries post-analysis types (`5/2` hands off Float64, not an Int64 label over Float64
  buffers; fixed 2026-07-13 after the F.expr bit-reinterpretation regression), keeping F.expr ==
  `spark.sql` on the Arrow path; one Utf8View→Utf8 handoff cast survives (FFI export mishandles
  Utf8View). A parse/analysis failure of the `expr` string routes through
  `crate::datafusion_to_py_err` (WG-3): a syntax error → `ParseException`, a column-referencing /
  unresolvable expr → `AnalysisException` (was a bare `ValueError` pre-WG-3).
  `is_null`/`is_not_null`/`case_when` for Column/F.when.
  WG2: the date functions (`year`/`month`/`quarter`/`weekofyear`/`dayofweek`/`dayofmonth`/
  `dayofyear`/`last_day`/`add_months`/`date_add`/`date_format`/`trunc`/`date_trunc`) delegate to
  `repark_functions::expr_fn`; **Group I** adds `weekday` (0=Monday..6=Sunday). The window surface
  `row_number` (built as `Cast(row_number() OVER (),
  Int32)` so the output type matches Spark's `IntegerType`, not DataFusion's `UInt64`) + `over`
  (rebuilds the `Expr::WindowFunction` via `ExprFunctionExt`, unwrapping/re-applying the parity cast).
  T1b: `ta_window(name, args)` builds an un-`OVER`ed TA window function from `repark_ta::udf`
  (the same `WindowUDF` the session registers); `over` then attaches the ordering. The `repark.ta`
  Python facade calls it (`ta.ema(col("close"), timeperiod=21).over(w)`).
  **Group E** aggregate builders (all NULL-skipping, Spark parity): `aggregate(kind, ignore_nulls)`
  for `sum`/`avg`/`min`/`max`/`first`/`last` (IGNORE NULLS via `NullTreatment` for first/last —
  PySpark's `ignorenulls`) and **Group J** `collect_list`/`collect_set` (DataFusion `array_agg`,
  with `DISTINCT` for the set form; both force `IGNORE NULLS` — Spark excludes NULL elements — and
  `coalesce(..., make_array())` so an empty group is `[]` not NULL; element order is
  nondeterministic), `count_aggregate(columns, distinct)` for the `count` family (one column =
  `count(col)` skipping NULLs; a literal-`1` column = `count(*)` counting rows; multi-column
  `distinct` packs into a null-if-any `struct` then single-arg `COUNT DISTINCT` — DataFusion rejects
  multi-arg `COUNT DISTINCT` natively; Spark excludes a row when any distinct column is NULL), and
  `display_name()` (the expr's schema name, `col("x")` → `"x"`, so the facade can compute the
  PySpark output name `sum(x)`). The facade aliases each aggregate to its Spark output name; the
  returned expr is deliberately un-aliased.

## Pointers

- Up: [../map.md](../map.md)
- Tests: `../tests/bindings.rs` (driven from Rust via the `auto-initialize` dev-dep).

## Debug

First checks: `cargo test -p repark-python` (keep `extension-module` OFF); `maturin develop` +
import smoke; `PYO3_PYTHON` set via `.cargo/config.toml`. Escalate to: [../map.md#debug](../map.md).

<!-- 2026-07-14: lint-pass doc touch for staged CTAS / metadata schema -->
<!-- 2026-07-25: C-Y-2 — rewrapped the UnsupportedOperationException create_exception! docstring to
     the 100-col house width (a >100 line inside a string literal is invisible to rustfmt) -->

- **PG2 (EC-3 REFUSE-ARM):** `PyReparkSession.read_postgres` (jdbc / format postgres surface) keeps
  its nine-argument port-pin signature and raises `UnsupportedOperationException`; `repark-postgres`
  is scheduled post-milestone-one (`task/todo.md` backlog). The refusal never echoes the connection
  URL or properties — pin `read_postgres_refuses_with_named_unsupported_operation` asserts that.
  The pin carries **two distinct sentinels, one per vector** (`sentinel-secret` inside the `url`,
  `sentinel-property-secret` inside a non-`None` `properties` map), because the claim names both:
  passing `properties=None` left the properties half unpinned and a properties-only leak stayed
  green (phase-3 PR-3 verify panel, F-4; docs/testing.md "Pin every class the claim names"). If a
  third credential-bearing argument is ever added, it gets its own sentinel here.

## DF 54.1 note (2026-08-01)
as_any trait methods removed (DF54 trait upcasting); Cast uses field-aware API where touched.

<!-- 2026-08-02: r16 combine rider — doc-markdown backtick fix in column/mod.rs struct doc -->

<!-- 2026-08-04 (r24 combine rider): PyO3 note_local_write_root passthrough for the SEC-02
  typed-writer narrowing (internal; not a PySpark surface). -->
- r25 morning critic fix: `collapse_identity_alias_chain` (column/expr_build.rs) preserves the outer
  Alias `relation` + field `metadata` and passes a lone Alias through untouched; pin
  `collapse_identity_alias_chain_preserves_qualifier_and_metadata`.
