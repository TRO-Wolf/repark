# map — python/repark/src/repark/spark

## Purpose

This package is the PySpark-compatible facade over the Rust engine. It owns public
Spark names, argument validation, SQL lowering, Arrow/Python boundary handling, and
session-local state. Engine computation stays in Rust; user UDF callbacks execute
in Python over Arrow batches.

The package exposes `ReparkSession`, the `SparkSession` and `ReParkSession`
aliases, `DataFrame`, `Column`, `Catalog`, `Window`, `Row`, Spark data
types, scalar/aggregate/UDF functions, and table/storage helpers. The package's
`sql` and `types` aliases preserve common PySpark import paths.

## Modules

- `__init__.py` — public exports, version loading, and process-wide ANSI SQL
  entry point. Version metadata is loaded before facade imports.
- `_csv_smart.py` — deterministic CSV preparation and schema inference. It handles
  BOMs, preambles, delimiter/header detection, ragged rows, and typed inference with
  explicit fallback to string.
- `_idents.py` — single home for SQL identifier, path-segment, and string-literal
  escaping. Callers must use these helpers for embedded user names and values.
- `_integral.py` — **Round 3 (2026-09-06):** Spark INTEGRAL-type coercion for facade
  integer knobs (`checked_integral`); numpy `__index__` types run, bool/float/str fail
  with `AnalysisException` / `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` carrying Spark's
  sqlExpr/paramIndex/inputSql/inputType/requiredType (live 4.1.2). pins: perf-approxpct-1/C-002
- `_secrets.py` — secret-property classification and redacted runtime configuration
  listing. Explicit `get` calls do not redact values.
- `_temp_views.py` — temporary-view ownership and cleanup helpers.
- `catalog.py` — Spark catalog facade. It lists namespaces, Iceberg tables, temporary
  views, and schema tables; supports current catalog/database state, function
  registration, cache clearing, and table/view existence operations. Engine-private
  temporary names remain hidden from listing APIs.
- `column.py` — lazy expression objects, type gates, aliases, field access, generators,
  aggregates, windows, casts, and Spark-compatible operator behavior. Column identity
  metadata preserves join and duplicate-name semantics.
- `functions.py` — scalar, collection, date/time, aggregate, generator, UDF, and
  window function exports. SQL fragments use centralized escaping helpers and
  unsupported operations fail explicitly.
- `functions_agg.py` — aggregate-function re-exports.
- `functions_bitwise.py` — bitwise scalar wrappers.
- `functions_collections.py` — array, map, sequence, and collection wrappers.
  **FN-FIX-1:** `arrays_overlap` is the three-valued kernel, not the size-of-intersect shim.
  Live co-collect `test_live_fn_fix_1_arrays`.
  pins: fn-fix-1-registry-rows/C-002
- `functions_datetime.py` — date/time and timestamp wrappers.
- `functions_declared.py` — FNP-15/16 declared-absent refusals (unreachable / deferred-by-cost).
  Installed onto `functions.py` after `__all__` so the sql.functions re-export sees them.
  Sketches (32), CSV/XML/XPath (11), VARIANT (8), and geospatial (5) are deferred-by-cost.
  pins: fnp-15-16/C-001, C-008, C-009, C-010, C-011, C-014, C-016
- `functions_expr.py` — shared expression builders and scalar lowering.
  FN-REGEXP-EXTRACT-1 (2026-09-04): `regexp_extract` calls the native kernel on both doors; its
  docstring is one line.
  SEM-1: `log(col)` or `log(base, expr)` (PySpark `log(arg1, arg2=None)`).
  LOG1P-1: `log1p` / `expm1` are `_scalar` onto the precise kernels, not
  `log(1+col)` / `exp(col)-1`.
  pins: sem-1-spark-answer-parity/C-006
  pins: log1p-1-precise-kernels/C-002
  **DATE-FN-1 (2026-09-04):** `unix_timestamp` is `_scalar` onto the kernel (format arg still
  unsupported). pins: date-fn-1-spark-date-spelling/C-002
  **FN-FIX-1 (2026-09-03):** `sha2` hex string + bit lengths; `array_sort` vs
  `sort_array`; `percentile_approx` discrete type.
  pins: fn-fix-1-registry-rows/C-002
  **PERF-APPROXPCT-1 (2026-09-05):** `percentile_approx` threads accuracy: the native
  `_inner` call takes it as `Option` (None is the two-arg default), and the `sql_expr`
  carries a `, {accuracy}` tail because the list form always lowers through the
  global-aggregate SQL path (nested parens fail the native classifier), where a missing
  tail would silently run at default accuracy.
  pins: perf-approxpct-1/C-002
  **Round 2 (2026-09-06):** accuracy normalizes through `_integral.checked_integral`
  before either path (Spark's INTEGRAL contract, measured on live 4.1.2): numpy integers
  run as the int on both forms, bool/float/str fail with
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` — the NULL-tail fallback is gone. The two
  Column builds merged into one shared return; ceiling 2259 → 2258.
  pins: perf-approxpct-1/C-002
  **Round 3 (2026-09-06):** that refusal is `AnalysisException` with Spark's class,
  message and params (not `PySparkTypeError` / `{arg_name, arg_type}`).
  pins: perf-approxpct-1/C-002
  **FN-FIX-2 (2026-09-04):** `trim`/`ltrim`/`rtrim` optional charset; `initcap` /
  `chr`/`elt`/`rlike` lower onto Spark kernels. pins: fn-fix-2-string-rows/C-002
  **FN-REGEXP-EXTRACT-1 (2026-09-04):** `regexp_extract` is `_scalar` onto the
  kernel (bare pattern forced-lit, optional idx defaulting to 1).
  pins: fn-regexp-extract-1/C-001
  **TYPES-1 (2026-09-05):** `from_unixtime` forwards the optional format argument.
  pins: types-1/C-006
- `functions_lambda.py` — higher-order function and lambda builders. FNP-4c adds
  `transform`, `filter`, `forall`, `aggregate`, `reduce`, `zip_with`, `transform_keys`,
  `transform_values`, `map_filter`, `map_zip_with` (installed onto `functions.py` `__all__`).
  Spark 4.1.2 `NUM_ARGS_MISMATCH` puts the user arity in expects and the declared arity in got.
  pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012
- `functions_try.py` — FNP-7a/7b `try_*` wrappers installed onto `functions.py` `__all__`.
  pins: fnp-7-try-inversions/C-013, C-016
- `functions_math.py` — mathematical and trigonometric wrappers.
- `functions_session.py` — session-bound function helpers.
- `functions_udf.py` — Python UDF and pandas UDF markers, validation, and return-type
  contracts. Execution uses the DataFrame Arrow bridge.
- `functions_url.py` — URL parsing and encoding wrappers.
- `functions_window.py` — window function wrappers.
- `merge.py` — `mergeInto` builder and SQL MERGE source registration. DML-A:
  `whenNotMatchedBySource` DELETE/UPDATE execute.
  pins: dml-a-merge-not-matched-by-source/C-002, C-003
- `polars.py` — optional Polars-style facade. Imports Polars lazily and keeps join,
  sort, and null-placement semantics explicit. TYPES-1 round 4: `with_row_index` casts
  `row_number` to BIGINT (pins: types-1/C-005).
- `row.py` — Spark-compatible Row construction, indexing, equality, nested conversion,
  display, and pickling.
- `storage.py` — StorageLevel flags and the facade cache contract. Disk, off-heap,
  and replication flags are recorded; actual persistence is engine-owned.
- `ta.py` — TA-Lib technical-analysis/window helpers and `with_indicators`. ML
  estimators and feature/evaluation surfaces live in [ml/map.md](ml/map.md).
- `types.py` — Spark SQL data types, DDL/JSON conversion, schema inspection, interval
  support, metadata, and Python-value verification.
- `udtf.py` — user-defined table-function validation, registration, scalar literal
  calls, and Arrow expansion.
- `window.py` — Window and WindowSpec construction, frame bounds, ordering, and
  partition expressions.

## Durable contracts

- DataFrame transformations are lazy until an action. Metadata inspection does not
  execute UDFs or consume rows.
- SQL identifiers and string literals are escaped centrally. Never rebuild those rules
  in a caller.
- Python UDFs run through Arrow batches; user exceptions retain the PySpark exception
  taxonomy and traceback. Unsupported composition fails loudly.
- Cache and temporary-view names are tracked for cleanup. Intermediate engine names never
  appear in user-facing schemas or catalog listings.
- Spark aliases remain identity aliases where promised. Error classes preserve native
  identity and Python multiple-inheritance behavior.
- Optional dependencies fail at the point of use with a classified error. Importing the
  core facade does not require Polars, pandas, or other optional packages.

## Known limitations

- `struct_type_from_arrow` validates its input with `assert`. Optimized Python removes that
  check; a separate behavior change must replace it with a structured runtime error.

## Pointers

- Parent package: [../map.md](../map.md)
- SQL aliases: [sql/map.md](sql/map.md)
- DataFrame implementation: [dataframe/map.md](dataframe/map.md)
- Session implementation: [session/map.md](session/map.md)
- Tests: [../../../tests/map.md](../../../tests/map.md)
- Design: [../../../../../docs/design/python-facade.md](../../../../../docs/design/python-facade.md)
