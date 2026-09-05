# `repark.spark.dataframe`

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).
CC-2 close: `dynamicFlatten` docstring again names ``repark_core::dynamic_flatten`` and ``Unnest``.

## Purpose

The Spark DataFrame facade. It builds lazy native plans and exposes Spark-compatible actions,
joins, grouping, exports, UDF bridges, and writers. Engine computation stays in Rust; Python
callbacks run only where the API accepts user UDFs and receive Arrow batches.

## Modules

- `core.py` owns `DataFrame`, plan construction, joins, actions, schema/type conversion, cache,
  checkpoint, temp-view registration, declared sorting, `dynamicFlatten`, and public re-exports.
  DML-A: `mergeInto` `whenNotMatchedBySource` DELETE/UPDATE execute.
  TYPES-1 (2026-09-05): null-top `last()` overlap wraps `F.when(overlap, NULL)` (+2 lines).
- `actions_export.py` owns `DataFrameNaFunctions.fill` and `drop`; `DataFrame.replace` stays in
  `core.py`.
- `rows_export.py` owns Arrow-to-`Row` materialization for `collect` / `take` / `head` /
  `toLocalIterator`. Two converters live here: `rows_from_arrow_table_python` is the unchanged
  pure-Python path and stays the correctness oracle, and `rows_from_arrow_table` adds the
  binding fast path in front of it. The fast path is declined — whole batch, straight to the
  Python converter — whenever a column may hold a calendar interval or the object cannot export
  `__arrow_c_array__`; map and tz-aware-timestamp columns are converted in Python and handed to
  the binding as supplied columns, so `_arrow_cell_to_spark_python` keeps its refusal table.
  Two costs the old loop paid per row are gone: the names tuple is built once per batch instead
  of once per `Row`, and the cyclic collector is suspended across the batch, restored in a
  `finally`, because a million freshly tracked `Row` objects otherwise make every generation-2
  pass rescan the whole result. pins: perf-facade-1/C-001, C-002, C-003
- `joins_columns.py` owns `GroupedData`, grouping sets, pivot, and pandas UDF grouping bridges.
- `plan_collapse.py` owns plan simplification, window structural keys, show formatting, Arrow
  display/type conversion, SQL literal quoting, identifier rewrites, and writer safety helpers.
- `udf_bridge.py` owns action-time pandas, classic, and Arrow UDF callbacks without importing
  `DataFrame` at module scope.
- `writer_readwriter.py` owns `DataFrameWriter`, `DataFrameWriterV2`, statistics, and write
  helpers. **DML-B:** `overwritePartitions()` emits dynamic `INSERT OVERWRITE … PARTITION`
  (ceiling 1117→1113). pins: dml-b-insert-overwrite/C-003, C-004
- `__init__.py` preserves the package import surface, including private compatibility names.

## Durable contracts

- Transformations remain lazy. Actions and Arrow exports execute the plan.
- `to_arrow_batches` holds one Arrow batch and emits one typed empty batch for an empty result.
  `collect` materializes rows and converts maps to dictionaries; calendar intervals refuse.
- `columns` reads the plan's logical schema, not the analyzed one. A dependent `withColumn`
  chain used to pay one analyzer pass per existing column per call — 5,750 `column_names` calls
  at depth 100 — because `_bind_schema_column` re-resolved each name against `self.columns`.
  `_iter_bound_columns` now reads `columns` once and passes the canonical name through, falling
  back to the resolving path when the frame carries duplicate names so the `[AMBIGUOUS_REFERENCE]`
  contract is unchanged. pins: perf-facade-1/C-004, C-005
- Arrow export maps execution failures to `PySparkException` while preserving the engine message.
  Planning and analysis errors keep their classified facade exceptions.
- DataFrame origins preserve side identity through joins. Semi and anti joins emit left columns
  only; right-origin columns remain unavailable until an emitting join restores them.
- Joins, grouping, windows, generators, and dynamic flattening refuse unsupported combinations
  before unsafe SQL text is built. `dynamicFlatten.max_depth` bounds rewrite passes, not rows.
- Scratch and cache views use home-qualified names. Cache is object-identity based and lazy until
  an action; checkpoint and write publication follow their native lifecycle.
- `declare_sorted` accepts only session-created source views, verifies order, and may tighten
  verified-null-free keys. Writers refuse tightened frames when required non-null fields persist.
- SQL identifiers and literals are quoted before free-SQL construction. Display names may retain
  Spark-legal duplicates; engine names remain unique and private.
- Window structural keys include null placement. `ascending=` follows Spark's truthy/falsy
  re-marking behavior; RePark rejects a short list instead of truncating it.
- Optional pandas, Polars, and DuckDB surfaces fail clearly when their dependencies are absent.
  Streaming UDF bridges preserve plan stability and close Arrow resources on failure.
- `printSchema` stdout is Spark's tree plus the blank line (`treeString`'s newline and
  `print`'s). pins: df-printschema-1-trailing-newline/C-001, C-004

## Navigation

| Need | Home |
|---|---|
| DataFrame methods and plan glue | [`core.py`](core.py) |
| Grouping, pivot, and `applyInPandas` | [`joins_columns.py`](joins_columns.py) |
| Missing-data helpers | [`actions_export.py`](actions_export.py) |
| UDF callbacks | [`udf_bridge.py`](udf_bridge.py) |
| Plan rewrites and display | [`plan_collapse.py`](plan_collapse.py) |
| Writes and statistics | [`writer_readwriter.py`](writer_readwriter.py) |
| Parent navigation | [`../map.md`](../map.md) |
| Rust engine contracts | [`../../../../../../crates/repark-core/src/map.md`](../../../../../../crates/repark-core/src/map.md) |
| Tests | [`../../../../tests/map.md`](../../../../tests/map.md) |
| Facade design | [`../../../../../../docs/design/python-facade.md`](../../../../../../docs/design/python-facade.md) |

## Debugging

- Import failures: inspect the re-export block in `core.py` and package `__init__.py`.
- Circular imports: region modules may import helpers from `core.py`; `core.py` binds them before
  importing region classes.
- Origin or display regressions: inspect `_origin_map`, engine-name overlays, and `_spawn` paths.
- File-size records: PYC-1 (2026-08-22) moved the UDF callbacks from `core.py` to
  `udf_bridge.py`. Under CAP-1, `core.py` and `plan_collapse.py` carry exact exception rows;
  `udf_bridge.py` stays below the source-size default.
- Scratch-view failures: inspect `_temp_views.py`. Facade-owned views are home-qualified; engine-
  owned scratch registration has its own lifecycle.
