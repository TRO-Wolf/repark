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
  DFCORE-1 (2026-09-07): the leaf helpers above the class moved out — Arrow cell conversion to
  `rows_export.py`, export-error mapping to `export_errors.py`, mapInArrow schema checks to
  `udf_schema.py`, grouped-UDF assembly to `grouped_udf.py` (6302 → 5954, mirrored in the CAP-1
  test). `core.py` re-imports every moved private name, so the package export surface is
  unchanged; the `PySparkNotImplementedError` import stays because it is part of that surface.
  pins: dfcore-1/C-001, C-002, C-004, C-005, C-006
  DML-A: `mergeInto` `whenNotMatchedBySource` DELETE/UPDATE execute.
  NULLABILITY-2 (2026-09-05): the `schema` property maps the `timestamp`/`timestamp_ntz`
  type keys through `ReparkDataType.fromDDL` — `fromDDL("timestamp")` equals the old
  `TimestampType()` arm, and `timestamp_ntz` now lands on `TimestampNTZType` instead of
  the `StringType` fallback. One import line removed (6303 → 6302, mirrored in the CAP-1
  test). Registry `READ-TSNTZ-DTYPE-1`.
  pins: nullability-2/C-006
  TYPES-1 (2026-09-05): `sample`/`randomSplit` hash arithmetic wraps `__repark_rn` in
  `CAST(__repark_rn AS BIGINT)` (+2 lines, absorbed back in round 4).
  DFCORE-2 (2026-09-07): the four UDF select rewrites moved out — scalar and
  classic to `udf_projection.py`, the window variants to `udf_window_projection.py`
  (5954 → 5263, mirrored in the CAP-1 test). `select` keeps two one-line
  delegations; `core` binds the two modules, so the package and core surfaces
  gain exactly those two names and the class loses exactly the four methods.
  pins: dfcore-2/C-001, C-002, C-003, C-006
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
  DFCORE-1 (2026-09-07): also owns `_arrow_map_pairs`, `_arrow_cell_to_spark_python`, and
  `_refuse_calendar_interval_python_value`, moved here from `core.py` with no behaviour change;
  its `core` imports are gone. An empty map arrives as `[]` from `to_pylist` and must become
  `{}` — never an empty array. pins: dfcore-1/C-004
- `export_errors.py` owns mid-stream Arrow export failure mapping (DFCORE-1, moved from
  `core.py`). Sort failures surface through DataFusion or PyArrow. The marker list names
  memory / ExternalSorter / FairSpillPool texts, and matching is case-insensitive. The
  sort-preserving merge shares the ExternalSorter pool, so its markers ride along. PyArrow
  sometimes wraps capsule failures in `dynamically evaluated source` noise instead of the
  engine message. The extractor prefers the longest non-noise candidate and strips the
  leading `External error: ` shell DataFusion adds on the Arrow boundary. Error classes,
  chaining, and the memory advice text are unchanged. pins: dfcore-1/C-005
- `udf_schema.py` owns mapInArrow schema coercion and batch validation (DFCORE-1, moved from
  `core.py`). Arrow widths match the session `createDataFrame` path, so `SMALLINT` / `TINYINT`
  / `FLOAT` stay narrow. pins: dfcore-1/C-006
- `grouped_udf.py` owns contiguous-group assembly for the applyInPandas bridge (DFCORE-1,
  moved from `core.py`). The key-missing sentinel marks "no group seen yet" in the single-pass
  boundary scan; it is never a real key. An empty returned frame with no columns is an empty
  group result, not a mismatch. A group that continues past a batch edge is stitched onto the
  pending segments, never closed. pins: dfcore-1/C-006
- `udf_projection.py` owns the scalar pandas and classic UDF select rewrites (DFCORE-2,
  moved from `core.py`). Windowed GROUPED_AGG markers never enter the scalar bridge; they
  route to `udf_window_projection.py`. Partition-transform inputs are refused (they project
  all-null dummies the UDF would silently read); generator and aggregate inputs are refused
  (unnest or aggregate first). Stable bare refs rebind to the frame; compounds keep their
  plan expression. The intermediate projection is plan-only: no UDF runs, no rows move.
  Pass-through types come from the intermediate analyzed Arrow schema (metadata only, never
  a `limit(0)` action); physical types stay as-is so zoned timestamps survive.
  `return_type_sql` is revalidated at the bridge (markers can be mutated after
  construction). Both map-bridge halves are patched with the built identity: `mapInArrow`
  coercion drops timestamp timezones and collapses `timestamp_ntz` / `varchar(n)` /
  `char(n)`. Classic UDFs run once per row: slower than `pandas_udf` by design.
  pins: dfcore-2/C-004
- `udf_window_projection.py` owns the windowed GROUPED_AGG select rewrites (DFCORE-2,
  moved from `core.py`). One select's markers share partition keys, order keys, and frame
  bounds; order is all-or-none. The unbounded path strips the window, aggregates per
  partition, and joins back with `IS NOT DISTINCT FROM` so NULL keys match. Both sides
  materialize first (bridge plans qualify keys and trip ambiguity). The final projection
  follows caller order with last-wins duplicates, so `withColumn` overwrite keeps the
  window result. The ordered path carries partition, order, and UDF inputs plus every
  source column on the group frame, overwrites same-name sources, and projects caller
  order last-wins. pins: dfcore-2/C-005
- `joins_columns.py` owns `GroupedData`, grouping sets, pivot, and pandas UDF grouping bridges.
  DFCORE-1 (2026-09-07): imports the moved schema/group helpers directly from `udf_schema.py`
  and `grouped_udf.py`, not through `core`. The grouped-UDF names arrive via a module import
  with qualified call sites: the canonical two-name from-import costs two lines the exact
  ceiling cannot spare, and sibling ceilings never rise (1239 → 1238, mirrored in the CAP-1
  test). pins: dfcore-1/C-006, C-007
- `plan_collapse.py` owns plan simplification, window structural keys, show formatting, Arrow
  display/type conversion, SQL literal quoting, identifier rewrites, and writer safety helpers.
- `udf_bridge.py` owns action-time pandas, classic, and Arrow UDF callbacks without importing
  `DataFrame` at module scope. DFCORE-2 (2026-09-07) keeps callback execution here; only the
  projection rewrites moved out. pins: dfcore-2/C-005
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
| Export-error mapping | [`export_errors.py`](export_errors.py) |
| `mapInArrow` schema checks | [`udf_schema.py`](udf_schema.py) |
| Grouped-UDF assembly | [`grouped_udf.py`](grouped_udf.py) |
| UDF callbacks | [`udf_bridge.py`](udf_bridge.py) |
| Scalar and classic UDF projection | [`udf_projection.py`](udf_projection.py) |
| Windowed UDF projection | [`udf_window_projection.py`](udf_window_projection.py) |
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
  `udf_bridge.py` stays below the source-size default. TYPES-1 round 4 (2026-09-05): `core.py`
  6305→6303 — one import joined absorbs the round's increase (pins: types-1/C-008).
  DFCORE-1 (2026-09-07): `core.py` 6302→5954, `joins_columns.py` 1239→1238; the three new leaf
  modules stay below the source-size default (pins: dfcore-1/C-007).
  DFCORE-2 (2026-09-07): `core.py` 5954→5263; `udf_projection.py` (349) and
  `udf_window_projection.py` (337) stay below the source-size default
  (pins: dfcore-2/C-006).
- Scratch-view failures: inspect `_temp_views.py`. Facade-owned views are home-qualified; engine-
  owned scratch registration has its own lifecycle.
