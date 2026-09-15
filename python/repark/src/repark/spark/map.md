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
  **FACADE-4 step 1 (2026-09-14):** `rung_to_spark_type` / `rung_to_engine_cast` /
  `rung_to_sql_cast` read the shared Rust table (`csv_rung_descriptor`,
  `csv_sql_cast_token`); the rung answers are unchanged (D3–D5 stay pinned).
  **Round 2 (PY-P2-003):** `rung_to_engine_cast` answers `csv_engine_token`
  straight from `csv_rung_type` — no `DataType` construction; all three binds
  go through the cached `_type_table._native_function`.
  pins: facade-4/C-013, C-027
- `_type_table.py` — Python-side descriptor bridge for the shared Rust type table:
  the class→row answer table (descriptor head, `simpleString`, `_engine_type`),
  descriptor encode/decode, tree walks, and the container-token fallbacks for
  foreign `DataType` subtypes. Class references resolve lazily so `types.py` keeps
  a one-directional import.
  **FACADE-4 step-1 remediation (2026-09-14, P1-DTYPES):** atomic token answers are
  Python-side constants/parameter derivations byte-identical to the Rust table
  (pinned in `test_facade_4_census_pins.py`); nested trees compose over them in
  Python because a per-column descriptor FFI (~15 µs for nested3's `mid`) cannot
  meet the +5 % `dtypes` bar.
  P2-DICT follow-through: the descriptor decode caches its kind→class and
  class→head maps and the `types` module handle — the first landing rebuilt a
  19-row map per recursive node and re-imported `types` per call (~30 µs on a
  7-node nested decode, regressing `df.schema` +50 %).
  **FACADE-4 step-1 remediation round 2 (2026-09-14):** the row table grows to
  five per-class answers (descriptor head, `simpleString`, `_engine_type`, SQL
  marker, DDL marker) resolved by an MRO scan so pass-through subclasses keep
  the base answer (`_inherited_type_name` reproduces the dynamic
  `type(self).typeName()` fallback); `_leaf_ddl` dispatches nested DDL leaves
  the same way. `_parse_datatype_string` keeps a Python
  residue (`_parse_datatype_string_python` + `_parse_field_list`) for integer
  parameters beyond i64 and non-printable text whose refusal `repr` bytes
  differ from Rust's; `_native_function` caches lazy native lookups.
  **Round 2 (PY-P2-002):** `_descriptor_to_datatype` decodes the tagged-tuple
  wire shape (`("kind", …)`) the bridge now emits — positional indexing, no
  per-node dict lookups.
  **Round 4 (2026-09-14, L-008/L-009):** base answers each surface with a
  different resolution — MRO for `simpleString`-style answers, an
  integer-first `isinstance` order for Arrow and SQL markers, and a
  string-first order for DDL. `_arrow_order`/`_sql_order`/`_ddl_order` cache
  the three orders; `_primary_class` picks the first matching class in an
  order (exact tabled classes and exact containers short-circuit);
  `_atomic_token`'s `order` parameter switches its subclass scan from MRO to
  the surface order; `_datatype_to_descriptor` propagates `None` upward so
  unknown subtrees reach the Python fallbacks in one walk, and struct members
  build through `_field_descriptor`. `_leaf_ddl`/`_ddl_token_python` keep the
  DDL fallback on `data_type.simpleString().upper()` so `simpleString`
  overrides (intervals and every other leaf) survive nested.
  **Round 5 (2026-09-14, L-011):** the `isinstance(StructField)` arm is deleted
  — a field is encoded only from `StructType.fields` through
  `_field_descriptor`, so a `Left+StructField` class built without `.dataType`
  falls to the Python fallbacks and answers its non-field parent like base.
  pins: facade-4/C-010, C-012, C-016, C-018, C-020..C-024, C-026, C-029..C-031, C-033
- `_idents.py` — single home for SQL identifier, path-segment, and string-literal
  escaping. Callers must use these helpers for embedded user names and values.
- `_integral.py` — **Round 3 (2026-09-06):** Spark INTEGRAL-type coercion for facade
  integer knobs (`checked_integral`); numpy `__index__` types run, bool/float/str fail
  with `AnalysisException` / `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` carrying Spark's
  sqlExpr/paramIndex/inputSql/inputType/requiredType (live 4.1.2). pins: perf-approxpct-1/C-002
- `_secrets.py` — secret-property classification and redacted runtime configuration
  listing. Explicit `get` calls do not redact values.
- `_temp_views.py` — temporary-view ownership and cleanup helpers.
- `_pyarrow.py` — **FACADE-1 (2026-09-12):** `require_pyarrow()` imports pyarrow or raises
  `ImportError` naming `repark[pyarrow]`. Package import does not load pyarrow.
  pins: facade-1/C-002
- `_arrow_stream.py` — **FACADE-1 (2026-09-12):** `register_arrow_exporter_as_temp_view`
  prefers `register_arrow_stream_as_temp_view` (the `__arrow_c_stream__` capsule seam) and
  keeps `pa_ipc.new_stream` + `register_ipc_stream_as_temp_view` as the version-skew fallback
  when the native capsule symbol is absent. pins: facade-1/C-001
- `catalog.py` — Spark catalog facade. It lists namespaces, Iceberg tables, temporary
  views, and schema tables; supports current catalog/database state, function
  registration, cache clearing, and table/view existence operations. Engine-private
  temporary names remain hidden from listing APIs.
  **EAGER-OWN-1 step 1 (2026-09-13):** `clearCache` releases the session's live
  `CacheViewHandle`s (registered in a WeakSet under the alive token) before the
  unchanged registry `unpersist` loop and the `__repark_cache_*` prefix sweep —
  idempotent, checkpoint views untouched.
  pins: eager-own-1/C-006, C-011
  **CFG-2 step 2 (2026-09-13):** `SourceMetadata` (the `name` / `kind` / `key_path` /
  `auto_register` / `properties` namedtuple) lives beside `CatalogMetadata` — the
  `listCatalogs` idiom — for `ReparkSession.sources()` rows. pins: cfg-2/C-013
- `column.py` — lazy expression objects, type gates, aliases, field access, generators,
  aggregates, windows, casts, and Spark-compatible operator behavior. Column identity
  metadata preserves join and duplicate-name semantics.
  **FACADE-2 step 2 (2026-09-12):** Group-2 operator/method families make one
  `_native.PyColumnParts` call per operation; display/SQL/join text is rendered in
  `crates/repark-python/src/column/display.rs`. The public `Column` class, `__slots__`,
  and `isinstance(c, repark.Column)` stay. pins: facade-2/C-009
  **FACADE-2 step 2b (2026-09-12):** `alias` keeps `sql_expr` as a Python passthrough
  (`self.sql_expr_part()` — reuse, not assembly) instead of round-tripping the string
  through Rust; the native `alias` returns `(PyColumn, spark_display)`. pins: facade-2/C-013
- `functions.py` — scalar, collection, date/time, aggregate, generator, UDF, and
  window function exports. SQL fragments use centralized escaping helpers and
  unsupported operations fail explicitly.
  **FACADE-2 step 3 (2026-09-13):** `lit` temporal arms and the numpy-array cast path
  use typed `_native.PyColumnParts` constructors (`lit_timestamp` / `lit_date` /
  `lit_time` / `lit_array_cast`); `_scalar` ships child display/SQL/join fragments to
  `PyColumnParts.call_scalar`, which renders `name(args)` in Rust while foldability,
  aggregate and ungroupable flags stay Python-side bookkeeping. `F.expr` stays the
  one `_native.PyColumn.sql` caller — its text is the caller's. `_lit_numpy_ndarray`
  still walks NumPy elements in Python (recorded P3, no change). pins: facade-2/C-014,
  C-016, C-018, C-021
  **ABS-EXPR-1 (2026-09-13):** `abs` is one native `_scalar("abs", …)` call — the old
  `when(c < 0, 0 - c).otherwise(c)` rewrite embedded its child 3× per level, so nested
  `F.abs` chains were exponential in native memory and aborted the process at depth ~14
  (run 9 OBS-R9-6 / INC-R9-1). pins: abs-expr-1/C-002, C-003
- `functions_agg.py` — aggregate-function re-exports.
- `functions_bitwise.py` — bitwise scalar wrappers.
- `functions_collections.py` — array, map, sequence, and collection wrappers. **FNP-9
  (2026-09-05):** `create_map`, `map_concat` and `array_insert` land here.
  pins: fnp-9-collections-json/C-006
  **ARRAY-NULL-1 (2026-09-14):** `_glue_element` is one `_scalar("array_append"/
  "array_prepend", a, e)` call per level — the null-preserving
  `spark_array_*_udf` shims replaced the `when(isnull(a), NULL).otherwise(flatten(...))`
  rewrite that embedded `array_col` 2× per level (measured ~×3/level: 51 MB at
  depth 12, 269 MB at depth 16 on the base tree).
  pins: array-null-1/C-002, C-003
  **FN-FIX-1:** `arrays_overlap` is the three-valued kernel, not the size-of-intersect shim.
  Live co-collect `test_live_fn_fix_1_arrays`.
  pins: fn-fix-1-registry-rows/C-002
- `functions_datetime.py` — date/time and timestamp wrappers.
- `functions_declared.py` — FNP-15/16 declared-absent refusals (unreachable / deferred-by-cost).
  Installed onto `functions.py` after `__all__` so the sql.functions re-export sees them.
  Sketches (32), CSV/XML/XPath (11), VARIANT (8), and geospatial (5) are deferred-by-cost.
  pins: fnp-15-16/C-001, C-008, C-009, C-010, C-011, C-014, C-016
- `functions_expr.py` — shared expression builders and scalar lowering. **FNP-9/10
  (2026-09-05):** `arrays_zip` and `schema_of_json` stop refusing and route to their kernels.
  pins: fnp-9-collections-json/C-003, C-006
  **ABS-EXPR-1 (2026-09-13):** `cbrt` and `nullif` are one native `_scalar` call each
  (`expr_fn::cbrt` / `expr_fn::nullif`); both `when(...)` rewrites embedded their child
  more than once per level (cbrt 3×, nullif 2×). `nvl2` stays a `when` — each child is
  embedded exactly once (linear). pins: abs-expr-1/C-002, C-004
- `functions_stack.py` — **PERF-UNPIVOT-1 (2026-09-12):** `F.stack` / `StackCall` /
  `select_with_stack_if_present`. Installed last onto `functions.py`. pins: perf-unpivot-1/C-004
- `functions_json.py` — **FNP-10 (2026-09-05):** the JSON wrappers (`get_json_object`,
  `json_array_length`, `json_object_keys`, `to_json`, `from_json`). Its `install_into` also
  re-exports the collection constructors from `functions_collections`, so the whole FNP-9/10
  surface reaches `functions.py` through the existing installer chain instead of growing that
  module past its exact size baseline. `FNP9_NAMES` is the export table
  `scripts/check_example_coverage.py` reads, so a name added here is a name that needs an
  example. The unit's unbuilt names (`inline`, `inline_outer`, `call_udf`,
  `call_function`) are deliberately NOT here (`stack` landed in PERF-UNPIVOT-1): exporting a
  refusal would add rows to an example backlog whose count only ratchets down.
  §7 `FNP9-GENERATORS-1` / `FNP9-BYNAME-1`.
  The `DataType` import is under `TYPE_CHECKING` — a runtime one closes an import cycle through
  `repark.spark.types`. `_refuse_json_options` is the one rule `from_json`, `to_json` and
  `schema_of_json` share: repark implements no JSON option beyond `mode` and
  `columnNameOfCorruptRecord`, and Spark's `ignoreNullFields` / `primitivesAsString` change the
  answer, so a non-empty mapping refuses instead of being ignored.
  `install_into` ran last in `functions.py`'s installer chain until PERF-UNPIVOT-1 appended
  `functions_stack`; `test_functions_split_identity.py` pins the order, which
  `test_functions_split_identity.py` pins by position. The rules each wrapper carries — Spark's
  `map(k1, v1, …)` spelling behind `create_map`, the `-1`-appends and NULL-padding rules of
  `array_insert`, the NULL-fill of `arrays_zip`, PERMISSIVE decoding and the `_corrupt_record`
  column of `from_json`, and `schema_of_json` reading a bare `str` as the document rather than a
  column name — are recorded here and in the unit ledger, not in the function bodies: each keeps
  exactly the one-line docstring the presence gate requires.
  pins: fnp-9-collections-json/C-001, C-007
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
  FNP-8-REVIEW (2026-09-07) restored the `_lambda_arity` docstring #412 reworded.
  pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012
  **FNP-8 (2026-09-07):** Column builders preserve function-specific
  `NUM_ARGS_MISMATCH` errors after the generic 1–3 parameter check. Invalid-return errors name
  functions, callable objects, and `functools.partial` instances without assuming `__name__`.
  pins: fnp-8/C-003
- `functions_try.py` — FNP-7a/7b `try_*` wrappers installed onto `functions.py` `__all__`.
  pins: fnp-7-try-inversions/C-013, C-016
- `functions_math.py` — mathematical and trigonometric wrappers.
- `functions_session.py` — session-bound function helpers.
- `functions_udf.py` — Python UDF and pandas UDF markers, validation, and return-type
  contracts. Execution uses the DataFrame Arrow bridge. DFCORE-2 (2026-09-07): the
  `pandas_udf` docstring cross-reference follows the scalar rewrite to its new home,
  `dataframe/udf_projection.py` (line-count neutral; ceiling stays 1300).
  pins: dfcore-2/C-004
- `functions_url.py` — URL parsing and encoding wrappers.
- `functions_window.py` — window function wrappers.
- `merge.py` — `mergeInto` builder and SQL MERGE source registration. DML-A:
  `whenNotMatchedBySource` DELETE/UPDATE execute.
  pins: dml-a-merge-not-matched-by-source/C-002, C-003
- `polars.py` — optional Polars-style facade. Imports Polars lazily and keeps join,
  sort, and null-placement semantics explicit. TYPES-1 round 4: `with_row_index` casts
  `row_number` to BIGINT (pins: types-1/C-005). DF-EAGER-1 step 2 (2026-09-09):
  `PolarsFrame.eager()` wraps the Spark `eager()`; `collect()` is untouched
  (pins: df-eager-1/C-006).
- `row.py` — Spark-compatible Row construction, indexing, equality, nested conversion,
  display, and pickling. ROW-TUPLE-1 step 1 (2026-09-14): `count` / `index` delegate to
  the stored values tuple (factory rows: the field-name tuple), answering the recorded
  `row.*` oracle cells; `index_missing` keeps Spark's bare-`ValueError` message. Critic
  round 1 (R-3): both signatures positional-only like CPython's `tuple` — keyword calls
  raise `TypeError`.
  pins: row-tuple-1/C-001, C-002, C-004
- `storage.py` — StorageLevel flags and the facade cache contract. Disk, off-heap,
  and replication flags are recorded; actual persistence is engine-owned.
- `ta.py` — TA-Lib technical-analysis/window helpers and `with_indicators`. ML
  estimators and feature/evaluation surfaces live in [ml/map.md](ml/map.md).
- `types.py` — Spark SQL data types, DDL/JSON conversion, schema inspection, interval
  support, metadata, and Python-value verification.
  **FACADE-4 step 1 (2026-09-14):** the conversion surfaces
  (`fromDDL`/`_parse_datatype_string`, `StructType.toDDL`, `_arrow_type_to_repark`,
  `struct_type_from_arrow`, `repark_type_to_arrow`) thin to descriptor build + one
  `_native` call over `repark_spark::type_table`; the public classes and
  `isinstance` identity are unchanged. `simpleString`/`_engine_type` answer from
  the `_type_table.py` row table (P1-DTYPES: per-column descriptor FFI regressed
  `dtypes` +106 % on wide50). Python residue: descriptor trees containing a
  foreign `DataType` subtype, decimals outside the Arrow FFI scale envelope,
  collation refusal policy, and the `json`/`fromJson` surface.
  **Remediation round 2 (2026-09-14):** `_parse_datatype_string` moved to
  `_type_table.py` (Python parse for beyond-i64 parameters and non-printable
  text); parameterised `jsonValue` formats locally; `repark_type_to_arrow`
  checks the decimal FFI bound first and falls back to
  `_repark_type_to_arrow_python` on any FFI export failure (Arrow nesting-depth
  ceilings); the FFI-covered wide-decimal pre-walks are gone; native calls bind
  through the cached `_type_table._native_function`.
  **Remediation round 3 (2026-09-14, ruling R14b-D-2):** the per-class literal
  `simpleString`/`_engine_type` methods are restored on every atomic class
  exactly as base had them (the five dynamic-answer classes and `DataType`
  keep `type(self).typeName()` bodies) — the row-table dict path was ~0.24 µs
  per leaf against base's ~0.05 µs method call, and `dtypes` breached the
  surface bar. `_atomic_token` still answers for nested composition, foreign
  subclasses and the SQL/DDL marker columns; the MRO mutation still turns the
  marker pins red.
  **Remediation round 4 (2026-09-14, L-007/L-008/L-009):** `DataType.simpleString`
  answers from `_SIMPLE_STRING_FAST` for exact classes then falls back to
  `type(self).typeName()` — the five dynamic-answer classes lose their literal
  methods so multiple-inheritance MRO matches base; `DataType._engine_type`
  delegates to `self.simpleString()`. `ArrayType`/`MapType`/`StructField`/
  `StructType` get base's `simpleString`/`_engine_type` bodies back so child
  and field overrides compose (a `StructField.simpleString` override survives
  inside `StructType`, `ArrayType` and `MapType`); the parameterized
  `jsonValue`s dispatch through `self.simpleString()` again. `StructType.toDDL`
  and `repark_type_to_arrow` pass their surface orders into
  `_type_table`'s descriptor build.
  **Remediation round 5 (2026-09-14, L-010):** `repark_type_to_arrow` builds
  the descriptor before the C-028 wide-decimal envelope guard and keys the
  guard on the descriptor's `decimal` kind — `.precision`/`.scale` are read
  only after the `_arrow_order` pick resolves to `DecimalType`, so a
  `Left+DecimalType` class without decimal attrs keeps base's left-parent
  Arrow answer instead of crashing.
  pins: facade-4/C-011, C-012, C-014, C-015, C-016, C-020..C-024, C-028..C-033
  **TYPES-BASES-1 (2026-09-14):** the concrete classes re-parent onto the Spark
  abstract bases imported from `types_bases.py` (`DataType` moved there so the
  bases can subclass it without an import cycle; `_SIMPLE_STRING_FAST` is a
  shared dict populated here so the fast path survives the move), and
  `types.Row` re-exports `spark.row.Row`. DDL routing through the Rust table is
  unchanged — spatial DDL tokens stay refused pending the Rust spatial step.
  Follow-up: `_merge_type` gains Spark's two mixed-SRID arms
  (Geometry×Geometry / Geography×Geography with different `srid` → the `ANY`
  form) plus `SpatialType` in the `StringType` soft-merge tuple;
  `StructField` / `StructType` gain `needConversion` / `toInternal` /
  `fromInternal` delegating to the shared helpers in `types_bases.py`.
  pins: types-bases-1/C-001, C-003, C-005, C-006
- `types_bases.py` — **TYPES-BASES-1 (2026-09-14):** `DataType` plus the Spark
  abstract bases (`AtomicType`, `NumericType`, `IntegralType`, `FractionalType`,
  `DatetimeType`, `AnyTimeType`, `AnsiIntervalType`, `SpatialType`),
  `GeographyType` / `GeometryType` with the vendored SRID→CRS table and Spark's
  `ST_*` refusals, `UserDefinedType` (TYPES-UDT-1 declared refusal on column
  use; the `serialize` / `deserialize` / `_cachedSqlType` / `toInternal` /
  `fromInternal` / `jsonValue` / `__eq__` template follows Spark on a
  subclass), the spatial JSON token helpers, and the shared
  `_struct_to_internal` / `_struct_from_internal` conversion bodies.
  `types.py` imports and re-exports every public
  name. pins: types-bases-1/C-001, C-002, C-003, C-004, C-006
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
