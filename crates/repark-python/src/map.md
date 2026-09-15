# map — repark-python/src

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Rust implementation of the `_native` PyO3 module. The modules below keep the Python facade thin
and hand execution, SQL, and ML semantics to the engine crates.

## Modules

| Path | Contract |
|---|---|
| [`lib.rs`](lib.rs) | Module registration, error conversion, and tracing setup. |
| [`allocator.rs`](allocator.rs) | Optional mimalloc allocator for wheel builds. |
| [`arrow_export.rs`](arrow_export.rs) | Arrow C Stream export boundary: coerces Utf8View to Utf8 so `collect`/`to_arrow` read Spark-equal string types (CUTOVER-SCHEMA-1, 2026-09-04). Round 3 (2026-09-05): `coerce_batch_views` casts any analyzed-vs-physical mismatch under safe Arrow cast options — a per-batch copy; non-string mismatches either widen losslessly or refuse loud (see the two coercion pins). The four `StreamingBatchReader` comments moved here verbatim from `dataframe.rs`. **H3-SPILL-RESIDUE-1 (2026-09-06):** the reader carries the session's `PoolRefusalLog` (`with_refusals`, fed by `refusal_log`) and the refusal count it read at open. `as_pool_refusal` replaces the internal-error report with the engine's own `Resources exhausted` text plus one disclosure line, and logs the contained panic detail at `warn` on `repark::spill`, behind **four** gates: the item is a fenced panic; its payload is on `CONTAINABLE_PANIC_PAYLOADS`; the session's pool recorded a refusal since the reader opened; and the session is bounded at all (an unbounded one installs no log). Each gate has its own pin, and the allow-list is pinned in both directions — the NLJ payload is contained, an injected `index out of bounds` after the same refusal is still the bug report. **Round 2 (2026-09-06)** added the allow-list after the critic showed the first three gates dressed an unrelated panic up as a pool refusal. Its entries are the panics DataFusion 54.1 can reach *on the pool-refusal and spill-fallback paths*, each read from the vendored source: `partition not used yet` (`repartition/mod.rs:1277`), `at least one spill reader should exist` (`:1333`) and `at least one receiver should exist` (`:1339`) — all three in the same `execute` body whose second call is the defect; and from `joins/nested_loop_join.rs`'s memory-limited fallback, `right_data must be present` (`:1441`, `:1446`, `:1695`, `:1936`, `:2458`), `left_stream must be set after spill future resolves` (`:1566`), `left_schema must be set` (`:1631`, `:1965`), `right bitmap should be available` (`:1795`) and `without Active spill state` (`:1531`, `:1919`). A payload NOT on the list is left as the bug report it is, which is the point. The list's own pin asserts it is non-empty before iterating — an empty list would otherwise make that loop vacuously green, which the M-6 mutation caught. The scope is the SESSION's pool, not this stream's allocations — `MemoryPool` has no per-stream identity, so a refusal another query on the same session caused would also arm the rewrite; the allow-list is what keeps that bounded. The disclosure line is a `concat!` rather than a `\`-continued literal: rustfmt folds the continuation onto one line and leaves the indent's spaces inside the string. pins: h3-spill-residue-1/C-002, C-004 **CFG-1 step 3 (2026-09-09):** `drain_arrow_c_stream` (with its capsule-name constant and imports) moved here verbatim from `session.rs`; the two `pyo3::types` method traits the prelude glob used to provide are now explicit imports. pins: cfg-1/C-027 |
| [`exceptions.rs`](exceptions.rs) | PySpark-shaped exception types. **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** `CommitStateUnknownException(PySparkException)` — the ambiguous-commit class; `to_py_err` sets `operation_id` on the instance from `Error::CommitStateUnknown` and still returns the class (with a `tracing::warn`) if that setattr ever fails — the class is never demoted by the attribute write. pins: ice-commit-unknown-1/C-002 |
| [`fence.rs`](fence.rs) | Panic fences for PyO3 methods and Arrow stream polls. `fenced_panic_detail` tells a fenced panic apart from an ordinary stream error by downcasting the Arrow external error, which is what lets `arrow_export.rs` rewrite only the panics. pins: h3-spill-residue-1/C-002 |
| [`session.rs`](session.rs) | Shared runtime, session doors, readers, catalogs, and temp views.
  **NULLABILITY-2 (2026-09-05):** `finish_session` installs
  `repark_functions::install_shared_analyzer_rules` (integer overflow plus boolean-to-decimal
  casts, both doors) in place of the integer-only call — one line for one line, same count.
  **EAGER-BUDGET-1 step 2 (2026-09-13):** `materialize_as_cache_view`'s third
  argument is now the `(max_bytes, max_total_bytes)` budgets tuple, forwarded
  to the core admission loop; the one-argument signature keeps the file's exact
  CAP-1 baseline. pins: eager-budget-1/C-005 |
  pins: nullability-2/C-003 |
  `PyReparkSession.sql` runs the FNP-15/16 declared-function valve so the native
  `repark.sql()` callable (DataFusionDialect) refuses with the registry reason.
  Native and Spark Python sessions install F-Y10-1 integer overflow checks.
  Native `PyReparkSession::native` registers LOG1P-1 `log1p` / `expm1` (DataFusionDialect
  has no `on_session_built` hook; Spark door gets them from `register_all`).
  pins: log1p-1-precise-kernels/C-002
  The Spark-door routing probe is `MERGE … OUTPUT` (TRUNCATE and `INSERT OVERWRITE … PARTITION` are live).
  pins: fnp-15-16/C-001; dml-c-truncate/C-004
  **CFG-1 step 3 (2026-09-09):** `PyReparkSession::new` takes `config_path` (forced file
  into `from_config_file`) and the `config_file_pairs` static exposes the translated pairs
  for the facade fold, paid for by moving `drain_arrow_c_stream` to `arrow_export.rs`.
  Baseline 1177 → 1128, a ratchet DOWN.
  pins: cfg-1/C-026, C-027
  **CFG-2 step 1 (2026-09-13):** `finish_session` calls
  `register_configured_sources()` beside `register_configured_catalogs()`, so a loaded
  `repark.toml`'s database sources register their refusing providers at session build —
  no Python source surface lands until step 2. The added line was made line-neutral
  (the file sits on its exact 1128 baseline) by joining two `use` items in the test
  block. pins: cfg-2/C-010 |
| [`session_sources.rs`](session_sources.rs) | **CFG-2 step 2 (2026-09-13):** the named-source
  door — three free `#[pyfunction]`s taking `PyRef<'_, PyReparkSession>` (the
  `catalog_census` shape, since pyo3 allows one `#[pymethods]` block per type):
  `session_sources` answers `(name, kind, key_path, auto_register, redacted properties)`
  tuples, `session_source` resolves one name through `ReparkSession::source` (the
  undeclared-name refusal maps through `to_py_err`), and `session_source_ping` re-resolves
  then calls `NamedSource::ping` so the connector-pending refusal keeps its engine class.
  pins: cfg-2/C-013, C-014, C-015 |
| [`dataframe.rs`](dataframe.rs) | Lazy plans, actions, transforms, schema, and Arrow C Stream export. |
| [`plan_introspect.rs`](plan_introspect.rs) | DF-PLAN-INTROSPECT-1 (2026-09-14; follow-ups 2026-09-15): three free `#[pyfunction]`s over `&PyDataFrame` (the `session_sources` shape, since pyo3 allows one `#[pymethods]` block per type): `input_files` builds the physical plan on the shared runtime without executing it and walks it through `repark_core::input_files`, `semantic_hash(frame, lineages)` splits the frame into state plus logical plan, clones each lineage frame's plan into a cache-view definition map, and folds all three through `repark_core::semantic_hash` with the GIL detached, and `same_semantics(left, right, lineages)` compares both canonical streams through `repark_core::same_semantics` with the GIL detached. pins: df-plan-introspect-1/C-001, C-002, C-006, C-011 |
| [`dataframe_stack.rs`](dataframe_stack.rs) | **PERF-UNPIVOT-1:** `stack_dataframe` binds `repark_core::apply_stack`; the internal `row_labels`/`cell_indices` kwargs bind `apply_labeled_stack` for the describe grid. pins: perf-unpivot-1/C-002, C-014 |
  `filter_sql` bypasses the statement router, so it applies parse-altitude valves itself.
  Nested DDL element tokens come from `repark-spark::spark_ddl_type_name_at_depth`
  (SQL-DESCRIBE-1 D-3); `long` stays local for `printSchema`. pins: sql-describe-1/C-003 |
| [`column/`](column/map.md) | Immutable expressions, scalar functions, aggregates, and windows.
  `PyColumn.sql` also runs the FNP-15/16 declared-function valve (`refuse_declared_function_in_sql`).
  **FACADE-2 step 2 (2026-09-12):** `column/display.rs` (`PyColumnParts`) renders Group-2
  display/SQL/join strings. pins: facade-2/C-008, C-009 |
| [`collect_rows.rs`](collect_rows.rs) | Arrow batch → Python value tuples for `collect`.
  Imports the batch back through the Arrow C Data Interface and converts only the cell kinds
  whose `to_pylist` mapping is unambiguous; anything else is supplied pre-converted by the
  facade or declined with `None`, so the facade's converter keeps decimals, dates, times,
  timestamps, intervals and nested values. It converts cells, never rows — the facade builds
  every `Row`, so `Row` semantics have one implementation.
  **H3-SPILL-RESIDUE-1 (2026-09-06):** every CPython allocation on this path now goes through
  `owned`, which is `Bound::from_owned_ptr_or_err` — a NULL return becomes the `MemoryError`
  CPython already set, never a panic. pyo3's safe constructors could not be used: `PyTuple::new`,
  `PyList::new` and the scalar `IntoPyObject` impls all reach `assume_owned`, which panics on
  NULL even where the signature returns `PyResult`. The list is now allocated once at its final
  length and filled in place (`PyList_SetItem`), and each row tuple likewise, so the intermediate
  `Vec<Bound<PyTuple>>` is gone.
  pins: perf-facade-1/C-002
  pins: h3-spill-residue-1/C-001 |
| [`cdf_infer.rs`](cdf_infer.rs) + [`cdf_infer/`](cdf_infer/) | **FACADE-3 step 2
  (2026-09-13):** `createDataFrame` rows/tuple/dict inference and cell conversion in Rust.
  `cdf_arrow_export` extracts each tuple cell into a typed `Cell` (cells.rs), infers the
  Arrow schema with Spark's merge rules or imports the caller's explicit `pa.Schema` through
  `__arrow_c_schema__` (infer.rs), builds the `RecordBatch` with typed arrays (build.rs), and
  hands it back as a `PyCdfArrowExport` whose `__arrow_c_stream__` capsule `pa.table` drains —
  the same FACADE-1 seam used for export, here run in the import direction. Every cell kind,
  merge outcome, or nested shape the port does not reproduce returns `None`, and the Python
  wrapper falls back to the column-wise or legacy converter, which reproduces the pinned
  refusal class and message. Timestamp localization covers only the session-UTC case and
  explicit `tzinfo.utcoffset` offsets; NTZ honors `is_default_timestamp_ntz`. The decimal
  envelope mirrors `_validate_decimal_envelope` (precision ceiling, scale-18 truncation only
  when discarded digits are zero). Null struct parents write each child's type default
  (`CellKind::Fill`: 0, `""`, epoch, empty list/map, recursive defaults) rather than a child
  null, matching `pa.array` fill so pandas NaN-coercion parity holds. **F-FALLBACK
  remediation:** before extraction, `screen.rs` runs a kind-tag pass per cell (exact-type
  pointer hits, probe-chain only for subclasses/Row/exotics, no payload) accumulating a
  per-column kind mask plus the list-element merge mask; when the mask already predicts the
  `None` the extract/infer/build path would return (uncovered kind anywhere, scalar-merge or
  list-element-merge refusal, a kind the inferred or explicit field type cannot build,
  non-str or null dict keys under struct inference) the export returns `None` after the tag
  pass alone — the doomed extraction is never paid and Python owns the identical refusal.
  Step-3 targets recorded in the ledger findings table: F-FUNNEL (feed `Row`/dict lists into
  native without the Python-side walk) and F-TIMETUPLE (the per-cell `timetuple` callback —
  needs an abi3-compatible route since `Py_LIMITED_API` hides the `PyDateTime`/`PyDate`
  getters); F-SLOTS and F-RESCAN are ledger-only P3s. **F-FUNNEL (step 3, 2026-09-13):**
  `cdf_infer/named.rs` adds `cdf_arrow_export_named(is_row, rows, schema_names, schema, …)` —
  `Row` and `dict` lists enter native directly; the module resolves the bind the Python
  funnel owned — dict key-union (sorted first-row keys, then newly seen keys sorted per
  row), explicit-schema null-fill (extras dropped, absent keys `Null`), or the strict bind
  (`_schema_names_and_permutation` parity: identity / by-name reorder / positional rename /
  fail-loud partial or length mismatch, plus the per-row key-set check `_bind_named_row`
  owned) — then runs the same tag-screen → extract → `build_batch` tail, fetching cells by
  interned `PyString` keys. Doomed inputs decline at the cheapest probe first: pointer
  type-checks run before any `asDict()`, strict `Row` key-sets are validated on
  `_Row__field_names` tuples before the mapping collection, and the residual key-set check
  is fused into the tag pass (`len` + a missing lookup key decline). Any refusal it cannot
  reproduce (heterogeneous elements, non-str keys, subclassed `dict`/`Row`, strict key-set
  mismatch, dup names) returns `None` before extraction and Python's
  `_rows_from_mapping_list` owns the pinned class/message/index. **F-TIMETUPLE (step 3, 2026-09-13):** `cells.rs` drops `timetuple()`
  for the probe-measured abi3 routes — `date` cells subtract a cached `date(1970,1,1)` and
  read `.days`; `datetime` cells read seven interned wall-clock getattrs; `utcoffset` is
  cached by `tzinfo` identity armed only for exact `datetime.timezone`. `Ctx` gains
  `timezone_type`, `epoch_date` and the `utcoffset_cache`, built once per export call in the
  shared `make_ctx`.
  pins: facade-3/C-010, C-013, C-014, C-016, C-017, C-019, C-020, C-024, C-025 |
| [`cache_budget.rs`](cache_budget.rs) | **EAGER-BUDGET-1 step 1 (2026-09-13):** `_native.retained_cache_bytes(session)` returns the live session's distinct-buffer retained cache bytes (D-2) as `u64`, blocking on the shared runtime inside `py.detach`. A free `#[pyfunction]` like `catalog_census` because `session.rs` sits on its exact CAP-1 baseline and pyo3 allows one `#[pymethods]` block per type; `PyReparkSession.runtime` went `pub(crate)` (same line, same count) to expose the shared runtime. Step 2 (2026-09-13): unchanged — the session-total budget flows through `session.rs::materialize_as_cache_view`'s `(max_bytes, max_total_bytes)` budgets tuple. pins: eager-budget-1/C-002, C-003 |
| [`catalog_census.rs`](catalog_census.rs) | **PERF-ICE-CATALOG-IO-1 (2026-09-05):**
  `iceberg_metadata_cache_census(session)` returns `(enabled, hits, misses, body_fetches,
  entries)` for this session's Iceberg metadata-location cache. It is the census the Python pins
  read: `body_fetches` is exactly the number of `metadata.json` documents parsed, which on a Glue
  or S3 Tables catalog is the number of S3 GETs the statement would pay. A free `#[pyfunction]`
  rather than a `PyReparkSession` method, because `session.rs` sits on its exact CAP-1 baseline
  and pyo3 allows one `#[pymethods]` block per type; the product path pays nothing, since the
  counters are two relaxed atomic loads read only when asked.
  pins: perf-ice-catalog-io-1/C-001 |
| [`logical_names.rs`](logical_names.rs) | `DataFrame.columns` from the plan's logical schema,
  with no analyzer pass. Sound because every rule in `repark_functions::analyzer_rules` rewrites
  through `NamePreserver` and none adds, drops or reorders a projection expression;
  `column_names` stays analyzer-backed as the oracle the byte-equality pin measures against.
  pins: perf-facade-1/C-004 |
| [`ml.rs`](ml.rs) | Batch-streaming binders for linear, logistic, and KMeans fits. |
| [`type_bridge.rs`](type_bridge.rs) | **FACADE-4 step 1 (2026-09-14):** PyO3 bridge over `repark_spark::type_table` — free `#[pyfunction]`s the facade's type module calls. Descriptors cross the boundary as tagged tuples (`("kind", …)` positional slots) outbound and dicts inbound; Arrow types cross as `"arrow_schema"` C-Data-Interface capsules both directions (`__arrow_c_schema__` in, `pa.DataType._import_from_c_capsule` out). Exposes `spark_descriptor_from_ddl`, `spark_descriptor_from_arrow_type`, `spark_descriptor_from_arrow_schema`, `arrow_type_capsule_from_descriptor`, `sql_token_to_arrow_capsule`, the four token surfaces (`simple_string`, engine, DDL, SQL marker), `struct_field_ddl_from_descriptor`, `csv_sql_cast_token`, `csv_engine_token`, `csv_rung_descriptor` and `default_timestamp_descriptor`. Python keeps every behavior the table cannot carry: unknown-subtype descriptor trees, decimals outside the Arrow FFI scale envelope, collation refusal policy, and the `fromJson`/`jsonValue` surface. S2-21 remediation (P2-DICT, P3-COLLATION): every dict key and kind tag interned via `pyo3::intern!`; the inbound `kind`/collation extract as borrowed `Cow<str>` and the default collation borrows `DEFAULT_COLLATION` instead of allocating per node. Round-2 remediation (L-001): `TypeTableError::IntegerOverflow` maps to `PyOverflowError`; every other table error stays `PyValueError`. PY-P2-002/003: the emit side produces tagged tuples instead of `PyDict` trees and `csv_engine_token` answers rung engine casts without a `DataType` round-trip. **TYPES-GEO-DDL-1 (2026-09-15):** the two spatial tags cross both ways — `Geometry`/`Geography { srid }` emit as `("geometry"/"geography", srid)` tuples and build from `{"kind", "srid"}` dicts (`-1` is Spark's `any`, `repark_spark::type_table::SPATIAL_MIXED_SRID`). pins: facade-4/C-011, C-018, C-019, C-020, C-026, C-027; types-geo-ddl-1/C-002 |
| [`tests.rs`](tests.rs) | Unit pins for module registration and exception/type identity. |

## Boundary rules

- Python owns orchestration; Rust owns execution and ML kernels.
- Fallible PyO3 entry points use `fenced!` or `fenced_span!`; `note_local_write_root` is the
  infallible low-level trust-registration exception.
- Arrow export opens a stream without collecting. The reader polls one batch per callback.
- Arrow import holds the GIL while Python-backed exporters are drained and retains all non-empty
  batches; an empty stream still registers its schema.
- Keep Spark and native doors separate at session construction.

## Known limitations

- Stream-poll `KeyboardInterrupt` is deferred until the current poll returns.
- Excel and PostgreSQL readers are typed unsupported operations.
Measured 2026-08-29 against `73af134`:
- Direct low-level binding callers can bypass facade validation; preserve typed errors at this
  boundary and record measured base behavior here before a separate fix.
- BF-CC2-PYBIND-001 (S1): `note_local_write_root` lets direct callers trust an arbitrary local
  write root; the facade performs the intended validation.
- BF-CC2-PYBIND-002 (S1): logistic `max_iter=0` does not inspect a missing label column.
- BF-CC2-PYBIND-003 (S1): Int64 ML values beyond f64's exact range can round during conversion.
- BF-CC2-PYBIND-004 (S2): the generated `IllegalArgumentException` description mentions only
  invalid configuration, although ML errors also map to that exception.

## Navigation

See [crate navigation](../map.md), [Column navigation](column/map.md), and
[test navigation](../tests/map.md).

**PERF-DYNFLATTEN-1:** the module exposes `__debug_assertions__` from
`repark_core::built_with_debug_assertions()`, so the measurement harness can prove a release
build instead of guessing from the shared-object size.
pins: perf-dynflatten-1-measure/C-002
