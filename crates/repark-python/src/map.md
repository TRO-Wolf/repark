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
| [`arrow_export.rs`](arrow_export.rs) | Arrow C Stream export boundary: coerces Utf8View to Utf8 so `collect`/`to_arrow` read Spark-equal string types (CUTOVER-SCHEMA-1, 2026-09-04). Round 3 (2026-09-05): `coerce_batch_views` casts any analyzed-vs-physical mismatch under safe Arrow cast options — a per-batch copy; non-string mismatches either widen losslessly or refuse loud (see the two coercion pins). The four `StreamingBatchReader` comments moved here verbatim from `dataframe.rs`. **H3-SPILL-RESIDUE-1 (2026-09-06):** the reader carries the session's `PoolRefusalLog` (`with_refusals`, fed by `refusal_log`) and the refusal count it started with. When a poll comes back as a fenced panic AND the pool refused an allocation after the reader opened, `as_pool_refusal` replaces the internal-error report with the engine's own `Resources exhausted` text plus one disclosure line, and logs the contained panic detail at `warn` on `repark::spill`. Only a fenced panic is rewritten, only when a refusal was recorded, and an unbounded session has no log at all — three separate gates, each with its own pin. The disclosure line is a `concat!` rather than a `\`-continued literal: rustfmt folds the continuation onto one line and leaves the indent's spaces inside the string. pins: h3-spill-residue-1/C-002 |
| [`exceptions.rs`](exceptions.rs) | PySpark-shaped exception types. |
| [`fence.rs`](fence.rs) | Panic fences for PyO3 methods and Arrow stream polls. `fenced_panic_detail` tells a fenced panic apart from an ordinary stream error by downcasting the Arrow external error, which is what lets `arrow_export.rs` rewrite only the panics. pins: h3-spill-residue-1/C-002 |
| [`session.rs`](session.rs) | Shared runtime, session doors, readers, catalogs, and temp views.
  `PyReparkSession.sql` runs the FNP-15/16 declared-function valve so the native
  `repark.sql()` callable (DataFusionDialect) refuses with the registry reason.
  Native and Spark Python sessions install F-Y10-1 integer overflow checks.
  Native `PyReparkSession::native` registers LOG1P-1 `log1p` / `expm1` (DataFusionDialect
  has no `on_session_built` hook; Spark door gets them from `register_all`).
  pins: log1p-1-precise-kernels/C-002
  The Spark-door routing probe is `MERGE … OUTPUT` (TRUNCATE and `INSERT OVERWRITE … PARTITION` are live).
  pins: fnp-15-16/C-001; dml-c-truncate/C-004 |
| [`dataframe.rs`](dataframe.rs) | Lazy plans, actions, transforms, schema, and Arrow C Stream export.
  `filter_sql` bypasses the statement router, so it applies parse-altitude valves itself. |
| [`column/`](column/map.md) | Immutable expressions, scalar functions, aggregates, and windows.
  `PyColumn.sql` also runs the FNP-15/16 declared-function valve (`refuse_declared_function_in_sql`). |
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
  pins: perf-facade-1/C-002; h3-spill-residue-1/C-001 |
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
