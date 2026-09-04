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
| [`exceptions.rs`](exceptions.rs) | PySpark-shaped exception types. |
| [`fence.rs`](fence.rs) | Panic fences for PyO3 methods and Arrow stream polls. |
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
