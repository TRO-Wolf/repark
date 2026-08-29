# map — repark-python/src

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
| [`session.rs`](session.rs) | Shared runtime, session doors, readers, catalogs, and temp views. |
| [`dataframe.rs`](dataframe.rs) | Lazy plans, actions, transforms, schema, and Arrow C Stream export. |
| [`column/`](column/map.md) | Immutable expressions, scalar functions, aggregates, and windows. |
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
