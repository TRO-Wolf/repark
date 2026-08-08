//! PyO3 bindings exposing the repark engine to Python as the `repark._native` module.
//!
//! The pure-Python `repark` package (under `python/repark`) imports from here and presents
//! the near-drop-in PySpark surface (`from repark import ReparkSession`). All compute happens in
//! Rust; data crosses the boundary as Apache Arrow via the C Data Interface — zero-copy, no
//! serialization. This is the only crate permitted to use `unsafe` (the PyO3 FFI boundary).
//!
//! ## Layout
//!
//! - [`session`] — [`session::PyReparkSession`], the synchronous Python wrapper over the async
//!   [`repark_core::ReparkSession`]. Every session `block_on`s through a process-wide shared
//!   Tokio runtime handle ([`repark_core::EngineRuntime`] in a `OnceLock`), not a per-constructor
//!   runtime.
//! - [`dataframe`] — [`dataframe::PyDataFrame`], wrapping a DataFusion `DataFrame`, with
//!   `collect`/`count`/`show`, the zero-copy Arrow handoff (`__arrow_c_stream__`), and the
//!   transform surface (`with_column`/`filter`/`select`/`drop`/`sort`/`join_*`).
//! - [`column`] — [`column::PyColumn`], wrapping a DataFusion `Expr`: the `col`/`lit`/`expr`
//!   constructors, the operator set, `alias`, and `cast`.
//!
//! ## Arrow handoff
//!
//! The boundary uses the **Arrow `PyCapsule` interface** (`__arrow_c_stream__`) built on
//! [`arrow::ffi_stream::FFI_ArrowArrayStream`]. It is independent of the `pyo3` version (no
//! `arrow-pyarrow` pin to reconcile) and is exactly what `pyarrow.table(df)` /
//! `polars.from_arrow(df)` consume.

mod column;
mod dataframe;
mod fence;
mod ml;
mod session;

use datafusion::error::DataFusionError;
use pyo3::prelude::*;
use repark_core::ErrorClass;

pub use column::PyColumn;
pub use dataframe::PyDataFrame;
pub use session::PyReparkSession;

// The PySpark-shaped exception taxonomy (WG-3; U4 added `UnsupportedOperationException`;
// Group X added `IllegalArgumentException`). Defined here in the native module so an exception
// raised by the Rust engine is the SAME class object the Python facade re-exports from
// `repark.errors` — `except AnalysisException` catches an engine analysis error by identity, not
// by message-sniffing. All subclass `RuntimeError` (via `PySparkException`), so existing
// `except RuntimeError` code keeps working after a PySpark → repark migration (near-drop-in).
//
// A leaf type lands here ONLY with ≥1 reachable engine raise mapped to it (the Group S rule) —
// which is why PySpark's `ArithmeticException`, `NumberFormatException`, `DateTimeException`,
// `ArrayIndexOutOfBoundsException`, `SparkRuntimeException`, … are absent: repark has no error
// that classifies into them today (see the Group X ledger in `task/todo.md`). The
// Python-argument-validation leaves (`PySparkValueError`/`PySparkTypeError`/
// `PySparkAttributeError`) are raised by the pure-Python facade only and are defined in
// `python/repark/src/repark/errors.py` — they need MULTIPLE bases (`PySparkException` + the
// builtin), which `pyo3::create_exception!` cannot express.
/// The exception taxonomy lives in [`exceptions`] (file-backed; see its module doc for the
/// module-scoped `disallowed_methods` expectation and the P-4/P-5 provocation record).
mod exceptions;
pub use exceptions::{
    AnalysisException, IllegalArgumentException, ParseException, PySparkException,
    UnsupportedOperationException,
};

/// ===========================================================================================
/// Convert a crate-wide [`repark_core::Error`] into the matching PySpark-shaped Python exception.
///
/// The taxonomy boundary: [`repark_core::Error::exception_class`] enumerates every variant into a
/// [`ErrorClass`] (an exhaustive, no-`_` match in `repark-core` — a new variant fails to compile
/// until routed), and this function maps that class to a concrete exception. The `ErrorClass` match
/// is itself exhaustive (no `_`), so both hops are compile-time-checked: no silent default arm can
/// swallow a new class into the wrong Python type. The underlying engine message is preserved
/// verbatim in `str(exc)` (the cause chain). Takes the error by value so it slots into `.map_err`.
/// ===========================================================================================
#[allow(clippy::needless_pass_by_value)]
fn to_py_err(err: repark_core::Error) -> PyErr {
    let message = err.to_string();
    match err.exception_class() {
        ErrorClass::Parse => ParseException::new_err(message),
        ErrorClass::Analysis => AnalysisException::new_err(message),
        ErrorClass::Unsupported => UnsupportedOperationException::new_err(message),
        ErrorClass::IllegalArgument => IllegalArgumentException::new_err(message),
        ErrorClass::Base => PySparkException::new_err(message),
    }
}

/// ===========================================================================================
/// Convert a raw [`DataFusionError`] into a classified Python exception.
///
/// The DataFrame-op and `F.expr` surfaces produce `DataFusionError`s directly (rather than a
/// `repark_core::Error`), so they route through the shared `repark_core::engine_err` classifier
/// and then [`to_py_err`] — a parse error becomes `ParseException`, an analysis error
/// `AnalysisException`, everything else the base `PySparkException`. One boundary, one taxonomy.
/// ===========================================================================================
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn datafusion_to_py_err(err: DataFusionError) -> PyErr {
    to_py_err(repark_core::engine_err(err))
}

/// ===========================================================================================
/// Env-gated `tracing` subscriber for live phase profiles through the wheel (R-TRACE-SUBSCRIBER).
///
/// Prefer `REPARK_LOG` (repark-native; does not fight host `RUST_LOG` tooling). Fall back to
/// `RUST_LOG` when `REPARK_LOG` is unset/empty so existing docs that say
/// `RUST_LOG=repark_core=info` keep working once the wheel loads.
///
/// - Absent both env vars → **no** subscriber (zero overhead; `tracing` macros stay no-ops).
/// - Set → `tracing_subscriber::fmt` on stderr with `EnvFilter` + [`FmtSpan::CLOSE`] so each
///   span logs measured duration (the live MERGE phase profile depends on close timings).
/// - [`try_init`](tracing_subscriber::util::SubscriberInitExt::try_init) — never panics if a
///   subscriber already exists (tests, embedding hosts).
///
/// Runs once from the pymodule entry (import of `repark._native`); not per-session.
/// ===========================================================================================
fn try_init_repark_tracing() {
    use std::sync::Once;

    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt::format::FmtSpan;

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter_directive = match std::env::var("REPARK_LOG") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => match std::env::var("RUST_LOG") {
                Ok(value) if !value.trim().is_empty() => value,
                _ => return,
            },
        };
        let filter =
            EnvFilter::try_new(filter_directive.trim()).unwrap_or_else(|_| EnvFilter::new("info"));
        // try_init: Ok if we won the global slot; Err if another subscriber is already installed.
        // Either way we never panic — a second import / host-installed subscriber is fine.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

/// ===========================================================================================
/// The native module entry point.
///
/// The function name must match the `[lib] name` (`_native`) and maturin's `module-name`
/// (`repark._native`). The pure-Python facade imports [`PyReparkSession`] / [`PyDataFrame`]
/// from here and presents them as `ReparkSession` / `DataFrame`.
/// ===========================================================================================
#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // R-TRACE-SUBSCRIBER: install before any engine work can emit spans (import-time, once).
    try_init_repark_tracing();
    module.add("__doc__", "repark native engine (PyO3 bindings).")?;
    module.add_class::<PyReparkSession>()?;
    module.add_class::<PyDataFrame>()?;
    module.add_class::<PyColumn>()?;
    // The error taxonomy (WG-3). `repark.errors` re-exports these by identity.
    module.add(
        "PySparkException",
        module.py().get_type::<PySparkException>(),
    )?;
    module.add(
        "AnalysisException",
        module.py().get_type::<AnalysisException>(),
    )?;
    module.add("ParseException", module.py().get_type::<ParseException>())?;
    module.add(
        "UnsupportedOperationException",
        module.py().get_type::<UnsupportedOperationException>(),
    )?;
    module.add(
        "IllegalArgumentException",
        module.py().get_type::<IllegalArgumentException>(),
    )?;
    // M3 native estimators: streaming fit entry points (params-only results).
    ml::register(module)?;
    Ok(())
}

#[cfg(test)]
mod tests;
