//! PyO3 bindings expose the repark engine as `repark._native`.

#[cfg(feature = "allocator-mimalloc")]
mod allocator;
mod arrow_export;
mod cache_budget;
mod catalog_census;
mod cdf_infer;
mod collect_rows;
mod column;
mod dataframe;
mod dataframe_stack;
mod dataframe_stats;
mod fence;
mod logical_names;
mod ml;
mod plan_introspect;
mod session;
mod session_runtime;
mod session_sources;
mod text_io;
mod type_bridge;

use datafusion::error::DataFusionError;
use pyo3::prelude::*;
use repark_core::ErrorClass;

pub use column::PyColumn;
pub use dataframe::PyDataFrame;
pub use session::PyReparkSession;

/// The exception taxonomy lives in [`exceptions`]; see that module for the lint expectation.
mod exceptions;
pub use exceptions::{
    AnalysisException, CommitStateUnknownException, IllegalArgumentException, ParseException,
    PySparkException, UnsupportedOperationException,
};

/// Convert a crate error to its PySpark-shaped Python exception.
#[allow(clippy::needless_pass_by_value)]
fn to_py_err(err: repark_core::Error) -> PyErr {
    let message = err.to_string();
    match err.exception_class() {
        ErrorClass::Parse => ParseException::new_err(message),
        ErrorClass::Analysis => AnalysisException::new_err(message),
        ErrorClass::Unsupported => UnsupportedOperationException::new_err(message),
        ErrorClass::IllegalArgument => IllegalArgumentException::new_err(message),
        ErrorClass::CommitStateUnknown => {
            let operation_id = match &err {
                repark_core::Error::CommitStateUnknown { operation_id, .. } => operation_id.clone(),
                _ => None,
            };
            let raised = CommitStateUnknownException::new_err(message);
            Python::attach(|py| {
                if let Err(failure) = raised.value(py).setattr("operation_id", operation_id) {
                    tracing::warn!(error = %failure, "operation_id setattr failed");
                }
                raised
            })
        }
        ErrorClass::Base => PySparkException::new_err(message),
    }
}

/// Convert a [`DataFusionError`] through the shared engine classifier.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn datafusion_to_py_err(err: DataFusionError) -> PyErr {
    to_py_err(repark_core::engine_err(err))
}

/// Install the optional environment-gated tracing subscriber once at module import.
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
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

/// The native module entry point.
#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    try_init_repark_tracing();
    module.add("__doc__", "repark native engine (PyO3 bindings).")?;
    module.add(
        "__debug_assertions__",
        repark_core::built_with_debug_assertions(),
    )?;
    module.add_class::<PyReparkSession>()?;
    module.add_class::<PyDataFrame>()?;
    module.add_class::<PyColumn>()?;
    module.add_class::<column::display::PyColumnParts>()?;
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
    module.add(
        "CommitStateUnknownException",
        module.py().get_type::<CommitStateUnknownException>(),
    )?;
    dataframe_stack::register(module)?;
    dataframe_stats::register(module)?;
    cache_budget::register(module)?;
    catalog_census::register(module)?;
    cdf_infer::register(module)?;
    collect_rows::register(module)?;
    logical_names::register(module)?;
    ml::register(module)?;
    plan_introspect::register(module)?;
    session_runtime::register(module)?;
    session_sources::register(module)?;
    text_io::register(module)?;
    type_bridge::register(module)?;
    Ok(())
}

#[cfg(test)]
mod tests;
