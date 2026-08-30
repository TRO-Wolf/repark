//! PyO3 bindings expose the repark engine as `repark._native`.

#[cfg(feature = "allocator-mimalloc")]
mod allocator;
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

/// The exception taxonomy lives in [`exceptions`]; see that module for the lint expectation.
mod exceptions;
pub use exceptions::{
    AnalysisException, IllegalArgumentException, ParseException, PySparkException,
    UnsupportedOperationException,
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
    module.add_class::<PyReparkSession>()?;
    module.add_class::<PyDataFrame>()?;
    module.add_class::<PyColumn>()?;
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
    ml::register(module)?;
    Ok(())
}

#[cfg(test)]
mod tests;
