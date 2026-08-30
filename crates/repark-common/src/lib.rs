//! Shared domain types and the crate-wide error type for the repark engine.

pub mod surfaces;

use thiserror::Error;

/// The crate-wide error type for repark.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The engine does not support this operation.
    #[error("{0}")]
    NotImplemented(String),

    /// A DataFusion planning, optimization, or execution error stored without heavy dependencies.
    #[error("datafusion engine error: {0}")]
    DataFusion(String),

    /// A SQL or expression syntax error rendered verbatim for PySpark's `ParseException`.
    #[error("{0}")]
    Parse(String),

    /// A query analysis or planning error rendered verbatim for PySpark's `AnalysisException`.
    #[error("{0}")]
    Analysis(String),

    /// A session or catalog configuration error naming the invalid key.
    #[error("repark config error: {0}")]
    Config(String),

    /// An Iceberg error other than an unsupported feature or catalog analysis error.
    #[error("{0}")]
    Iceberg(String),
}

/// The PySpark exception partition an [`Error`] maps onto at the Python (PyO3) boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// A SQL or expression syntax error mapped to `repark.errors.ParseException`.
    Parse,
    /// A planning or analysis error mapped to `repark.errors.AnalysisException`.
    Analysis,
    /// A deterministic unsupported operation mapped to `UnsupportedOperationException`.
    Unsupported,
    /// An invalid engine or catalog config value mapped to `IllegalArgumentException`.
    IllegalArgument,
    /// Everything else mapped to the `RuntimeError`-compatible `repark.errors.PySparkException`.
    Base,
}

/// Classifies each [`Error`] variant into the PySpark exception partition.
impl Error {
    /// The PySpark exception partition this error belongs to (see [`ErrorClass`]).
    #[must_use]
    pub fn exception_class(&self) -> ErrorClass {
        match self {
            Error::Parse(_) => ErrorClass::Parse,
            Error::Analysis(_) => ErrorClass::Analysis,
            Error::NotImplemented(_) => ErrorClass::Unsupported,
            Error::Config(_) => ErrorClass::IllegalArgument,
            Error::DataFusion(_) | Error::Iceberg(_) => ErrorClass::Base,
        }
    }
}

/// Result type using the crate-wide [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
