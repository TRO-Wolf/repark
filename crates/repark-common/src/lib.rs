//! Shared domain types and the crate-wide error type for the repark engine.
//!
//! `repark-common` sits at the bottom of the dependency DAG. It carries the types that every
//! other crate agrees on (engine config, identifiers, the top-level [`Error`]) and deliberately
//! depends on nothing from the rest of the workspace — this is what breaks the otherwise-circular
//! `session ↔ sql` relationship.
//!
//! Domain types and error variants land alongside the code that needs them.

pub mod surfaces;

use thiserror::Error;

/// ===========================================================================================
/// The crate-wide error type for repark.
///
/// **Error boundary (C1-CRATE-001 honesty):** library crates *should* return
/// `Result<T, repark_common::Error>` at public seams so the Python binding can convert one enum
/// to a PySpark-shaped exception. In practice today, some crates still surface sibling error
/// types at intermediate layers (`iceberg::Result` from `repark-write` alter primitives;
/// `DataFusionError` from `repark-sql` / catalog wiring) and fold into `repark_common::Error` at
/// the session / PyO3 boundary via `engine_err` / `iceberg_err`. That fold is the real boundary —
/// not every crate has been retyped onto `repark_common::Error` end-to-end. Variants are added as
/// subsystems land — keep them specific (no catch-all `String` variant) so callers can match
/// meaningfully.
/// ===========================================================================================
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An operation the engine deterministically does not support (yet): the documented scope
    /// gates (partitioned / merge-on-read / `NOT MATCHED BY SOURCE` MERGE, CTAS partition
    /// transforms, ALTER v1 limits, …). The classifier (`repark_session::engine_err`) folds
    /// `DataFusionError::NotImplemented` into this variant, so the PyO3 boundary can raise
    /// `repark.errors.UnsupportedOperationException` — the class PySpark raises for a JVM
    /// `UnsupportedOperationException` (pyspark v3.5.1 `errors/exceptions/captured.py`
    /// `convert_exception`; hierarchy `UnsupportedOperationException(PySparkException)` in
    /// `errors/exceptions/base.py`). The message is the underlying engine text verbatim (`{0}`),
    /// so the original gate diagnostic survives in `str(exc)`.
    #[error("{0}")]
    NotImplemented(String),

    /// An error surfaced by the DataFusion execution engine (planning, optimization, execution).
    ///
    /// Held as a formatted message rather than the `DataFusionError` itself so this crate stays at
    /// the bottom of the DAG with no heavy dependencies; the originating crate (e.g.
    /// `repark-session`) does the `DataFusionError -> Error` conversion. This is the catch-all
    /// engine bucket: the parse and analysis sub-classes below are split out of it so the Python
    /// (PyO3) boundary can raise the matching PySpark exception type — everything the classifier
    /// (`repark_session::engine_err`) cannot place as parse or analysis stays here.
    #[error("datafusion engine error: {0}")]
    DataFusion(String),

    /// A SQL or expression **syntax** error (the query/expression did not parse). Split out of
    /// [`Error::DataFusion`] so the PyO3 boundary can raise `repark.errors.ParseException` — the
    /// PySpark name. The message is the underlying engine text verbatim (`{0}`), so the original
    /// diagnostic survives in `str(exc)`.
    #[error("{0}")]
    Parse(String),

    /// A query **analysis / planning** error — an unresolved table or column, a type error, an
    /// otherwise-invalid plan. Split out of [`Error::DataFusion`] so the PyO3 boundary can raise
    /// `repark.errors.AnalysisException` — the PySpark name. The message is the underlying engine
    /// text verbatim (`{0}`), so the original diagnostic survives in `str(exc)`.
    #[error("{0}")]
    Analysis(String),

    /// A session/catalog configuration error: a `spark.sql.catalog.<name>.*` block (or another
    /// config key) that cannot be mapped to a valid engine configuration. The message names the
    /// exact config key at fault so the user can fix their `.config(...)` call.
    ///
    /// Routes to [`ErrorClass::IllegalArgument`] — Spark rejects an invalid config VALUE with a
    /// JVM `IllegalArgumentException`, which PySpark surfaces as
    /// `pyspark.errors.IllegalArgumentException` (Group X live oracle, pyspark 4.0.0: setting
    /// `spark.sql.shuffle.partitions=-1` raises `IllegalArgumentException: '-1' in
    /// spark.sql.shuffle.partitions is invalid…`, and `spark.sql.ansi.enabled=notabool` raises
    /// `IllegalArgumentException: … should be boolean, but was notabool`).
    #[error("repark config error: {0}")]
    Config(String),

    /// An iceberg-origin error that is neither an unsupported feature nor a catalog analysis
    /// error: commit conflicts, invalid data/metadata, unexpected internal failures. Split out of
    /// [`Error::DataFusion`] so an iceberg failure is not mislabeled as a DataFusion engine error
    /// and stays matchable at the seams. The message is the iceberg error's own rendering, which
    /// LEADS with the structured `ErrorKind` name (`"CatalogCommitConflicts, … => …"`), so the
    /// kind survives to `str(exc)` verbatim (`{0}`). Classification by kind happens in
    /// `repark_session` on the live error (`FeatureUnsupported` → [`Error::NotImplemented`],
    /// not-found / already-exists kinds → [`Error::Analysis`], everything else → here); this
    /// crate stays at the bottom of the DAG, so it carries the already-classified residue only.
    #[error("{0}")]
    Iceberg(String),
}

/// ===========================================================================================
/// The PySpark exception partition an [`Error`] maps onto at the Python (PyO3) boundary.
///
/// This is the single enumerated variant→exception routing the error taxonomy requires. The
/// match in [`Error::exception_class`] is **exhaustive with no `_` arm**, so a new [`Error`]
/// variant added later fails to compile until it is explicitly routed here — no silent default can
/// swallow a new variant into the wrong exception type. The PyO3 crate maps each class to a
/// concrete Python exception (`Parse` → `ParseException`, `Analysis` → `AnalysisException`,
/// `Unsupported` → `UnsupportedOperationException`, `IllegalArgument` →
/// `IllegalArgumentException`, `Base` → the `RuntimeError`-compatible `PySparkException`).
/// ===========================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// A SQL/expression syntax error → `repark.errors.ParseException`.
    Parse,
    /// A planning / analysis error → `repark.errors.AnalysisException`.
    Analysis,
    /// A deterministically unsupported operation (a scope gate, an unsupported iceberg feature)
    /// → `repark.errors.UnsupportedOperationException` — the class PySpark raises for a JVM
    /// `UnsupportedOperationException` (pyspark v3.5.1 `convert_exception`).
    Unsupported,
    /// An invalid engine/catalog CONFIGURATION value → `repark.errors.IllegalArgumentException`,
    /// the class PySpark raises for a JVM `IllegalArgumentException` (`captured.py`
    /// `convert_exception`). Group X: the only [`Error`] variant repark can classify here today is
    /// [`Error::Config`] — a `.config(...)` key/value the session cannot map — which matches what
    /// live pyspark 4.0.0 raises for an invalid `SQLConf` value.
    IllegalArgument,
    /// Everything else (execution, IO, iceberg commit/data errors, …) → the
    /// `RuntimeError`-compatible base `repark.errors.PySparkException`.
    Base,
}

/// ===========================================================================================
/// Classify each [`Error`] variant into the [`ErrorClass`] partition the PyO3 boundary raises as a
/// PySpark-shaped exception.
///
/// Exhaustive by construction (no `_` arm) — the compile-time guarantee that every variant is
/// routed. See [`ErrorClass`].
/// ===========================================================================================
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

/// Convenient alias used throughout the workspace.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
