//! DataFusion/iceberg/postgres error classification into crate-wide [`repark_common::Error`].
//!
//! Extracted MOVE-ONLY from `lib.rs` (r25 T0). Zero behavior change.

use std::collections::HashMap;

use datafusion::error::DataFusionError;
use iceberg::ErrorKind;
use repark_common::{Error, Result};

use crate::object_store_s3;

/// The partition a raw [`DataFusionError`] belongs to, before it is folded into a [`Error`].
/// Mirrors [`repark_common::ErrorClass`] but is derived from the live *DataFusion* variant — the
/// only place the parse-vs-analysis-vs-unsupported-vs-execution distinction still exists (once
/// the error is stringified it is gone). The `Iceberg` arm carries the PEELED live iceberg error
/// (borrowed from the classified `DataFusionError`), so its structured `ErrorKind` — lost the
/// moment anything stringifies — reaches [`classify_iceberg_error`] intact (audit CQ-015).
#[derive(Debug)]
pub(crate) enum EngineErrorKind<'a> {
    Parse,
    Analysis,
    /// `DataFusionError::NotImplemented` — the deterministic scope gates ride this variant
    /// (audit CQ-002); folded to `Error::NotImplemented` → `UnsupportedOperationException`.
    Unsupported,
    /// A peeled `External` wrapping a live [`iceberg::Error`], classified by its `kind()`.
    Iceberg(&'a iceberg::Error),
    Other,
}

/// Hard cap on wrapper-peeling iterations (see [`classify_datafusion_error`]). DataFusion wraps a
/// small, bounded number of times; 32 is far beyond any real nesting and guarantees termination.
pub(crate) const MAX_ERROR_PEEL_DEPTH: usize = 32;

/// ===========================================================================================
/// Classify a [`DataFusionError`] into the parse / analysis / unsupported / iceberg / other
/// partition.
///
/// DataFusion loses the distinction the moment the error is stringified, so classification happens
/// here, on the live error. Wrapper variants (`Context` / `Diagnostic` / `Shared` / `Collection`)
/// are peeled **iteratively** to the innermost meaningful error — the iteration cap
/// ([`MAX_ERROR_PEEL_DEPTH`]) makes even a pathologically nested error incapable of overflowing the
/// stack (no unbounded recursion over an engine-produced structure). `SQL` → parse; `Plan` /
/// `SchemaError` → analysis (unresolved table or column, a type/plan error); `NotImplemented` →
/// unsupported (the deterministic scope gates — audit CQ-002); `External` carrying an
/// [`iceberg::Error`] (the repark-sql / repark-write / fork-provider folds all box the live error)
/// → the iceberg arm, handing the peeled error to [`classify_iceberg_error`] so its structured
/// `ErrorKind` survives (audit CQ-015); everything else (execution, IO, internal, configuration,
/// non-iceberg external, …) → other, which becomes the `RuntimeError`-compatible base exception.
/// ===========================================================================================
pub(crate) fn classify_datafusion_error(error: &DataFusionError) -> EngineErrorKind<'_> {
    let mut current = error;
    for _ in 0..MAX_ERROR_PEEL_DEPTH {
        match current {
            DataFusionError::SQL(_, _) => return EngineErrorKind::Parse,
            DataFusionError::Plan(_) | DataFusionError::SchemaError(_, _) => {
                return EngineErrorKind::Analysis;
            }
            DataFusionError::NotImplemented(_) => return EngineErrorKind::Unsupported,
            DataFusionError::External(inner) => {
                return match inner.downcast_ref::<iceberg::Error>() {
                    Some(iceberg_error) => EngineErrorKind::Iceberg(iceberg_error),
                    None => EngineErrorKind::Other,
                };
            }
            DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
                current = &**inner;
            }
            DataFusionError::Shared(inner) => current = &**inner,
            DataFusionError::Collection(errors) => match errors.first() {
                Some(first) => current = first,
                None => return EngineErrorKind::Other,
            },
            _ => return EngineErrorKind::Other,
        }
    }
    EngineErrorKind::Other
}

/// ===========================================================================================
/// Convert a DataFusion engine error into the crate-wide [`Error`], **classified** into the parse /
/// analysis / base partition.
///
/// The single `DataFusionError -> Error` boundary for the workspace: the session `sql` path and the
/// PyO3 DataFrame-op / `F.expr` surface (`repark-python`) both route their DataFusion errors through
/// here, so the Python exception taxonomy (`ParseException` / `AnalysisException` / the base
/// `PySparkException`) is driven off one classifier rather than three ad-hoc conversions. (Orphan
/// rules forbid a blanket `From<DataFusionError>` — neither type is local — so it is a plain public
/// helper.) Takes the error by value so it slots straight into `.map_err`.
/// ===========================================================================================
#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn engine_err(err: DataFusionError) -> Error {
    match classify_datafusion_error(&err) {
        EngineErrorKind::Parse => Error::Parse(err.to_string()),
        EngineErrorKind::Analysis => Error::Analysis(err.to_string()),
        EngineErrorKind::Unsupported => Error::NotImplemented(err.to_string()),
        EngineErrorKind::Iceberg(iceberg_error) => classify_iceberg_error(iceberg_error),
        EngineErrorKind::Other => Error::DataFusion(err.to_string()),
    }
}

/// Resolve the optional S3 region override from the two accepted config spellings.
///
/// Identical values under both keys collapse; different values fail loud naming both keys
/// (never a silent prefer-one-spelling pick).
pub(crate) fn resolve_s3_region_override(
    config: &HashMap<String, String>,
) -> Result<Option<String>> {
    let repark = config.get(object_store_s3::REPARK_S3A_REGION_CONFIG_KEY);
    let spark = config.get(object_store_s3::S3A_REGION_CONFIG_KEY);
    match (repark, spark) {
        (Some(left), Some(right)) if left != right => Err(Error::Config(format!(
            "conflicting S3 region config: `{}` and `{}` set different values",
            object_store_s3::REPARK_S3A_REGION_CONFIG_KEY,
            object_store_s3::S3A_REGION_CONFIG_KEY,
        ))),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value.clone())),
        (None, None) => Ok(None),
    }
}

/// ===========================================================================================
/// Classify a live [`iceberg::Error`] into the crate-wide [`Error`] by its structured
/// [`iceberg::ErrorKind`] — the ONE kind→class mapping for the workspace (audit CQ-004/CQ-015).
///
/// Both routes converge here: the direct session fold ([`iceberg_err`]) and the peeled
/// `DataFusionError::External` arm of [`classify_datafusion_error`] (the repark-sql /
/// repark-write / fork-provider folds). The mapping follows the PySpark v3.5.1 oracle (U4
/// design records, `task/todo.md`): `FeatureUnsupported` → [`Error::NotImplemented`] (a JVM
/// `UnsupportedOperationException` → PySpark `UnsupportedOperationException`); the not-found /
/// already-exists catalog kinds → [`Error::Analysis`] (Spark's `NoSuchTableException` /
/// `TableAlreadyExistsException` families all extend `AnalysisException`); everything else
/// (commit conflicts, invalid data, unexpected, …) → [`Error::Iceberg`] — the base bucket,
/// PySpark itself types no Iceberg commit/validation error. The message is the iceberg error's
/// own Display, which leads with the kind name — canonical for both routes, so no
/// "External error:" / "datafusion engine error:" wrapper misattributes the origin. The `_` arm
/// is compiler-forced (`ErrorKind` is `#[non_exhaustive]` upstream) and routes FUTURE kinds to
/// the base bucket; all 12 current kinds are matched explicitly.
/// ===========================================================================================
pub(crate) fn classify_iceberg_error(error: &iceberg::Error) -> Error {
    let message = error.to_string();
    match error.kind() {
        ErrorKind::FeatureUnsupported => Error::NotImplemented(message),
        ErrorKind::TableNotFound
        | ErrorKind::NamespaceNotFound
        | ErrorKind::ViewNotFound
        | ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::ViewAlreadyExists => Error::Analysis(message),
        // The identical-body split is deliberate (allow below): the named arm documents the 12
        // CURRENT kinds' routing; the `_` arm exists only because `ErrorKind` is
        // #[non_exhaustive] upstream, so a FUTURE kind cannot be named here without a fork bump —
        // it lands in the iceberg base bucket, kind text still leading the message. (The
        // repark-core `exception_class` match stays no-`_`.)
        #[allow(clippy::match_same_arms)]
        ErrorKind::PreconditionFailed
        | ErrorKind::Unexpected
        | ErrorKind::DataInvalid
        | ErrorKind::CatalogCommitConflicts
        | ErrorKind::CommitStateUnknown => Error::Iceberg(message),
        _ => Error::Iceberg(message),
    }
}

/// Convert an iceberg error into the crate-wide [`Error`], classified by its structured kind
/// (see [`classify_iceberg_error`] — this is the direct-fold route; the External route peels).
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn iceberg_err(err: iceberg::Error) -> Error {
    classify_iceberg_error(&err)
}
