//! Classify DataFusion and Iceberg errors into the crate-wide error taxonomy.

use std::collections::HashMap;

use datafusion::error::DataFusionError;
use iceberg::ErrorKind;
use repark_common::{Error, Result};

use crate::object_store_s3;

/// DataFusion error partition used before conversion to [`Error`]. Iceberg errors remain borrowed
/// so their structured [`ErrorKind`] survives classification.
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

/// Cap wrapper peeling; exceeding this limit returns [`EngineErrorKind::Other`] and guarantees termination.
pub(crate) const MAX_ERROR_PEEL_DEPTH: usize = 32;

/// ===========================================================================================
/// Classify a DataFusion error after peeling wrapper variants up to
/// [`MAX_ERROR_PEEL_DEPTH`]. Structured Iceberg errors reach [`classify_iceberg_error`].
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
/// Convert one DataFusion error into the crate-wide [`Error`] taxonomy.
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
/// Classify a live [`iceberg::Error`] by structured [`iceberg::ErrorKind`] before choosing [`Error`].
/// Both direct and peeled external routes use this mapping: `FeatureUnsupported` is
/// [`Error::NotImplemented`], not-found and already-exists kinds are [`Error::Analysis`], and all
/// other current or future kinds are [`Error::Iceberg`]. Preserve Iceberg's own display message.
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
