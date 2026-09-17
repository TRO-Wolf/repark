//! Classify DataFusion and Iceberg errors into the crate-wide error taxonomy.

use std::collections::HashMap;

use datafusion::error::DataFusionError;
use iceberg::ErrorKind;
use repark_common::{Error, Result};
use repark_iceberg::write::CommitStateUnknownError;

use crate::object_store_s3;

#[derive(Debug)]
pub struct IllegalArgumentMarker(pub String);

impl std::fmt::Display for IllegalArgumentMarker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for IllegalArgumentMarker {}

#[must_use]
pub fn illegal_argument_error(message: String) -> DataFusionError {
    DataFusionError::External(Box::new(IllegalArgumentMarker(message)))
}

/// DataFusion error partition used before conversion to [`Error`].
#[derive(Debug)]
pub(crate) enum EngineErrorKind<'a> {
    Parse,
    Analysis,
    /// `DataFusionError::NotImplemented`.
    Unsupported,
    IllegalArgument,
    IllegalArgumentMarked(&'a IllegalArgumentMarker),
    /// A peeled `External` wrapping a live [`iceberg::Error`], classified by its `kind()`.
    Iceberg(&'a iceberg::Error),
    CommitStateUnknown(&'a CommitStateUnknownError),
    Other,
}

/// Cap wrapper peeling.
pub(crate) const MAX_ERROR_PEEL_DEPTH: usize = 32;

/// Classify a DataFusion error after peeling wrapper variants up to [`MAX_ERROR_PEEL_DEPTH`].
pub(crate) fn classify_datafusion_error(error: &DataFusionError) -> EngineErrorKind<'_> {
    let mut current = error;
    for _ in 0..MAX_ERROR_PEEL_DEPTH {
        match current {
            DataFusionError::SQL(_, _) => return EngineErrorKind::Parse,
            DataFusionError::Plan(_) | DataFusionError::SchemaError(_, _) => {
                return EngineErrorKind::Analysis;
            }
            DataFusionError::NotImplemented(_) => return EngineErrorKind::Unsupported,
            DataFusionError::Configuration(_) => return EngineErrorKind::IllegalArgument,
            DataFusionError::External(inner) => {
                return match inner.downcast_ref::<CommitStateUnknownError>() {
                    Some(stamped) => EngineErrorKind::CommitStateUnknown(stamped),
                    None => match inner.downcast_ref::<IllegalArgumentMarker>() {
                        Some(marker) => EngineErrorKind::IllegalArgumentMarked(marker),
                        None => match inner.downcast_ref::<iceberg::Error>() {
                            Some(iceberg_error) => EngineErrorKind::Iceberg(iceberg_error),
                            None => EngineErrorKind::Other,
                        },
                    },
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

#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn engine_err_for_sql(sql: &str, err: DataFusionError) -> Error {
    if !matches!(classify_datafusion_error(&err), EngineErrorKind::Parse)
        && let Some(message) =
            crate::unknown_routine::map_unknown_routine_message(sql, &err.to_string())
    {
        return Error::Analysis(message);
    }
    engine_err(err)
}

/// Convert one DataFusion error into the crate-wide [`Error`] taxonomy.
#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn engine_err(err: DataFusionError) -> Error {
    match classify_datafusion_error(&err) {
        EngineErrorKind::Parse => Error::Parse(err.to_string()),
        EngineErrorKind::Analysis => Error::Analysis(err.to_string()),
        EngineErrorKind::Unsupported => Error::NotImplemented(err.to_string()),
        EngineErrorKind::IllegalArgument => Error::IllegalArgument(err.to_string()),
        EngineErrorKind::IllegalArgumentMarked(marker) => Error::IllegalArgument(marker.0.clone()),
        EngineErrorKind::Iceberg(iceberg_error) => classify_iceberg_error(iceberg_error),
        EngineErrorKind::CommitStateUnknown(stamped) => Error::CommitStateUnknown {
            message: stamped.inner().to_string(),
            operation_id: Some(stamped.operation_id().to_string()),
        },
        EngineErrorKind::Other => Error::DataFusion(err.to_string()),
    }
}

/// Resolve the optional S3 region override from the two accepted config spellings.
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

/// Classify a live [`iceberg::Error`] by structured [`iceberg::ErrorKind`] before choosing
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
        ErrorKind::CommitStateUnknown => Error::CommitStateUnknown {
            message,
            operation_id: None,
        },
        #[allow(clippy::match_same_arms)]
        ErrorKind::PreconditionFailed
        | ErrorKind::Unexpected
        | ErrorKind::DataInvalid
        | ErrorKind::CatalogCommitConflicts => Error::Iceberg(message),
        _ => Error::Iceberg(message),
    }
}

/// Convert an iceberg error into the crate-wide [`Error`], classified by its structured kind.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn iceberg_err(err: iceberg::Error) -> Error {
    classify_iceberg_error(&err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn illegal_argument_marker_maps_to_illegal_argument() {
        let error = engine_err(illegal_argument_error(
            "Cannot use options [foo]".to_string(),
        ));
        assert_eq!(
            error.exception_class(),
            repark_common::ErrorClass::IllegalArgument
        );
        assert!(
            matches!(error, Error::IllegalArgument(message) if message == "Cannot use options [foo]")
        );
    }

    #[test]
    fn illegal_argument_marker_survives_context_wrapping() {
        let wrapped = DataFusionError::Context(
            "rewrite".to_string(),
            Box::new(illegal_argument_error("boom".to_string())),
        );
        let error = engine_err(wrapped);
        assert!(matches!(error, Error::IllegalArgument(_)));
    }
}
