//! Classify DataFusion and Iceberg errors into the crate-wide error taxonomy.

use std::collections::HashMap;

use datafusion::error::DataFusionError;
use iceberg::ErrorKind;
use repark_common::{Error, Result};
use repark_iceberg::write::CommitStateUnknownError;
#[cfg(test)]
use repark_iceberg::write::unsupported_error;
pub use repark_iceberg::write::{IllegalArgumentMarker, UnsupportedMarker, illegal_argument_error};

use crate::object_store_s3;

/// DataFusion error partition used before conversion to [`Error`].
#[derive(Debug)]
pub(crate) enum EngineErrorKind<'a> {
    Parse,
    Analysis,
    /// `DataFusionError::NotImplemented`.
    Unsupported,
    IllegalArgument,
    IllegalArgumentMarked(&'a IllegalArgumentMarker),
    ArithmeticOverflow(&'a str),
    UnsupportedMarked(&'a UnsupportedMarker),
    /// A peeled `External` wrapping a live [`iceberg::Error`], classified by its `kind()`.
    Iceberg(&'a iceberg::Error),
    CommitStateUnknown(&'a CommitStateUnknownError),
    Other,
}

/// Cap wrapper peeling.
pub(crate) const MAX_ERROR_PEEL_DEPTH: usize = 32;

const ARITHMETIC_OVERFLOW_HEAD: &str = "[ARITHMETIC_OVERFLOW]";
const CAUSED_BY_SEPARATOR: &str = "\ncaused by\n";
const COERCION_FAILED_MARKER: &str = "user-defined coercion failed with: ";
const GROUPING_MISMATCH_HEAD: &str = "[GROUPING_ID_COLUMN_MISMATCH]";
const GROUPING_UNSUPPORTED_HEAD: &str = "[UNSUPPORTED_GROUPING_EXPRESSION]";
const PLAN_DISPLAY_PREFIX: &str = "Error during planning: ";
const SQLSTATE_MARKER: &str = "SQLSTATE: ";
const SQLSTATE_LEN: usize = 5;

fn coercion_refusal_message(display: &str) -> Option<String> {
    let rest = display.split(COERCION_FAILED_MARKER).nth(1)?;
    let rest = rest.strip_prefix(PLAN_DISPLAY_PREFIX).unwrap_or(rest);
    if !rest.starts_with('[') {
        return None;
    }
    let state_start = rest.find(SQLSTATE_MARKER)? + SQLSTATE_MARKER.len();
    let state = rest.get(state_start..state_start + SQLSTATE_LEN)?;
    if !state.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return None;
    }
    rest.get(..state_start + SQLSTATE_LEN).map(str::to_string)
}

fn grouping_refusal_message(display: &str) -> Option<String> {
    let rest = display
        .rsplit_once(CAUSED_BY_SEPARATOR)
        .map_or(display, |(_, tail)| tail);
    let rest = rest.strip_prefix(PLAN_DISPLAY_PREFIX).unwrap_or(rest);
    if !rest.starts_with(GROUPING_MISMATCH_HEAD) && !rest.starts_with(GROUPING_UNSUPPORTED_HEAD) {
        return None;
    }
    let state_start = rest.find(SQLSTATE_MARKER)? + SQLSTATE_MARKER.len();
    let state = rest.get(state_start..state_start + SQLSTATE_LEN)?;
    if !state.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return None;
    }
    rest.get(..state_start + SQLSTATE_LEN).map(str::to_string)
}

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
                        None => match inner.downcast_ref::<UnsupportedMarker>() {
                            Some(marker) => EngineErrorKind::UnsupportedMarked(marker),
                            None => match inner.downcast_ref::<iceberg::Error>() {
                                Some(iceberg_error) => EngineErrorKind::Iceberg(iceberg_error),
                                None => EngineErrorKind::Other,
                            },
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
            DataFusionError::Execution(message)
                if message.starts_with(ARITHMETIC_OVERFLOW_HEAD) =>
            {
                return EngineErrorKind::ArithmeticOverflow(message);
            }
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
        EngineErrorKind::Analysis => {
            let display = err.to_string();
            Error::Analysis(
                grouping_refusal_message(&display)
                    .or_else(|| coercion_refusal_message(&display))
                    .unwrap_or(display),
            )
        }
        EngineErrorKind::ArithmeticOverflow(message) => Error::Arithmetic(message.to_string()),
        EngineErrorKind::Unsupported => Error::NotImplemented(err.to_string()),
        EngineErrorKind::IllegalArgument => Error::IllegalArgument(err.to_string()),
        EngineErrorKind::IllegalArgumentMarked(marker) => Error::IllegalArgument(marker.0.clone()),
        EngineErrorKind::UnsupportedMarked(marker) => Error::NotImplemented(marker.0.clone()),
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
    fn unsupported_marker_maps_to_not_implemented_verbatim() {
        let error = engine_err(unsupported_error(
            "Creating a view is not supported by catalog: glue".to_string(),
        ));
        assert_eq!(
            error.exception_class(),
            repark_common::ErrorClass::Unsupported
        );
        assert!(matches!(error, Error::NotImplemented(message)
                if message == "Creating a view is not supported by catalog: glue"));
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

    #[test]
    fn arithmetic_overflow_execution_maps_to_arithmetic_verbatim() {
        let message = "[ARITHMETIC_OVERFLOW] conv overflow. If necessary set \"spark.sql.ansi.enabled\" \
             to \"false\" to bypass this error. SQLSTATE: 22003";
        let error = engine_err(DataFusionError::Execution(message.to_string()));
        assert_eq!(
            error.exception_class(),
            repark_common::ErrorClass::Arithmetic
        );
        assert!(matches!(error, Error::Arithmetic(text) if text == message));
    }

    #[test]
    fn arithmetic_overflow_survives_context_wrapping() {
        let wrapped = DataFusionError::Context(
            "compute".to_string(),
            Box::new(DataFusionError::Execution(
                "[ARITHMETIC_OVERFLOW] conv overflow. SQLSTATE: 22003".to_string(),
            )),
        );
        let error = engine_err(wrapped);
        assert!(matches!(error, Error::Arithmetic(_)));
    }

    #[test]
    fn plain_execution_stays_base() {
        let error = engine_err(DataFusionError::Execution("Cast error: boom".to_string()));
        assert_eq!(error.exception_class(), repark_common::ErrorClass::Base);
    }

    #[test]
    fn coercion_wrap_peels_to_the_refusal_payload() {
        let payload = "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"bin(<expr>)\" \
         due to data type mismatch: The first parameter requires the \"BIGINT\" type, however \
         the argument has the type \"BOOLEAN\". SQLSTATE: 42K09";
        let display = format!(
            "Execution error: Function 'bin' user-defined coercion failed with: Error during \
             planning: {payload}. No function matches the given name and argument types \
             'bin(Boolean)'. You might need to add explicit type casts."
        );
        let error = engine_err(DataFusionError::Plan(display));
        assert_eq!(error.exception_class(), repark_common::ErrorClass::Analysis);
        assert!(matches!(error, Error::Analysis(text) if text == payload));
    }

    #[test]
    fn grouping_rule_wrap_peels_to_the_bare_refusal() {
        let payload = "[GROUPING_ID_COLUMN_MISMATCH] Columns of grouping_id (k,g) does not \
             match grouping columns (g,k). SQLSTATE: 42803";
        let wrapped = DataFusionError::Context(
            "resolve_grouping_id".to_string(),
            Box::new(DataFusionError::Plan(payload.to_string())),
        );
        let error = engine_err(wrapped);
        assert!(matches!(error, Error::Analysis(text) if text == payload));
    }

    #[test]
    fn coercion_wrap_without_payload_keeps_the_full_display() {
        let display = "Execution error: Function 'last_day' user-defined coercion failed with: \
         Error during planning: unsupported type. No function matches.";
        let error = engine_err(DataFusionError::Plan(display.to_string()));
        assert!(matches!(error, Error::Analysis(text) if text.contains("No function matches")));
    }

    #[test]
    fn grouping_unsupported_peels_without_the_rule_wrap() {
        let payload = "[UNSUPPORTED_GROUPING_EXPRESSION] grouping()/grouping_id() can only be \
             used with GroupingSets/Cube/Rollup. SQLSTATE: 42K0E";
        let error = engine_err(DataFusionError::Plan(payload.to_string()));
        assert!(matches!(error, Error::Analysis(text) if text == payload));
    }

    #[test]
    fn foreign_bracketed_wrap_keeps_the_full_display() {
        let wrapped = DataFusionError::Context(
            "lambda_rebind".to_string(),
            Box::new(DataFusionError::Plan(
                "[INVALID_LAMBDA_FUNCTION_CALL.NUM_ARGS_MISMATCH] Invalid lambda function \
                 call. SQLSTATE: 42605"
                    .to_string(),
            )),
        );
        let error = engine_err(wrapped);
        assert!(
            matches!(error, Error::Analysis(text) if text.contains("lambda_rebind\ncaused by\n"))
        );
    }

    #[test]
    fn grouping_head_without_sqlstate_keeps_the_full_display() {
        let payload = "[GROUPING_ID_COLUMN_MISMATCH] Columns of grouping_id (k) does not match \
             grouping columns (g,k).";
        let error = engine_err(DataFusionError::Plan(payload.to_string()));
        let Error::Analysis(text) = error else {
            panic!("expected an Analysis error, got {error:?}");
        };
        assert_eq!(text, format!("Error during planning: {payload}"));
    }
}
