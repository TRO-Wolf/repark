use datafusion::error::DataFusionError;
use iceberg::ErrorKind;
use repark_common::Error;
use repark_iceberg::write::commit_err;

use crate::error_map::{EngineErrorKind, classify_datafusion_error, engine_err, iceberg_err};

fn iceberg_error_of(kind: ErrorKind, message: &str) -> iceberg::Error {
    iceberg::Error::new(kind, message.to_string())
}

#[test]
fn commit_state_unknown_classifies_to_dedicated_variant_on_both_routes() {
    let raw = iceberg_error_of(
        ErrorKind::CommitStateUnknown,
        "the catalog may have applied the commit",
    );
    let rendered = raw.to_string();
    let via_external = engine_err(DataFusionError::External(Box::new(raw)));
    let via_direct = iceberg_err(iceberg_error_of(
        ErrorKind::CommitStateUnknown,
        "the catalog may have applied the commit",
    ));
    for converted in [via_external, via_direct] {
        match converted {
            Error::CommitStateUnknown {
                message,
                operation_id,
            } => {
                assert_eq!(
                    message, rendered,
                    "the message must stay byte-identical to today's iceberg fold"
                );
                assert_eq!(operation_id, None, "an unstamped kind carries no id");
            }
            other => panic!("expected Error::CommitStateUnknown, got {other:?}"),
        }
    }
}

#[test]
fn stamped_commit_unknown_carries_the_minted_operation_id() {
    let stamped = commit_err(
        iceberg_error_of(ErrorKind::CommitStateUnknown, "lost UpdateTable response"),
        "op-42",
    );
    assert!(matches!(
        classify_datafusion_error(&stamped),
        EngineErrorKind::CommitStateUnknown(_)
    ));
    let converted = engine_err(stamped);
    match converted {
        Error::CommitStateUnknown {
            message,
            operation_id,
        } => {
            assert!(message.starts_with("CommitStateUnknown"));
            assert!(message.contains("lost UpdateTable response"));
            assert_eq!(operation_id.as_deref(), Some("op-42"));
        }
        other => panic!("expected Error::CommitStateUnknown, got {other:?}"),
    }
}

#[test]
fn commit_err_leaves_definite_kinds_in_the_base_bucket() {
    for kind in [
        ErrorKind::CatalogCommitConflicts,
        ErrorKind::PreconditionFailed,
        ErrorKind::Unexpected,
        ErrorKind::DataInvalid,
    ] {
        let converted = engine_err(commit_err(iceberg_error_of(kind, "probe"), "op-1"));
        assert!(
            matches!(converted, Error::Iceberg(_)),
            "{kind:?} must stay in the definite iceberg bucket, got {converted:?}"
        );
    }
}
