use datafusion::error::DataFusionError;
use iceberg::ErrorKind;

use super::unsupported::{UnsupportedMarker, unsupported_message_error};

fn external_source(error: &DataFusionError) -> &(dyn std::error::Error + Send + Sync + 'static) {
    match error {
        DataFusionError::External(source) => source.as_ref(),
        other => panic!("expected DataFusionError::External, got {other:?}"),
    }
}

#[test]
fn feature_unsupported_keeps_only_the_fork_message() {
    let error = unsupported_message_error(iceberg::Error::new(
        ErrorKind::FeatureUnsupported,
        "Cannot rename Hadoop tables",
    ));
    let source = external_source(&error);
    let marker = source
        .downcast_ref::<UnsupportedMarker>()
        .unwrap_or_else(|| panic!("expected UnsupportedMarker, got {source:?}"));
    assert_eq!(marker.0, "Cannot rename Hadoop tables");
    assert_eq!(source.to_string(), "Cannot rename Hadoop tables");
    assert!(source.downcast_ref::<iceberg::Error>().is_none());
}

#[test]
fn other_kinds_stay_external_iceberg_errors() {
    for (kind, message) in [
        (ErrorKind::Unexpected, "metadata write failed"),
        (ErrorKind::TableNotFound, "Table does not exist: db.t"),
        (ErrorKind::TableAlreadyExists, "Table already exists: db.u"),
    ] {
        let error = unsupported_message_error(iceberg::Error::new(kind, message));
        let source = external_source(&error);
        assert!(
            source.downcast_ref::<UnsupportedMarker>().is_none(),
            "{kind:?} must not become an UnsupportedMarker"
        );
        let iceberg_error = source
            .downcast_ref::<iceberg::Error>()
            .unwrap_or_else(|| panic!("expected iceberg::Error for {kind:?}, got {source:?}"));
        assert_eq!(iceberg_error.kind(), kind);
        assert_eq!(iceberg_error.message(), message);
    }
}
