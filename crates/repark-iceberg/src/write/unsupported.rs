use datafusion::error::DataFusionError;

#[derive(Debug)]
pub struct UnsupportedMarker(pub String);

impl std::fmt::Display for UnsupportedMarker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for UnsupportedMarker {}

#[must_use]
pub fn unsupported_error(message: String) -> DataFusionError {
    DataFusionError::External(Box::new(UnsupportedMarker(message)))
}

#[must_use]
pub fn unsupported_message_error(error: iceberg::Error) -> DataFusionError {
    match error.kind() {
        iceberg::ErrorKind::FeatureUnsupported => unsupported_error(error.message().to_string()),
        _ => DataFusionError::External(Box::new(error)),
    }
}
