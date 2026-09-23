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
