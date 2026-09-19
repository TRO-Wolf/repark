use datafusion::error::DataFusionError;

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
