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

#[derive(Debug)]
pub struct NumberFormatMarker(pub String);

impl std::fmt::Display for NumberFormatMarker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for NumberFormatMarker {}

#[must_use]
pub fn number_format_error(input: &str) -> DataFusionError {
    DataFusionError::External(Box::new(NumberFormatMarker(format!(
        "For input string: \"{input}\""
    ))))
}
