use std::borrow::Cow;

use datafusion::error::Result;
use iceberg::spec::TableMetadataBuilder;
use iceberg::table::Table;

use crate::write::illegal_argument::illegal_argument_error;
use crate::write::write_options::WriterStagingOverrides;

#[allow(clippy::missing_errors_doc)]
pub fn parse_output_spec_id(raw: &str) -> Result<i32> {
    raw.parse::<i32>()
        .map_err(|_| illegal_argument_error(format!("For input string: \"{raw}\"")))
}

#[allow(clippy::missing_errors_doc)]
pub fn validate_output_spec_id(table: &Table, output_spec_id: Option<i32>) -> Result<()> {
    iceberg::writer::resolve_output_spec(table, output_spec_id)
        .map(|_| ())
        .map_err(|error| illegal_argument_error(error.message().to_string()))
}

#[allow(clippy::missing_errors_doc)]
pub fn staging_table<'a>(
    table: &'a Table,
    staging: &WriterStagingOverrides,
) -> Result<Cow<'a, Table>> {
    let Some(spec_id) = staging.output_spec_id else {
        return Ok(Cow::Borrowed(table));
    };
    validate_output_spec_id(table, Some(spec_id))?;
    if table.metadata().default_partition_spec_id() == spec_id {
        return Ok(Cow::Borrowed(table));
    }
    let metadata = TableMetadataBuilder::new_from_metadata(table.metadata().clone(), None)
        .set_default_partition_spec(spec_id)
        .and_then(TableMetadataBuilder::build)
        .map_err(crate::catalog::iceberg_to_datafusion)?
        .metadata;
    let mut builder = Table::builder()
        .metadata(metadata)
        .identifier(table.identifier().clone())
        .file_io(table.file_io().clone())
        .readonly(true)
        .disable_cache();
    if let Some(location) = table.metadata_location() {
        builder = builder.metadata_location(location);
    }
    builder
        .build()
        .map(Cow::Owned)
        .map_err(crate::catalog::iceberg_to_datafusion)
}
