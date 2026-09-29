use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use futures::{Stream, StreamExt};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFile;
use iceberg::table::Table;

use super::append::{
    fanout_conformed_stream_with_concurrency, fanout_data_files_with_concurrency, iceberg_err,
};
use crate::write::concurrency::WriteConcurrency;
use crate::write::conform::{conform_batch_scoped, conform_batches, write_default_column_names};
use crate::write::write_options::WriterStagingOverrides;

#[allow(clippy::missing_errors_doc)]
pub async fn write_partitioned_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFile>> {
    write_partitioned_data_files_with_concurrency(table, batches, WriteConcurrency::default()).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_partitioned_data_files_with_concurrency(
    table: &Table,
    batches: Vec<RecordBatch>,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>> {
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = conform_batches(&write_schema, &write_default_columns, &batches)?;
    if conformed.is_empty() {
        return Ok(Vec::new());
    }
    fanout_data_files_with_concurrency(table, conformed, concurrency).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_partitioned_data_files_from_stream<S>(
    table: &Table,
    stream: S,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    write_partitioned_data_files_from_stream_with_concurrency(
        table,
        stream,
        WriteConcurrency::default(),
        true,
    )
    .await
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_partitioned_data_files_from_stream_with_concurrency<S>(
    table: &Table,
    stream: S,
    concurrency: WriteConcurrency,
    case_insensitive: bool,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = stream.map(move |item| {
        conform_batch_scoped(
            &write_schema,
            &write_default_columns,
            &item?,
            case_insensitive,
        )
    });
    fanout_conformed_stream_with_concurrency(
        table,
        conformed,
        concurrency,
        &WriterStagingOverrides::none(),
    )
    .await
}
