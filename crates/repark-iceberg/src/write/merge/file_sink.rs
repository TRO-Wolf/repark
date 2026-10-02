use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use futures::{Stream, StreamExt};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFile;
use iceberg::table::Table;

use super::iceberg_err;
use super::session_staging::build_unpartitioned_data_file_writer;
use crate::write::concurrency::WriteConcurrency;
use crate::write::conform::{
    conform_batch_retaining_unmapped_columns_scoped, write_default_column_names,
};
use crate::write::distribution::drive_unpartitioned;

#[allow(clippy::missing_errors_doc)]
pub async fn write_data_files(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>> {
    write_data_files_with_concurrency(table, batches, WriteConcurrency::default()).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_data_files_with_concurrency(
    table: &Table,
    batches: Vec<RecordBatch>,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>> {
    write_data_files_from_stream_with_concurrency(
        table,
        futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
        concurrency,
        true,
    )
    .await
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_data_files_from_stream<S>(table: &Table, stream: S) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    write_data_files_from_stream_with_concurrency(table, stream, WriteConcurrency::default(), true)
        .await
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_data_files_from_stream_with_concurrency<S>(
    table: &Table,
    stream: S,
    concurrency: WriteConcurrency,
    case_insensitive: bool,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let max_concurrent = concurrency.max_concurrent_files;
    if max_concurrent < 1 {
        return Err(DataFusionError::Plan(format!(
            "repark.write.max-concurrent-files must be >= 1 (got {max_concurrent})"
        )));
    }
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = stream.map(move |item| {
        conform_batch_retaining_unmapped_columns_scoped(
            &write_schema,
            &write_default_columns,
            &item?,
            case_insensitive,
        )
    });
    let build_writer = || async { build_unpartitioned_data_file_writer(table).await };
    drive_unpartitioned(table, conformed, max_concurrent, build_writer).await
}
