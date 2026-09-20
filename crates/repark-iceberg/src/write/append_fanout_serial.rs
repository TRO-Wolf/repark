use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use futures::{Stream, TryStreamExt};
use iceberg::arrow::{FieldMatchMode, RecordBatchPartitionSplitter};
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use uuid::Uuid;

use super::append::iceberg_err;
use super::distribution::stamp;
use super::file_order::ascending_partition_order;
use super::write_options::WriterStagingOverrides;
use super::writer_props::{target_file_size_with, writer_properties_with};

pub(crate) async fn fanout_conformed_stream_serial<S>(
    table: &Table,
    conformed: &mut S,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let no_abort = AtomicBool::new(false);
    let files =
        fanout_conformed_stream_serial_with_abort(table, conformed, &no_abort, staging).await?;
    Ok(ascending_partition_order(files))
}

pub(crate) async fn fanout_conformed_stream_serial_with_abort<S>(
    table: &Table,
    conformed: &mut S,
    aborted: &AtomicBool,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )
    .map_err(iceberg_err)?;

    let parquet_builder = ParquetWriterBuilder::new_with_match_mode(
        writer_properties_with(table, staging.codec.as_deref(), staging.level.as_deref())?,
        table.metadata().current_schema().clone(),
        FieldMatchMode::Name,
    )
    .with_metrics_config(crate::write::writer_props::metrics_config_for(table)?);
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(iceberg_err)?;
    let file_name_generator =
        DefaultFileNameGenerator::new(Uuid::new_v4().to_string(), None, file_format);
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        target_file_size_with(table, staging.target_file_size_bytes)?,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut fanout = FanoutWriter::new(stamp(DataFileWriterBuilder::new(rolling_builder), table));

    while let Some(batch) = conformed.try_next().await? {
        if aborted.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }
        if batch.num_rows() == 0 {
            continue;
        }
        for (partition_key, partition_batch) in splitter.split(&batch).map_err(iceberg_err)? {
            if let Err(error) = fanout.write(partition_key, partition_batch).await {
                aborted.store(true, Ordering::SeqCst);
                return Err(iceberg_err(error));
            }
        }
    }
    if aborted.load(Ordering::SeqCst) {
        return Ok(Vec::new());
    }
    fanout.close().await.map_err(iceberg_err)
}
