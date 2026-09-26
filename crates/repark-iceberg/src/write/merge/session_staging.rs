use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::{FieldMatchMode, schema_to_arrow_schema};
use iceberg::spec::{DataFile, DataFileFormat, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::AnyFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use uuid::Uuid;

use super::{cast_one_batch_to_write_schema, iceberg_err};
use crate::catalog::uuid_presentation::stored_arrow_schema;
use crate::write::append::fanout_conformed_stream_with_concurrency;
use crate::write::concurrency::WriteConcurrency;
use crate::write::conform::{conform_batch, write_default_column_names};
use crate::write::write_options::WriterStagingOverrides;

pub(crate) async fn write_new_data_files_from_stream_with<S>(
    table: &Table,
    write_schema: &SchemaRef,
    stream: S,
    concurrency: WriteConcurrency,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    if concurrency.max_concurrent_files < 1 {
        return Err(DataFusionError::Plan(format!(
            "repark.write.max-concurrent-files must be >= 1 (got {})",
            concurrency.max_concurrent_files
        )));
    }
    let write_schema = stored_arrow_schema(write_schema, table.metadata().current_schema());
    let cast_stream = stream.try_filter_map(move |batch| {
        let write_schema = Arc::clone(&write_schema);
        async move {
            if batch.num_rows() == 0 {
                return Ok(None);
            }
            Ok(Some(super::row_lineage::attach_present_lineage(
                cast_one_batch_to_write_schema(&write_schema, &batch)?,
                &batch,
            )?))
        }
    });
    let cast_stream = std::pin::pin!(cast_stream);
    if table.metadata().default_partition_spec().is_unpartitioned() {
        let build_writer =
            || async { build_unpartitioned_data_file_writer_with(table, staging).await };
        crate::write::distribution::drive_unpartitioned(
            table,
            cast_stream,
            concurrency.max_concurrent_files,
            build_writer,
        )
        .await
    } else if super::row_lineage::table_carries_merge_lineage(table) {
        super::row_lineage::write_partitioned_lineage_files_with(table, cast_stream, staging).await
    } else {
        let current_schema = table.metadata().current_schema();
        let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
        let write_default_columns = write_default_column_names(current_schema);
        let conformed = cast_stream
            .map(move |item| conform_batch(&write_schema, &write_default_columns, &item?));
        fanout_conformed_stream_with_concurrency(table, conformed, concurrency, staging).await
    }
}

pub(crate) async fn build_unpartitioned_data_file_writer(
    table: &Table,
) -> Result<impl IcebergWriter + use<>> {
    build_unpartitioned_data_file_writer_with(table, &WriterStagingOverrides::none()).await
}

async fn build_unpartitioned_data_file_writer_with(
    table: &Table,
    staging: &WriterStagingOverrides,
) -> Result<impl IcebergWriter + use<>> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format = crate::write::data_format::resolve_data_format(
        staging.write_format.as_deref(),
        Some(table_props.write_format_default.as_str()),
    )?;
    let file_writer_builder = if file_format == DataFileFormat::Parquet {
        AnyFileWriterBuilder::Parquet(Box::new(
            crate::write::writer_props::name_matched_parquet_builder(table, staging)?,
        ))
    } else {
        AnyFileWriterBuilder::for_format(
            file_format,
            crate::write::merge::row_lineage::iceberg_parquet_schema(table)?,
            table.metadata().properties(),
            crate::write::writer_props::metrics_config_for(table)?,
            FieldMatchMode::Name,
        )
        .map_err(iceberg_err)?
    };
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(iceberg_err)?;
    let file_name_generator =
        DefaultFileNameGenerator::new(Uuid::new_v4().to_string(), None, file_format);
    let rolling_builder = RollingFileWriterBuilder::new(
        file_writer_builder,
        crate::write::writer_props::target_file_size_with(table, staging.target_file_size_bytes)?,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let unpartitioned_key = PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        table.metadata().current_schema().clone(),
        Struct::empty(),
    )
    .map_err(iceberg_err)?;
    crate::write::distribution::stamp(DataFileWriterBuilder::new(rolling_builder), table)
        .build(Some(unpartitioned_key))
        .await
        .map_err(iceberg_err)
}
