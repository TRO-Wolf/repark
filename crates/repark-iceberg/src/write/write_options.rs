use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::Catalog;
use iceberg::arrow::{FieldMatchMode, RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::expr::Predicate;
use iceberg::spec::{DataFile, DataFileFormat, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use uuid::Uuid;

use crate::write::commit_error::commit_result;
use crate::write::commit_target::{maybe_to_branch, snapshot_id_for_commit};
use crate::write::concurrency::WriteConcurrency;
use crate::write::conform::{conform_batches, write_default_column_names};
use crate::write::file_order::ascending_partition_order;
use crate::write::merge::OPERATION_ID_PROP;
use crate::write::overwrite::{OverwriteIsolation, parse_overwrite_isolation};
use crate::write::writer_props::{target_file_size_with, writer_properties_with};

#[derive(Debug, Default, Clone)]
pub struct WriterStagingOverrides {
    pub codec: Option<String>,
    pub level: Option<String>,
    pub target_file_size_bytes: Option<u64>,
}

impl WriterStagingOverrides {
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }
}

#[must_use]
pub fn summary_with_extras(extra: &[(String, String)]) -> (String, HashMap<String, String>) {
    let operation_id = Uuid::new_v4().to_string();
    let mut summary = HashMap::from([(OPERATION_ID_PROP.to_string(), operation_id.clone())]);
    for (key, value) in extra {
        summary.insert(key.clone(), value.clone());
    }
    (operation_id, summary)
}

#[allow(clippy::missing_errors_doc)]
pub fn isolation_with_override(
    table: &Table,
    isolation_override: Option<&str>,
) -> Result<Option<OverwriteIsolation>> {
    let Some(raw) = isolation_override else {
        return parse_overwrite_isolation(table);
    };
    if raw.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    match raw.to_ascii_lowercase().as_str() {
        "serializable" => Ok(Some(OverwriteIsolation::Serializable)),
        "snapshot" => Ok(Some(OverwriteIsolation::Snapshot)),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid isolation level: {raw}"
        ))),
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn append_with_statement_options(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    batches: Vec<RecordBatch>,
    summary_extra: &[(String, String)],
    staging: &WriterStagingOverrides,
    concurrency: WriteConcurrency,
    branch: Option<&str>,
) -> Result<Table> {
    reject_non_parquet_append(table)?;
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = conform_batches(&write_schema, &write_default_columns, &batches)?;
    let new_files = if conformed.is_empty() {
        Vec::new()
    } else if table.metadata().default_partition_spec().is_unpartitioned() {
        stage_unpartitioned_with_overrides(table, conformed, concurrency, staging).await?
    } else {
        stage_partitioned_with_overrides(table, conformed, staging).await?
    };
    commit_append_with_summary(catalog, table, new_files, summary_extra, branch).await
}

fn reject_non_parquet_append(table: &Table) -> Result<()> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "append writes only Parquet data files yet (table default is {file_format})"
        )));
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub async fn stage_unpartitioned_with_overrides(
    table: &Table,
    batches: Vec<RecordBatch>,
    concurrency: WriteConcurrency,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>> {
    stage_unpartitioned_stream_with_overrides(
        table,
        futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
        concurrency,
        staging,
    )
    .await
}

#[allow(clippy::missing_errors_doc)]
pub async fn stage_unpartitioned_stream_with_overrides<S>(
    table: &Table,
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
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = stream.map(move |item| {
        crate::write::conform::conform_batch_retaining_unmapped_columns(
            &write_schema,
            &write_default_columns,
            &item?,
        )
    });
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "MERGE INTO writes only Parquet data files yet (table default is {file_format})"
        )));
    }
    let build_writer = || async { build_unpartitioned_writer_with(table, staging).await };
    crate::write::distribution::drive_unpartitioned(
        table,
        conformed,
        concurrency.max_concurrent_files,
        build_writer,
    )
    .await
}

#[allow(clippy::missing_errors_doc)]
pub async fn stage_partitioned_with_overrides(
    table: &Table,
    batches: Vec<RecordBatch>,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>> {
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = conform_batches(&write_schema, &write_default_columns, &batches)?;
    let mut stream = futures::stream::iter(conformed.into_iter().map(Ok::<_, DataFusionError>));
    stage_partitioned_stream_with_overrides(table, &mut stream, staging).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn stage_partitioned_stream_with_overrides<S>(
    table: &Table,
    conformed: &mut S,
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
    );
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
    let mut fanout = FanoutWriter::new(DataFileWriterBuilder::new(rolling_builder));
    while let Some(batch) = conformed.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        for (partition_key, partition_batch) in splitter.split(&batch).map_err(iceberg_err)? {
            fanout
                .write(partition_key, partition_batch)
                .await
                .map_err(iceberg_err)?;
        }
    }
    let files = fanout.close().await.map_err(iceberg_err)?;
    Ok(ascending_partition_order(files))
}

#[allow(clippy::missing_errors_doc)]
pub async fn stage_overwrite_files_with<S>(
    table: &Table,
    stream: S,
    column_names: Vec<String>,
    concurrency: WriteConcurrency,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let write_schema: datafusion::arrow::datatypes::SchemaRef =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).map_err(iceberg_err)?);
    let mapped = stream
        .map(move |item| {
            let batch = item?;
            crate::write::overwrite::positional_map_overwrite_batch(
                &batch,
                &write_schema,
                &column_names,
            )
        })
        .try_filter(|batch| futures::future::ready(batch.num_rows() > 0));
    if table.metadata().default_partition_spec().is_unpartitioned() {
        stage_unpartitioned_stream_with_overrides(table, mapped, concurrency, staging).await
    } else {
        let current_schema = table.metadata().current_schema();
        let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
        let write_default_columns =
            crate::write::conform::write_default_column_names(current_schema);
        let mapped: Vec<RecordBatch> = mapped.try_collect().await?;
        let mut conformed = Vec::with_capacity(mapped.len());
        for batch in &mapped {
            conformed.push(crate::write::conform::conform_batch(
                &write_schema,
                &write_default_columns,
                batch,
            )?);
        }
        let mut stream = futures::stream::iter(conformed.into_iter().map(Ok::<_, DataFusionError>));
        stage_partitioned_stream_with_overrides(table, &mut stream, staging).await
    }
}

async fn build_unpartitioned_writer_with(
    table: &Table,
    staging: &WriterStagingOverrides,
) -> Result<impl IcebergWriter + use<>> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    let parquet_builder = ParquetWriterBuilder::new_with_match_mode(
        writer_properties_with(table, staging.codec.as_deref(), staging.level.as_deref())?,
        crate::write::merge::row_lineage::iceberg_parquet_schema(table)?,
        FieldMatchMode::Name,
    );
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
    let unpartitioned_key = PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        table.metadata().current_schema().clone(),
        Struct::empty(),
    )
    .map_err(iceberg_err)?;
    DataFileWriterBuilder::new(rolling_builder)
        .build(Some(unpartitioned_key))
        .await
        .map_err(iceberg_err)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_append_with_summary(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    new_files: Vec<DataFile>,
    summary_extra: &[(String, String)],
    branch: Option<&str>,
) -> Result<Table> {
    let (operation_id, summary) = summary_with_extras(summary_extra);
    let tx = Transaction::new(table);
    let action = tx
        .fast_append()
        .add_data_files(new_files)
        .set_snapshot_properties(summary);
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action.apply(tx).map_err(iceberg_err)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_overwrite_replace_all_with_summary(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    branch: Option<&str>,
    summary_extra: &[(String, String)],
    isolation_override: Option<&str>,
) -> Result<Table> {
    let isolation = isolation_with_override(table, isolation_override)?;
    let (operation_id, summary) = summary_with_extras(summary_extra);
    let tx = Transaction::new(table);
    let mut action = tx
        .overwrite_files()
        .overwrite_by_row_filter(Predicate::AlwaysTrue)
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = snapshot_id_for_commit(table, branch) {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action.apply(tx).map_err(iceberg_err)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_overwrite_by_row_filter_with_summary(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    predicate: Predicate,
    branch: Option<&str>,
    summary_extra: &[(String, String)],
    isolation_override: Option<&str>,
) -> Result<Table> {
    let isolation = isolation_with_override(table, isolation_override)?;
    let (operation_id, summary) = summary_with_extras(summary_extra);
    let tx = Transaction::new(table);
    let mut action = tx
        .overwrite_files()
        .overwrite_by_row_filter(predicate)
        .validate_added_files_match_overwrite_filter()
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = snapshot_id_for_commit(table, branch) {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action.apply(tx).map_err(iceberg_err)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_replace_partitions_with_summary(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    branch: Option<&str>,
    summary_extra: &[(String, String)],
    isolation_override: Option<&str>,
) -> Result<Table> {
    crate::write::partition_overwrite::refuse_empty_dynamic_overwrite(&staged_files)?;
    let isolation = isolation_with_override(table, isolation_override)?;
    let (operation_id, summary) = summary_with_extras(summary_extra);
    let tx = Transaction::new(table);
    let mut action = tx
        .replace_partitions()
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = snapshot_id_for_commit(table, branch) {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action.apply(tx).map_err(iceberg_err)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}

fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    crate::catalog::iceberg_to_datafusion(err)
}
