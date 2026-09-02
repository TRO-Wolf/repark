use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::{CastOptions, cast_with_options, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use futures::{Stream, TryStreamExt};
use iceberg::arrow::{FieldMatchMode, PartitionValueCalculator, arrow_struct_to_literal};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
    format_supports_row_lineage, schema_with_row_lineage,
};
use iceberg::spec::{DataFile, DataFileFormat, Literal, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use uuid::Uuid;

use super::not_matched_by_source;
use super::{FILE_PATH_COL, MergeSql, POS_COL, iceberg_err, quote_ident};

pub(crate) fn table_carries_merge_lineage(table: &Table) -> bool {
    format_supports_row_lineage(table.metadata().format_version())
}

pub(super) fn iceberg_parquet_schema(table: &Table) -> Result<iceberg::spec::SchemaRef> {
    let schema = table.metadata().current_schema().clone();
    if !table_carries_merge_lineage(table) {
        return Ok(schema);
    }
    Ok(Arc::new(
        schema_with_row_lineage(schema.as_ref()).map_err(iceberg_err)?,
    ))
}

pub(crate) fn scratch_schema_for_table(write_schema: &SchemaRef, table: &Table) -> SchemaRef {
    scratch_schema_with_lineage(write_schema, table_carries_merge_lineage(table))
}

pub(super) fn scratch_schema_with_lineage(
    write_schema: &SchemaRef,
    carry_lineage: bool,
) -> SchemaRef {
    let mut fields: Vec<Field> = write_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    if carry_lineage {
        fields.push(lineage_arrow_field(
            RESERVED_COL_NAME_ROW_ID,
            RESERVED_FIELD_ID_ROW_ID,
        ));
        fields.push(lineage_arrow_field(
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ));
    }
    fields.push(Field::new(FILE_PATH_COL, DataType::Utf8, false));
    fields.push(Field::new(POS_COL, DataType::Int64, false));
    Arc::new(ArrowSchema::new(fields))
}

pub(super) fn maybe_append_lineage_projection(sql: &MergeSql<'_>, user: String) -> String {
    if !sql.carry_lineage {
        return user;
    }
    let target_alias = &sql.spec.target_alias;
    let row_id = quote_ident(RESERVED_COL_NAME_ROW_ID);
    let last_updated = quote_ident(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER);
    let changed = changed_row_sql(sql);
    let lineage = format!(
        "{target_alias}.{row_id} AS {row_id}, \
         CASE WHEN ({changed}) THEN CAST(NULL AS BIGINT) ELSE {target_alias}.{last_updated} END \
         AS {last_updated}"
    );
    if user.is_empty() {
        lineage
    } else {
        format!("{user}, {lineage}")
    }
}

fn changed_row_sql(sql: &MergeSql<'_>) -> String {
    let matched_update = if sql.spec.matched.is_empty() {
        "FALSE".to_string()
    } else {
        sql.update_applies()
    };
    let unmatched_by_source = not_matched_by_source::update_applies(sql);
    if unmatched_by_source == "FALSE" {
        matched_update
    } else if matched_update == "FALSE" {
        unmatched_by_source
    } else {
        format!("({matched_update}) OR ({unmatched_by_source})")
    }
}

pub(super) fn attach_present_lineage(
    user: RecordBatch,
    source: &RecordBatch,
) -> Result<RecordBatch> {
    let row_id = source.column_by_name(RESERVED_COL_NAME_ROW_ID);
    let last_updated = source.column_by_name(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER);
    match (row_id, last_updated) {
        (None, None) => Ok(user),
        (Some(row_id), Some(last_updated)) => {
            let mut fields: Vec<Arc<Field>> = user.schema().fields().iter().cloned().collect();
            let mut columns = user.columns().to_vec();
            fields.push(Arc::new(lineage_arrow_field(
                RESERVED_COL_NAME_ROW_ID,
                RESERVED_FIELD_ID_ROW_ID,
            )));
            fields.push(Arc::new(lineage_arrow_field(
                RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            )));
            columns.push(cast_with_options(row_id, &DataType::Int64, &strict_cast())?);
            columns.push(cast_with_options(
                last_updated,
                &DataType::Int64,
                &strict_cast(),
            )?);
            RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
        }
        _ => Err(DataFusionError::Internal(
            "merge write batch projected one lineage column without the other".into(),
        )),
    }
}

pub(super) async fn write_partitioned_lineage_files<S>(
    table: &Table,
    mut stream: S,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    let write_schema = iceberg_parquet_schema(table)?;
    let user_schema = table.metadata().current_schema().clone();
    let user_count = user_schema.as_struct().fields().len();
    let partition_spec = table.metadata().default_partition_spec().clone();
    let calculator =
        PartitionValueCalculator::try_new(&partition_spec, &user_schema).map_err(iceberg_err)?;
    let parquet_builder = ParquetWriterBuilder::new_with_match_mode(
        crate::write::writer_props::writer_properties_for(table)?,
        write_schema,
        FieldMatchMode::Name,
    );
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(iceberg_err)?;
    let file_name_generator =
        DefaultFileNameGenerator::new(Uuid::new_v4().to_string(), None, file_format);
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        table_props.write_target_file_size_bytes,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut fanout = FanoutWriter::new(DataFileWriterBuilder::new(rolling_builder));
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        for (partition_key, partition_batch) in split_lineage_batch(
            &batch,
            user_count,
            &calculator,
            partition_spec.as_ref(),
            user_schema.as_ref(),
        )? {
            fanout
                .write(partition_key, partition_batch)
                .await
                .map_err(iceberg_err)?;
        }
    }
    fanout.close().await.map_err(iceberg_err)
}

fn split_lineage_batch(
    batch: &RecordBatch,
    user_count: usize,
    calculator: &PartitionValueCalculator,
    partition_spec: &iceberg::spec::PartitionSpec,
    user_schema: &iceberg::spec::Schema,
) -> Result<Vec<(PartitionKey, RecordBatch)>> {
    let prefix = prefix_user_columns(batch, user_count)?;
    let partition_array = calculator.calculate(&prefix).map_err(iceberg_err)?;
    let struct_array = arrow_struct_to_literal(&partition_array, calculator.partition_type())
        .map_err(iceberg_err)?;
    let partition_structs: Vec<Struct> = struct_array
        .into_iter()
        .map(|value| match value {
            Some(Literal::Struct(partition_struct)) => Ok(partition_struct),
            _ => Err(DataFusionError::Internal(
                "partition value is not a struct literal".into(),
            )),
        })
        .collect::<Result<_>>()?;
    let mut groups: HashMap<&Struct, (usize, Vec<usize>)> = HashMap::new();
    for (row, partition_struct) in partition_structs.iter().enumerate() {
        groups
            .entry(partition_struct)
            .or_insert_with(|| (row, Vec::new()))
            .1
            .push(row);
    }
    let mut out = Vec::with_capacity(groups.len());
    for (representative, row_ids) in groups.into_values() {
        let partition_struct = partition_structs[representative].clone();
        let indices = UInt32Array::from(
            row_ids
                .into_iter()
                .map(u32::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| DataFusionError::Internal("partition row index exceeds u32".into()))?,
        );
        let taken = take_record_batch(batch, &indices)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let partition_key = PartitionKey::new(
            partition_spec.clone(),
            Arc::new(user_schema.clone()),
            partition_struct,
        )
        .map_err(iceberg_err)?;
        out.push((partition_key, taken));
    }
    Ok(out)
}

fn prefix_user_columns(batch: &RecordBatch, user_count: usize) -> Result<RecordBatch> {
    if batch.num_columns() < user_count {
        return Err(DataFusionError::Internal(format!(
            "merge write batch has {} columns, expected at least {user_count} table columns",
            batch.num_columns()
        )));
    }
    let fields: Vec<Arc<Field>> = batch
        .schema()
        .fields()
        .iter()
        .take(user_count)
        .cloned()
        .collect();
    let columns = batch.columns()[..user_count].to_vec();
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn lineage_arrow_field(name: &'static str, field_id: i32) -> Field {
    Field::new(name, DataType::Int64, true).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

fn strict_cast() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}
