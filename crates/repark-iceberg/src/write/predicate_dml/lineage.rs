use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
};

use crate::write::merge::{FILE_PATH_COL, POS_COL, quote_ident};
use crate::write::position_delete::PositionDeletePair;

pub(super) fn push_identity_pair(pairs: &mut Vec<PositionDeletePair>, path: &str, position: i64) {
    let reuse = pairs
        .last()
        .filter(|(last, _)| last.as_ref() == path)
        .map(|(last, _)| Arc::clone(last));
    pairs.push((reuse.unwrap_or_else(|| Arc::<str>::from(path)), position));
}

pub(super) fn survivor_sql(
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    rewrite_name: &str,
    ident_table: &str,
    carry_lineage: bool,
) -> String {
    let columns = rewrite_column_names(write_schema, carry_lineage)
        .iter()
        .map(|name| format!("t.{}", quote_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {columns} FROM {rewrite} AS t WHERE NOT EXISTS (\
         SELECT 1 FROM {idents} AS i WHERE i.{file} = t.{file} AND i.{pos} = t.{pos})",
        rewrite = quote_ident(rewrite_name),
        idents = quote_ident(ident_table),
        file = quote_ident(FILE_PATH_COL),
        pos = quote_ident(POS_COL),
    )
}

pub(super) fn rewrite_column_names(
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    carry_lineage: bool,
) -> Vec<String> {
    let mut names: Vec<String> = write_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    if carry_lineage {
        names.push(RESERVED_COL_NAME_ROW_ID.to_string());
        names.push(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER.to_string());
    }
    names
}

pub(super) fn update_values_schema(
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    carry_lineage: bool,
) -> Arc<ArrowSchema> {
    let mut fields: Vec<Field> = write_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    if carry_lineage {
        fields.push(Field::new(RESERVED_COL_NAME_ROW_ID, DataType::Int64, true));
        fields.push(Field::new(
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            DataType::Int64,
            true,
        ));
    }
    Arc::new(ArrowSchema::new(fields))
}

pub(super) fn update_projection_sql(
    values_schema: &datafusion::arrow::datatypes::SchemaRef,
    alias: &str,
    assignments: &[(String, String)],
) -> String {
    values_schema
        .fields()
        .iter()
        .map(|field| {
            let quoted = quote_ident(field.name());
            if field.name() == RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER {
                return format!("CAST(NULL AS BIGINT) AS {quoted}");
            }
            if let Some((_, expr_sql)) = assignments
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(field.name()))
            {
                format!("({expr_sql}) AS {quoted}")
            } else {
                format!("{}.{}", quote_ident(alias), quoted)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn project_update_data_batch(
    batch: &RecordBatch,
    values_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let indices: Vec<usize> = (2..batch.num_columns()).collect();
    let projected = batch.project(&indices)?;
    let fields: Vec<datafusion::arrow::datatypes::Field> = values_schema
        .fields()
        .iter()
        .zip(projected.columns())
        .map(|(field, array)| {
            datafusion::arrow::datatypes::Field::new(
                field.name(),
                array.data_type().clone(),
                array.is_nullable(),
            )
        })
        .collect();
    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, projected.columns().to_vec()).map_err(|error| {
        DataFusionError::Internal(format!(
            "identity UPDATE data batch does not match the projected schema: {error}"
        ))
    })
}

pub(super) fn push_pairs_from_batch(
    batch: &RecordBatch,
    pairs: &mut Vec<PositionDeletePair>,
) -> Result<()> {
    let files = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("identity SELECT `_file` column is not Utf8".to_string())
        })?;
    let positions = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("identity SELECT `_pos` column is not Int64".to_string())
        })?;
    for row in 0..batch.num_rows() {
        if files.is_null(row) || positions.is_null(row) {
            return Err(DataFusionError::Internal(
                "identity SELECT produced a NULL `(_file, _pos)` pair".to_string(),
            ));
        }
        push_identity_pair(pairs, files.value(row), positions.value(row));
    }
    Ok(())
}
