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
        rewrite = crate::write::merge::cow_scratch::quote_scratch_name(rewrite_name),
        idents = crate::write::merge::cow_scratch::quote_scratch_name(ident_table),
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

pub(super) async fn analyzed_identity_update(
    ctx: &datafusion::prelude::SessionContext,
    target_name: &str,
    spec: &super::PredicateDmlSpec,
    assignments: &[(String, String)],
    values_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<datafusion::logical_expr::LogicalPlan> {
    let identity_sql = |instant: &[usize]| {
        format!(
            "SELECT {file}, {pos}, {projections} FROM {scratch} AS {alias} WHERE {selection}",
            file = quote_ident(FILE_PATH_COL),
            pos = quote_ident(POS_COL),
            projections =
                update_projection_sql(values_schema, &spec.target_alias, assignments, instant),
            scratch = crate::write::merge::cow_scratch::quote_scratch_name(target_name),
            alias = quote_ident(&spec.target_alias),
            selection = spec.selection_sql,
        )
    };
    let analyze = crate::write::store_overflow::analyzed_store_source;
    let raw = analyze(ctx, &identity_sql(&[])).await?;
    let instant = instant_store_columns(raw.schema(), values_schema);
    if instant.is_empty() {
        return Ok(raw);
    }
    analyze(ctx, &identity_sql(&instant)).await
}

pub(super) fn instant_store_columns(
    analyzed: &datafusion::common::DFSchema,
    values_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Vec<usize> {
    values_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(index, field)| {
            crate::write::ntz_store::is_ltz_instant_target(field.data_type())
                && analyzed.fields().get(index + 2).is_some_and(|source| {
                    crate::write::ntz_store::needs_ltz_instant_cast(source.data_type())
                })
        })
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn update_projection_sql(
    values_schema: &datafusion::arrow::datatypes::SchemaRef,
    alias: &str,
    assignments: &[(String, String)],
    instant: &[usize],
) -> String {
    values_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let quoted = quote_ident(field.name());
            if field.name() == RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER {
                return format!("CAST(NULL AS BIGINT) AS {quoted}");
            }
            if let Some((_, expr_sql)) = assignments
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(field.name()))
            {
                match crate::write::ntz_store::store_kernel_sql(expr_sql, field.data_type()) {
                    Some(wall) => format!("({wall}) AS {quoted}"),
                    None if instant.contains(&index) => {
                        let cast = crate::write::update_cast::store_assignment_cast_sql(
                            expr_sql,
                            field.data_type(),
                        );
                        format!("({cast}) AS {quoted}")
                    }
                    None => format!("({expr_sql}) AS {quoted}"),
                }
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
