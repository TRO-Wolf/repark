//! Batch conforming for the append write path: name resolution, store assignment, strict casts.
//!
//! Split from `append.rs` (file-size ratchet): the conform step is the writer-preparation half
//! of bulk append, kept next to its name-resolution and store-assignment dependencies.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef};
use datafusion::error::{DataFusionError, Result};

use crate::write::name_resolution::{CaseInsensitiveColumnIndex, SourceMatch};
use crate::write::store_assign::refuse_unless_write_store_assignable;

/// Conform batches to the Iceberg write schema: every target column takes its source column.
/// Columns carrying an Iceberg `write-default` may be absent — the fork's `DataFileWriter::write`
/// fills them (RP-3 C-009), so conform builds those batches against the reduced schema.
pub(crate) fn conform_batches(
    write_schema: &SchemaRef,
    write_default_columns: &HashSet<String>,
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    let mut conformed = Vec::with_capacity(batches.len());
    for batch in batches {
        conformed.push(conform_batch(write_schema, write_default_columns, batch)?);
    }
    conformed.retain(|batch| batch.num_rows() > 0);
    Ok(conformed)
}

/// Top-level column names (lowercased) whose Iceberg field carries a `write-default`.
pub(crate) fn write_default_column_names(schema: &iceberg::spec::Schema) -> HashSet<String> {
    schema
        .as_struct()
        .fields()
        .iter()
        .filter(|field| field.write_default.is_some())
        .map(|field| field.name.to_ascii_lowercase())
        .collect()
}

pub(crate) fn conform_batch_retaining_unmapped_columns(
    write_schema: &SchemaRef,
    write_default_columns: &HashSet<String>,
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    let write_names: HashSet<String> = write_schema
        .fields()
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect();
    let extra: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !write_names.contains(&field.name().to_ascii_lowercase()))
        .map(|(index, _)| index)
        .collect();
    if extra.is_empty() {
        if write_schema_types_already_match(write_schema, batch) {
            return attach_write_field_ids(write_schema, batch);
        }
        return conform_batch(write_schema, write_default_columns, batch);
    }
    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|index| !extra.contains(index))
        .collect();
    let projected = batch
        .project(&keep)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    let conformed = if write_schema_types_already_match(write_schema, &projected) {
        attach_write_field_ids(write_schema, &projected)?
    } else {
        conform_batch(write_schema, write_default_columns, &projected)?
    };
    let mut fields: Vec<Arc<Field>> = conformed.schema().fields().iter().cloned().collect();
    let mut columns = conformed.columns().to_vec();
    for index in extra {
        fields.push(Arc::new(batch.schema().field(index).clone()));
        columns.push(Arc::clone(batch.column(index)));
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn attach_write_field_ids(write_schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
    let fields: Vec<Arc<Field>> = write_schema
        .fields()
        .iter()
        .zip(batch.schema().fields())
        .map(|(target, source)| {
            let Some(field_id) = target.metadata().get("PARQUET:field_id") else {
                return Arc::clone(source);
            };
            let mut metadata = source.metadata().clone();
            metadata.insert("PARQUET:field_id".to_string(), field_id.clone());
            Arc::new(source.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), batch.columns().to_vec())
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn write_schema_types_already_match(write_schema: &SchemaRef, batch: &RecordBatch) -> bool {
    if batch.num_columns() != write_schema.fields().len() {
        return false;
    }
    write_schema
        .fields()
        .iter()
        .zip(batch.schema().fields())
        .all(|(target, source)| {
            target.data_type() == source.data_type()
                && target.name().eq_ignore_ascii_case(source.name())
        })
}

/// Conform ONE consumer batch to the write schema.
pub(crate) fn conform_batch(
    write_schema: &SchemaRef,
    write_default_columns: &HashSet<String>,
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    let source_names: Vec<&str> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let source_index = CaseInsensitiveColumnIndex::new(source_names.iter().copied());
    let mut consumed = vec![false; source_names.len()];
    let mut fill_omitted: Vec<String> = Vec::new();
    let conformed = write_schema
        .fields()
        .iter()
        .map(|field| match source_index.resolve(field.name()) {
            SourceMatch::Unique(index) => {
                consumed[index] = true;
                let column = batch.column(index);
                if column.data_type() == field.data_type() {
                    return Ok(Some(Arc::clone(column)));
                }
                // WI-1: ANSI store assignment BEFORE the kernel.
                refuse_unless_write_store_assignable(
                    "append",
                    field.name(),
                    column.data_type(),
                    field.data_type(),
                )?;
                Ok(Some(cast_with_options(
                    column,
                    field.data_type(),
                    &strict_cast(),
                )?))
            }
            SourceMatch::Missing => {
                if write_default_columns.contains(&field.name().to_ascii_lowercase()) {
                    fill_omitted.push(field.name().clone());
                    Ok(None)
                } else {
                    Err(DataFusionError::Plan(format!(
                        "append batch is missing column `{}` required by the target table \
                         (columns resolve by name, case-insensitively — Spark default)",
                        field.name()
                    )))
                }
            }
            SourceMatch::Ambiguous(colliding) => Err(DataFusionError::Plan(format!(
                "append batch column `{}` is ambiguous — source columns `{}` all resolve to it \
                 (Spark case-insensitive resolution rejects the collision; a first-match rebuild \
                 would silently drop every copy after the first)",
                field.name(),
                colliding.join("`, `")
            ))),
        })
        .collect::<Result<Vec<Option<ArrayRef>>>>()?;
    if let Some(extra) = consumed.iter().position(|matched| !matched) {
        return Err(DataFusionError::Plan(format!(
            "append batch contains column `{}` that does not exist in the target table \
             (columns resolve by name, case-insensitively — Spark default)",
            source_names[extra]
        )));
    }
    let columns: Vec<ArrayRef> = conformed.into_iter().flatten().collect();
    if fill_omitted.is_empty() {
        return Ok(RecordBatch::try_new(write_schema.clone(), columns)?);
    }
    let reduced = Arc::new(ArrowSchema::new(
        write_schema
            .fields()
            .iter()
            .filter(|field| !fill_omitted.contains(field.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ));
    Ok(RecordBatch::try_new(reduced, columns)?)
}

/// Strict cast options: an overflowing cast is an error, never a silent NULL.
fn strict_cast() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::DataType;
    use std::collections::HashMap;

    #[test]
    fn retaining_unmapped_identity_attaches_write_schema_field_ids() {
        let write = Arc::new(ArrowSchema::new(vec![
            Field::new("survey_id", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "5".to_string(),
            )])),
        ]));
        let source = Arc::new(ArrowSchema::new(vec![
            Field::new("survey_id", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(
            source,
            vec![Arc::new(StringArray::from(vec!["s1"])) as ArrayRef],
        )
        .expect("batch");
        let out = conform_batch_retaining_unmapped_columns(&write, &HashSet::new(), &batch)
            .expect("conform");
        assert_eq!(
            out.schema()
                .field(0)
                .metadata()
                .get("PARQUET:field_id")
                .map(String::as_str),
            Some("5")
        );
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8")
                .value(0),
            "s1"
        );
    }
}
