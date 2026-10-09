use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

use crate::catalog::uuid_presentation::convert_uuid_column;

#[allow(clippy::missing_errors_doc)]
pub fn resolve_projection(batch: &RecordBatch, schema: &SchemaRef) -> Result<Vec<usize>> {
    let batch_schema = batch.schema();
    let by_name: HashMap<&str, usize> = batch_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name().as_str(), index))
        .collect();
    schema
        .fields()
        .iter()
        .map(|field| {
            by_name.get(field.name().as_str()).copied().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "iceberg scan missing column '{}' in {:?}",
                    field.name(),
                    batch.schema()
                ))
            })
        })
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn conform_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    projection: &mut Option<(SchemaRef, Vec<usize>)>,
) -> Result<RecordBatch> {
    let batch_schema = batch.schema();
    let cached = match projection {
        Some((cached_schema, indices)) if Arc::ptr_eq(cached_schema, &batch_schema) => indices,
        _ => {
            let indices = resolve_projection(batch, schema)?;
            &projection.insert((Arc::clone(&batch_schema), indices)).1
        }
    };
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, index) in schema.fields().iter().zip(cached.iter()) {
        let column = batch.column(*index);
        if column.data_type() == field.data_type() {
            columns.push(Arc::clone(column));
            continue;
        }
        columns.push(convert_uuid_column(column, field.data_type())?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        DataFusionError::Internal(format!("iceberg scan could not rebuild batch: {error}"))
    })
}

#[must_use]
pub fn iceberg_predicate_from_filters(filters: &[Expr]) -> Option<Predicate> {
    let converted: Vec<Predicate> = filters.iter().filter_map(equality_predicate).collect();
    converted.into_iter().reduce(Predicate::and)
}

fn equality_predicate(expr: &Expr) -> Option<Predicate> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let (name, literal) = column_eq_literal(binary.left.as_ref(), binary.right.as_ref())
        .or_else(|| column_eq_literal(binary.right.as_ref(), binary.left.as_ref()))?;
    let datum = scalar_to_datum(literal)?;
    Some(Reference::new(name).equal_to(datum))
}

fn column_eq_literal<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a str, &'a ScalarValue)> {
    match (left, right) {
        (Expr::Column(column), Expr::Literal(value, _)) => Some((column.name.as_str(), value)),
        _ => None,
    }
}

fn scalar_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int32(Some(number)) => Some(Datum::int(*number)),
        ScalarValue::Int64(Some(number)) => Some(Datum::long(*number)),
        ScalarValue::Utf8(Some(text))
        | ScalarValue::Utf8View(Some(text))
        | ScalarValue::LargeUtf8(Some(text)) => Some(Datum::string(text)),
        ScalarValue::Boolean(Some(flag)) => Some(Datum::bool(*flag)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn source_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let ids = Int32Array::from(vec![1_i32; rows]);
        RecordBatch::try_new(schema, vec![Arc::new(ids)]).expect("source batch")
    }

    fn empty_schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    #[test]
    fn empty_projection_keeps_the_batch_row_count() {
        let source = source_batch(4);
        let schema = empty_schema();
        let mut projection = None;
        let conformed = conform_batch(&source, &schema, &mut projection).expect("conforms");
        assert_eq!(conformed.schema(), schema);
        assert_eq!(conformed.num_columns(), 0);
        assert_eq!(conformed.num_rows(), 4);
    }

    #[test]
    fn empty_projection_keeps_an_empty_batch_empty() {
        let source = source_batch(0);
        let schema = empty_schema();
        let mut projection = None;
        let conformed = conform_batch(&source, &schema, &mut projection).expect("conforms");
        assert_eq!(conformed.schema(), schema);
        assert_eq!(conformed.num_columns(), 0);
        assert_eq!(conformed.num_rows(), 0);
    }

    #[test]
    fn empty_projection_counts_each_batch_across_a_stream() {
        let schema = empty_schema();
        let mut projection = None;
        for rows in [3_usize, 0, 1] {
            let conformed =
                conform_batch(&source_batch(rows), &schema, &mut projection).expect("conforms");
            assert_eq!(conformed.num_columns(), 0);
            assert_eq!(conformed.num_rows(), rows);
        }
    }
}
