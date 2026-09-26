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
