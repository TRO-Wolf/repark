use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{Datum, PrimitiveType, Type};

use crate::write::partition_overwrite::PartitionLiteral;

fn literal_array(literal: &PartitionLiteral) -> ArrayRef {
    match literal {
        PartitionLiteral::Boolean(flag) => Arc::new(BooleanArray::from(vec![*flag])),
        PartitionLiteral::Int(value) => Arc::new(Int32Array::from(vec![*value])),
        PartitionLiteral::Long(value) => Arc::new(Int64Array::from(vec![*value])),
        PartitionLiteral::String(text) => Arc::new(StringArray::from(vec![text.as_str()])),
    }
}

fn cast_literal(literal: &PartitionLiteral, target: &DataType) -> Result<ScalarValue> {
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let cast = cast_with_options(&literal_array(literal), target, &options)?;
    ScalarValue::try_from_array(&cast, 0)
}

pub(crate) fn cast_constant_array(
    literal: &PartitionLiteral,
    target: &DataType,
    rows: usize,
) -> Result<ArrayRef> {
    cast_literal(literal, target)?.to_array_of_size(rows)
}

pub(crate) fn cast_datum(
    literal: &PartitionLiteral,
    primitive: &PrimitiveType,
    column: &str,
) -> Result<Datum> {
    let target = iceberg::arrow::type_to_arrow_type(&Type::Primitive(primitive.clone()))
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let datum = match cast_literal(literal, &target)? {
        ScalarValue::Boolean(Some(flag)) => Some(Datum::bool(flag)),
        ScalarValue::Int32(Some(value)) => Some(Datum::int(value)),
        ScalarValue::Int64(Some(value)) => Some(Datum::long(value)),
        ScalarValue::Float32(Some(value)) => Some(Datum::float(value)),
        ScalarValue::Float64(Some(value)) => Some(Datum::double(value)),
        ScalarValue::Date32(Some(days)) => Some(Datum::date(days)),
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => Some(Datum::string(text)),
        ScalarValue::TimestampMicrosecond(Some(micros), None) => {
            Some(Datum::timestamp_micros(micros))
        }
        ScalarValue::TimestampMicrosecond(Some(micros), Some(_)) => {
            Some(Datum::timestamptz_micros(micros))
        }
        _ => None,
    };
    datum.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "INSERT OVERWRITE PARTITION literal `{literal:?}` is not assignable to `{column}` \
             ({primitive})"
        ))
    })
}
