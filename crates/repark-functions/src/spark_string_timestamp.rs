use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use chrono::{DateTime, Utc};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{ColumnarValue, Expr, ExprSchemable};

pub(crate) mod grammar;
pub(crate) mod instant;
pub(crate) mod zone;

use zone::SparkZone;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StringCastFailure {
    Raise,
    Null,
}

#[must_use]
pub(crate) fn string_to_timestamp_micros(
    text: &str,
    session_zone: Tz,
    now: DateTime<Utc>,
) -> Option<i64> {
    let parsed = grammar::parse_timestamp_string(text)?;
    let zone = match parsed.zone {
        Some(zone_id) => zone::spark_zone_id(zone_id)?,
        None => SparkZone::Named(session_zone),
    };
    instant::instant_micros(&parsed, zone, now)
}

fn spark_sql_string_value(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[must_use]
pub(crate) fn cast_invalid_input_timestamp(text: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value {} of the type \"STRING\" cannot be cast to \
         \"TIMESTAMP\" because it is malformed. Correct the value as per the syntax, or change \
         its target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018",
        spark_sql_string_value(text)
    ))
}

#[must_use]
pub(crate) fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn cast_strings_to_ltz(
    strings: &ArrayRef,
    session_zone: Tz,
    failure: StringCastFailure,
) -> Result<ArrayRef> {
    let texts = cast(strings.as_ref(), &DataType::Utf8)?;
    let texts = texts.as_string::<i32>();
    let now = Utc::now();
    let mut builder = TimestampMicrosecondArray::builder(texts.len());
    for row in 0..texts.len() {
        if texts.is_null(row) {
            builder.append_null();
            continue;
        }
        let text = texts.value(row);
        match string_to_timestamp_micros(text, session_zone, now) {
            Some(micros) => builder.append_value(micros),
            None if failure == StringCastFailure::Raise => {
                return Err(cast_invalid_input_timestamp(text));
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish().with_timezone("UTC")))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn cast_columnar_strings_to_ltz(
    value: &ColumnarValue,
    session_zone: Tz,
    failure: StringCastFailure,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_strings_to_ltz(
            array,
            session_zone,
            failure,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            let produced = cast_strings_to_ltz(&scalar.to_array()?, session_zone, failure)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &produced, 0,
            )?))
        }
    }
}

fn ltz_literal_of_string(text: &str, session_zone: Tz) -> Option<ScalarValue> {
    string_to_timestamp_micros(text, session_zone, Utc::now()).map(|micros| {
        ScalarValue::TimestampMicrosecond(Some(micros), Some(Arc::<str>::from("UTC")))
    })
}

#[must_use]
pub(crate) fn spark_string_literal(expr: &Expr, zone: &str) -> Option<Expr> {
    let Expr::Literal(scalar, _) = expr else {
        return None;
    };
    let text = match scalar {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => text.as_str(),
        _ => return None,
    };
    let parsed_zone = zone.parse::<Tz>().ok()?;
    ltz_literal_of_string(text, parsed_zone).map(|literal| Expr::Literal(literal, None))
}

#[must_use]
pub(crate) fn rewrite_string_try_cast(expr: &Expr, schema: &DFSchema, zone: &str) -> Option<Expr> {
    let Expr::TryCast(try_cast) = expr else {
        return None;
    };
    if !matches!(try_cast.field.data_type(), DataType::Timestamp(_, _)) {
        return None;
    }
    let source = try_cast.expr.get_type(schema).ok()?;
    if !is_string_type(&source) {
        return None;
    }
    Some(
        spark_string_literal(&try_cast.expr, zone).unwrap_or_else(|| {
            Expr::ScalarFunction(ScalarFunction::new_udf(
                crate::timestamp_ltz_ntz::try_to_timestamp_udf(),
                vec![*try_cast.expr.clone()],
            ))
        }),
    )
}
