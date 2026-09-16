use std::sync::Arc;

use arrow::compute::{DatePart, date_part};
use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub const DATE_PART_NAME: &str = "__repark_date_part__";

#[must_use]
pub fn date_part_udf() -> Arc<ScalarUDF> {
    Arc::new(
        ScalarUDF::from(SparkDatePart {
            signature: Signature::user_defined(Volatility::Immutable),
        })
        .with_aliases(["date_part", "datepart"]),
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkDatePart {
    signature: Signature,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DatePartKind {
    Arrow(DatePart),
    Dow,
    DowIso,
    SecondFraction,
}

fn field_kind(name: &str) -> Option<DatePartKind> {
    Some(match name {
        "year" | "y" | "years" | "yr" | "yrs" => DatePartKind::Arrow(DatePart::Year),
        "yearofweek" => DatePartKind::Arrow(DatePart::YearISO),
        "quarter" | "qtr" => DatePartKind::Arrow(DatePart::Quarter),
        "month" | "mon" | "mons" | "months" => DatePartKind::Arrow(DatePart::Month),
        "week" | "w" | "weeks" => DatePartKind::Arrow(DatePart::WeekISO),
        "day" | "d" | "days" => DatePartKind::Arrow(DatePart::Day),
        "dayofweek" | "dow" => DatePartKind::Dow,
        "dayofweek_iso" | "dow_iso" => DatePartKind::DowIso,
        "doy" => DatePartKind::Arrow(DatePart::DayOfYear),
        "hour" | "h" | "hours" | "hr" | "hrs" => DatePartKind::Arrow(DatePart::Hour),
        "minute" | "m" | "min" | "mins" | "minutes" => DatePartKind::Arrow(DatePart::Minute),
        "second" | "s" | "sec" | "seconds" | "secs" => DatePartKind::SecondFraction,
        _ => return None,
    })
}

fn literal_field_name(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(value.to_lowercase()),
        _ => None,
    }
}

fn invalid_extract_field(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INVALID_EXTRACT_FIELD] Cannot extract `{name}` from <source>. SQLSTATE: 42601"
    ))
}

fn microsecond_fraction(array: &ArrayRef) -> Result<ArrayRef> {
    use arrow::datatypes::TimeUnit;
    let values: Vec<Option<i128>> = match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| DataFusionError::Execution("expected timestamp micros".into()))?;
            (0..typed.len())
                .map(|row| {
                    if typed.is_null(row) {
                        None
                    } else {
                        let micros = typed.value(row).rem_euclid(60_000_000);
                        Some(i128::from(micros))
                    }
                })
                .collect()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| DataFusionError::Execution("expected timestamp millis".into()))?;
            (0..typed.len())
                .map(|row| {
                    if typed.is_null(row) {
                        None
                    } else {
                        let micros = typed.value(row).rem_euclid(60_000) * 1000;
                        Some(i128::from(micros))
                    }
                })
                .collect()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| DataFusionError::Execution("expected timestamp nanos".into()))?;
            (0..typed.len())
                .map(|row| {
                    if typed.is_null(row) {
                        None
                    } else {
                        let nanos = typed.value(row).rem_euclid(60_000_000_000);
                        Some(i128::from(nanos / 1000))
                    }
                })
                .collect()
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .ok_or_else(|| DataFusionError::Execution("expected timestamp seconds".into()))?;
            (0..typed.len())
                .map(|row| {
                    if typed.is_null(row) {
                        None
                    } else {
                        let micros = typed.value(row).rem_euclid(60) * 1_000_000;
                        Some(i128::from(micros))
                    }
                })
                .collect()
        }
        DataType::Date32 | DataType::Date64 => (0..array.len())
            .map(|row| (!array.is_null(row)).then_some(0i128))
            .collect(),
        other => {
            return Err(DataFusionError::Execution(format!(
                "date_part SECONDS unsupported source {other}"
            )));
        }
    };
    let result = Decimal128Array::from(values)
        .with_precision_and_scale(8, 6)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    Ok(Arc::new(result))
}

impl ScalarUDFImpl for SparkDatePart {
    fn name(&self) -> &'static str {
        DATE_PART_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let data_type = match args.scalar_arguments.first().and_then(|s| *s) {
            Some(scalar) => match literal_field_name(scalar) {
                Some(name) => match field_kind(&name) {
                    Some(DatePartKind::SecondFraction) => DataType::Decimal128(8, 6),
                    Some(_) => DataType::Int32,
                    None => return Err(invalid_extract_field(&name)),
                },
                None => DataType::Int32,
            },
            None => DataType::Int32,
        };
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `date_part` requires 2 parameters but \
                 the actual number is {}",
                arg_types.len()
            )));
        }
        Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let name = match &args.args[0] {
            ColumnarValue::Scalar(scalar) => {
                literal_field_name(scalar).ok_or_else(|| invalid_extract_field("<expr>"))?
            }
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Plan(
                    "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] The `date_part` field argument \
                     should be a foldable \"STRING\" expression."
                        .to_string(),
                ));
            }
        };
        let kind = field_kind(&name).ok_or_else(|| invalid_extract_field(&name))?;
        let arrays = ColumnarValue::values_to_arrays(&args.args[1..])?;
        let source = &arrays[0];
        let source = if matches!(source.data_type(), DataType::Date64) {
            arrow::compute::cast(source, &DataType::Date32)?
        } else {
            source.clone()
        };
        match kind {
            DatePartKind::SecondFraction => {
                Ok(ColumnarValue::Array(microsecond_fraction(&source)?))
            }
            DatePartKind::DowIso => {
                let parts = date_part(source.as_ref(), DatePart::DayOfWeekMonday0)?;
                let ints = parts.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    DataFusionError::Execution("date_part isodow expected int32".into())
                })?;
                let shifted: Vec<Option<i32>> = (0..ints.len())
                    .map(|row| {
                        if ints.is_null(row) {
                            None
                        } else {
                            Some(ints.value(row) + 1)
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(shifted))))
            }
            DatePartKind::Dow => {
                let parts = date_part(source.as_ref(), DatePart::DayOfWeekSunday0)?;
                let ints = parts.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    DataFusionError::Execution("date_part dow expected int32".into())
                })?;
                let shifted: Vec<Option<i32>> = (0..ints.len())
                    .map(|row| {
                        if ints.is_null(row) {
                            None
                        } else {
                            Some(ints.value(row) + 1)
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(shifted))))
            }
            DatePartKind::Arrow(part) => {
                Ok(ColumnarValue::Array(date_part(source.as_ref(), part)?))
            }
        }
    }
}
