use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, DurationMicrosecondBuilder, Float64Array,
    Int32Array, Int64Array,
};
use arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float64Type, Int32Type, Int64Type, TimeUnit,
};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::intervals::interval_overflow;
use super::{exec_error, plan_error};

const MICROS_PER_MINUTE: i64 = 60_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const SECONDS_SCALE: i32 = 6;
const SECONDS_UNSCALED_LIMIT: i128 = 1_000_000_000_000_000_000;
const SECONDS_INTEGRAL_DIGITS: i32 = 12;
const MAX_ARGS: usize = 4;
const FAST_PATH_MARGIN: f64 = 0.25;

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![make_dt_interval_udf()]
}

#[must_use]
pub fn make_dt_interval_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(MakeDtInterval::new()))
}

#[must_use]
pub fn day_time_cast_expr(expr: Expr, try_cast: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(DayTimeCast::new(try_cast))),
        vec![expr],
    ))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DayTimeCast {
    signature: Signature,
    try_cast: bool,
}

impl DayTimeCast {
    fn new(try_cast: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            try_cast,
        }
    }
}

impl ScalarUDFImpl for DayTimeCast {
    fn name(&self) -> &str {
        if self.try_cast {
            "__repark_try_cast_day_time__"
        } else {
            "__repark_cast_day_time__"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Duration(TimeUnit::Microsecond))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = self.try_cast || args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Duration(TimeUnit::Microsecond),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Duration(TimeUnit::Microsecond) | DataType::Null] => {
                Ok(vec![DataType::Duration(TimeUnit::Microsecond)])
            }
            [other] => Err(plan_error(format!(
                "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve the cast because of \
                 a data type mismatch: cannot cast \"{}\" to \"INTERVAL DAY TO SECOND\". SQLSTATE: 42K09",
                crate::spark_math::spark_type_name(other).to_uppercase()
            ))),
            _ => Err(plan_error(format!(
                "a day-time interval cast takes one argument, got {}",
                arg_types.len()
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.into_iter().next() {
            Some(value) => Ok(value),
            None => Err(exec_error(
                "a day-time interval cast takes one argument".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
struct MakeDtInterval {
    signature: Signature,
}

impl MakeDtInterval {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for MakeDtInterval {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for MakeDtInterval {}

impl Hash for MakeDtInterval {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for MakeDtInterval {
    crate::shim_udf_boilerplate!("make_dt_interval");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Duration(TimeUnit::Microsecond))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable())
            || args
                .arg_fields
                .get(3)
                .is_some_and(|field| seconds_force_nullable(field.data_type()));
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Duration(TimeUnit::Microsecond),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > MAX_ARGS {
            return Err(wrong_num_args(arg_types.len()));
        }
        arg_types
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                if index < 3 {
                    part_type(data_type)
                } else {
                    seconds_type(data_type)
                }
            })
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(
                Some(0),
            )));
        }
        if args.args.len() > MAX_ARGS {
            return Err(wrong_num_args(args.args.len()));
        }
        let ansi = crate::ansi::spark_ansi_enabled_from_options(&args.config_options);
        let all_scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let built = build(&arrays, ansi)?;
        if all_scalar {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &built, 0,
            )?));
        }
        Ok(ColumnarValue::Array(built))
    }
}

fn wrong_num_args(count: usize) -> DataFusionError {
    plan_error(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `make_dt_interval` requires [0, 1, 2, 3, 4] \
         parameters but the actual number is {count}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

fn signature_mismatch(position: &str, data_type: &DataType) -> DataFusionError {
    plan_error(format!(
        "Function 'make_dt_interval' failed to match any signature: the {position} argument \
         takes a number, got {data_type}"
    ))
}

fn part_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Null
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(DataType::Int32),
        other => Err(signature_mismatch("days, hours or minutes", other)),
    }
}

fn seconds_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            Ok(DataType::Int32)
        }
        DataType::Int64 | DataType::UInt32 => Ok(DataType::Int64),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => Ok(DataType::Decimal128(*precision, *scale)),
        DataType::Null
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::UInt64
        | DataType::Decimal256(_, _) => Ok(DataType::Float64),
        other => Err(signature_mismatch("seconds", other)),
    }
}

fn seconds_force_nullable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            false
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            let carry = i32::from(i32::from(*scale) > SECONDS_SCALE);
            i32::from(*precision) - i32::from(*scale) + carry > SECONDS_INTEGRAL_DIGITS
        }
        _ => true,
    }
}

enum Seconds<'a> {
    Zero,
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float(&'a Float64Array),
    Decimal(&'a Decimal128Array, i8),
}

fn int32_part(arrays: &[ArrayRef], index: usize) -> Result<Option<&Int32Array>> {
    arrays
        .get(index)
        .map(|array| {
            array.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                exec_error(format!(
                    "make_dt_interval argument {index} must be Int32, got {}",
                    array.data_type()
                ))
            })
        })
        .transpose()
}

fn seconds_input(arrays: &[ArrayRef]) -> Result<Seconds<'_>> {
    let Some(array) = arrays.get(3) else {
        return Ok(Seconds::Zero);
    };
    let unexpected = || {
        exec_error(format!(
            "make_dt_interval seconds argument has an unexpected type {}",
            array.data_type()
        ))
    };
    match array.data_type() {
        DataType::Int32 => array
            .as_primitive_opt::<Int32Type>()
            .map(Seconds::Int32)
            .ok_or_else(unexpected),
        DataType::Int64 => array
            .as_primitive_opt::<Int64Type>()
            .map(Seconds::Int64)
            .ok_or_else(unexpected),
        DataType::Float64 => array
            .as_primitive_opt::<Float64Type>()
            .map(Seconds::Float)
            .ok_or_else(unexpected),
        DataType::Decimal128(_, scale) => array
            .as_primitive_opt::<Decimal128Type>()
            .map(|values| Seconds::Decimal(values, *scale))
            .ok_or_else(unexpected),
        _ => Err(unexpected()),
    }
}

fn part_value(values: Option<&Int32Array>, row: usize) -> Option<i64> {
    match values {
        None => Some(0),
        Some(values) if values.is_null(row) => None,
        Some(values) => Some(i64::from(values.value(row))),
    }
}

fn build(arrays: &[ArrayRef], ansi: bool) -> Result<ArrayRef> {
    let width = arrays.first().map_or(0, Array::len);
    let days = int32_part(arrays, 0)?;
    let hours = int32_part(arrays, 1)?;
    let mins = int32_part(arrays, 2)?;
    let seconds = seconds_input(arrays)?;
    let mut builder = DurationMicrosecondBuilder::with_capacity(width);
    for row in 0..width {
        let value = match (
            part_value(days, row),
            part_value(hours, row),
            part_value(mins, row),
        ) {
            (Some(day), Some(hour), Some(min)) => match seconds_micros(&seconds, row, ansi)? {
                Some(micros) => Some(day_time_micros(day, hour, min, micros)?),
                None => None,
            },
            _ => None,
        };
        builder.append_option(value);
    }
    Ok(Arc::new(builder.finish()))
}

fn day_time_micros(day: i64, hour: i64, min: i64, micros: i64) -> Result<i64> {
    let total = day
        .checked_mul(MICROS_PER_DAY)
        .and_then(|part| micros.checked_add(part))
        .and_then(|total| {
            hour.checked_mul(MICROS_PER_HOUR)
                .and_then(|part| total.checked_add(part))
        })
        .and_then(|total| {
            min.checked_mul(MICROS_PER_MINUTE)
                .and_then(|part| total.checked_add(part))
        });
    total.ok_or_else(interval_overflow)
}

#[derive(Clone, Copy)]
enum Shown {
    Int(i64),
    Float(f64),
    Decimal(i128, i8),
}

impl Shown {
    fn text(self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Decimal(value, scale) => decimal_text(value, scale),
        }
    }
}

fn seconds_micros(seconds: &Seconds<'_>, row: usize, ansi: bool) -> Result<Option<i64>> {
    let (unscaled, shown) = match seconds {
        Seconds::Zero => return Ok(Some(0)),
        Seconds::Int32(values) => {
            if values.is_null(row) {
                return Ok(None);
            }
            let value = i64::from(values.value(row));
            (rescale(i128::from(value), 0), Shown::Int(value))
        }
        Seconds::Int64(values) => {
            if values.is_null(row) {
                return Ok(None);
            }
            let value = values.value(row);
            (rescale(i128::from(value), 0), Shown::Int(value))
        }
        Seconds::Float(values) => {
            if values.is_null(row) || !values.value(row).is_finite() {
                return Ok(None);
            }
            let value = values.value(row);
            (float_unscaled(value), Shown::Float(value))
        }
        Seconds::Decimal(values, scale) => {
            if values.is_null(row) {
                return Ok(None);
            }
            let value = values.value(row);
            (
                rescale(value, i32::from(*scale)),
                Shown::Decimal(value, *scale),
            )
        }
    };
    match unscaled.and_then(|value| i64::try_from(value).ok()) {
        Some(micros) => Ok(Some(micros)),
        None if ansi => Err(seconds_out_of_range(&shown.text())),
        None => Ok(None),
    }
}

fn within_limit(value: i128) -> Option<i128> {
    (value.checked_abs()? < SECONDS_UNSCALED_LIMIT).then_some(value)
}

fn rescale(value: i128, scale: i32) -> Option<i128> {
    if scale <= SECONDS_SCALE {
        let factor = 10_i128.checked_pow(u32::try_from(SECONDS_SCALE - scale).ok()?)?;
        return within_limit(value.checked_mul(factor)?);
    }
    let shift = u32::try_from(scale - SECONDS_SCALE).ok()?;
    let magnitude = round_half_up(value.checked_abs()?, shift);
    within_limit(if value < 0 { -magnitude } else { magnitude })
}

fn round_half_up(magnitude: i128, shift: u32) -> i128 {
    let Some(divisor) = 10_i128.checked_pow(shift) else {
        return 0;
    };
    let quotient = magnitude / divisor;
    let remainder = magnitude % divisor;
    if remainder >= divisor - remainder {
        quotient + 1
    } else {
        quotient
    }
}

fn float_unscaled(value: f64) -> Option<i128> {
    float_unscaled_fast(value).or_else(|| float_unscaled_exact(value))
}

#[allow(clippy::cast_possible_truncation)]
fn float_unscaled_fast(value: f64) -> Option<i128> {
    let magnitude = (value * 1_000_000.0).abs();
    let margin = 4.0 * magnitude * f64::EPSILON;
    let fraction = magnitude - magnitude.floor();
    if margin >= FAST_PATH_MARGIN || (fraction - 0.5).abs() <= margin {
        return None;
    }
    let rounded = magnitude.round() as i128;
    Some(if value.is_sign_negative() {
        -rounded
    } else {
        rounded
    })
}

fn float_unscaled_exact(value: f64) -> Option<i128> {
    let text = format!("{:e}", value.abs());
    let (mantissa, exponent) = text.split_once('e')?;
    let exponent: i32 = exponent.parse().ok()?;
    let fraction_len = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits: i128 = mantissa
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    let scale = i32::try_from(fraction_len).ok()?.checked_sub(exponent)?;
    let unscaled = rescale(digits, scale)?;
    Some(if value.is_sign_negative() {
        -unscaled
    } else {
        unscaled
    })
}

fn decimal_text(value: i128, scale: i8) -> String {
    let Ok(scale) = usize::try_from(scale) else {
        return value.to_string();
    };
    if scale == 0 {
        return value.to_string();
    }
    let digits = value.unsigned_abs().to_string();
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    let sign = if value < 0 { "-" } else { "" };
    format!("{sign}{whole}.{fraction}")
}

fn seconds_out_of_range(shown: &str) -> DataFusionError {
    exec_error(format!(
        "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {shown} cannot be represented as \
         Decimal(18, 6). If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass \
         this error, and return NULL instead. SQLSTATE: 22003"
    ))
}

#[cfg(test)]
#[path = "day_time_tests.rs"]
mod tests;
