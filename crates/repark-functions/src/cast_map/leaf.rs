use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Int64Array, PrimitiveArray,
    StringArray, new_null_array,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, i256,
};
use datafusion::common::{DataFusionError, Result};

use super::spark_sql_name;
use crate::java_double::{java_double_text, java_float_text};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Ansi,
    Legacy,
    Try,
}

impl Mode {
    pub(super) fn nulls_on_failure(self) -> bool {
        self != Self::Ansi
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_integral(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_fractional(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn is_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn is_numeric(data_type: &DataType) -> bool {
    is_integral(data_type) || is_fractional(data_type) || is_decimal(data_type)
}

fn is_date(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

fn is_timestamp(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _))
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn integral_bits(data_type: &DataType) -> u32 {
    match data_type {
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 => 16,
        DataType::Int32 | DataType::UInt32 => 32,
        _ => 64,
    }
}

fn decimal_parts(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

fn fits_decimal(source: &DataType, target: &DataType) -> bool {
    let Some((precision, scale)) = decimal_parts(target) else {
        return false;
    };
    let integer_digits = i16::from(precision) - i16::from(scale);
    match source {
        DataType::Boolean => integer_digits >= 1,
        DataType::Int8 | DataType::UInt8 => integer_digits >= 3,
        DataType::Int16 | DataType::UInt16 => integer_digits >= 5,
        DataType::Int32 | DataType::UInt32 => integer_digits >= 10,
        DataType::Int64 | DataType::UInt64 => integer_digits >= 20,
        other => decimal_parts(other).is_some_and(|(from_precision, from_scale)| {
            from_scale <= scale
                && i16::from(from_precision) - i16::from(from_scale) <= integer_digits
        }),
    }
}

pub(super) fn atomic_castable(source: &DataType, target: &DataType, ansi: bool) -> bool {
    if source == target || source == &DataType::Null || is_string(target) {
        return true;
    }
    if is_string(source) {
        return true;
    }
    let numeric_like =
        |data_type: &DataType| is_numeric(data_type) || data_type == &DataType::Boolean;
    match (source, target) {
        (from, to) if numeric_like(from) && numeric_like(to) => true,
        (from, to) if is_numeric(from) && is_timestamp(to) => true,
        (DataType::Boolean, to) if is_timestamp(to) => !ansi,
        (from, to) if is_date(from) && is_timestamp(to) => true,
        (from, to) if is_timestamp(from) && (is_date(to) || is_timestamp(to)) => true,
        (from, to) if is_timestamp(from) && is_numeric(to) => true,
        (from, DataType::Boolean) if is_timestamp(from) => !ansi,
        (from, to) if is_integral(from) && is_binary(to) => !ansi,
        _ => false,
    }
}

pub(super) fn legacy_force_nullable(source: &DataType, target: &DataType) -> bool {
    if source == target || source == &DataType::Null {
        return false;
    }
    if is_string(source) {
        return !is_string(target) && !is_binary(target);
    }
    if is_string(target) {
        return false;
    }
    if is_timestamp(source) && is_integral(target) {
        return integral_bits(target) < 64;
    }
    if is_decimal(target) {
        return !fits_decimal(source, target);
    }
    (is_fractional(source) || is_decimal(source)) && (is_integral(target) || is_timestamp(target))
}

pub(super) fn key_castable(source: &DataType, target: &DataType, mode: Mode) -> bool {
    match mode {
        Mode::Ansi => atomic_castable(source, target, true),
        Mode::Legacy | Mode::Try => {
            atomic_castable(source, target, false) && !legacy_force_nullable(source, target)
        }
    }
}

pub(super) fn cast_invalid_input(
    value: &str,
    source: &DataType,
    target: &DataType,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{}' of the type \"{}\" cannot be cast to \"{}\" \
         because it is malformed. Correct the value as per the syntax, or change its target \
         type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: \
         22018",
        value.replace('\0', "\\u0000"),
        spark_sql_name(source),
        spark_sql_name(target)
    ))
}

fn cast_overflow(value: &str, source: &DataType, target: &DataType) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_OVERFLOW] The value {value} of the type \"{}\" cannot be cast to \"{}\" due to an \
         overflow. Use `try_cast` to tolerate overflow and return NULL instead. SQLSTATE: 22003",
        spark_sql_name(source),
        spark_sql_name(target)
    ))
}

fn integral_literal(value: i64, source: &DataType) -> String {
    match source {
        DataType::Int8 => format!("{value}Y"),
        DataType::Int16 => format!("{value}S"),
        DataType::Int64 => format!("{value}L"),
        _ => value.to_string(),
    }
}

fn spark_trim(text: &str) -> &str {
    text.trim_matches(|character: char| character <= ' ' || character == '\u{7f}')
}

fn parse_strict_integer(text: &str) -> Option<i64> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse::<i64>().ok()
}

fn parse_legacy_integer(text: &str) -> Option<i64> {
    let Some((whole, fraction)) = text.split_once('.') else {
        return parse_strict_integer(text);
    };
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if matches!(whole, "" | "+" | "-") {
        return Some(0);
    }
    parse_strict_integer(whole)
}

fn parse_boolean(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "0" => Some(false),
        _ => None,
    }
}

fn integral_range(target: &DataType) -> (i64, i64) {
    match target {
        DataType::Int8 => (i64::from(i8::MIN), i64::from(i8::MAX)),
        DataType::Int16 => (i64::from(i16::MIN), i64::from(i16::MAX)),
        DataType::Int32 => (i64::from(i32::MIN), i64::from(i32::MAX)),
        _ => (i64::MIN, i64::MAX),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn wrap_integral(value: i64, target: &DataType) -> i64 {
    match target {
        DataType::Int8 => i64::from(value as i8),
        DataType::Int16 => i64::from(value as i16),
        DataType::Int32 => i64::from(value as i32),
        _ => value,
    }
}

fn build_integral(values: Vec<Option<i64>>, target: &DataType) -> Result<ArrayRef> {
    Ok(match target {
        DataType::Int8 => Arc::new(narrow::<Int8Type>(&values)?) as ArrayRef,
        DataType::Int16 => Arc::new(narrow::<Int16Type>(&values)?),
        DataType::Int32 => Arc::new(narrow::<Int32Type>(&values)?),
        _ => Arc::new(Int64Array::from(values)),
    })
}

fn narrow<T: ArrowPrimitiveType>(values: &[Option<i64>]) -> Result<PrimitiveArray<T>>
where
    T::Native: TryFrom<i64>,
{
    values
        .iter()
        .map(|value| {
            value
                .map(|inner| {
                    T::Native::try_from(inner).map_err(|_| {
                        DataFusionError::Internal(format!("integral {inner} out of range"))
                    })
                })
                .transpose()
        })
        .collect()
}

fn strings(source: &ArrayRef) -> Result<Vec<Option<&str>>> {
    Ok(match source.data_type() {
        DataType::Utf8 => source.as_string::<i32>().iter().collect(),
        DataType::LargeUtf8 => source.as_string::<i64>().iter().collect(),
        DataType::Utf8View => source.as_string_view().iter().collect(),
        other => {
            return Err(DataFusionError::Internal(format!(
                "expected a string leaf, got {other}"
            )));
        }
    })
}

pub(super) fn string_to_integral(
    source: &ArrayRef,
    target: &DataType,
    mode: Mode,
) -> Result<ArrayRef> {
    let (low, high) = integral_range(target);
    let mut out = Vec::with_capacity(source.len());
    for text in strings(source)? {
        let Some(text) = text else {
            out.push(None);
            continue;
        };
        let trimmed = spark_trim(text);
        let parsed = if mode == Mode::Legacy {
            parse_legacy_integer(trimmed)
        } else {
            parse_strict_integer(trimmed)
        }
        .filter(|value| (low..=high).contains(value));
        if parsed.is_none() && mode == Mode::Ansi {
            return Err(cast_invalid_input(text, source.data_type(), target));
        }
        out.push(parsed);
    }
    build_integral(out, target)
}

fn string_to_boolean(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let mut out = Vec::with_capacity(source.len());
    for text in strings(source)? {
        let Some(text) = text else {
            out.push(None);
            continue;
        };
        let parsed = parse_boolean(spark_trim(text));
        if parsed.is_none() && mode == Mode::Ansi {
            return Err(cast_invalid_input(text, source.data_type(), target));
        }
        out.push(parsed);
    }
    Ok(Arc::new(BooleanArray::from(out)))
}

fn string_via_arrow(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let texts = strings(source)?;
    let trimmed: ArrayRef = Arc::new(StringArray::from(
        texts
            .iter()
            .map(|text| text.map(spark_trim))
            .collect::<Vec<_>>(),
    ));
    let lenient = CastOptions {
        safe: true,
        ..CastOptions::default()
    };
    let cast = cast_with_options(trimmed.as_ref(), target, &lenient)?;
    if mode == Mode::Ansi
        && let Some(row) = (0..cast.len()).find(|&row| trimmed.is_valid(row) && cast.is_null(row))
    {
        let original = texts.get(row).copied().flatten().unwrap_or_default();
        return Err(cast_invalid_input(original, source.data_type(), target));
    }
    Ok(cast)
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    if scale < 0 {
        if unscaled == 0 {
            return "0".to_owned();
        }
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return format!("{unscaled}{zeros}");
    }
    let digits = unscaled.unsigned_abs().to_string();
    let width = usize::from(scale.unsigned_abs());
    let body = if digits.len() > width {
        let split = digits.len() - width;
        format!("{}.{}", &digits[..split], &digits[split..])
    } else {
        format!("0.{digits:0>width$}")
    };
    if unscaled < 0 {
        format!("-{body}")
    } else {
        body
    }
}

fn decimal_overflow(
    unscaled: i128,
    from_scale: i8,
    to_precision: u8,
    to_scale: i8,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {} cannot be represented as \
         Decimal({to_precision}, {to_scale}). If necessary set \"spark.sql.ansi.enabled\" to \
         \"false\" to bypass this error, and return NULL instead. SQLSTATE: 22003",
        format_decimal(unscaled, from_scale)
    ))
}

fn try_map_primitive<S, O, F>(source: &PrimitiveArray<S>, mut map: F) -> Result<PrimitiveArray<O>>
where
    S: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: FnMut(S::Native) -> Result<O::Native>,
{
    let mut out = Vec::with_capacity(source.len());
    if let Some(nulls) = source.nulls() {
        for (index, &value) in source.values().iter().enumerate() {
            if nulls.is_valid(index) {
                out.push(map(value)?);
            } else {
                out.push(O::default_value());
            }
        }
        Ok(PrimitiveArray::<O>::new(out.into(), Some(nulls.clone())))
    } else {
        for &value in source.values() {
            out.push(map(value)?);
        }
        Ok(PrimitiveArray::<O>::new(out.into(), None))
    }
}

fn narrow_signed_array<S, O>(
    source: &PrimitiveArray<S>,
    from_type: &DataType,
    target: &DataType,
    mode: Mode,
    wrap: fn(S::Native) -> O::Native,
) -> Result<ArrayRef>
where
    S: ArrowPrimitiveType,
    S::Native: Into<i64>,
    O: ArrowPrimitiveType,
    O::Native: TryFrom<S::Native>,
{
    match mode {
        Mode::Legacy => Ok(Arc::new(source.unary::<_, O>(wrap))),
        Mode::Try => {
            Ok(Arc::new(source.unary_opt::<_, O>(|value| {
                O::Native::try_from(value).ok()
            })))
        }
        Mode::Ansi => Ok(Arc::new(try_map_primitive::<S, O, _>(source, |value| {
            O::Native::try_from(value).map_err(|_| {
                cast_overflow(
                    &integral_literal(value.into(), from_type),
                    from_type,
                    target,
                )
            })
        })?)),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn signed_to_signed(source: &ArrayRef, target: &DataType, mode: Mode) -> Option<Result<ArrayRef>> {
    let from = source.data_type();
    match (from, target) {
        (DataType::Int64, DataType::Int32) => Some(narrow_signed_array::<Int64Type, Int32Type>(
            source.as_primitive::<Int64Type>(),
            from,
            target,
            mode,
            |value| value as i32,
        )),
        (DataType::Int64, DataType::Int16) => Some(narrow_signed_array::<Int64Type, Int16Type>(
            source.as_primitive::<Int64Type>(),
            from,
            target,
            mode,
            |value| value as i16,
        )),
        (DataType::Int64, DataType::Int8) => Some(narrow_signed_array::<Int64Type, Int8Type>(
            source.as_primitive::<Int64Type>(),
            from,
            target,
            mode,
            |value| value as i8,
        )),
        (DataType::Int32, DataType::Int16) => Some(narrow_signed_array::<Int32Type, Int16Type>(
            source.as_primitive::<Int32Type>(),
            from,
            target,
            mode,
            |value| value as i16,
        )),
        (DataType::Int32, DataType::Int8) => Some(narrow_signed_array::<Int32Type, Int8Type>(
            source.as_primitive::<Int32Type>(),
            from,
            target,
            mode,
            |value| value as i8,
        )),
        (DataType::Int16, DataType::Int8) => Some(narrow_signed_array::<Int16Type, Int8Type>(
            source.as_primitive::<Int16Type>(),
            from,
            target,
            mode,
            |value| value as i8,
        )),
        _ => None,
    }
}

fn integral_to_integral(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    if let Some(fast) = signed_to_signed(source, target, mode) {
        return fast;
    }
    let wide = cast_with_options(source.as_ref(), &DataType::Int64, &CastOptions::default())?;
    let (low, high) = integral_range(target);
    let mut out = Vec::with_capacity(wide.len());
    for value in wide.as_primitive::<Int64Type>() {
        out.push(match value {
            None => None,
            Some(inner) if (low..=high).contains(&inner) => Some(inner),
            Some(inner) => match mode {
                Mode::Ansi => {
                    return Err(cast_overflow(
                        &integral_literal(inner, source.data_type()),
                        source.data_type(),
                        target,
                    ));
                }
                Mode::Legacy => Some(wrap_integral(inner, target)),
                Mode::Try => None,
            },
        });
    }
    build_integral(out, target)
}

#[allow(clippy::cast_possible_truncation)]
fn truncate_decimal_to_i128(unscaled: i128, scale: i8) -> (Option<i128>, i64) {
    if unscaled == 0 {
        return (Some(0), 0);
    }
    if scale == 0 {
        return (Some(unscaled), unscaled as i64);
    }
    if scale > 0 {
        let drop = u32::from(scale.unsigned_abs());
        if drop > 38 {
            return (Some(0), 0);
        }
        let whole = unscaled / 10i128.pow(drop);
        return (Some(whole), whole as i64);
    }
    let shift = u32::from(scale.unsigned_abs());
    let Some(factor) = i256::from_i128(10).checked_pow(shift) else {
        return (None, 0);
    };
    let Some(wide) = i256::from_i128(unscaled).checked_mul(factor) else {
        return (None, 0);
    };
    let bytes = wide.to_le_bytes();
    let low = i64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    (wide.to_i128(), low)
}

fn decimal_array_to_signed<O>(
    source: &Decimal128Array,
    from_type: &DataType,
    scale: i8,
    target: &DataType,
    mode: Mode,
    wrap: fn(i64) -> O::Native,
) -> Result<ArrayRef>
where
    O: ArrowPrimitiveType,
    O::Native: TryFrom<i128>,
{
    match mode {
        Mode::Legacy => Ok(Arc::new(source.unary::<_, O>(|unscaled| {
            wrap(truncate_decimal_to_i128(unscaled, scale).1)
        }))),
        Mode::Try => Ok(Arc::new(source.unary_opt::<_, O>(|unscaled| {
            truncate_decimal_to_i128(unscaled, scale)
                .0
                .and_then(|whole| O::Native::try_from(whole).ok())
        }))),
        Mode::Ansi => Ok(Arc::new(try_map_primitive::<Decimal128Type, O, _>(
            source,
            |unscaled| {
                truncate_decimal_to_i128(unscaled, scale)
                    .0
                    .and_then(|whole| O::Native::try_from(whole).ok())
                    .ok_or_else(|| {
                        cast_overflow(
                            &format!("{}BD", format_decimal(unscaled, scale)),
                            from_type,
                            target,
                        )
                    })
            },
        )?)),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn decimal_to_integral(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let from = source.data_type();
    let DataType::Decimal128(_, scale) = from else {
        let options = CastOptions {
            safe: mode.nulls_on_failure(),
            ..CastOptions::default()
        };
        return Ok(cast_with_options(source.as_ref(), target, &options)?);
    };
    let scale = *scale;
    let decimals = source.as_primitive::<Decimal128Type>();
    match target {
        DataType::Int8 => {
            decimal_array_to_signed::<Int8Type>(decimals, from, scale, target, mode, |low| {
                low as i8
            })
        }
        DataType::Int16 => {
            decimal_array_to_signed::<Int16Type>(decimals, from, scale, target, mode, |low| {
                low as i16
            })
        }
        DataType::Int32 => {
            decimal_array_to_signed::<Int32Type>(decimals, from, scale, target, mode, |low| {
                low as i32
            })
        }
        DataType::Int64 => {
            decimal_array_to_signed::<Int64Type>(decimals, from, scale, target, mode, |low| low)
        }
        other => Err(DataFusionError::Internal(format!(
            "expected a signed integral target, got {other}"
        ))),
    }
}

fn rescale_decimal(unscaled: i128, from_scale: i8, to_precision: u8, to_scale: i8) -> Option<i128> {
    let limit = 10u128.checked_pow(u32::from(to_precision))?;
    let delta = i32::from(to_scale) - i32::from(from_scale);
    let rescaled = match delta.cmp(&0) {
        std::cmp::Ordering::Equal => Some(unscaled),
        std::cmp::Ordering::Greater => {
            if unscaled == 0 {
                Some(0)
            } else {
                let step = u32::try_from(delta).ok()?;
                if step > 38 {
                    None
                } else {
                    10i128
                        .checked_pow(step)
                        .and_then(|factor| unscaled.checked_mul(factor))
                }
            }
        }
        std::cmp::Ordering::Less => {
            let drop = u32::try_from(-delta).ok()?;
            if drop > 38 {
                Some(0)
            } else {
                let factor = 10i128.checked_pow(drop)?;
                let half = (factor / 2).unsigned_abs();
                let quotient = unscaled / factor;
                let remainder = unscaled % factor;
                if remainder.unsigned_abs() >= half {
                    quotient.checked_add(if unscaled >= 0 { 1 } else { -1 })
                } else {
                    Some(quotient)
                }
            }
        }
    };
    rescaled.filter(|value| value.unsigned_abs() < limit)
}

fn decimal_to_decimal(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let (&DataType::Decimal128(_, from_scale), &DataType::Decimal128(to_precision, to_scale)) =
        (source.data_type(), target)
    else {
        let options = CastOptions {
            safe: mode.nulls_on_failure(),
            ..CastOptions::default()
        };
        return Ok(cast_with_options(source.as_ref(), target, &options)?);
    };
    let decimals = source.as_primitive::<Decimal128Type>();
    let out: Decimal128Array = match mode {
        Mode::Legacy | Mode::Try => decimals
            .unary_opt(|unscaled| rescale_decimal(unscaled, from_scale, to_precision, to_scale)),
        Mode::Ansi => {
            try_map_primitive::<Decimal128Type, Decimal128Type, _>(decimals, |unscaled| {
                rescale_decimal(unscaled, from_scale, to_precision, to_scale)
                    .ok_or_else(|| decimal_overflow(unscaled, from_scale, to_precision, to_scale))
            })?
        }
    };
    Ok(Arc::new(
        out.with_precision_and_scale(to_precision, to_scale)?,
    ))
}

fn float32_literal(value: f32) -> String {
    java_float_text(value)
}

fn float64_literal(value: f64) -> String {
    if value.is_finite() {
        format!("{}D", java_double_text(value))
    } else {
        java_double_text(value)
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn float_array_to_signed<F, O>(
    source: &PrimitiveArray<F>,
    from_type: &DataType,
    target: &DataType,
    mode: Mode,
    format_value: fn(F::Native) -> String,
    legacy: fn(f64) -> O::Native,
    checked: fn(f64) -> Option<O::Native>,
) -> Result<ArrayRef>
where
    F: ArrowPrimitiveType,
    F::Native: Into<f64>,
    O: ArrowPrimitiveType,
{
    match mode {
        Mode::Legacy => Ok(Arc::new(source.unary::<_, O>(|value| legacy(value.into())))),
        Mode::Try => Ok(Arc::new(
            source.unary_opt::<_, O>(|value| checked(value.into())),
        )),
        Mode::Ansi => Ok(Arc::new(try_map_primitive::<F, O, _>(source, |value| {
            checked(value.into())
                .ok_or_else(|| cast_overflow(&format_value(value), from_type, target))
        })?)),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn float_checked_i8(value: f64) -> Option<i8> {
    let truncated = value.trunc();
    (truncated.is_finite() && truncated >= f64::from(i8::MIN) && truncated <= f64::from(i8::MAX))
        .then_some(truncated as i8)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn float_checked_i16(value: f64) -> Option<i16> {
    let truncated = value.trunc();
    (truncated.is_finite() && truncated >= f64::from(i16::MIN) && truncated <= f64::from(i16::MAX))
        .then_some(truncated as i16)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn float_checked_i32(value: f64) -> Option<i32> {
    let truncated = value.trunc();
    (truncated.is_finite() && truncated >= f64::from(i32::MIN) && truncated <= f64::from(i32::MAX))
        .then_some(truncated as i32)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn float_checked_i64(value: f64) -> Option<i64> {
    let truncated = value.trunc();
    (truncated.is_finite() && truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64)
        .then_some(truncated as i64)
}

#[allow(clippy::cast_possible_truncation)]
fn float_dispatch<F>(
    source: &PrimitiveArray<F>,
    from_type: &DataType,
    target: &DataType,
    mode: Mode,
    format_value: fn(F::Native) -> String,
) -> Result<ArrayRef>
where
    F: ArrowPrimitiveType,
    F::Native: Into<f64>,
{
    match target {
        DataType::Int8 => float_array_to_signed::<F, Int8Type>(
            source,
            from_type,
            target,
            mode,
            format_value,
            |value| (value as i32) as i8,
            float_checked_i8,
        ),
        DataType::Int16 => float_array_to_signed::<F, Int16Type>(
            source,
            from_type,
            target,
            mode,
            format_value,
            |value| (value as i32) as i16,
            float_checked_i16,
        ),
        DataType::Int32 => float_array_to_signed::<F, Int32Type>(
            source,
            from_type,
            target,
            mode,
            format_value,
            |value| value as i32,
            float_checked_i32,
        ),
        DataType::Int64 => float_array_to_signed::<F, Int64Type>(
            source,
            from_type,
            target,
            mode,
            format_value,
            |value| value as i64,
            float_checked_i64,
        ),
        other => Err(DataFusionError::Internal(format!(
            "expected a signed integral target, got {other}"
        ))),
    }
}

fn fractional_to_integral(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let from = source.data_type();
    match from {
        DataType::Float32 => float_dispatch::<Float32Type>(
            source.as_primitive::<Float32Type>(),
            from,
            target,
            mode,
            float32_literal,
        ),
        DataType::Float64 => float_dispatch::<Float64Type>(
            source.as_primitive::<Float64Type>(),
            from,
            target,
            mode,
            float64_literal,
        ),
        _ => {
            let wide =
                cast_with_options(source.as_ref(), &DataType::Float64, &CastOptions::default())?;
            float_dispatch::<Float64Type>(
                wide.as_primitive::<Float64Type>(),
                from,
                target,
                mode,
                float64_literal,
            )
        }
    }
}

pub(super) fn narrow_numeric(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    if source.data_type() == &DataType::Null {
        return Ok(new_null_array(target, source.len()));
    }
    let decoded;
    let source = if let DataType::Dictionary(_, value) = source.data_type() {
        decoded = cast_with_options(source.as_ref(), value.as_ref(), &CastOptions::default())?;
        &decoded
    } else {
        source
    };
    let from = source.data_type();
    if from == target {
        return Ok(Arc::clone(source));
    }
    let signed_target = matches!(
        target,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    );
    if is_integral(from) && signed_target {
        return integral_to_integral(source, target, mode);
    }
    if is_decimal(from) && signed_target {
        return decimal_to_integral(source, target, mode);
    }
    if is_fractional(from) && signed_target {
        return fractional_to_integral(source, target, mode);
    }
    if is_decimal(from) && is_decimal(target) {
        return decimal_to_decimal(source, target, mode);
    }
    let options = CastOptions {
        safe: mode.nulls_on_failure(),
        ..CastOptions::default()
    };
    Ok(cast_with_options(source.as_ref(), target, &options)?)
}

pub(super) fn leaf_cast(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let from = source.data_type();
    if from == target {
        return Ok(Arc::clone(source));
    }
    let signed_target = matches!(
        target,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    );
    if is_string(from) && signed_target {
        return string_to_integral(source, target, mode);
    }
    if is_string(from) && target == &DataType::Boolean {
        return string_to_boolean(source, target, mode);
    }
    if is_string(from) && !is_string(target) && !is_binary(target) {
        return string_via_arrow(source, target, mode);
    }
    if is_integral(from) && signed_target {
        return integral_to_integral(source, target, mode);
    }
    if is_decimal(from) && signed_target {
        return decimal_to_integral(source, target, mode);
    }
    if is_fractional(from) && signed_target {
        return fractional_to_integral(source, target, mode);
    }
    if is_decimal(from) && is_decimal(target) {
        return decimal_to_decimal(source, target, mode);
    }
    let options = CastOptions {
        safe: mode.nulls_on_failure(),
        ..CastOptions::default()
    };
    Ok(cast_with_options(source.as_ref(), target, &options)?)
}
