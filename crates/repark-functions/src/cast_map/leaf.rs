use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int64Array, PrimitiveArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimeUnit,
};
use datafusion::common::{DataFusionError, Result};

use super::spark_sql_name;
use crate::java_double::parse_float::parse_java_float_text;
use crate::spark_string_timestamp::grammar::parse_timestamp_string;
use crate::spark_string_timestamp::instant::{
    date_days, instant_micros, timestamp_wall_micros, wall_micros_to_instant,
};
use crate::spark_string_timestamp::zone::{SparkZone, spark_zone_id};

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
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"{}\" cannot be cast to \"{}\" \
         because it is malformed. Correct the value as per the syntax, or change its target \
         type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: \
         22018",
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
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if whole.is_empty() || whole == "+" || whole == "-" {
        return (!fraction.is_empty()).then_some(0);
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

fn string_to_integral(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
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

fn numeric_out_of_range(value: &str, precision: u8, scale: i8) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {value} cannot be represented as \
         Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" to \"false\" \
         to bypass this error, and return NULL instead. SQLSTATE: 22003"
    ))
}

fn signed_nan(text: &str) -> bool {
    matches!(text.to_ascii_lowercase().as_str(), "+nan" | "-nan")
}

fn parse_fractional<Float: FromStr>(text: &str) -> Option<Float> {
    if signed_nan(text) {
        return None;
    }
    parse_java_float_text(text)
}

fn string_to_fractional(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    if target == &DataType::Float64 {
        let mut out = Vec::with_capacity(source.len());
        for text in strings(source)? {
            let Some(text) = text else {
                out.push(None);
                continue;
            };
            let parsed = parse_fractional::<f64>(spark_trim(text));
            if parsed.is_none() && mode == Mode::Ansi {
                return Err(cast_invalid_input(text, source.data_type(), target));
            }
            out.push(parsed);
        }
        return Ok(Arc::new(Float64Array::from(out)));
    }
    let mut out = Vec::with_capacity(source.len());
    for text in strings(source)? {
        let Some(text) = text else {
            out.push(None);
            continue;
        };
        let parsed = parse_fractional::<f32>(spark_trim(text));
        if parsed.is_none() && mode == Mode::Ansi {
            return Err(cast_invalid_input(text, source.data_type(), target));
        }
        out.push(parsed);
    }
    Ok(Arc::new(Float32Array::from(out)))
}

struct DecimalDigits {
    negative: bool,
    digits: Vec<u8>,
    exponent: i64,
}

fn parse_decimal_exponent(text: &str) -> Option<i64> {
    let (negative, digits) = match text.strip_prefix('+') {
        Some(tail) => (false, tail),
        None => match text.strip_prefix('-') {
            Some(tail) => (true, tail),
            None => (false, text),
        },
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let magnitude: i64 = if digits.len() > 6 {
        999_999
    } else {
        digits.parse().ok()?
    };
    Some(if negative { -magnitude } else { magnitude })
}

fn parse_decimal_digits(text: &str) -> Option<DecimalDigits> {
    let (negative, rest) = match text.strip_prefix('+') {
        Some(tail) => (false, tail),
        None => match text.strip_prefix('-') {
            Some(tail) => (true, tail),
            None => (false, text),
        },
    };
    let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
        Some((head, tail)) => (head, parse_decimal_exponent(tail)?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((head, tail)) => (head, tail),
        None => (mantissa, ""),
    };
    if !int_part.bytes().all(|byte| byte.is_ascii_digit())
        || !frac_part.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let joined: Vec<u8> = int_part.bytes().chain(frac_part.bytes()).collect();
    if joined.is_empty() {
        return None;
    }
    let stripped = joined
        .iter()
        .position(|byte| *byte != b'0')
        .unwrap_or(joined.len());
    if stripped == joined.len() {
        return Some(DecimalDigits {
            negative: false,
            digits: vec![b'0'],
            exponent: 0,
        });
    }
    let point = i64::try_from(int_part.len()).ok()? - i64::try_from(stripped).ok()?;
    Some(DecimalDigits {
        negative,
        digits: joined[stripped..].to_vec(),
        exponent: point.checked_add(exponent)?,
    })
}

fn round_up(digits: &mut Vec<u8>) {
    let mut index = digits.len();
    while index > 0 {
        index -= 1;
        if digits[index] < b'9' {
            digits[index] += 1;
            return;
        }
        digits[index] = b'0';
    }
    digits.insert(0, b'1');
}

fn decimal_unscaled(parsed: &DecimalDigits, scale: i32) -> Vec<u8> {
    let shift = parsed.exponent + i64::from(scale);
    if shift <= 0 {
        if shift == 0 && parsed.digits.first().is_some_and(|digit| *digit >= b'5') {
            return vec![b'1'];
        }
        return vec![b'0'];
    }
    let keep = usize::try_from(shift).unwrap_or(usize::MAX);
    let mut kept: Vec<u8> = parsed.digits.iter().take(keep).copied().collect();
    while kept.len() < keep {
        kept.push(b'0');
    }
    if parsed.digits.len() > keep && parsed.digits[keep] >= b'5' {
        round_up(&mut kept);
    }
    kept
}

fn render_plain(digits: &[u8], negative: bool, scale: i32) -> String {
    let mut text = String::new();
    if negative {
        text.push('-');
    }
    if scale <= 0 {
        text.push_str(std::str::from_utf8(digits).unwrap_or_default());
        for _ in 0..-scale {
            text.push('0');
        }
        return text;
    }
    let scale = usize::try_from(scale).unwrap_or(usize::MAX);
    if digits.len() > scale {
        let point = digits.len() - scale;
        text.push_str(std::str::from_utf8(&digits[..point]).unwrap_or_default());
        text.push('.');
        text.push_str(std::str::from_utf8(&digits[point..]).unwrap_or_default());
    } else {
        text.push_str("0.");
        for _ in 0..scale - digits.len() {
            text.push('0');
        }
        text.push_str(std::str::from_utf8(digits).unwrap_or_default());
    }
    text
}

fn string_to_decimal(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let DataType::Decimal128(precision, scale) = target else {
        return Err(DataFusionError::Internal(format!(
            "expected a decimal128 target, got {target}"
        )));
    };
    let mut out = Vec::with_capacity(source.len());
    for text in strings(source)? {
        let Some(text) = text else {
            out.push(None);
            continue;
        };
        let Some(parsed) = parse_decimal_digits(spark_trim(text)) else {
            if mode == Mode::Ansi {
                return Err(cast_invalid_input(text, source.data_type(), target));
            }
            out.push(None);
            continue;
        };
        let unscaled = decimal_unscaled(&parsed, i32::from(*scale));
        if unscaled.len() > usize::from(*precision) {
            if mode == Mode::Ansi {
                let rendered = render_plain(&unscaled, parsed.negative, i32::from(*scale));
                return Err(numeric_out_of_range(&rendered, *precision, *scale));
            }
            out.push(None);
            continue;
        }
        let mut value: i128 = 0;
        for digit in &unscaled {
            value = value * 10 + i128::from(*digit - b'0');
        }
        out.push(Some(if parsed.negative { -value } else { value }));
    }
    Ok(Arc::new(
        Decimal128Array::from(out).with_precision_and_scale(*precision, *scale)?,
    ))
}

fn string_to_date(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let mut out = Vec::with_capacity(source.len());
    for text in strings(source)? {
        let Some(text) = text else {
            out.push(None);
            continue;
        };
        let days = parse_timestamp_string(spark_trim(text)).and_then(|parsed| date_days(&parsed));
        if days.is_none() && mode == Mode::Ansi {
            return Err(cast_invalid_input(text, source.data_type(), target));
        }
        out.push(days);
    }
    Ok(Arc::new(Date32Array::from(out)))
}

fn micros_to_unit(micros: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => Some(micros / 1_000_000),
        TimeUnit::Millisecond => Some(micros / 1_000),
        TimeUnit::Microsecond => Some(micros),
        TimeUnit::Nanosecond => micros.checked_mul(1_000),
    }
}

fn string_to_timestamp(
    source: &ArrayRef,
    target: &DataType,
    zone: SparkZone,
    now: DateTime<Utc>,
    mode: Mode,
) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, zone_name) = target else {
        return Err(DataFusionError::Internal(format!(
            "expected a timestamp target, got {target}"
        )));
    };
    let mut out = Vec::with_capacity(source.len());
    for text in strings(source)? {
        let Some(text) = text else {
            out.push(None);
            continue;
        };
        let micros = parse_timestamp_string(spark_trim(text)).and_then(|parsed| {
            if zone_name.is_none() {
                return timestamp_wall_micros(&parsed, zone, now);
            }
            match parsed.zone {
                Some(name) => {
                    spark_zone_id(name).and_then(|named| instant_micros(&parsed, named, now))
                }
                None => instant_micros(&parsed, zone, now),
            }
        });
        let value = micros.and_then(|wall| micros_to_unit(wall, *unit));
        if value.is_none() && mode == Mode::Ansi {
            return Err(cast_invalid_input(text, source.data_type(), target));
        }
        out.push(value);
    }
    Ok(match unit {
        TimeUnit::Second => {
            let array = TimestampSecondArray::from(out);
            match zone_name {
                Some(name) => Arc::new(array.with_timezone(name.as_ref())),
                None => Arc::new(array),
            }
        }
        TimeUnit::Millisecond => {
            let array = TimestampMillisecondArray::from(out);
            match zone_name {
                Some(name) => Arc::new(array.with_timezone(name.as_ref())),
                None => Arc::new(array),
            }
        }
        TimeUnit::Microsecond => {
            let array = TimestampMicrosecondArray::from(out);
            match zone_name {
                Some(name) => Arc::new(array.with_timezone(name.as_ref())),
                None => Arc::new(array),
            }
        }
        TimeUnit::Nanosecond => {
            let array = TimestampNanosecondArray::from(out);
            match zone_name {
                Some(name) => Arc::new(array.with_timezone(name.as_ref())),
                None => Arc::new(array),
            }
        }
    })
}

fn primitive_mismatch(source: &ArrayRef) -> DataFusionError {
    DataFusionError::Internal(format!(
        "a {} array of {} rows arrived in a non-primitive layout",
        source.data_type(),
        source.len(),
    ))
}

fn temporal_wall_micros(source: &ArrayRef) -> Result<Vec<Option<i64>>> {
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    Ok(match source.data_type() {
        DataType::Date32 => source
            .as_primitive_opt::<Date32Type>()
            .ok_or_else(|| primitive_mismatch(source))?
            .iter()
            .map(|days| days.and_then(|day| i64::from(day).checked_mul(MICROS_PER_DAY)))
            .collect(),
        DataType::Date64 => source
            .as_primitive_opt::<Date64Type>()
            .ok_or_else(|| primitive_mismatch(source))?
            .iter()
            .map(|days| days.and_then(|day| day.checked_mul(1_000)))
            .collect(),
        DataType::Timestamp(unit, None) => {
            let values: Vec<Option<i64>> = match unit {
                TimeUnit::Second => source
                    .as_primitive_opt::<datafusion::arrow::datatypes::TimestampSecondType>()
                    .ok_or_else(|| primitive_mismatch(source))?
                    .iter()
                    .map(|value| value.and_then(|inner| inner.checked_mul(1_000_000)))
                    .collect(),
                TimeUnit::Millisecond => source
                    .as_primitive_opt::<datafusion::arrow::datatypes::TimestampMillisecondType>()
                    .ok_or_else(|| primitive_mismatch(source))?
                    .iter()
                    .map(|value| value.and_then(|inner| inner.checked_mul(1_000)))
                    .collect(),
                TimeUnit::Microsecond => source
                    .as_primitive_opt::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
                    .ok_or_else(|| primitive_mismatch(source))?
                    .iter()
                    .collect(),
                TimeUnit::Nanosecond => source
                    .as_primitive_opt::<datafusion::arrow::datatypes::TimestampNanosecondType>()
                    .ok_or_else(|| primitive_mismatch(source))?
                    .iter()
                    .map(|value| value.map(|inner| inner / 1_000))
                    .collect(),
            };
            values
        }
        other => {
            return Err(DataFusionError::Internal(format!(
                "expected a wall-clock source, got {other}"
            )));
        }
    })
}

fn wall_to_ltz(
    source: &ArrayRef,
    target: &DataType,
    zone: SparkZone,
    mode: Mode,
) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, zone_name) = target else {
        return Err(DataFusionError::Internal(format!(
            "expected a zoned timestamp target, got {target}"
        )));
    };
    let Some(zone_name) = zone_name else {
        return Err(DataFusionError::Internal(format!(
            "expected a zoned timestamp target, got {target}"
        )));
    };
    let mut out = Vec::with_capacity(source.len());
    for wall in temporal_wall_micros(source)? {
        let instant = wall.and_then(|wall| wall_micros_to_instant(wall, zone));
        let value = instant.and_then(|wall| micros_to_unit(wall, *unit));
        if value.is_none() && wall.is_some() {
            if mode == Mode::Ansi {
                return Err(DataFusionError::Internal(format!(
                    "a wall clock outside the supported range cannot shift to {target}"
                )));
            }
            out.push(None);
            continue;
        }
        out.push(value);
    }
    Ok(match unit {
        TimeUnit::Second => {
            Arc::new(TimestampSecondArray::from(out).with_timezone(zone_name.as_ref()))
        }
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(out).with_timezone(zone_name.as_ref()))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(out).with_timezone(zone_name.as_ref()))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(out).with_timezone(zone_name.as_ref()))
        }
    })
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

fn integral_to_integral(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
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

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn fractional_to_integral(source: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    let wide = cast_with_options(source.as_ref(), &DataType::Float64, &CastOptions::default())?;
    let (low, high) = integral_range(target);
    let suffix = if source.data_type() == &DataType::Float64 {
        "D"
    } else {
        "F"
    };
    let mut out = Vec::with_capacity(wide.len());
    for value in wide.as_primitive::<Float64Type>() {
        out.push(match value {
            None => None,
            Some(inner) => {
                let truncated = inner.trunc();
                let in_range = truncated.is_finite()
                    && truncated >= low as f64
                    && truncated <= high as f64
                    && !(high == i64::MAX && truncated >= 9_223_372_036_854_775_807.0);
                if in_range {
                    Some(truncated as i64)
                } else {
                    match mode {
                        Mode::Ansi => {
                            return Err(cast_overflow(
                                &format!("{inner:?}{suffix}"),
                                source.data_type(),
                                target,
                            ));
                        }
                        Mode::Legacy if integral_bits(target) >= 32 => {
                            Some(if integral_bits(target) == 32 {
                                i64::from(inner as i32)
                            } else {
                                inner as i64
                            })
                        }
                        Mode::Legacy => Some(wrap_integral(i64::from(inner as i32), target)),
                        Mode::Try => None,
                    }
                }
            }
        });
    }
    build_integral(out, target)
}

pub(super) fn leaf_cast(
    source: &ArrayRef,
    target: &DataType,
    mode: Mode,
    zone: SparkZone,
    now: DateTime<Utc>,
) -> Result<ArrayRef> {
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
    if is_string(from) && matches!(target, DataType::Float32 | DataType::Float64) {
        return string_to_fractional(source, target, mode);
    }
    if is_string(from) && matches!(target, DataType::Decimal128(_, _)) {
        return string_to_decimal(source, target, mode);
    }
    if is_string(from) && target == &DataType::Date32 {
        return string_to_date(source, target, mode);
    }
    if is_string(from) && matches!(target, DataType::Timestamp(_, _)) {
        return string_to_timestamp(source, target, zone, now, mode);
    }
    if is_string(from) && !is_string(target) && !is_binary(target) {
        return string_via_arrow(source, target, mode);
    }
    if is_integral(from) && signed_target {
        return integral_to_integral(source, target, mode);
    }
    if is_fractional(from) && signed_target {
        return fractional_to_integral(source, target, mode);
    }
    if matches!(
        from,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
    ) && matches!(target, DataType::Timestamp(_, Some(_)))
    {
        return wall_to_ltz(source, target, zone, mode);
    }
    let options = CastOptions {
        safe: mode.nulls_on_failure(),
        ..CastOptions::default()
    };
    Ok(cast_with_options(source.as_ref(), target, &options)?)
}
