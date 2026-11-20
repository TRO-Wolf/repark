use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, BooleanBufferBuilder,
    PrimitiveArray, StringArrayType, new_null_array,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, lit,
};
use repark_common::zone_horizon::{days_from_civil, is_leap_year};

use crate::session_time_zone::session_time_zone_from_options;
use crate::spark_string_timestamp::grammar::parse_timestamp_string;
use crate::spark_string_timestamp::instant::instant_micros;
use crate::spark_string_timestamp::string_to_timestamp_micros;
use crate::spark_string_timestamp::zone::SparkZone;
use crate::timestamp_cast::parse_session_zone;

pub const CSV_FIELD_UDF: &str = "__repark_csv_field__";

const MAX_DECIMAL_PRECISION: u8 = 38;
const MIN_DATE_YEAR_DIGITS: usize = 4;
const MAX_DATE_YEAR_DIGITS: usize = 7;
const MAX_DECIMAL_EXPONENT_DIGITS: usize = 10;
const KEPT_HEX_BITS: u32 = 116;
const HEX_EXPONENT_CAP: i64 = 1 << 40;

#[must_use]
pub fn java_long(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    let (negative, digits) = match bytes.split_first()? {
        (b'-', rest) => (true, rest),
        (b'+', rest) => (false, rest),
        _ => (false, bytes),
    };
    if digits.is_empty() {
        return None;
    }
    let mut magnitude: u64 = 0;
    for byte in digits {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        magnitude = magnitude.checked_mul(10)?.checked_add(u64::from(digit))?;
    }
    if negative {
        0_i64.checked_sub_unsigned(magnitude)
    } else {
        i64::try_from(magnitude).ok()
    }
}

#[must_use]
pub fn java_integral<T: TryFrom<i64>>(text: &str) -> Option<T> {
    T::try_from(java_long(text)?).ok()
}

fn java_trim(text: &str) -> &[u8] {
    let bytes = text.as_bytes();
    let start = bytes
        .iter()
        .position(|byte| *byte > b' ')
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| *byte > b' ')
        .map_or(start, |last| last + 1);
    &bytes[start..end]
}

enum JavaNumber<'text> {
    NotANumber,
    Infinite(bool),
    Decimal(&'text str),
    Hex {
        negative: bool,
        mantissa: &'text [u8],
        exponent: i64,
    },
}

fn is_float_suffix(byte: u8) -> bool {
    matches!(byte, b'f' | b'F' | b'd' | b'D')
}

fn java_number(text: &str) -> Option<JavaNumber<'_>> {
    let bytes = java_trim(text);
    let (negative, rest) = match bytes.split_first()? {
        (b'-', rest) => (true, rest),
        (b'+', rest) => (false, rest),
        _ => (false, bytes),
    };
    match rest.first()? {
        b'N' => return (rest == b"NaN").then_some(JavaNumber::NotANumber),
        b'I' => return (rest == b"Infinity").then_some(JavaNumber::Infinite(negative)),
        b'0' if matches!(rest.get(1), Some(b'x' | b'X')) => {
            return java_hex(negative, rest.get(2..)?);
        }
        _ => {}
    }
    let mut index = 0;
    let mut digits = false;
    let mut point = false;
    while let Some(&byte) = rest.get(index) {
        if byte.is_ascii_digit() {
            digits = true;
        } else if byte == b'.' && !point {
            point = true;
        } else {
            break;
        }
        index += 1;
    }
    if !digits {
        return None;
    }
    if matches!(rest.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(rest.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let start = index;
        while rest.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == start {
            return None;
        }
    }
    match rest.get(index..)? {
        [] => {}
        [suffix] if is_float_suffix(*suffix) => {}
        _ => return None,
    }
    let signed = bytes.len() - rest.len();
    std::str::from_utf8(bytes.get(..signed + index)?)
        .ok()
        .map(JavaNumber::Decimal)
}

fn java_hex(negative: bool, rest: &[u8]) -> Option<JavaNumber<'_>> {
    let marker = rest.iter().position(|byte| matches!(byte, b'p' | b'P'))?;
    let (mantissa, tail) = (rest.get(..marker)?, rest.get(marker + 1..)?);
    let mut point = false;
    let mut digits = false;
    for byte in mantissa {
        if byte.is_ascii_hexdigit() {
            digits = true;
        } else if *byte == b'.' && !point {
            point = true;
        } else {
            return None;
        }
    }
    if !digits {
        return None;
    }
    let tail = match tail {
        [body @ .., suffix] if is_float_suffix(*suffix) => body,
        _ => tail,
    };
    let (exponent_negative, exponent_digits) = match tail.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, tail),
    };
    if exponent_digits.is_empty() || !exponent_digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let magnitude = exponent_digits
        .iter()
        .fold(0_i64, |total, byte| {
            total
                .saturating_mul(10)
                .saturating_add(i64::from(byte - b'0'))
        })
        .min(HEX_EXPONENT_CAP);
    Some(JavaNumber::Hex {
        negative,
        mantissa,
        exponent: if exponent_negative {
            -magnitude
        } else {
            magnitude
        },
    })
}

struct BinaryFormat {
    precision: u32,
    max_exponent: i64,
    exponent_bits: u32,
}

const DOUBLE_FORMAT: BinaryFormat = BinaryFormat {
    precision: 53,
    max_exponent: 1023,
    exponent_bits: 11,
};

const FLOAT_FORMAT: BinaryFormat = BinaryFormat {
    precision: 24,
    max_exponent: 127,
    exponent_bits: 8,
};

fn hex_digit(byte: u8) -> u128 {
    u128::from(match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => byte.wrapping_sub(b'A').wrapping_add(10),
    })
}

fn rounded_significand(value: u128, sticky: bool, shift: i64, length: i64) -> Option<u64> {
    if shift <= 0 {
        return u64::try_from(value << u32::try_from(-shift).ok()?).ok();
    }
    if shift > length {
        return Some(0);
    }
    let shift = u32::try_from(shift).ok()?;
    let quotient = value >> shift;
    let remainder = value & ((1_u128 << shift) - 1);
    let half = 1_u128 << (shift - 1);
    let up = remainder > half || (remainder == half && (sticky || quotient & 1 == 1));
    u64::try_from(quotient + u128::from(up)).ok()
}

fn hex_bits(negative: bool, mantissa: &[u8], exponent: i64, format: &BinaryFormat) -> Option<u64> {
    let mut value: u128 = 0;
    let mut sticky = false;
    let mut scale = exponent;
    let mut after_point = false;
    for &byte in mantissa {
        if byte == b'.' {
            after_point = true;
            continue;
        }
        if after_point {
            scale -= 4;
        }
        if value >> KEPT_HEX_BITS == 0 {
            value = (value << 4) | hex_digit(byte);
        } else {
            sticky |= hex_digit(byte) != 0;
            scale += 4;
        }
    }
    let fraction_bits = format.precision - 1;
    let sign = u64::from(negative) << (fraction_bits + format.exponent_bits);
    if value == 0 {
        return Some(sign);
    }
    let infinity = sign | (((1_u64 << format.exponent_bits) - 1) << fraction_bits);
    let length = i64::from(128 - value.leading_zeros());
    let mut top = length - 1 + scale;
    if top > format.max_exponent {
        return Some(infinity);
    }
    let min_exponent = 1 - format.max_exponent;
    let precision = i64::from(format.precision);
    let keep = if top >= min_exponent {
        precision
    } else {
        precision - (min_exponent - top)
    };
    let mut kept = rounded_significand(value, sticky, length - keep, length)?;
    if top < min_exponent {
        return Some(sign | kept);
    }
    if kept >> format.precision != 0 {
        kept >>= 1;
        top += 1;
        if top > format.max_exponent {
            return Some(infinity);
        }
    }
    let biased = u64::try_from(top + format.max_exponent).ok()?;
    Some(sign | (biased << fraction_bits) | (kept & ((1_u64 << fraction_bits) - 1)))
}

#[must_use]
pub fn java_double(text: &str) -> Option<f64> {
    Some(match java_number(text)? {
        JavaNumber::NotANumber => f64::NAN,
        JavaNumber::Infinite(true) => f64::NEG_INFINITY,
        JavaNumber::Infinite(false) => f64::INFINITY,
        JavaNumber::Decimal(body) => body.parse().ok()?,
        JavaNumber::Hex {
            negative,
            mantissa,
            exponent,
        } => f64::from_bits(hex_bits(negative, mantissa, exponent, &DOUBLE_FORMAT)?),
    })
}

#[must_use]
pub fn java_float(text: &str) -> Option<f32> {
    Some(match java_number(text)? {
        JavaNumber::NotANumber => f32::NAN,
        JavaNumber::Infinite(true) => f32::NEG_INFINITY,
        JavaNumber::Infinite(false) => f32::INFINITY,
        JavaNumber::Decimal(body) => body.parse().ok()?,
        JavaNumber::Hex {
            negative,
            mantissa,
            exponent,
        } => f32::from_bits(
            u32::try_from(hex_bits(negative, mantissa, exponent, &FLOAT_FORMAT)?).ok()?,
        ),
    })
}

#[must_use]
pub fn csv_double(text: &str) -> Option<f64> {
    match text {
        "NaN" => Some(f64::NAN),
        "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        _ => java_double(text),
    }
}

#[must_use]
pub fn csv_float(text: &str) -> Option<f32> {
    match text {
        "NaN" => Some(f32::NAN),
        "Inf" => Some(f32::INFINITY),
        "-Inf" => Some(f32::NEG_INFINITY),
        _ => java_float(text),
    }
}

fn java_equals_ignore_case(text: &str, word: &str) -> bool {
    let mut chars = text.chars();
    for expected in word.bytes() {
        match chars.next() {
            Some(found)
                if found.is_ascii() && found.to_ascii_lowercase() == char::from(expected) => {}
            Some('\u{17f}') if expected == b's' => {}
            _ => return false,
        }
    }
    chars.next().is_none()
}

#[must_use]
pub fn csv_boolean(text: &str) -> Option<bool> {
    if java_equals_ignore_case(text, "true") {
        Some(true)
    } else if java_equals_ignore_case(text, "false") {
        Some(false)
    } else {
        None
    }
}

fn big_decimal_exponent(text: &[u8]) -> Option<i64> {
    let (negative, mut digits) = match text.split_first()? {
        (b'-', rest) => (true, rest),
        (b'+', rest) => (false, rest),
        _ => (false, text),
    };
    while digits.len() > MAX_DECIMAL_EXPONENT_DIGITS && digits.first() == Some(&b'0') {
        digits = digits.get(1..)?;
    }
    if digits.is_empty()
        || digits.len() > MAX_DECIMAL_EXPONENT_DIGITS
        || !digits.iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let magnitude = digits
        .iter()
        .fold(0_i64, |total, byte| total * 10 + i64::from(byte - b'0'));
    let value = if negative { -magnitude } else { magnitude };
    i32::try_from(value).ok().map(i64::from)
}

fn significant_prefix(mantissa: &[u8], count: usize) -> (i128, u8) {
    let mut units: i128 = 0;
    let mut taken = 0;
    for &byte in mantissa {
        if byte == b'.' || (taken == 0 && units == 0 && byte == b'0') {
            continue;
        }
        let digit = byte - b'0';
        if taken == count {
            return (units, digit);
        }
        units = units * 10 + i128::from(digit);
        taken += 1;
    }
    (units, 0)
}

fn big_decimal_units(bytes: &[u8], precision: u8, scale: i8) -> Option<i128> {
    let (negative, body) = match bytes.split_first()? {
        (b'-', rest) => (true, rest),
        (b'+', rest) => (false, rest),
        _ => (false, bytes),
    };
    let marker = body
        .iter()
        .position(|byte| matches!(byte, b'e' | b'E'))
        .unwrap_or(body.len());
    let (mantissa, exponent_text) = body.split_at(marker);
    let exponent = match exponent_text.split_first() {
        None => 0,
        Some((_, digits)) => big_decimal_exponent(digits)?,
    };
    let mut point = false;
    let mut digits = false;
    let mut fraction_digits: i64 = 0;
    let mut significant: usize = 0;
    for &byte in mantissa {
        match byte {
            b'.' if !point => point = true,
            b'0'..=b'9' => {
                digits = true;
                fraction_digits += i64::from(point);
                if byte != b'0' || significant > 0 {
                    significant += 1;
                }
            }
            _ => return None,
        }
    }
    if !digits {
        return None;
    }
    let java_scale = fraction_digits - exponent;
    i32::try_from(java_scale).ok()?;
    if significant == 0 {
        return Some(0);
    }
    let limit = 10_i128.checked_pow(u32::from(precision))?;
    let width = i64::from(precision);
    let significant_width = i64::try_from(significant).ok()?;
    let shift = i64::from(scale) - java_scale;
    let units = if shift >= 0 {
        if significant_width + shift > width {
            return None;
        }
        let (units, _) = significant_prefix(mantissa, significant);
        units * 10_i128.checked_pow(u32::try_from(shift).ok()?)?
    } else if -shift > significant_width {
        0
    } else {
        let kept = significant_width + shift;
        if kept > width {
            return None;
        }
        let (units, next) = significant_prefix(mantissa, usize::try_from(kept).ok()?);
        units + i128::from(next >= 5)
    };
    if units >= limit {
        return None;
    }
    Some(if negative { -units } else { units })
}

#[must_use]
pub fn csv_decimal(text: &str, precision: u8, scale: i8) -> Option<i128> {
    if text.as_bytes().contains(&b',') {
        let stripped: String = text.chars().filter(|found| *found != ',').collect();
        return big_decimal_units(stripped.as_bytes(), precision, scale);
    }
    big_decimal_units(text.as_bytes(), precision, scale)
}

fn is_spark_trimmable(byte: u8) -> bool {
    byte <= b' ' || byte == 0x7f
}

fn date_digits_fit(segment: usize, digits: usize) -> bool {
    if segment == 0 {
        (MIN_DATE_YEAR_DIGITS..=MAX_DATE_YEAR_DIGITS).contains(&digits)
    } else {
        (1..=2).contains(&digits)
    }
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn spark_string_to_date(bytes: &[u8]) -> Option<i32> {
    let start = bytes.iter().position(|byte| !is_spark_trimmable(*byte))?;
    let end = bytes.iter().rposition(|byte| !is_spark_trimmable(*byte))? + 1;
    let mut index = start;
    let negative = bytes.get(index) == Some(&b'-');
    if negative || bytes.get(index) == Some(&b'+') {
        index += 1;
    }
    let mut segments = [1_i64; 3];
    let mut segment = 0;
    let mut value = 0_i64;
    let mut digits = 0;
    while index < end {
        let byte = *bytes.get(index)?;
        if byte == b' ' || byte == b'T' {
            break;
        }
        if segment < 2 && byte == b'-' {
            if !date_digits_fit(segment, digits) {
                return None;
            }
            *segments.get_mut(segment)? = value;
            value = 0;
            digits = 0;
            segment += 1;
        } else if byte.is_ascii_digit() {
            value = value
                .saturating_mul(10)
                .saturating_add(i64::from(byte - b'0'));
            digits += 1;
        } else {
            return None;
        }
        index += 1;
    }
    if !date_digits_fit(segment, digits) || (segment < 2 && index < end) {
        return None;
    }
    *segments.get_mut(segment)? = value;
    let [year, month, day] = segments;
    let year = if negative { -year } else { year };
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    i32::try_from(days_from_civil(year, month, day)).ok()
}

fn without_gmt(text: &str) -> Option<String> {
    text.contains("GMT").then(|| text.replace("GMT", ""))
}

#[must_use]
pub fn csv_date(text: &str) -> Option<i32> {
    spark_string_to_date(text.as_bytes())
        .or_else(|| without_gmt(text).and_then(|clean| spark_string_to_date(clean.as_bytes())))
}

#[must_use]
pub fn csv_timestamp(text: &str, zone: Tz, now: DateTime<Utc>) -> Option<i64> {
    string_to_timestamp_micros(text, zone, now).or_else(|| {
        without_gmt(text).and_then(|clean| string_to_timestamp_micros(&clean, zone, now))
    })
}

#[must_use]
pub fn csv_timestamp_ntz(text: &str) -> Option<i64> {
    let parsed = parse_timestamp_string(text)?;
    if parsed.just_time || parsed.zone.is_some() {
        return None;
    }
    instant_micros(
        &parsed,
        SparkZone::Offset(FixedOffset::east_opt(0)?),
        DateTime::UNIX_EPOCH,
    )
}

fn primitive<'text, T, S>(
    texts: S,
    parse: impl Fn(&'text str) -> Option<T::Native>,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    S: StringArrayType<'text> + Copy,
{
    let length = texts.len();
    let mut values = Vec::with_capacity(length);
    let mut valid = BooleanBufferBuilder::new(length);
    for row in 0..length {
        let parsed = if texts.is_null(row) {
            None
        } else {
            parse(texts.value(row))
        };
        valid.append(parsed.is_some());
        values.push(parsed.unwrap_or_default());
    }
    PrimitiveArray::new(
        ScalarBuffer::from(values),
        Some(NullBuffer::new(valid.finish())),
    )
}

fn unsupported_target(data_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(unsupported_csv_field_text(&super::spark_type_name(
        data_type,
    )))
}

#[must_use]
pub fn unsupported_csv_field_text(name: &str) -> String {
    format!(
        "[UNSUPPORTED_DATATYPE] a CSV field of type {name} is not supported by repark yet: \
         repark has no CSV converter for it"
    )
}

fn typed_column<'text, S>(
    texts: S,
    raw: &ArrayRef,
    target: &DataType,
    zone: Tz,
    now: DateTime<Utc>,
) -> Result<ArrayRef>
where
    S: StringArrayType<'text> + Copy,
{
    Ok(match target {
        DataType::Utf8 => cast(raw, &DataType::Utf8)?,
        DataType::Binary => cast(&cast(raw, &DataType::Utf8)?, &DataType::Binary)?,
        DataType::Boolean => Arc::new(
            texts
                .iter()
                .map(|text| text.and_then(csv_boolean))
                .collect::<BooleanArray>(),
        ),
        DataType::Int8 => Arc::new(primitive::<Int8Type, _>(texts, java_integral::<i8>)),
        DataType::Int16 => Arc::new(primitive::<Int16Type, _>(texts, java_integral::<i16>)),
        DataType::Int32 => Arc::new(primitive::<Int32Type, _>(texts, java_integral::<i32>)),
        DataType::Int64 => Arc::new(primitive::<Int64Type, _>(texts, java_long)),
        DataType::Float32 => Arc::new(primitive::<Float32Type, _>(texts, csv_float)),
        DataType::Float64 => Arc::new(primitive::<Float64Type, _>(texts, csv_double)),
        DataType::Decimal128(precision, scale) if *precision <= MAX_DECIMAL_PRECISION => Arc::new(
            primitive::<Decimal128Type, _>(texts, |text| csv_decimal(text, *precision, *scale))
                .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Date32 => Arc::new(primitive::<Date32Type, _>(texts, csv_date)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Arc::new(primitive::<TimestampMicrosecondType, _>(
                texts,
                csv_timestamp_ntz,
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(field_zone)) => Arc::new(
            primitive::<TimestampMicrosecondType, _>(texts, |text| csv_timestamp(text, zone, now))
                .with_timezone(Arc::clone(field_zone)),
        ),
        other => return Err(unsupported_target(other)),
    })
}

#[allow(clippy::missing_errors_doc)]
pub fn csv_field_array(
    texts: &ArrayRef,
    target: &DataType,
    zone: Tz,
    now: DateTime<Utc>,
) -> Result<ArrayRef> {
    check_csv_field_target(target)?;
    match texts.data_type() {
        DataType::Null => Ok(new_null_array(target, texts.len())),
        DataType::Utf8 => typed_column(texts.as_string::<i32>(), texts, target, zone, now),
        DataType::LargeUtf8 => typed_column(texts.as_string::<i64>(), texts, target, zone, now),
        DataType::Utf8View => typed_column(texts.as_string_view(), texts, target, zone, now),
        other => exec_err!("'{CSV_FIELD_UDF}' reads STRING text, not {other}"),
    }
}

#[must_use]
pub fn misfit_rows(texts: &dyn Array, fields: &dyn Array) -> BooleanBuffer {
    let present = texts.logical_nulls().map_or_else(
        || BooleanBuffer::new_set(texts.len()),
        NullBuffer::into_inner,
    );
    let parsed = fields.logical_nulls().map_or_else(
        || BooleanBuffer::new_set(fields.len()),
        NullBuffer::into_inner,
    );
    &present & &!&parsed
}

#[allow(clippy::missing_errors_doc)]
pub fn check_csv_field_target(target: &DataType) -> Result<()> {
    match target {
        DataType::Utf8
        | DataType::Binary
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(()),
        DataType::Decimal128(precision, _) if (1..=MAX_DECIMAL_PRECISION).contains(precision) => {
            Ok(())
        }
        other => Err(unsupported_target(other)),
    }
}

fn check_text_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => Ok(()),
        other => plan_err!(
            "'{CSV_FIELD_UDF}' reads STRING text, but its input has the type {}",
            super::spark_type_name(other)
        ),
    }
}

#[derive(Debug)]
struct SparkCsvField {
    signature: Signature,
}

impl PartialEq for SparkCsvField {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkCsvField {}

impl Hash for SparkCsvField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn marker_target(marker: Option<&ScalarValue>) -> Result<DataType> {
    let Some(
        ScalarValue::Utf8(Some(name))
        | ScalarValue::LargeUtf8(Some(name))
        | ScalarValue::Utf8View(Some(name)),
    ) = marker
    else {
        return plan_err!(
            "'{CSV_FIELD_UDF}' names its field type with a string literal second argument"
        );
    };
    DataType::from_str(name).map_err(|_| {
        DataFusionError::Plan(format!(
            "'{CSV_FIELD_UDF}' got {name:?}, which names no type"
        ))
    })
}

fn wrong_arity(count: usize) -> DataFusionError {
    DataFusionError::Plan(format!(
        "'{CSV_FIELD_UDF}' takes the text and a string literal naming the field type, not \
         {count} arguments"
    ))
}

impl ScalarUDFImpl for SparkCsvField {
    fn name(&self) -> &str {
        CSV_FIELD_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [_, _] => Ok(arg_types.to_vec()),
            _ => Err(wrong_arity(arg_types.len())),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("'{CSV_FIELD_UDF}' reads its field type from the literal second argument")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [text, _] = args.arg_fields else {
            return Err(wrong_arity(args.arg_fields.len()));
        };
        check_text_type(text.data_type())?;
        let target = marker_target(args.scalar_arguments.get(1).copied().flatten())?;
        check_csv_field_target(&target)?;
        Ok(Arc::new(Field::new(self.name(), target, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [text, _] = args.args.as_slice() else {
            return Err(wrong_arity(args.args.len()));
        };
        let target = args.return_field.data_type();
        let zone = if matches!(target, DataType::Timestamp(_, Some(_))) {
            parse_session_zone(session_time_zone_from_options(args.config_options.as_ref()))?
        } else {
            Tz::from_str("UTC")?
        };
        let now = Utc::now();
        match text {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(csv_field_array(
                array, target, zone, now,
            )?)),
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(1)?;
                let fields = csv_field_array(&array, target, zone, now)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &fields, 0,
                )?))
            }
        }
    }
}

#[must_use]
pub fn csv_field_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkCsvField {
        signature: Signature::user_defined(Volatility::Stable),
    }))
}

#[allow(clippy::missing_errors_doc)]
pub fn csv_field_expr(text: Expr, target: &DataType) -> Result<Expr> {
    check_csv_field_target(target)?;
    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
        csv_field_udf(),
        vec![text, lit(target.to_string())],
    )))
}

#[cfg(test)]
mod tests;
