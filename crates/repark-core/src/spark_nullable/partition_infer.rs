use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use datafusion::common::ScalarValue;

use crate::partition_discovery::{parse_decimal_scaled, parse_timestamp_micros_zone};

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn unescape_spark_path_name(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_nibble(bytes[index + 1]), hex_nibble(bytes[index + 2]))
        {
            out.push(char::from((high << 4) | low));
            index += 3;
            continue;
        }
        if let Some(ch) = raw[index..].chars().next() {
            out.push(ch);
            index += ch.len_utf8();
        } else {
            break;
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InferredPartType {
    Null,
    Int32,
    Int64,
    Decimal(u8),
    Float64,
    Date32,
    Timestamp,
    Utf8,
}

pub(super) fn parse_java_integral_decimal(text: &str) -> Option<(i128, u8)> {
    if text.is_empty() || text.bytes().any(|byte| byte <= b' ') {
        return None;
    }
    let (neg, rest) = if let Some(tail) = text.strip_prefix('-') {
        (true, tail)
    } else if let Some(tail) = text.strip_prefix('+') {
        (false, tail)
    } else {
        (false, text)
    };
    let (mantissa, exp) = if let Some(pos) = rest.find(['e', 'E']) {
        let exp_str = &rest[pos + 1..];
        if exp_str.is_empty()
            || !exp_str
                .strip_prefix(['+', '-'])
                .unwrap_or(exp_str)
                .bytes()
                .all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        let exp_val = exp_str.parse::<i64>().ok()?;
        (&rest[..pos], exp_val)
    } else {
        (rest, 0_i64)
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((left, right)) => (left, right),
        None => (mantissa, ""),
    };
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|byte| byte.is_ascii_digit())
        || !frac_part.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let frac_len = i64::try_from(frac_part.len()).ok()?;
    let raw_scale = frac_len.checked_sub(exp)?;
    if raw_scale > 0 {
        return None;
    }
    let zeros = usize::try_from(raw_scale.checked_neg()?).ok()?;
    let mut digits = String::with_capacity(int_part.len() + frac_part.len() + zeros.min(40));
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        let precision = 1_usize.checked_add(zeros)?;
        if precision > 38 {
            return None;
        }
        return u8::try_from(precision).ok().map(|prec| (0, prec));
    }
    let precision = trimmed.len().checked_add(zeros)?;
    if precision > 38 {
        return None;
    }
    let mut scaled = String::with_capacity(precision + usize::from(neg));
    if neg {
        scaled.push('-');
    }
    scaled.push_str(trimmed);
    for _ in 0..zeros {
        scaled.push('0');
    }
    let value = scaled.parse::<i128>().ok()?;
    let prec_u8 = u8::try_from(precision).ok()?;
    Some((value, prec_u8))
}

fn parse_saturating_exp(exp_str: &str) -> Option<i64> {
    let (neg, digits) = if let Some(tail) = exp_str.strip_prefix('-') {
        (true, tail)
    } else if let Some(tail) = exp_str.strip_prefix('+') {
        (false, tail)
    } else {
        (false, exp_str)
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut acc: i64 = 0;
    for byte in digits.bytes() {
        let digit = i64::from(byte - b'0');
        acc = acc
            .saturating_mul(10)
            .saturating_add(digit)
            .min(1_000_000_000);
    }
    Some(if neg { -acc } else { acc })
}

fn collect_hex_significand(
    int_hex: &str,
    frac_hex: &str,
    first_bit_index: usize,
    keep_bits: usize,
) -> Option<u64> {
    let mut mantissa: u64 = 0;
    let mut seen: usize = 0;
    let mut guard = false;
    let mut sticky = false;
    let mut pos: usize = 0;
    for byte in int_hex.bytes().chain(frac_hex.bytes()) {
        let nibble = hex_nibble(byte)?;
        for shift in (0..4).rev() {
            if pos >= first_bit_index {
                let bit = (nibble >> shift) & 1;
                if seen < keep_bits {
                    mantissa = (mantissa << 1) | u64::from(bit);
                } else if seen == keep_bits {
                    guard = bit != 0;
                } else if bit != 0 {
                    sticky = true;
                }
                seen += 1;
            }
            pos += 1;
        }
    }
    if seen < keep_bits {
        mantissa <<= keep_bits - seen;
    }
    if guard && (sticky || (mantissa & 1) == 1) {
        mantissa += 1;
    }
    Some(mantissa)
}

fn parse_java_hex_float(body: &str) -> Option<f64> {
    let (neg, rest) = if let Some(tail) = body.strip_prefix('-') {
        (true, tail)
    } else if let Some(tail) = body.strip_prefix('+') {
        (false, tail)
    } else {
        (false, body)
    };
    let after_prefix = rest
        .strip_prefix("0x")
        .or_else(|| rest.strip_prefix("0X"))?;
    let sep = after_prefix.find(['p', 'P'])?;
    let sig = &after_prefix[..sep];
    let exp = parse_saturating_exp(&after_prefix[sep + 1..])?;
    let (int_hex, frac_hex) = match sig.split_once('.') {
        Some((left, right)) => (left, right),
        None => (sig, ""),
    };
    if (int_hex.is_empty() && frac_hex.is_empty())
        || !int_hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !frac_hex.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    let int_len = i64::try_from(int_hex.len()).ok()?;
    let mut first_exp: Option<i64> = None;
    let mut first_bit_index: usize = 0;
    let mut bit_cursor: usize = 0;
    for (idx, byte) in int_hex.bytes().chain(frac_hex.bytes()).enumerate() {
        let nibble = hex_nibble(byte)?;
        for shift in (0..4).rev() {
            if ((nibble >> shift) & 1) != 0 && first_exp.is_none() {
                let idx_i64 = i64::try_from(idx).ok()?;
                let weight = int_len
                    .saturating_sub(1)
                    .saturating_sub(idx_i64)
                    .saturating_mul(4)
                    .saturating_add(i64::from(shift));
                first_exp = Some(exp.saturating_add(weight));
                first_bit_index = bit_cursor;
            }
            bit_cursor += 1;
        }
    }
    let sign_bits: u64 = if neg { 1_u64 << 63 } else { 0 };
    let Some(mut unbiased) = first_exp else {
        return Some(f64::from_bits(sign_bits));
    };
    if unbiased > 1023 {
        return Some(if neg {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    if unbiased < -1075 {
        return Some(f64::from_bits(sign_bits));
    }
    let is_normal = unbiased >= -1022;
    let keep_bits: usize = if is_normal {
        53
    } else {
        let shift = usize::try_from(-1022 - unbiased).ok()?;
        53_usize.saturating_sub(shift)
    };
    let mut mantissa = collect_hex_significand(int_hex, frac_hex, first_bit_index, keep_bits)?;
    if is_normal {
        if mantissa == (1_u64 << 53) {
            mantissa >>= 1;
            unbiased += 1;
        }
        if unbiased > 1023 {
            return Some(if neg {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            });
        }
        let biased = u64::try_from(unbiased + 1023).ok()?;
        let frac = mantissa & ((1_u64 << 52) - 1);
        Some(f64::from_bits(sign_bits | (biased << 52) | frac))
    } else {
        Some(f64::from_bits(sign_bits | mantissa))
    }
}

pub(super) fn parse_java_double(text: &str) -> Option<f64> {
    let trimmed = text.trim_matches(|ch: char| (ch as u32) <= 0x20);
    if trimmed.is_empty() {
        return None;
    }
    match trimmed {
        "NaN" | "+NaN" | "-NaN" => return Some(f64::NAN),
        "Infinity" | "+Infinity" => return Some(f64::INFINITY),
        "-Infinity" => return Some(f64::NEG_INFINITY),
        _ => {}
    }
    let lower_unsigned = trimmed
        .strip_prefix(['+', '-'])
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    if matches!(lower_unsigned.as_str(), "nan" | "inf" | "infinity") {
        return None;
    }
    let body = trimmed
        .strip_suffix(['d', 'D', 'f', 'F'])
        .unwrap_or(trimmed);
    let unsigned_body = body.strip_prefix(['+', '-']).unwrap_or(body);
    if unsigned_body.starts_with("0x") || unsigned_body.starts_with("0X") {
        return parse_java_hex_float(body);
    }
    if body.is_empty()
        || !body.bytes().any(|byte| byte.is_ascii_digit())
        || body.bytes().any(|byte| byte == b'_' || byte <= b' ')
    {
        return None;
    }
    body.parse::<f64>().ok()
}

fn looks_like_spark_partition_date(text: &str) -> bool {
    text.len() == 10 && NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()
}

fn looks_like_spark_partition_timestamp(text: &str) -> bool {
    if text.len() == 19 {
        return NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").is_ok();
    }
    if text.len() == 21 {
        let bytes = text.as_bytes();
        return bytes[19] == b'.'
            && bytes[20].is_ascii_digit()
            && NaiveDateTime::parse_from_str(&text[..19], "%Y-%m-%d %H:%M:%S").is_ok();
    }
    false
}

fn infer_single_part_value(text: &str) -> InferredPartType {
    if text.parse::<i32>().is_ok() {
        InferredPartType::Int32
    } else if text.parse::<i64>().is_ok() {
        InferredPartType::Int64
    } else if let Some((_, precision)) = parse_java_integral_decimal(text) {
        InferredPartType::Decimal(precision)
    } else if parse_java_double(text).is_some() {
        InferredPartType::Float64
    } else if looks_like_spark_partition_date(text) {
        InferredPartType::Date32
    } else if looks_like_spark_partition_timestamp(text) {
        InferredPartType::Timestamp
    } else {
        InferredPartType::Utf8
    }
}

fn unify_part_type(left: InferredPartType, right: InferredPartType) -> InferredPartType {
    match (left, right) {
        (InferredPartType::Null, other) | (other, InferredPartType::Null) => other,
        (a, b) if a == b => a,
        (InferredPartType::Int32, InferredPartType::Int64)
        | (InferredPartType::Int64, InferredPartType::Int32) => InferredPartType::Int64,
        (InferredPartType::Int32 | InferredPartType::Int64, InferredPartType::Float64)
        | (InferredPartType::Float64, InferredPartType::Int32 | InferredPartType::Int64) => {
            InferredPartType::Float64
        }
        (InferredPartType::Decimal(p1), InferredPartType::Decimal(p2)) => {
            InferredPartType::Decimal(p1.max(p2))
        }
        (InferredPartType::Int32, InferredPartType::Decimal(precision))
        | (InferredPartType::Decimal(precision), InferredPartType::Int32) => {
            InferredPartType::Decimal(precision.max(10))
        }
        (InferredPartType::Int64, InferredPartType::Decimal(precision))
        | (InferredPartType::Decimal(precision), InferredPartType::Int64) => {
            InferredPartType::Decimal(precision.max(20))
        }
        (InferredPartType::Date32, InferredPartType::Timestamp)
        | (InferredPartType::Timestamp, InferredPartType::Date32) => InferredPartType::Timestamp,
        _ => InferredPartType::Utf8,
    }
}

pub(super) fn infer_column_data_type(
    values: &[Option<&str>],
    session_zone: &str,
    type_inference: bool,
) -> DataType {
    let mut has_non_null = false;
    let mut acc = InferredPartType::Null;
    for candidate in values {
        let Some(text) = candidate else {
            continue;
        };
        has_non_null = true;
        if !type_inference {
            return DataType::Utf8;
        }
        acc = unify_part_type(acc, infer_single_part_value(text));
        if acc == InferredPartType::Utf8 {
            return DataType::Utf8;
        }
    }
    if !has_non_null {
        return DataType::Null;
    }
    match acc {
        InferredPartType::Null => DataType::Null,
        InferredPartType::Int32 => DataType::Int32,
        InferredPartType::Int64 => DataType::Int64,
        InferredPartType::Decimal(precision) => DataType::Decimal128(precision, 0),
        InferredPartType::Float64 => DataType::Float64,
        InferredPartType::Date32 => DataType::Date32,
        InferredPartType::Timestamp => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_zone.into()))
        }
        InferredPartType::Utf8 => DataType::Utf8,
    }
}

fn parse_date32(text: &str) -> Option<i32> {
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from(date.signed_duration_since(epoch).num_days()).ok()
}

fn typed_null_scalar(data_type: &DataType) -> ScalarValue {
    ScalarValue::try_from(data_type).unwrap_or(ScalarValue::Null)
}

pub(super) fn scalar_from_partition_text(
    raw: Option<&str>,
    data_type: &DataType,
    zone: Tz,
) -> ScalarValue {
    let Some(text) = raw else {
        return typed_null_scalar(data_type);
    };
    match data_type {
        DataType::Null => ScalarValue::Null,
        DataType::Int32 => ScalarValue::Int32(text.parse::<i32>().ok()),
        DataType::Int64 => ScalarValue::Int64(text.parse::<i64>().ok()),
        DataType::Decimal128(precision, scale) => {
            let value = if *scale == 0 {
                parse_java_integral_decimal(text)
                    .map(|(val, _)| val)
                    .or_else(|| parse_decimal_scaled(text, *precision, *scale))
            } else {
                parse_decimal_scaled(text, *precision, *scale)
            };
            ScalarValue::Decimal128(value, *precision, *scale)
        }
        DataType::Float64 => ScalarValue::Float64(parse_java_double(text)),
        DataType::Date32 => ScalarValue::Date32(parse_date32(text)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let micros = parse_timestamp_micros_zone(text, zone).or_else(|| {
                let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
                parse_timestamp_micros_zone(&format!("{date} 00:00:00"), zone)
            });
            ScalarValue::TimestampMicrosecond(micros, tz.clone())
        }
        _ => ScalarValue::Utf8(Some(text.to_string())),
    }
}
