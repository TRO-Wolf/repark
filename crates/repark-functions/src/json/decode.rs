use std::sync::Arc;

use arrow::array::timezone::Tz;
use chrono::{MappedLocalTime, NaiveDate, NaiveDateTime, TimeZone};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, ListArray, MapArray,
    StringArray, StringBuilder, StructArray, TimestampMicrosecondBuilder, new_null_array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{Result, exec_err};

use super::reader::{JsonValue, write_compact};

pub(crate) struct DecodeContext {
    pub zone: Tz,
}

pub(crate) fn build_array(
    target: &DataType,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<ArrayRef> {
    match target {
        DataType::Struct(fields) => build_struct(fields, rows, context),
        DataType::List(field) => build_list(field, rows, context),
        DataType::Map(entries, _) => build_map(target, entries, rows, context),
        DataType::Null => Ok(new_null_array(&DataType::Null, rows.len())),
        primitive => build_primitive(primitive, rows, context),
    }
}

fn build_struct(
    fields: &Fields,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<ArrayRef> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for field in fields {
        let child: Vec<Option<&JsonValue<'_>>> = rows
            .iter()
            .map(|row| row.and_then(|value| object_field(value, field.name())))
            .collect();
        columns.push(build_array(field.data_type(), &child, context)?);
    }
    let present: Vec<bool> = rows.iter().map(Option::is_some).collect();
    let nulls = if present.iter().all(|found| *found) {
        None
    } else {
        Some(NullBuffer::from(present))
    };
    if fields.is_empty() {
        return Ok(Arc::new(StructArray::new_empty_fields(rows.len(), nulls)));
    }
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        nulls,
    )?))
}

fn object_field<'a>(value: &'a JsonValue<'a>, name: &str) -> Option<&'a JsonValue<'a>> {
    let JsonValue::Object(entries) = value else {
        return None;
    };
    entries
        .iter()
        .find(|(key, _)| key.as_ref() == name)
        .map(|(_, found)| found)
        .filter(|found| !matches!(found, JsonValue::Null))
}

fn build_list(
    field: &Arc<Field>,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<ArrayRef> {
    let mut flat: Vec<Option<&JsonValue<'_>>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    let mut present: Vec<bool> = Vec::with_capacity(rows.len());
    offsets.push(0);
    for row in rows {
        match row {
            Some(JsonValue::Array(items)) => {
                for item in items {
                    flat.push(if matches!(item, JsonValue::Null) {
                        None
                    } else {
                        Some(item)
                    });
                }
                present.push(true);
            }
            _ => present.push(false),
        }
        offsets.push(i32::try_from(flat.len()).unwrap_or(i32::MAX));
    }
    let values = build_array(field.data_type(), &flat, context)?;
    let nulls = if present.iter().all(|found| *found) {
        None
    } else {
        Some(NullBuffer::from(present))
    };
    Ok(Arc::new(ListArray::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    )?))
}

fn build_map(
    target: &DataType,
    entries: &Arc<Field>,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<ArrayRef> {
    let DataType::Struct(pair) = entries.data_type() else {
        return exec_err!("'from_json' expects a MAP entry struct, got {target}");
    };
    let (key_field, value_field) = (&pair[0], &pair[1]);
    if key_field.data_type() != &DataType::Utf8 {
        return exec_err!("'from_json' supports only STRING map keys, got {target}");
    }
    let mut keys: Vec<&str> = Vec::new();
    let mut flat: Vec<Option<&JsonValue<'_>>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    let mut present: Vec<bool> = Vec::with_capacity(rows.len());
    offsets.push(0);
    for row in rows {
        match row {
            Some(JsonValue::Object(pairs)) => {
                for (key, item) in pairs {
                    keys.push(key.as_ref());
                    flat.push(if matches!(item, JsonValue::Null) {
                        None
                    } else {
                        Some(item)
                    });
                }
                present.push(true);
            }
            _ => present.push(false),
        }
        offsets.push(i32::try_from(flat.len()).unwrap_or(i32::MAX));
    }
    let values = build_array(value_field.data_type(), &flat, context)?;
    let key_array: ArrayRef = Arc::new(StringArray::from(keys));
    let pair_array = StructArray::try_new(pair.clone(), vec![key_array, values], None)?;
    let nulls = if present.iter().all(|found| *found) {
        None
    } else {
        Some(NullBuffer::from(present))
    };
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(entries),
        OffsetBuffer::new(offsets.into()),
        pair_array,
        nulls,
        false,
    )?))
}

macro_rules! build_integer {
    ($builder:expr, $rows:expr, $kind:ty) => {{
        let mut builder = $builder;
        for row in $rows {
            match row
                .and_then(integer_of)
                .and_then(|found| <$kind>::try_from(found).ok())
            {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

fn build_primitive(
    target: &DataType,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<ArrayRef> {
    match target {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row {
                    Some(JsonValue::Bool(flag)) => builder.append_value(*flag),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => build_integer!(Int8Builder::with_capacity(rows.len()), rows, i8),
        DataType::Int16 => build_integer!(Int16Builder::with_capacity(rows.len()), rows, i16),
        DataType::Int32 => build_integer!(Int32Builder::with_capacity(rows.len()), rows, i32),
        DataType::Int64 => build_integer!(Int64Builder::with_capacity(rows.len()), rows, i64),
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.and_then(double_of) {
                    Some(value) => builder.append_value(truncate_to_f32(value)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.and_then(double_of) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => Ok(build_text(rows)),
        DataType::Binary => Ok(build_binary(rows)),
        DataType::Date32 => Ok(build_date(rows)),
        DataType::Timestamp(_, zone) => Ok(build_timestamp(rows, context, zone.clone())),
        DataType::Decimal128(precision, scale) => build_decimal(rows, *precision, *scale),
        other => exec_err!("'from_json' cannot decode into {other}"),
    }
}

fn integer_of(value: &JsonValue<'_>) -> Option<i64> {
    match value {
        JsonValue::Number(raw) if !raw.contains(['.', 'e', 'E']) => raw.parse::<i64>().ok(),
        _ => None,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn truncate_to_f32(value: f64) -> f32 {
    value as f32
}

fn double_of(value: &JsonValue<'_>) -> Option<f64> {
    match value {
        JsonValue::Number(raw) => raw.parse::<f64>().ok(),
        _ => None,
    }
}

fn build_text(rows: &[Option<&JsonValue<'_>>]) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    for row in rows {
        match row {
            Some(JsonValue::Text(text)) => builder.append_value(text.as_ref()),
            Some(JsonValue::Number(raw)) => builder.append_value(*raw),
            Some(JsonValue::Bool(flag)) => {
                builder.append_value(if *flag { "true" } else { "false" });
            }
            Some(other) => {
                let mut rendered = String::new();
                write_compact(other, &mut rendered);
                builder.append_value(&rendered);
            }
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_binary(rows: &[Option<&JsonValue<'_>>]) -> ArrayRef {
    let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 8);
    for row in rows {
        match row.and_then(|value| match value {
            JsonValue::Text(text) => base64_bytes(text.as_ref()),
            _ => None,
        }) {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn base64_bytes(text: &str) -> Option<Vec<u8>> {
    let mut accumulator: u32 = 0;
    let mut bits = 0;
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    for found in text.chars() {
        if found == '=' {
            break;
        }
        let sextet = match found {
            'A'..='Z' => u32::from(found as u8 - b'A'),
            'a'..='z' => u32::from(found as u8 - b'a') + 26,
            '0'..='9' => u32::from(found as u8 - b'0') + 52,
            '+' => 62,
            '/' => 63,
            '\n' | '\r' => continue,
            _ => return None,
        };
        accumulator = (accumulator << 6) | sextet;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            let byte = u8::try_from((accumulator >> bits) & 0xFF).ok()?;
            out.push(byte);
        }
    }
    Some(out)
}

fn build_date(rows: &[Option<&JsonValue<'_>>]) -> ArrayRef {
    let mut builder = Date32Builder::with_capacity(rows.len());
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1);
    for row in rows {
        let parsed = row.and_then(|value| match value {
            JsonValue::Text(text) => NaiveDate::parse_from_str(text.as_ref(), "%Y-%m-%d").ok(),
            _ => None,
        });
        match (parsed, epoch) {
            (Some(found), Some(start)) => {
                builder.append_value(i32::try_from((found - start).num_days()).unwrap_or(0));
            }
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_timestamp(
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
    zone: Option<Arc<str>>,
) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
    for row in rows {
        match row.and_then(|value| match value {
            JsonValue::Text(text) => timestamp_micros(text.as_ref(), context.zone),
            _ => None,
        }) {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    let finished = builder.finish();
    match zone {
        Some(name) => Arc::new(finished.with_timezone(name)),
        None => Arc::new(finished),
    }
}

fn timestamp_micros(text: &str, zone: Tz) -> Option<i64> {
    let trimmed = text.trim();
    for pattern in ["%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%.f%#z"] {
        if let Ok(found) = chrono::DateTime::parse_from_str(trimmed, pattern) {
            return Some(found.timestamp_micros());
        }
    }
    for pattern in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(trimmed, pattern) {
            return local_micros(naive, zone);
        }
        if pattern == "%Y-%m-%d"
            && let Ok(date) = NaiveDate::parse_from_str(trimmed, pattern)
        {
            return local_micros(date.and_hms_opt(0, 0, 0)?, zone);
        }
    }
    None
}

fn local_micros(naive: NaiveDateTime, zone: Tz) -> Option<i64> {
    match zone.from_local_datetime(&naive) {
        MappedLocalTime::Single(moment) | MappedLocalTime::Ambiguous(moment, _) => {
            Some(moment.timestamp_micros())
        }
        MappedLocalTime::None => None,
    }
}

fn build_decimal(rows: &[Option<&JsonValue<'_>>], precision: u8, scale: i8) -> Result<ArrayRef> {
    let mut builder = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        match row.and_then(|value| match value {
            JsonValue::Number(raw) => decimal_units(raw, scale),
            _ => None,
        }) {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(
        builder
            .finish()
            .with_precision_and_scale(precision, scale)?,
    ))
}

#[allow(clippy::cast_possible_truncation)]
fn truncate_to_i128(value: f64) -> i128 {
    value as i128
}

fn decimal_units(raw: &str, scale: i8) -> Option<i128> {
    if raw.contains(['e', 'E']) {
        let value: f64 = raw.parse().ok()?;
        let scaled = (value * 10_f64.powi(i32::from(scale))).round();
        return if scaled.is_finite() && scaled.abs() < 1.7e38 {
            Some(truncate_to_i128(scaled))
        } else {
            None
        };
    }
    let (sign, body) = match raw.strip_prefix('-') {
        Some(rest) => (-1_i128, rest),
        None => (1_i128, raw),
    };
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    let width = usize::try_from(scale).ok()?;
    let mut digits = String::with_capacity(whole.len() + width);
    digits.push_str(whole);
    if fraction.len() >= width {
        digits.push_str(&fraction[..width]);
    } else {
        digits.push_str(fraction);
        digits.push_str(&"0".repeat(width - fraction.len()));
    }
    digits.parse::<i128>().ok().map(|value| value * sign)
}
