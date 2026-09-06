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

use super::reader::{JsonValue, java_double_text, json_number_text, write_compact};

pub(crate) struct DecodeContext {
    pub zone: Tz,
}

pub(crate) struct Decoded {
    pub array: ArrayRef,
    pub bad: Vec<bool>,
}

pub(crate) fn build_root(
    target: &DataType,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<Decoded> {
    match target {
        DataType::Struct(fields) => build_struct(fields, rows, context, true),
        DataType::List(_) => {
            let wrapped: Vec<Option<JsonValue<'_>>> = rows
                .iter()
                .map(|row| match row {
                    Some(value @ JsonValue::Object(_)) => {
                        Some(JsonValue::Array(vec![(*value).clone()]))
                    }
                    _ => None,
                })
                .collect();
            let effective: Vec<Option<&JsonValue<'_>>> = rows
                .iter()
                .zip(&wrapped)
                .map(|(row, extra)| extra.as_ref().or(*row))
                .collect();
            build_array(target, &effective, context)
        }
        other => build_array(other, rows, context),
    }
}

fn build_array(
    target: &DataType,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<Decoded> {
    match target {
        DataType::Struct(fields) => build_struct(fields, rows, context, false),
        DataType::List(field) => build_list(field, rows, context),
        DataType::Map(entries, _) => build_map(target, entries, rows, context),
        DataType::Null => Ok(Decoded {
            array: new_null_array(&DataType::Null, rows.len()),
            bad: vec![false; rows.len()],
        }),
        primitive => build_primitive(primitive, rows, context),
    }
}

fn nulls_of(present: &[bool]) -> Option<NullBuffer> {
    if present.iter().all(|found| *found) {
        None
    } else {
        Some(NullBuffer::from(present.to_vec()))
    }
}

fn build_struct(
    fields: &Fields,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
    root: bool,
) -> Result<Decoded> {
    let shaped: Vec<bool> = rows
        .iter()
        .map(|row| matches!(row, Some(JsonValue::Object(_))))
        .collect();
    let mut bad: Vec<bool> = rows
        .iter()
        .zip(&shaped)
        .map(|(row, ok)| row.is_some() && !ok)
        .collect();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for field in fields {
        let child: Vec<Option<&JsonValue<'_>>> = rows
            .iter()
            .map(|row| row.and_then(|value| object_field(value, field.name())))
            .collect();
        let decoded = build_array(field.data_type(), &child, context)?;
        for (slot, found) in bad.iter_mut().zip(&decoded.bad) {
            *slot |= *found;
        }
        columns.push(decoded.array);
    }
    let present: Vec<bool> = rows
        .iter()
        .zip(&shaped)
        .map(|(row, ok)| row.is_some() && (root || *ok))
        .collect();
    let nulls = nulls_of(&present);
    let array: ArrayRef = if fields.is_empty() {
        Arc::new(StructArray::new_empty_fields(rows.len(), nulls))
    } else {
        Arc::new(StructArray::try_new(fields.clone(), columns, nulls)?)
    };
    Ok(Decoded { array, bad })
}

fn object_field<'a>(value: &'a JsonValue<'a>, name: &str) -> Option<&'a JsonValue<'a>> {
    let JsonValue::Object(entries) = value else {
        return None;
    };
    entries
        .iter()
        .rev()
        .find(|(key, _)| key.as_ref() == name)
        .map(|(_, found)| found)
        .filter(|found| !matches!(found, JsonValue::Null))
}

fn build_list(
    field: &Arc<Field>,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<Decoded> {
    let mut flat: Vec<Option<&JsonValue<'_>>> = Vec::new();
    let mut spans: Vec<Option<(usize, usize)>> = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            Some(JsonValue::Array(items)) => {
                let start = flat.len();
                for item in items {
                    flat.push(if matches!(item, JsonValue::Null) {
                        None
                    } else {
                        Some(item)
                    });
                }
                spans.push(Some((start, flat.len())));
            }
            _ => spans.push(None),
        }
    }
    let decoded = build_array(field.data_type(), &flat, context)?;
    let mut bad: Vec<bool> = Vec::with_capacity(rows.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    let mut present: Vec<bool> = Vec::with_capacity(rows.len());
    offsets.push(0);
    let mut length = 0_i32;
    for (row, span) in rows.iter().zip(&spans) {
        match span {
            Some((start, end)) if !decoded.bad[*start..*end].iter().any(|found| *found) => {
                length += i32::try_from(end - start).unwrap_or(0);
                present.push(true);
                bad.push(false);
            }
            Some(_) => {
                present.push(false);
                bad.push(true);
            }
            None => {
                present.push(false);
                bad.push(row.is_some());
            }
        }
        offsets.push(length);
    }
    let values = compact_values(&decoded.array, &spans, &present)?;
    Ok(Decoded {
        array: Arc::new(ListArray::try_new(
            Arc::clone(field),
            OffsetBuffer::new(offsets.into()),
            values,
            nulls_of(&present),
        )?),
        bad,
    })
}

fn compact_values(
    values: &ArrayRef,
    spans: &[Option<(usize, usize)>],
    present: &[bool],
) -> Result<ArrayRef> {
    let mut indices: Vec<u32> = Vec::new();
    for (span, keep) in spans.iter().zip(present) {
        if !keep {
            continue;
        }
        if let Some((start, end)) = span {
            for index in *start..*end {
                indices.push(u32::try_from(index).unwrap_or(0));
            }
        }
    }
    let taken = datafusion::arrow::compute::take(
        values.as_ref(),
        &datafusion::arrow::array::UInt32Array::from(indices),
        None,
    )?;
    Ok(taken)
}

fn build_map(
    target: &DataType,
    entries: &Arc<Field>,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<Decoded> {
    let DataType::Struct(pair) = entries.data_type() else {
        return exec_err!("'from_json' expects a MAP entry struct, got {target}");
    };
    let (key_field, value_field) = (&pair[0], &pair[1]);
    if key_field.data_type() != &DataType::Utf8 {
        return exec_err!(
            "[DATATYPE_MISMATCH.INVALID_JSON_MAP_KEY_TYPE] Input schema {target} can only \
             contain STRING as a key type for a MAP"
        );
    }
    let mut keys: Vec<&str> = Vec::new();
    let mut flat: Vec<Option<&JsonValue<'_>>> = Vec::new();
    let mut spans: Vec<Option<(usize, usize)>> = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            Some(JsonValue::Object(pairs)) => {
                let start = flat.len();
                for (key, item) in pairs {
                    keys.push(key.as_ref());
                    flat.push(if matches!(item, JsonValue::Null) {
                        None
                    } else {
                        Some(item)
                    });
                }
                spans.push(Some((start, flat.len())));
            }
            _ => spans.push(None),
        }
    }
    let decoded = build_array(value_field.data_type(), &flat, context)?;
    let mut bad: Vec<bool> = Vec::with_capacity(rows.len());
    let mut present: Vec<bool> = Vec::with_capacity(rows.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    let mut length = 0_i32;
    let mut kept: Vec<&str> = Vec::new();
    let mut indices: Vec<u32> = Vec::new();
    for (row, span) in rows.iter().zip(&spans) {
        match span {
            Some((start, end)) if !decoded.bad[*start..*end].iter().any(|found| *found) => {
                for (offset, key) in keys[*start..*end].iter().enumerate() {
                    kept.push(key);
                    indices.push(u32::try_from(start + offset).unwrap_or(0));
                }
                length += i32::try_from(end - start).unwrap_or(0);
                present.push(true);
                bad.push(false);
            }
            Some(_) => {
                present.push(false);
                bad.push(true);
            }
            None => {
                present.push(false);
                bad.push(row.is_some());
            }
        }
        offsets.push(length);
    }
    let values = datafusion::arrow::compute::take(
        decoded.array.as_ref(),
        &datafusion::arrow::array::UInt32Array::from(indices),
        None,
    )?;
    let key_array: ArrayRef = Arc::new(StringArray::from(kept));
    let pair_array = StructArray::try_new(pair.clone(), vec![key_array, values], None)?;
    Ok(Decoded {
        array: Arc::new(MapArray::try_new(
            Arc::clone(entries),
            OffsetBuffer::new(offsets.into()),
            pair_array,
            nulls_of(&present),
            false,
        )?),
        bad,
    })
}

macro_rules! build_integer {
    ($builder:expr, $rows:expr, $kind:ty) => {{
        let mut builder = $builder;
        let mut bad = Vec::with_capacity($rows.len());
        for row in $rows {
            let value = row
                .and_then(integer_of)
                .and_then(|found| <$kind>::try_from(found).ok());
            match value {
                Some(found) => {
                    builder.append_value(found);
                    bad.push(false);
                }
                None => {
                    builder.append_null();
                    bad.push(row.is_some());
                }
            }
        }
        Ok(Decoded {
            array: Arc::new(builder.finish()) as ArrayRef,
            bad,
        })
    }};
}

fn build_primitive(
    target: &DataType,
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
) -> Result<Decoded> {
    match target {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            let mut bad = Vec::with_capacity(rows.len());
            for row in rows {
                match row {
                    Some(JsonValue::Bool(flag)) => {
                        builder.append_value(*flag);
                        bad.push(false);
                    }
                    other => {
                        builder.append_null();
                        bad.push(other.is_some());
                    }
                }
            }
            Ok(Decoded {
                array: Arc::new(builder.finish()),
                bad,
            })
        }
        DataType::Int8 => build_integer!(Int8Builder::with_capacity(rows.len()), rows, i8),
        DataType::Int16 => build_integer!(Int16Builder::with_capacity(rows.len()), rows, i16),
        DataType::Int32 => build_integer!(Int32Builder::with_capacity(rows.len()), rows, i32),
        DataType::Int64 => build_integer!(Int64Builder::with_capacity(rows.len()), rows, i64),
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(rows.len());
            let mut bad = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = row.and_then(double_of) {
                    builder.append_value(truncate_to_f32(value));
                    bad.push(false);
                } else {
                    builder.append_null();
                    bad.push(row.is_some());
                }
            }
            Ok(Decoded {
                array: Arc::new(builder.finish()),
                bad,
            })
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            let mut bad = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = row.and_then(double_of) {
                    builder.append_value(value);
                    bad.push(false);
                } else {
                    builder.append_null();
                    bad.push(row.is_some());
                }
            }
            Ok(Decoded {
                array: Arc::new(builder.finish()),
                bad,
            })
        }
        DataType::Utf8 => Ok(build_text(rows)),
        DataType::Binary => Ok(build_binary(rows)),
        DataType::Date32 => Ok(build_date(rows)),
        DataType::Timestamp(_, zone) => Ok(build_timestamp(rows, context, zone.clone())),
        DataType::Decimal128(precision, scale) => Ok(build_decimal(rows, *precision, *scale)?),
        other => exec_err!("'from_json' cannot decode into {other}"),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn truncate_to_f32(value: f64) -> f32 {
    value as f32
}

#[allow(clippy::cast_possible_truncation)]
fn truncate_to_i128(value: f64) -> i128 {
    value as i128
}

fn integer_of(value: &JsonValue<'_>) -> Option<i64> {
    match value {
        JsonValue::Number(raw) if !raw.contains(['.', 'e', 'E']) => raw.parse::<i64>().ok(),
        _ => None,
    }
}

fn double_of(value: &JsonValue<'_>) -> Option<f64> {
    match value {
        JsonValue::Number(raw) => raw.parse::<f64>().ok(),
        JsonValue::NonFinite(found) => Some(*found),
        JsonValue::Text(text) => non_finite_text(text.as_ref()),
        _ => None,
    }
}

fn non_finite_text(text: &str) -> Option<f64> {
    match text.trim() {
        "NaN" => Some(f64::NAN),
        "Infinity" | "+Infinity" | "INF" | "+INF" => Some(f64::INFINITY),
        "-Infinity" | "-INF" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

fn build_text(rows: &[Option<&JsonValue<'_>>]) -> Decoded {
    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    for row in rows {
        match row {
            Some(JsonValue::Text(text)) => builder.append_value(text.as_ref()),
            Some(JsonValue::Number(raw)) => builder.append_value(json_number_text(raw)),
            Some(JsonValue::NonFinite(found)) => builder.append_value(java_double_text(*found)),
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
    Decoded {
        array: Arc::new(builder.finish()),
        bad: vec![false; rows.len()],
    }
}

fn build_binary(rows: &[Option<&JsonValue<'_>>]) -> Decoded {
    let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut bad = Vec::with_capacity(rows.len());
    for row in rows {
        let decoded = row.and_then(|value| match value {
            JsonValue::Text(text) => base64_bytes(text.as_ref()),
            _ => None,
        });
        if let Some(bytes) = decoded {
            builder.append_value(bytes);
            bad.push(false);
        } else {
            builder.append_null();
            bad.push(row.is_some());
        }
    }
    Decoded {
        array: Arc::new(builder.finish()),
        bad,
    }
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

fn build_date(rows: &[Option<&JsonValue<'_>>]) -> Decoded {
    let mut builder = Date32Builder::with_capacity(rows.len());
    let mut bad = Vec::with_capacity(rows.len());
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1);
    for row in rows {
        let parsed = row.and_then(|value| match value {
            JsonValue::Text(text) => NaiveDate::parse_from_str(text.as_ref(), "%Y-%m-%d").ok(),
            _ => None,
        });
        if let (Some(found), Some(start)) = (parsed, epoch) {
            builder.append_value(i32::try_from((found - start).num_days()).unwrap_or(0));
            bad.push(false);
        } else {
            builder.append_null();
            bad.push(row.is_some());
        }
    }
    Decoded {
        array: Arc::new(builder.finish()),
        bad,
    }
}

fn build_timestamp(
    rows: &[Option<&JsonValue<'_>>],
    context: &DecodeContext,
    zone: Option<Arc<str>>,
) -> Decoded {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
    let mut bad = Vec::with_capacity(rows.len());
    for row in rows {
        let decoded = row.and_then(|value| match value {
            JsonValue::Text(text) => timestamp_micros(text.as_ref(), context.zone),
            JsonValue::Number(raw) => raw
                .parse::<i64>()
                .ok()
                .and_then(|seconds| seconds.checked_mul(1_000_000)),
            _ => None,
        });
        if let Some(value) = decoded {
            builder.append_value(value);
            bad.push(false);
        } else {
            builder.append_null();
            bad.push(row.is_some());
        }
    }
    let finished = builder.finish();
    Decoded {
        array: match zone {
            Some(name) => Arc::new(finished.with_timezone(name)),
            None => Arc::new(finished),
        },
        bad,
    }
}

fn timestamp_micros(text: &str, zone: Tz) -> Option<i64> {
    let trimmed = text.trim();
    for pattern in ["%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%.f%#z"] {
        if let Ok(found) = chrono::DateTime::parse_from_str(trimmed, pattern) {
            return Some(found.timestamp_micros());
        }
    }
    for pattern in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(trimmed, pattern) {
            return local_micros(naive, zone);
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return local_micros(date.and_hms_opt(0, 0, 0)?, zone);
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

fn build_decimal(rows: &[Option<&JsonValue<'_>>], precision: u8, scale: i8) -> Result<Decoded> {
    let mut builder = Decimal128Builder::with_capacity(rows.len());
    let mut bad = Vec::with_capacity(rows.len());
    for row in rows {
        let decoded = row.and_then(|value| match value {
            JsonValue::Number(raw) => decimal_units(raw, precision, scale),
            JsonValue::Text(text) => decimal_units(text.as_ref().trim(), precision, scale),
            _ => None,
        });
        if let Some(value) = decoded {
            builder.append_value(value);
            bad.push(false);
        } else {
            builder.append_null();
            bad.push(row.is_some());
        }
    }
    Ok(Decoded {
        array: Arc::new(
            builder
                .finish()
                .with_precision_and_scale(precision, scale)?,
        ),
        bad,
    })
}

fn decimal_units(raw: &str, precision: u8, scale: i8) -> Option<i128> {
    let (negative, body) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    let (mantissa, exponent) = match body.split_once(['e', 'E']) {
        Some((head, tail)) => (head, tail.parse::<i32>().ok()?),
        None => (body, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.chars().all(|found| found.is_ascii_digit())
        || !fraction.chars().all(|found| found.is_ascii_digit())
    {
        return None;
    }
    let digits: String = format!("{whole}{fraction}");
    let point = i32::try_from(whole.len()).ok()? + exponent;
    let shift = i32::from(scale) + point - i32::try_from(digits.len()).ok()?;
    let mut units = scaled_units(&digits, shift)?;
    if negative {
        units = -units;
    }
    let width = units.unsigned_abs().to_string().len();
    if width > usize::from(precision) {
        return None;
    }
    Some(units)
}

fn scaled_units(digits: &str, shift: i32) -> Option<i128> {
    if shift >= 0 {
        let padded = format!("{digits}{}", "0".repeat(usize::try_from(shift).ok()?));
        return padded.parse::<i128>().ok();
    }
    let drop = usize::try_from(-shift).ok()?;
    if drop >= digits.len() {
        let leading = "0".repeat(drop - digits.len());
        let probe = format!("0.{leading}{digits}");
        let rounded: f64 = probe.parse().ok()?;
        return Some(truncate_to_i128(rounded.round()));
    }
    let keep = &digits[..digits.len() - drop];
    let dropped = &digits[digits.len() - drop..];
    let mut units = if keep.is_empty() {
        0_i128
    } else {
        keep.parse::<i128>().ok()?
    };
    if dropped.as_bytes()[0] >= b'5' {
        units = units.checked_add(1)?;
    }
    Some(units)
}
