use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use chrono::{MappedLocalTime, TimeZone};
use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimeUnit, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::reader::{java_double_text, java_float_text, write_escaped};
use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_cast::parse_session_zone;

#[must_use]
pub fn to_json_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToJson::new()))
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_text(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = chunk.get(1).map_or(0, |byte| u32::from(*byte));
        let third = chunk.get(2).map_or(0, |byte| u32::from(*byte));
        let packed = (first << 16) | (second << 8) | third;
        out.push(BASE64_ALPHABET[((packed >> 18) & 0x3F) as usize] as char);
        out.push(BASE64_ALPHABET[((packed >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64_ALPHABET[((packed >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(BASE64_ALPHABET[(packed & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn decimal_text(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        return raw.to_string();
    }
    let width = usize::try_from(scale).unwrap_or(0);
    let sign = if raw < 0 { "-" } else { "" };
    let digits = raw.unsigned_abs().to_string();
    if digits.len() > width {
        let split = digits.len() - width;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    } else {
        let zeros = "0".repeat(width - digits.len());
        format!("{sign}0.{zeros}{digits}")
    }
}

fn timestamp_text(micros: i64, zone: Tz) -> String {
    let seconds = micros.div_euclid(1_000_000);
    let remainder = micros.rem_euclid(1_000_000);
    let nanos = u32::try_from(remainder).unwrap_or(0) * 1_000;
    match zone.timestamp_opt(seconds, nanos) {
        MappedLocalTime::Single(moment) => {
            let stamp = moment.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
            let offset = moment.format("%:z").to_string();
            if offset == "+00:00" {
                format!("{stamp}Z")
            } else {
                format!("{stamp}{offset}")
            }
        }
        _ => String::new(),
    }
}

fn date_text(days: i32) -> String {
    Date32Type::to_naive_date_opt(days)
        .map(|found| found.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

fn micros_of(array: &dyn Array, row: usize, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => {
            array
                .as_primitive::<datafusion::arrow::datatypes::TimestampSecondType>()
                .value(row)
                * 1_000_000
        }
        TimeUnit::Millisecond => {
            array
                .as_primitive::<datafusion::arrow::datatypes::TimestampMillisecondType>()
                .value(row)
                * 1_000
        }
        TimeUnit::Microsecond => array
            .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
            .value(row),
        TimeUnit::Nanosecond => {
            array
                .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>()
                .value(row)
                / 1_000
        }
    }
}

fn write_scalar(array: &dyn Array, row: usize, out: &mut String, zone: Tz) -> bool {
    match array.data_type() {
        DataType::Boolean => {
            out.push_str(if array.as_boolean().value(row) {
                "true"
            } else {
                "false"
            });
        }
        DataType::Int8 => out.push_str(&array.as_primitive::<Int8Type>().value(row).to_string()),
        DataType::Int16 => out.push_str(&array.as_primitive::<Int16Type>().value(row).to_string()),
        DataType::Int32 => out.push_str(&array.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => out.push_str(&array.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::UInt8 => out.push_str(&array.as_primitive::<UInt8Type>().value(row).to_string()),
        DataType::UInt16 => {
            out.push_str(&array.as_primitive::<UInt16Type>().value(row).to_string())
        }
        DataType::UInt32 => {
            out.push_str(&array.as_primitive::<UInt32Type>().value(row).to_string())
        }
        DataType::UInt64 => {
            out.push_str(&array.as_primitive::<UInt64Type>().value(row).to_string())
        }
        DataType::Float32 => {
            let value = array.as_primitive::<Float32Type>().value(row);
            write_number(&java_float_text(value), value.is_finite(), out);
        }
        DataType::Float64 => {
            let value = array.as_primitive::<Float64Type>().value(row);
            write_number(&java_double_text(value), value.is_finite(), out);
        }
        DataType::Utf8 => write_escaped(array.as_string::<i32>().value(row), out),
        DataType::LargeUtf8 => write_escaped(array.as_string::<i64>().value(row), out),
        DataType::Utf8View => write_escaped(array.as_string_view().value(row), out),
        DataType::Binary => write_escaped(&base64_text(array.as_binary::<i32>().value(row)), out),
        DataType::LargeBinary => {
            write_escaped(&base64_text(array.as_binary::<i64>().value(row)), out);
        }
        DataType::BinaryView => {
            write_escaped(&base64_text(array.as_binary_view().value(row)), out);
        }
        DataType::Date32 => {
            write_escaped(
                &date_text(array.as_primitive::<Date32Type>().value(row)),
                out,
            );
        }
        DataType::Timestamp(unit, _) => {
            write_escaped(&timestamp_text(micros_of(array, row, *unit), zone), out);
        }
        DataType::Decimal128(_, scale) => {
            out.push_str(&decimal_text(
                array.as_primitive::<Decimal128Type>().value(row),
                *scale,
            ));
        }
        _ => return false,
    }
    true
}

fn write_number(text: &str, finite: bool, out: &mut String) {
    if finite {
        out.push_str(text);
    } else {
        write_escaped(text, out);
    }
}

fn write_json(array: &dyn Array, row: usize, out: &mut String, zone: Tz) -> Result<()> {
    if array.is_null(row) {
        out.push_str("null");
        return Ok(());
    }
    if write_scalar(array, row, out, zone) {
        return Ok(());
    }
    match array.data_type() {
        DataType::Null => out.push_str("null"),
        DataType::List(_) => write_list(array.as_list::<i32>(), row, out, zone)?,
        DataType::LargeList(_) => write_large_list(array.as_list::<i64>(), row, out, zone)?,
        DataType::Struct(_) => write_struct(array, row, out, zone)?,
        DataType::Map(_, _) => write_map(array, row, out, zone)?,
        DataType::Dictionary(_, _) => {
            let flat = datafusion::arrow::compute::cast(array, &flatten_dictionary(array)?)?;
            write_json(flat.as_ref(), row, out, zone)?;
        }
        other => return exec_err!("'to_json' cannot render a value of type {other}"),
    }
    Ok(())
}

fn flatten_dictionary(array: &dyn Array) -> Result<DataType> {
    match array.data_type() {
        DataType::Dictionary(_, value) => Ok(value.as_ref().clone()),
        other => exec_err!("'to_json' expected a dictionary, got {other}"),
    }
}

fn write_list(
    list: &datafusion::arrow::array::GenericListArray<i32>,
    row: usize,
    out: &mut String,
    zone: Tz,
) -> Result<()> {
    let values = list.value(row);
    out.push('[');
    for index in 0..values.len() {
        if index > 0 {
            out.push(',');
        }
        write_json(values.as_ref(), index, out, zone)?;
    }
    out.push(']');
    Ok(())
}

fn write_large_list(
    list: &datafusion::arrow::array::GenericListArray<i64>,
    row: usize,
    out: &mut String,
    zone: Tz,
) -> Result<()> {
    let values = list.value(row);
    out.push('[');
    for index in 0..values.len() {
        if index > 0 {
            out.push(',');
        }
        write_json(values.as_ref(), index, out, zone)?;
    }
    out.push(']');
    Ok(())
}

fn write_struct(array: &dyn Array, row: usize, out: &mut String, zone: Tz) -> Result<()> {
    let entries = array.as_struct();
    let DataType::Struct(fields) = array.data_type() else {
        return exec_err!("'to_json' expected a struct");
    };
    out.push('{');
    let mut written = 0;
    for (index, field) in fields.iter().enumerate() {
        let column = entries.column(index);
        if column.is_null(row) {
            continue;
        }
        if written > 0 {
            out.push(',');
        }
        write_escaped(field.name(), out);
        out.push(':');
        write_json(column.as_ref(), row, out, zone)?;
        written += 1;
    }
    out.push('}');
    Ok(())
}

fn write_map(array: &dyn Array, row: usize, out: &mut String, zone: Tz) -> Result<()> {
    let map = array.as_map();
    let entries = map.value(row);
    let keys = entries.column(0);
    let values = entries.column(1);
    out.push('{');
    for index in 0..entries.len() {
        if index > 0 {
            out.push(',');
        }
        write_map_key(keys.as_ref(), index, out, zone)?;
        out.push(':');
        write_json(values.as_ref(), index, out, zone)?;
    }
    out.push('}');
    Ok(())
}

fn write_map_key(keys: &dyn Array, index: usize, out: &mut String, zone: Tz) -> Result<()> {
    let mut rendered = String::new();
    write_json(keys, index, &mut rendered, zone)?;
    if rendered.starts_with('"') {
        out.push_str(&rendered);
    } else {
        write_escaped(&rendered, out);
    }
    Ok(())
}

#[derive(Debug)]
struct SparkToJson {
    signature: Signature,
}

impl SparkToJson {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkToJson {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkToJson {}

impl Hash for SparkToJson {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkToJson {
    crate::shim_udf_boilerplate!("to_json");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [value] => Ok(vec![value.clone()]),
            [value, options] => Ok(vec![value.clone(), options.clone()]),
            _ => exec_err!(
                "'to_json' requires 1 or 2 arguments, got {}",
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(values) = arrays.first() else {
            return exec_err!("'to_json' requires 1 or 2 arguments, got 0");
        };
        if !matches!(
            values.data_type(),
            DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_) | DataType::LargeList(_)
        ) {
            return exec_err!(
                "'to_json' requires a STRUCT, ARRAY, or MAP argument, got {}",
                values.data_type()
            );
        }
        let zone =
            parse_session_zone(session_time_zone_from_options(args.config_options.as_ref()))?;
        let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 24);
        let mut rendered = String::new();
        for row in 0..values.len() {
            if values.is_null(row) {
                builder.append_null();
                continue;
            }
            rendered.clear();
            write_json(values.as_ref(), row, &mut rendered, zone)?;
            builder.append_value(&rendered);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
