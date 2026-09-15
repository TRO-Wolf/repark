use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    StringArray, StringBuilder, TimestampMicrosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use chrono::{DateTime, NaiveDate, TimeZone as _};
use datafusion::error::DataFusionError;

use crate::Error;
use crate::partition_timestamp::{
    looks_like_timestamp, parse_timestamp_ntz_micros, parse_wall_naive,
};

const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PartitionValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Binary(Vec<u8>),
    Date32(i32),
    Decimal128(i128, i8),
    TimestampMicros(i64),
    Text(String),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoveredPartitions {
    pub(crate) fields: Vec<Field>,
    pub(crate) values: HashMap<PathBuf, Vec<PartitionValue>>,
    pub(crate) raw: HashMap<PathBuf, Vec<Option<String>>>,
}

fn unescape_partition_value(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let hex_value = |digit: u8| {
        if digit.is_ascii_digit() {
            digit - b'0'
        } else {
            digit.to_ascii_lowercase() - b'a' + 10
        }
    };
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        let high = bytes.get(index + 1).copied().filter(u8::is_ascii_hexdigit);
        let low = bytes.get(index + 2).copied().filter(u8::is_ascii_hexdigit);
        if byte == b'%'
            && let Some((high, low)) = high.zip(low)
        {
            out.push(hex_value(high) * 16 + hex_value(low));
            index += 3;
        } else {
            out.push(byte);
            index += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| raw.to_string())
}

pub(crate) fn partition_path_specs(root: &Path, file: &Path) -> Vec<(String, Option<String>)> {
    let relative = file.strip_prefix(root).unwrap_or(file);
    let Some(parent) = relative.parent() else {
        return Vec::new();
    };
    let mut specs = Vec::new();
    for component in parent.components() {
        let text = component.as_os_str().to_string_lossy();
        if let Some((key, raw)) = text.split_once('=')
            && !key.is_empty()
        {
            let value = if raw == HIVE_DEFAULT_PARTITION || raw.is_empty() {
                None
            } else {
                Some(unescape_partition_value(raw))
            };
            specs.push((key.to_string(), value));
        }
    }
    specs
}

fn looks_like_date(text: &str) -> bool {
    text.len() == 10 && NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()
}

fn infer_partition_type(candidates: &[&str], session_zone: &str) -> DataType {
    if candidates.iter().all(|text| text.parse::<i32>().is_ok()) {
        DataType::Int32
    } else if candidates.iter().all(|text| text.parse::<i64>().is_ok()) {
        DataType::Int64
    } else if candidates.iter().all(|text| text.parse::<f64>().is_ok()) {
        DataType::Float64
    } else if candidates.iter().all(|text| looks_like_date(text)) {
        DataType::Date32
    } else if candidates.iter().all(|text| looks_like_timestamp(text)) {
        DataType::Timestamp(TimeUnit::Microsecond, Some(session_zone.into()))
    } else {
        DataType::Utf8
    }
}

fn parse_partition_value(text: &str, data_type: &DataType, zone: Tz) -> PartitionValue {
    match data_type {
        DataType::Int32 => text
            .parse::<i32>()
            .map_or(PartitionValue::Null, PartitionValue::Int32),
        DataType::Int64 => text
            .parse::<i64>()
            .map_or(PartitionValue::Null, PartitionValue::Int64),
        DataType::Float64 => text
            .parse::<f64>()
            .map_or(PartitionValue::Null, PartitionValue::Float64),
        DataType::Timestamp(TimeUnit::Microsecond, _) => parse_timestamp_micros_zone(text, zone)
            .map_or(PartitionValue::Null, PartitionValue::TimestampMicros),
        DataType::Date32 => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()
            .and_then(|date| {
                i32::try_from(
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                        .num_days(),
                )
                .ok()
            })
            .map_or(PartitionValue::Null, PartitionValue::Date32),
        _ => PartitionValue::Text(text.to_string()),
    }
}

pub(crate) fn canonical_partition_text(value: &PartitionValue) -> Option<String> {
    match value {
        PartitionValue::Null => None,
        PartitionValue::Boolean(flag) => Some(flag.to_string()),
        PartitionValue::Int8(number) => Some(number.to_string()),
        PartitionValue::Int16(number) => Some(number.to_string()),
        PartitionValue::Int32(number) => Some(number.to_string()),
        PartitionValue::Int64(number) => Some(number.to_string()),
        PartitionValue::Float32(number) => Some(number.to_string()),
        PartitionValue::Float64(number) => Some(number.to_string()),
        PartitionValue::Binary(bytes) => String::from_utf8(bytes.clone()).ok(),
        PartitionValue::Date32(days) => Some(days.to_string()),
        PartitionValue::Decimal128(scaled, scale) => {
            crate::text_partition::decimal_plain_text(*scaled, *scale).ok()
        }
        PartitionValue::TimestampMicros(micros) => Some(micros.to_string()),
        PartitionValue::Text(text) => Some(text.clone()),
    }
}

pub(crate) fn parse_decimal_scaled(raw: &str, precision: u8, scale: i8) -> Option<i128> {
    let balanced = raw.strip_prefix('+').unwrap_or(raw);
    let (negative, digits) = match balanced.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, balanced),
    };
    let (head, tail) = match digits.split_once('.') {
        Some((head, tail)) => (head, Some(tail)),
        None => (digits, None),
    };
    if head.is_empty() && tail.is_none_or(str::is_empty) {
        return None;
    }
    if !head.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let tail = tail.unwrap_or("");
    let width = usize::try_from(scale.max(0)).ok()?;
    if tail.len() > width || !tail.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let padded = format!("{head}{tail:0<width$}");
    let significant = padded.trim_start_matches('0');
    if significant.len() > usize::from(precision) {
        return None;
    }
    let magnitude: i128 = if significant.is_empty() {
        0
    } else {
        significant.parse().ok()?
    };
    if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    }
}

fn zoned_wall_micros(zoned: DateTime<Tz>) -> Option<i64> {
    zoned
        .timestamp()
        .checked_mul(1_000_000)?
        .checked_add(i64::from(zoned.timestamp_subsec_micros()))
}

pub(crate) fn parse_timestamp_micros_zone(raw: &str, zone: Tz) -> Option<i64> {
    let wall = parse_wall_naive(raw)?;
    let zoned = zone.from_local_datetime(&wall).single()?;
    zoned_wall_micros(zoned)
}

macro_rules! finish_partition_numbers {
    ($taken:expr, $builder:ty, $scalar:ty) => {{
        let mut out = <$builder>::new();
        for index in 0..$taken.len() {
            if $taken.is_null(index) {
                out.append_null();
            } else {
                let parsed: $scalar = $taken.value(index).parse().map_err(|_| {
                    DataFusionError::Execution(
                        "partition value does not match its inferred type".to_string(),
                    )
                })?;
                out.append_value(parsed);
            }
        }
        Arc::new(out.finish())
    }};
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn finish_partition_columns(
    builders: Vec<StringBuilder>,
    types: &[DataType],
    count: usize,
) -> Result<Vec<ArrayRef>, DataFusionError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(builders.len());
    for (mut builder, data_type) in builders.into_iter().zip(types.iter()) {
        let taken: StringArray = builder.finish();
        let taken = taken.slice(0, count);
        let column: ArrayRef = match data_type {
            DataType::Boolean => finish_partition_numbers!(taken, BooleanBuilder, bool),
            DataType::Int8 => finish_partition_numbers!(taken, Int8Builder, i8),
            DataType::Int16 => finish_partition_numbers!(taken, Int16Builder, i16),
            DataType::Int32 => finish_partition_numbers!(taken, Int32Builder, i32),
            DataType::Int64 => finish_partition_numbers!(taken, Int64Builder, i64),
            DataType::Float32 => finish_partition_numbers!(taken, Float32Builder, f32),
            DataType::Float64 => finish_partition_numbers!(taken, Float64Builder, f64),
            DataType::Binary => {
                let mut out = BinaryBuilder::new();
                for index in 0..taken.len() {
                    if taken.is_null(index) {
                        out.append_null();
                    } else {
                        out.append_value(taken.value(index).as_bytes());
                    }
                }
                Arc::new(out.finish())
            }
            DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
                let live = (0..taken.len()).any(|index| !taken.is_null(index));
                if live {
                    return Err(DataFusionError::Execution(
                        "partition value does not match its inferred type".to_string(),
                    ));
                }
                new_null_array(data_type, taken.len())
            }
            DataType::Date32 => finish_partition_numbers!(taken, Date32Builder, i32),
            DataType::Decimal128(precision, scale) => {
                let mut out = Decimal128Builder::new();
                for index in 0..taken.len() {
                    if taken.is_null(index) {
                        out.append_null();
                    } else {
                        let scaled = parse_decimal_scaled(taken.value(index), *precision, *scale)
                            .ok_or_else(|| {
                            DataFusionError::Execution(
                                "partition value does not match its inferred type".to_string(),
                            )
                        })?;
                        out.append_value(scaled);
                    }
                }
                Arc::new(out.finish().with_precision_and_scale(*precision, *scale)?)
            }
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                let mut values = Vec::with_capacity(taken.len());
                for index in 0..taken.len() {
                    if taken.is_null(index) {
                        values.push(None);
                    } else {
                        let micros: i64 = taken.value(index).parse().map_err(|_| {
                            DataFusionError::Execution(
                                "partition value does not match its inferred type".to_string(),
                            )
                        })?;
                        values.push(Some(micros));
                    }
                }
                Arc::new(
                    TimestampMicrosecondArray::from(values).with_timezone_opt(timezone.as_deref()),
                )
            }
            _ => Arc::new(taken),
        };
        columns.push(column);
    }
    Ok(columns)
}

pub(crate) fn plan_partition_slots(
    plan: &[Option<usize>],
    partitions: usize,
    types: &[DataType],
) -> (Vec<Option<usize>>, Vec<DataType>) {
    let mut slots: Vec<Option<usize>> = vec![None; partitions];
    let mut included = Vec::new();
    for column in plan {
        if let Some(provider) = column
            && slots[*provider].is_none()
        {
            slots[*provider] = Some(included.len());
            included.push(types[*provider].clone());
        }
    }
    (slots, included)
}

pub(crate) fn push_partition_row(
    parts: &mut [StringBuilder],
    slots: &[Option<usize>],
    current: &[Option<String>],
) {
    for (provider, slot) in slots.iter().enumerate() {
        if let Some(index) = slot {
            match current.get(provider).and_then(|value| value.as_ref()) {
                None => parts[*index].append_null(),
                Some(text) => parts[*index].append_value(text),
            }
        }
    }
}

pub(crate) struct TextRowSink<'a> {
    pub(crate) value: Option<&'a mut StringBuilder>,
    pub(crate) parts: &'a mut [StringBuilder],
    pub(crate) slots: &'a [Option<usize>],
    pub(crate) current: &'a [Option<String>],
    pub(crate) blank: &'a mut usize,
}

pub(crate) fn emit_text_row(
    sink: &mut TextRowSink<'_>,
    piece: &[u8],
    limit: Option<usize>,
    emitted: usize,
) -> bool {
    let pending = if let Some(builder) = sink.value.as_ref() {
        builder.len()
    } else if let Some(builder) = sink.parts.first() {
        builder.len()
    } else {
        *sink.blank
    };
    if limit.is_some_and(|max| emitted + pending >= max) {
        return false;
    }
    match sink.value.as_mut() {
        Some(builder) => builder.append_value(&String::from_utf8_lossy(piece)),
        None if sink.parts.is_empty() => *sink.blank += 1,
        None => {}
    }
    push_partition_row(sink.parts, sink.slots, sink.current);
    true
}

pub(crate) fn order_batch_columns(
    plan: &[Option<usize>],
    value: Option<&ArrayRef>,
    finished: &[ArrayRef],
    slots: &[Option<usize>],
) -> Result<Vec<ArrayRef>, DataFusionError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(plan.len());
    for column in plan {
        match column {
            None => columns.push(value.cloned().ok_or_else(|| {
                DataFusionError::Internal("text scan lost its value column".to_string())
            })?),
            Some(provider) => {
                let slot = slots[*provider].ok_or_else(|| {
                    DataFusionError::Internal("text scan lost its partition column".to_string())
                })?;
                columns.push(Arc::clone(&finished[slot]));
            }
        }
    }
    Ok(columns)
}

pub(crate) fn user_partition_type(name: &str, session_zone: &str) -> Option<(DataType, String)> {
    match name {
        "string" => Some((DataType::Utf8, String::from("STRING"))),
        "int" | "integer" => Some((DataType::Int32, String::from("INT"))),
        "bigint" | "long" => Some((DataType::Int64, String::from("BIGINT"))),
        "double" => Some((DataType::Float64, String::from("DOUBLE"))),
        "boolean" => Some((DataType::Boolean, String::from("BOOLEAN"))),
        "float" => Some((DataType::Float32, String::from("FLOAT"))),
        "smallint" => Some((DataType::Int16, String::from("SMALLINT"))),
        "tinyint" => Some((DataType::Int8, String::from("TINYINT"))),
        "binary" => Some((DataType::Binary, String::from("BINARY"))),
        "date" => Some((DataType::Date32, String::from("DATE"))),
        "timestamp" => Some((
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_zone.into())),
            String::from("TIMESTAMP"),
        )),
        "timestamp_ntz" => Some((
            DataType::Timestamp(TimeUnit::Microsecond, None),
            String::from("TIMESTAMP_NTZ"),
        )),
        _ => parse_decimal_user_type(name).or_else(|| parse_array_user_type(name)),
    }
}

fn parse_array_user_type(name: &str) -> Option<(DataType, String)> {
    let inner = name.strip_prefix("array<")?.strip_suffix('>')?;
    let item = match inner {
        "boolean" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "bigint" | "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32,
        _ => return None,
    };
    Some((
        DataType::List(Arc::new(Field::new("item", item, true))),
        format!("ARRAY<{}>", inner.to_uppercase()),
    ))
}

fn parse_decimal_user_type(name: &str) -> Option<(DataType, String)> {
    let inner = name.strip_prefix("decimal(")?.strip_suffix(')')?;
    let (precision_text, scale_text) = inner.split_once(',')?;
    let precision: u8 = precision_text.trim().parse().ok()?;
    let scale: i8 = scale_text.trim().parse().ok()?;
    let ceiling = i8::try_from(precision).ok()?;
    if precision == 0 || precision > 38 || scale < 0 || scale > ceiling {
        return None;
    }
    Some((
        DataType::Decimal128(precision, scale),
        format!("DECIMAL({precision},{scale})"),
    ))
}

pub(crate) fn cast_raw_partition_value(
    raw: &str,
    data_type: &DataType,
    zone: Tz,
) -> Option<PartitionValue> {
    match data_type {
        DataType::Boolean => {
            if raw.eq_ignore_ascii_case("true") {
                Some(PartitionValue::Boolean(true))
            } else if raw.eq_ignore_ascii_case("false") {
                Some(PartitionValue::Boolean(false))
            } else {
                None
            }
        }
        DataType::Int8 => raw.parse::<i8>().ok().map(PartitionValue::Int8),
        DataType::Int16 => raw.parse::<i16>().ok().map(PartitionValue::Int16),
        DataType::Int32 => raw.parse::<i32>().ok().map(PartitionValue::Int32),
        DataType::Int64 => raw.parse::<i64>().ok().map(PartitionValue::Int64),
        DataType::Float32 => raw.parse::<f32>().ok().map(PartitionValue::Float32),
        DataType::Float64 => raw.parse::<f64>().ok().map(PartitionValue::Float64),
        DataType::Binary => Some(PartitionValue::Binary(raw.as_bytes().to_vec())),
        DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => None,
        DataType::Date32 => NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .ok()
            .and_then(|date| {
                i32::try_from(
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                        .num_days(),
                )
                .ok()
            })
            .map(PartitionValue::Date32),
        DataType::Decimal128(precision, scale) => parse_decimal_scaled(raw, *precision, *scale)
            .map(|scaled| PartitionValue::Decimal128(scaled, *scale)),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            parse_timestamp_micros_zone(raw, zone).map(PartitionValue::TimestampMicros)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            parse_timestamp_ntz_micros(raw).map(PartitionValue::TimestampMicros)
        }
        _ => Some(PartitionValue::Text(raw.to_string())),
    }
}

pub(crate) fn invalid_partition_message(raw: &str, display: &str, column: &str) -> String {
    format!(
        "[INVALID_PARTITION_VALUE] Failed to cast value '{raw}' to data type \"{display}\" for partition column `{column}`. Ensure the value matches the expected data type for this partition column. SQLSTATE: 42846"
    )
}

fn conflicting_names_message(
    lists: &[Vec<String>],
    dir_by_list: &HashMap<Vec<String>, PathBuf>,
) -> String {
    let mut message = String::from(
        "[CONFLICTING_PARTITION_COLUMN_NAMES] Conflicting partition column names detected:\n\n",
    );
    for (index, keys) in lists.iter().enumerate() {
        let _ = writeln!(
            message,
            "\tPartition column name list #{index}: {}",
            keys.join(", ")
        );
    }
    message.push_str(
        "\nFor partitioned table directories, data files should only live in leaf directories.\nAnd directories at the same level should have the same partition column name.\nPlease check the following directories for unexpected files or inconsistent partition column names:\n\n",
    );
    for (index, keys) in lists.iter().enumerate() {
        if index > 0 {
            message.push('\n');
        }
        if let Some(dir) = dir_by_list.get(keys) {
            let _ = write!(message, "\tfile:{}", dir.display());
        }
    }
    message.push_str(" SQLSTATE: KD009");
    message
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn discover_partitions(
    root: &Path,
    files: &[PathBuf],
    session_zone: &str,
) -> Result<DiscoveredPartitions, Error> {
    let mut seen_lists: Vec<Vec<String>> = Vec::new();
    let mut dir_by_list: HashMap<Vec<String>, PathBuf> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    let mut raws: HashMap<String, Vec<String>> = HashMap::new();
    let mut per_file: HashMap<PathBuf, HashMap<String, Option<String>>> = HashMap::new();
    for file in files {
        let mut seen: HashMap<String, Option<String>> = HashMap::new();
        let mut keys: Vec<String> = Vec::new();
        for (key, value) in partition_path_specs(root, file) {
            keys.push(key.clone());
            if !raws.contains_key(&key) {
                names.push(key.clone());
                raws.insert(key.clone(), Vec::new());
            }
            if let Some(text) = &value
                && let Some(texts) = raws.get_mut(&key)
            {
                texts.push(text.clone());
            }
            seen.insert(key, value);
        }
        if !keys.is_empty() && !dir_by_list.contains_key(&keys) {
            dir_by_list.insert(
                keys.clone(),
                file.parent()
                    .map_or_else(|| root.to_path_buf(), Path::to_path_buf),
            );
            seen_lists.push(keys);
        }
        per_file.insert(file.clone(), seen);
    }
    if seen_lists.len() > 1 {
        seen_lists.sort_by(|left, right| {
            left.len()
                .cmp(&right.len())
                .then(left.join(", ").cmp(&right.join(", ")))
        });
        return Err(Error::Iceberg(conflicting_names_message(
            &seen_lists,
            &dir_by_list,
        )));
    }
    let zone: Tz = session_zone.parse().map_err(|error| {
        Error::Analysis(format!(
            "text read cannot parse session time zone {session_zone:?}: {error}"
        ))
    })?;
    let mut fields = Vec::with_capacity(names.len());
    let mut types: HashMap<String, DataType> = HashMap::new();
    for name in &names {
        let empty = Vec::new();
        let candidates: Vec<&str> = raws
            .get(name)
            .unwrap_or(&empty)
            .iter()
            .map(String::as_str)
            .collect();
        let data_type = if candidates.is_empty() {
            DataType::Utf8
        } else {
            infer_partition_type(&candidates, session_zone)
        };
        fields.push(Field::new(name, data_type.clone(), true));
        types.insert(name.clone(), data_type);
    }
    let mut values: HashMap<PathBuf, Vec<PartitionValue>> = HashMap::new();
    let mut raw: HashMap<PathBuf, Vec<Option<String>>> = HashMap::new();
    for file in files {
        let empty = HashMap::new();
        let seen = per_file.get(file).unwrap_or(&empty);
        let mut row = Vec::with_capacity(names.len());
        let mut raw_row = Vec::with_capacity(names.len());
        for name in &names {
            let text = seen.get(name).cloned().flatten();
            let value = match text.as_ref() {
                None => PartitionValue::Null,
                Some(text) => parse_partition_value(text, &types[name], zone),
            };
            row.push(value);
            raw_row.push(text);
        }
        values.insert(file.clone(), row);
        raw.insert(file.clone(), raw_row);
    }
    Ok(DiscoveredPartitions {
        fields,
        values,
        raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc_zone() -> Tz {
        "UTC".parse().expect("UTC parses as a session zone")
    }

    fn leaf(root: &Path, segments: &[&str], name: &str) -> PathBuf {
        let mut path = root.to_path_buf();
        for segment in segments {
            path.push(segment);
        }
        path.push(name);
        path
    }

    #[test]
    fn partition_unescape_decodes_hex_and_keeps_broken() {
        assert_eq!(unescape_partition_value("a%2Fb"), "a/b");
        assert_eq!(unescape_partition_value("50%25"), "50%");
        assert_eq!(unescape_partition_value("c%3ad"), "c:d");
        assert_eq!(unescape_partition_value("100%"), "100%");
        assert_eq!(unescape_partition_value("x%zz"), "x%zz");
    }

    #[test]
    fn partition_discovery_reads_keys_in_directory_order() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["d=2024-01-02", "t=a"], "part-00000.txt"),
            leaf(&root, &["d=2024-01-03", "t=b"], "part-00000.txt"),
        ];
        let discovered = discover_partitions(&root, &files, "UTC").unwrap();
        assert_eq!(
            discovered
                .fields
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            vec![
                (String::from("d"), DataType::Date32),
                (String::from("t"), DataType::Utf8),
            ]
        );
        assert_eq!(
            discovered.values[&files[0]],
            vec![
                PartitionValue::Date32(19724),
                PartitionValue::Text(String::from("a")),
            ]
        );
    }

    #[test]
    fn partition_discovery_maps_default_to_null_and_infers_types() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(
                &root,
                &["k=__HIVE_DEFAULT_PARTITION__", "n=1.50", "b=true"],
                "part-00000.txt",
            ),
            leaf(&root, &["k=7", "n=2.50", "b=false"], "part-00001.txt"),
            leaf(&root, &["k=9", "n=1.50", "b=true"], "part-00000.txt"),
        ];
        let discovered = discover_partitions(&root, &files, "UTC").unwrap();
        assert_eq!(discovered.values[&files[0]][0], PartitionValue::Null);
        assert_eq!(discovered.values[&files[1]][0], PartitionValue::Int32(7));
        let names: Vec<String> = discovered
            .fields
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(
            names,
            vec![String::from("k"), String::from("n"), String::from("b")]
        );
        assert_eq!(
            discovered.values[&files[2]][1],
            PartitionValue::Float64(1.5)
        );
        assert_eq!(
            discovered.values[&files[2]][2],
            PartitionValue::Text(String::from("true"))
        );
    }

    #[test]
    fn partition_discovery_ignores_leaf_paths_without_base() {
        let root = PathBuf::from("/root/k=x");
        let files = vec![leaf(&root, &[], "part-00000.txt")];
        let discovered = discover_partitions(&root, &files, "UTC").unwrap();
        assert!(discovered.fields.is_empty());
        assert_eq!(discovered.values[&files[0]], Vec::new());
    }

    #[test]
    fn partition_discovery_prefers_bigint_over_double_for_long() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=3000000000"], "part-00000.txt"),
            leaf(&root, &["k=9"], "part-00001.txt"),
        ];
        let discovered = discover_partitions(&root, &files, "UTC").unwrap();
        assert_eq!(discovered.fields[0].data_type(), &DataType::Int64);
        assert_eq!(
            discovered.values[&files[0]][0],
            PartitionValue::Int64(3_000_000_000)
        );
    }

    #[test]
    fn partition_discovery_refuses_conflicting_names_at_same_depth() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=x"], "part-00000.txt"),
            leaf(&root, &["n=y"], "part-00000.txt"),
        ];
        let error = discover_partitions(&root, &files, "UTC").unwrap_err();
        let text = error.to_string();
        assert!(text.starts_with("[CONFLICTING_PARTITION_COLUMN_NAMES]"));
        assert!(text.contains("Partition column name list #0: k"));
        assert!(text.contains("Partition column name list #1: n"));
        assert!(text.contains("SQLSTATE: KD009"));
    }

    #[test]
    fn partition_discovery_refuses_uneven_depth_name_lists() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=x", "n=1"], "part-00000.txt"),
            leaf(&root, &["k=y"], "part-00000.txt"),
        ];
        let error = discover_partitions(&root, &files, "UTC").unwrap_err();
        let text = error.to_string();
        assert!(text.starts_with("[CONFLICTING_PARTITION_COLUMN_NAMES]"));
        assert!(text.contains("Partition column name list #0: k"));
        assert!(text.contains("Partition column name list #1: k, n"));
        assert!(text.contains("SQLSTATE: KD009"));
    }

    #[test]
    fn partition_discovery_refuses_nonleaf_data_file_name_lists() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=x"], "part-00000.txt"),
            leaf(&root, &["k=x", "n=1"], "part-00000.txt"),
        ];
        let error = discover_partitions(&root, &files, "UTC").unwrap_err();
        let text = error.to_string();
        assert!(text.starts_with("[CONFLICTING_PARTITION_COLUMN_NAMES]"));
        assert!(text.contains("Partition column name list #0: k"));
        assert!(text.contains("Partition column name list #1: k, n"));
        assert!(text.contains("SQLSTATE: KD009"));
    }

    #[test]
    fn partition_user_type_maps_probe_shapes() {
        assert_eq!(
            user_partition_type("string", "UTC"),
            Some((DataType::Utf8, String::from("STRING")))
        );
        assert_eq!(
            user_partition_type("int", "UTC"),
            Some((DataType::Int32, String::from("INT")))
        );
        assert_eq!(
            user_partition_type("bigint", "UTC"),
            Some((DataType::Int64, String::from("BIGINT")))
        );
        assert_eq!(
            user_partition_type("double", "UTC"),
            Some((DataType::Float64, String::from("DOUBLE")))
        );
        assert_eq!(
            user_partition_type("date", "UTC"),
            Some((DataType::Date32, String::from("DATE")))
        );
        assert_eq!(
            user_partition_type("timestamp", "UTC"),
            Some((
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                String::from("TIMESTAMP")
            ))
        );
        assert_eq!(
            user_partition_type("decimal(10,2)", "UTC"),
            Some((DataType::Decimal128(10, 2), String::from("DECIMAL(10,2)")))
        );
        assert_eq!(user_partition_type("decimal(0,0)", "UTC"), None);
        assert_eq!(user_partition_type("decimal(10,11)", "UTC"), None);
        assert_eq!(
            cast_raw_partition_value("007", &DataType::Int32, utc_zone()),
            Some(PartitionValue::Int32(7))
        );
        assert_eq!(
            cast_raw_partition_value("+7", &DataType::Int32, utc_zone()),
            Some(PartitionValue::Int32(7))
        );
        assert_eq!(
            cast_raw_partition_value("y", &DataType::Int32, utc_zone()),
            None
        );
        assert_eq!(
            cast_raw_partition_value("007", &DataType::Utf8, utc_zone()),
            Some(PartitionValue::Text(String::from("007")))
        );
        assert_eq!(
            cast_raw_partition_value("1.50", &DataType::Decimal128(10, 2), utc_zone()),
            Some(PartitionValue::Decimal128(150, 2))
        );
        assert_eq!(
            cast_raw_partition_value("2024-01-02", &DataType::Date32, utc_zone()),
            Some(PartitionValue::Date32(19724))
        );
        assert_eq!(
            cast_raw_partition_value(
                "2024-01-02",
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                utc_zone()
            ),
            Some(PartitionValue::TimestampMicros(1_704_153_600_000_000))
        );
        assert_eq!(parse_decimal_scaled("1.50", 10, 2), Some(150));
        assert_eq!(parse_decimal_scaled("007", 10, 0), Some(7));
        assert_eq!(parse_decimal_scaled("1.567", 10, 2), None);
        assert_eq!(
            parse_timestamp_micros_zone("2024-01-02", utc_zone()),
            Some(1_704_153_600_000_000)
        );
        assert_eq!(
            invalid_partition_message("y", "INT", "k"),
            "[INVALID_PARTITION_VALUE] Failed to cast value 'y' to data type \"INT\" for partition column `k`. Ensure the value matches the expected data type for this partition column. SQLSTATE: 42846"
        );
    }

    #[test]
    fn partition_user_type_maps_probe6_shapes() {
        assert_eq!(
            user_partition_type("boolean", "UTC"),
            Some((DataType::Boolean, String::from("BOOLEAN")))
        );
        assert_eq!(
            user_partition_type("float", "UTC"),
            Some((DataType::Float32, String::from("FLOAT")))
        );
        assert_eq!(
            user_partition_type("smallint", "UTC"),
            Some((DataType::Int16, String::from("SMALLINT")))
        );
        assert_eq!(
            user_partition_type("tinyint", "UTC"),
            Some((DataType::Int8, String::from("TINYINT")))
        );
        assert_eq!(
            user_partition_type("binary", "UTC"),
            Some((DataType::Binary, String::from("BINARY")))
        );
        assert_eq!(
            user_partition_type("timestamp_ntz", "UTC"),
            Some((
                DataType::Timestamp(TimeUnit::Microsecond, None),
                String::from("TIMESTAMP_NTZ")
            ))
        );
        assert_eq!(
            user_partition_type("array<int>", "UTC"),
            Some((
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                String::from("ARRAY<INT>")
            ))
        );
        assert_eq!(user_partition_type("map<string,int>", "UTC"), None);
        assert_eq!(user_partition_type("struct<x:int>", "UTC"), None);
        assert_eq!(user_partition_type("array<array<int>>", "UTC"), None);
        assert_eq!(
            cast_raw_partition_value("TRUE", &DataType::Boolean, utc_zone()),
            Some(PartitionValue::Boolean(true))
        );
        assert_eq!(
            cast_raw_partition_value("false", &DataType::Boolean, utc_zone()),
            Some(PartitionValue::Boolean(false))
        );
        assert_eq!(
            cast_raw_partition_value("yes", &DataType::Boolean, utc_zone()),
            None
        );
        assert_eq!(
            cast_raw_partition_value("1.50", &DataType::Float32, utc_zone()),
            Some(PartitionValue::Float32(1.5))
        );
        assert_eq!(
            cast_raw_partition_value("1.50", &DataType::Int16, utc_zone()),
            None
        );
        assert_eq!(
            cast_raw_partition_value("7", &DataType::Int16, utc_zone()),
            Some(PartitionValue::Int16(7))
        );
        assert_eq!(
            cast_raw_partition_value("7", &DataType::Int8, utc_zone()),
            Some(PartitionValue::Int8(7))
        );
        assert_eq!(
            cast_raw_partition_value("7", &DataType::Binary, utc_zone()),
            Some(PartitionValue::Binary(vec![b'7']))
        );
        assert_eq!(
            cast_raw_partition_value(
                "7",
                &DataType::Timestamp(TimeUnit::Microsecond, None),
                utc_zone()
            ),
            None
        );
        assert_eq!(
            cast_raw_partition_value(
                "7",
                &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                utc_zone()
            ),
            None
        );
    }
}
