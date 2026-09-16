use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, AsArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};

use crate::datetime::localize_wall_micros_in_zone;
use crate::java_datetime::{ParsedPattern, compile_java_pattern, parse_wall_text};

pub(crate) mod fold;
pub(crate) mod from_csv;
pub(crate) mod schema_of_csv;

pub(crate) const DEFAULT_SEP: char = ',';
pub(crate) const DEFAULT_QUOTE: char = '"';
pub(crate) const DEFAULT_ESCAPE: char = '\\';

#[derive(Clone, Debug)]
pub(crate) struct CsvOptions {
    pub(crate) sep: char,
    pub(crate) quote: char,
    pub(crate) escape: char,
    pub(crate) null_value: String,
    pub(crate) date_format: Option<String>,
    pub(crate) timestamp_format: Option<String>,
    pub(crate) mode: CsvMode,
    pub(crate) corrupt_record_column: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum CsvMode {
    #[default]
    Permissive,
    FailFast,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            sep: DEFAULT_SEP,
            quote: DEFAULT_QUOTE,
            escape: DEFAULT_ESCAPE,
            null_value: String::new(),
            date_format: None,
            timestamp_format: None,
            mode: CsvMode::Permissive,
            corrupt_record_column: None,
        }
    }
}

fn single_char(name: &str, value: &str) -> Result<char> {
    let mut chars = value.chars();
    if let (Some(found), None) = (chars.next(), chars.next()) {
        Ok(found)
    } else {
        plan_err!(
            "[INVALID_OPTIONS.WRONG_OPTION_VALUE] Incorrect option value '{value}' for option '{name}'."
        )
    }
}

fn is_string_array(array: &datafusion::arrow::array::ArrayRef) -> bool {
    matches!(
        array.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn utf8_value(array: &datafusion::arrow::array::ArrayRef, row: usize) -> String {
    match array.data_type() {
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_string(),
        DataType::Utf8View => array.as_string_view().value(row).to_string(),
        _ => array.as_string::<i32>().value(row).to_string(),
    }
}

fn map_entries(scalar: &ScalarValue) -> Result<HashMap<String, String>> {
    let ScalarValue::Map(map) = scalar else {
        return plan_err!("CSV options arrive as a map literal");
    };
    if map.is_empty() {
        return Ok(HashMap::new());
    }
    let entries = map.value(0);
    string_map_entries(entries.column(0), entries.column(1))
}

pub(crate) fn string_map_entries(
    keys: &datafusion::arrow::array::ArrayRef,
    values: &datafusion::arrow::array::ArrayRef,
) -> Result<HashMap<String, String>> {
    if !is_string_array(keys) || !is_string_array(values) {
        return plan_err!(
            "[INVALID_OPTIONS.NON_STRING_TYPE] The options expression must be a map of strings."
        );
    }
    let mut out = HashMap::with_capacity(keys.len());
    for row in 0..keys.len() {
        if keys.is_null(row) || values.is_null(row) {
            continue;
        }
        out.insert(utf8_value(keys, row), utf8_value(values, row));
    }
    Ok(out)
}

pub(crate) fn options_from_map(scalar: &ScalarValue) -> Result<CsvOptions> {
    let entries = map_entries(scalar)?;
    options_from_entries(&entries)
}

pub(crate) fn non_string_literal_schema() -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(
        "[INVALID_SCHEMA.NON_STRING_LITERAL] The input schema is not a valid schema string. The \
         input expression must be string literal and not null."
            .to_string(),
    )
}

pub(crate) fn options_from_entries(entries: &HashMap<String, String>) -> Result<CsvOptions> {
    let mut options = CsvOptions::default();
    for (name, value) in entries {
        match name.as_str() {
            "sep" | "delimiter" => options.sep = single_char(name, value)?,
            "quote" => options.quote = single_char(name, value)?,
            "escape" => options.escape = single_char(name, value)?,
            "nullValue" => options.null_value.clone_from(value),
            "dateFormat" => options.date_format = Some(value.clone()),
            "timestampFormat" => options.timestamp_format = Some(value.clone()),
            "mode" => {
                options.mode = match value.to_ascii_uppercase().as_str() {
                    "PERMISSIVE" => CsvMode::Permissive,
                    "FAILFAST" => CsvMode::FailFast,
                    _ => {
                        return plan_err!(
                            "[PARSE_MODE_UNSUPPORTED] The parse mode '{value}' is not supported."
                        );
                    }
                };
            }
            "columnNameOfCorruptRecord" => options.corrupt_record_column = Some(value.clone()),
            _ => {}
        }
    }
    Ok(options)
}

fn take_owned(current: &mut String, owned: &mut bool, text: &str, start: usize, index: usize) {
    if !*owned {
        current.push_str(&text[start..index]);
        *owned = true;
    }
}

pub(crate) fn split_csv_record<'text>(
    text: &'text str,
    options: &CsvOptions,
) -> Vec<Cow<'text, str>> {
    let mut tokens: Vec<Cow<'_, str>> = Vec::new();
    let mut current = String::new();
    let mut owned = false;
    let mut start = 0usize;
    let mut quoted = false;
    let mut chars = text.char_indices().peekable();
    while let Some((index, found)) = chars.next() {
        if quoted {
            if found == options.escape {
                if let Some((_, escaped)) = chars.next() {
                    take_owned(&mut current, &mut owned, text, start, index);
                    current.push(escaped);
                }
            } else if found == options.quote {
                if chars.peek().map(|(_, next)| *next) == Some(options.quote) {
                    chars.next();
                    take_owned(&mut current, &mut owned, text, start, index);
                    current.push(options.quote);
                } else {
                    take_owned(&mut current, &mut owned, text, start, index);
                    quoted = false;
                }
            } else {
                take_owned(&mut current, &mut owned, text, start, index);
                current.push(found);
            }
        } else if found == options.sep {
            if owned {
                tokens.push(Cow::Owned(std::mem::take(&mut current)));
                owned = false;
            } else {
                tokens.push(Cow::Borrowed(&text[start..index]));
            }
            start = index + found.len_utf8();
        } else if found == options.quote {
            take_owned(&mut current, &mut owned, text, start, index);
            quoted = true;
        } else if found == options.escape {
            take_owned(&mut current, &mut owned, text, start, index);
            if let Some((_, escaped)) = chars.next() {
                current.push(escaped);
            }
        } else if owned {
            current.push(found);
        }
    }
    if owned {
        tokens.push(Cow::Owned(current));
    } else {
        tokens.push(Cow::Borrowed(&text[start..]));
    }
    tokens
}

pub(crate) fn token_is_null(token: &str, options: &CsvOptions) -> bool {
    token.is_empty() || (!options.null_value.is_empty() && token == options.null_value)
}

#[derive(Clone, Copy, Debug, Default)]
struct DateParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
}

pub(crate) fn parse_csv_date(text: &str, format: Option<&str>) -> Option<chrono::NaiveDate> {
    let parts = parse_csv_datetime(text, format)?;
    chrono::NaiveDate::from_ymd_opt(parts.year, parts.month, parts.day)
}

pub(crate) fn parse_csv_timestamp(text: &str, format: Option<&str>) -> Option<i64> {
    let parts = parse_csv_datetime(text, format)?;
    let date = chrono::NaiveDate::from_ymd_opt(parts.year, parts.month, parts.day)?;
    let time = chrono::NaiveTime::from_hms_opt(parts.hour, parts.minute, parts.second)?;
    Some(date.and_time(time).and_utc().timestamp_micros())
}

fn parse_csv_datetime(text: &str, format: Option<&str>) -> Option<DateParts> {
    let pattern = format.unwrap_or("yyyy-MM-dd");
    let mut parts = DateParts {
        month: 1,
        day: 1,
        ..DateParts::default()
    };
    let mut text_bytes = text.as_bytes();
    let mut pattern_chars = pattern.chars().peekable();
    while let Some(found) = pattern_chars.next() {
        if found == '\'' {
            let mut literal = String::new();
            for next in pattern_chars.by_ref() {
                if next == '\'' {
                    break;
                }
                literal.push(next);
            }
            if !text_bytes.starts_with(literal.as_bytes()) {
                return None;
            }
            text_bytes = &text_bytes[literal.len()..];
        } else if "yMdHms".contains(found) {
            let mut width = 1;
            while pattern_chars.peek() == Some(&found) {
                pattern_chars.next();
                width += 1;
            }
            let take = match found {
                'y' => width.max(4),
                _ => width,
            };
            if text_bytes.len() < take {
                return None;
            }
            let (digits, rest) = text_bytes.split_at(take);
            let digits = std::str::from_utf8(digits).ok()?;
            let value: i32 = digits.parse().ok()?;
            match found {
                'y' => parts.year = value,
                'M' => parts.month = u32::try_from(value).ok()?,
                'd' => parts.day = u32::try_from(value).ok()?,
                'H' => parts.hour = u32::try_from(value).ok()?,
                'm' => parts.minute = u32::try_from(value).ok()?,
                's' => parts.second = u32::try_from(value).ok()?,
                _ => return None,
            }
            text_bytes = rest;
        } else {
            let need = found.len_utf8();
            if text_bytes.len() < need {
                return None;
            }
            let (head, rest) = text_bytes.split_at(need);
            if head != found.to_string().as_bytes() {
                return None;
            }
            text_bytes = rest;
        }
    }
    if !text_bytes.is_empty() {
        return None;
    }
    Some(parts)
}

pub(crate) struct CsvStampParsers {
    pub(crate) date: Option<ParsedPattern>,
    pub(crate) timestamp: Option<ParsedPattern>,
    pub(crate) zone: Tz,
}

#[must_use]
pub(crate) fn stamp_parsers(options: &CsvOptions, zone: Tz) -> CsvStampParsers {
    CsvStampParsers {
        date: options
            .date_format
            .as_deref()
            .and_then(|pattern| compile_java_pattern(pattern).ok()),
        timestamp: options
            .timestamp_format
            .as_deref()
            .and_then(|pattern| compile_java_pattern(pattern).ok()),
        zone,
    }
}

fn stamp_micros(naive: chrono::NaiveDateTime, zone: Option<Tz>) -> Option<i64> {
    let wall = naive.and_utc().timestamp_micros();
    match zone {
        Some(zone) => localize_wall_micros_in_zone(wall, zone),
        None => Some(wall),
    }
}

const DEFAULT_STAMP_OFFSET: [&str; 2] = ["%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%.f%#z"];

const DEFAULT_STAMP_NAIVE: [&str; 4] = [
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M",
];

pub(crate) fn default_timestamp_micros(text: &str, zone: Option<Tz>) -> Option<i64> {
    for pattern in DEFAULT_STAMP_OFFSET {
        if let Ok(found) = chrono::DateTime::parse_from_str(text, pattern) {
            return Some(found.timestamp_micros());
        }
    }
    for pattern in DEFAULT_STAMP_NAIVE {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(text, pattern) {
            return stamp_micros(naive, zone);
        }
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .and_then(|naive| stamp_micros(naive, zone));
    }
    None
}

#[must_use]
pub(crate) fn infers_as_timestamp(text: &str) -> bool {
    DEFAULT_STAMP_OFFSET
        .iter()
        .any(|pattern| chrono::DateTime::parse_from_str(text, pattern).is_ok())
        || DEFAULT_STAMP_NAIVE
            .iter()
            .any(|pattern| chrono::NaiveDateTime::parse_from_str(text, pattern).is_ok())
}

pub(crate) fn parse_dated_token(
    token: &str,
    options: &CsvOptions,
    parsers: &CsvStampParsers,
) -> Option<chrono::NaiveDate> {
    if let Some(pattern) = parsers.date.as_ref() {
        return parse_wall_text(token, pattern)
            .ok()
            .and_then(|wall| wall.to_naive())
            .map(|naive| naive.date());
    }
    parse_csv_date(token, options.date_format.as_deref())
}

pub(crate) fn parse_stamp_token(
    token: &str,
    field_zone: Option<&Arc<str>>,
    options: &CsvOptions,
    parsers: &CsvStampParsers,
) -> Option<i64> {
    let zone = field_zone.map(|_| parsers.zone);
    if let Some(pattern) = parsers.timestamp.as_ref() {
        return parse_wall_text(token, pattern)
            .ok()
            .and_then(|wall| wall.to_naive())
            .and_then(|naive| stamp_micros(naive, zone));
    }
    if let Some(format) = options.timestamp_format.as_deref() {
        return parse_csv_timestamp(token, Some(format));
    }
    default_timestamp_micros(token, zone)
}

pub(crate) fn read_options_array(array: &datafusion::arrow::array::ArrayRef) -> Result<CsvOptions> {
    let scalar = ScalarValue::try_from_array(array, 0)?;
    if !matches!(scalar, ScalarValue::Map(_)) {
        return plan_err!(
            "[INVALID_OPTIONS.NON_MAP_FUNCTION] Must use the `map()` function for options."
        );
    }
    let raw = map_entries(&scalar)?;
    options_from_entries(&raw)
}

pub(crate) fn string_column(array: &datafusion::arrow::array::ArrayRef) -> Result<StringArray> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().clone()),
        DataType::Null => Ok(StringArray::new_null(array.len())),
        _ => {
            let cast = datafusion::arrow::compute::cast(array.as_ref(), &DataType::Utf8)?;
            Ok(cast.as_string::<i32>().clone())
        }
    }
}

pub(crate) fn unsupported_datatype(
    field: &str,
    data_type: &DataType,
) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::NotImplemented(format!(
        "[UNSUPPORTED_DATATYPE] Unsupported datatype: the CSV data source does not support data type {}, field name: {field}. SQLSTATE: 0A000",
        render_complex_type(data_type)
    ))
}

fn render_complex_type(data_type: &DataType) -> String {
    match data_type {
        DataType::List(field) => format!("ARRAY<{}>", render_complex_type(field.data_type())),
        DataType::LargeList(field) => {
            format!("ARRAY<{}>", render_complex_type(field.data_type()))
        }
        DataType::Map(entry, _) => match entry.data_type() {
            DataType::Struct(members) if members.len() == 2 => format!(
                "MAP<{}, {}>",
                render_complex_type(members[0].data_type()),
                render_complex_type(members[1].data_type())
            ),
            _ => "MAP<STRING, STRING>".to_string(),
        },
        DataType::Struct(members) => {
            let rendered = members
                .iter()
                .map(|member| {
                    format!(
                        "{}: {}",
                        member.name(),
                        render_complex_type(member.data_type())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT<{rendered}>")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 | DataType::UInt32 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".to_string(),
        other => format!("{other:?}").to_ascii_uppercase(),
    }
}

pub(crate) fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Null => "VOID".to_string(),
        _ => render_complex_type(data_type),
    }
}

pub(crate) fn functions() -> Vec<Arc<datafusion::logical_expr::ScalarUDF>> {
    vec![from_csv::from_csv_udf(), schema_of_csv::schema_of_csv_udf()]
}
