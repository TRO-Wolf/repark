use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};

pub(crate) mod fold;
pub(crate) mod from_csv;

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
    match (chars.next(), chars.next()) {
        (Some(found), None) => Ok(found),
        _ => plan_err!(
            "[INVALID_OPTIONS.WRONG_OPTION_VALUE] Incorrect option value '{value}' for option '{name}'."
        ),
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

pub(crate) fn options_from_entries(entries: &HashMap<String, String>) -> Result<CsvOptions> {
    let mut options = CsvOptions::default();
    for (name, value) in entries {
        match name.as_str() {
            "sep" | "delimiter" => options.sep = single_char(name, value)?,
            "quote" => options.quote = single_char(name, value)?,
            "escape" => options.escape = single_char(name, value)?,
            "nullValue" => options.null_value = value.clone(),
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

pub(crate) fn split_csv_record(text: &str, options: &CsvOptions) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut chars = text.chars().peekable();
    let mut quoted = false;
    while let Some(found) = chars.next() {
        if quoted {
            if found == options.escape {
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                }
            } else if found == options.quote {
                if chars.peek() == Some(&options.quote) {
                    chars.next();
                    current.push(options.quote);
                } else {
                    quoted = false;
                }
            } else {
                current.push(found);
            }
        } else if found == options.sep {
            tokens.push(std::mem::take(&mut current));
        } else if found == options.quote {
            quoted = true;
        } else if found == options.escape {
            if let Some(escaped) = chars.next() {
                current.push(escaped);
            }
        } else {
            current.push(found);
        }
    }
    tokens.push(current);
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
                'M' => parts.month = value as u32,
                'd' => parts.day = value as u32,
                'H' => parts.hour = value as u32,
                'm' => parts.minute = value as u32,
                's' => parts.second = value as u32,
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
        DataType::Utf8 => "STRING".to_string(),
        DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
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
    vec![from_csv::from_csv_udf()]
}
