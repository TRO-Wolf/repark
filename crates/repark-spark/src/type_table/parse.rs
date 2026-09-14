use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock, PoisonError};

use regex::Regex;

use std::borrow::Cow;

use super::{DEFAULT_COLLATION, SparkDataType, SparkField, TypeTableError};

fn is_py_whitespace(character: char) -> bool {
    character.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&character) || character == '\u{85}'
}

fn py_trim(text: &str) -> &str {
    text.trim_matches(is_py_whitespace)
}

fn decimal_digit_value(character: char) -> Option<u32> {
    const RANGES: &[(u32, u32)] = &[
        (0x30, 0x39),
        (0x660, 0x669),
        (0x6F0, 0x6F9),
        (0x7C0, 0x7C9),
        (0x966, 0x96F),
        (0x9E6, 0x9EF),
        (0xA66, 0xA6F),
        (0xAE6, 0xAEF),
        (0xB66, 0xB6F),
        (0xBE6, 0xBEF),
        (0xC66, 0xC6F),
        (0xCE6, 0xCEF),
        (0xD66, 0xD6F),
        (0xDE6, 0xDEF),
        (0xE50, 0xE59),
        (0xED0, 0xED9),
        (0xF20, 0xF29),
        (0x1040, 0x1049),
        (0x1090, 0x1099),
        (0x17E0, 0x17E9),
        (0x1810, 0x1819),
        (0x1946, 0x194F),
        (0x19D0, 0x19D9),
        (0x1A80, 0x1A89),
        (0x1A90, 0x1A99),
        (0x1B50, 0x1B59),
        (0x1BB0, 0x1BB9),
        (0x1C40, 0x1C49),
        (0x1C50, 0x1C59),
        (0xA620, 0xA629),
        (0xA8D0, 0xA8D9),
        (0xA900, 0xA909),
        (0xA9D0, 0xA9D9),
        (0xA9F0, 0xA9F9),
        (0xAA50, 0xAA59),
        (0xABF0, 0xABF9),
        (0xFF10, 0xFF19),
        (0x104A0, 0x104A9),
        (0x10D30, 0x10D39),
        (0x11066, 0x1106F),
        (0x110F0, 0x110F9),
        (0x11136, 0x1113F),
        (0x111D0, 0x111D9),
        (0x112F0, 0x112F9),
        (0x11450, 0x11459),
        (0x114D0, 0x114D9),
        (0x11650, 0x11659),
        (0x116C0, 0x116C9),
        (0x11730, 0x11739),
        (0x118E0, 0x118E9),
        (0x11950, 0x11959),
        (0x11C50, 0x11C59),
        (0x11D50, 0x11D59),
        (0x11DA0, 0x11DA9),
        (0x16A60, 0x16A69),
        (0x16AC0, 0x16AC9),
        (0x16B50, 0x16B59),
        (0x1D7CE, 0x1D7FF),
        (0x1E140, 0x1E149),
        (0x1E2F0, 0x1E2F9),
        (0x1E950, 0x1E959),
        (0x1FBF0, 0x1FBF9),
    ];
    let code = u32::from(character);
    RANGES
        .iter()
        .find(|(start, end)| (*start..=*end).contains(&code))
        .map(|(start, _)| code - start)
}

fn parse_py_int(text: &str) -> Result<i64, TypeTableError> {
    let invalid = || {
        TypeTableError::Message(format!(
            "invalid literal for int() with base 10: {}",
            python_repr(text)
        ))
    };
    let mut value: i64 = 0;
    let mut negative = false;
    let mut digits_seen = false;
    let mut expect_digit = true;
    let mut chars = text.chars().peekable();
    if let Some(&first) = chars.peek()
        && (first == '+' || first == '-')
    {
        negative = first == '-';
        chars.next();
    }
    for character in chars {
        if character == '_' {
            if !digits_seen || expect_digit {
                return Err(invalid());
            }
            expect_digit = true;
            continue;
        }
        let Some(digit) = decimal_digit_value(character) else {
            return Err(invalid());
        };
        digits_seen = true;
        expect_digit = false;
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(digit)))
            .ok_or(TypeTableError::IntegerOverflow)?;
    }
    if !digits_seen || expect_digit {
        return Err(invalid());
    }
    Ok(if negative { -value } else { value })
}

#[must_use]
fn python_repr(text: &str) -> String {
    let escaped = text
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    if !escaped.contains('\'') {
        format!("'{escaped}'")
    } else if !escaped.contains('"') {
        format!("\"{escaped}\"")
    } else {
        format!("'{}'", escaped.replace('\'', "\\'"))
    }
}

fn regex_fullmatch<'a>(pattern: &'static str, text: &'a str) -> Option<regex::Captures<'a>> {
    static CACHE: OnceLock<Mutex<BTreeMap<&'static str, Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = cache.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(regex) = guard.get(pattern) {
        return regex.captures(text);
    }
    let regex = Regex::new(pattern).ok()?;
    let captures = regex.captures(text);
    guard.insert(pattern, regex);
    captures
}

fn atomic_type_from_name(lower: &str) -> Option<SparkDataType> {
    Some(match lower {
        "string" | "str" | "varchar" => SparkDataType::SparkString {
            collation: Cow::Borrowed(DEFAULT_COLLATION),
        },
        "binary" => SparkDataType::Binary,
        "boolean" | "bool" => SparkDataType::Boolean,
        "byte" | "tinyint" => SparkDataType::Byte,
        "short" | "smallint" => SparkDataType::Short,
        "int" | "integer" => SparkDataType::Integer,
        "long" | "bigint" => SparkDataType::Long,
        "float" | "real" => SparkDataType::Float,
        "double" => SparkDataType::Double,
        "date" => SparkDataType::Date,
        "timestamp" => SparkDataType::Timestamp,
        "timestamp_ntz" => SparkDataType::TimestampNtz,
        "void" | "null" => SparkDataType::Null,
        "variant" => SparkDataType::Variant,
        "interval" | "calendarinterval" => SparkDataType::CalendarInterval,
        _ => return None,
    })
}

fn atomic_type_names_contains(lower: &str) -> bool {
    atomic_type_from_name(lower).is_some()
}

fn parse_atomic_token(token: &str) -> Result<Option<SparkDataType>, TypeTableError> {
    let stripped = py_trim(token);
    if stripped.is_empty() {
        return Ok(None);
    }
    let lower = stripped.to_lowercase();
    if let Some(data_type) = atomic_type_from_name(lower.as_str()) {
        return Ok(Some(data_type));
    }
    if let Some(captures) =
        regex_fullmatch(r"(?i)\Adecimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\z", stripped)
    {
        let (Some(precision_match), Some(scale_match)) = (captures.get(1), captures.get(2)) else {
            return Ok(None);
        };
        let precision = parse_py_int(precision_match.as_str())?;
        let scale = parse_py_int(scale_match.as_str())?;
        return Ok(Some(SparkDataType::Decimal { precision, scale }));
    }
    if let Some(captures) = regex_fullmatch(r"(?i)\Achar\s*\(\s*(\d+)\s*\)\z", stripped) {
        let Some(length_match) = captures.get(1) else {
            return Ok(None);
        };
        let length = parse_py_int(length_match.as_str())?;
        return Ok(Some(SparkDataType::Char { length }));
    }
    if let Some(captures) = regex_fullmatch(r"(?i)\Avarchar\s*\(\s*(\d+)\s*\)\z", stripped) {
        let Some(length_match) = captures.get(1) else {
            return Ok(None);
        };
        let length = parse_py_int(length_match.as_str())?;
        return Ok(Some(SparkDataType::Varchar { length }));
    }
    if let Some(captures) = regex_fullmatch(r"(?i)\Atime\s*\(\s*(\d+)\s*\)\z", stripped) {
        let Some(precision_match) = captures.get(1) else {
            return Ok(None);
        };
        let precision = parse_py_int(precision_match.as_str())?;
        return Ok(Some(SparkDataType::Time { precision }));
    }
    if let Some(captures) = regex_fullmatch(r"(?i)\Astring\s+collate\s+(\w+)\z", stripped) {
        let Some(collation_match) = captures.get(1) else {
            return Ok(None);
        };
        let collation = Cow::Owned(collation_match.as_str().to_string());
        return Ok(Some(SparkDataType::SparkString { collation }));
    }
    if lower == "decimal" {
        return Ok(Some(SparkDataType::Decimal {
            precision: 10,
            scale: 0,
        }));
    }
    if lower.starts_with("time") {
        return Ok(Some(SparkDataType::Time { precision: 6 }));
    }
    Ok(None)
}

fn split_top_level(text: &str, separator: char) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut depth_angle = 0i32;
    let mut depth_paren = 0i32;
    let mut start = 0usize;
    for (index, character) in text.char_indices() {
        if character == '<' {
            depth_angle += 1;
        } else if character == '>' {
            depth_angle = (depth_angle - 1).max(0);
        } else if character == '(' {
            depth_paren += 1;
        } else if character == ')' {
            depth_paren = (depth_paren - 1).max(0);
        } else if character == separator && depth_angle == 0 && depth_paren == 0 {
            parts.push(text[start..index].to_string());
            start = index + character.len_utf8();
        }
    }
    parts.push(text[start..].to_string());
    parts
}

fn split_whitespace_once(text: &str) -> Option<(&str, &str)> {
    for (offset, character) in text.char_indices() {
        if is_py_whitespace(character) {
            let name_end = offset;
            let mut index = offset + character.len_utf8();
            while index < text.len() {
                let next = text[index..].chars().next();
                match next {
                    Some(c) if is_py_whitespace(c) => index += c.len_utf8(),
                    _ => break,
                }
            }
            if index >= text.len() {
                return None;
            }
            return Some((&text[..name_end], &text[index..]));
        }
    }
    None
}

fn strip_quotes(name: &str) -> &str {
    name.trim_matches(|c| c == '`' || c == '"')
}

fn parse_complex_or_atomic(text: &str) -> Result<SparkDataType, TypeTableError> {
    let stripped = py_trim(text);
    let lower = stripped.to_lowercase();
    if lower.starts_with("array<") && stripped.ends_with('>') {
        let inner = &stripped[6..stripped.len() - 1];
        return Ok(SparkDataType::Array {
            element: Box::new(parse_complex_or_atomic(inner)?),
            contains_null: true,
        });
    }
    if lower.starts_with("map<") && stripped.ends_with('>') {
        let inner = &stripped[4..stripped.len() - 1];
        let parts = split_top_level(inner, ',');
        if parts.len() != 2 {
            return Err(TypeTableError::Message(format!(
                "cannot parse map type: {}",
                python_repr(text)
            )));
        }
        return Ok(SparkDataType::Map {
            key: Box::new(parse_complex_or_atomic(&parts[0])?),
            value: Box::new(parse_complex_or_atomic(&parts[1])?),
            value_contains_null: true,
        });
    }
    if lower.starts_with("struct<") && stripped.ends_with('>') {
        let inner = py_trim(&stripped[7..stripped.len() - 1]);
        if inner.is_empty() {
            return Ok(SparkDataType::Struct(Vec::new()));
        }
        let mut fields: Vec<SparkField> = Vec::new();
        for part in split_top_level(inner, ',') {
            let part = py_trim(&part);
            if part.is_empty() {
                continue;
            }
            let (name, type_text) = if let Some(colon) = part.find(':') {
                (&part[..colon], &part[colon + 1..])
            } else {
                let Some((name, type_text)) = split_whitespace_once(part) else {
                    return Err(TypeTableError::Message(format!(
                        "cannot parse struct field: {}",
                        python_repr(part)
                    )));
                };
                (name, type_text)
            };
            fields.push(SparkField {
                name: strip_quotes(py_trim(name)).to_string(),
                data_type: parse_complex_or_atomic(type_text)?,
                nullable: true,
                metadata: None,
            });
        }
        return Ok(SparkDataType::Struct(fields));
    }
    if let Some(atomic) = parse_atomic_token(stripped)? {
        return Ok(atomic);
    }
    Err(TypeTableError::Message(format!(
        "cannot parse datatype: {}",
        python_repr(text)
    )))
}

fn parse_field_list(text: &str) -> Result<SparkDataType, TypeTableError> {
    let mut fields: Vec<SparkField> = Vec::new();
    for part in split_top_level(text, ',') {
        let part = py_trim(&part);
        if part.is_empty() {
            continue;
        }
        let (name, type_text) =
            if let Some(captures) = regex_fullmatch(r"\A([A-Za-z_]\w*)\s*:\s*(.+)\z", part) {
                let name = captures.get(1).map(|m| m.as_str()).unwrap_or_default();
                let type_text = captures.get(2).map(|m| m.as_str()).unwrap_or_default();
                (
                    strip_quotes(py_trim(name)).to_string(),
                    type_text.to_string(),
                )
            } else {
                let Some((name, type_text)) = split_whitespace_once(part) else {
                    return Err(TypeTableError::Message(format!(
                        "cannot parse field: {}",
                        python_repr(part)
                    )));
                };
                (
                    strip_quotes(py_trim(name)).to_string(),
                    type_text.to_string(),
                )
            };
        fields.push(SparkField {
            name,
            data_type: parse_complex_or_atomic(&type_text)?,
            nullable: true,
            metadata: None,
        });
    }
    Ok(SparkDataType::Struct(fields))
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_ddl(text: &str) -> Result<SparkDataType, TypeTableError> {
    let stripped = py_trim(text);
    if stripped.is_empty() {
        return Ok(SparkDataType::Struct(Vec::new()));
    }
    let lower = stripped.to_lowercase();
    let parameterized = regex_fullmatch(r"(?i)\Adecimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\z", stripped)
        .is_some()
        || regex_fullmatch(r"(?i)\Achar\s*\(\s*(\d+)\s*\)\z", stripped).is_some()
        || regex_fullmatch(r"(?i)\Avarchar\s*\(\s*(\d+)\s*\)\z", stripped).is_some()
        || regex_fullmatch(r"(?i)\Atime\s*\(\s*(\d+)\s*\)\z", stripped).is_some()
        || regex_fullmatch(r"(?i)\Astring\s+collate\s+(\w+)\z", stripped).is_some();
    if parameterized
        || lower.starts_with("array<")
        || lower.starts_with("map<")
        || lower.starts_with("struct<")
        || atomic_type_names_contains(lower.as_str())
    {
        return parse_complex_or_atomic(stripped);
    }
    if stripped.contains(',') || stripped.contains(':') || stripped.contains(' ') {
        match parse_field_list(stripped) {
            Ok(value) => return Ok(value),
            Err(TypeTableError::IntegerOverflow) => {
                return Err(TypeTableError::IntegerOverflow);
            }
            Err(_) => {}
        }
    }
    parse_complex_or_atomic(stripped)
}

#[allow(clippy::missing_errors_doc)]
pub fn sql_type_from_token(sql_type: &str) -> Result<SparkDataType, TypeTableError> {
    let stripped = py_trim(sql_type);
    let upper = stripped.to_uppercase();
    let base = py_trim(upper.split('(').next().unwrap_or_default());
    let data_type = match base {
        "BOOLEAN" | "BOOL" => SparkDataType::Boolean,
        "TINYINT" => SparkDataType::Byte,
        "SMALLINT" => SparkDataType::Short,
        "INT" | "INTEGER" => SparkDataType::Integer,
        "BIGINT" | "LONG" => SparkDataType::Long,
        "FLOAT" | "REAL" => SparkDataType::Float,
        "DOUBLE" | "FLOAT8" => SparkDataType::Double,
        "DATE" => SparkDataType::Date,
        "TIMESTAMP" => SparkDataType::Timestamp,
        "TIMESTAMP_NTZ" => SparkDataType::TimestampNtz,
        "BINARY" | "BYTEA" => SparkDataType::Binary,
        "VOID" | "NULL" => SparkDataType::Null,
        "DECIMAL" | "NUMERIC" => {
            let mut precision: i64 = 38;
            let mut scale: i64 = 18;
            if let Some(open) = upper.find('(') {
                let close = upper
                    .rfind(')')
                    .ok_or_else(|| TypeTableError::Message("substring not found".to_string()))?;
                let inside = if open < close {
                    &upper[open + 1..close]
                } else {
                    ""
                };
                let parts: Vec<&str> = inside.split(',').map(py_trim).collect();
                if parts.len() == 2 {
                    precision = parse_py_int(parts[0])?;
                    scale = parse_py_int(parts[1])?;
                }
            }
            SparkDataType::Decimal { precision, scale }
        }
        _ => SparkDataType::SparkString {
            collation: Cow::Borrowed(DEFAULT_COLLATION),
        },
    };
    Ok(data_type)
}
