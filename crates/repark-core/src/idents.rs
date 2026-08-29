//! Table-identifier segment parsing and path-escape refuse (identity boundary).

/// ===========================================================================================
/// Split a multipart table identifier with quote awareness (double-quote / backtick).
///
/// Unquoted segments must match `[A-Za-z_][A-Za-z0-9_]*`. Quoted segments may contain dots;
/// `""` inside double quotes escapes a quote. Mirrors the Python `_parse_table_identifier_segments`
/// used by `spark.table` so `table_exists` and writer SQL share one identity model (C2-L-006).
/// ===========================================================================================
/// Reject identifier segments that could escape a warehouse root (O3-C4-SEC-001).
///
/// Needles from [`repark_iceberg::write::idents::path_escape_kind`] (r23 QI1 single-source) so
/// `table_exists` / Python `_sql_table_ref` fail at the identity boundary, not only at CTAS
/// path composition. Mirrors `repark-sql::reject_path_escape_ident` (C2-SEC-003 / C1-SEC-001).
pub(crate) fn reject_path_escape_segment(segment: &str) -> std::result::Result<(), String> {
    match repark_iceberg::write::idents::path_escape_kind(segment) {
        Some(repark_iceberg::write::idents::PathEscapeKind::Traversal) => Err(format!(
            "identifier segment {segment:?} must not contain path traversal ('..')"
        )),
        Some(repark_iceberg::write::idents::PathEscapeKind::Separator) => Err(format!(
            "identifier segment {segment:?} must not contain path separators"
        )),
        None => Ok(()),
    }
}

pub(crate) fn parse_table_identifier_segments(
    name: &str,
) -> std::result::Result<Vec<String>, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("table name must not be empty".to_string());
    }
    let chars: Vec<char> = name.chars().collect();
    let mut segments: Vec<String> = Vec::new();
    let mut index = 0usize;
    let length = chars.len();
    while index < length {
        let char = chars[index];
        if char == '"' || char == '`' {
            let quote = char;
            index += 1;
            let mut buffer = String::new();
            let mut closed = false;
            while index < length {
                let current = chars[index];
                if current == quote {
                    if quote == '"' && index + 1 < length && chars[index + 1] == '"' {
                        buffer.push('"');
                        index += 2;
                        continue;
                    }
                    closed = true;
                    index += 1;
                    break;
                }
                buffer.push(current);
                index += 1;
            }
            if !closed {
                return Err("unterminated quoted identifier".to_string());
            }
            if buffer.is_empty() {
                return Err("empty quoted identifier segment".to_string());
            }
            reject_path_escape_segment(&buffer)?;
            segments.push(buffer);
        } else if char.is_ascii_alphabetic() || char == '_' {
            let start = index;
            index += 1;
            while index < length {
                let current = chars[index];
                if current.is_ascii_alphanumeric() || current == '_' {
                    index += 1;
                } else {
                    break;
                }
            }
            let segment: String = chars[start..index].iter().collect();
            reject_path_escape_segment(&segment)?;
            segments.push(segment);
        } else {
            return Err(format!(
                "unexpected character {char:?} in table identifier (SQL fragments are not allowed)"
            ));
        }
        if index >= length {
            break;
        }
        if chars[index] != '.' {
            return Err(format!(
                "expected '.' between identifier segments, found {:?}",
                chars[index]
            ));
        }
        index += 1;
        if index >= length {
            return Err("trailing '.' in table identifier".to_string());
        }
    }
    if segments.is_empty() {
        return Err("table name must not be empty".to_string());
    }
    Ok(segments)
}
