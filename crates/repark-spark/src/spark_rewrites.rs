//! Secondary Spark-door token rewrites over the shared lexer.
//!
//! The primary literal pass lives in [`crate::spark_literals`]; this module holds the four
//! rewrites that turn Spark-only spellings into DataFusion-plannable SQL: numeric literal
//! suffixes, `DROP TEMPORARY`, `* EXCLUDE` columns, and call-base struct field access.

use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan};

use super::spark_literals::{LiteralRegion, requote_generic};
/// Collect Spark numeric-suffix rewrites (`2.5D` → `CAST('2.5' AS DOUBLE)`). A number
/// written with an exponent (`1.0E6`) behaves as if it carried the `D` suffix (DOUBLE).
pub(crate) fn plan_suffix_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let Token::Number(digits, long) = &tokens[index].token else {
            index += 1;
            continue;
        };
        if *long {
            if digits.bytes().all(|byte| byte.is_ascii_digit()) {
                regions.push(LiteralRegion {
                    start: tokens[index].span.start,
                    end: tokens[index].span.end,
                    replacement: format!("CAST({digits} AS BIGINT)"),
                });
            }
        } else if is_exponent_double(digits) {
            let (end, target) = match adjacent_suffix_word(tokens, index) {
                Some(next) => match suffix_target(&tokens[next].token) {
                    Some(target) => (tokens[next].span.end, target),
                    None => (tokens[index].span.end, "DOUBLE"),
                },
                None => (tokens[index].span.end, "DOUBLE"),
            };
            regions.push(LiteralRegion {
                start: tokens[index].span.start,
                end,
                replacement: suffix_replacement(digits, target),
            });
        } else if let Some(next) = adjacent_suffix_word(tokens, index)
            && let Some(target) = suffix_target(&tokens[next].token)
        {
            regions.push(LiteralRegion {
                start: tokens[index].span.start,
                end: tokens[next].span.end,
                replacement: suffix_replacement(digits, target),
            });
        }
        index += 1;
    }
    regions
}

/// The word index after a number when it sits directly against it, unquoted.
fn adjacent_suffix_word(tokens: &[TokenWithSpan], number: usize) -> Option<usize> {
    let end = tokens[number].span.end;
    let mut cursor = number + 1;
    while matches!(tokens.get(cursor)?.token, Token::Whitespace(_)) {
        cursor += 1;
    }
    let candidate = tokens.get(cursor)?;
    if candidate.span.start != end {
        return None;
    }
    matches!(candidate.token, Token::Word(_)).then_some(cursor)
}

/// True for an exponent-form number (`1E2`, `1.0E-3`): Spark types it DOUBLE while a
/// plain decimal stays DECIMAL. The `f64` parse rejects non-decimal spellings (hex words).
fn is_exponent_double(digits: &str) -> bool {
    digits.bytes().any(|byte| byte == b'e' || byte == b'E') && digits.parse::<f64>().is_ok()
}

/// The cast target for a suffixed-number word, or `None` when it is not a suffix.
fn suffix_target(token: &Token) -> Option<&'static str> {
    let Token::Word(word) = token else {
        return None;
    };
    if word.quote_style.is_some() {
        return None;
    }
    match word.value.to_ascii_uppercase().as_str() {
        "D" => Some("DOUBLE"),
        "F" => Some("FLOAT"),
        "S" => Some("SMALLINT"),
        "Y" => Some("TINYINT"),
        "BD" => Some("DECIMAL"),
        _ => None,
    }
}

/// The cast text: a string cast for `DOUBLE` (a huge exponent overflows decimal), digits else.
fn suffix_replacement(digits: &str, target: &str) -> String {
    if target == "DOUBLE" {
        format!("CAST('{digits}' AS DOUBLE)")
    } else {
        format!("CAST({digits} AS {target})")
    }
}

/// Drop a `TEMPORARY` keyword directly after `DROP` (valid Spark SQL for `FUNCTION`
/// and `VIEW`; the Databricks lexer has no `TEMPORARY`). DataFusion tracks no
/// temp-ness through SQL, so the bare `DROP` keeps the statement's meaning.
pub(crate) fn plan_drop_temporary_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let Token::Word(drop) = &tokens[index].token else {
            index += 1;
            continue;
        };
        if drop.quote_style.is_some() || !drop.value.eq_ignore_ascii_case("DROP") {
            index += 1;
            continue;
        }
        let candidate = skip_whitespace(tokens, index + 1);
        let Some(Token::Word(temporary)) = tokens.get(candidate).map(|with_span| &with_span.token)
        else {
            index += 1;
            continue;
        };
        if temporary.quote_style.is_some() || !temporary.value.eq_ignore_ascii_case("TEMPORARY") {
            index += 1;
            continue;
        }
        let word = &tokens[candidate];
        regions.push(LiteralRegion {
            start: word.span.start,
            end: word.span.end,
            replacement: String::new(),
        });
        index = candidate + 1;
    }
    regions
}

/// Rewrite `* EXCLUDE (…)` to `* EXCEPT (…)` (the Databricks lexer has no `EXCLUDE`).
pub(crate) fn plan_wildcard_except_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if !matches!(tokens[index].token, Token::Mul) {
            index += 1;
            continue;
        }
        let word_at = skip_whitespace(tokens, index + 1);
        let Some(Token::Word(exclude)) = tokens.get(word_at).map(|with_span| &with_span.token)
        else {
            index += 1;
            continue;
        };
        if exclude.quote_style.is_some() || !exclude.value.eq_ignore_ascii_case("EXCLUDE") {
            index += 1;
            continue;
        }
        let cursor = skip_whitespace(tokens, word_at + 1);
        if !matches!(
            tokens.get(cursor).map(|with_span| &with_span.token),
            Some(Token::LParen)
        ) {
            index += 1;
            continue;
        }
        let word = &tokens[word_at];
        regions.push(LiteralRegion {
            start: word.span.start,
            end: word.span.end,
            replacement: "EXCEPT".to_string(),
        });
        index = cursor + 1;
    }
    regions
}

/// Rewrite `named_struct(…).field` to a subscript (the planner rejects call-base field access).
pub(crate) fn plan_struct_field_regions(
    tokens: &[TokenWithSpan],
    sql: &str,
    regions: &mut Vec<LiteralRegion>,
) {
    let mut matches = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let Token::Word(name) = &tokens[index].token else {
            index += 1;
            continue;
        };
        if name.quote_style.is_some() || !name.value.eq_ignore_ascii_case("named_struct") {
            index += 1;
            continue;
        }
        if let Some(found) = match_struct_field(tokens, index) {
            matches.push(found);
        }
        index += 1;
    }
    if matches.is_empty() {
        return;
    }
    let starts = line_starts(sql);
    matches.sort_by_key(|found| (found.start.line, found.start.column));
    for found in matches.iter().rev() {
        let (Some(call_start), Some(call_end), Some(full_end)) = (
            byte_offset(&starts, sql, found.start),
            byte_offset(&starts, sql, found.call_end),
            byte_offset(&starts, sql, found.end),
        ) else {
            continue;
        };
        let mut inners: Vec<(usize, usize, String)> = Vec::new();
        for region in regions.iter() {
            let (Some(inner_start), Some(inner_end)) = (
                byte_offset(&starts, sql, region.start),
                byte_offset(&starts, sql, region.end),
            ) else {
                continue;
            };
            if (call_start, full_end) != (inner_start, inner_end)
                && call_start <= inner_start
                && inner_end <= full_end
            {
                inners.push((inner_start, inner_end, region.replacement.clone()));
            }
        }
        inners.sort();
        let mut replacement = String::new();
        let mut cursor = call_start;
        for (inner_start, inner_end, inner_text) in &inners {
            replacement.push_str(sql.get(cursor..*inner_start).unwrap_or_default());
            replacement.push_str(inner_text);
            cursor = *inner_end;
        }
        replacement.push_str(sql.get(cursor..call_end).unwrap_or_default());
        replacement.push('[');
        replacement.push_str(&requote_generic(&found.field));
        replacement.push(']');
        regions.retain(|region| {
            let (Some(inner_start), Some(inner_end)) = (
                byte_offset(&starts, sql, region.start),
                byte_offset(&starts, sql, region.end),
            ) else {
                return true;
            };
            !((call_start, full_end) != (inner_start, inner_end)
                && call_start <= inner_start
                && inner_end <= full_end)
        });
        regions.push(LiteralRegion {
            start: found.start,
            end: found.end,
            replacement,
        });
    }
}

/// A `named_struct(…) . field` match: statement span, call end, and field name.
struct StructFieldMatch {
    start: Location,
    call_end: Location,
    end: Location,
    field: String,
}

/// Match `named_struct (` balanced-parens `) . field` at `tokens[index]`.
fn match_struct_field(tokens: &[TokenWithSpan], index: usize) -> Option<StructFieldMatch> {
    let mut cursor = skip_whitespace(tokens, index + 1);
    if !matches!(tokens.get(cursor)?.token, Token::LParen) {
        return None;
    }
    let mut depth = 0usize;
    let call_end = loop {
        match tokens.get(cursor)?.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    break tokens[cursor].span.end;
                }
            }
            _ => {}
        }
        cursor += 1;
    };
    cursor = skip_whitespace(tokens, cursor + 1);
    if !matches!(tokens.get(cursor)?.token, Token::Period) {
        return None;
    }
    cursor = skip_whitespace(tokens, cursor + 1);
    let Token::Word(field) = tokens.get(cursor).map(|with_span| &with_span.token)? else {
        return None;
    };
    if field.quote_style.is_some_and(|quote| quote != '`') {
        return None;
    }
    Some(StructFieldMatch {
        start: tokens[index].span.start,
        call_end,
        end: tokens[cursor].span.end,
        field: field.value.clone(),
    })
}

/// The first non-whitespace token at or after `index`.
fn skip_whitespace(tokens: &[TokenWithSpan], mut index: usize) -> usize {
    while matches!(
        tokens.get(index).map(|with_span| &with_span.token),
        Some(Token::Whitespace(_))
    ) {
        index += 1;
    }
    index
}

/// Byte index of every line start in `sql`.
fn line_starts(sql: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (offset, character) in sql.char_indices() {
        if character == '\n' {
            starts.push(offset + 1);
        }
    }
    starts
}

/// Byte offset of a 1-based char-column location, or `None` when out of range. An
/// exclusive span end sitting exactly at the input end has no character to index.
fn byte_offset(starts: &[usize], sql: &str, location: Location) -> Option<usize> {
    let line_start = *starts.get(usize::try_from(location.line).ok()?.checked_sub(1)?)?;
    let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
    if column == 0 {
        return Some(line_start);
    }
    let tail = &sql[line_start..];
    match tail.char_indices().nth(column) {
        Some((offset, _)) => Some(line_start + offset),
        None if tail.chars().count() == column => Some(line_start + tail.len()),
        None => None,
    }
}
