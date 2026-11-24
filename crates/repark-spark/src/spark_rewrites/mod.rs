use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan};

use super::spark_literals::{LiteralRegion, requote_generic};

pub(crate) fn plan_suffix_regions(tokens: &[TokenWithSpan]) -> Result<Vec<LiteralRegion>> {
    let mut regions = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let Token::Number(digits, long) = &tokens[index].token else {
            index += 1;
            continue;
        };
        let signed = signed_digits(tokens, index, digits);
        if *long {
            if digits.bytes().all(|byte| byte.is_ascii_digit()) {
                if digits.parse::<i64>().is_ok() {
                    let cast = format!("CAST({digits} AS BIGINT)");
                    regions.push(LiteralRegion {
                        start: tokens[index].span.start,
                        end: tokens[index].span.end,
                        replacement: marked_operand(tokens, index, index, "BIGINT", cast),
                    });
                } else {
                    let minus = unary_minus_start(tokens, index);
                    if signed.parse::<i64>().is_ok() && minus.is_some() {
                        regions.push(LiteralRegion {
                            start: minus.unwrap_or(tokens[index].span.start),
                            end: tokens[index].span.end,
                            replacement: format!("CAST({signed} AS BIGINT)"),
                        });
                    } else {
                        let literal = minus.map_or(digits.as_str(), |_| signed.as_str());
                        return Err(invalid_numeric_literal_range(&format!("{literal}L")));
                    }
                }
            } else if is_exponent_double(digits) {
                regions.push(LiteralRegion {
                    start: tokens[index].span.start,
                    end: tokens[index].span.end,
                    replacement: format!("`{digits}L`"),
                });
            }
        } else if is_exponent_double(digits) {
            match adjacent_suffix(tokens, index) {
                Some(found) if is_identifying_integer_suffix(&tokens[found.index].token) => {
                    let Token::Word(word) = &tokens[found.index].token else {
                        index += 1;
                        continue;
                    };
                    regions.push(LiteralRegion {
                        start: tokens[index].span.start,
                        end: found.end,
                        replacement: format!("`{digits}{}`", word.value),
                    });
                }
                Some(found) => {
                    let target = suffix_target(&tokens[found.index].token).unwrap_or("DOUBLE");
                    regions.push(LiteralRegion {
                        start: tokens[index].span.start,
                        end: found.end,
                        replacement: suffix_replacement(digits, target, &signed)?,
                    });
                }
                None => {
                    regions.push(LiteralRegion {
                        start: tokens[index].span.start,
                        end: tokens[index].span.end,
                        replacement: suffix_replacement(digits, "DOUBLE", &signed)?,
                    });
                }
            }
        } else if let Some(found) = adjacent_suffix(tokens, index)
            && let Some(target) = suffix_target(&tokens[found.index].token)
        {
            let (start, literal) = match target {
                "SMALLINT" | "TINYINT" => match unary_minus_start(tokens, index) {
                    Some(start) => (start, signed.clone()),
                    None => (tokens[index].span.start, digits.clone()),
                },
                _ => (suffix_start(tokens, index), signed.clone()),
            };
            let cast = suffix_replacement(digits, target, &literal)?;
            regions.push(LiteralRegion {
                start,
                end: found.end,
                replacement: marked_operand(tokens, index, found.index, target, cast),
            });
        }
        index += 1;
    }
    Ok(regions)
}

struct AdjacentSuffix {
    index: usize,
    end: Location,
}

fn adjacent_suffix(tokens: &[TokenWithSpan], number: usize) -> Option<AdjacentSuffix> {
    let mut end = tokens[number].span.end;
    let mut cursor = skip_whitespace(tokens, number + 1);
    if matches!(
        tokens.get(cursor).map(|token| &token.token),
        Some(Token::Period)
    ) && tokens[cursor].span.start == end
    {
        end = tokens[cursor].span.end;
        cursor = skip_whitespace(tokens, cursor + 1);
    }
    let candidate = tokens.get(cursor)?;
    if candidate.span.start != end {
        return None;
    }
    matches!(candidate.token, Token::Word(_)).then_some(AdjacentSuffix {
        index: cursor,
        end: candidate.span.end,
    })
}

fn signed_digits(tokens: &[TokenWithSpan], number: usize, digits: &str) -> String {
    if number == 0 {
        return digits.to_string();
    }
    let mut cursor = number;
    loop {
        if cursor == 0 {
            return digits.to_string();
        }
        cursor -= 1;
        match &tokens[cursor].token {
            Token::Whitespace(_) => {}
            Token::Minus => return format!("-{digits}"),
            _ => return digits.to_string(),
        }
    }
}

fn marked_operand(
    tokens: &[TokenWithSpan],
    at: usize,
    last: usize,
    ty: &str,
    cast: String,
) -> String {
    let unary = unary_minus_start(tokens, at).is_some();
    repark_functions::decimal_precision::spell_integer_suffix(tokens, at, last, unary, ty, cast)
}

fn suffix_start(tokens: &[TokenWithSpan], number: usize) -> Location {
    tokens[number].span.start
}

fn unary_minus_start(tokens: &[TokenWithSpan], number: usize) -> Option<Location> {
    use datafusion::sql::sqlparser::keywords::Keyword;
    let mut cursor = number;
    let minus = loop {
        if cursor == 0 {
            return None;
        }
        cursor -= 1;
        match &tokens[cursor].token {
            Token::Whitespace(_) => {}
            Token::Minus => break cursor,
            _ => return None,
        }
    };
    let mut before = minus;
    loop {
        if before == 0 {
            return Some(tokens[minus].span.start);
        }
        before -= 1;
        match &tokens[before].token {
            Token::Whitespace(_) => {}
            Token::Number(_, _)
            | Token::RParen
            | Token::RBracket
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::Placeholder(_) => return None,
            Token::Word(word) => {
                return if word.keyword == Keyword::NoKeyword {
                    None
                } else {
                    Some(tokens[minus].span.start)
                };
            }
            _ => return Some(tokens[minus].span.start),
        }
    }
}

fn is_exponent_double(digits: &str) -> bool {
    digits.bytes().any(|byte| byte == b'e' || byte == b'E') && digits.parse::<f64>().is_ok()
}

fn is_identifying_integer_suffix(token: &Token) -> bool {
    let Token::Word(word) = token else {
        return false;
    };
    if word.quote_style.is_some() {
        return false;
    }
    matches!(word.value.to_ascii_uppercase().as_str(), "L" | "S" | "Y")
}

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

fn float_replacement(digits: &str) -> String {
    let operand = decimal_cast_operand(digits);
    if operand.starts_with('\'') {
        format!("CAST({}({operand}) AS FLOAT)", crate::SUFFIX_LITERAL_NAME)
    } else {
        format!("CAST({operand} AS FLOAT)")
    }
}

fn suffix_replacement(digits: &str, target: &str, signed: &str) -> Result<String> {
    match target {
        "DOUBLE" => Ok(double_replacement(digits)),
        "FLOAT" => Ok(float_replacement(digits)),
        "SMALLINT" => {
            integer_suffix_replacement(signed, "SMALLINT", "S", i16::MIN.into(), i16::MAX.into())
        }
        "TINYINT" => {
            integer_suffix_replacement(signed, "TINYINT", "Y", i8::MIN.into(), i8::MAX.into())
        }
        "DECIMAL" => decimal_suffix_replacement(digits),
        other => Ok(format!("CAST({digits} AS {other})")),
    }
}

fn integer_suffix_replacement(
    signed: &str,
    sql_type: &str,
    suffix: &str,
    min: i64,
    max: i64,
) -> Result<String> {
    let value = signed
        .parse::<i64>()
        .map_err(|_| invalid_numeric_literal_range(&format!("{signed}{suffix}")))?;
    if value < min || value > max {
        return Err(invalid_numeric_literal_range(&format!("{signed}{suffix}")));
    }
    Ok(format!("CAST({signed} AS {sql_type})"))
}

fn invalid_numeric_literal_range(token: &str) -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "[INVALID_NUMERIC_LITERAL_RANGE] Numeric literal {token} is outside the valid range"
        ))),
        None,
    )
}

fn double_replacement(digits: &str) -> String {
    let operand = decimal_cast_operand(digits);
    if operand.starts_with('\'') {
        format!("CAST({}({operand}) AS DOUBLE)", crate::SUFFIX_LITERAL_NAME)
    } else {
        format!("CAST({operand} AS DOUBLE)")
    }
}

fn decimal_cast_operand(digits: &str) -> String {
    if let Some((plain, precision, _scale)) = decimal_plain_and_precision(digits)
        && precision <= 38
    {
        return plain;
    }
    requote_generic(digits.trim_start_matches('+'))
}

fn decimal_suffix_replacement(digits: &str) -> Result<String> {
    let Some((plain, precision, scale)) = decimal_plain_and_precision(digits) else {
        return Err(invalid_numeric_literal_range(&format!("{digits}BD")));
    };
    if precision == 0 || precision > 38 || scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(invalid_numeric_literal_range(&format!("{digits}BD")));
    }
    Ok(format!(
        "CAST({}({plain}) AS DECIMAL({precision},{scale}))",
        crate::SUFFIX_LITERAL_NAME
    ))
}

fn decimal_plain_and_precision(digits: &str) -> Option<(String, u8, i8)> {
    let trimmed = digits.trim_start_matches('+');
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.trim_start_matches('-');
    let (body, exponent) = split_exponent(unsigned)?;
    let (integer, fraction) = match body.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (body, ""),
    };
    if integer.bytes().any(|byte| !byte.is_ascii_digit())
        || fraction.bytes().any(|byte| !byte.is_ascii_digit())
    {
        return None;
    }
    let mut combined = String::new();
    combined.push_str(integer);
    combined.push_str(fraction);
    if combined.is_empty() {
        return None;
    }
    let mut scale = i32::try_from(fraction.len()).ok()? - exponent;
    if scale < 0 {
        let extra = usize::try_from(-scale).ok()?;
        combined.push_str(&"0".repeat(extra));
        scale = 0;
    }
    let stripped = combined.trim_start_matches('0');
    let unscaled = if stripped.is_empty() { "0" } else { stripped };
    let java_precision = u8::try_from(unscaled.len()).ok()?;
    let scale = i8::try_from(scale).ok()?;
    let precision = java_precision.max(u8::try_from(scale).ok()?);
    let plain = decimal_plain_string(negative, unscaled, scale);
    Some((plain, precision, scale))
}

fn split_exponent(digits: &str) -> Option<(&str, i32)> {
    let Some(position) = digits.bytes().position(|byte| byte == b'e' || byte == b'E') else {
        return Some((digits, 0));
    };
    let (body, rest) = digits.split_at(position);
    let exponent = rest[1..].parse::<i32>().ok()?;
    Some((body, exponent))
}

fn decimal_plain_string(negative: bool, unscaled: &str, scale: i8) -> String {
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(unscaled);
        return out;
    }
    let scale = usize::from(u8::try_from(scale).unwrap_or(0));
    if unscaled.len() <= scale {
        out.push('0');
        out.push('.');
        out.push_str(&"0".repeat(scale - unscaled.len()));
        out.push_str(unscaled);
    } else {
        let split = unscaled.len() - scale;
        out.push_str(&unscaled[..split]);
        out.push('.');
        out.push_str(&unscaled[split..]);
    }
    out
}

pub(crate) fn plan_zero_x_hex_ident_regions(
    tokens: &[TokenWithSpan],
    sql: &str,
) -> Vec<LiteralRegion> {
    let starts = line_starts(sql);
    let mut regions = Vec::new();
    for token in tokens {
        let Token::HexStringLiteral(_) = &token.token else {
            continue;
        };
        let Some(offset) = byte_offset(&starts, sql, token.span.start) else {
            continue;
        };
        let rest = sql.get(offset..).unwrap_or_default();
        let bytes = rest.as_bytes();
        if bytes.len() < 2 || bytes[0] != b'0' || (bytes[1] != b'x' && bytes[1] != b'X') {
            continue;
        }
        let Some(end) = byte_offset(&starts, sql, token.span.end) else {
            continue;
        };
        let original = sql.get(offset..end).unwrap_or(rest);
        regions.push(LiteralRegion {
            start: token.span.start,
            end: token.span.end,
            replacement: format!("`{}`", original.replace('`', "``")),
        });
    }
    regions
}

pub(crate) fn plan_delete_from_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut index = 0;
    while matches!(
        tokens.get(index).map(|with_span| &with_span.token),
        Some(Token::Whitespace(_))
    ) {
        index += 1;
    }
    let Some(Token::Word(delete)) = tokens.get(index).map(|with_span| &with_span.token) else {
        return Vec::new();
    };
    if delete.quote_style.is_some() || !delete.value.eq_ignore_ascii_case("DELETE") {
        return Vec::new();
    }
    let next = skip_whitespace(tokens, index + 1);
    let Some(Token::Word(word)) = tokens.get(next).map(|with_span| &with_span.token) else {
        return Vec::new();
    };
    if word.quote_style.is_none() {
        let upper = word.value.to_ascii_uppercase();
        if matches!(
            upper.as_str(),
            "FROM"
                | "WHERE"
                | "AND"
                | "WHEN"
                | "THEN"
                | "SET"
                | "ELSE"
                | "END"
                | "OUTPUT"
                | "RETURNING"
        ) {
            return Vec::new();
        }
    }
    vec![LiteralRegion {
        start: tokens[index].span.end,
        end: tokens[index].span.end,
        replacement: " FROM".to_string(),
    }]
}

pub(crate) fn plan_keyword_regions(
    tokens: &[TokenWithSpan],
    sql: &str,
) -> Result<Vec<LiteralRegion>> {
    let mut regions = plan_drop_temporary_regions(tokens);
    regions.extend(timestamp_ltz_literal::plan_timestamp_ltz_literal_regions(
        tokens,
    ));
    regions.extend(timestamp_ntz_literal::plan_timestamp_ntz_literal_regions(
        tokens, sql,
    )?);
    Ok(regions)
}

fn plan_drop_temporary_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
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

pub(super) fn collect_inners(
    regions: &[LiteralRegion],
    starts: &[usize],
    sql: &str,
    span_start: usize,
    span_end: usize,
    exclude_exact: bool,
) -> Vec<(usize, usize, String)> {
    let mut inners: Vec<(usize, usize, String)> = Vec::new();
    for region in regions {
        let (Some(inner_start), Some(inner_end)) = (
            byte_offset(starts, sql, region.start),
            byte_offset(starts, sql, region.end),
        ) else {
            continue;
        };
        let is_exact = exclude_exact && (span_start, span_end) == (inner_start, inner_end);
        if !is_exact && span_start <= inner_start && inner_end <= span_end {
            inners.push((inner_start, inner_end, region.replacement.clone()));
        }
    }
    inners.sort();
    inners
}

pub(super) fn retain_outside(
    regions: &mut Vec<LiteralRegion>,
    starts: &[usize],
    sql: &str,
    span_start: usize,
    span_end: usize,
    exclude_exact: bool,
) {
    regions.retain(|region| {
        let (Some(inner_start), Some(inner_end)) = (
            byte_offset(starts, sql, region.start),
            byte_offset(starts, sql, region.end),
        ) else {
            return true;
        };
        let is_exact = exclude_exact && (span_start, span_end) == (inner_start, inner_end);
        is_exact || !(span_start <= inner_start && inner_end <= span_end)
    });
}

pub(crate) fn splice_inners(
    sql: &str,
    from: usize,
    to: usize,
    inners: &[(usize, usize, String)],
) -> String {
    let mut out = String::new();
    let mut cursor = from;
    for (inner_start, inner_end, inner_text) in inners {
        if *inner_start < from || *inner_end > to || *inner_start < cursor {
            continue;
        }
        out.push_str(sql.get(cursor..*inner_start).unwrap_or_default());
        out.push_str(inner_text);
        cursor = *inner_end;
    }
    out.push_str(sql.get(cursor..to).unwrap_or_default());
    out
}

pub(crate) fn skip_whitespace(tokens: &[TokenWithSpan], mut index: usize) -> usize {
    while matches!(
        tokens.get(index).map(|with_span| &with_span.token),
        Some(Token::Whitespace(_))
    ) {
        index += 1;
    }
    index
}

pub(crate) fn line_starts(sql: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (offset, character) in sql.char_indices() {
        if character == '\n' {
            starts.push(offset + 1);
        }
    }
    starts
}

pub(crate) fn byte_offset(starts: &[usize], sql: &str, location: Location) -> Option<usize> {
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

pub(crate) fn plan_insert_partition_column_list_regions(
    tokens: &[TokenWithSpan],
    sql: &str,
    regions: &mut Vec<LiteralRegion>,
) {
    let first = skip_whitespace(tokens, 0);
    let Some(Token::Word(insert)) = tokens.get(first).map(|with_span| &with_span.token) else {
        return;
    };
    if insert.quote_style.is_some() || !insert.value.eq_ignore_ascii_case("INSERT") {
        return;
    }
    let mut index = first + 1;
    while index < tokens.len() {
        let Token::Word(word) = &tokens[index].token else {
            index += 1;
            continue;
        };
        if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case("PARTITION") {
            index += 1;
            continue;
        }
        if let Some((clause_close, list_open, list_close)) =
            partition_column_list_swap(tokens, index)
        {
            let starts = line_starts(sql);
            let (Some(part_from), Some(part_to), Some(list_from), Some(list_to)) = (
                byte_offset(&starts, sql, tokens[index].span.start),
                byte_offset(&starts, sql, tokens[clause_close].span.end),
                byte_offset(&starts, sql, tokens[list_open].span.start),
                byte_offset(&starts, sql, tokens[list_close].span.end),
            ) else {
                return;
            };
            let part_inners = collect_inners(regions, &starts, sql, part_from, part_to, false);
            let list_inners = collect_inners(regions, &starts, sql, list_from, list_to, false);
            let part_text = splice_inners(sql, part_from, part_to, &part_inners);
            let list_text = splice_inners(sql, list_from, list_to, &list_inners);
            retain_outside(regions, &starts, sql, part_from, part_to, false);
            retain_outside(regions, &starts, sql, list_from, list_to, false);
            regions.push(LiteralRegion {
                start: tokens[index].span.start,
                end: tokens[clause_close].span.end,
                replacement: list_text,
            });
            regions.push(LiteralRegion {
                start: tokens[list_open].span.start,
                end: tokens[list_close].span.end,
                replacement: part_text,
            });
            return;
        }
        index += 1;
    }
}

fn partition_column_list_swap(
    tokens: &[TokenWithSpan],
    partition: usize,
) -> Option<(usize, usize, usize)> {
    let clause_open = skip_whitespace(tokens, partition + 1);
    let clause_close = matching_paren(tokens, clause_open)?;
    let list_open = skip_whitespace(tokens, clause_close + 1);
    let list_close = matching_paren(tokens, list_open)?;
    if !is_bare_column_list(tokens, list_open, list_close) {
        return None;
    }
    let after = skip_whitespace(tokens, list_close + 1);
    if !starts_insert_source(tokens.get(after).map(|with_span| &with_span.token)) {
        return None;
    }
    Some((clause_close, list_open, list_close))
}

pub(crate) fn matching_paren(tokens: &[TokenWithSpan], open: usize) -> Option<usize> {
    if !matches!(
        tokens.get(open).map(|with_span| &with_span.token),
        Some(Token::LParen)
    ) {
        return None;
    }
    let mut depth = 0usize;
    for (offset, token) in tokens.iter().enumerate().skip(open) {
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn is_bare_column_list(tokens: &[TokenWithSpan], open: usize, close: usize) -> bool {
    let mut words = 0usize;
    for token in &tokens[open + 1..close] {
        match &token.token {
            Token::Whitespace(_) | Token::Comma | Token::Period => {}
            Token::Word(word) => {
                if word.quote_style.is_none() && starts_insert_source(Some(&token.token)) {
                    return false;
                }
                words += 1;
            }
            _ => return false,
        }
    }
    words > 0
}

fn starts_insert_source(token: Option<&Token>) -> bool {
    let Some(Token::Word(word)) = token else {
        return matches!(token, Some(Token::LParen));
    };
    word.quote_style.is_none()
        && matches!(
            word.value.to_ascii_uppercase().as_str(),
            "SELECT" | "VALUES" | "WITH" | "TABLE" | "FROM"
        )
}

pub(crate) mod create_options;
pub(crate) mod extract_field;
pub(crate) mod struct_field;
pub(crate) mod timestamp_ltz_literal;
pub(crate) mod timestamp_ntz_literal;

pub(crate) use struct_field::plan_struct_field_regions;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_numeric_literal_range_maps_to_the_full_parse_message() {
        let expected =
            "[INVALID_NUMERIC_LITERAL_RANGE] Numeric literal 128Y is outside the valid range";
        let mapped = repark_core::engine_err(invalid_numeric_literal_range("128Y"));
        let repark_common::Error::Parse(message) = mapped else {
            panic!("expected a Parse error");
        };
        assert_eq!(message, expected);
    }
}
