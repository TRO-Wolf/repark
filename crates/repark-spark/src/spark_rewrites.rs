use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan};

use super::spark_literals::{LiteralRegion, requote_generic};
use super::spark_typed::SPARK_AS_NAME;

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
                if digits.parse::<i64>().is_err()
                    && signed.parse::<i64>().is_ok()
                    && let Some(start) = unary_minus_start(tokens, index)
                {
                    regions.push(LiteralRegion {
                        start,
                        end: tokens[index].span.end,
                        replacement: format!("CAST({signed} AS BIGINT)"),
                    });
                } else {
                    regions.push(LiteralRegion {
                        start: tokens[index].span.start,
                        end: tokens[index].span.end,
                        replacement: format!("CAST({digits} AS BIGINT)"),
                    });
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
            regions.push(LiteralRegion {
                start: suffix_start(tokens, index),
                end: found.end,
                replacement: suffix_replacement(digits, target, &signed)?,
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
        "SMALLINT" => integer_suffix_replacement(
            digits,
            signed,
            "SMALLINT",
            "S",
            i16::MIN.into(),
            i16::MAX.into(),
        ),
        "TINYINT" => integer_suffix_replacement(
            digits,
            signed,
            "TINYINT",
            "Y",
            i8::MIN.into(),
            i8::MAX.into(),
        ),
        "DECIMAL" => decimal_suffix_replacement(digits),
        other => Ok(format!("CAST({digits} AS {other})")),
    }
}

fn integer_suffix_replacement(
    digits: &str,
    signed: &str,
    sql_type: &str,
    suffix: &str,
    min: i64,
    max: i64,
) -> Result<String> {
    let value = signed
        .parse::<i64>()
        .map_err(|_| invalid_numeric_literal_range(&format!("{digits}{suffix}")))?;
    if value < min || value > max {
        return Err(invalid_numeric_literal_range(&format!("{signed}{suffix}")));
    }
    Ok(format!("CAST({digits} AS {sql_type})"))
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
        if let Some(extracted) = extract_named_struct_value(tokens, sql, found) {
            let display = spark_struct_display(
                sql.get(call_start..call_end).unwrap_or_default(),
                &found.fields,
            );
            replacement = format!(
                "{SPARK_AS_NAME}({}, ({extracted}))",
                requote_generic(&display)
            );
        } else {
            replacement.push('[');
            for (index, field) in found.fields.iter().enumerate() {
                if index > 0 {
                    replacement.push_str("][");
                }
                replacement.push_str(&requote_generic(field));
            }
            replacement.push(']');
        }
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

struct StructFieldMatch {
    name_index: usize,
    start: Location,
    call_end: Location,
    end: Location,
    fields: Vec<String>,
}

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
    let mut fields = Vec::new();
    let mut end = call_end;
    loop {
        cursor = skip_whitespace(tokens, cursor + 1);
        let Token::Word(field) = tokens.get(cursor).map(|with_span| &with_span.token)? else {
            break;
        };
        if field.quote_style.is_some_and(|quote| quote != '`') {
            break;
        }
        fields.push(field.value.clone());
        end = tokens[cursor].span.end;
        let next = skip_whitespace(tokens, cursor + 1);
        if !matches!(
            tokens.get(next).map(|with_span| &with_span.token),
            Some(Token::Period)
        ) {
            break;
        }
        cursor = next;
    }
    if fields.is_empty() {
        return None;
    }
    Some(StructFieldMatch {
        name_index: index,
        start: tokens[index].span.start,
        call_end,
        end,
        fields,
    })
}

fn extract_named_struct_value(
    tokens: &[TokenWithSpan],
    sql: &str,
    found: &StructFieldMatch,
) -> Option<String> {
    let mut args = named_struct_args(tokens, found.name_index)?;
    let mut remaining = found.fields.as_slice();
    loop {
        let field = remaining.first()?;
        let (_name, value_start, value_end) =
            args.iter().find(|(name, _, _)| name == field).cloned()?;
        if remaining.len() == 1 {
            return Some(sql_between(tokens, sql, value_start, value_end));
        }
        let Token::Word(word) = &tokens.get(value_start)?.token else {
            return None;
        };
        if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case("named_struct") {
            return None;
        }
        args = named_struct_args(tokens, value_start)?;
        remaining = &remaining[1..];
    }
}

fn named_struct_args(
    tokens: &[TokenWithSpan],
    name_index: usize,
) -> Option<Vec<(String, usize, usize)>> {
    let mut cursor = skip_whitespace(tokens, name_index + 1);
    if !matches!(tokens.get(cursor)?.token, Token::LParen) {
        return None;
    }
    cursor += 1;
    let mut args = Vec::new();
    loop {
        cursor = skip_whitespace(tokens, cursor);
        let name = literal_field_name(&tokens.get(cursor)?.token)?;
        cursor = skip_whitespace(tokens, cursor + 1);
        if !matches!(tokens.get(cursor)?.token, Token::Comma) {
            return None;
        }
        cursor = skip_whitespace(tokens, cursor + 1);
        let value_start = cursor;
        let value_end = consume_value(tokens, cursor)?;
        args.push((name, value_start, value_end));
        cursor = skip_whitespace(tokens, value_end);
        match tokens.get(cursor)?.token {
            Token::Comma => cursor += 1,
            Token::RParen => break,
            _ => return None,
        }
    }
    Some(args)
}

fn consume_value(tokens: &[TokenWithSpan], start: usize) -> Option<usize> {
    let mut depth_paren = 0i32;
    let mut depth_bracket = 0i32;
    let mut cursor = start;
    while let Some(token) = tokens.get(cursor) {
        match &token.token {
            Token::LParen => depth_paren += 1,
            Token::RParen => {
                if depth_paren == 0 && depth_bracket == 0 {
                    return Some(cursor);
                }
                depth_paren -= 1;
            }
            Token::LBracket => depth_bracket += 1,
            Token::RBracket => depth_bracket -= 1,
            Token::Comma if depth_paren == 0 && depth_bracket == 0 => {
                return Some(cursor);
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn literal_field_name(token: &Token) -> Option<String> {
    match token {
        Token::SingleQuotedString(value) | Token::DoubleQuotedString(value) => Some(value.clone()),
        Token::Word(word) if word.quote_style == Some('\'') || word.quote_style == Some('"') => {
            Some(word.value.clone())
        }
        _ => None,
    }
}

fn sql_between(tokens: &[TokenWithSpan], sql: &str, start: usize, end_exclusive: usize) -> String {
    let starts = line_starts(sql);
    let from = byte_offset(&starts, sql, tokens[start].span.start).unwrap_or(0);
    let last = end_exclusive.saturating_sub(1).max(start);
    let to = byte_offset(&starts, sql, tokens[last].span.end).unwrap_or(sql.len());
    sql.get(from..to).unwrap_or_default().to_string()
}

fn spark_struct_display(call_sql: &str, fields: &[String]) -> String {
    let mut display = call_sql.to_string();
    for field in fields {
        display = display.replace(&format!("'{field}'"), field);
        display = display.replace(&format!("\"{field}\""), field);
    }
    if fields.is_empty() {
        display
    } else {
        format!("{display}.{}", fields.join("."))
    }
}

fn skip_whitespace(tokens: &[TokenWithSpan], mut index: usize) -> usize {
    while matches!(
        tokens.get(index).map(|with_span| &with_span.token),
        Some(Token::Whitespace(_))
    ) {
        index += 1;
    }
    index
}

fn line_starts(sql: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (offset, character) in sql.char_indices() {
        if character == '\n' {
            starts.push(offset + 1);
        }
    }
    starts
}

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
