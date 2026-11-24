use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan};

use super::{
    byte_offset, collect_inners, line_starts, retain_outside, skip_whitespace, splice_inners,
};
use crate::spark_literals::{LiteralRegion, requote_generic};
use crate::spark_typed::SPARK_AS_NAME;

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
        let inners = collect_inners(regions, &starts, sql, call_start, full_end, true);
        let replacement = if let Some((value_from, value_to)) =
            extract_named_struct_span(tokens, sql, &starts, found)
        {
            let extracted = splice_inners(sql, value_from, value_to, &inners);
            let display = spark_struct_display(
                sql.get(call_start..call_end).unwrap_or_default(),
                &found.fields,
            );
            format!(
                "{SPARK_AS_NAME}({}, ({extracted}))",
                requote_generic(&display)
            )
        } else {
            let mut rewritten = splice_inners(sql, call_start, call_end, &inners);
            rewritten.push('[');
            for (index, field) in found.fields.iter().enumerate() {
                if index > 0 {
                    rewritten.push_str("][");
                }
                rewritten.push_str(&requote_generic(field));
            }
            rewritten.push(']');
            rewritten
        };
        retain_outside(regions, &starts, sql, call_start, full_end, true);
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

fn extract_named_struct_span(
    tokens: &[TokenWithSpan],
    sql: &str,
    starts: &[usize],
    found: &StructFieldMatch,
) -> Option<(usize, usize)> {
    let mut args = named_struct_args(tokens, found.name_index)?;
    let mut remaining = found.fields.as_slice();
    loop {
        let field = remaining.first()?;
        let (_name, value_start, value_end) =
            args.iter().find(|(name, _, _)| name == field).cloned()?;
        if remaining.len() == 1 {
            let from = byte_offset(starts, sql, tokens.get(value_start)?.span.start)?;
            let last = value_end.saturating_sub(1).max(value_start);
            let to = byte_offset(starts, sql, tokens.get(last)?.span.end)?;
            return Some((from, to));
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
