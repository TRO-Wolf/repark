use super::{byte_offset, line_starts, matching_paren, skip_whitespace};
use crate::spark_literals::{LiteralRegion, requote_generic, unescape_spark_literal};
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan};

pub(crate) fn plan_create_options_regions(
    tokens: &[TokenWithSpan],
    sql: &str,
    regions: &mut Vec<LiteralRegion>,
) {
    let mut matches = create_options_matches(tokens);
    if matches.len() != 1 {
        return;
    }
    let found = matches.remove(0);
    let starts = line_starts(sql);
    let (Some(span_start), Some(span_end)) = (
        byte_offset(&starts, sql, found.start),
        byte_offset(&starts, sql, found.end),
    ) else {
        return;
    };
    let mut inners: Vec<(usize, usize, String)> = Vec::new();
    for region in regions.iter() {
        let (Some(inner_start), Some(inner_end)) = (
            byte_offset(&starts, sql, region.start),
            byte_offset(&starts, sql, region.end),
        ) else {
            continue;
        };
        if span_start <= inner_start && inner_end <= span_end {
            inners.push((inner_start, inner_end, region.replacement.clone()));
        }
    }
    inners.sort();
    let mut replacement = String::from("TBLPROPERTIES (");
    for (index, pair) in found.pairs.iter().enumerate() {
        if index > 0 {
            replacement.push_str(", ");
        }
        let Some(value) = create_options_value(tokens, sql, &starts, pair, &inners) else {
            return;
        };
        replacement.push_str(&requote_generic(&pair.key));
        replacement.push('=');
        replacement.push_str(&value);
        replacement.push_str(", ");
        replacement.push_str(&requote_generic(&format!("option.{}", pair.key)));
        replacement.push('=');
        replacement.push_str(&value);
    }
    replacement.push(')');
    regions.retain(|region| {
        let (Some(inner_start), Some(inner_end)) = (
            byte_offset(&starts, sql, region.start),
            byte_offset(&starts, sql, region.end),
        ) else {
            return true;
        };
        !(span_start <= inner_start && inner_end <= span_end)
    });
    regions.push(LiteralRegion {
        start: found.start,
        end: found.end,
        replacement,
    });
}

struct CreateOptionsMatch {
    start: Location,
    end: Location,
    pairs: Vec<CreateOptionsPair>,
}

struct CreateOptionsPair {
    key: String,
    value_start: usize,
    value_end: usize,
}

fn create_options_matches(tokens: &[TokenWithSpan]) -> Vec<CreateOptionsMatch> {
    let Some(mut cursor) = create_table_using_iceberg_prefix(tokens) else {
        return Vec::new();
    };
    let mut matches = Vec::new();
    let mut options_seen = 0usize;
    let mut depth = 0usize;
    while cursor < tokens.len() {
        match &tokens[cursor].token {
            Token::Word(word)
                if depth == 0
                    && word.quote_style.is_none()
                    && word.value.eq_ignore_ascii_case("AS") =>
            {
                break;
            }
            Token::Word(word)
                if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("OPTIONS") =>
            {
                options_seen += 1;
                if let Some((found, after)) = match_options_clause(tokens, cursor) {
                    matches.push(found);
                    cursor = after;
                    continue;
                }
            }
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ => {}
        }
        cursor += 1;
    }
    if options_seen != 1 {
        return Vec::new();
    }
    matches
}

fn create_table_using_iceberg_prefix(tokens: &[TokenWithSpan]) -> Option<usize> {
    let mut cursor = skip_whitespace(tokens, 0);
    cursor = match_bare_word(tokens, cursor, "CREATE")?;
    cursor = skip_whitespace(tokens, cursor + 1);
    if word_at_is(tokens, cursor, "OR") {
        cursor = skip_whitespace(tokens, cursor + 1);
        cursor = match_bare_word(tokens, cursor, "REPLACE")?;
        cursor = skip_whitespace(tokens, cursor + 1);
    }
    cursor = match_bare_word(tokens, cursor, "TABLE")?;
    cursor = skip_whitespace(tokens, cursor + 1);
    if word_at_is(tokens, cursor, "IF") {
        cursor = skip_whitespace(tokens, cursor + 1);
        cursor = match_bare_word(tokens, cursor, "NOT")?;
        cursor = skip_whitespace(tokens, cursor + 1);
        cursor = match_bare_word(tokens, cursor, "EXISTS")?;
        cursor = skip_whitespace(tokens, cursor + 1);
    }
    if !matches!(
        tokens.get(cursor).map(|token| &token.token),
        Some(Token::Word(_))
    ) {
        return None;
    }
    cursor = skip_whitespace(tokens, cursor + 1);
    while matches!(
        tokens.get(cursor).map(|token| &token.token),
        Some(Token::Period)
    ) {
        cursor = skip_whitespace(tokens, cursor + 1);
        if !matches!(
            tokens.get(cursor).map(|token| &token.token),
            Some(Token::Word(_))
        ) {
            return None;
        }
        cursor = skip_whitespace(tokens, cursor + 1);
    }
    if matches!(
        tokens.get(cursor).map(|token| &token.token),
        Some(Token::LParen)
    ) {
        cursor = matching_paren(tokens, cursor)? + 1;
        cursor = skip_whitespace(tokens, cursor);
    }
    cursor = match_bare_word(tokens, cursor, "USING")?;
    cursor = skip_whitespace(tokens, cursor + 1);
    let Token::Word(provider) = &tokens.get(cursor)?.token else {
        return None;
    };
    if provider.quote_style.is_some() || !provider.value.eq_ignore_ascii_case("iceberg") {
        return None;
    }
    Some(cursor + 1)
}

fn match_bare_word(tokens: &[TokenWithSpan], index: usize, keyword: &str) -> Option<usize> {
    let Token::Word(word) = &tokens.get(index)?.token else {
        return None;
    };
    (word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword)).then_some(index)
}

fn word_at_is(tokens: &[TokenWithSpan], index: usize, keyword: &str) -> bool {
    match_bare_word(tokens, index, keyword).is_some()
}

fn match_options_clause(
    tokens: &[TokenWithSpan],
    options_index: usize,
) -> Option<(CreateOptionsMatch, usize)> {
    let open = skip_whitespace(tokens, options_index + 1);
    if !matches!(
        tokens.get(open).map(|token| &token.token),
        Some(Token::LParen)
    ) {
        return None;
    }
    let close = matching_paren(tokens, open)?;
    let mut pairs = Vec::new();
    let mut cursor = skip_whitespace(tokens, open + 1);
    loop {
        if matches!(
            tokens.get(cursor).map(|token| &token.token),
            Some(Token::RParen)
        ) {
            break;
        }
        let key = match &tokens.get(cursor)?.token {
            Token::Word(word) => word.value.clone(),
            Token::SingleQuotedString(raw) | Token::DoubleQuotedString(raw) => {
                unescape_spark_literal(raw)
            }
            _ => return None,
        };
        cursor = skip_whitespace(tokens, cursor + 1);
        if !matches!(
            tokens.get(cursor).map(|token| &token.token),
            Some(Token::Eq)
        ) {
            return None;
        }
        cursor = skip_whitespace(tokens, cursor + 1);
        let value_start = cursor;
        loop {
            match tokens.get(cursor).map(|token| &token.token) {
                Some(Token::Comma | Token::RParen) => break,
                None | Some(Token::EOF) => return None,
                _ => cursor += 1,
            }
        }
        if cursor == value_start {
            return None;
        }
        pairs.push(CreateOptionsPair {
            key,
            value_start,
            value_end: cursor,
        });
        if matches!(
            tokens.get(cursor).map(|token| &token.token),
            Some(Token::RParen)
        ) {
            break;
        }
        cursor = skip_whitespace(tokens, cursor + 1);
        if matches!(
            tokens.get(cursor).map(|token| &token.token),
            Some(Token::RParen)
        ) {
            return None;
        }
    }
    Some((
        CreateOptionsMatch {
            start: tokens[options_index].span.start,
            end: tokens[close].span.end,
            pairs,
        },
        close + 1,
    ))
}

fn create_options_value(
    tokens: &[TokenWithSpan],
    sql: &str,
    starts: &[usize],
    pair: &CreateOptionsPair,
    inners: &[(usize, usize, String)],
) -> Option<String> {
    let value_from = byte_offset(starts, sql, tokens[pair.value_start].span.start)?;
    let last = pair.value_end.checked_sub(1)?;
    let value_to = byte_offset(starts, sql, tokens[last].span.end)?;
    let mut value = String::new();
    let mut cursor = value_from;
    for (inner_start, inner_end, inner_text) in inners {
        if *inner_start < value_from || *inner_end > value_to {
            continue;
        }
        value.push_str(sql.get(cursor..*inner_start).unwrap_or_default());
        value.push_str(inner_text);
        cursor = *inner_end;
    }
    value.push_str(sql.get(cursor..value_to).unwrap_or_default());
    Some(value)
}

pub(crate) fn sql_may_have_create_options(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    lower.contains("create") && lower.contains("using") && lower.contains("options")
}
