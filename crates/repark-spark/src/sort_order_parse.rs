use datafusion::sql::sqlparser::dialect::{DatabricksDialect, Dialect};
use iceberg::spec::{NullOrder, SortDirection};

#[derive(Debug)]
pub(crate) struct WriteOrderField {
    pub(crate) name: String,
    pub(crate) direction: SortDirection,
    pub(crate) null_order: NullOrder,
}

#[derive(Debug, Clone)]
pub(crate) enum Sig {
    Word(String),
    Period,
    Number(String),
    LParen,
    RParen,
    Comma,
    String(String),
    Minus,
    Hex(String),
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrderParseError {
    Empty,
    Unterminated,
    QuotedName,
    MissingName,
    Transform { name: String },
    BadNulls { name: String, got: String },
    Trailing { name: String, got: String },
    EmptySegment { got: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ZOrderScan {
    Absent,
    Columns(Vec<String>),
    Mixed,
}

#[derive(Debug)]
struct SparkOrderDialect(DatabricksDialect);

impl Dialect for SparkOrderDialect {
    fn dialect(&self) -> std::any::TypeId {
        self.0.dialect()
    }
    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        self.0.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        self.0.is_identifier_part(ch)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.0.is_delimited_identifier_start(ch)
    }
    fn is_nested_delimited_identifier_start(&self, ch: char) -> bool {
        self.0.is_nested_delimited_identifier_start(ch)
    }
    fn peek_nested_delimited_identifier_quotes(
        &self,
        chars: std::iter::Peekable<std::str::Chars<'_>>,
    ) -> Option<(char, Option<char>)> {
        self.0.peek_nested_delimited_identifier_quotes(chars)
    }
    fn is_custom_operator_part(&self, ch: char) -> bool {
        self.0.is_custom_operator_part(ch)
    }
    fn supports_triple_quoted_string(&self) -> bool {
        self.0.supports_triple_quoted_string()
    }
    fn supports_quote_delimited_string(&self) -> bool {
        self.0.supports_quote_delimited_string()
    }
    fn supports_string_escape_constant(&self) -> bool {
        self.0.supports_string_escape_constant()
    }
    fn supports_unicode_string_literal(&self) -> bool {
        self.0.supports_unicode_string_literal()
    }
    fn supports_numeric_literal_underscores(&self) -> bool {
        self.0.supports_numeric_literal_underscores()
    }
    fn supports_numeric_prefix(&self) -> bool {
        self.0.supports_numeric_prefix()
    }
    fn supports_multiline_comment_hints(&self) -> bool {
        self.0.supports_multiline_comment_hints()
    }
    fn supports_nested_comments(&self) -> bool {
        self.0.supports_nested_comments()
    }
    fn requires_single_line_comment_whitespace(&self) -> bool {
        self.0.requires_single_line_comment_whitespace()
    }
    fn supports_geometric_types(&self) -> bool {
        self.0.supports_geometric_types()
    }
    fn supports_pipe_operator(&self) -> bool {
        self.0.supports_pipe_operator()
    }
    fn supports_dollar_placeholder(&self) -> bool {
        self.0.supports_dollar_placeholder()
    }
    fn supports_dollar_as_money_prefix(&self) -> bool {
        self.0.supports_dollar_as_money_prefix()
    }
    fn supports_bang_not_operator(&self) -> bool {
        self.0.supports_bang_not_operator()
    }
    fn ignores_wildcard_escapes(&self) -> bool {
        self.0.ignores_wildcard_escapes()
    }
}

pub(crate) fn tokenize_significant(sql: &str) -> Option<Vec<Sig>> {
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
    let tokens = Tokenizer::new(&SparkOrderDialect(DatabricksDialect {}), sql)
        .tokenize_with_location()
        .ok()?;
    let line_starts = line_starts(sql);
    tokens
        .into_iter()
        .filter_map(|with_span| match with_span.token {
            Token::Whitespace(_) | Token::EOF | Token::SemiColon => None,
            Token::Word(word) => Some(Some(Sig::Word(word.value))),
            Token::Period => Some(Some(Sig::Period)),
            Token::Number(raw, _) => Some(Some(Sig::Number(raw))),
            Token::LParen => Some(Some(Sig::LParen)),
            Token::RParen => Some(Some(Sig::RParen)),
            Token::Comma => Some(Some(Sig::Comma)),
            Token::Minus => Some(Some(Sig::Minus)),
            Token::SingleQuotedString(text) | Token::DoubleQuotedString(text) => Some(Some(
                span_text(sql, &line_starts, &with_span.span)
                    .and_then(quoted_body)
                    .map_or(Sig::String(text), |(body, quote)| {
                        Sig::String(unescape_spark_string(body, quote))
                    }),
            )),
            Token::HexStringLiteral(_) => {
                Some(span_text(sql, &line_starts, &with_span.span).map(|typed| {
                    if typed.starts_with("0x") || typed.starts_with("0X") {
                        Sig::Word(typed.to_string())
                    } else {
                        Sig::Hex(typed.to_string())
                    }
                }))
            }
            _ => Some(
                span_text(sql, &line_starts, &with_span.span)
                    .map(|typed| Sig::Other(typed.to_string())),
            ),
        })
        .collect()
}

fn line_starts(sql: &str) -> Vec<usize> {
    std::iter::once(0)
        .chain(sql.match_indices('\n').map(|(offset, _)| offset + 1))
        .collect()
}

fn span_text<'a>(
    sql: &'a str,
    line_starts: &[usize],
    span: &datafusion::sql::sqlparser::tokenizer::Span,
) -> Option<&'a str> {
    let start = location_offset(sql, line_starts, span.start.line, span.start.column)?;
    let end = location_offset(sql, line_starts, span.end.line, span.end.column)?;
    sql.get(start..end)
}

fn location_offset(sql: &str, line_starts: &[usize], line: u64, column: u64) -> Option<usize> {
    let line = usize::try_from(line).ok()?.checked_sub(1)?;
    let column = usize::try_from(column).ok()?.checked_sub(1)?;
    let start = *line_starts.get(line)?;
    let rest = sql.get(start..)?;
    match rest.char_indices().nth(column) {
        Some((offset, _)) => Some(start + offset),
        None => (rest.chars().count() == column).then_some(sql.len()),
    }
}

fn quoted_body(typed: &str) -> Option<(&str, char)> {
    let quote = typed
        .chars()
        .next()
        .filter(|first| matches!(first, '\'' | '"'))?;
    let body = typed.strip_prefix(quote)?.strip_suffix(quote)?;
    Some((body, quote))
}

pub(crate) fn unescape_spark_string(body: &str, quote: char) -> String {
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(character) = chars.next() {
        if character == quote && chars.peek() == Some(&quote) {
            chars.next();
            out.push(quote);
        } else if character == '\\' {
            push_escape(&mut out, &mut chars);
        } else {
            out.push(character);
        }
    }
    out
}

fn push_escape(out: &mut String, chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    let Some(escaped) = chars.next() else {
        out.push('\\');
        return;
    };
    if let Some(decoded) = decode_code_point(chars, escaped) {
        out.push(decoded);
        return;
    }
    match escaped {
        '0' => out.push('\0'),
        'b' => out.push('\u{8}'),
        'n' => out.push('\n'),
        'r' => out.push('\r'),
        't' => out.push('\t'),
        'Z' => out.push('\u{1a}'),
        '%' | '_' => {
            out.push('\\');
            out.push(escaped);
        }
        other => out.push(other),
    }
}

fn decode_code_point(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    escaped: char,
) -> Option<char> {
    let (width, radix) = match escaped {
        'u' => (4, 16),
        'U' => (8, 16),
        '0' | '1' => (2, 8),
        _ => return None,
    };
    let digits: String = chars.clone().take(width).collect();
    if digits.chars().count() != width || !digits.chars().all(|digit| digit.is_digit(radix)) {
        return None;
    }
    let mut value = u32::from_str_radix(&digits, radix).ok()?;
    if radix == 8 {
        value += escaped.to_digit(8)? * 64;
    }
    let decoded = char::from_u32(value)?;
    chars.by_ref().take(width).for_each(drop);
    Some(decoded)
}

pub(crate) fn hex_literal_body(typed: &str) -> Option<&str> {
    typed
        .strip_prefix(['x', 'X'])?
        .strip_prefix('\'')?
        .strip_suffix('\'')
}

pub(crate) fn hex_constant(typed: &str) -> Option<String> {
    let digits = hex_literal_body(typed)?;
    if !digits.chars().all(|digit| digit.is_ascii_hexdigit()) {
        return None;
    }
    let padding = if digits.len() % 2 == 1 { "0" } else { "" };
    Some(format!("0x{padding}{}", digits.to_ascii_uppercase()))
}

pub(crate) fn quote_if_needed(part: &str) -> String {
    let plain = part
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && part
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');
    if plain {
        part.to_string()
    } else {
        format!("`{}`", part.replace('`', "``"))
    }
}

pub(crate) fn quote_constant(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

pub(crate) fn word_eq(significant: &[Sig], index: usize, expected: &str) -> bool {
    word_at(significant, index).is_some_and(|word| word.eq_ignore_ascii_case(expected))
}

pub(crate) fn word_at(significant: &[Sig], index: usize) -> Option<&str> {
    match significant.get(index) {
        Some(Sig::Word(word)) => Some(word.as_str()),
        _ => None,
    }
}

pub(crate) fn is_period_at(significant: &[Sig], index: usize) -> bool {
    matches!(significant.get(index), Some(Sig::Period))
}

pub(crate) fn collect_name_parts(
    significant: &[Sig],
    start: usize,
    end: usize,
) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut index = start;
    while index < end {
        let part = word_at(significant, index)?.to_string();
        parts.push(part);
        index += 1;
        if index < end {
            if !is_period_at(significant, index) {
                return None;
            }
            index += 1;
        }
    }
    if parts.is_empty() { None } else { Some(parts) }
}

pub(crate) fn render_sig_at(significant: &[Sig], index: usize) -> String {
    match significant.get(index) {
        Some(Sig::Word(word)) => word.clone(),
        Some(Sig::Number(number)) => number.clone(),
        Some(Sig::Period) => ".".into(),
        Some(Sig::LParen) => "(".into(),
        Some(Sig::RParen) => ")".into(),
        Some(Sig::Comma) => ",".into(),
        Some(Sig::String(text)) => format!("'{text}'"),
        Some(Sig::Minus) => "-".into(),
        Some(Sig::Hex(typed) | Sig::Other(typed)) => typed.clone(),
        None => "<eof>".into(),
    }
}

pub(crate) fn split_sig_comma_segments(tokens: &[Sig]) -> Vec<&[Sig]> {
    let mut segments = Vec::new();
    let mut depth = 0_i32;
    let mut start = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => depth -= 1,
            Sig::Comma if depth == 0 => {
                if start < index {
                    segments.push(&tokens[start..index]);
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    if start < tokens.len() {
        segments.push(&tokens[start..]);
    }
    segments
}

pub(crate) fn order_list_segments(
    significant: &[Sig],
    start: usize,
) -> Result<(Vec<&[Sig]>, usize), OrderParseError> {
    if !matches!(significant.get(start), Some(Sig::LParen)) {
        let segments = split_order_segments(&significant[start..], "<EOF>")?;
        return non_empty(segments, significant.len());
    }
    let mut depth = 0_i32;
    let mut close = None;
    for (offset, token) in significant.iter().enumerate().skip(start) {
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => {
                depth -= 1;
                if depth == 0 {
                    close = Some(offset);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close.ok_or(OrderParseError::Unterminated)?;
    let segments = split_order_segments(&significant[start + 1..close], ")")?;
    non_empty(segments, close + 1)
}

fn split_order_segments<'a>(
    tokens: &'a [Sig],
    end: &str,
) -> Result<Vec<&'a [Sig]>, OrderParseError> {
    let mut segments = Vec::new();
    let mut depth = 0_i32;
    let mut start = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => depth -= 1,
            Sig::Comma if depth == 0 => {
                if start == index {
                    return Err(OrderParseError::EmptySegment { got: ",".into() });
                }
                segments.push(&tokens[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    if start < tokens.len() {
        segments.push(&tokens[start..]);
    } else if !segments.is_empty() {
        return Err(OrderParseError::EmptySegment { got: end.into() });
    }
    Ok(segments)
}

fn non_empty(segments: Vec<&[Sig]>, next: usize) -> Result<(Vec<&[Sig]>, usize), OrderParseError> {
    if segments.is_empty() {
        return Err(OrderParseError::Empty);
    }
    Ok((segments, next))
}

fn parse_order_segments(segments: &[&[Sig]]) -> Result<Vec<WriteOrderField>, OrderParseError> {
    if segments.is_empty() {
        return Err(OrderParseError::Empty);
    }
    let mut fields = Vec::with_capacity(segments.len());
    for segment in segments {
        fields.push(parse_order_segment(segment)?);
    }
    Ok(fields)
}

pub(crate) fn parse_order_segment(segment: &[Sig]) -> Result<WriteOrderField, OrderParseError> {
    let mut name = match segment.first() {
        Some(Sig::Word(word)) => word.clone(),
        Some(Sig::String(_)) => return Err(OrderParseError::QuotedName),
        _ => return Err(OrderParseError::MissingName),
    };
    let mut index = 1usize;
    while matches!(segment.get(index), Some(Sig::Period)) {
        let Some(Sig::Word(part)) = segment.get(index + 1) else {
            break;
        };
        name.push('.');
        name.push_str(part);
        index += 2;
    }
    if matches!(segment.get(index), Some(Sig::LParen)) {
        return Err(OrderParseError::Transform { name });
    }
    let mut direction = SortDirection::Ascending;
    if word_eq(segment, index, "ASC") {
        index += 1;
    } else if word_eq(segment, index, "DESC") {
        direction = SortDirection::Descending;
        index += 1;
    }
    let mut null_order = match direction {
        SortDirection::Ascending => NullOrder::First,
        SortDirection::Descending => NullOrder::Last,
    };
    if word_eq(segment, index, "NULLS") {
        if word_eq(segment, index + 1, "FIRST") {
            null_order = NullOrder::First;
        } else if word_eq(segment, index + 1, "LAST") {
            null_order = NullOrder::Last;
        } else {
            return Err(OrderParseError::BadNulls {
                name,
                got: render_sig_at(segment, index + 1),
            });
        }
        index += 2;
    }
    if index < segment.len() {
        return Err(OrderParseError::Trailing {
            name,
            got: render_sig_at(segment, index),
        });
    }
    Ok(WriteOrderField {
        name,
        direction,
        null_order,
    })
}

pub(crate) fn parse_identity_sort_order(
    text: &str,
) -> Result<Vec<WriteOrderField>, OrderParseError> {
    let significant = tokenize_significant(text).ok_or(OrderParseError::Empty)?;
    let segments = split_sig_comma_segments(&significant);
    parse_order_segments(&segments)
}

pub(crate) fn parse_zorder_columns(text: &str) -> ZOrderScan {
    let Some(significant) = tokenize_significant(text) else {
        return ZOrderScan::Absent;
    };
    let segments = split_sig_comma_segments(&significant);
    if segments.is_empty() {
        return ZOrderScan::Absent;
    }
    let mut columns = Vec::new();
    let mut zorder_terms = 0usize;
    let mut identity_terms = 0usize;
    for segment in segments {
        match zorder_segment_columns(segment) {
            Some(mut names) => {
                zorder_terms += 1;
                columns.append(&mut names);
            }
            None => identity_terms += 1,
        }
    }
    if zorder_terms == 0 {
        return ZOrderScan::Absent;
    }
    if identity_terms > 0 {
        return ZOrderScan::Mixed;
    }
    ZOrderScan::Columns(columns)
}

fn zorder_segment_columns(segment: &[Sig]) -> Option<Vec<String>> {
    if !word_eq(segment, 0, "zorder") || !matches!(segment.get(1), Some(Sig::LParen)) {
        return None;
    }
    let close = segment
        .iter()
        .rposition(|token| matches!(token, Sig::RParen))?;
    let inner = split_sig_comma_segments(&segment[2..close]);
    let mut names = Vec::with_capacity(inner.len());
    for term in inner {
        let mut name = match term.first() {
            Some(Sig::Word(word)) => word.clone(),
            _ => return None,
        };
        let mut index = 1usize;
        while matches!(term.get(index), Some(Sig::Period)) {
            let Some(Sig::Word(part)) = term.get(index + 1) else {
                return None;
            };
            name.push('.');
            name.push_str(part);
            index += 2;
        }
        if index < term.len() {
            return None;
        }
        names.push(name);
    }
    Some(names)
}
