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
    Other,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ZOrderScan {
    Absent,
    Columns(Vec<String>),
    Mixed,
}

pub(crate) fn tokenize_significant(sql: &str) -> Option<Vec<Sig>> {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    Some(
        tokens
            .into_iter()
            .filter_map(|token| match token {
                Token::Whitespace(_) | Token::EOF | Token::SemiColon => None,
                Token::Word(word) => Some(Sig::Word(word.value)),
                Token::Period => Some(Sig::Period),
                Token::Number(raw, _) => Some(Sig::Number(raw)),
                Token::LParen => Some(Sig::LParen),
                Token::RParen => Some(Sig::RParen),
                Token::Comma => Some(Sig::Comma),
                Token::Minus => Some(Sig::Minus),
                Token::SingleQuotedString(text) | Token::DoubleQuotedString(text) => {
                    Some(Sig::String(text))
                }
                _ => Some(Sig::Other),
            })
            .collect(),
    )
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
        Some(Sig::Other) => "<other>".into(),
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
        let segments = split_sig_comma_segments(&significant[start..]);
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
    let segments = split_sig_comma_segments(&significant[start + 1..close]);
    non_empty(segments, close + 1)
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
