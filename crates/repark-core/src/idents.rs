//! Table-identifier segment parsing and path-escape refuse (identity boundary).

/// Split a multipart table identifier with quote awareness (double-quote / backtick).
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
                    if index + 1 < length && chars[index + 1] == quote {
                        buffer.push(quote);
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
        } else if char.is_ascii_alphanumeric() || char == '_' {
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
            if spark_numeric_literal(&segment) {
                return Err(format!(
                    "numeric literal {segment:?} is not a table identifier segment"
                ));
            }
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

#[must_use]
pub fn spark_numeric_literal(word: &str) -> bool {
    let digits = word.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return false;
    }
    let rest = word[digits..].to_ascii_uppercase();
    if let Some(exponent) = rest.strip_prefix('E') {
        let exponent_digits = exponent.bytes().take_while(u8::is_ascii_digit).count();
        return exponent_digits > 0
            && matches!(&exponent[exponent_digits..], "" | "D" | "F" | "BD");
    }
    matches!(rest.as_str(), "" | "L" | "S" | "Y" | "D" | "F" | "BD")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Lexeme {
    Word(String),
    Backquoted,
    Quoted { raw: String, single: bool },
    Punct(char),
}

#[derive(Debug, Clone, Copy)]
enum QuotedAfterDot {
    AtDot,
    AtSegment,
    ExtraInput,
}

fn skip_comment(chars: &[char], index: usize) -> Option<usize> {
    match (chars.get(index), chars.get(index + 1)) {
        (Some('-'), Some('-')) => Some(
            chars[index..]
                .iter()
                .position(|char| *char == '\n')
                .map_or(chars.len(), |offset| index + offset + 1),
        ),
        (Some('/'), Some('*')) => {
            let mut depth = 0usize;
            let mut cursor = index;
            while cursor < chars.len() {
                match (chars[cursor], chars.get(cursor + 1)) {
                    ('/', Some('*')) => {
                        depth += 1;
                        cursor += 2;
                    }
                    ('*', Some('/')) => {
                        depth -= 1;
                        cursor += 2;
                        if depth == 0 {
                            return Some(cursor);
                        }
                    }
                    _ => cursor += 1,
                }
            }
            Some(chars.len())
        }
        _ => None,
    }
}

fn quoted_end(chars: &[char], index: usize, quote: char, escapes: bool) -> usize {
    let mut cursor = index + 1;
    while cursor < chars.len() {
        let char = chars[cursor];
        if escapes && char == '\\' {
            cursor += 2;
            continue;
        }
        if char == quote {
            if chars.get(cursor + 1) == Some(&quote) {
                cursor += 2;
                continue;
            }
            return cursor + 1;
        }
        cursor += 1;
    }
    chars.len()
}

fn spark_lexemes(sql: &str) -> Vec<Lexeme> {
    let chars: Vec<char> = sql.chars().collect();
    let mut lexemes = Vec::new();
    let mut index = 0usize;
    while index < chars.len() {
        let char = chars[index];
        if char.is_whitespace() {
            index += 1;
        } else if let Some(end) = skip_comment(&chars, index) {
            index = end;
        } else if char == '`' {
            index = quoted_end(&chars, index, '`', false);
            lexemes.push(Lexeme::Backquoted);
        } else if char == '\'' || char == '"' {
            let end = quoted_end(&chars, index, char, true);
            lexemes.push(Lexeme::Quoted {
                raw: chars[index..end].iter().collect(),
                single: char == '\'',
            });
            index = end;
        } else if char.is_alphanumeric() || char == '_' {
            let start = index;
            while index < chars.len() && (chars[index].is_alphanumeric() || chars[index] == '_') {
                index += 1;
            }
            lexemes.push(Lexeme::Word(chars[start..index].iter().collect()));
        } else {
            lexemes.push(Lexeme::Punct(char));
            index += 1;
        }
    }
    lexemes
}

fn word_is(lexeme: Option<&Lexeme>, keyword: &str) -> bool {
    matches!(lexeme, Some(Lexeme::Word(word)) if word.eq_ignore_ascii_case(keyword))
}

fn parse_near(near: &str) -> String {
    repark_common::spark_error::message(
        repark_common::spark_error::PARSE_SYNTAX_ERROR,
        &[("near", near)],
    )
}

fn invalid_identifier(word: &str) -> String {
    repark_common::spark_error::message(
        repark_common::spark_error::INVALID_IDENTIFIER,
        &[("ident", word)],
    )
}

fn read_identifier(
    lexemes: &[Lexeme],
    start: usize,
    quoted_after_dot: QuotedAfterDot,
) -> std::result::Result<Option<usize>, String> {
    let mut index = start;
    loop {
        match lexemes.get(index) {
            Some(Lexeme::Word(word)) if !word.is_ascii() => return Err(invalid_identifier(word)),
            Some(Lexeme::Word(word)) if index == start && spark_numeric_literal(word) => {
                return Err(parse_near(&format!("'{word}'")));
            }
            Some(Lexeme::Word(_) | Lexeme::Backquoted) => {}
            Some(Lexeme::Quoted { raw, single }) => {
                let near = format!("'{raw}'");
                return Err(parse_near(
                    &match (*single, index == start, quoted_after_dot) {
                        (true, false, QuotedAfterDot::AtDot) => "'.'".to_string(),
                        (true, true, QuotedAfterDot::ExtraInput) => {
                            format!("{near}: extra input {near}")
                        }
                        _ => near,
                    },
                ));
            }
            _ => return Ok(None),
        }
        index += 1;
        if lexemes.get(index) != Some(&Lexeme::Punct('.')) {
            return Ok(Some(index));
        }
        index += 1;
    }
}

fn head_target(lexemes: &[Lexeme]) -> Option<(usize, bool, QuotedAfterDot)> {
    let first = lexemes.first();
    let second = lexemes.get(1);
    let skip_if_exists = |start: usize| {
        if word_is(lexemes.get(start), "IF") && word_is(lexemes.get(start + 1), "EXISTS") {
            start + 2
        } else {
            start
        }
    };
    let skip_table = |start: usize| start + usize::from(word_is(lexemes.get(start), "TABLE"));
    if word_is(first, "DROP") && (word_is(second, "TABLE") || word_is(second, "VIEW")) {
        Some((skip_if_exists(2), false, QuotedAfterDot::AtDot))
    } else if (word_is(first, "TRUNCATE") && word_is(second, "TABLE"))
        || (word_is(first, "DELETE") && word_is(second, "FROM"))
    {
        Some((2, false, QuotedAfterDot::AtDot))
    } else if word_is(first, "UPDATE") {
        Some((1, true, QuotedAfterDot::AtDot))
    } else if word_is(first, "MERGE") && word_is(second, "INTO") {
        Some((2, true, QuotedAfterDot::AtDot))
    } else if word_is(first, "INSERT") && word_is(second, "INTO") {
        Some((skip_table(2), true, QuotedAfterDot::AtSegment))
    } else if word_is(first, "INSERT") && word_is(second, "OVERWRITE") {
        Some((skip_table(2), true, QuotedAfterDot::AtDot))
    } else if word_is(first, "ALTER") && (word_is(second, "TABLE") || word_is(second, "VIEW")) {
        Some((skip_if_exists(2), true, QuotedAfterDot::AtSegment))
    } else {
        None
    }
}

fn head_target_refusal(lexemes: &[Lexeme]) -> Option<String> {
    let (start, continued, quoted_after_dot) = head_target(lexemes)?;
    match read_identifier(lexemes, start, quoted_after_dot) {
        Err(message) => Some(message),
        Ok(Some(end)) => match lexemes.get(end) {
            Some(Lexeme::Punct(',')) => Some(parse_near("','")),
            None if continued => Some(parse_near("end of input")),
            _ => None,
        },
        Ok(None) => None,
    }
}

fn relation_refusal(lexemes: &[Lexeme]) -> Option<String> {
    let mut query_parens: Vec<bool> = Vec::new();
    for (index, lexeme) in lexemes.iter().enumerate() {
        match lexeme {
            Lexeme::Punct('(') => query_parens.push(
                ["SELECT", "WITH", "FROM", "VALUES"]
                    .iter()
                    .any(|keyword| word_is(lexemes.get(index + 1), keyword)),
            ),
            Lexeme::Punct(')') => {
                query_parens.pop();
            }
            Lexeme::Word(_)
                if (word_is(Some(lexeme), "FROM") || word_is(Some(lexeme), "JOIN"))
                    && query_parens.last().copied().unwrap_or(true)
                    && !(index > 0 && word_is(lexemes.get(index - 1), "DISTINCT")) =>
            {
                if let Err(message) =
                    read_identifier(lexemes, index + 1, QuotedAfterDot::ExtraInput)
                {
                    return Some(message);
                }
            }
            _ => {}
        }
    }
    None
}

#[must_use]
pub fn spark_target_grammar_message(sql: &str) -> Option<String> {
    let lexemes = spark_lexemes(sql);
    let statement = lexemes
        .iter()
        .position(|lexeme| *lexeme != Lexeme::Punct(';'))
        .map_or(&lexemes[..0], |start| &lexemes[start..]);
    head_target_refusal(statement).or_else(|| relation_refusal(statement))
}
