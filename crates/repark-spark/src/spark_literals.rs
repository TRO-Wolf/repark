//! Spark string-literal canonicalisation at the Spark SQL front door.

use std::any::TypeId;
use std::borrow::Cow;
use std::iter::Peekable;
use std::str::Chars;

use datafusion::common::{Diagnostic, Span as DataFusionSpan};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

/// Spark's measured replacement for an unrepresentable code point.
const UNREPRESENTABLE: char = '\u{003F}';

/// Generic lexing with Spark's single-quoted backslash behavior.
#[derive(Debug)]
struct SparkLexDialect(GenericDialect);

impl Dialect for SparkLexDialect {
    /// Preserve Generic identity for sqlparser's concrete-type prefix checks.
    fn dialect(&self) -> TypeId {
        self.0.dialect()
    }

    /// Let a backslash escape the next character inside a single-quoted string.
    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    // All other tokenization decisions stay Generic.
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
        chars: Peekable<Chars<'_>>,
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
    fn ignores_wildcard_escapes(&self) -> bool {
        self.0.ignores_wildcard_escapes()
    }
}

/// Rewrite every single-quoted literal to the value Spark 4.1.2's lexer would produce.
/// # Errors
/// # Errors [`DataFusionError::SQL`] with the lexer's line/column when the text does not tokenise.
pub(crate) fn canonicalize(sql: &str) -> Result<Cow<'_, str>> {
    // Neither a `'` nor a `\`: Spark's lexer and Generic agree, so borrow without tokenising.
    if !sql.as_bytes().contains(&b'\'') && !sql.as_bytes().contains(&b'\\') {
        return Ok(Cow::Borrowed(sql));
    }
    match canonical_rewrite(sql)? {
        Some(rewrite) => Ok(Cow::Owned(rewrite.sql)),
        None => Ok(Cow::Borrowed(sql)),
    }
}

/// Translate a downstream parser location from canonical text to the caller's SQL.
pub(crate) fn translate_downstream_error(
    original: &str,
    canonical: &str,
    error: DataFusionError,
) -> DataFusionError {
    let Ok(Some(rewrite)) = canonical_rewrite(original) else {
        return error;
    };
    if rewrite.sql != canonical {
        return error;
    }
    translate_parser_error(error, &rewrite)
}

fn translate_parser_error(error: DataFusionError, rewrite: &CanonicalRewrite) -> DataFusionError {
    match error {
        DataFusionError::SQL(parser_error, backtrace) => match *parser_error {
            ParserError::ParserError(message) => {
                let translated = rewrite_parser_location(&message, rewrite).unwrap_or(message);
                DataFusionError::SQL(Box::new(ParserError::ParserError(translated)), backtrace)
            }
            other => DataFusionError::SQL(Box::new(other), backtrace),
        },
        DataFusionError::Diagnostic(diagnostic, inner) => match *inner {
            DataFusionError::SQL(parser_error, backtrace) => DataFusionError::Diagnostic(
                Box::new(translate_diagnostic(*diagnostic, rewrite)),
                Box::new(translate_parser_error(
                    DataFusionError::SQL(parser_error, backtrace),
                    rewrite,
                )),
            ),
            other => DataFusionError::Diagnostic(diagnostic, Box::new(other)),
        },
        other => other,
    }
}

fn translate_diagnostic(mut diagnostic: Diagnostic, rewrite: &CanonicalRewrite) -> Diagnostic {
    diagnostic.span = diagnostic
        .span
        .map(|span| translate_datafusion_span(span, rewrite).unwrap_or(span));
    for note in &mut diagnostic.notes {
        note.span = note
            .span
            .map(|span| translate_datafusion_span(span, rewrite).unwrap_or(span));
    }
    for help in &mut diagnostic.helps {
        help.span = help
            .span
            .map(|span| translate_datafusion_span(span, rewrite).unwrap_or(span));
    }
    diagnostic
}

fn translate_datafusion_span(
    span: DataFusionSpan,
    rewrite: &CanonicalRewrite,
) -> Option<DataFusionSpan> {
    let start = rewrite.original_location(Location {
        line: span.start.line,
        column: span.start.column,
    })?;
    let end = rewrite.original_location(Location {
        line: span.end.line,
        column: span.end.column,
    })?;
    Some(DataFusionSpan::new(start.into(), end.into()))
}

fn canonical_rewrite(sql: &str) -> Result<Option<CanonicalRewrite>> {
    // `with_unescape(false)` keeps the raw between-quote text so this module applies Spark's rules.
    let tokens = Tokenizer::new(&SparkLexDialect(GenericDialect {}), sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|error| DataFusionError::SQL(Box::new(ParserError::from(error)), None))?;
    // COPY and CREATE [OR REPLACE] EXTERNAL TABLE are DataFusion-native, not Spark.
    if is_datafusion_native_statement(&tokens) {
        return Ok(None);
    }
    let regions = plan_literal_regions(&tokens);
    if regions.is_empty() {
        return Ok(None);
    }
    Ok(Some(apply_regions(sql, &regions)))
}

/// The leading significant word tokens, up to `max`; stops at the first non-word token.
fn leading_significant_words(tokens: &[TokenWithSpan], max: usize) -> Vec<&str> {
    let mut words: Vec<&str> = Vec::new();
    for token in tokens {
        match &token.token {
            Token::Whitespace(_) => {}
            Token::Word(word) => {
                words.push(word.value.as_str());
                if words.len() == max {
                    break;
                }
            }
            _ => break,
        }
    }
    words
}

/// True for a `COPY …` or `CREATE [OR REPLACE] EXTERNAL TABLE …` statement.
fn is_datafusion_native_statement(tokens: &[TokenWithSpan]) -> bool {
    let words = leading_significant_words(tokens, 5);
    let eq = |a: &&str, b: &str| a.eq_ignore_ascii_case(b);
    match words.as_slice() {
        [copy, ..] if eq(copy, "COPY") => true,
        [create, external, table, ..]
            if eq(create, "CREATE") && eq(external, "EXTERNAL") && eq(table, "TABLE") =>
        {
            true
        }
        [create, or_, replace, external, table]
            if eq(create, "CREATE")
                && eq(or_, "OR")
                && eq(replace, "REPLACE")
                && eq(external, "EXTERNAL")
                && eq(table, "TABLE") =>
        {
            true
        }
        _ => false,
    }
}

/// A source span to swap in for a canonicalised literal group.
struct LiteralRegion {
    start: Location,
    end: Location,
    replacement: String,
}

/// Canonical SQL plus the original location of each output character and the output EOF.
struct CanonicalRewrite {
    sql: String,
    original_locations: Vec<Location>,
}

impl CanonicalRewrite {
    fn original_location(&self, target: Location) -> Option<Location> {
        let mut canonical = Location { line: 1, column: 1 };
        for (character, original) in self.sql.chars().zip(&self.original_locations) {
            if canonical == target {
                return Some(*original);
            }
            advance_location(&mut canonical, character);
        }
        if canonical == target {
            self.original_locations.last().copied()
        } else {
            None
        }
    }
}

/// Collect the literal spans that must change.
fn plan_literal_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let Some(first_value) = literal_token_value(&tokens[index].token) else {
            index += 1;
            continue;
        };
        let start = tokens[index].span.start;
        let mut end = tokens[index].span.end;
        let mut merged = first_value;
        let mut literal_count = 1usize;
        let single_needs_rewrite = literal_needs_rewrite(&tokens[index].token);
        // Absorb following literals separated only by whitespace.
        let mut cursor = index + 1;
        loop {
            let mut lookahead = cursor;
            while lookahead < tokens.len()
                && matches!(tokens[lookahead].token, Token::Whitespace(_))
            {
                lookahead += 1;
            }
            let Some(next_value) = tokens
                .get(lookahead)
                .and_then(|t| literal_token_value(&t.token))
            else {
                break;
            };
            merged.push_str(&next_value);
            end = tokens[lookahead].span.end;
            literal_count += 1;
            cursor = lookahead + 1;
        }
        if literal_count > 1 || single_needs_rewrite {
            regions.push(LiteralRegion {
                start,
                end,
                replacement: requote_generic(&merged),
            });
        }
        index = cursor;
    }
    regions
}

/// The Spark value of a single-quoted literal token (raw strings verbatim, E19), or `None`.
fn literal_token_value(token: &Token) -> Option<String> {
    match token {
        Token::SingleQuotedString(raw) => Some(unescape_spark_literal(raw)),
        Token::SingleQuotedRawStringLiteral(raw) => Some(raw.clone()),
        _ => None,
    }
}

/// True when a single literal alone produces different text than the downstream Generic lexer.
fn literal_needs_rewrite(token: &Token) -> bool {
    match token {
        Token::SingleQuotedString(raw) => raw.contains('\\'),
        Token::SingleQuotedRawStringLiteral(_) => true,
        _ => false,
    }
}

/// Re-quote a finished Spark string value as Generic-canonical: wrap in `'…'`, double every `'`.
fn requote_generic(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for character in value.chars() {
        if character == '\'' {
            out.push('\'');
        }
        out.push(character);
    }
    out.push('\'');
    out
}

/// Rebuild `sql`, replacing each [`LiteralRegion`].
fn apply_regions(sql: &str, regions: &[LiteralRegion]) -> CanonicalRewrite {
    let mut out = String::with_capacity(sql.len());
    let mut original_locations = Vec::with_capacity(sql.chars().count() + 1);
    let mut regions = regions.iter().peekable();
    let mut line = 1u64;
    let mut column = 1u64;
    let mut skip_until: Option<Location> = None;
    for character in sql.chars() {
        let here = Location { line, column };
        // A region ends at the location of the first character AFTER its closing quote.
        if skip_until == Some(here) {
            skip_until = None;
        }
        if skip_until.is_none()
            && let Some(region) = regions.peek()
            && region.start == here
        {
            for replacement_character in region.replacement.chars() {
                out.push(replacement_character);
                original_locations.push(region.start);
            }
            skip_until = Some(region.end);
            regions.next();
        }
        if skip_until.is_none() {
            out.push(character);
            original_locations.push(here);
        }
        if character == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    original_locations.push(Location { line, column });
    CanonicalRewrite {
        sql: out,
        original_locations,
    }
}

fn advance_location(location: &mut Location, character: char) {
    if character == '\n' {
        location.line += 1;
        location.column = 1;
    } else {
        location.column += 1;
    }
}

fn rewrite_parser_location(message: &str, rewrite: &CanonicalRewrite) -> Option<String> {
    const LINE_MARKER: &str = " at Line: ";
    const COLUMN_MARKER: &str = ", Column: ";
    let Some(marker) = message.rfind(LINE_MARKER) else {
        return rewrite_unlocated_eof(message, rewrite);
    };
    let line_start = marker + LINE_MARKER.len();
    let line_end = ascii_digit_end(message, line_start);
    message.get(line_end..)?.strip_prefix(COLUMN_MARKER)?;
    let column_start = line_end + COLUMN_MARKER.len();
    let column_end = ascii_digit_end(message, column_start);
    let canonical = Location {
        line: message.get(line_start..line_end)?.parse().ok()?,
        column: message.get(column_start..column_end)?.parse().ok()?,
    };
    let original = rewrite.original_location(canonical)?;
    Some(format!(
        "{}{}{}{}{}",
        &message[..line_start],
        original.line,
        COLUMN_MARKER,
        original.column,
        &message[column_end..]
    ))
}

fn rewrite_unlocated_eof(message: &str, rewrite: &CanonicalRewrite) -> Option<String> {
    if !message.ends_with("found: EOF") {
        return None;
    }
    let original_eof = rewrite.original_locations.last()?;
    Some(format!("{message}{original_eof}"))
}

fn ascii_digit_end(text: &str, start: usize) -> usize {
    text.as_bytes()[start..]
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .map_or(text.len(), |offset| start + offset)
}

/// Apply Spark 4.1.2's escape rules to the raw between-quote text `raw`.
fn unescape_spark_literal(raw: &str) -> String {
    let characters: Vec<char> = raw.chars().collect();
    let mut out = String::with_capacity(raw.len());
    let mut index = 0;
    while index < characters.len() {
        let current = characters[index];
        if current == '\'' {
            // A `'` here is only ever the first of a doubled `''`; a lone `'` ends the literal.
            out.push('\'');
            index += if characters.get(index + 1) == Some(&'\'') {
                2
            } else {
                1
            };
            continue;
        }
        if current != '\\' {
            out.push(current);
            index += 1;
            continue;
        }
        // The lexer refuses `'a\'` as unterminated before this runs.
        let Some(&escaped) = characters.get(index + 1) else {
            out.push('\\');
            index += 1;
            continue;
        };
        index = apply_escape(escaped, &characters, index, &mut out);
    }
    out
}

/// Handle one `\<escaped>` sequence starting at `index`; returns the next unconsumed index.
fn apply_escape(escaped: char, characters: &[char], index: usize, out: &mut String) -> usize {
    match escaped {
        'n' => push_and_advance('\n', index, out),
        't' => push_and_advance('\t', index, out),
        'r' => push_and_advance('\r', index, out),
        'b' => push_and_advance('\u{0008}', index, out),
        'Z' => push_and_advance('\u{001A}', index, out),
        // `\%` and `\_` keep the backslash: Spark's LIKE reads the escaped wildcard (E12).
        '%' => push_kept_backslash('%', index, out),
        '_' => push_kept_backslash('_', index, out),
        'u' => apply_unicode_16(characters, index, out),
        'U' => apply_unicode_32(characters, index, out),
        '0'..='7' => apply_octal(escaped, characters, index, out),
        // Any other escape drops the backslash and keeps the character.
        other => push_and_advance(other, index, out),
    }
}

/// Emit `character` for a two-character escape (`\` plus one) and step past both.
fn push_and_advance(character: char, index: usize, out: &mut String) -> usize {
    out.push(character);
    index + 2
}

/// Emit `\<wildcard>` verbatim — the E12 rule where the backslash is kept for LIKE.
fn push_kept_backslash(wildcard: char, index: usize, out: &mut String) -> usize {
    out.push('\\');
    out.push(wildcard);
    index + 2
}

/// `\NNN` octal (E11, E27, U13).
fn apply_octal(first: char, characters: &[char], index: usize, out: &mut String) -> usize {
    let second = characters.get(index + 2).copied();
    let third = characters.get(index + 3).copied();
    if matches!(first, '0'..='1')
        && let Some(second) = second.filter(|c| c.is_digit(8))
        && let Some(third) = third.filter(|c| c.is_digit(8))
    {
        // Each digit is 0..=7 and the value is ≤ 0o177, so it is a valid ASCII byte.
        let value = ((octal_value(first)) << 6) | (octal_value(second) << 3) | octal_value(third);
        out.push(char::from(value));
        return index + 4;
    }
    if first == '0' {
        out.push('\0');
        return index + 2;
    }
    // A single octal digit 1..=7 (or a short run) is an unknown escape: drop the backslash.
    out.push(first);
    index + 2
}

/// The numeric value of one ASCII octal digit (`'0'..='7'`); the caller has checked the range.
fn octal_value(digit: char) -> u8 {
    (digit as u8).saturating_sub(b'0')
}

/// `\uXXXX` (exactly 4 hex → a code point).
fn apply_unicode_16(characters: &[char], index: usize, out: &mut String) -> usize {
    let Some(high) = read_hex(characters, index + 2, 4) else {
        out.push('u');
        return index + 2;
    };
    if (0xD800..=0xDBFF).contains(&high)
        && characters.get(index + 6) == Some(&'\\')
        && characters.get(index + 7) == Some(&'u')
        && let Some(low) =
            read_hex(characters, index + 8, 4).filter(|v| (0xDC00..=0xDFFF).contains(v))
    {
        let combined = 0x1_0000 + ((high - 0xD800) << 10) + (low - 0xDC00);
        push_code_point(combined, out);
        return index + 12;
    }
    push_code_point(high, out);
    index + 6
}

/// `\UXXXXXXXX` (exactly 8 hex → a code point; U5).
fn apply_unicode_32(characters: &[char], index: usize, out: &mut String) -> usize {
    let Some(value) = read_hex(characters, index + 2, 8) else {
        out.push('U');
        return index + 2;
    };
    push_code_point(value, out);
    index + 10
}

/// Read exactly `count` hex digits from `start`, or `None` if fewer are present.
fn read_hex(characters: &[char], start: usize, count: usize) -> Option<u32> {
    let end = start.checked_add(count)?;
    let slice = characters.get(start..end)?;
    let mut value = 0u32;
    for digit in slice {
        value = value * 16 + digit.to_digit(16)?;
    }
    Some(value)
}

/// Emit the character for `code_point`, or [`UNREPRESENTABLE`] when it is not a Unicode scalar.
fn push_code_point(code_point: u32, out: &mut String) {
    match char::from_u32(code_point) {
        Some(character) => out.push(character),
        None => out.push(UNREPRESENTABLE),
    }
}

#[cfg(test)]
mod location_translation_tests {
    use super::*;
    use std::sync::Arc;

    fn parser_error(message: &str) -> DataFusionError {
        DataFusionError::SQL(
            Box::new(ParserError::ParserError(message.to_string())),
            None,
        )
    }

    #[test]
    fn expansion_and_mixed_regions_map_to_original_locations() {
        let cases = [
            (
                "SELECT '\\n' AS shifted, )",
                "Expected: end of statement, found: ) at Line: 2, Column: 15",
                "Line: 1, Column: 25",
            ),
            (
                "SELECT '\\u0027' AS expanded, '\\u0061' AS shrunk, )",
                "Expected: end of statement, found: ) at Line: 1, Column: 41",
                "Line: 1, Column: 50",
            ),
        ];
        for (original, message, expected) in cases {
            let canonical = canonicalize(original).expect("test SQL canonicalizes");
            let translated =
                translate_downstream_error(original, canonical.as_ref(), parser_error(message));
            assert!(translated.to_string().contains(expected), "{translated}");
        }
    }

    #[test]
    fn direct_eof_parser_errors_map_to_original_eof() {
        let original = "SELECT '\\u0061' +";
        let canonical = canonicalize(original).expect("test SQL canonicalizes");
        let error = parser_error("Expected: an expression, found: EOF");
        let translated = translate_downstream_error(original, canonical.as_ref(), error);
        assert!(
            translated.to_string().contains("Line: 1, Column: 18"),
            "{translated}"
        );
    }

    #[test]
    fn spark_passthrough_parser_boundary_returns_only_reachable_parser_variants() {
        let context = datafusion::execution::context::SessionContext::new();
        let state = context.state();
        let dialect = state.config().options().sql_parser.dialect;
        let cases = [
            ("SELECT '\\u0061' + )", false),
            ("SELECT '\\n' AS shifted, )", true),
            ("SELECT '\\u0027' AS expanded, '\\u0061' AS shrunk, )", true),
        ];
        for (original, has_diagnostic) in cases {
            let canonical = canonicalize(original).expect("test SQL canonicalizes");
            let error = state
                .sql_to_statement(canonical.as_ref(), &dialect)
                .expect_err("invalid canonical SQL must fail at the parser boundary");
            match error {
                DataFusionError::SQL(parser_error, _) if !has_diagnostic => {
                    assert!(matches!(*parser_error, ParserError::ParserError(_)));
                }
                DataFusionError::Diagnostic(_, inner) if has_diagnostic => {
                    assert!(matches!(*inner, DataFusionError::SQL(_, _)));
                }
                other => panic!("unexpected parser boundary variant: {other:?}"),
            }
        }
    }

    #[test]
    fn unsupported_shared_error_tree_stays_identical() {
        let original = "SELECT '\\u0061' + )";
        let canonical = canonicalize(original).expect("test SQL canonicalizes");
        let shared = Arc::new(DataFusionError::Collection(vec![
            parser_error("Expected: an expression, found: ) at Line: 1, Column: 14"),
            DataFusionError::NotImplemented("unsupported sibling".to_string()),
        ]));
        let retained = Arc::clone(&shared);
        let translated = translate_downstream_error(
            original,
            canonical.as_ref(),
            DataFusionError::Shared(Arc::clone(&shared)),
        );
        let DataFusionError::Shared(translated) = translated else {
            panic!("expected shared wrapper");
        };
        assert!(Arc::ptr_eq(&translated, &shared));
        assert!(retained.to_string().contains("Line: 1, Column: 14"));
    }

    #[test]
    fn tokenizer_and_unlocated_non_eof_errors_stay_unchanged() {
        let original = "SELECT '\\u0061' + )";
        let canonical = canonicalize(original).expect("test SQL canonicalizes");
        let tokenizer = DataFusionError::SQL(
            Box::new(ParserError::TokenizerError(
                "tokenizer location".to_string(),
            )),
            None,
        );
        let tokenizer = translate_downstream_error(original, canonical.as_ref(), tokenizer);
        assert_eq!(
            tokenizer.to_string(),
            "SQL error: TokenizerError(\"tokenizer location\")"
        );
        let parser = parser_error("Expected: an expression");
        let parser = translate_downstream_error(original, canonical.as_ref(), parser);
        assert_eq!(
            parser.to_string(),
            "SQL error: ParserError(\"Expected: an expression\")"
        );
    }
}
