//! The rules (Spark 4.1.2, `<pyspark-4.1.2-oracle>`): backslash KEPT; one astral char.
use std::any::TypeId;
use std::borrow::Cow;
use std::iter::Peekable;
use std::str::Chars;

use datafusion::common::config::ConfigExtension;
use datafusion::common::{Diagnostic, Span as DataFusionSpan};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionConfig;
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

/// Spark's measured replacement for an unrepresentable code point.
const UNREPRESENTABLE: char = '\u{003F}';

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
        ch == '`'
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

/// # Errors
/// # Errors [`DataFusionError::SQL`] with the lexer's line/column when the text does not tokenise.
pub fn canonicalize(sql: &str) -> Result<Cow<'_, str>> {
    canonicalize_verbatim(sql, false)
}

pub(crate) fn canonicalize_verbatim(sql: &str, keep_verbatim: bool) -> Result<Cow<'_, str>> {
    if !sql.as_bytes().contains(&b'\'')
        && !sql.as_bytes().contains(&b'"')
        && !sql.as_bytes().contains(&b'\\')
        && !sql_may_have_numeric_suffix(sql)
        && !sql_may_have_drop_temporary(sql)
        && !sql_may_have_wildcard_exclude(sql)
    {
        return Ok(Cow::Borrowed(sql));
    }
    match canonical_rewrite(sql, keep_verbatim)? {
        Some(rewrite) => Ok(Cow::Owned(rewrite.sql)),
        None => Ok(Cow::Borrowed(sql)),
    }
}

fn sql_may_have_wildcard_exclude(sql: &str) -> bool {
    sql.as_bytes().contains(&b'*') && sql.to_ascii_lowercase().contains("exclude")
}

fn sql_may_have_drop_temporary(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    lower.contains("drop") && lower.contains("temporary")
}

fn sql_may_have_numeric_suffix(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    bytes.windows(2).any(|pair| {
        pair[0].is_ascii_digit()
            && matches!(
                pair[1],
                b'd' | b'D'
                    | b'f'
                    | b'F'
                    | b's'
                    | b'S'
                    | b'Y'
                    | b'y'
                    | b'L'
                    | b'l'
                    | b'B'
                    | b'b'
                    | b'e'
                    | b'E'
            )
    })
}

/// Translate a downstream parser location from canonical text to the caller's SQL.
#[must_use]
pub fn translate_downstream_error(
    original: &str,
    canonical: &str,
    error: DataFusionError,
) -> DataFusionError {
    translate_downstream_error_verbatim(original, canonical, error, false)
}

pub(crate) fn translate_downstream_error_verbatim(
    original: &str,
    canonical: &str,
    error: DataFusionError,
    keep_verbatim: bool,
) -> DataFusionError {
    let Ok(Some(rewrite)) = canonical_rewrite(original, keep_verbatim) else {
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

fn canonical_rewrite(sql: &str, keep_verbatim: bool) -> Result<Option<CanonicalRewrite>> {
    // `with_unescape(false)` keeps the raw between-quote text so this module applies Spark's rules.
    let tokens = Tokenizer::new(&SparkLexDialect(GenericDialect {}), sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|error| DataFusionError::SQL(Box::new(ParserError::from(error)), None))?;
    // COPY and CREATE [OR REPLACE] EXTERNAL TABLE are DataFusion-native, not Spark.
    if is_datafusion_native_statement(&tokens) {
        return Ok(None);
    }
    let mut regions = plan_literal_regions(&tokens, keep_verbatim);
    regions.extend(crate::spark_rewrites::plan_suffix_regions(&tokens));
    regions.extend(crate::spark_rewrites::plan_drop_temporary_regions(&tokens));
    regions.extend(crate::spark_rewrites::plan_wildcard_except_regions(&tokens));
    crate::spark_rewrites::plan_struct_field_regions(&tokens, sql, &mut regions);
    regions.sort_by_key(|region| (region.start.line, region.start.column));
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
pub(crate) struct LiteralRegion {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) replacement: String,
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
fn plan_literal_regions(tokens: &[TokenWithSpan], keep_verbatim: bool) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let Some(first_value) = literal_token_value(&tokens[index].token, keep_verbatim) else {
            index += 1;
            continue;
        };
        let start = tokens[index].span.start;
        let mut end = tokens[index].span.end;
        let mut merged = first_value;
        let mut literal_count = 1usize;
        let single_is_double = matches!(tokens[index].token, Token::DoubleQuotedString(_));
        let single_needs_rewrite = literal_needs_rewrite(&tokens[index].token, keep_verbatim);
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
                .and_then(|t| literal_token_value(&t.token, keep_verbatim))
            else {
                break;
            };
            merged.push_str(&next_value);
            end = tokens[lookahead].span.end;
            literal_count += 1;
            cursor = lookahead + 1;
        }
        if literal_count > 1 || single_needs_rewrite {
            let replacement = if single_is_double && !merged.contains('"') {
                requote_double(&merged)
            } else {
                requote_generic(&merged)
            };
            regions.push(LiteralRegion {
                start,
                end,
                replacement,
            });
        }
        index = cursor;
    }
    regions
}

fn literal_token_value(token: &Token, keep_verbatim: bool) -> Option<String> {
    let unescape = |raw: &String| {
        if keep_verbatim {
            unescape_verbatim_literal(raw)
        } else {
            unescape_spark_literal(raw)
        }
    };
    match token {
        Token::SingleQuotedString(raw) | Token::DoubleQuotedString(raw) => Some(unescape(raw)),
        Token::SingleQuotedRawStringLiteral(raw) => Some(raw.clone()),
        _ => None,
    }
}

fn literal_needs_rewrite(token: &Token, keep_verbatim: bool) -> bool {
    match token {
        Token::SingleQuotedString(raw) => raw.contains('\\'),
        Token::DoubleQuotedString(raw) => !keep_verbatim && raw.contains('\\'),
        Token::SingleQuotedRawStringLiteral(_) => true,
        _ => false,
    }
}

fn unescape_verbatim_literal(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut characters = raw.chars().peekable();
    while let Some(current) = characters.next() {
        if current == '\'' && characters.peek() == Some(&'\'') {
            characters.next();
        }
        out.push(current);
    }
    out
}

fn requote_double(value: &str) -> String {
    format!("\"{value}\"")
}

/// Re-quote a finished Spark string value as Generic-canonical: wrap in `'…'`, double every `'`.
pub(crate) fn requote_generic(value: &str) -> String {
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

fn push_code_point(code_point: u32, out: &mut String) {
    if let Some(character) = char::from_u32(code_point) {
        out.push(character);
        return;
    }
    if code_point <= 0xFFFF {
        out.push(UNREPRESENTABLE);
        return;
    }
    push_java_surrogate_artifact(code_point, out);
}

fn push_java_surrogate_artifact(code_point: u32, out: &mut String) {
    let shifted = code_point.wrapping_sub(0x1_0000);
    let high = 0xD800u32.wrapping_add((shifted.cast_signed() >> 10).cast_unsigned()) & 0xFFFF;
    let low = 0xDC00 + (shifted & 0x3FF);
    if (0xD800..=0xDBFF).contains(&high) && (0xDC00..=0xDFFF).contains(&low) {
        let combined = 0x1_0000 + ((high - 0xD800) << 10) + (low - 0xDC00);
        if let Some(character) = char::from_u32(combined) {
            out.push(character);
            return;
        }
    }
    push_java_unit(high, out);
    push_java_unit(low, out);
}

fn push_java_unit(unit: u32, out: &mut String) {
    if (0xD800..=0xDFFF).contains(&unit) {
        out.push(UNREPRESENTABLE);
    } else if let Some(character) = char::from_u32(unit) {
        out.push(character);
    } else {
        out.push(UNREPRESENTABLE);
    }
}

pub(crate) const SPARK_SQL_PARSER_ESCAPED_STRING_LITERALS_KEY: &str =
    "spark.sql.parser.escapedStringLiterals";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SparkEscapedStringLiteralsConfig {
    pub keep_verbatim: bool,
}

impl datafusion::common::config::ConfigExtension for SparkEscapedStringLiteralsConfig {
    const PREFIX: &'static str = "repark.escaped-string-literals";
}

impl datafusion::common::config::ExtensionOptions for SparkEscapedStringLiteralsConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn datafusion::common::config::ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> datafusion::common::Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the verbatim-literal mode is set with \
             `{SPARK_SQL_PARSER_ESCAPED_STRING_LITERALS_KEY}` on the session builder and is fixed at session build",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<datafusion::common::config::ConfigEntry> {
        Vec::new()
    }
}

pub(crate) fn parse_escaped_string_literals(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(DataFusionError::Configuration(format!(
            "The value '{raw}' in the config \
             \"{SPARK_SQL_PARSER_ESCAPED_STRING_LITERALS_KEY}\" is invalid. \
             {SPARK_SQL_PARSER_ESCAPED_STRING_LITERALS_KEY} should be boolean, but was {raw}"
        ))),
    }
}

pub(crate) fn escaped_string_literals_from_config_map<S>(
    config: &std::collections::HashMap<String, String, S>,
) -> Result<bool>
where
    S: std::hash::BuildHasher,
{
    match config.get(SPARK_SQL_PARSER_ESCAPED_STRING_LITERALS_KEY) {
        Some(raw) => parse_escaped_string_literals(raw),
        None => Ok(false),
    }
}

#[must_use]
pub(crate) fn with_escaped_string_literals_config(
    config: SessionConfig,
    keep_verbatim: bool,
) -> SessionConfig {
    config.with_option_extension(SparkEscapedStringLiteralsConfig { keep_verbatim })
}

#[must_use]
pub(crate) fn escaped_verbatim_from_options(
    options: &datafusion::common::config::ConfigOptions,
) -> bool {
    options
        .extensions
        .get::<SparkEscapedStringLiteralsConfig>()
        .is_some_and(|extension| extension.keep_verbatim)
}

#[cfg(test)]
mod location_translation_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn fragment_struct_call_base_field_access_rewrites_to_subscript() {
        let canonical = canonicalize("named_struct('a', 1).a").expect("fragment canonicalizes");
        assert_eq!(canonical.as_ref(), "named_struct('a', 1)['a']");
    }

    #[test]
    fn exponent_numbers_rewrite_to_double_casts() {
        let canonical =
            canonicalize("SELECT 1.0E6, 1E2, 1e-3, 1.0E21, 4.9E-324, 1.5").expect("canonicalizes");
        assert_eq!(
            canonical.as_ref(),
            "SELECT CAST('1.0E6' AS DOUBLE), CAST('1E2' AS DOUBLE), CAST('1e-3' AS DOUBLE), \
             CAST('1.0E21' AS DOUBLE), CAST('4.9E-324' AS DOUBLE), 1.5"
        );
    }

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
