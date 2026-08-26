//! Spark string-literal canonicalisation at the Spark door's **front door**.
//!
//! Spark's lexer processes escape sequences inside a single-quoted string literal; the SQL door
//! kept every backslash verbatim, so `'\d'` reached the engine as two characters where Spark
//! returns one, and `regexp_count('a1b22', '\\d')` returned 0 where Spark returns 3 — a silent
//! wrong answer on every migrated regex pattern. Spark-valid SQL such as `'it\'s'` did not even
//! tokenise here. [`canonicalize`] closes that class by rewriting each literal to the value
//! Spark's lexer would produce, **once**, before any router tokeniser sees the text.
//!
//! **Why the front door, and only here.** `router::execute` hands the user text to a dozen router
//! tokenisers (`normalize`, `time_travel`, `describe_show`, `ref_ddl`, `alter`, `metadata_tables`)
//! before `spark_ast::execute_passthrough` re-parses it. Every one of those lexes `\'` wrongly.
//! Making the text canonical once, first, means each downstream stage sees the value Spark already
//! produced. Internally-generated SQL (`predicate_dml`, `merge`) never enters `router::execute`, so
//! it is never re-processed — that is what keeps the pass exactly-once (charter C-005 / C-010).
//!
//! **The output is Generic-canonical.** Each rewritten literal is re-quoted as a plain `'…'` with
//! every embedded `'` doubled and no backslash meaning. The executing parse (DataFusion's session
//! dialect is `Generic`) and the router's `Databricks` routing parse both keep backslashes literal
//! and both fold `''`→`'`, so the canonical form re-tokenises to the same value under either —
//! the value cannot be escape-processed a second time.
//!
//! **The rules (Spark 4.1.2, `spark.sql.parser.escapedStringLiterals=false`, measured against the
//! live oracle `<pyspark-4.1.2-oracle>`; the charter ledger holds the full transcript):**
//!
//! | Input after `\` | Result | Oracle |
//! |---|---|---|
//! | `\` `'` `"` | `\` `'` `"` (drop the leading backslash) | E5, E15, E28 |
//! | `n` `t` `r` `b` | LF, TAB, CR, U+0008 | E3, E4 |
//! | `0` | U+0000 (NUL) | E4 |
//! | `Z` | U+001A | E4 |
//! | `%` `_` | `\%` `\_` (backslash KEPT — LIKE reads it) | E12 |
//! | `NNN` (3 octal, first `0`–`1`) | that byte (`\101`→`A`, `\000`→NUL) | E11, U13 |
//! | `uXXXX` (exactly 4 hex) | that code point; a `\uD8xx\uDCxx` pair → one astral char | U1–U6 |
//! | `UXXXXXXXX` (exactly 8 hex) | that code point | U5 |
//! | any other `c` | `c` (`\d`→d, `\q`→q, `\f`→f, `\1`→1, `\200`→2 then `00`) | E1, E8, E11, E27 |
//! | `''` (doubled quote) | `'` | E6 |
//!
//! An unrepresentable code point — a lone UTF-16 surrogate (`\ud83d` with no low-surrogate
//! partner), or a `\U` value outside the Unicode scalar range — becomes [`UNREPRESENTABLE`]
//! (`?`). Measured: `hex('\ud83d')` is `3F`, Spark's Java UTF-8 encoder replacement, **not**
//! U+FFFD. Spark's exact 2-char output for an out-of-range `\U` (a Java `char[]` artifact) is not
//! reproduced and is not in the pinned contract; `?` keeps the result sane and single-homed.
//!
//! Raw strings (`r'…'` / `R'…'`) keep their content verbatim (E19). An unpaired trailing backslash
//! (`'a\'`) is a lexer error, surfaced as a parse error carrying line/column (E16). Backtick and
//! double-quoted identifiers are never touched (E26; double-quoted string literals stay OUT of
//! scope — FNP-4b, docs/spark-sql-iceberg-parity.md §7).

use std::borrow::Cow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::BigQueryDialect;
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

/// The character Spark emits for a code point it cannot represent — a lone UTF-16 surrogate, or a
/// `\U` value outside the scalar range. Measured `'\ud83d'` → `?` (U+003F), Java's UTF-8 encoder
/// replacement, never U+FFFD. A single home so the rule and the pins quote the same constant.
const UNREPRESENTABLE: char = '\u{003F}';

/// ===========================================================================================
/// Rewrite every single-quoted string literal in `sql` to the value Spark 4.1.2's lexer would
/// produce, and re-emit it as a Generic-canonical literal. Called once as the first act of
/// `router::execute` (proven to have a single caller by `front_door_has_one_caller`).
///
/// Returns [`Cow::Borrowed`] unchanged when the text carries no single-quoted literal and no
/// backslash — the common case, where the Spark lexer and the Generic executing lexer already
/// agree — and otherwise the rewritten text. Non-literal spans (identifiers, keywords, whitespace,
/// comments) are copied from the source byte-for-byte; only literal spans are replaced, so a
/// backtick identifier carrying a backslash is left exactly as written.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::SQL`] carrying the lexer's line/column when the text does not tokenise —
/// an unterminated literal, most visibly an unpaired trailing backslash `'a\'` (Spark's
/// `PARSE_SYNTAX_ERROR`).
pub(crate) fn canonicalize(sql: &str) -> Result<Cow<'_, str>> {
    // A backslash can only change a value inside a single-quoted literal; with neither a `'` nor a
    // `\` present there is nothing Spark's lexer would do that Generic does not, so borrow without
    // tokenising. (A `\` inside a backtick/double-quoted identifier reaches the slow path but is
    // copied from source verbatim there — never unescaped.)
    if !sql.as_bytes().contains(&b'\'') && !sql.as_bytes().contains(&b'\\') {
        return Ok(Cow::Borrowed(sql));
    }
    // BigQuery is the one stock dialect that both lexes `\'` as an escape (so it does not end the
    // literal) AND recognises `r'…'` raw strings; `with_unescape(false)` keeps the raw between-quote
    // text so this module — not sqlparser, whose escape rules differ from Spark's — applies Spark's
    // rules. Only string-literal spans are consumed from this tokenisation; everything else is
    // copied from the source, so BigQuery's other lexing choices cannot reach the output.
    let tokens = Tokenizer::new(&BigQueryDialect {}, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|error| DataFusionError::SQL(Box::new(ParserError::from(error)), None))?;
    let regions = plan_literal_regions(&tokens);
    if regions.is_empty() {
        return Ok(Cow::Borrowed(sql));
    }
    Ok(Cow::Owned(apply_regions(sql, &regions)))
}

/// A source span to replace with a canonicalised literal. `start`/`end` are the sqlparser
/// [`Location`]s bounding the whole literal group (opening quote of the first literal through the
/// closing quote of the last), so [`apply_regions`] can copy everything outside verbatim.
struct LiteralRegion {
    start: Location,
    end: Location,
    replacement: String,
}

/// Walk the token stream and collect the literal spans that must change. A single literal is left
/// alone (no region → copied verbatim) unless it is a raw string or actually carries a backslash
/// escape; two or more adjacent literals always form one region, because Spark concatenates them
/// (`'ab' 'cd'` → `abcd`, E7) where the downstream parser errors.
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
        // Absorb every following literal separated only by whitespace/comments (Spark's
        // adjacent-literal concatenation). The inter-literal whitespace falls inside [start, end)
        // and is dropped when the region is replaced.
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

/// The Spark string value of a single-quoted literal token, or `None` for any other token. A
/// plain single-quoted literal is unescaped by Spark's rules; a raw string keeps its content
/// verbatim (E19).
fn literal_token_value(token: &Token) -> Option<String> {
    match token {
        Token::SingleQuotedString(raw) => Some(unescape_spark_literal(raw)),
        Token::SingleQuotedRawStringLiteral(raw) => Some(raw.clone()),
        _ => None,
    }
}

/// True when a single literal, taken alone, produces different text under Spark's rules than the
/// downstream Generic lexer would — i.e. it is a raw string, or it carries a backslash. `''`
/// doubling is not a difference: Generic already folds it, so a literal whose only special content
/// is `''` is copied verbatim.
fn literal_needs_rewrite(token: &Token) -> bool {
    match token {
        Token::SingleQuotedString(raw) => raw.contains('\\'),
        Token::SingleQuotedRawStringLiteral(_) => true,
        _ => false,
    }
}

/// Re-quote a finished Spark string value as a Generic-canonical literal: wrap in `'…'` and double
/// every embedded `'`. Backslashes and control characters (a real TAB, NUL, …) stay literal —
/// Generic gives them no meaning, so the value round-trips through the executing parse unchanged.
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

/// Rebuild `sql`, replacing each [`LiteralRegion`] with its canonical text and copying every other
/// character from the source. The walk tracks sqlparser's `(line, column)` position (column counts
/// characters, matching the tokeniser) so the region boundaries — themselves `Location`s — match
/// without any byte-offset arithmetic. Regions arrive in source order and do not overlap.
fn apply_regions(sql: &str, regions: &[LiteralRegion]) -> String {
    let mut out = String::with_capacity(sql.len());
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
            out.push_str(&region.replacement);
            skip_until = Some(region.end);
            regions.next();
        }
        if skip_until.is_none() {
            out.push(character);
        }
        if character == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    out
}

/// ===========================================================================================
/// Apply Spark 4.1.2's escape rules to the raw between-quote text of a single-quoted literal.
///
/// `raw` is the text sqlparser kept with `unescape(false)`: backslash sequences survive as
/// `\`+char and a doubled `''` survives as two quotes. Every rule row in the module doc is
/// discharged here.
/// ===========================================================================================
fn unescape_spark_literal(raw: &str) -> String {
    let characters: Vec<char> = raw.chars().collect();
    let mut out = String::with_capacity(raw.len());
    let mut index = 0;
    while index < characters.len() {
        let current = characters[index];
        if current == '\'' {
            // The lexer only leaves a `'` here as the first of a doubled `''` (E6); a lone `'`
            // would have ended the literal. Consume the pair, emit one quote.
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
        // A backslash with no following character cannot occur — the lexer refuses `'a\'` as
        // unterminated before this runs — but keep the byte rather than panic if it ever does.
        let Some(&escaped) = characters.get(index + 1) else {
            out.push('\\');
            index += 1;
            continue;
        };
        index = apply_escape(escaped, &characters, index, &mut out);
    }
    out
}

/// Handle one `\<escaped>` sequence starting at `index` (the backslash). Returns the index of the
/// next unconsumed character. Split out of [`unescape_spark_literal`] to keep each function to one
/// responsibility.
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
        // Any other escape drops the backslash and keeps the character: `\\`→`\`, `\'`→`'`,
        // `\"`→`"`, `\d`→d, `\q`→q, `\f`→f (E1, E5, E8; measured `\f` is `f`, not form-feed).
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

/// `\NNN` octal (E11, E27, U13). Exactly three octal digits whose value is ≤ 0o177 (first digit
/// `0` or `1`) map to that byte; `\0` alone maps to NUL; every other run is the unknown-escape
/// rule on the first digit (`\200` → `2` then literal `00`).
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

/// The numeric value of one ASCII octal digit (`'0'..='7'`). The caller has already checked the
/// range, so the subtraction cannot underflow.
fn octal_value(digit: char) -> u8 {
    (digit as u8).saturating_sub(b'0')
}

/// `\uXXXX` (exactly 4 hex → a code point). A high surrogate immediately followed by `\uYYYY` low
/// surrogate combines into one astral character (U6); a lone surrogate becomes [`UNREPRESENTABLE`].
/// Fewer than 4 hex digits is the unknown-escape rule: `\u004` → `u` then literal `004` (U3).
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

/// `\UXXXXXXXX` (exactly 8 hex → a code point; U5). Fewer than 8 hex digits is the unknown-escape
/// rule: `\U0001F40` → `U` then literal `0001F40`.
fn apply_unicode_32(characters: &[char], index: usize, out: &mut String) -> usize {
    let Some(value) = read_hex(characters, index + 2, 8) else {
        out.push('U');
        return index + 2;
    };
    push_code_point(value, out);
    index + 10
}

/// Read exactly `count` hex digits starting at `start`, or `None` if fewer are present (the
/// unknown-escape fall-through) — Spark requires the exact width (`\u004` is not ``).
fn read_hex(characters: &[char], start: usize, count: usize) -> Option<u32> {
    let end = start.checked_add(count)?;
    let slice = characters.get(start..end)?;
    let mut value = 0u32;
    for digit in slice {
        value = value * 16 + digit.to_digit(16)?;
    }
    Some(value)
}

/// Emit the character for `code_point`, or [`UNREPRESENTABLE`] (`?`) when it is not a Unicode
/// scalar value — a lone surrogate (`\ud83d`) or a `\U` value past `U+10FFFF`. Matches Spark's
/// Java UTF-8 encoder, which replaces such a code point with `?` (measured `hex('\ud83d')` = `3F`).
fn push_code_point(code_point: u32, out: &mut String) {
    match char::from_u32(code_point) {
        Some(character) => out.push(character),
        None => out.push(UNREPRESENTABLE),
    }
}
