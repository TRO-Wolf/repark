//! ANSI-quoting-aware SQL text scanning — the one place this door reads raw SQL text.
//!
//! Three router-head concerns (multi-statement refuse, the write-to-branch sniff, the error-path
//! wrong-door sniff) have to look at SQL *before* or *without* a parse. Each of them is a
//! false-positive machine if written against the raw string: `SELECT 'a; b'` is not two
//! statements, `-- USING` is not a Spark-ism, and `"tbl;name"` is one identifier. So all three go
//! through [`blank_out_quoted_and_comments`], which replaces the CONTENT of string literals,
//! quoted identifiers, and comments with spaces while preserving byte length and every structural
//! character.
//!
//! **ANSI quoting, and only ANSI quoting** (design §2 Q10/Q12): `'…'` is a string literal with
//! `''` as the escape, `"…"` is a QUOTED IDENTIFIER with `""` as the escape (never a string —
//! that is the ANSI/Trino rule and the Spark divergence), `--` runs to end of line, `/* … */`
//! nests-not (SQL block comments do not nest). Backticks are **not** quoting characters here:
//! a backtick is a Spark-ism, so it is left in the scrubbed text on purpose, where the wrong-door
//! sniff can find it and refuse. Blanking a backtick "string" would hide exactly the token this
//! door most wants to report.

/// ===========================================================================================
/// Replace the contents of string literals, quoted identifiers, and comments with ASCII spaces,
/// preserving length and all structural punctuation.
///
/// The result is safe to scan for `;`, keywords, or `` ` `` without literal/comment false
/// positives. Multi-byte characters inside a literal are replaced byte-for-byte with spaces, so
/// byte offsets into the result still line up with the input.
/// ===========================================================================================
pub(crate) fn blank_out_quoted_and_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = blank_delimited(bytes, index, b'\'', &mut out),
            b'"' => index = blank_delimited(bytes, index, b'"', &mut out),
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index = blank_line_comment(bytes, index, &mut out);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = blank_block_comment(bytes, index, &mut out);
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    // Every byte written is ASCII or a byte copied verbatim from a valid UTF-8 string outside any
    // literal, so the result is valid UTF-8 by construction; the lossy conversion is a
    // total-function formality, never a lossy path in practice.
    String::from_utf8_lossy(&out).into_owned()
}

/// Blank a `'…'` / `"…"` run, honouring the doubled-delimiter escape. Returns the next index.
/// An UNTERMINATED literal blanks to end of input (fail-closed: the tail cannot then be read as
/// structure).
fn blank_delimited(bytes: &[u8], start: usize, delimiter: u8, out: &mut Vec<u8>) -> usize {
    out.push(delimiter);
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == delimiter {
            if bytes.get(index + 1) == Some(&delimiter) {
                // Doubled delimiter: an escaped delimiter INSIDE the literal.
                out.push(b' ');
                out.push(b' ');
                index += 2;
                continue;
            }
            out.push(delimiter);
            return index + 1;
        }
        out.push(b' ');
        index += 1;
    }
    index
}

/// Blank a `-- …` line comment (the newline itself survives). Returns the next index.
fn blank_line_comment(bytes: &[u8], start: usize, out: &mut Vec<u8>) -> usize {
    let mut index = start;
    while index < bytes.len() && bytes[index] != b'\n' {
        out.push(b' ');
        index += 1;
    }
    index
}

/// Blank a `/* … */` block comment. Newlines survive so line numbers in any downstream parser
/// error stay meaningful. Returns the next index.
fn blank_block_comment(bytes: &[u8], start: usize, out: &mut Vec<u8>) -> usize {
    let mut index = start;
    while index < bytes.len() {
        if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            out.push(b' ');
            out.push(b' ');
            return index + 2;
        }
        out.push(if bytes[index] == b'\n' { b'\n' } else { b' ' });
        index += 1;
    }
    index
}

/// ===========================================================================================
/// True when `needle` (ASCII, case-insensitive) occurs in `haystack` as a whole WORD — i.e. not
/// flanked by an identifier character. `needle` may itself contain spaces, in which case the run
/// of whitespace in `haystack` is collapsed for the comparison (`PARTITIONED    BY` matches
/// `"PARTITIONED BY"`).
/// ===========================================================================================
pub(crate) fn contains_word(haystack: &str, needle: &str) -> bool {
    let normalized = collapse_whitespace(haystack);
    let normalized_lower = normalized.to_ascii_lowercase();
    let needle_lower = collapse_whitespace(needle).to_ascii_lowercase();
    if needle_lower.is_empty() {
        return false;
    }
    let bytes = normalized_lower.as_bytes();
    let needle_bytes = needle_lower.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = normalized_lower[from..].find(&needle_lower) {
        let start = from + offset;
        let end = start + needle_bytes.len();
        let before_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_ident_byte(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

/// Collapse every whitespace run to one ASCII space (leading/trailing runs are kept as one space
/// so word-boundary logic stays uniform).
fn collapse_whitespace(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_space = false;
    for ch in value.chars() {
        if ch.is_whitespace() {
            if !in_space {
                out.push(' ');
                in_space = true;
            }
        } else {
            out.push(ch);
            in_space = false;
        }
    }
    out
}

/// Identifier bytes for word-boundary purposes: ASCII alphanumerics, `_`, and `$` (so
/// `orders$snapshots` is one word and cannot half-match).
fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

/// ===========================================================================================
/// Every identifier-ish word in already-scrubbed text as `(start, end, word)` BYTE spans.
///
/// The spans index the scrubbed string, which is byte-length-identical to the input, so they are
/// valid offsets into the ORIGINAL SQL too. That is the whole point: a pre-parse recognizer can
/// locate a keyword and edit the user's text at that offset while remaining structurally blind to
/// string literals, quoted identifiers, and comments.
/// ===========================================================================================
pub(crate) fn word_spans(scrubbed: &str) -> Vec<(usize, usize, &str)> {
    let mut spans = Vec::new();
    let mut start: Option<usize> = None;
    for (index, ch) in scrubbed.char_indices() {
        let is_word = ch.is_ascii_alphanumeric() || ch == '_' || ch == '$';
        match (is_word, start) {
            (true, None) => start = Some(index),
            (false, Some(begin)) => {
                spans.push((begin, index, &scrubbed[begin..index]));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(begin) = start {
        spans.push((begin, scrubbed.len(), &scrubbed[begin..]));
    }
    spans
}

/// The first significant word of a statement (uppercased), skipping comments/whitespace.
/// `None` for an empty / comment-only string.
pub(crate) fn leading_keyword(scrubbed: &str) -> Option<String> {
    scrubbed
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .find(|word| !word.is_empty())
        .map(str::to_ascii_uppercase)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// String-literal content is blanked; the delimiters and everything structural survive.
    #[test]
    fn blanks_string_literal_content_only() {
        let scrubbed = blank_out_quoted_and_comments("SELECT 'a; b' AS x");
        assert_eq!(scrubbed, "SELECT '    ' AS x");
        assert_eq!(scrubbed.len(), "SELECT 'a; b' AS x".len());
        assert!(!scrubbed.contains(';'), "literal `;` must not survive");
    }

    /// `''` inside a literal is an escape, not a terminator — the tail stays blanked.
    #[test]
    fn honours_doubled_single_quote_escape() {
        let scrubbed = blank_out_quoted_and_comments("SELECT 'it''s; here' AS x");
        assert!(
            !scrubbed.contains(';'),
            "escaped literal leaked: {scrubbed}"
        );
        assert!(scrubbed.ends_with(" AS x"), "tail must survive: {scrubbed}");
    }

    /// ANSI rule: `"…"` is a QUOTED IDENTIFIER (with `""` escape), and its content is blanked too.
    #[test]
    fn blanks_double_quoted_identifier_content() {
        let scrubbed = blank_out_quoted_and_comments(r#"SELECT * FROM "we;ird" "#);
        assert!(!scrubbed.contains(';'), "quoted ident leaked: {scrubbed}");
        let escaped = blank_out_quoted_and_comments(r#"SELECT * FROM "a""b;c" x"#);
        assert!(!escaped.contains(';'), "escaped ident leaked: {escaped}");
        assert!(escaped.ends_with(" x"), "tail must survive: {escaped}");
    }

    /// Backticks are deliberately NOT quoting: the Spark-ism must survive for the sniff.
    #[test]
    fn backticks_are_not_quoting() {
        let scrubbed = blank_out_quoted_and_comments("SELECT * FROM `db`.`t`");
        assert!(
            scrubbed.contains('`'),
            "backticks must survive scrubbing: {scrubbed}"
        );
    }

    /// Line and block comments are blanked; newlines survive.
    #[test]
    fn blanks_comments() {
        let line = blank_out_quoted_and_comments("SELECT 1 -- ; USING\nFROM t");
        assert!(!line.contains(';'), "line comment leaked: {line}");
        assert!(
            !contains_word(&line, "USING"),
            "line comment leaked: {line}"
        );
        assert!(line.contains('\n'), "newline must survive: {line}");

        let block = blank_out_quoted_and_comments("SELECT /* ; USING */ 1");
        assert!(!block.contains(';'), "block comment leaked: {block}");
        assert!(!contains_word(&block, "USING"), "block comment leaked");
    }

    /// An unterminated literal blanks to end of input (fail-closed).
    #[test]
    fn unterminated_literal_blanks_to_end() {
        let scrubbed = blank_out_quoted_and_comments("SELECT 'oops; DROP TABLE t");
        assert!(!scrubbed.contains(';'), "unterminated leaked: {scrubbed}");
    }

    /// Multi-byte content inside a literal keeps byte length (offsets stay aligned).
    #[test]
    fn preserves_byte_length_across_multibyte_literals() {
        let sql = "SELECT 'héllo — wörld' AS x";
        let scrubbed = blank_out_quoted_and_comments(sql);
        assert_eq!(scrubbed.len(), sql.len(), "byte length must be preserved");
    }

    /// Word matching respects identifier boundaries and collapses internal whitespace.
    #[test]
    fn contains_word_is_boundary_aware() {
        assert!(contains_word("CREATE TABLE t USING iceberg", "USING"));
        assert!(!contains_word("SELECT unusuing FROM t", "USING"));
        assert!(!contains_word("SELECT using_col FROM t", "USING"));
        assert!(contains_word("PARTITIONED   BY (a)", "PARTITIONED BY"));
        assert!(!contains_word(
            "SELECT partitioned_by FROM t",
            "PARTITIONED BY"
        ));
        assert!(!contains_word("anything", ""));
    }

    /// `$` counts as an identifier byte, so a metadata-table name cannot half-match.
    #[test]
    fn contains_word_treats_dollar_as_ident() {
        assert!(!contains_word("SELECT * FROM t$files", "t"));
        assert!(contains_word("SELECT * FROM t", "t"));
    }

    /// Word spans index the SCRUBBED text, and those offsets are valid in the original SQL —
    /// which is what lets a pre-parse recognizer edit at an offset without seeing into literals.
    #[test]
    fn word_spans_offsets_are_valid_in_the_original_sql() {
        let sql = "ALTER TABLE t SET PROPERTIES (a = 'SET PROPERTIES')";
        let scrubbed = blank_out_quoted_and_comments(sql);
        let spans = word_spans(&scrubbed);
        let words: Vec<&str> = spans.iter().map(|(_, _, word)| *word).collect();
        assert_eq!(
            words,
            vec!["ALTER", "TABLE", "t", "SET", "PROPERTIES", "a"],
            "the literal's words must be invisible"
        );
        let (start, end, word) = spans[4];
        assert_eq!(word, "PROPERTIES");
        assert_eq!(
            &sql[start..end],
            "PROPERTIES",
            "offset maps to the original"
        );
    }

    /// The leading keyword skips comments and whitespace.
    #[test]
    fn leading_keyword_skips_comments() {
        let scrubbed = blank_out_quoted_and_comments("  -- hi\n  create table t (a int)");
        assert_eq!(leading_keyword(&scrubbed).as_deref(), Some("CREATE"));
        assert_eq!(leading_keyword("   ").as_deref(), None);
    }
}
