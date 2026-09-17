//! Facade `OPTIONS(...)` clause extraction and validation.
//!
//! The Python facade renders its stored writer options onto generated Iceberg SQL as
//! `OPTIONS('key'='value', ...)`; this recognizer extracts the clause before `sqlparser`
//! sees it, validates every key in Rust, and hands the levers to the executors. Raw SQL
//! never carries the clause: extraction only fires on leading `INSERT`/`CREATE`, the
//! content must parse as quoted pairs, and non-Iceberg targets refuse a non-empty set.
//! `pins: ice-write-options-1/C-001, C-002, C-003, C-004`.

use datafusion::error::{DataFusionError, Result};

/// Prefix for snapshot-summary properties (Spark strips it, lower-cases the suffix).
pub const SNAPSHOT_PROPERTY_PREFIX: &str = "snapshot-property.";

/// A validated statement write-option set.
#[derive(Debug, Default, Clone)]
pub struct StatementWriteOptions {
    /// Lower-cased keys as written, last wins.
    pub raw: Vec<(String, String)>,
    /// `(suffix, value)` pairs for the snapshot summary; suffix is lower-cased.
    pub snapshot_extra: Vec<(String, String)>,
    /// Lower-cased `write-format` value.
    pub write_format: Option<String>,
    /// Parsed `target-file-size-bytes` value (option over table property).
    pub target_file_size_bytes: Option<u64>,
    /// Raw `compression-codec` value (option over table property).
    pub codec: Option<String>,
    /// Raw `compression-level` value (option over table property).
    pub level: Option<String>,
    /// Lower-cased `distribution-mode` value.
    pub distribution_mode: Option<String>,
    /// Raw `isolation-level` value (option over table property).
    pub isolation: Option<String>,
}

impl StatementWriteOptions {
    /// An empty set; the statement carries no facade options.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Whether the statement carries any facade option.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    /// Refuse a non-empty set outside the Iceberg write paths that honour it.
    /// # Errors
    /// A plan error when options are present, so they are never silently dropped.
    pub fn refuse_if_non_empty(&self, context: &str) -> Result<()> {
        if self.raw.is_empty() {
            return Ok(());
        }
        let keys: Vec<&str> = self.raw.iter().map(|(key, _)| key.as_str()).collect();
        Err(DataFusionError::Plan(format!(
            "{context} does not support the facade OPTIONS clause ({}); it is only \
             honoured on Iceberg table writes (ICE-WRITE-OPTIONS-1)",
            keys.join(", ")
        )))
    }

    /// The staging levers for `repark-iceberg`.
    #[must_use]
    pub fn staging_overrides(&self) -> repark_iceberg::write::WriterStagingOverrides {
        repark_iceberg::write::WriterStagingOverrides {
            codec: self.codec.clone(),
            level: self.level.clone(),
            target_file_size_bytes: self.target_file_size_bytes,
        }
    }
}

/// Extract the facade `OPTIONS(...)` clause from `sql`, returning cleaned SQL plus options.
///
/// The clause is `OPTIONS('key'='value', ...)` at top level before the statement source;
/// anything else keeps the SQL byte-identical.
/// # Errors
/// A validated key with a bad value (never an extraction miss: those stay untouched).
pub fn extract_statement_write_options(sql: &str) -> Result<(String, StatementWriteOptions)> {
    if !is_options_write_statement(sql) {
        return Ok((sql.to_string(), StatementWriteOptions::empty()));
    }
    let mut cleaned = sql.to_string();
    let mut pairs: Vec<(String, String)> = Vec::new();
    loop {
        let Some((start, end, found)) = find_options_clause(&cleaned) else {
            break;
        };
        let Some(clause_pairs) = parse_options_pairs(&found) else {
            return Ok((sql.to_string(), StatementWriteOptions::empty()));
        };
        pairs.extend(clause_pairs);
        cleaned = format!("{}{}", &cleaned[..start], &cleaned[end..]);
    }
    if pairs.is_empty() {
        return Ok((sql.to_string(), StatementWriteOptions::empty()));
    }
    Ok((cleaned, StatementWriteOptions::validate(pairs)?))
}

impl StatementWriteOptions {
    /// Validate raw pairs into a typed set.
    /// # Errors
    /// Refusals for un-writable formats and bad values; unknown keys are ignored like Spark.
    fn validate(pairs: Vec<(String, String)>) -> Result<Self> {
        let mut merged: Vec<(String, String)> = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let lowered = key.to_ascii_lowercase();
            if let Some(position) = merged.iter().position(|(prior, _)| *prior == lowered) {
                merged[position] = (lowered, value);
            } else {
                merged.push((lowered, value));
            }
        }
        let mut options = StatementWriteOptions {
            raw: merged,
            ..StatementWriteOptions::default()
        };
        for (key, value) in options.raw.clone() {
            if let Some(suffix) = key.strip_prefix(SNAPSHOT_PROPERTY_PREFIX) {
                options.snapshot_extra.push((suffix.to_string(), value));
                continue;
            }
            match key.as_str() {
                "write-format" => options.write_format = Some(validate_write_format(&value)?),
                "target-file-size-bytes" => {
                    options.target_file_size_bytes =
                        Some(repark_iceberg::write::parse_target_file_size(&value)?);
                }
                "compression-codec" => options.codec = Some(value),
                "compression-level" => options.level = Some(value),
                "distribution-mode" => {
                    options.distribution_mode = Some(validate_distribution_mode(&value)?);
                }
                "fanout-enabled" | "check-nullability" | "check-ordering" => {}
                "isolation-level" => options.isolation = Some(validate_isolation_level(&value)?),
                _ => {}
            }
        }
        repark_iceberg::write::parse_compression(
            options.codec.as_deref(),
            options.level.as_deref(),
        )?;
        Ok(options)
    }
}

/// Validate a `write-format` value: parquet passes, orc/avro are declared refusals.
/// # Errors
/// `NotImplemented` naming the registry row for orc/avro; `Plan` for unknown formats.
fn validate_write_format(raw: &str) -> Result<String> {
    match raw.to_ascii_lowercase().as_str() {
        "parquet" => Ok("parquet".to_string()),
        "orc" | "avro" => Err(DataFusionError::NotImplemented(format!(
            "write-format {raw:?} has no RePark Iceberg writer — only parquet is written \
             (ICE-WRITE-OPTIONS-1 ORC/AVRO declared 2026-09-17)"
        ))),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid file format: {raw}"
        ))),
    }
}

/// Validate a `distribution-mode` value against the engine domain.
/// # Errors
/// Any value outside none/hash/range, mirroring Spark's refusal text.
fn validate_distribution_mode(raw: &str) -> Result<String> {
    match raw.to_ascii_lowercase().as_str() {
        "none" | "hash" | "range" => Ok(raw.to_ascii_lowercase()),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid distribution mode: {raw}"
        ))),
    }
}

/// Validate an `isolation-level` value against the option domain.
/// # Errors
/// Any value outside none/snapshot/serializable, mirroring Spark's refusal text.
fn validate_isolation_level(raw: &str) -> Result<String> {
    match raw.to_ascii_lowercase().as_str() {
        "none" | "snapshot" | "serializable" => Ok(raw.to_string()),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid isolation level: {raw}"
        ))),
    }
}

/// Whether `sql` is an INSERT or CREATE [OR REPLACE] TABLE whose facade clause is stripped.
///
/// Any other leading form keeps the SQL byte-identical, so user DDL can never lose text.
fn is_options_write_statement(sql: &str) -> bool {
    let mut words = sql
        .split(|byte: char| byte.is_whitespace() || byte == '(')
        .filter(|word| !word.is_empty());
    let Some(first) = words.next() else {
        return false;
    };
    if first.eq_ignore_ascii_case("insert") {
        return true;
    }
    if !first.eq_ignore_ascii_case("create") {
        return false;
    }
    let mut next = words.next();
    if next.is_some_and(|word| word.eq_ignore_ascii_case("or")) {
        next = words.next();
        if !next.is_some_and(|word| word.eq_ignore_ascii_case("replace")) {
            return false;
        }
        next = words.next();
    }
    next.is_some_and(|word| word.eq_ignore_ascii_case("table"))
}

/// Locate the first top-level `OPTIONS(...)` before the statement source.
///
/// Returns the byte span to strip plus the raw clause text. Quote-, comment- and
/// paren-aware; a bare table named `options` never matches (no opening paren follows).
fn find_options_clause(sql: &str) -> Option<(usize, usize, String)> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut depth = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'\'' {
            index = skip_quoted(bytes, index, b'\'');
            continue;
        }
        if byte == b'"' || byte == b'`' {
            index = skip_quoted(bytes, index, byte);
            continue;
        }
        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len()
                && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
            {
                index += 1;
            }
            index += 2;
            continue;
        }
        if byte == b'(' {
            depth += 1;
            index += 1;
            continue;
        }
        if byte == b')' {
            depth = depth.saturating_sub(1);
            index += 1;
            continue;
        }
        if depth == 0 && (byte.is_ascii_alphabetic() || byte == b'_') {
            let start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
            {
                index += 1;
            }
            if sql[start..index].eq_ignore_ascii_case("options") {
                let mut cursor = index;
                while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                    cursor += 1;
                }
                if bytes.get(cursor) == Some(&b'(') {
                    let (end, text) = scan_balanced(sql, cursor)?;
                    return Some((start, end, text));
                }
            }
            continue;
        }
        index += 1;
    }
    None
}

/// Skip a quoted span; `at` points at the opening quote. Handles doubled-quote escapes.
fn skip_quoted(bytes: &[u8], at: usize, quote: u8) -> usize {
    let mut index = at + 1;
    while index < bytes.len() {
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    index
}

/// Scan a balanced paren span; `at` points at `(`. Returns the end offset plus inner text.
fn scan_balanced(sql: &str, at: usize) -> Option<(usize, String)> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut index = at;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'\'' {
            index = skip_quoted(bytes, index, b'\'');
            continue;
        }
        if byte == b'"' || byte == b'`' {
            index = skip_quoted(bytes, index, byte);
            continue;
        }
        if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            depth -= 1;
            if depth == 0 {
                return Some((index + 1, sql[at + 1..index].to_string()));
            }
        }
        index += 1;
    }
    None
}

/// Parse clause text as `'key'='value'` pairs; `None` when the text is not pairs.
fn parse_options_pairs(text: &str) -> Option<Vec<(String, String)>> {
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut pairs = Vec::new();
    loop {
        while index < bytes.len() && (bytes[index].is_ascii_whitespace() || bytes[index] == b',') {
            index += 1;
        }
        if index >= bytes.len() {
            return Some(pairs);
        }
        let key = parse_single_literal(text, &mut index)?;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if bytes.get(index) != Some(&b'=') {
            return None;
        }
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let value = parse_single_literal(text, &mut index)?;
        pairs.push((key, value));
    }
}

/// Parse one single-quoted literal with `''` escapes; `None` on any other shape.
fn parse_single_literal(text: &str, index: &mut usize) -> Option<String> {
    let bytes = text.as_bytes();
    if bytes.get(*index) != Some(&b'\'') {
        return None;
    }
    *index += 1;
    let mut out = String::new();
    loop {
        let byte = *bytes.get(*index)?;
        if byte == b'\'' {
            if bytes.get(*index + 1) == Some(&b'\'') {
                out.push('\'');
                *index += 2;
                continue;
            }
            *index += 1;
            return Some(out);
        }
        out.push(byte as char);
        *index += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_insert_passes_through_untouched() {
        let sql = "INSERT INTO cat.ns.t SELECT a FROM v";
        let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(cleaned, sql);
        assert!(options.is_empty());
    }

    #[test]
    fn options_clause_extracts_and_strips() {
        let sql = "INSERT INTO cat.ns.t OPTIONS('snapshot-property.run_id'='abc', 'write-format'='parquet') SELECT a FROM v";
        let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(cleaned, "INSERT INTO cat.ns.t  SELECT a FROM v");
        assert_eq!(
            options.snapshot_extra,
            vec![("run_id".to_string(), "abc".to_string())]
        );
        assert_eq!(options.write_format.as_deref(), Some("parquet"));
    }

    #[test]
    fn suffix_lower_cases_like_spark() {
        let sql = "INSERT INTO t OPTIONS('SNAPSHOT-PROPERTY.UPPER_KEY'='v') SELECT 1";
        let (_, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(
            options.snapshot_extra,
            vec![("upper_key".to_string(), "v".to_string())]
        );
    }

    #[test]
    fn empty_suffix_keeps_empty_key_like_spark() {
        let sql = "INSERT INTO t OPTIONS('snapshot-property.'='v') SELECT 1";
        let (_, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(
            options.snapshot_extra,
            vec![("".to_string(), "v".to_string())]
        );
    }

    #[test]
    fn duplicate_keys_last_wins_like_spark() {
        let sql = "INSERT INTO t OPTIONS('write-format'='orc', 'WRITE-FORMAT'='parquet') SELECT 1";
        let (_, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(options.write_format.as_deref(), Some("parquet"));
    }

    #[test]
    fn orc_refuses_naming_the_registry_row() {
        let error = extract_statement_write_options("INSERT INTO t OPTIONS('write-format'='orc') SELECT 1")
            .expect_err("orc must refuse");
        let message = error.to_string();
        assert!(message.contains("ICE-WRITE-OPTIONS-1"), "{message}");
    }

    #[test]
    fn bogus_format_mirrors_spark_text() {
        let error = extract_statement_write_options("INSERT INTO t OPTIONS('write-format'='bogus') SELECT 1")
            .expect_err("bogus must refuse");
        assert!(error.to_string().contains("Invalid file format: bogus"));
    }

    #[test]
    fn unknown_keys_ignored_like_spark() {
        let sql = "INSERT INTO t OPTIONS('repark-nope'='zzz') SELECT 1";
        let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(cleaned, "INSERT INTO t  SELECT 1");
        assert!(!options.is_empty());
        assert!(options.snapshot_extra.is_empty());
    }

    #[test]
    fn quoted_options_word_never_matches() {
        let sql = "INSERT INTO t SELECT * FROM x WHERE y = 'OPTIONS('";
        let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(cleaned, sql);
        assert!(options.is_empty());
    }

    #[test]
    fn table_named_options_without_pairs_passes_through() {
        let sql = "CREATE TABLE options (a INT)";
        let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(cleaned, sql);
        assert!(options.is_empty());
    }

    #[test]
    fn non_insert_create_passes_through() {
        let sql = "SELECT OPTIONS('a'='b')";
        let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(cleaned, sql);
        assert!(options.is_empty());
    }

    #[test]
    fn create_namespace_and_view_keep_their_text() {
        for sql in [
            "CREATE NAMESPACE x WITH PROPERTIES ('a'='b')",
            "CREATE VIEW v OPTIONS('a'='b') AS SELECT 1",
            "CREATE OR REPLACE VIEW v AS SELECT 1",
        ] {
            let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
            assert_eq!(cleaned, sql, "{sql}");
            assert!(options.is_empty());
        }
    }

    #[test]
    fn create_or_replace_table_strips() {
        let sql = "CREATE OR REPLACE TABLE t OPTIONS('write-format'='parquet') AS SELECT 1";
        let (cleaned, options) = extract_statement_write_options(sql).expect("extract");
        assert_eq!(cleaned, "CREATE OR REPLACE TABLE t  AS SELECT 1");
        assert_eq!(options.write_format.as_deref(), Some("parquet"));
    }
}
