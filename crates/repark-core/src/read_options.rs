//! CSV/JSON read-option helpers for Spark-style option maps.

use std::collections::HashMap;

use datafusion::prelude::{CsvReadOptions, JsonReadOptions};
use repark_common::{Error, Result};

/// ===========================================================================================
/// Build [`CsvReadOptions`] from a lowercased Spark option map. Recognized keys are header,
/// delimiter, quote, escape, comment, nullvalue, multiline, and compression.
/// ===========================================================================================
///
/// # Errors
/// Malformed single-byte options or unknown compression → [`Error::Analysis`].
pub(crate) fn csv_read_options_from_map(
    options: &HashMap<String, String>,
) -> Result<CsvReadOptions<'static>> {
    use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
    use std::str::FromStr;

    // Spark default header=false when the facade does not pass header; DF new() defaults true.
    let mut opts = CsvReadOptions::new().has_header(false);
    if let Some(header) = options.get("header") {
        opts = opts.has_header(parse_bool_option("header", header)?);
    }
    if let Some(sep) = options.get("sep").or_else(|| options.get("delimiter")) {
        opts = opts.delimiter(parse_single_byte_option("sep", sep)?);
    }
    if let Some(quote) = options.get("quote") {
        opts = opts.quote(parse_single_byte_option("quote", quote)?);
    }
    if let Some(escape) = options.get("escape") {
        opts = opts.escape(parse_single_byte_option("escape", escape)?);
    }
    if let Some(comment) = options.get("comment")
        && !comment.is_empty()
    {
        opts = opts.comment(parse_single_byte_option("comment", comment)?);
    }
    if let Some(null_value) = options.get("nullvalue") {
        // DataFusion null_regex: treat the literal token as null (escape regex metacharacters).
        let escaped = regex_escape_literal(null_value);
        opts = opts.null_regex(Some(format!("^{escaped}$")));
    }
    if let Some(multi_line) = options.get("multiline") {
        opts = opts.newlines_in_values(parse_bool_option("multiLine", multi_line)?);
    }
    if let Some(compression) = options.get("compression") {
        let normalized = normalize_compression_name(compression);
        let compression_type = FileCompressionType::from_str(&normalized).map_err(|_| {
            Error::Analysis(format!(
                "unsupported CSV compression {compression:?}; \
                 repark supports gzip, bzip2, xz, zstd, none/uncompressed"
            ))
        })?;
        opts = opts.file_compression_type(compression_type);
        // DataFusion matches listing files by `file_extension` only. Compressed COPY TO emits
        // `*.csv.gz` (etc.); the default extension `.csv` finds zero files.
        opts = opts.file_extension(csv_extension_for_compression(compression_type));
    }
    Ok(opts)
}

/// ===========================================================================================
/// Build [`JsonReadOptions`] from a Spark option map. `multiline` inverts `newline_delimited`;
/// `compression` is also recognized.
/// ===========================================================================================
///
/// # Errors
/// Unknown compression → [`Error::Analysis`].
pub(crate) fn json_read_options_from_map(
    options: &HashMap<String, String>,
) -> Result<JsonReadOptions<'static>> {
    use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
    use std::str::FromStr;

    let mut opts = JsonReadOptions::default();
    // Spark multiLine=false (default) ≡ NDJSON ≡ DF newline_delimited=true.
    if let Some(multi_line) = options.get("multiline") {
        let multi = parse_bool_option("multiLine", multi_line)?;
        opts = opts.newline_delimited(!multi);
    }
    if let Some(compression) = options.get("compression") {
        let normalized = normalize_compression_name(compression);
        let compression_type = FileCompressionType::from_str(&normalized).map_err(|_| {
            Error::Analysis(format!(
                "unsupported JSON compression {compression:?}; \
                 repark supports gzip, bzip2, xz, zstd, none/uncompressed"
            ))
        })?;
        opts = opts.file_compression_type(compression_type);
        // Mirror CSV: compressed writers emit `*.json.gz`; default `.json` finds nothing.
        opts = opts.file_extension(json_extension_for_compression(compression_type));
    }
    Ok(opts)
}

/// ===========================================================================================
/// Listing extension DataFusion expects for a compressed CSV (matches DF's own compression tests).
/// ===========================================================================================
pub(crate) fn csv_extension_for_compression(
    compression: datafusion::datasource::file_format::file_compression_type::FileCompressionType,
) -> &'static str {
    use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
    if compression == FileCompressionType::GZIP {
        "csv.gz"
    } else if compression == FileCompressionType::BZIP2 {
        "csv.bz2"
    } else if compression == FileCompressionType::XZ {
        "csv.xz"
    } else if compression == FileCompressionType::ZSTD {
        "csv.zst"
    } else {
        ".csv"
    }
}

/// ===========================================================================================
/// Listing extension DataFusion expects for a compressed JSON file.
/// ===========================================================================================
pub(crate) fn json_extension_for_compression(
    compression: datafusion::datasource::file_format::file_compression_type::FileCompressionType,
) -> &'static str {
    use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
    if compression == FileCompressionType::GZIP {
        "json.gz"
    } else if compression == FileCompressionType::BZIP2 {
        "json.bz2"
    } else if compression == FileCompressionType::XZ {
        "json.xz"
    } else if compression == FileCompressionType::ZSTD {
        "json.zst"
    } else {
        ".json"
    }
}

/// ===========================================================================================
/// True when a file name is a CSV listing candidate (plain or compressed compound suffix).
/// ===========================================================================================
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub(crate) fn csv_listing_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with(".csv")
        || lower.ends_with(".csv.gz")
        || lower.ends_with(".csv.bz2")
        || lower.ends_with(".csv.xz")
        || lower.ends_with(".csv.zst")
}

/// ===========================================================================================
/// True when the name ends with a known compression suffix (gz/bz2/xz/zst).
/// ===========================================================================================
pub(crate) fn name_looks_compressed(name: &str) -> bool {
    matches!(
        name.rsplit('.')
            .next()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("gz" | "bz2" | "xz" | "zst")
    )
}

/// ===========================================================================================
/// Build an all-Utf8 schema from the first local CSV record when `nullValue` is set. Object-store
/// paths retain DataFusion's typed inference.
/// ===========================================================================================
pub(crate) fn csv_utf8_schema_from_path(
    path: &str,
    has_header: bool,
    delimiter: u8,
) -> Result<Option<arrow::datatypes::Schema>> {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::fs;
    use std::io::{BufRead, BufReader};
    use std::path::Path;

    let path_obj = Path::new(path);
    // Object-store / remote URLs: no local pre-scan (facade residual).
    if path.contains("://") {
        return Ok(None);
    }
    let file_path = if path_obj.is_dir() {
        let mut entries: Vec<_> = fs::read_dir(path_obj)
            .map_err(|error| Error::Analysis(format!("cannot list CSV directory {path}: {error}")))?
            .filter_map(|entry| entry.ok().map(|dir_entry| dir_entry.path()))
            .filter(|candidate| {
                candidate
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(csv_listing_name)
            })
            .collect();
        entries.sort();
        match entries.into_iter().next() {
            Some(first) => first,
            None => return Ok(None),
        }
    } else if path_obj.is_file() {
        path_obj.to_path_buf()
    } else {
        return Ok(None);
    };

    // Compressed parts: skip local pre-scan (would need a decompress stream); DF utf8 force
    // residual for gzip+nullValue — round-trip still works when values are clean strings.
    if file_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(name_looks_compressed)
    {
        return Ok(None);
    }

    let file = fs::File::open(&file_path).map_err(|error| {
        Error::Analysis(format!(
            "cannot open CSV path {} for nullValue schema: {error}",
            file_path.display()
        ))
    })?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line).map_err(|error| {
            Error::Analysis(format!(
                "cannot read CSV header from {}: {error}",
                file_path.display()
            ))
        })?;
        if read == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if !trimmed.is_empty() {
            line = trimmed.to_string();
            break;
        }
    }

    // Minimal CSV split: honor delimiter; strip one layer of surrounding quotes per field.
    // Enough for header/name discovery; full CSV quoting is handled by the engine scan.
    let fields: Vec<Field> = split_csv_header_line(&line, delimiter)
        .into_iter()
        .enumerate()
        .map(|(index, raw_name)| {
            let name = if has_header {
                let name = raw_name.trim();
                if name.is_empty() {
                    format!("_c{index}")
                } else {
                    name.to_string()
                }
            } else {
                // Match DataFusion's default no-header naming; facade renames to Spark `_cN`.
                format!("column_{}", index + 1)
            };
            Field::new(name, DataType::Utf8, true)
        })
        .collect();
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(Schema::new(fields)))
}

/// ===========================================================================================
/// Split a single CSV header/data line on `delimiter`, stripping optional surrounding quotes.
/// ===========================================================================================
pub(crate) fn split_csv_header_line(line: &str, delimiter: u8) -> Vec<String> {
    let delim = delimiter as char;
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            if in_quotes && chars.peek() == Some(&'"') {
                current.push('"');
                chars.next();
            } else {
                in_quotes = !in_quotes;
            }
        } else if ch == delim && !in_quotes {
            fields.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    fields.push(current);
    fields
}

/// ===========================================================================================
/// Parse a Spark-style boolean option (`true`/`false`/`1`/`0`, case-insensitive).
/// ===========================================================================================
pub(crate) fn parse_bool_option(key: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "t" | "y" => Ok(true),
        "false" | "0" | "no" | "f" | "n" => Ok(false),
        _ => Err(Error::Analysis(format!(
            "reader option {key:?} expects a boolean, got {raw:?}"
        ))),
    }
}

/// ===========================================================================================
/// Parse a single-byte CSV option (sep/quote/escape/comment).
/// ===========================================================================================
pub(crate) fn parse_single_byte_option(key: &str, raw: &str) -> Result<u8> {
    let bytes = raw.as_bytes();
    if bytes.len() == 1 {
        return Ok(bytes[0]);
    }
    // Spark accepts some escape spellings; keep the surface tight.
    if raw == r"\\" {
        return Ok(b'\\');
    }
    if raw == r"\t" {
        return Ok(b'\t');
    }
    Err(Error::Analysis(format!(
        "reader option {key:?} expects a single character, got {raw:?}"
    )))
}

/// ===========================================================================================
/// Escape a literal string for use as a full-match regex (nullValue token).
/// ===========================================================================================
pub(crate) fn regex_escape_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            r @ ('.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$'
            | '\\') => {
                out.push('\\');
                out.push(r);
            }
            other => out.push(other),
        }
    }
    out
}

/// ===========================================================================================
/// Normalize Spark compression names to DataFusion `FileCompressionType` tokens.
/// ===========================================================================================
pub(crate) fn normalize_compression_name(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "none" | "uncompressed" => "UNCOMPRESSED".to_string(),
        "gzip" | "gz" => "GZIP".to_string(),
        "bzip2" | "bz2" => "BZIP2".to_string(),
        "xz" => "XZ".to_string(),
        "zstd" | "zst" => "ZSTD".to_string(),
        other => other.to_ascii_uppercase(),
    }
}
