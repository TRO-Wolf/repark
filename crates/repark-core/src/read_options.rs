//! CSV/JSON read-option helpers for Spark-style option maps.

use std::collections::{HashMap, HashSet};

use datafusion::prelude::{CsvReadOptions, DataFrame, JsonReadOptions, SessionContext};
use repark_common::{Error, Result};

use crate::engine_err;

/// Build [`CsvReadOptions`] from a lowercased Spark option map.
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
        // DataFusion matches listing files by `file_extension` only.
        opts = opts.file_extension(csv_extension_for_compression(compression_type));
    }
    Ok(opts)
}

/// Build [`JsonReadOptions`] from a Spark option map.
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

/// Listing extension DataFusion expects for a compressed CSV (matches DF's own compression tests).
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

/// Listing extension DataFusion expects for a compressed JSON file.
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

/// True when a file name is a CSV listing candidate (plain or compressed compound suffix).
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub(crate) fn csv_listing_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with(".csv")
        || lower.ends_with(".csv.gz")
        || lower.ends_with(".csv.bz2")
        || lower.ends_with(".csv.xz")
        || lower.ends_with(".csv.zst")
}

/// True when the name ends with a known compression suffix (gz/bz2/xz/zst).
pub(crate) fn name_looks_compressed(name: &str) -> bool {
    matches!(
        name.rsplit('.')
            .next()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("gz" | "bz2" | "xz" | "zst")
    )
}

pub(crate) fn csv_force_utf8_schema(options: &HashMap<String, String>) -> bool {
    options.contains_key("nullvalue")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecretColumnFlag {
    Off,
    Warn,
    Refuse,
}

pub(crate) fn secret_column_flag(options: &HashMap<String, String>) -> Result<SecretColumnFlag> {
    let Some(raw) = options
        .get("flag_secret_columns")
        .or_else(|| options.get("flagsecretcolumns"))
    else {
        return Ok(SecretColumnFlag::Off);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(SecretColumnFlag::Off),
        "warn" => Ok(SecretColumnFlag::Warn),
        "refuse" => Ok(SecretColumnFlag::Refuse),
        _ => Err(Error::Analysis(format!(
            "reader option \"flag_secret_columns\" accepts only \"off\", \"warn\", or \
             \"refuse\"; got {:?}",
            repark_common::redaction::mask_value_credentials(raw)
        ))),
    }
}

pub(crate) fn apply_secret_column_flag(
    flag: SecretColumnFlag,
    schema: &arrow::datatypes::Schema,
) -> Result<()> {
    if flag == SecretColumnFlag::Off {
        return Ok(());
    }
    let flagged: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| repark_common::redaction::column_name_is_secret_shaped(name))
        .collect();
    if flagged.is_empty() {
        return Ok(());
    }
    let names = flagged.join(", ");
    match flag {
        SecretColumnFlag::Warn => {
            eprintln!(
                "WARNING: flag_secret_columns=warn: credential-shaped column names \
                 in the read schema: {names}"
            );
            Ok(())
        }
        SecretColumnFlag::Refuse => Err(Error::Analysis(format!(
            "flag_secret_columns=refuse: credential-shaped column names \
             in the read schema: {names}"
        ))),
        SecretColumnFlag::Off => Ok(()),
    }
}

pub(crate) async fn read_csv_path(
    context: &SessionContext,
    path: &str,
    options: &HashMap<String, String>,
) -> Result<DataFrame> {
    let resolved = crate::object_store_s3::resolve_s3_prefix_for_read(context, path).await?;
    let flag = secret_column_flag(options)?;
    let mut csv_options = csv_read_options_from_map(options)?;
    // nullValue: force all-Utf8 schema so the scan path never type-parses null tokens.
    let multiline = options
        .get("multiline")
        .is_some_and(|raw| parse_bool_option("multiLine", raw).ok() == Some(true));
    let utf8_schema = if options.contains_key(CSV_TEXT_SCAN_OPTION) {
        let null_value = options.get("nullvalue").map(String::as_str);
        Some(csv_text_scan_schema(context, &resolved, &csv_options, null_value).await?)
    } else if csv_force_utf8_schema(options) || options.contains_key("utf8_columns") || multiline {
        csv_utf8_schema_from_path(path, csv_options.has_header, csv_options.delimiter)?
    } else {
        None
    };
    if let Some(ref schema) = utf8_schema {
        csv_options = csv_options.schema(schema);
    }
    let frame = context
        .read_csv(&resolved, csv_options.clone())
        .await
        .map_err(engine_err)?;
    let frame = if utf8_schema.is_some() {
        frame
    } else {
        match csv_utf8_column_schema(options, frame.schema().as_ref()) {
            None => frame,
            Some(schema) => context
                .read_csv(&resolved, csv_options.schema(&schema))
                .await
                .map_err(engine_err)?,
        }
    };
    apply_secret_column_flag(flag, frame.schema().as_ref())?;
    Ok(frame)
}

pub(crate) const CSV_TEXT_SCAN_OPTION: &str = "text_scan";

async fn csv_text_scan_schema(
    context: &SessionContext,
    path: &str,
    csv_options: &CsvReadOptions<'_>,
    null_value: Option<&str>,
) -> Result<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::file_format::options::ReadOptions;
    use datafusion::datasource::listing::ListingTableUrl;

    let header_only = csv_options.clone().schema_infer_max_records(0);
    let table_path = ListingTableUrl::parse(path).map_err(engine_err)?;
    let scanned = header_only
        .get_resolved_schema(&context.copied_config(), context.state(), table_path)
        .await
        .map_err(engine_err)?;
    let mut seen: HashMap<String, usize> = HashMap::new();
    for field in scanned.fields() {
        *seen.entry(field.name().to_lowercase()).or_default() += 1;
    }
    let fields: Vec<Field> = scanned
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = field.name().as_str();
            let spark_name = if !csv_options.has_header {
                name.to_string()
            } else if name.is_empty() || null_value == Some(name) {
                format!("_c{index}")
            } else if seen
                .get(&name.to_lowercase())
                .is_some_and(|count| *count > 1)
            {
                format!("{name}{index}")
            } else {
                name.to_string()
            };
            Field::new(spark_name, DataType::Utf8, true)
        })
        .collect();
    Ok(Schema::new(fields))
}

pub(crate) fn csv_utf8_column_schema(
    options: &HashMap<String, String>,
    inferred: &arrow::datatypes::Schema,
) -> Option<arrow::datatypes::Schema> {
    let raw = options.get("utf8_columns")?;
    let names: HashSet<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    let fields: Vec<arrow::datatypes::Field> = inferred
        .fields()
        .iter()
        .map(|field| {
            if names.contains(field.name().as_str()) {
                arrow::datatypes::Field::new(field.name(), arrow::datatypes::DataType::Utf8, true)
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    Some(arrow::datatypes::Schema::new(fields))
}

/// Build an all-Utf8 schema from the first local CSV record when `nullValue` is set.
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

    // Skip local pre-scan on compressed parts; gzip plus nullValue still round-trips clean strings.
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

/// Split a single CSV header/data line on `delimiter`, stripping optional surrounding quotes.
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

/// Parse a Spark-style boolean option (`true`/`false`/`1`/`0`, case-insensitive).
pub(crate) fn parse_bool_option(key: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "t" | "y" => Ok(true),
        "false" | "0" | "no" | "f" | "n" => Ok(false),
        _ => Err(Error::Analysis(format!(
            "reader option {key:?} expects a boolean, got {:?}",
            repark_common::redaction::mask_value_credentials(raw)
        ))),
    }
}

/// Parse a single-byte CSV option (sep/quote/escape/comment).
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
        "reader option {key:?} expects a single character, got {:?}",
        repark_common::redaction::mask_value_credentials(raw)
    )))
}

/// Escape a literal string for use as a full-match regex (nullValue token).
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

/// Normalize Spark compression names to DataFusion `FileCompressionType` tokens.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn options_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn a_boolean_option_refusal_masks_a_url_password() {
        let message = parse_bool_option("header", "jdbc:postgresql://u:ReadPw1@db.example.com/s")
            .expect_err("a URL is not a boolean")
            .to_string();
        assert!(!message.contains("ReadPw1"), "{message}");
        assert!(message.contains("u:***@db.example.com"), "{message}");
    }

    fn utf8_schema_of(names: &[&str]) -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(
            names
                .iter()
                .map(|name| {
                    arrow::datatypes::Field::new(*name, arrow::datatypes::DataType::Utf8, true)
                })
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn secret_column_flag_defaults_off_when_unset() {
        let options = options_map(&[("header", "true")]);
        assert_eq!(secret_column_flag(&options).unwrap(), SecretColumnFlag::Off);
    }

    #[test]
    fn secret_column_flag_parses_each_accepted_value_case_and_space_insensitive() {
        for (raw, expected) in [
            ("off", SecretColumnFlag::Off),
            ("warn", SecretColumnFlag::Warn),
            ("refuse", SecretColumnFlag::Refuse),
            (" WARN ", SecretColumnFlag::Warn),
            ("REFUSE", SecretColumnFlag::Refuse),
        ] {
            let options = options_map(&[("flag_secret_columns", raw)]);
            assert_eq!(secret_column_flag(&options).unwrap(), expected, "{raw}");
        }
    }

    #[test]
    fn secret_column_flag_accepts_the_folded_spelling() {
        let options = options_map(&[("flagsecretcolumns", "warn")]);
        assert_eq!(
            secret_column_flag(&options).unwrap(),
            SecretColumnFlag::Warn
        );
    }

    #[test]
    fn secret_column_flag_refuses_bad_values_naming_option_and_choices() {
        let options = options_map(&[("flag_secret_columns", "bogus")]);
        let Err(error) = secret_column_flag(&options) else {
            panic!("a value outside off/warn/refuse must refuse");
        };
        let message = error.to_string();
        for token in ["flag_secret_columns", "off", "warn", "refuse"] {
            assert!(message.contains(token), "{token} missing from: {message}");
        }
    }

    #[test]
    fn apply_secret_column_flag_off_and_warn_pass_flagged_schemas() {
        let flagged = utf8_schema_of(&["password", "note"]);
        apply_secret_column_flag(SecretColumnFlag::Off, &flagged).unwrap();
        apply_secret_column_flag(SecretColumnFlag::Warn, &flagged).unwrap();
    }

    #[test]
    fn apply_secret_column_flag_refuse_names_only_flagged_columns() {
        let schema = utf8_schema_of(&["id", "password", "bucket_key", "note"]);
        let Err(error) = apply_secret_column_flag(SecretColumnFlag::Refuse, &schema) else {
            panic!("a flagged schema under refuse must raise");
        };
        let message = error.to_string();
        assert!(message.contains("refuse"), "{message}");
        let listed: Vec<&str> = message.rsplit(": ").next().unwrap().split(", ").collect();
        assert_eq!(listed, ["password"], "{message}");
    }

    #[test]
    fn apply_secret_column_flag_refuse_passes_a_clean_schema() {
        let schema = utf8_schema_of(&["id", "note", "bucket_key"]);
        apply_secret_column_flag(SecretColumnFlag::Refuse, &schema).unwrap();
    }

    #[tokio::test]
    async fn read_options_flag_secret_columns_csv_refuse_names_flagged_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(&path, "id,password,bucket_key\n1,repark-fake-1,obj\n").unwrap();
        let session = crate::ReparkSession::builder().build().unwrap();
        let options = options_map(&[("header", "true"), ("flag_secret_columns", "refuse")]);
        let Err(error) = session.read_csv(path.to_str().unwrap(), &options).await else {
            panic!("a credential-shaped column under refuse must raise");
        };
        let message = error.to_string();
        let listed: Vec<&str> = message.rsplit(": ").next().unwrap().split(", ").collect();
        assert_eq!(listed, ["password"], "{message}");
    }

    #[tokio::test]
    async fn read_options_flag_secret_columns_csv_off_and_warn_read_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(
            &path,
            "id,password,note\n1,repark-fake-1,x\n2,repark-fake-2,y\n",
        )
        .unwrap();
        let session = crate::ReparkSession::builder().build().unwrap();
        for mode in ["off", "warn"] {
            let options = options_map(&[("header", "true"), ("flag_secret_columns", mode)]);
            let frame = session
                .read_csv(path.to_str().unwrap(), &options)
                .await
                .unwrap();
            let batches = frame.collect().await.unwrap();
            let rows: usize = batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(rows, 2, "{mode} must read every row");
        }
    }

    #[tokio::test]
    async fn read_options_flag_secret_columns_csv_bad_value_refuses_before_read() {
        let session = crate::ReparkSession::builder().build().unwrap();
        let options = options_map(&[("flag_secret_columns", "loud")]);
        let Err(error) = session.read_csv("/nonexistent/nowhere.csv", &options).await else {
            panic!("a bad flag value must refuse before any read");
        };
        assert!(error.to_string().contains("flag_secret_columns"), "{error}");
    }

    #[tokio::test]
    async fn a_file_url_csv_utf8_columns_read_types_only_the_named_columns_as_utf8() {
        use arrow::datatypes::DataType;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(
            &path,
            "n,ts,d\n1,2024-03-10 02:30:00,1e2\n2,2024-01-01 03:00:00,1.5E-1\n",
        )
        .unwrap();
        let session = crate::ReparkSession::builder().build().unwrap();
        let types_of = |frame: &DataFrame| -> Vec<(String, DataType)> {
            frame
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect()
        };
        let local = path.to_str().unwrap().to_string();
        let typed = [DataType::Int64, DataType::Utf8, DataType::Float64];
        let text = [DataType::Utf8, DataType::Utf8, DataType::Utf8];
        for (target, expected) in [(format!("file://{local}"), typed), (local, text)] {
            let pairs = [("header", "true"), ("utf8_columns", "ts")];
            let frame = session
                .read_csv(&target, &options_map(&pairs))
                .await
                .unwrap();
            let names: Vec<String> = ["n", "ts", "d"].map(str::to_string).into();
            assert_eq!(
                types_of(&frame),
                names.into_iter().zip(expected).collect::<Vec<_>>(),
                "{target}"
            );
        }
    }

    fn text_scan_rows(frame_batches: &[arrow::array::RecordBatch]) -> Vec<Vec<Option<String>>> {
        use arrow::array::{Array, AsArray};
        let mut rows = Vec::new();
        for batch in frame_batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let text = column.as_string::<i32>();
                            text.is_valid(row).then(|| text.value(row).to_string())
                        })
                        .collect(),
                );
            }
        }
        rows
    }

    async fn text_scan(
        target: &str,
        pairs: &[(&str, &str)],
    ) -> (Vec<String>, Vec<Vec<Option<String>>>) {
        let session = crate::ReparkSession::builder().build().unwrap();
        let mut options = options_map(pairs);
        options.insert(CSV_TEXT_SCAN_OPTION.to_string(), "true".to_string());
        let frame = session.read_csv(target, &options).await.unwrap();
        let names = frame
            .schema()
            .fields()
            .iter()
            .map(|field| {
                assert_eq!(field.data_type(), &arrow::datatypes::DataType::Utf8);
                field.name().clone()
            })
            .collect();
        (names, text_scan_rows(&frame.collect().await.unwrap()))
    }

    fn texts(row: &[&str]) -> Vec<Option<String>> {
        row.iter()
            .map(|text| (!text.is_empty()).then(|| (*text).to_string()))
            .collect()
    }

    #[tokio::test]
    async fn a_text_scan_reads_every_field_as_the_file_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(&path, "x,k\n007,1e3\nTRUE,1.50\n").unwrap();
        let (names, rows) = text_scan(path.to_str().unwrap(), &[("header", "true")]).await;
        assert_eq!(names, ["x", "k"]);
        assert_eq!(rows, [texts(&["007", "1e3"]), texts(&["TRUE", "1.50"])]);
    }

    #[tokio::test]
    async fn a_text_scan_names_an_empty_or_null_token_header_by_its_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(&path, ",NA, k \n007,1.5,abc\n").unwrap();
        let pairs = [("header", "true"), ("nullvalue", "NA")];
        let (names, rows) = text_scan(path.to_str().unwrap(), &pairs).await;
        assert_eq!(names, ["_c0", "_c1", " k "]);
        assert_eq!(rows, [texts(&["007", "1.5", "abc"])]);
        let (names, _) = text_scan(path.to_str().unwrap(), &[("header", "true")]).await;
        assert_eq!(names, ["_c0", "NA", " k "]);
        std::fs::write(&path, "x,X,k\n1,2,3\n").unwrap();
        let (names, _) = text_scan(path.to_str().unwrap(), &[("header", "true")]).await;
        assert_eq!(names, ["x0", "X1", "k"]);
    }

    #[tokio::test]
    async fn a_text_scan_reads_a_misfit_past_the_inference_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("late.csv");
        let mut body: String = (0..1200).map(|index| format!("{index}\n")).collect();
        body.insert_str(0, "n\n");
        body.push_str("abc\n1.5\n");
        std::fs::write(&path, body).unwrap();
        let (_, rows) = text_scan(path.to_str().unwrap(), &[("header", "true")]).await;
        assert_eq!(rows.len(), 1202);
        assert_eq!(rows[1200..], [texts(&["abc"]), texts(&["1.5"])]);
    }

    #[tokio::test]
    async fn a_text_scan_over_a_glob_reads_each_file_without_a_schema_merge() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.csv"), "x,k\n007,k1\n").unwrap();
        std::fs::write(dir.path().join("b.csv"), "x,k\n1.50,k2\n").unwrap();
        let glob = format!("{}/*.csv", dir.path().to_str().unwrap());
        let (names, mut rows) = text_scan(&glob, &[("header", "true")]).await;
        rows.sort();
        assert_eq!(names, ["x", "k"]);
        assert_eq!(rows, [texts(&["007", "k1"]), texts(&["1.50", "k2"])]);
    }

    #[tokio::test]
    async fn read_options_flag_secret_columns_json_refuse_names_flagged_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(&path, "{\"id\":1,\"password\":\"repark-fake\"}\n").unwrap();
        let session = crate::ReparkSession::builder().build().unwrap();
        let options = options_map(&[("flag_secret_columns", "refuse")]);
        let Err(error) = session.read_json(path.to_str().unwrap(), &options).await else {
            panic!("a credential-shaped JSON column under refuse must raise");
        };
        assert!(error.to_string().contains("password"), "{error}");
    }
}
