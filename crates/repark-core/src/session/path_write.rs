use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::arrow_writer::ArrowWriter;
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::path::Path as ObjectPath;
use repark_common::{Error, Result};

use crate::OverwriteIntent;
use crate::engine_err;
use crate::object_store_s3;
use crate::session::ReparkSession;
use crate::session::df_guards::duplicate_names::recorded_display_names;
use crate::session::text_write_format::is_text_write_format_option;
use crate::session::text_write_format::select::{TextWriteCopyParts, merge_spec_options};
use crate::session::text_write_format::spec::{SPEC_WRITE_ID_KEY, TextWritePathRegistry};

mod append;
mod rollback;

impl ReparkSession {
    pub fn note_local_write_root(&self, path: &str) {
        self.catalogs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .note_local_warehouse_root(path.to_string());
    }
}

enum WriteFormat {
    Parquet,
    Csv,
    Json,
}

impl WriteFormat {
    fn parse(raw: &str) -> Result<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            _ => Err(Error::Analysis(format!(
                "unknown path write format '{}' (expected parquet, csv or json)",
                repark_common::redaction::mask_value_credentials(raw)
            ))),
        }
    }

    fn stored_as(&self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
            Self::Csv => "CSV",
            Self::Json => "JSON",
        }
    }

    fn extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Json => "json",
        }
    }
}

enum SaveMode {
    ErrorIfExists,
    Ignore,
    Overwrite,
    Append,
}

impl SaveMode {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "error" | "errorifexists" => Ok(Self::ErrorIfExists),
            "ignore" => Ok(Self::Ignore),
            "overwrite" => Ok(Self::Overwrite),
            "append" => Ok(Self::Append),
            _ => Err(Error::Analysis(format!(
                "path write mode must be one of ('append', 'overwrite', 'error', \
                 'errorifexists', 'ignore'), got '{}'",
                repark_common::redaction::mask_value_credentials(raw)
            ))),
        }
    }
}

fn is_simple_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|next| next.is_alphanumeric() || next == '_')
}

fn resolve_partition_columns(
    frame_columns: &[String],
    partition_by: &[String],
) -> Result<Vec<String>> {
    let frame_by_case: HashMap<String, String> = frame_columns
        .iter()
        .map(|name| (name.to_lowercase(), name.clone()))
        .collect();
    let mut resolved = Vec::with_capacity(partition_by.len());
    let mut seen = HashSet::with_capacity(partition_by.len());
    for column in partition_by {
        let Some(matched) = frame_by_case.get(&column.to_lowercase()) else {
            let available = frame_columns
                .iter()
                .map(|name| format!("'{name}'"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::Analysis(format!(
                "partitionBy column '{column}' is not in the DataFrame columns [{available}]; \
                 path partitionBy requires identity columns present on the frame (Spark-shaped)"
            )));
        };
        if !is_simple_identifier(matched) {
            return Err(Error::Analysis(format!(
                "partitionBy column '{matched}' is not a simple SQL identifier; repark path \
                 partitionBy supports simple column names only"
            )));
        }
        if !seen.insert(matched.to_lowercase()) {
            return Err(Error::Analysis(format!(
                "duplicate partitionBy column '{matched}'; path partitionBy requires unique \
                 column names"
            )));
        }
        resolved.push(matched.clone());
    }
    Ok(resolved)
}

fn partition_clause(resolved: &[String]) -> String {
    if resolved.is_empty() {
        String::new()
    } else {
        format!(" PARTITIONED BY ({})", resolved.join(", "))
    }
}

fn writer_bool(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "t" | "y"
    )
}

fn sql_escape(value: &str) -> String {
    value.replace('\'', "''")
}

fn with_text_write_id(spec_options_sql: String, view: &str) -> String {
    if spec_options_sql.is_empty() {
        spec_options_sql
    } else {
        format!(
            "{spec_options_sql}, '{SPEC_WRITE_ID_KEY}' '{}'",
            sql_escape(view)
        )
    }
}

fn normalize_write_compression(raw: &str) -> Result<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "none" | "uncompressed" => Ok("uncompressed"),
        "gzip" | "gz" => Ok("gzip"),
        "bzip2" | "bz2" => Ok("bzip2"),
        "xz" => Ok("xz"),
        "zstd" | "zst" => Ok("zstd"),
        _ => Err(Error::Analysis(format!(
            "unsupported write compression '{}'; repark supports gzip, bzip2, xz, zstd, \
             none/uncompressed",
            repark_common::redaction::mask_value_credentials(raw)
        ))),
    }
}

fn normalize_parquet_write_compression(raw: &str) -> Result<String> {
    let lowered = raw.trim().to_ascii_lowercase();
    match lowered.as_str() {
        "" | "none" | "uncompressed" => Ok("uncompressed".to_string()),
        "snappy" => Ok("snappy".to_string()),
        "gzip" | "gz" => Ok("gzip(6)".to_string()),
        "zstd" | "zst" => Ok("zstd(3)".to_string()),
        "lz4" => Ok("lz4".to_string()),
        other
            if other.starts_with("gzip(")
                || other.starts_with("zstd(")
                || other.starts_with("brotli(") =>
        {
            Ok(lowered)
        }
        _ => Err(Error::Analysis(format!(
            "unsupported parquet write compression '{}'; repark supports snappy, gzip, zstd, \
             lz4, none/uncompressed",
            repark_common::redaction::mask_value_credentials(raw)
        ))),
    }
}

fn refused_csv_option(key: &str) -> bool {
    matches!(
        key,
        "encoding"
            | "linesep"
            | "chartoescapequoteescaping"
            | "ignoreleadingwhitespace"
            | "ignoretrailingwhitespace"
            | "maxrecordsperfile"
            | "emptyvalue"
    )
}

fn refused_json_option(key: &str) -> bool {
    matches!(key, "encoding" | "linesep" | "ignorenullfields")
}

fn copy_options_sql(format: &WriteFormat, options: &HashMap<String, String>) -> Result<String> {
    if options.is_empty() {
        return Ok(String::new());
    }
    let mut ordered: Vec<(&String, &String)> = options.iter().collect();
    ordered.sort();
    let mut pairs = Vec::with_capacity(ordered.len());
    for (key, value) in ordered {
        let lowered = key.to_lowercase();
        if lowered == "path" {
            continue;
        }
        match format {
            WriteFormat::Csv => {
                if is_text_write_format_option(&lowered) {
                    continue;
                }
                if refused_csv_option(&lowered) {
                    return Err(Error::Analysis(format!(
                        "DataFrameWriter.csv option '{key}' is not supported yet (Spark \
                         SimpleDateFormat / encoding / lineSep knobs would silently diverge \
                         from DataFusion strftime writers if ignored or passed raw — \
                         refuse-loud; see task/r2-read-formats2-ledger.md)"
                    )));
                }
                if lowered == "header" {
                    pairs.push(format!("'format.has_header' '{}'", sql_escape(value)));
                } else if lowered == "sep" || lowered == "delimiter" {
                    pairs.push(format!("'format.delimiter' '{}'", sql_escape(value)));
                } else if lowered == "quote" {
                    pairs.push(format!("'format.quote' '{}'", sql_escape(value)));
                } else if lowered == "escape" {
                    pairs.push(format!("'format.escape' '{}'", sql_escape(value)));
                } else if lowered == "nullvalue" {
                    pairs.push(format!("'format.null_value' '{}'", sql_escape(value)));
                } else if lowered == "quoteall" {
                    let style = if writer_bool(value) {
                        "Always"
                    } else {
                        "Necessary"
                    };
                    pairs.push(format!("'format.quote_style' '{style}'"));
                } else if lowered == "escapequotes" {
                    let flag = if writer_bool(value) { "true" } else { "false" };
                    pairs.push(format!("'format.double_quote' '{flag}'"));
                } else if lowered == "compression" {
                    let token = normalize_write_compression(value)?;
                    pairs.push(format!("'format.compression' '{}'", sql_escape(token)));
                } else {
                    return Err(Error::Analysis(format!(
                        "DataFrameWriter.csv option '{key}' is not supported yet (would \
                         silently change write semantics if ignored)"
                    )));
                }
            }
            WriteFormat::Json => {
                if is_text_write_format_option(&lowered) {
                    continue;
                }
                if refused_json_option(&lowered) {
                    return Err(Error::Analysis(format!(
                        "DataFrameWriter.json option '{key}' is not supported yet (would \
                         silently change write semantics if ignored — refuse-loud; see \
                         task/r2-read-formats2-ledger.md)"
                    )));
                }
                if lowered == "compression" {
                    let token = normalize_write_compression(value)?;
                    pairs.push(format!("'format.compression' '{}'", sql_escape(token)));
                } else {
                    return Err(Error::Analysis(format!(
                        "DataFrameWriter.json option '{key}' is not supported yet (would \
                         silently change write semantics if ignored)"
                    )));
                }
            }
            WriteFormat::Parquet => {
                if lowered == "compression" {
                    let token = normalize_parquet_write_compression(value)?;
                    pairs.push(format!("'format.compression' '{}'", sql_escape(&token)));
                } else {
                    return Err(Error::Analysis(format!(
                        "DataFrameWriter.parquet/save option '{key}' is not supported yet \
                         (would silently change write semantics if ignored)"
                    )));
                }
            }
        }
    }
    if pairs.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(" OPTIONS ({})", pairs.join(", ")))
    }
}

fn parse_write_destination(url: &str) -> Result<(String, String, String)> {
    if let Some((scheme, bucket, key)) = object_store_s3::split_s3_url_raw(url) {
        return Ok((scheme, bucket, key));
    }
    match url.split_once("://") {
        Some((scheme, _)) if !object_store_s3::is_s3_scheme(&scheme.to_ascii_lowercase()) => {
            Err(Error::Analysis(format!(
                "path write destination '{}' is not an s3:// or s3a:// URL",
                repark_common::redaction::mask_value_credentials(url)
            )))
        }
        Some(_) => Err(Error::Analysis(format!(
            "path write destination '{}' has no bucket",
            repark_common::redaction::mask_value_credentials(url)
        ))),
        None => Err(Error::Analysis(format!(
            "invalid path write destination '{}'",
            repark_common::redaction::mask_value_credentials(url)
        ))),
    }
}

fn unique_view_name() -> String {
    let thread = format!("{:?}", std::thread::current().id());
    let thread_digits: String = thread.chars().filter(char::is_ascii_alphanumeric).collect();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |span| span.as_nanos());
    format!(
        "_repark_s3_write_{}_{thread_digits}_{nanos}",
        std::process::id()
    )
}

async fn prefix_has_objects(
    store: &dyn ObjectStore,
    scope: Option<&ObjectPath>,
    origin: &str,
) -> Result<bool> {
    let mut listed = store.list(scope);
    match listed.next().await {
        Some(Ok(_)) => Ok(true),
        Some(Err(error)) => Err(Error::DataFusion(format!(
            "cannot list S3 destination {origin}: {error}"
        ))),
        None => Ok(false),
    }
}

async fn exact_key_exists(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    origin: &str,
) -> Result<bool> {
    match store.head(prefix).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(Error::DataFusion(format!(
            "cannot probe S3 destination {origin}: {error}"
        ))),
    }
}

async fn destination_has_objects(
    store: &dyn ObjectStore,
    scope: Option<&ObjectPath>,
    prefix: &ObjectPath,
    at_root: bool,
    origin: &str,
) -> Result<bool> {
    if prefix_has_objects(store, scope, origin).await? {
        return Ok(true);
    }
    if at_root {
        return Ok(false);
    }
    exact_key_exists(store, prefix, origin).await
}

async fn prepare_overwrite_destination(
    store: &dyn ObjectStore,
    frame: &DataFrame,
    bucket: &str,
    prefix_text: &str,
    prefix: &ObjectPath,
    url: &str,
) -> Result<()> {
    if prefix_text.is_empty() {
        return Err(Error::Analysis(format!(
            "cannot overwrite path '{url}': refusing to delete a whole bucket \
             (overwrite needs a key prefix)"
        )));
    }
    if crate::plan_introspect::plan_reads_s3_prefix(frame.logical_plan(), bucket, prefix_text) {
        return Err(Error::Analysis(format!(
            "[UNSUPPORTED_OVERWRITE.PATH] Cannot overwrite the path {url} that is \
             also being read from."
        )));
    }
    delete_prefix_objects(store, Some(prefix), url).await?;
    if exact_key_exists(store, prefix, url).await? {
        store.delete(prefix).await.map_err(|error| {
            Error::DataFusion(format!(
                "cannot delete S3 object {prefix} under {url}: {error}"
            ))
        })?;
    }
    Ok(())
}

async fn delete_prefix_objects(
    store: &dyn ObjectStore,
    scope: Option<&ObjectPath>,
    origin: &str,
) -> Result<usize> {
    let mut listed = store.list(scope);
    let mut locations = Vec::new();
    while let Some(meta) = listed.next().await {
        let meta = meta.map_err(|error| {
            Error::DataFusion(format!("cannot list S3 destination {origin}: {error}"))
        })?;
        locations.push(meta.location);
    }
    for location in &locations {
        store.delete(location).await.map_err(|error| {
            Error::DataFusion(format!(
                "cannot delete S3 object {location} under {origin}: {error}"
            ))
        })?;
    }
    Ok(locations.len())
}

async fn list_part_keys(
    store: &dyn ObjectStore,
    scope: Option<&ObjectPath>,
    extension: &str,
    origin: &str,
) -> Result<Vec<ObjectPath>> {
    let suffix = format!(".{extension}");
    let mut listed = store.list(scope);
    let mut parts = Vec::new();
    while let Some(meta) = listed.next().await {
        let meta = meta.map_err(|error| {
            Error::DataFusion(format!("cannot list S3 destination {origin}: {error}"))
        })?;
        if meta
            .location
            .filename()
            .is_some_and(|name| name.ends_with(&suffix))
        {
            parts.push(meta.location);
        }
    }
    Ok(parts)
}

fn empty_parquet_bytes(schema: &SchemaRef) -> Result<Vec<u8>> {
    let mut buffer = std::io::Cursor::new(Vec::new());
    let writer = ArrowWriter::try_new(&mut buffer, Arc::clone(schema), None)
        .map_err(|error| Error::DataFusion(format!("cannot write empty parquet part: {error}")))?;
    writer
        .close()
        .map_err(|error| Error::DataFusion(format!("cannot write empty parquet part: {error}")))?;
    Ok(buffer.into_inner())
}

fn empty_csv_bytes(frame_columns: &[String], options: &HashMap<String, String>) -> Vec<u8> {
    let mut header_on = true;
    let mut separator = ",".to_string();
    for (key, value) in options {
        let lowered = key.to_lowercase();
        if lowered == "header" {
            header_on = writer_bool(value);
        } else if lowered == "sep" || lowered == "delimiter" {
            separator.clone_from(value);
        }
    }
    if header_on && !frame_columns.is_empty() {
        format!("{}\n", frame_columns.join(&separator)).into_bytes()
    } else {
        Vec::new()
    }
}

async fn materialize_empty_part(
    store: &dyn ObjectStore,
    location: &ObjectPath,
    format: &WriteFormat,
    schema: &SchemaRef,
    columns: &[String],
    options: &HashMap<String, String>,
    url: &str,
) -> Result<()> {
    let bytes = match format {
        WriteFormat::Parquet => empty_parquet_bytes(schema)?,
        WriteFormat::Csv => match recorded_display_names(schema) {
            Some(displays) => empty_csv_bytes(&displays, options),
            None => empty_csv_bytes(columns, options),
        },
        WriteFormat::Json => Vec::new(),
    };
    store
        .put(location, PutPayload::from(bytes))
        .await
        .map_err(|error| {
            Error::DataFusion(format!(
                "cannot write empty S3 part {location} under {url}: {error}"
            ))
        })?;
    Ok(())
}

struct S3Commit<'a> {
    store: &'a dyn ObjectStore,
    prefix: &'a ObjectPath,
    at_root: bool,
    url: &'a str,
    format: &'a WriteFormat,
    frame: &'a DataFrame,
    columns: &'a [String],
    partitioned: bool,
    options: &'a HashMap<String, String>,
    copy_sql: &'a str,
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    fn copy_inner_parts(
        &self,
        frame: &DataFrame,
        format: &WriteFormat,
        view: &str,
        options: &HashMap<String, String>,
        partitions: &[String],
    ) -> Result<TextWriteCopyParts> {
        match format {
            WriteFormat::Parquet => Ok(TextWriteCopyParts {
                select_sql: format!("SELECT * FROM {view}"),
                stored_as: format.stored_as().to_string(),
                spec_options_sql: String::new(),
            }),
            WriteFormat::Csv | WriteFormat::Json => {
                self.text_write_copy_parts(frame, view, options, partitions, format.stored_as())
            }
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn write_path(
        &self,
        frame: &DataFrame,
        url: &str,
        format: &str,
        mode: &str,
        options: &HashMap<String, String>,
        partition_by: &[String],
    ) -> Result<usize> {
        let format = WriteFormat::parse(format)?;
        let mode = SaveMode::parse(mode)?;
        let (scheme, bucket, prefix_text) = parse_write_destination(url)?;
        let frame_columns: Vec<String> = frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let resolved_partitions = resolve_partition_columns(&frame_columns, partition_by)?;
        let options_clause = copy_options_sql(&format, options)?;
        self.ensure_s3_bucket_registered(&bucket)?;
        let store_url = ObjectStoreUrl::parse(format!("s3://{bucket}")).map_err(engine_err)?;
        let store = self
            .context()
            .runtime_env()
            .object_store(&store_url)
            .map_err(engine_err)?;
        let prefix = ObjectPath::parse(&prefix_text)
            .map_err(|error| Error::DataFusion(format!("invalid S3 destination {url}: {error}")))?;
        let at_root = prefix_text.is_empty();
        let scope = if at_root { None } else { Some(&prefix) };
        match mode {
            SaveMode::ErrorIfExists => {
                if destination_has_objects(&store, scope, &prefix, at_root, url).await? {
                    return Err(Error::Analysis(format!(
                        "[PATH_ALREADY_EXISTS] Path {url} already exists. Set mode as \
                         \"overwrite\" to overwrite the existing path."
                    )));
                }
            }
            SaveMode::Ignore => {
                if destination_has_objects(&store, scope, &prefix, at_root, url).await? {
                    return Ok(list_part_keys(&store, scope, format.extension(), url)
                        .await?
                        .len());
                }
            }
            SaveMode::Overwrite => {
                prepare_overwrite_destination(&store, frame, &bucket, &prefix_text, &prefix, url)
                    .await?;
            }
            SaveMode::Append => {
                if !at_root && exact_key_exists(&store, &prefix, url).await? {
                    return Err(Error::Analysis(format!(
                        "cannot append to path {url}: an object already exists at the exact \
                         destination key, and the appended rows would be invisible through \
                         that path"
                    )));
                }
                let existing = list_part_keys(&store, scope, format.extension(), url).await?;
                if !existing.is_empty() {
                    let part_urls: Vec<String> = existing
                        .iter()
                        .map(|key| {
                            format!(
                                "s3://{bucket}/{}",
                                object_store_s3::encode_s3_key_for_url(key.as_ref())
                            )
                        })
                        .collect();
                    self.validate_append(
                        &format,
                        frame.schema().inner(),
                        &resolved_partitions,
                        options,
                        &part_urls,
                    )
                    .await?;
                }
            }
        }
        let view = unique_view_name();
        let copy_target = object_store_s3::write_target_url(&scheme, &bucket, &prefix_text);
        let parts = self.copy_inner_parts(frame, &format, &view, options, &resolved_partitions)?;
        let spec_options_sql = with_text_write_id(parts.spec_options_sql, &view);
        let options_clause = merge_spec_options(options_clause, &spec_options_sql);
        let copy_sql = format!(
            "COPY ({}) TO '{}' STORED AS {}{}{}",
            parts.select_sql,
            sql_escape(&copy_target),
            parts.stored_as,
            partition_clause(&resolved_partitions),
            options_clause
        );
        let commit = S3Commit {
            store: &store,
            prefix: &prefix,
            at_root,
            url,
            format: &format,
            frame,
            columns: &frame_columns,
            partitioned: !resolved_partitions.is_empty(),
            options,
            copy_sql: &copy_sql,
        };
        self.commit_s3_write(&view, &commit).await
    }

    async fn commit_s3_write(&self, view: &str, commit: &S3Commit<'_>) -> Result<usize> {
        match commit.format {
            WriteFormat::Parquet => self.commit_s3_write_parquet(view, commit).await,
            WriteFormat::Csv | WriteFormat::Json => self.commit_s3_write_text(view, commit).await,
        }
    }

    async fn commit_s3_write_parquet(&self, view: &str, commit: &S3Commit<'_>) -> Result<usize> {
        self.create_or_replace_temp_view_from(view, commit.frame)?;
        let scope = if commit.at_root {
            None
        } else {
            Some(commit.prefix)
        };
        let outcome = async {
            let copy_outcome = self
                .sql_built_with_write_options(
                    commit.copy_sql,
                    &HashMap::new(),
                    OverwriteIntent::Session,
                    false,
                )
                .await?;
            let _copy_batches = copy_outcome.collect().await.map_err(engine_err)?;
            let mut parts =
                list_part_keys(commit.store, scope, commit.format.extension(), commit.url).await?;
            if parts.is_empty() && !commit.partitioned {
                let location = if commit.at_root {
                    ObjectPath::from(format!("part-00000.{}", commit.format.extension()))
                } else {
                    commit
                        .prefix
                        .clone()
                        .join(format!("part-00000.{}", commit.format.extension()))
                };
                materialize_empty_part(
                    commit.store,
                    &location,
                    commit.format,
                    commit.frame.schema().inner(),
                    commit.columns,
                    commit.options,
                    commit.url,
                )
                .await?;
                parts = list_part_keys(commit.store, scope, commit.format.extension(), commit.url)
                    .await?;
            }
            let success = if commit.at_root {
                ObjectPath::from("_SUCCESS")
            } else {
                commit.prefix.clone().join("_SUCCESS")
            };
            commit
                .store
                .put(&success, PutPayload::from(Vec::<u8>::new()))
                .await
                .map_err(|error| {
                    Error::DataFusion(format!(
                        "cannot write S3 _SUCCESS under {}: {error}",
                        commit.url
                    ))
                })?;
            Ok::<usize, Error>(parts.len())
        }
        .await;
        match outcome {
            Ok(count) => {
                self.drop_temp_view(view)?;
                Ok(count)
            }
            Err(error) => {
                let _drop_result = self.drop_temp_view(view);
                Err(error)
            }
        }
    }

    async fn commit_s3_write_text(&self, view: &str, commit: &S3Commit<'_>) -> Result<usize> {
        self.create_or_replace_temp_view_from(view, commit.frame)?;
        let scope = if commit.at_root {
            None
        } else {
            Some(commit.prefix)
        };
        let mut materialized: Option<ObjectPath> = None;
        let outcome = async {
            self.register_text_write(view);
            let copy_outcome = self
                .sql_built_with_write_options(
                    commit.copy_sql,
                    &HashMap::new(),
                    OverwriteIntent::Session,
                    false,
                )
                .await?;
            let _copy_batches = copy_outcome.collect().await.map_err(engine_err)?;
            let mut parts =
                list_part_keys(commit.store, scope, commit.format.extension(), commit.url).await?;
            if parts.is_empty() && !commit.partitioned {
                let location = if commit.at_root {
                    ObjectPath::from(format!("part-00000.{}", commit.format.extension()))
                } else {
                    commit
                        .prefix
                        .clone()
                        .join(format!("part-00000.{}", commit.format.extension()))
                };
                materialize_empty_part(
                    commit.store,
                    &location,
                    commit.format,
                    commit.frame.schema().inner(),
                    commit.columns,
                    commit.options,
                    commit.url,
                )
                .await?;
                materialized = Some(location);
                parts = list_part_keys(commit.store, scope, commit.format.extension(), commit.url)
                    .await?;
            }
            let success = if commit.at_root {
                ObjectPath::from("_SUCCESS")
            } else {
                commit.prefix.clone().join("_SUCCESS")
            };
            commit
                .store
                .put(&success, PutPayload::from(Vec::<u8>::new()))
                .await
                .map_err(|error| {
                    Error::DataFusion(format!(
                        "cannot write S3 _SUCCESS under {}: {error}",
                        commit.url
                    ))
                })?;
            Ok::<usize, Error>(parts.len())
        }
        .await;
        let mut locations = self.take_text_write_paths(view);
        locations.extend(materialized);
        match outcome {
            Ok(count) => {
                self.drop_temp_view(view)?;
                Ok(count)
            }
            Err(error) => {
                let _drop_result = self.drop_temp_view(view);
                let cleanup =
                    rollback::delete_recorded_keys(commit.store, &locations, commit.url).await;
                match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(rollback::with_cleanup_note(
                        error,
                        &rollback::cleanup_message(&cleanup),
                    )),
                }
            }
        }
    }

    fn register_text_write(&self, view: &str) {
        let state_lock = self.context().state_ref();
        let mut state = state_lock.write();
        let options = state.config_mut().options_mut();
        if options.extensions.get::<TextWritePathRegistry>().is_none() {
            options.extensions.insert(TextWritePathRegistry::default());
        }
        if let Some(registry) = options.extensions.get::<TextWritePathRegistry>() {
            registry.register(view);
        }
    }

    fn take_text_write_paths(&self, view: &str) -> Vec<ObjectPath> {
        self.context()
            .copied_config()
            .options()
            .extensions
            .get::<TextWritePathRegistry>()
            .map(|registry| registry.take(view))
            .unwrap_or_default()
    }
}
