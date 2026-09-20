use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::file::metadata::PageIndexPolicy;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergScanOptions;
use repark_core::{ReparkSession, SqlDialect};
use repark_iceberg::catalog::{
    FOOTER_CACHE_BYTES_KEY, MANIFEST_CACHE_BYTES_KEY, METADATA_CACHE_KEY,
};
use repark_spark::{SparkDialect, SparkExtension};
use serde_json::{Value, json};

use crate::BoxError;
use crate::cli::{Outcome, boxed};
use crate::r3::{R3Verdict, StepSummary, quoted_files_table, r3_gate};

pub const DEFAULT_FILES: usize = 200;

pub const DEFAULT_ROWS_PER_FILE: u64 = 50_000;

pub const CATALOG: &str = "bench";

pub const NAMESPACE: &str = "perf";

pub const TABLE: &str = "events";

pub const MANIFEST_FILE: &str = "ice_read_perf_bed.json";

pub const TS_BASE_SECONDS: i64 = 1_700_000_000;

pub const TS_STEP_SECONDS: i64 = 7;

pub const CATEGORIES: u64 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupOptions {
    pub warehouse: PathBuf,
    pub files: usize,
    pub rows_per_file: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BedShape {
    pub rows: u64,
    pub files: u64,
    pub metadata_location: Option<String>,
}

impl BedShape {
    #[must_use]
    pub fn from_counts(files: usize, rows_per_file: u64) -> Self {
        let files = u64::try_from(files).unwrap_or(u64::MAX);
        Self {
            rows: files.saturating_mul(rows_per_file),
            files,
            metadata_location: None,
        }
    }
}

pub fn spark_session(baseline: bool) -> Result<ReparkSession, BoxError> {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let mut builder = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .with_extension(Arc::new(SparkExtension));
    if baseline {
        builder = builder
            .config(METADATA_CACHE_KEY, "false")
            .config(MANIFEST_CACHE_BYTES_KEY, "0")
            .config(FOOTER_CACHE_BYTES_KEY, "0");
    }
    let session = builder.build()?;
    if baseline {
        let mut options = IcebergScanOptions::default();
        options.row_selection_enabled = false;
        session
            .context()
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .extensions
            .insert(options);
    }
    Ok(session)
}

#[must_use]
pub fn local_table_name() -> String {
    format!("{CATALOG}.{NAMESPACE}.{TABLE}")
}

#[must_use]
pub fn manifest_path(warehouse: &Path) -> PathBuf {
    warehouse.join(MANIFEST_FILE)
}

fn path_text(path: &Path) -> Result<&str, BoxError> {
    path.to_str()
        .ok_or_else(|| boxed(format!("path {} is not UTF-8", path.display())))
}

#[must_use]
pub fn insert_sql(table: &str, first_id: u64, rows: u64) -> String {
    let end = first_id + rows;
    format!(
        "INSERT INTO {table} SELECT id, \
         CAST({TS_BASE_SECONDS} + id * {TS_STEP_SECONDS} AS TIMESTAMP) AS ts, \
         concat('cat_', CAST(pmod(id * 2654435761, {CATEGORIES}) AS STRING)) AS category, \
         CAST(pmod(id * 40503, 1000003) AS DOUBLE) / 1000.0 AS value, \
         concat(sha2(concat('a', CAST(id AS STRING)), 256), \
         sha2(concat('b', CAST(id AS STRING)), 256), \
         sha2(concat('c', CAST(id AS STRING)), 256), \
         sha2(concat('d', CAST(id AS STRING)), 256)) AS payload \
         FROM range({first_id}, {end}) ORDER BY id"
    )
}

#[must_use]
pub fn create_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (id BIGINT, ts TIMESTAMP, category STRING, value DOUBLE, \
         payload STRING) USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
}

pub async fn write_files(
    session: &ReparkSession,
    table: &str,
    files: usize,
    rows_per_file: u64,
) -> Result<f64, BoxError> {
    let started = Instant::now();
    for file in 0..files {
        let first_id = u64::try_from(file)? * rows_per_file;
        session
            .sql(&insert_sql(table, first_id, rows_per_file))
            .await?
            .collect()
            .await?;
        let written = file + 1;
        if written % 20 == 0 || written == files {
            println!(
                "setup: {written}/{files} files in {:.1}s",
                started.elapsed().as_secs_f64()
            );
        }
    }
    Ok(started.elapsed().as_secs_f64())
}

pub async fn setup(options: &SetupOptions, summary: &StepSummary) -> Result<Outcome, BoxError> {
    std::fs::create_dir_all(&options.warehouse)?;
    let warehouse = std::fs::canonicalize(&options.warehouse)?;
    let root = path_text(&warehouse)?;
    let table_dir = warehouse.join(NAMESPACE).join(TABLE);
    if table_dir.exists() {
        return Err(boxed(format!(
            "{} already exists; setup writes a fresh bed into an empty warehouse",
            table_dir.display()
        )));
    }
    let session = spark_session(false)?;
    session.register_memory_catalog(CATALOG, root).await?;
    session
        .create_namespace(
            CATALOG,
            NAMESPACE,
            HashMap::from([("location".to_string(), format!("{root}/{NAMESPACE}"))]),
        )
        .await?;
    let table = local_table_name();
    session
        .sql(&create_table_sql(&table))
        .await?
        .collect()
        .await?;
    let setup_seconds = write_files(&session, &table, options.files, options.rows_per_file).await?;
    let handle = session
        .catalogs_snapshot()
        .get(CATALOG)
        .cloned()
        .ok_or_else(|| boxed("the bench catalog is not registered"))?;
    let loaded = handle
        .load_table(&TableIdent::from_strs([NAMESPACE, TABLE])?)
        .await?;
    let metadata_location = loaded
        .metadata_location()
        .ok_or_else(|| boxed("the bench table has no metadata location"))?
        .to_string();
    let (footprint, verdict) = r3_gate(&session, &table, summary, None).await?;
    let expected_files = u64::try_from(options.files)?;
    if footprint.data_files() != expected_files || footprint.delete_files != 0 {
        return Err(boxed(format!(
            "the bed has {} files ({} delete files); setup promises exactly {expected_files} data files",
            footprint.files, footprint.delete_files
        )));
    }
    let pages = first_file_pages(&session, &table).await?;
    let manifest = json!({
        "bench": "ice_read_perf",
        "layout": "memory catalog over a local directory; one INSERT per data file",
        "catalog": CATALOG,
        "table": table,
        "warehouse": root,
        "metadata_location": metadata_location,
        "files": footprint.files,
        "rows_per_file": options.rows_per_file,
        "rows": expected_files * options.rows_per_file,
        "bytes": footprint.bytes,
        "ts_base_seconds": TS_BASE_SECONDS,
        "ts_step_seconds": TS_STEP_SECONDS,
        "categories": CATEGORIES,
        "first_file_pages_per_column": pages,
        "setup_seconds": setup_seconds,
    });
    std::fs::write(
        manifest_path(&warehouse),
        serde_json::to_string_pretty(&manifest)?,
    )?;
    println!("metadata_location={metadata_location}");
    println!("files={} bytes={}", footprint.files, footprint.bytes);
    println!("manifest={}", manifest_path(&warehouse).display());
    Ok(match verdict {
        R3Verdict::WithinLimit => Outcome::Done,
        R3Verdict::Flagged => Outcome::SizeFlagged(Box::new(session.iceberg_io_stats())),
    })
}

async fn first_file_pages(session: &ReparkSession, table: &str) -> Result<Value, BoxError> {
    let batches = session
        .sql(&format!(
            "SELECT file_path FROM {} LIMIT 1",
            quoted_files_table(table)
        ))
        .await?
        .collect()
        .await?;
    let path = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .map(|column| column.value(0).to_string())
        })
        .ok_or_else(|| boxed("the bed has no data file"))?;
    let file = std::fs::File::open(path.trim_start_matches("file://"))?;
    let metadata = ArrowReaderMetadata::load(
        &file,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )?;
    let parquet = metadata.metadata();
    let schema = parquet.file_metadata().schema_descr();
    let offsets = parquet
        .offset_index()
        .ok_or_else(|| boxed("the first data file carries no offset index"))?;
    let mut pages = serde_json::Map::new();
    for (index, column) in schema.columns().iter().enumerate() {
        let count: usize = offsets
            .iter()
            .map(|row_group| row_group.get(index).map_or(0, |o| o.page_locations().len()))
            .sum();
        pages.insert(column.name().to_string(), json!(count));
    }
    Ok(json!({
        "row_groups": parquet.num_row_groups(),
        "pages": Value::Object(pages),
    }))
}

pub fn read_shape(path: &Path) -> Result<BedShape, BoxError> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        boxed(format!(
            "cannot read bed manifest {}: {error}",
            path.display()
        ))
    })?;
    let manifest: Value = serde_json::from_str(&text)?;
    let number = |key: &str| {
        manifest[key]
            .as_u64()
            .ok_or_else(|| boxed(format!("bed manifest {} lacks `{key}`", path.display())))
    };
    Ok(BedShape {
        rows: number("rows")?,
        files: number("files")?,
        metadata_location: manifest["metadata_location"].as_str().map(str::to_string),
    })
}

pub async fn register_local_table(
    session: &ReparkSession,
    warehouse: &Path,
    metadata_location: &str,
) -> Result<(), BoxError> {
    session
        .register_memory_catalog(CATALOG, path_text(warehouse)?)
        .await?;
    let handle = session
        .catalogs_snapshot()
        .get(CATALOG)
        .cloned()
        .ok_or_else(|| boxed("the bench catalog is not registered"))?;
    handle
        .create_namespace(&NamespaceIdent::new(NAMESPACE.to_string()), HashMap::new())
        .await?;
    handle
        .register_table(
            &TableIdent::from_strs([NAMESPACE, TABLE])?,
            metadata_location.to_string(),
        )
        .await?;
    session.refresh_catalog_provider(CATALOG).await?;
    Ok(())
}
