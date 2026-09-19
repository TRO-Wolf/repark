use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::execute_stream;
use futures::StreamExt;
use repark_core::ReparkSession;
use repark_iceberg::catalog::{glue_catalog_counted, s3tables_catalog_counted};
use serde_json::{Value, json};

use crate::BoxError;
use crate::bed::{self, BedShape, TS_BASE_SECONDS, TS_STEP_SECONDS};
use crate::cli::{Outcome, boxed};
use crate::r3::{R3Verdict, StepSummary, r3_gate};
use crate::report::{self, QueryRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Cold,
    Warm,
    Concurrent,
}

impl Mode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "cold" => Ok(Self::Cold),
            "warm" => Ok(Self::Warm),
            "concurrent" => Ok(Self::Concurrent),
            other => Err(format!("unknown mode `{other}` (cold|warm|concurrent)")),
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::Concurrent => "concurrent",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogChoice {
    Local,
    Glue,
    S3Tables,
}

impl CatalogChoice {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "local" => Ok(Self::Local),
            "glue" => Ok(Self::Glue),
            "s3tables" => Ok(Self::S3Tables),
            other => Err(format!("unknown catalog `{other}` (local|glue|s3tables)")),
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Glue => "glue",
            Self::S3Tables => "s3tables",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOptions {
    pub mode: Mode,
    pub warehouse: Option<PathBuf>,
    pub out: Option<PathBuf>,
    pub catalog: CatalogChoice,
    pub props: Vec<(String, String)>,
    pub table: Option<String>,
    pub manifest: Option<PathBuf>,
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySpec {
    pub name: &'static str,
    pub label: &'static str,
    pub sql: String,
}

pub const CONCURRENT_QUERIES: [&str; 4] = ["Q2", "Q3", "Q5", "Q6"];

#[must_use]
pub fn queries(table: &str, shape: &BedShape) -> Vec<QuerySpec> {
    let window_rows = (shape.rows / 100).max(1);
    let window_first_id = i64::try_from(shape.rows * 45 / 100).unwrap_or(0);
    let window_rows = i64::try_from(window_rows).unwrap_or(1);
    let ts_low = TS_BASE_SECONDS + window_first_id * TS_STEP_SECONDS;
    let ts_high = ts_low + window_rows * TS_STEP_SECONDS;
    let point_id = shape.rows / 2 + 17;
    vec![
        QuerySpec {
            name: "Q1",
            label: "count(*)",
            sql: format!("SELECT count(*) FROM {table}"),
        },
        QuerySpec {
            name: "Q2",
            label: "full-scan aggregate over value",
            sql: format!("SELECT sum(value), min(value), max(value) FROM {table}"),
        },
        QuerySpec {
            name: "Q3",
            label: "1% ts window",
            sql: format!(
                "SELECT id, value FROM {table} WHERE ts >= CAST({ts_low} AS TIMESTAMP) \
                 AND ts < CAST({ts_high} AS TIMESTAMP)"
            ),
        },
        QuerySpec {
            name: "Q4",
            label: "id point lookup",
            sql: format!("SELECT * FROM {table} WHERE id = {point_id}"),
        },
        QuerySpec {
            name: "Q5",
            label: "value range filter",
            sql: format!("SELECT id, value FROM {table} WHERE value >= 500.0 AND value < 505.0"),
        },
        QuerySpec {
            name: "Q6",
            label: "category equality + payload",
            sql: format!("SELECT id, payload FROM {table} WHERE category = 'cat_7'"),
        },
    ]
}

struct Target {
    table: String,
    shape: BedShape,
    warehouse: Option<PathBuf>,
}

fn resolve_target(options: &RunOptions) -> Result<Target, BoxError> {
    match options.catalog {
        CatalogChoice::Local => {
            let warehouse = options
                .warehouse
                .as_ref()
                .ok_or_else(|| boxed("run --catalog local needs --warehouse"))?;
            let warehouse = std::fs::canonicalize(warehouse)?;
            let manifest = options
                .manifest
                .clone()
                .unwrap_or_else(|| bed::manifest_path(&warehouse));
            let shape = bed::read_shape(&manifest)?;
            if shape.metadata_location.is_none() {
                return Err(boxed("the bed manifest lacks `metadata_location`"));
            }
            Ok(Target {
                table: bed::local_table_name(),
                shape,
                warehouse: Some(warehouse),
            })
        }
        CatalogChoice::Glue | CatalogChoice::S3Tables => {
            let table = options.table.as_ref().ok_or_else(|| {
                boxed(format!(
                    "run --catalog {} needs --table <namespace.table>",
                    options.catalog.name()
                ))
            })?;
            let manifest = options.manifest.as_ref().ok_or_else(|| {
                boxed(format!(
                    "run --catalog {} needs --manifest <bed.json> for the query constants",
                    options.catalog.name()
                ))
            })?;
            Ok(Target {
                table: format!("{}.{table}", bed::CATALOG),
                shape: bed::read_shape(manifest)?,
                warehouse: None,
            })
        }
    }
}

async fn open_session(options: &RunOptions, target: &Target) -> Result<ReparkSession, BoxError> {
    let session = bed::spark_session()?;
    let props: std::collections::HashMap<String, String> = options.props.iter().cloned().collect();
    match options.catalog {
        CatalogChoice::Local => {
            let warehouse = target
                .warehouse
                .as_deref()
                .ok_or_else(|| boxed("a local run has no warehouse"))?;
            let metadata = target
                .shape
                .metadata_location
                .as_deref()
                .ok_or_else(|| boxed("the bed manifest lacks `metadata_location`"))?;
            bed::register_local_table(&session, warehouse, metadata).await?;
        }
        CatalogChoice::Glue => {
            let catalog = glue_catalog_counted(&props, session.iceberg_io_counters()).await?;
            session
                .register_iceberg_catalog(bed::CATALOG, catalog)
                .await?;
        }
        CatalogChoice::S3Tables => {
            let catalog = s3tables_catalog_counted(&props, session.iceberg_io_counters()).await?;
            session
                .register_iceberg_catalog(bed::CATALOG, catalog)
                .await?;
        }
    }
    Ok(session)
}

pub async fn run(
    options: &RunOptions,
    summary: &StepSummary,
    size_override: Option<u64>,
) -> Result<Outcome, BoxError> {
    let target = resolve_target(options)?;
    let mut specs = queries(&target.table, &target.shape);
    if let Some(only) = &options.query {
        specs.retain(|spec| spec.name == only);
        if specs.is_empty() {
            return Err(boxed(format!("unknown query `{only}` (Q1..Q6)")));
        }
    }
    let gate_session = open_session(options, &target).await?;
    let (footprint, verdict) =
        r3_gate(&gate_session, &target.table, summary, size_override).await?;
    let gate_io = gate_session.iceberg_io_stats();
    if verdict == R3Verdict::Flagged {
        return Ok(Outcome::SizeFlagged(Box::new(gate_io)));
    }
    let started = Instant::now();
    let (records, group) = match options.mode {
        Mode::Cold => (run_cold(options, &target, &specs).await?, None),
        Mode::Warm => (run_warm(&gate_session, &specs).await?, None),
        Mode::Concurrent => {
            let (records, group) = run_concurrent(&gate_session, &specs).await?;
            (records, Some(group))
        }
    };
    let document = json!({
        "environment": report::environment(),
        "mode": options.mode.name(),
        "catalog": options.catalog.name(),
        "table": target.table,
        "bed": {
            "rows": target.shape.rows,
            "files": footprint.files,
            "delete_files": footprint.delete_files,
            "bytes": footprint.bytes,
            "metadata_location": target.shape.metadata_location,
        },
        "r3": {
            "bytes": footprint.bytes,
            "limit": crate::r3::R3_TABLE_SIZE_LIMIT_BYTES,
            "flagged": false,
            "gate_io": report::io_json(&gate_io),
        },
        "queries": records.iter().map(QueryRecord::to_json).collect::<Vec<Value>>(),
        "concurrent_group": group.as_ref().map(QueryRecord::to_json),
        "wall_seconds": started.elapsed().as_secs_f64(),
    });
    let text = serde_json::to_string_pretty(&document)?;
    if let Some(out) = &options.out {
        write_out(out, &text)?;
        println!("json={}", out.display());
    } else {
        println!("{text}");
    }
    println!(
        "{}",
        report::markdown(options.mode.name(), &records, group.as_ref())
    );
    Ok(Outcome::Done)
}

fn write_out(path: &Path, text: &str) -> Result<(), BoxError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, text)?;
    Ok(())
}

async fn run_cold(
    options: &RunOptions,
    target: &Target,
    specs: &[QuerySpec],
) -> Result<Vec<QueryRecord>, BoxError> {
    let mut records = Vec::new();
    for spec in specs {
        let session = open_session(options, target).await?;
        let registered = session.iceberg_io_stats();
        session.reset_iceberg_io_stats();
        let mut record = measured(&session, spec).await?;
        record.register_io = Some(registered);
        records.push(record);
    }
    Ok(records)
}

async fn run_warm(
    session: &ReparkSession,
    specs: &[QuerySpec],
) -> Result<Vec<QueryRecord>, BoxError> {
    let mut records = Vec::new();
    for spec in specs {
        execute(session, spec).await?;
        records.push(measured(session, spec).await?);
    }
    Ok(records)
}

async fn run_concurrent(
    session: &ReparkSession,
    specs: &[QuerySpec],
) -> Result<(Vec<QueryRecord>, QueryRecord), BoxError> {
    let chosen: Vec<&QuerySpec> = CONCURRENT_QUERIES
        .iter()
        .filter_map(|name| specs.iter().find(|spec| spec.name == *name))
        .collect();
    if chosen.is_empty() {
        return Err(boxed(
            "the concurrent mode runs Q2, Q3, Q5 and Q6; none was selected",
        ));
    }
    for spec in &chosen {
        execute(session, spec).await?;
    }
    let before = report::probe_before(session);
    let started = Instant::now();
    let results = futures::future::join_all(chosen.iter().map(|spec| execute(session, spec))).await;
    let wall = started.elapsed();
    let mut records = Vec::new();
    for (spec, result) in chosen.iter().zip(results) {
        let timing = result?;
        records.push(QueryRecord::new(spec, timing, None));
    }
    let rows = records.iter().map(|record| record.timing.rows).sum();
    let group_spec = QuerySpec {
        name: "concurrent",
        label: "Q2 + Q3 + Q5 + Q6 at once",
        sql: String::new(),
    };
    let group = QueryRecord::new(
        &group_spec,
        Timing {
            planning: Duration::ZERO,
            first_batch: None,
            total: wall,
            rows,
        },
        Some(&report::probe_after(session, &before)),
    );
    Ok((records, group))
}

async fn measured(session: &ReparkSession, spec: &QuerySpec) -> Result<QueryRecord, BoxError> {
    let before = report::probe_before(session);
    let timing = execute(session, spec).await?;
    Ok(QueryRecord::new(
        spec,
        timing,
        Some(&report::probe_after(session, &before)),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    pub planning: Duration,
    pub first_batch: Option<Duration>,
    pub total: Duration,
    pub rows: usize,
}

pub async fn execute(session: &ReparkSession, spec: &QuerySpec) -> Result<Timing, BoxError> {
    let started = Instant::now();
    let frame = session.sql(&spec.sql).await?;
    let task = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    let planning = started.elapsed();
    let mut stream = execute_stream(plan, task)?;
    let mut first_batch = None;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if first_batch.is_none() {
            first_batch = Some(started.elapsed());
        }
        rows += batch.num_rows();
    }
    Ok(Timing {
        planning,
        first_batch,
        total: started.elapsed(),
        rows,
    })
}
