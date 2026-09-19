use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::{displayable, execute_stream};
use futures::StreamExt;
use repark_core::ReparkSession;
use serde_json::json;

use crate::BoxError;
use crate::bed::{self, BedShape, TS_BASE_SECONDS, TS_STEP_SECONDS};
use crate::cli::{Outcome, boxed};
use crate::r3::{R3Verdict, StepSummary, TableFootprint, r3_gate};
use crate::remote;
use crate::report::{self, QueryRecord, RunTally};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Cold,
    Warm,
    Concurrent,
    ConcurrentCold,
}

impl Mode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "cold" => Ok(Self::Cold),
            "warm" => Ok(Self::Warm),
            "concurrent" => Ok(Self::Concurrent),
            "concurrent-cold" => Ok(Self::ConcurrentCold),
            other => Err(format!(
                "unknown mode `{other}` (cold|warm|concurrent|concurrent-cold)"
            )),
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::Concurrent => "concurrent",
            Self::ConcurrentCold => "concurrent-cold",
        }
    }

    #[must_use]
    pub fn cold_kind(self) -> Option<&'static str> {
        match self {
            Self::Cold | Self::ConcurrentCold => Some("new_session_same_process"),
            Self::Warm | Self::Concurrent => None,
        }
    }

    #[must_use]
    pub fn concurrent_kind(self) -> Option<&'static str> {
        match self {
            Self::Concurrent => Some("join_all_four_warmed_queries_one_session_one_driver_task"),
            Self::ConcurrentCold => {
                Some("join_all_four_queries_fresh_session_no_warmup_one_driver_task")
            }
            Self::Cold | Self::Warm => None,
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
    pub repeat: usize,
    pub files: Option<usize>,
    pub rows_per_file: Option<u64>,
}

impl RunOptions {
    #[must_use]
    pub fn local(mode: Mode, warehouse: &Path) -> Self {
        Self {
            mode,
            warehouse: Some(warehouse.to_path_buf()),
            out: None,
            catalog: CatalogChoice::Local,
            props: Vec::new(),
            table: None,
            manifest: None,
            query: None,
            repeat: 1,
            files: None,
            rows_per_file: None,
        }
    }
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
    let window_first = shape.rows * 45 / 100;
    let window_end = window_first + window_rows;
    let first_signed = i64::try_from(window_first).unwrap_or(0);
    let rows_signed = i64::try_from(window_rows).unwrap_or(1);
    let ts_low = TS_BASE_SECONDS + first_signed * TS_STEP_SECONDS;
    let ts_high = ts_low + rows_signed * TS_STEP_SECONDS;
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
        QuerySpec {
            name: "Q7",
            label: "Q3's 1% window as an id range (reaches the scan)",
            sql: format!(
                "SELECT id, value FROM {table} WHERE id >= {window_first} AND id < {window_end}"
            ),
        },
    ]
}

pub struct Target {
    pub table: String,
    pub shape: BedShape,
    pub warehouse: Option<PathBuf>,
}

pub fn resolve_target(options: &RunOptions) -> Result<Target, BoxError> {
    match options.catalog {
        CatalogChoice::Local => {
            if options.files.is_some() || options.rows_per_file.is_some() {
                return Err(boxed(
                    "run --catalog local reads --files / --rows-per-file from the bed manifest",
                ));
            }
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
            remote::split_table(table)?;
            if options.manifest.is_some() {
                return Err(boxed(format!(
                    "run --catalog {} derives its constants from --files / --rows-per-file; \
                     --manifest is for a local bed",
                    options.catalog.name()
                )));
            }
            Ok(Target {
                table: format!("{}.{table}", bed::CATALOG),
                shape: BedShape::from_counts(
                    options.files.unwrap_or(bed::DEFAULT_FILES),
                    options.rows_per_file.unwrap_or(bed::DEFAULT_ROWS_PER_FILE),
                ),
                warehouse: None,
            })
        }
    }
}

async fn open_session(options: &RunOptions, target: &Target) -> Result<ReparkSession, BoxError> {
    let session = bed::spark_session()?;
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
        CatalogChoice::Glue | CatalogChoice::S3Tables => {
            remote::register_remote_catalog(&session, options.catalog, &options.props).await?;
        }
    }
    Ok(session)
}

fn require_bed_shape(
    table: &str,
    footprint: &TableFootprint,
    shape: &BedShape,
) -> Result<(), BoxError> {
    if footprint.data_files() == shape.files
        && footprint.delete_files == 0
        && footprint.data_rows == shape.rows
    {
        return Ok(());
    }
    Err(boxed(format!(
        "{table} holds {} data files ({} rows) and {} delete files; the query constants assume \
         exactly {} data files, {} rows and no delete file",
        footprint.data_files(),
        footprint.data_rows,
        footprint.delete_files,
        shape.files,
        shape.rows
    )))
}

pub trait SessionSource {
    fn open(&self) -> impl Future<Output = Result<ReparkSession, BoxError>>;
}

pub struct ConfiguredSource<'a> {
    pub options: &'a RunOptions,
    pub target: &'a Target,
}

impl SessionSource for ConfiguredSource<'_> {
    async fn open(&self) -> Result<ReparkSession, BoxError> {
        open_session(self.options, self.target).await
    }
}

pub struct Measurements {
    pub records: Vec<QueryRecord>,
    pub groups: Vec<QueryRecord>,
}

pub async fn run(
    options: &RunOptions,
    summary: &StepSummary,
    size_override: Option<u64>,
) -> Result<Outcome, BoxError> {
    let target = resolve_target(options)?;
    let source = ConfiguredSource {
        options,
        target: &target,
    };
    run_gated(options, &target, &source, summary, size_override).await
}

pub async fn run_gated<S: SessionSource>(
    options: &RunOptions,
    target: &Target,
    source: &S,
    summary: &StepSummary,
    size_override: Option<u64>,
) -> Result<Outcome, BoxError> {
    let mut specs = queries(&target.table, &target.shape);
    if let Some(only) = &options.query {
        specs.retain(|spec| spec.name == only);
        if specs.is_empty() {
            return Err(boxed(format!("unknown query `{only}` (Q1..Q7)")));
        }
    }
    let repeat = options.repeat.max(1);
    let gate_session = source.open().await?;
    let (footprint, verdict) =
        r3_gate(&gate_session, &target.table, summary, size_override).await?;
    if verdict == R3Verdict::Flagged {
        return Ok(Outcome::SizeFlagged(Box::new(
            gate_session.iceberg_io_stats(),
        )));
    }
    let gate_io = gate_session.iceberg_io_stats();
    require_bed_shape(&target.table, &footprint, &target.shape)?;
    let tally = RunTally::default();
    let mut predicates = Vec::new();
    for spec in &specs {
        predicates.push((spec.name, scan_predicate(&gate_session, spec).await?));
    }
    tally.drain(&gate_session);
    let started = Instant::now();
    let measurements = measure(options.mode, source, &specs, repeat, &gate_session, &tally).await?;
    tally.drain(&gate_session);
    let wall_seconds = started.elapsed().as_secs_f64();
    let (queries_json, mut mismatches) = report::queries_json(
        options.mode.name(),
        &specs,
        &predicates,
        &measurements.records,
    );
    let group_json = if measurements.groups.is_empty() {
        None
    } else {
        let (json, group_mismatches) =
            report::group_json(options.mode.name(), &measurements.groups);
        mismatches.extend(group_mismatches);
        Some(json)
    };
    let run_total = tally.total();
    let document = json!({
        "environment": report::environment(options.mode),
        "mode": options.mode.name(),
        "catalog": options.catalog.name(),
        "table": target.table,
        "repeat": repeat,
        "bed": {
            "rows": target.shape.rows,
            "files": footprint.data_files(),
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
        "queries": queries_json,
        "concurrent_group": group_json,
        "io_mismatches": mismatches,
        "run_io_total": {"requests": run_total.requests, "bytes": run_total.bytes},
        "wall_seconds": wall_seconds,
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
        report::markdown(
            options.mode.name(),
            &specs,
            &measurements.records,
            &measurements.groups
        )
    );
    println!(
        "run_io_total requests={} bytes={}",
        run_total.requests, run_total.bytes
    );
    Ok(Outcome::Done)
}

async fn measure<S: SessionSource>(
    mode: Mode,
    source: &S,
    specs: &[QuerySpec],
    repeat: usize,
    gate_session: &ReparkSession,
    tally: &RunTally,
) -> Result<Measurements, BoxError> {
    let measurements = match mode {
        Mode::Cold => run_cold(source, specs, repeat, tally).await?,
        Mode::Warm => run_warm(gate_session, specs, repeat, tally).await?,
        Mode::Concurrent => {
            let chosen = concurrent_specs(specs)?;
            for spec in &chosen {
                execute(gate_session, spec).await?;
            }
            let mut measurements = Measurements {
                records: Vec::new(),
                groups: Vec::new(),
            };
            for _ in 0..repeat {
                concurrent_round(gate_session, &chosen, tally, &mut measurements).await?;
            }
            measurements
        }
        Mode::ConcurrentCold => {
            let chosen = concurrent_specs(specs)?;
            let mut measurements = Measurements {
                records: Vec::new(),
                groups: Vec::new(),
            };
            for _ in 0..repeat {
                let session = source.open().await?;
                let registered = session.iceberg_io_stats();
                concurrent_round(&session, &chosen, tally, &mut measurements).await?;
                if let Some(group) = measurements.groups.last_mut() {
                    group.register_io = Some(registered);
                }
                tally.drain(&session);
            }
            measurements
        }
    };
    Ok(measurements)
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

async fn run_cold<S: SessionSource>(
    source: &S,
    specs: &[QuerySpec],
    repeat: usize,
    tally: &RunTally,
) -> Result<Measurements, BoxError> {
    let mut records = Vec::new();
    for _ in 0..repeat {
        for spec in specs {
            let session = source.open().await?;
            let registered = session.iceberg_io_stats();
            let mut record = measured(&session, spec, tally).await?;
            record.register_io = Some(registered);
            records.push(record);
            tally.drain(&session);
        }
    }
    Ok(Measurements {
        records,
        groups: Vec::new(),
    })
}

async fn run_warm(
    session: &ReparkSession,
    specs: &[QuerySpec],
    repeat: usize,
    tally: &RunTally,
) -> Result<Measurements, BoxError> {
    let mut records = Vec::new();
    for spec in specs {
        execute(session, spec).await?;
        for _ in 0..repeat {
            records.push(measured(session, spec, tally).await?);
        }
    }
    Ok(Measurements {
        records,
        groups: Vec::new(),
    })
}

fn concurrent_specs(specs: &[QuerySpec]) -> Result<Vec<&QuerySpec>, BoxError> {
    let chosen: Vec<&QuerySpec> = CONCURRENT_QUERIES
        .iter()
        .filter_map(|name| specs.iter().find(|spec| spec.name == *name))
        .collect();
    if chosen.is_empty() {
        return Err(boxed(
            "the concurrent modes run Q2, Q3, Q5 and Q6; none was selected",
        ));
    }
    Ok(chosen)
}

async fn concurrent_round(
    session: &ReparkSession,
    chosen: &[&QuerySpec],
    tally: &RunTally,
    measurements: &mut Measurements,
) -> Result<(), BoxError> {
    let before = report::probe_before(session, tally);
    let started = Instant::now();
    let results = futures::future::join_all(chosen.iter().map(|spec| execute(session, spec))).await;
    let wall = started.elapsed();
    let delta = report::probe_after(session, &before);
    let mut rows = 0;
    for (spec, result) in chosen.iter().zip(results) {
        let timing = result?;
        rows += timing.rows;
        measurements
            .records
            .push(QueryRecord::new(spec, timing, None));
    }
    let names: Vec<&str> = chosen.iter().map(|spec| spec.name).collect();
    let group_spec = QuerySpec {
        name: "concurrent",
        label: "Q2 + Q3 + Q5 + Q6 at once",
        sql: names.join(" + "),
    };
    measurements.groups.push(QueryRecord::new(
        &group_spec,
        Timing {
            planning: Duration::ZERO,
            first_batch: None,
            total: wall,
            rows,
        },
        Some(&delta),
    ));
    Ok(())
}

async fn measured(
    session: &ReparkSession,
    spec: &QuerySpec,
    tally: &RunTally,
) -> Result<QueryRecord, BoxError> {
    let before = report::probe_before(session, tally);
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

impl Timing {
    #[must_use]
    pub fn execute_to_first_batch(&self) -> Option<Duration> {
        self.first_batch
            .map(|first| first.saturating_sub(self.planning))
    }
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

#[must_use]
pub fn iceberg_scan_predicate(plan_text: &str) -> Option<String> {
    let line = plan_text
        .lines()
        .find(|line| line.contains("IcebergTableScan"))?;
    let rest = line.split_once("predicate:[")?.1;
    let (predicate, _) = rest.rsplit_once(']')?;
    Some(predicate.to_string())
}

pub async fn scan_predicate(session: &ReparkSession, spec: &QuerySpec) -> Result<String, BoxError> {
    let plan = session.sql(&spec.sql).await?.create_physical_plan().await?;
    let text = displayable(plan.as_ref()).indent(false).to_string();
    iceberg_scan_predicate(&text).ok_or_else(|| {
        boxed(format!(
            "{}: the physical plan has no IcebergTableScan:\n{text}",
            spec.name
        ))
    })
}
