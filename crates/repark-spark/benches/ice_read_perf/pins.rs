mod bed;
mod cli;
mod r3;
mod remote;
mod report;
mod run;

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use repark_core::ReparkSession;
use repark_iceberg::catalog::{IcebergFileClass, IcebergIoOp};
use serde_json::Value;
use tempfile::TempDir;

use crate::bed::{BedShape, SetupOptions};
use crate::cli::{Command, EXIT_FAILURE, EXIT_USAGE, Outcome};
use crate::r3::{
    R3_EXIT_CODE, R3_TABLE_SIZE_LIMIT_BYTES, R3Verdict, StepSummary, r3_check, r3_verdict,
    table_footprint,
};
use crate::remote::{
    CreateOutcome, NamespaceRule, Phase, PhaseRequest, RemoteSetupOptions, WriteOutcome,
};
use crate::run::{CatalogChoice, Mode, RunOptions, SessionSource};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_string()).collect()
}

fn text(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn local_run(mode: Mode, warehouse: &Path) -> RunOptions {
    RunOptions::local(mode, warehouse)
}

fn summary_in(dir: &Path) -> StepSummary {
    StepSummary {
        path: Some(dir.join("step-summary.md")),
    }
}

#[test]
fn the_r3_limit_is_three_gibibytes_and_flags_only_above_it() {
    assert_eq!(R3_TABLE_SIZE_LIMIT_BYTES, 3_221_225_472);
    assert_eq!(
        r3_verdict(R3_TABLE_SIZE_LIMIT_BYTES - 1),
        R3Verdict::WithinLimit
    );
    assert_eq!(
        r3_verdict(R3_TABLE_SIZE_LIMIT_BYTES),
        R3Verdict::WithinLimit
    );
    assert_eq!(
        r3_verdict(R3_TABLE_SIZE_LIMIT_BYTES + 1),
        R3Verdict::Flagged
    );
    assert_eq!(r3_verdict(0), R3Verdict::WithinLimit);
    assert_eq!(r3_verdict(u64::MAX), R3Verdict::Flagged);
}

#[test]
fn the_r3_exit_code_is_distinct() {
    assert_eq!(R3_EXIT_CODE, 3);
    for other in [0, EXIT_FAILURE, EXIT_USAGE] {
        assert_ne!(R3_EXIT_CODE, other);
    }
}

#[test]
fn a_flag_appends_one_line_to_the_step_summary_and_a_pass_appends_none() {
    let dir = TempDir::new().unwrap();
    let summary = summary_in(dir.path());
    let path = summary.path.clone().unwrap();
    assert_eq!(
        r3_check("t", R3_TABLE_SIZE_LIMIT_BYTES, &summary).unwrap(),
        R3Verdict::WithinLimit
    );
    assert!(!path.exists());
    assert_eq!(
        r3_check("t", R3_TABLE_SIZE_LIMIT_BYTES + 1, &summary).unwrap(),
        R3Verdict::Flagged
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "R3-SIZE-FLAG table=t bytes=3221225473 limit=3221225472\n"
    );
    assert_eq!(
        r3_check("t", 1, &StepSummary::default()).unwrap(),
        R3Verdict::WithinLimit
    );
}

#[test]
fn the_limit_is_not_a_cli_option() {
    let error = cli::parse(&strings(&[
        "run",
        "--mode",
        "warm",
        "--warehouse",
        "/w",
        "--limit",
        "9",
    ]))
    .unwrap_err();
    assert!(error.contains("--limit"), "got: {error}");
}

#[test]
fn the_parser_reads_the_repeat_and_remote_setup_flags() {
    assert_eq!(
        cli::parse(&strings(&[
            "run",
            "--mode",
            "concurrent-cold",
            "--catalog",
            "s3tables",
            "--table",
            "ns.t",
            "--repeat",
            "3",
            "--files",
            "20",
            "--rows-per-file",
            "100",
        ]))
        .unwrap(),
        Command::Run(RunOptions {
            mode: Mode::ConcurrentCold,
            warehouse: None,
            catalog: CatalogChoice::S3Tables,
            table: Some("ns.t".to_string()),
            repeat: 3,
            files: Some(20),
            rows_per_file: Some(100),
            ..local_run(Mode::Warm, Path::new(""))
        })
    );
    assert_eq!(
        cli::parse(&strings(&[
            "setup",
            "--catalog",
            "glue",
            "--prop",
            "warehouse=s3://b/w",
            "--table",
            "ns.t",
            "--phase",
            "write",
            "--files",
            "20",
        ]))
        .unwrap(),
        Command::SetupRemote(RemoteSetupOptions {
            catalog: CatalogChoice::Glue,
            props: vec![("warehouse".to_string(), "s3://b/w".to_string())],
            table: "ns.t".to_string(),
            phase: Phase::Write,
            files: 20,
            rows_per_file: 50_000,
        })
    );
}

#[test]
fn the_parser_ignores_cargo_bench_and_reads_every_flag() {
    let parsed = cli::parse(&strings(&[
        "run",
        "--bench",
        "--mode",
        "cold",
        "--catalog",
        "glue",
        "--prop",
        "warehouse=s3://b/w",
        "--table",
        "perf.events",
        "--manifest",
        "/m.json",
        "--query",
        "Q3",
        "--out",
        "/o.json",
    ]))
    .unwrap();
    assert_eq!(
        parsed,
        Command::Run(RunOptions {
            mode: Mode::Cold,
            warehouse: None,
            out: Some(PathBuf::from("/o.json")),
            catalog: CatalogChoice::Glue,
            props: vec![("warehouse".to_string(), "s3://b/w".to_string())],
            table: Some("perf.events".to_string()),
            manifest: Some(PathBuf::from("/m.json")),
            query: Some("Q3".to_string()),
            repeat: 1,
            files: None,
            rows_per_file: None,
        })
    );
    assert_eq!(
        cli::parse(&strings(&["setup", "--warehouse", "/w"])).unwrap(),
        Command::Setup(SetupOptions {
            warehouse: PathBuf::from("/w"),
            files: 200,
            rows_per_file: 50_000,
        })
    );
    for bad in [
        vec!["setup"],
        vec!["setup", "--warehouse", "/w", "--files", "0"],
        vec!["run", "--warehouse", "/w"],
        vec!["run", "--mode", "hot", "--warehouse", "/w"],
        vec!["run", "--mode", "warm"],
        vec!["run", "--mode", "warm", "--catalog", "hive"],
        vec![
            "run",
            "--mode",
            "warm",
            "--catalog",
            "glue",
            "--prop",
            "novalue",
        ],
        vec![
            "run",
            "--mode",
            "warm",
            "--warehouse",
            "/w",
            "--repeat",
            "0",
        ],
        vec!["setup", "--catalog", "glue", "--table", "a.b"],
        vec!["setup", "--catalog", "glue", "--phase", "create"],
        vec![
            "setup",
            "--catalog",
            "glue",
            "--phase",
            "drop",
            "--table",
            "a.b",
        ],
        vec!["setup", "--warehouse", "/w", "--phase", "create"],
        vec![
            "setup",
            "--catalog",
            "s3tables",
            "--phase",
            "create",
            "--table",
            "a.b",
            "--warehouse",
            "/w",
        ],
        vec![],
    ] {
        assert!(cli::parse(&strings(&bad)).is_err(), "accepted {bad:?}");
    }
    assert_eq!(
        cli::main_with(&strings(&["bogus"]), &StepSummary::default()),
        ExitCode::from(EXIT_USAGE)
    );
}

fn tiny_bed(dir: &TempDir) -> PathBuf {
    let warehouse = dir.path().join("wh");
    let code = cli::main_with(
        &strings(&[
            "setup",
            "--warehouse",
            text(&warehouse),
            "--files",
            "3",
            "--rows-per-file",
            "400",
        ]),
        &StepSummary::from_env(),
    );
    assert_eq!(code, ExitCode::SUCCESS);
    warehouse
}

fn run_mode(dir: &TempDir, warehouse: &Path, mode: &str, repeat: &str) -> Value {
    let out = dir.path().join(format!("{mode}-{repeat}.json"));
    let code = cli::main_with(
        &strings(&[
            "run",
            "--mode",
            mode,
            "--warehouse",
            text(warehouse),
            "--repeat",
            repeat,
            "--out",
            text(&out),
        ]),
        &StepSummary::from_env(),
    );
    assert_eq!(code, ExitCode::SUCCESS, "mode {mode}");
    serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap()
}

fn query<'a>(document: &'a Value, name: &str) -> &'a Value {
    document["queries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|query| query["name"] == name)
        .unwrap()
}

fn requests(value: &Value) -> u64 {
    value["requests"].as_u64().unwrap()
}

#[test]
fn setup_writes_exactly_n_files_and_every_mode_runs_on_them() {
    let dir = TempDir::new().unwrap();
    let warehouse = tiny_bed(&dir);
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(bed::manifest_path(&warehouse)).unwrap())
            .unwrap();
    assert_eq!(manifest["files"], 3);
    assert_eq!(manifest["rows"], 1200);
    assert!(manifest["bytes"].as_u64().unwrap() > 0);
    let data_dir = warehouse.join("perf").join("events").join("data");
    assert_eq!(std::fs::read_dir(data_dir).unwrap().count(), 3);

    for mode in ["cold", "warm", "concurrent", "concurrent-cold"] {
        let document = run_mode(&dir, &warehouse, mode, "2");
        assert_eq!(document["mode"], mode);
        assert_eq!(document["repeat"], 2);
        let environment = &document["environment"];
        assert!(environment["fork_pin"].is_string());
        assert!(environment["rustc_version"].is_string());
        assert_eq!(environment["os_page_cache"], "not_dropped");
        let cold = mode == "cold" || mode == "concurrent-cold";
        assert_eq!(environment["cold_kind"].is_string(), cold, "mode {mode}");
        assert_eq!(
            environment["concurrent_kind"].is_string(),
            mode.starts_with("concurrent"),
            "mode {mode}"
        );
        assert!(document["run_io_total"]["bytes"].as_u64().unwrap() > 0);
        let queries = document["queries"].as_array().unwrap();
        let expected = if mode.starts_with("concurrent") { 4 } else { 7 };
        assert_eq!(queries.len(), expected, "mode {mode}");
        for entry in queries {
            assert_eq!(entry["samples_taken"], 2, "mode {mode}");
            assert_eq!(entry["samples"].as_array().unwrap().len(), 2);
            assert!(entry["median"]["total_ms"].is_number());
            assert!(entry["samples"][0]["execute_to_first_batch_ms"].is_number());
        }
        assert_eq!(query(&document, "Q3")["rows"], 12);
        if mode.starts_with("concurrent") {
            let group = &document["concurrent_group"];
            assert_eq!(group["samples_taken"], 2);
            let manifests = requests(&group["io"]["by_class"]["manifest"]);
            assert_eq!(manifests > 0, mode == "concurrent-cold", "mode {mode}");
            assert!(group["samples"][0]["rss_at_reset_kib"].is_number());
        } else {
            assert_eq!(query(&document, "Q4")["rows"], 1);
            assert_eq!(query(&document, "Q7")["rows"], 12);
            for entry in queries {
                assert_eq!(entry["io_identical_across_samples"], true, "mode {mode}");
                assert!(entry["samples"][1]["rss_at_reset_kib"].is_number());
            }
            let count = &query(&document, "Q1")["io"]["data_file_ranged"];
            assert_eq!(requests(&count["footer"]), 3, "mode {mode}");
            assert_eq!(requests(&count["page"]), 0, "mode {mode}");
            let full = &query(&document, "Q2")["io"]["data_file_ranged"];
            assert_eq!(requests(&full["footer"]), 3, "mode {mode}");
            assert!(requests(&full["page"]) > 0, "mode {mode}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn q3_never_reaches_the_scan_today_and_q7_does_with_the_same_rows() {
    let dir = TempDir::new().unwrap();
    let options = SetupOptions {
        warehouse: dir.path().join("wh"),
        files: 3,
        rows_per_file: 400,
    };
    bed::setup(&options, &StepSummary::default()).await.unwrap();
    let warehouse = std::fs::canonicalize(&options.warehouse).unwrap();
    let shape = bed::read_shape(&bed::manifest_path(&warehouse)).unwrap();
    let session = bed::spark_session().unwrap();
    bed::register_local_table(
        &session,
        &warehouse,
        shape.metadata_location.as_deref().unwrap(),
    )
    .await
    .unwrap();
    let specs = run::queries(&bed::local_table_name(), &shape);
    let spec = |name: &str| specs.iter().find(|spec| spec.name == name).unwrap();
    assert!(spec("Q3").sql.contains("CAST(1700003780 AS TIMESTAMP)"));
    assert_eq!(
        run::scan_predicate(&session, spec("Q3"))
            .await
            .unwrap()
            .as_deref(),
        Some("")
    );
    assert_eq!(
        run::scan_predicate(&session, spec("Q7"))
            .await
            .unwrap()
            .as_deref(),
        Some("(id >= 540) AND (id < 552)")
    );
    let mut answers = Vec::new();
    for name in ["Q3", "Q7"] {
        let batches = session
            .sql(&spec(name).sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut ids = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            ids.extend(column.values().iter().copied());
        }
        ids.sort_unstable();
        answers.push(ids);
    }
    assert_eq!(answers[0].len(), 12);
    assert_eq!(answers[0], answers[1]);
    assert_eq!(
        run::iceberg_scan_predicate("  IcebergTableScan projection:[id] predicate:[id = 3] N=1"),
        Some("id = 3".to_string())
    );
    assert_eq!(run::iceberg_scan_predicate("FilterExec: id@0 = 3"), None);
}

#[test]
fn the_counts_derive_the_local_manifests_query_constants() {
    let dir = TempDir::new().unwrap();
    let warehouse = std::fs::canonicalize(tiny_bed(&dir)).unwrap();
    let path = bed::manifest_path(&warehouse);
    let manifest: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let from_manifest = bed::read_shape(&path).unwrap();
    let derived = BedShape::from_counts(3, 400);
    assert_eq!(derived.rows, from_manifest.rows);
    assert_eq!(derived.files, from_manifest.files);
    assert_eq!(manifest["ts_base_seconds"], bed::TS_BASE_SECONDS);
    assert_eq!(manifest["ts_step_seconds"], bed::TS_STEP_SECONDS);
    assert_eq!(manifest["categories"], bed::CATEGORIES);
    let table = "bench.ns.events";
    assert_eq!(
        run::queries(table, &derived),
        run::queries(table, &from_manifest)
    );
    let defaults = BedShape::from_counts(bed::DEFAULT_FILES, bed::DEFAULT_ROWS_PER_FILE);
    assert_eq!(defaults.rows, 10_000_000);
}

struct StandIn {
    warehouse: PathBuf,
    metadata: String,
    opened: RefCell<Vec<ReparkSession>>,
}

impl StandIn {
    fn new(warehouse: &Path, metadata: &str) -> Self {
        Self {
            warehouse: warehouse.to_path_buf(),
            metadata: metadata.to_string(),
            opened: RefCell::new(Vec::new()),
        }
    }
}

impl SessionSource for StandIn {
    async fn open(&self) -> Result<ReparkSession, BoxError> {
        let session = bed::spark_session()?;
        bed::register_local_table(&session, &self.warehouse, &self.metadata).await?;
        self.opened.borrow_mut().push(session.clone());
        Ok(session)
    }
}

const MODES: [Mode; 4] = [
    Mode::Cold,
    Mode::Warm,
    Mode::Concurrent,
    Mode::ConcurrentCold,
];

const CATALOGS: [CatalogChoice; 3] = [
    CatalogChoice::Local,
    CatalogChoice::Glue,
    CatalogChoice::S3Tables,
];

fn gated_options(catalog: CatalogChoice, mode: Mode, warehouse: &Path) -> RunOptions {
    match catalog {
        CatalogChoice::Local => local_run(mode, warehouse),
        CatalogChoice::Glue | CatalogChoice::S3Tables => RunOptions {
            catalog,
            warehouse: None,
            table: Some(format!("{}.{}", bed::NAMESPACE, bed::TABLE)),
            files: Some(3),
            rows_per_file: Some(400),
            ..local_run(mode, warehouse)
        },
    }
}

async fn gated(
    options: &RunOptions,
    source: &StandIn,
    summary: &StepSummary,
    size: u64,
) -> Result<Outcome, BoxError> {
    let target = run::resolve_target(options)?;
    run::run_gated(options, &target, source, summary, Some(size)).await
}

fn assert_stopped_before_any_query(outcome: Outcome, source: &StandIn, case: &str) {
    let Outcome::SizeFlagged(io) = &outcome else {
        panic!("{case}: a size above the limit must flag, got {outcome:?}");
    };
    assert_eq!(
        io.by_class(IcebergFileClass::DataFile).requests,
        0,
        "{case}"
    );
    assert_eq!(
        io.by_class(IcebergFileClass::DeleteFile).requests,
        0,
        "{case}"
    );
    assert!(
        io.by_class(IcebergFileClass::Manifest).requests > 0,
        "{case}"
    );
    let opened = source.opened.borrow();
    assert_eq!(opened.len(), 1, "{case}: a session opened after the gate");
    assert_eq!(**io, opened[0].iceberg_io_stats(), "{case}");
    assert_eq!(
        cli::exit_code(Ok(outcome)),
        ExitCode::from(R3_EXIT_CODE),
        "{case}"
    );
}

#[test]
fn a_fake_size_above_the_limit_stops_every_mode_and_catalog_before_any_query() {
    let dir = TempDir::new().unwrap();
    let warehouse = std::fs::canonicalize(tiny_bed(&dir)).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(fake_size_stops_every_mode_and_catalog(&dir, &warehouse));
}

async fn fake_size_stops_every_mode_and_catalog(dir: &TempDir, warehouse: &Path) {
    let shape = bed::read_shape(&bed::manifest_path(warehouse)).unwrap();
    let metadata = shape.metadata_location.unwrap();
    for catalog in CATALOGS {
        let options = RunOptions {
            query: Some("Q2".to_string()),
            ..gated_options(catalog, Mode::Warm, warehouse)
        };
        let source = StandIn::new(warehouse, &metadata);
        let within = gated(
            &options,
            &source,
            &StepSummary::default(),
            R3_TABLE_SIZE_LIMIT_BYTES,
        )
        .await
        .unwrap();
        assert!(matches!(within, Outcome::Done), "{catalog:?}: {within:?}");
    }

    std::fs::remove_dir_all(warehouse.join(bed::NAMESPACE).join(bed::TABLE).join("data")).unwrap();
    for catalog in CATALOGS {
        let options = RunOptions {
            query: Some("Q2".to_string()),
            ..gated_options(catalog, Mode::Warm, warehouse)
        };
        let source = StandIn::new(warehouse, &metadata);
        let probe = gated(
            &options,
            &source,
            &StepSummary::default(),
            R3_TABLE_SIZE_LIMIT_BYTES,
        )
        .await;
        assert!(
            probe.is_err(),
            "{catalog:?}: a query ran without its data files: {probe:?}"
        );
    }

    for catalog in CATALOGS {
        for mode in MODES {
            let case = format!("{catalog:?} {mode:?}");
            let summary = StepSummary {
                path: Some(
                    dir.path()
                        .join(format!("{}-{}.md", catalog.name(), mode.name())),
                ),
            };
            let source = StandIn::new(warehouse, &metadata);
            let outcome = gated(
                &gated_options(catalog, mode, warehouse),
                &source,
                &summary,
                R3_TABLE_SIZE_LIMIT_BYTES + 1,
            )
            .await
            .unwrap_or_else(|error| panic!("{case}: {error}"));
            assert_stopped_before_any_query(outcome, &source, &case);
            assert_eq!(
                std::fs::read_to_string(summary.path.unwrap()).unwrap(),
                "R3-SIZE-FLAG table=bench.perf.events bytes=3221225473 limit=3221225472\n",
                "{case}"
            );
        }
    }

    for mode in MODES {
        let outcome = run::run(
            &local_run(mode, warehouse),
            &StepSummary::default(),
            Some(R3_TABLE_SIZE_LIMIT_BYTES + 1),
        )
        .await
        .unwrap_or_else(|error| panic!("{mode:?}: {error}"));
        let Outcome::SizeFlagged(io) = &outcome else {
            panic!("{mode:?}: got {outcome:?}");
        };
        assert_eq!(
            io.by_class(IcebergFileClass::DataFile).requests,
            0,
            "{mode:?}"
        );
        assert_eq!(cli::exit_code(Ok(outcome)), ExitCode::from(R3_EXIT_CODE));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_files_table_counts_delete_files_in_the_footprint() {
    let dir = TempDir::new().unwrap();
    let root = text(dir.path());
    let session = bed::spark_session().unwrap();
    session
        .register_memory_catalog("bench", root)
        .await
        .unwrap();
    session
        .create_namespace(
            "bench",
            "perf",
            std::collections::HashMap::from([("location".to_string(), format!("{root}/perf"))]),
        )
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE bench.perf.t (id BIGINT, v STRING) USING iceberg TBLPROPERTIES \
         ('format-version'='2', 'write.delete.mode'='merge-on-read')",
        "INSERT INTO bench.perf.t SELECT id, CAST(id AS STRING) AS v FROM range(0, 100)",
        "DELETE FROM bench.perf.t WHERE id = 1",
    ] {
        session.sql(sql).await.unwrap().collect().await.unwrap();
    }
    let footprint = table_footprint(&session, "bench.perf.t").await.unwrap();
    assert_eq!(footprint.files, 2);
    assert_eq!(footprint.delete_files, 1);
    let mut on_disk = 0;
    for entry in std::fs::read_dir(dir.path().join("perf/t/data")).unwrap() {
        on_disk += entry.unwrap().metadata().unwrap().len();
    }
    assert_eq!(footprint.bytes, on_disk);
    session.reset_iceberg_io_stats();
    table_footprint(&session, "bench.perf.t").await.unwrap();
    let io = session.iceberg_io_stats();
    assert_eq!(io.by_class(IcebergFileClass::DataFile).requests, 0);
    assert_eq!(io.by_class(IcebergFileClass::DeleteFile).requests, 0);
    assert_eq!(io.by_op(IcebergIoOp::Write).requests, 0);
}

#[tokio::test]
async fn aws_catalogs_fail_loud_on_missing_props_without_a_call() {
    for (catalog, key) in [
        (CatalogChoice::Glue, "warehouse"),
        (CatalogChoice::S3Tables, "table_bucket_arn"),
    ] {
        let options = RunOptions {
            catalog,
            warehouse: None,
            table: Some("perf.events".to_string()),
            ..local_run(Mode::Warm, Path::new(""))
        };
        let error = run::run(&options, &StepSummary::default(), None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "{catalog:?}: {error}");
        for (broken, needle) in [
            (
                RunOptions {
                    table: None,
                    ..options.clone()
                },
                "--table",
            ),
            (
                RunOptions {
                    table: Some("events".to_string()),
                    ..options.clone()
                },
                "<namespace>.<table>",
            ),
            (
                RunOptions {
                    manifest: Some(PathBuf::from("/bed.json")),
                    ..options.clone()
                },
                "--manifest",
            ),
        ] {
            let error = run::run(&broken, &StepSummary::default(), None)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(needle), "{catalog:?}: {error}");
        }
        let setup = RemoteSetupOptions {
            catalog,
            props: Vec::new(),
            table: "perf.events".to_string(),
            phase: Phase::Create,
            files: 3,
            rows_per_file: 10,
        };
        let error = remote::setup_remote(&setup, &StepSummary::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "{catalog:?}: {error}");
        let error = remote::setup_remote(
            &RemoteSetupOptions {
                table: "events".to_string(),
                ..setup
            },
            &StepSummary::default(),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("<namespace>.<table>"),
            "{catalog:?}: {error}"
        );
    }
    assert_eq!(
        remote::namespace_rule(
            CatalogChoice::Glue,
            &[("warehouse".to_string(), "s3://w/".to_string())],
            "scratch"
        )
        .unwrap(),
        NamespaceRule::Located("s3://w/scratch".to_string())
    );
    assert_eq!(
        remote::namespace_rule(CatalogChoice::S3Tables, &[], "scratch").unwrap(),
        NamespaceRule::Unlocated
    );
}

async fn stand_in_session(root: &str) -> repark_core::ReparkSession {
    let session = bed::spark_session().unwrap();
    session
        .register_memory_catalog(bed::CATALOG, root)
        .await
        .unwrap();
    session
}

fn writes(session: &repark_core::ReparkSession) -> u64 {
    session
        .iceberg_io_stats()
        .by_op(IcebergIoOp::Write)
        .requests
}

async fn sql(session: &repark_core::ReparkSession, statement: &str) {
    session
        .sql(statement)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_setup_phases_write_only_into_an_empty_table_and_skip_only_an_exact_one() {
    let dir = TempDir::new().unwrap();
    let root = text(dir.path());
    let session = stand_in_session(root).await;
    let located = NamespaceRule::Located(format!("{root}/scratch"));
    let created = remote::create_phase(&session, "scratch", "t", &located)
        .await
        .unwrap();
    assert_eq!(created, CreateOutcome::Created);
    let again = remote::create_phase(&session, "scratch", "t", &located)
        .await
        .unwrap();
    assert_eq!(again, CreateOutcome::Existed);

    let table = "bench.scratch.t";
    assert_eq!(
        remote::write_phase(&session, table, 2, 50).await.unwrap(),
        WriteOutcome::Wrote
    );
    let footprint = table_footprint(&session, table).await.unwrap();
    assert_eq!((footprint.data_files(), footprint.data_rows), (2, 100));

    session.reset_iceberg_io_stats();
    assert_eq!(
        remote::write_phase(&session, table, 2, 50).await.unwrap(),
        WriteOutcome::Skipped
    );
    for (files, rows) in [(3, 50), (1, 50), (2, 49)] {
        let error = remote::write_phase(&session, table, files, rows)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("refusing to write"), "{error}");
    }
    assert_eq!(writes(&session), 0);
    assert_eq!(table_footprint(&session, table).await.unwrap(), footprint);

    sql(
        &session,
        "CREATE TABLE bench.scratch.m (id BIGINT, ts TIMESTAMP, category STRING, value DOUBLE, \
         payload STRING) USING iceberg TBLPROPERTIES ('format-version'='2', \
         'write.delete.mode'='merge-on-read')",
    )
    .await;
    bed::write_files(&session, "bench.scratch.m", 2, 50)
        .await
        .unwrap();
    sql(&session, "DELETE FROM bench.scratch.m WHERE id = 1").await;
    session.reset_iceberg_io_stats();
    let error = remote::write_phase(&session, "bench.scratch.m", 2, 50)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("1 delete files"), "{error}");
    assert_eq!(writes(&session), 0);

    sql(
        &session,
        "CREATE TABLE bench.scratch.w (id BIGINT) USING iceberg",
    )
    .await;
    let error = remote::create_phase(&session, "scratch", "w", &located)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("not the bed schema"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_create_phase_refuses_a_glue_namespace_at_another_location() {
    let dir = TempDir::new().unwrap();
    let root = text(dir.path());
    let session = stand_in_session(root).await;
    session
        .create_namespace(
            bed::CATALOG,
            "moved",
            std::collections::HashMap::from([(
                "location".to_string(),
                format!("{root}/elsewhere"),
            )]),
        )
        .await
        .unwrap();
    session
        .create_namespace(bed::CATALOG, "bare", std::collections::HashMap::new())
        .await
        .unwrap();
    for namespace in ["moved", "bare"] {
        let rule = NamespaceRule::Located(format!("{root}/{namespace}"));
        let error = remote::create_phase(&session, namespace, "t", &rule)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("location"), "{namespace}: {error}");
    }
    let created = remote::create_phase(&session, "plain", "t", &NamespaceRule::Unlocated)
        .await
        .unwrap();
    assert_eq!(created, CreateOutcome::Created);
}

#[tokio::test(flavor = "multi_thread")]
async fn both_setup_phases_end_with_the_r3_check() {
    let dir = TempDir::new().unwrap();
    let root = text(dir.path());
    let session = stand_in_session(root).await;
    let summary = summary_in(dir.path());
    let mut request = PhaseRequest {
        namespace: "scratch".to_string(),
        name: "t".to_string(),
        phase: Phase::Create,
        rule: NamespaceRule::Located(format!("{root}/scratch")),
        files: 2,
        rows_per_file: 20,
    };
    let within = remote::run_phase(&session, &request, &summary, None)
        .await
        .unwrap();
    assert!(matches!(within, Outcome::Done), "got {within:?}");
    for phase in [Phase::Create, Phase::Write] {
        request.phase = phase;
        let flagged = remote::run_phase(
            &session,
            &request,
            &summary,
            Some(R3_TABLE_SIZE_LIMIT_BYTES + 1),
        )
        .await
        .unwrap();
        assert!(
            matches!(flagged, Outcome::SizeFlagged(_)),
            "{phase:?}: {flagged:?}"
        );
    }
    let lines = std::fs::read_to_string(summary.path.unwrap()).unwrap();
    assert_eq!(lines.lines().count(), 2);
    assert!(lines.starts_with("R3-SIZE-FLAG table=bench.scratch.t "));
}
