mod bed;
mod cli;
mod r3;
mod report;
mod run;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use repark_iceberg::catalog::{IcebergFileClass, IcebergIoOp};
use tempfile::TempDir;

use crate::bed::SetupOptions;
use crate::cli::{Command, EXIT_FAILURE, EXIT_USAGE, Outcome};
use crate::r3::{
    R3_EXIT_CODE, R3_TABLE_SIZE_LIMIT_BYTES, R3Verdict, StepSummary, r3_check, r3_verdict,
    table_footprint,
};
use crate::run::{CatalogChoice, Mode, RunOptions};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_string()).collect()
}

fn text(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn local_run(mode: Mode, warehouse: &Path) -> RunOptions {
    RunOptions {
        mode,
        warehouse: Some(warehouse.to_path_buf()),
        out: None,
        catalog: CatalogChoice::Local,
        props: Vec::new(),
        table: None,
        manifest: None,
        query: None,
    }
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

#[test]
fn setup_writes_exactly_n_files_and_every_mode_runs_on_them() {
    let dir = TempDir::new().unwrap();
    let warehouse = tiny_bed(&dir);
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(bed::manifest_path(&warehouse)).unwrap())
            .unwrap();
    assert_eq!(manifest["files"], 3);
    assert_eq!(manifest["rows"], 1200);
    assert!(manifest["bytes"].as_u64().unwrap() > 0);
    let data_dir = warehouse.join("perf").join("events").join("data");
    assert_eq!(std::fs::read_dir(data_dir).unwrap().count(), 3);

    for mode in ["cold", "warm", "concurrent"] {
        let out = dir.path().join(format!("{mode}.json"));
        let code = cli::main_with(
            &strings(&[
                "run",
                "--mode",
                mode,
                "--warehouse",
                text(&warehouse),
                "--out",
                text(&out),
            ]),
            &StepSummary::from_env(),
        );
        assert_eq!(code, ExitCode::SUCCESS, "mode {mode}");
        let document: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
        assert_eq!(document["mode"], mode);
        assert!(document["environment"]["fork_pin"].is_string());
        let queries = document["queries"].as_array().unwrap();
        let expected = if mode == "concurrent" { 4 } else { 6 };
        assert_eq!(queries.len(), expected, "mode {mode}");
        if let Some(point) = queries.iter().find(|query| query["name"] == "Q4") {
            assert_eq!(point["rows"], 1);
        }
        let window = queries.iter().find(|query| query["name"] == "Q3").unwrap();
        assert_eq!(window["rows"], 12);
        if mode == "concurrent" {
            assert!(
                document["concurrent_group"]["io"]["total"]["requests"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
        } else {
            let full = queries.iter().find(|query| query["name"] == "Q2").unwrap();
            assert!(
                full["io"]["by_op"]["ranged_read"]["by_class"]["data_file"]["requests"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
        }
    }
}

#[test]
fn a_fake_size_above_the_limit_stops_the_run_before_any_data_file_read() {
    let dir = TempDir::new().unwrap();
    let warehouse = tiny_bed(&dir);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(fake_size_stops_the_run(&dir, &warehouse));
}

async fn fake_size_stops_the_run(dir: &TempDir, warehouse: &Path) {
    let summary = summary_in(dir.path());
    let outcome = run::run(
        &local_run(Mode::Warm, warehouse),
        &summary,
        Some(R3_TABLE_SIZE_LIMIT_BYTES + 1),
    )
    .await
    .unwrap();
    let Outcome::SizeFlagged(io) = outcome else {
        panic!("a size above the limit must flag, got {outcome:?}");
    };
    assert_eq!(
        io.by_class(IcebergFileClass::DataFile).requests,
        0,
        "io {io:?}"
    );
    assert_eq!(
        io.by_class(IcebergFileClass::DeleteFile).requests,
        0,
        "io {io:?}"
    );
    assert!(
        io.by_class(IcebergFileClass::Manifest).requests > 0,
        "io {io:?}"
    );
    assert!(
        std::fs::read_to_string(summary.path.unwrap())
            .unwrap()
            .starts_with("R3-SIZE-FLAG table=bench.perf.events bytes=3221225473 limit=3221225472")
    );

    let within = run::run(
        &RunOptions {
            query: Some("Q2".to_string()),
            ..local_run(Mode::Warm, warehouse)
        },
        &StepSummary::default(),
        Some(R3_TABLE_SIZE_LIMIT_BYTES),
    )
    .await
    .unwrap();
    assert!(matches!(within, Outcome::Done), "got {within:?}");
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
    let dir = TempDir::new().unwrap();
    let manifest = dir.path().join("bed.json");
    std::fs::write(
        &manifest,
        r#"{"rows": 100, "files": 1, "metadata_location": null}"#,
    )
    .unwrap();
    for (catalog, key) in [
        (CatalogChoice::Glue, "warehouse"),
        (CatalogChoice::S3Tables, "table_bucket_arn"),
    ] {
        let options = RunOptions {
            mode: Mode::Warm,
            warehouse: None,
            out: None,
            catalog,
            props: Vec::new(),
            table: Some("perf.events".to_string()),
            manifest: Some(manifest.clone()),
            query: None,
        };
        let error = run::run(&options, &StepSummary::default(), None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "{catalog:?}: {error}");
        let error = run::run(
            &RunOptions {
                table: None,
                ..options.clone()
            },
            &StepSummary::default(),
            None,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("--table"), "{catalog:?}: {error}");
        let error = run::run(
            &RunOptions {
                manifest: None,
                ..options
            },
            &StepSummary::default(),
            None,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("--manifest"), "{catalog:?}: {error}");
    }
}
