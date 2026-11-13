use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

use iceberg::TableCreation;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use repark_common::ErrorClass;

use super::*;
use crate::writer_layout::{
    TableWriteRequest, WriterAction, WriterLayout, WriterRefusal, save_target_names_table,
};

const CELLS: &str = "../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl";
const GOLDENS: &str = "../../docs/corpus/hadoop-discovery-repark-goldens-2026-11-13.jsonl";
const CATALOG_STYLE: &str = "00009-2f6c1c3e-9a7b-4d2e-8f10-5b3a7c9d0e11.metadata.json";
const ROWS_IN_FILE: usize = 3904;
const PATH_HEADS: [&str; 2] = ["path_select", "path_insert"];
const PATH_ROWS: usize = 3820;
const SPARK_ROWS_PINNED: usize = 959;
const SPARK_BODY_PINNED: usize = 348;
const GOLDENS_BY_CARD: [(&str, usize); 4] = [
    ("C-005 checksum control", 5),
    ("D4 name lies about the payload", 472),
    ("D5 HADOOP-HINT-ZERO-TEXT-1", 126),
    ("D7 ICE-WRITE-DF-SAVE-1", 1910),
];

#[derive(Clone)]
struct Cell {
    head: String,
    cat: String,
    fixture: String,
    writer: String,
    names: String,
    cleanup: String,
    hint: String,
    crc_kept: bool,
    spark: String,
    repark_changed: Vec<String>,
}

#[derive(Clone)]
struct Golden {
    card: String,
    outcome: String,
}

type Key = (String, String, String);

fn json_lines(relative: &str) -> Vec<serde_json::Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} must exist: {error}", path.display()));
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json line"))
        .collect()
}

fn field(row: &serde_json::Value, key: &str) -> String {
    row[key]
        .as_str()
        .unwrap_or_else(|| panic!("row has no string {key}: {row}"))
        .to_string()
}

fn all_cells() -> Vec<Cell> {
    let rows = json_lines(CELLS);
    assert_eq!(rows.len(), ROWS_IN_FILE);
    rows.iter()
        .map(|row| {
            let fixture = field(row, "fixture");
            let parts: Vec<String> = fixture.split('/').map(str::to_string).collect();
            assert!(matches!(parts.len(), 4 | 5), "{fixture}");
            let crc_kept = parts.get(4).is_some_and(|tail| tail == "crc_kept");
            assert_eq!(crc_kept, parts.len() == 5, "{fixture}");
            Cell {
                head: field(row, "head"),
                cat: field(row, "cat"),
                writer: parts[0].clone(),
                names: parts[1].clone(),
                cleanup: parts[2].clone(),
                hint: parts[3]
                    .strip_prefix("hint_")
                    .unwrap_or_else(|| panic!("hint part: {fixture}"))
                    .to_string(),
                crc_kept,
                spark: field(row, "spark"),
                repark_changed: row["repark_changed"]
                    .as_array()
                    .expect("repark_changed")
                    .iter()
                    .map(|item| item.as_str().expect("change").to_string())
                    .collect(),
                fixture,
            }
        })
        .collect()
}

fn goldens() -> BTreeMap<Key, Golden> {
    let mut table = BTreeMap::new();
    for row in json_lines(GOLDENS) {
        let key = (
            field(&row, "head"),
            field(&row, "cat"),
            field(&row, "fixture"),
        );
        let golden = Golden {
            card: field(&row, "card"),
            outcome: field(&row, "outcome"),
        };
        assert!(
            table.insert(key, golden).is_none(),
            "duplicate golden {row}"
        );
    }
    table
}

fn key_of(cell: &Cell) -> Key {
    (cell.head.clone(), cell.cat.clone(), cell.fixture.clone())
}

async fn base_history() -> (tempfile::TempDir, crate::ReparkSession, Vec<Vec<u8>>) {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let root = warehouse.path().to_string_lossy().to_string();
    let session = crate::ReparkSession::new().expect("ReparkSession");
    session
        .register_memory_catalog("ice", &root)
        .await
        .expect("catalog");
    let handle = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("handle");
    let namespace = NamespaceIdent::new("db".to_string());
    handle
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("schema");
    let table_root = format!("{root}/db/t");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .location(table_root.clone())
        .schema(schema)
        .properties(HashMap::new())
        .build();
    handle
        .create_table(&namespace, creation)
        .await
        .expect("table");
    session
        .refresh_catalog_provider("ice")
        .await
        .expect("refresh");
    for statement in [
        "INSERT INTO ice.db.t VALUES (1), (2), (3)",
        "DELETE FROM ice.db.t WHERE id = 1",
        "INSERT INTO ice.db.t VALUES (4)",
        "DELETE FROM ice.db.t WHERE id = 2",
    ] {
        session
            .sql(statement)
            .await
            .expect("statement")
            .collect()
            .await
            .expect("collect");
    }
    let metadata = Path::new(&table_root).join("metadata");
    let mut names: Vec<String> = std::fs::read_dir(&metadata)
        .expect("read_dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .filter(|name| name.ends_with(".metadata.json"))
        .collect();
    names.sort();
    assert_eq!(names.len(), 5, "{names:?}");
    let payloads = names
        .iter()
        .map(|name| std::fs::read(metadata.join(name)).expect("read"))
        .collect();
    (warehouse, session, payloads)
}

#[derive(Clone, Copy, PartialEq)]
enum Form {
    Plain,
    GzInfix,
    GzLegacy,
    GzInfixPlainBytes,
    PlainGzBytes,
}

fn writer_versions(writer: &str, base: &[Vec<u8>]) -> Vec<(i32, Vec<u8>, Form)> {
    let at = |index: usize| base[index].clone();
    match writer {
        "sp_gzip" => (0..5)
            .map(|index| (version_of(index), at(index), Form::GzInfix))
            .collect(),
        "sp_mixed" => vec![
            (1, at(0), Form::Plain),
            (2, at(1), Form::Plain),
            (3, at(1), Form::GzInfix),
            (4, at(2), Form::GzInfix),
            (5, at(3), Form::GzInfix),
            (6, at(4), Form::GzInfix),
        ],
        "sp_cleanup" => vec![(4, at(3), Form::Plain), (5, at(4), Form::Plain)],
        "sp_plain" | "rp_plain" | "rp_gzip" | "rp_cleanup" => (0..5)
            .map(|index| (version_of(index), at(index), Form::Plain))
            .collect(),
        other => panic!("unknown writer {other}"),
    }
}

fn version_of(index: usize) -> i32 {
    i32::try_from(index + 1).expect("small index")
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).expect("gzip");
    encoder.finish().expect("gzip")
}

fn file_for(version: i32, payload: &[u8], form: Form) -> (String, Vec<u8>) {
    match form {
        Form::Plain => (format!("v{version}.metadata.json"), payload.to_vec()),
        Form::GzInfix => (format!("v{version}.gz.metadata.json"), gzip(payload)),
        Form::GzLegacy => (format!("v{version}.metadata.json.gz"), gzip(payload)),
        Form::GzInfixPlainBytes => (format!("v{version}.gz.metadata.json"), payload.to_vec()),
        Form::PlainGzBytes => (format!("v{version}.metadata.json"), gzip(payload)),
    }
}

fn hint_text(hint: &str, newest: i32) -> Option<String> {
    Some(match hint {
        "current" | "unreadable" => newest.to_string(),
        "stale_prev" => (newest - 1).to_string(),
        "stale_first" => "1".to_string(),
        "past" => (newest + 5).to_string(),
        "empty" => String::new(),
        "garbage" => "abc".to_string(),
        "lead_space" => format!(" {newest}"),
        "trail_nl" => format!("{newest}\n"),
        "crlf" => format!("{newest}\r\n"),
        "negative" => "-1".to_string(),
        "zero" => "0".to_string(),
        "overflow" => "2147483648".to_string(),
        "missing" | "dir" => return None,
        other => panic!("unknown hint {other}"),
    })
}

fn rename_form(names: &str) -> Option<Form> {
    match names {
        "gz_new" => Some(Form::GzInfix),
        "gz_old" => Some(Form::GzLegacy),
        "gzname_plainbytes" => Some(Form::GzInfixPlainBytes),
        "plainname_gzbytes" => Some(Form::PlainGzBytes),
        "as_written" | "catstyle" => None,
        other => panic!("unknown name form {other}"),
    }
}

fn build_fixture(cell: &Cell, base: &[Vec<u8>], root: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let metadata = root.join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    let mut versions = writer_versions(&cell.writer, base);
    let newest = versions
        .iter()
        .map(|(version, _, _)| *version)
        .max()
        .expect("versions");
    if let Some(form) = rename_form(&cell.names) {
        for entry in &mut versions {
            entry.2 = form;
        }
    }
    if cell.names == "catstyle" {
        let source = versions
            .iter()
            .find(|(version, _, _)| *version == 2)
            .or_else(|| versions.iter().min_by_key(|(version, _, _)| *version))
            .expect("catalog-style source");
        std::fs::write(metadata.join(CATALOG_STYLE), &source.1).expect("write");
    }
    versions.retain(|(version, _, _)| match cell.cleanup.as_str() {
        "gap" => *version != newest - 1,
        "trim" => *version >= newest - 1,
        "none" => true,
        other => panic!("unknown cleanup {other}"),
    });
    for (version, payload, form) in &versions {
        let (name, bytes) = file_for(*version, payload, *form);
        if cell.crc_kept {
            std::fs::write(metadata.join(format!(".{name}.crc")), b"crc\0").expect("crc");
        }
        std::fs::write(metadata.join(name), bytes).expect("write");
    }
    let hint = metadata.join("version-hint.text");
    if cell.hint == "dir" {
        std::fs::create_dir(&hint).expect("mkdir");
    } else if let Some(text) = hint_text(&cell.hint, newest) {
        std::fs::write(&hint, text).expect("hint");
        if cell.crc_kept {
            std::fs::write(metadata.join(".version-hint.text.crc"), b"crc\0stale").expect("crc");
        }
        if cell.hint == "unreadable" {
            std::fs::set_permissions(&hint, std::fs::Permissions::from_mode(0o000)).expect("chmod");
        }
    }
}

fn class_name(error: &Error) -> &'static str {
    match error.exception_class() {
        ErrorClass::Parse => "ParseException",
        ErrorClass::Analysis => "AnalysisException",
        ErrorClass::Arithmetic => "ArithmeticException",
        ErrorClass::Unsupported => "UnsupportedOperationException",
        ErrorClass::IllegalArgument => "IllegalArgumentException",
        ErrorClass::NumberFormat => "NumberFormatException",
        ErrorClass::CommitStateUnknown => "CommitStateUnknownException",
        ErrorClass::Base => "PySparkException",
    }
}

fn refused(error: &Error, root: &str) -> String {
    let parent = Path::new(root)
        .parent()
        .expect("fixture parent")
        .to_string_lossy()
        .to_string();
    format!(
        "ERR {}: {}",
        class_name(error),
        error
            .to_string()
            .replace(root, "<root>")
            .replace(&parent, "<parent>")
    )
}

async fn path_select(session: &crate::ReparkSession, root: &str) -> String {
    let answer: Result<Vec<i64>> = async {
        let batches = session
            .read_iceberg_path(root)
            .await?
            .collect()
            .await
            .map_err(engine_err)?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .expect("Int64 id")
                    .values()
                    .to_vec()
            })
            .collect())
    }
    .await;
    match answer {
        Ok(mut ids) => {
            ids.sort_unstable();
            format!("rows {ids:?}")
        }
        Err(error) => refused(&error, root),
    }
}

async fn path_insert(session: &crate::ReparkSession, root: &str) -> String {
    if save_target_names_table(root, true) {
        return "TARGET NAMES A TABLE".to_string();
    }
    let layout = WriterLayout {
        partition_columns: Vec::new(),
        num_buckets: None,
        bucket_columns: Vec::new(),
        sort_columns: Vec::new(),
    };
    let frame_columns = vec!["id".to_string()];
    let request = TableWriteRequest {
        action: WriterAction::Save,
        target: root,
        qualified: None,
        mode: "append",
        explicit_format: true,
        layout: &layout,
        frame_columns: &frame_columns,
        case_sensitive: false,
    };
    match session.plan_table_write(&request).await {
        Ok(statement) => format!("PLANNED {}", statement.as_str()),
        Err(WriterRefusal::Spark(error)) => refused(&error, root),
        Err(WriterRefusal::MissingBucketColumn(column)) => format!("ERR bucket column {column}"),
    }
}

fn tree_state(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut state = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("read_dir") {
            let path = entry.expect("entry").path();
            let name = path
                .strip_prefix(root)
                .expect("prefix")
                .display()
                .to_string();
            if path.is_dir() {
                state.insert(format!("{name}/"), Vec::new());
                pending.push(path);
            } else {
                state.insert(name, std::fs::read(&path).unwrap_or_default());
            }
        }
    }
    state
}

fn spark_expected(cell: &Cell) -> Option<String> {
    if let Some(list) = cell.spark.strip_prefix("rows ") {
        let mut rows: Vec<i64> = serde_json::from_str(list).expect("rows list");
        rows.sort_unstable();
        return Some(format!("rows {rows:?}"));
    }
    let (_, tail) = cell.spark.split_once("Metadata file for version ")?;
    let version = tail
        .split(' ')
        .next()
        .filter(|version| version.parse::<i64>().is_ok())
        .unwrap_or_else(|| panic!("{}: unparsed version in {}", cell.fixture, cell.spark));
    Some(format!(
        "ERR AnalysisException: Metadata file for version {version} is missing under <root>/metadata"
    ))
}

fn expected(cell: &Cell, golden: Option<&Golden>) -> std::result::Result<String, String> {
    let spark = if cell.head == "path_select" {
        spark_expected(cell)
    } else {
        None
    };
    match (golden, spark) {
        (Some(golden), Some(spark)) if golden.outcome == spark => Err(format!(
            "{}: golden {} restates the Spark outcome",
            cell.fixture, golden.card
        )),
        (Some(golden), _) => Ok(golden.outcome.clone()),
        (None, Some(spark)) => Ok(spark),
        (None, None) => Err(format!(
            "{} {}: no Spark outcome to compare and no golden",
            cell.head, cell.fixture
        )),
    }
}

struct Replay {
    observed: String,
    state_changed: bool,
}

async fn replay(
    cell: &Cell,
    session: &crate::ReparkSession,
    base: &[Vec<u8>],
    directory: &Path,
) -> Replay {
    let root = &directory.join("t");
    build_fixture(cell, base, root);
    let before = tree_state(root);
    let text = root.to_string_lossy().to_string();
    let observed = match cell.head.as_str() {
        "path_select" => path_select(session, &text).await,
        "path_insert" => path_insert(session, &text).await,
        other => panic!("not a path head: {other}"),
    };
    Replay {
        observed,
        state_changed: tree_state(root) != before,
    }
}

fn verdict(cell: &Cell, golden: Option<&Golden>, replay: &Replay) -> Option<String> {
    if replay.state_changed || !cell.repark_changed.is_empty() {
        return Some(format!(
            "{} {}: state changed (replay {}, recorded {:?})",
            cell.head, cell.fixture, replay.state_changed, cell.repark_changed
        ));
    }
    match expected(cell, golden) {
        Ok(outcome) if outcome == replay.observed => None,
        Ok(outcome) => Some(format!(
            "{} {} {}: expected {outcome} observed {}",
            cell.head, cell.cat, cell.fixture, replay.observed
        )),
        Err(problem) => Some(format!("{problem}; observed {}", replay.observed)),
    }
}

fn record_observed(lines: &[String]) {
    if let Ok(path) = std::env::var("REPARK_HADOOP_CELLS_OBSERVED") {
        std::fs::write(path, lines.join("\n") + "\n").expect("observed");
    }
}

#[tokio::test]
async fn path_read_cells_match_the_spark_column() {
    let (_warehouse, session, base) = base_history().await;
    let fixtures = tempfile::tempdir().expect("tempdir");
    let goldens = goldens();
    let mut used = BTreeSet::new();
    let (mut spark_rows, mut spark_body) = (0, 0);
    let mut by_card: BTreeMap<String, usize> = BTreeMap::new();
    let mut mismatches = Vec::new();
    let mut observed = Vec::new();
    let mut cells = 0;
    let started = std::time::Instant::now();
    for (index, cell) in all_cells().iter().enumerate() {
        if !PATH_HEADS.contains(&cell.head.as_str()) {
            continue;
        }
        cells += 1;
        let key = key_of(cell);
        let golden = goldens.get(&key);
        let replayed = replay(
            cell,
            &session,
            &base,
            &fixtures.path().join(format!("f{index:04}")),
        )
        .await;
        observed.push(
            serde_json::json!({"head": cell.head, "cat": cell.cat, "fixture": cell.fixture,
                "observed": replayed.observed, "spark": cell.spark})
            .to_string(),
        );
        mismatches.extend(verdict(cell, golden, &replayed));
        match golden {
            Some(golden) => {
                used.insert(key);
                *by_card.entry(golden.card.clone()).or_default() += 1;
            }
            None if cell.spark.starts_with("rows ") => spark_rows += 1,
            None => spark_body += 1,
        }
    }
    record_observed(&observed);
    eprintln!("path rows replayed in {:?}", started.elapsed());
    assert!(
        mismatches.is_empty(),
        "{} mismatches: {:#?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(40)]
    );
    let unused: Vec<&Key> = goldens
        .keys()
        .filter(|key| PATH_HEADS.contains(&key.0.as_str()) && !used.contains(*key))
        .collect();
    assert!(unused.is_empty(), "goldens with no row: {unused:?}");
    let expected_cards: BTreeMap<String, usize> = GOLDENS_BY_CARD
        .iter()
        .map(|(card, count)| ((*card).to_string(), *count))
        .collect();
    assert_eq!(by_card, expected_cards);
    assert_eq!(
        (cells, spark_rows, spark_body),
        (PATH_ROWS, SPARK_ROWS_PINNED, SPARK_BODY_PINNED)
    );
}

fn first_cell(head: &str, fixture: &str) -> Cell {
    all_cells()
        .into_iter()
        .find(|cell| cell.head == head && cell.fixture == fixture)
        .unwrap_or_else(|| panic!("{head} {fixture}"))
}

#[tokio::test]
async fn the_checker_rejects_a_wrong_value_a_wrong_text_and_a_changed_class() {
    let (_warehouse, session, base) = base_history().await;
    let fixtures = tempfile::tempdir().expect("tempdir");
    let goldens = goldens();
    let mut rows_cell = first_cell("path_select", "sp_plain/as_written/none/hint_current");
    let replayed = replay(&rows_cell, &session, &base, &fixtures.path().join("rows")).await;
    assert_eq!(verdict(&rows_cell, None, &replayed), None);
    rows_cell.spark = "rows [3, 5]".to_string();
    assert!(
        verdict(&rows_cell, None, &replayed).is_some(),
        "wrong value"
    );

    let mut body_cell = first_cell("path_select", "sp_plain/as_written/none/hint_past");
    let replayed = replay(&body_cell, &session, &base, &fixtures.path().join("body")).await;
    assert_eq!(verdict(&body_cell, None, &replayed), None);
    body_cell.spark = body_cell.spark.replace("version 10 ", "version 11 ");
    assert!(verdict(&body_cell, None, &replayed).is_some(), "wrong body");

    let zero = first_cell("path_select", "sp_plain/as_written/none/hint_zero");
    let golden = goldens
        .get(&key_of(&zero))
        .expect("zero-hint golden")
        .clone();
    let replayed = replay(&zero, &session, &base, &fixtures.path().join("zero")).await;
    assert_eq!(verdict(&zero, Some(&golden), &replayed), None);
    let wrong_text = Golden {
        outcome: golden.outcome.replacen("no Iceberg table", "no table", 1),
        ..golden.clone()
    };
    assert_ne!(wrong_text.outcome, golden.outcome);
    assert!(
        verdict(&zero, Some(&wrong_text), &replayed).is_some(),
        "wrong text"
    );
    let changed_class = Golden {
        outcome: golden
            .outcome
            .replacen("ERR AnalysisException:", "ERR PySparkException:", 1),
        ..golden.clone()
    };
    assert_ne!(changed_class.outcome, golden.outcome);
    assert!(
        verdict(&zero, Some(&changed_class), &replayed).is_some(),
        "changed class"
    );
    assert!(verdict(&zero, None, &replayed).is_some(), "no golden");

    let codec = first_cell(
        "path_select",
        "sp_plain/gzname_plainbytes/none/hint_current",
    );
    let golden = goldens.get(&key_of(&codec)).expect("codec golden").clone();
    let replayed = replay(&codec, &session, &base, &fixtures.path().join("codec")).await;
    assert_eq!(verdict(&codec, Some(&golden), &replayed), None);
    let wrong_rows = Golden {
        outcome: "rows [3]".to_string(),
        ..golden
    };
    assert!(
        verdict(&codec, Some(&wrong_rows), &replayed).is_some(),
        "wrong declared value"
    );

    let mut append = first_cell("path_insert", "sp_plain/as_written/none/hint_current");
    let golden = goldens
        .get(&key_of(&append))
        .expect("append golden")
        .clone();
    let replayed = replay(&append, &session, &base, &fixtures.path().join("append")).await;
    assert_eq!(verdict(&append, Some(&golden), &replayed), None);
    let changed = Replay {
        observed: replayed.observed.clone(),
        state_changed: true,
    };
    assert!(
        verdict(&append, Some(&golden), &changed).is_some(),
        "state change"
    );
    append.repark_changed = vec!["+v6.metadata.json".to_string()];
    assert!(
        verdict(&append, Some(&golden), &replayed).is_some(),
        "recorded change"
    );
}
