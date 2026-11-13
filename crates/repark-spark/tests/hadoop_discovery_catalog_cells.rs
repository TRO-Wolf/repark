use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use repark_common::ErrorClass;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use serde_json::Value;

const CELLS: &str = include_str!("../../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl");
const GOLDENS: &str =
    include_str!("../../../docs/corpus/hadoop-discovery-repark-goldens-2026-11-13.jsonl");
const CARD: &str = "D8 HADOOP-FACADE-FRESH-SESSION-1";
const CATALOG_ROWS: usize = 84;

struct Cell {
    head: String,
    cat: String,
    fixture: String,
    ordinal: usize,
    spark: String,
    repark_changed: usize,
}

fn text(row: &Value, key: &str) -> String {
    row[key]
        .as_str()
        .unwrap_or_else(|| panic!("row has no string {key}: {row}"))
        .to_string()
}

fn rows(body: &str) -> Vec<Value> {
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("json line"))
        .collect()
}

fn catalog_cells() -> Vec<Cell> {
    let all = rows(CELLS);
    let ordinals: HashMap<String, usize> = all
        .iter()
        .filter(|row| row["head"] == "path_select")
        .enumerate()
        .map(|(index, row)| (text(row, "fixture"), index + 1))
        .collect();
    all.iter()
        .filter(|row| text(row, "head").starts_with("catalog_"))
        .map(|row| {
            let fixture = text(row, "fixture");
            Cell {
                head: text(row, "head"),
                cat: text(row, "cat"),
                ordinal: *ordinals.get(&fixture).expect("fixture ordinal"),
                spark: text(row, "spark"),
                repark_changed: row["repark_changed"].as_array().expect("changes").len(),
                fixture,
            }
        })
        .collect()
}

fn goldens() -> BTreeMap<(String, String, String), (String, String)> {
    rows(GOLDENS)
        .iter()
        .filter(|row| text(row, "head").starts_with("catalog_"))
        .map(|row| {
            (
                (text(row, "head"), text(row, "cat"), text(row, "fixture")),
                (text(row, "card"), text(row, "outcome")),
            )
        })
        .collect()
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn gzip_stored(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
    let chunks: Vec<&[u8]> = bytes.chunks(usize::from(u16::MAX)).collect();
    for (index, chunk) in chunks.iter().enumerate() {
        let length = u16::try_from(chunk.len()).expect("chunk length");
        out.push(u8::from(index + 1 == chunks.len()));
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(&(!length).to_le_bytes());
        out.extend_from_slice(chunk);
    }
    out.extend_from_slice(&crc32(bytes).to_le_bytes());
    let size = u32::try_from(bytes.len()).expect("payload size");
    out.extend_from_slice(&size.to_le_bytes());
    out
}

async fn base_history() -> (tempfile::TempDir, Vec<Vec<u8>>) {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let root = warehouse.path().to_string_lossy().to_string();
    let session = ReparkSession::new().expect("session");
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
    (warehouse, payloads)
}

fn build_fixture(cell: &Cell, base: &[Vec<u8>], table: &Path) {
    let parts: Vec<&str> = cell.fixture.split('/').collect();
    assert_eq!(parts[1..3], ["as_written", "none"], "{}", cell.fixture);
    let plain =
        |version: usize, index: usize| (format!("v{version}.metadata.json"), base[index].clone());
    let gz = |version: usize, index: usize| {
        (
            format!("v{version}.gz.metadata.json"),
            gzip_stored(&base[index]),
        )
    };
    let files: Vec<(String, Vec<u8>)> = match parts[0] {
        "sp_gzip" => (0..5).map(|index| gz(index + 1, index)).collect(),
        "sp_mixed" => vec![
            plain(1, 0),
            plain(2, 1),
            gz(3, 1),
            gz(4, 2),
            gz(5, 3),
            gz(6, 4),
        ],
        "sp_cleanup" => vec![plain(4, 3), plain(5, 4)],
        "sp_plain" | "rp_plain" | "rp_gzip" | "rp_cleanup" => {
            (0..5).map(|index| plain(index + 1, index)).collect()
        }
        other => panic!("unknown writer {other}"),
    };
    let newest = match parts[0] {
        "sp_mixed" => 6,
        _ => 5,
    };
    let metadata = table.join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    for (name, bytes) in files {
        std::fs::write(metadata.join(name), bytes).expect("write");
    }
    let hint = match parts[3] {
        "hint_current" => Some(newest),
        "hint_stale_prev" => Some(newest - 1),
        "hint_missing" => None,
        other => panic!("unknown hint {other}"),
    };
    if let Some(version) = hint {
        std::fs::write(metadata.join("version-hint.text"), version.to_string()).expect("hint");
    }
}

fn door(cat: &str, warehouse: &str) -> (ReparkSession, &'static str) {
    let builder = ReparkSession::builder();
    let (builder, name) = match cat {
        "facade" => (
            builder
                .with_extension(Arc::new(SparkExtension))
                .with_sql_dialect(Arc::new(SparkDialect)),
            "hd",
        ),
        "native" => (builder, "nat"),
        other => panic!("unknown door {other}"),
    };
    let session = builder
        .config(format!("spark.sql.catalog.{name}.type"), "hadoop")
        .config(format!("spark.sql.catalog.{name}.warehouse"), warehouse)
        .build()
        .expect("session");
    (session, name)
}

fn class_name(error: &repark_core::Error) -> &'static str {
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

async fn outcome(session: &ReparkSession, sql: &str, warehouse: &str) -> String {
    let refused = |error: repark_core::Error| {
        format!(
            "ERR {}: {}",
            class_name(&error),
            error.to_string().replace(warehouse, "<warehouse>")
        )
    };
    let frame = match session.sql(sql).await {
        Ok(frame) => frame,
        Err(error) => return refused(error),
    };
    match frame.collect().await {
        Ok(batches) => {
            let mut shown: Vec<String> = batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).map(move |row| {
                        datafusion::arrow::util::display::array_value_to_string(
                            batch.column(batch.num_columns() - 1),
                            row,
                        )
                        .expect("cell")
                    })
                })
                .collect();
            shown.sort();
            format!("rows {shown:?}")
        }
        Err(error) => refused(repark_core::engine_err(error)),
    }
}

fn tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
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
                pending.push(path);
            } else {
                state.insert(name, std::fs::read(&path).expect("read"));
            }
        }
    }
    state
}

fn verdict(cell: &Cell, golden: Option<&(String, String)>, observed: &str) -> Option<String> {
    let label = format!("{} {} {}", cell.head, cell.cat, cell.fixture);
    match golden {
        None => Some(format!("{label}: no golden; observed {observed}")),
        Some((card, _)) if card != CARD => Some(format!("{label}: card {card}")),
        Some((_, outcome)) if *outcome == cell.spark => {
            Some(format!("{label}: golden restates Spark's {outcome}"))
        }
        Some((_, outcome)) if outcome != observed => {
            Some(format!("{label}: expected {outcome} observed {observed}"))
        }
        Some(_) if cell.repark_changed != 0 => Some(format!("{label}: recorded state change")),
        Some(_) => None,
    }
}

async fn replay_all(cells: &[Cell]) -> (tempfile::TempDir, Vec<String>, bool) {
    let (_history, base) = base_history().await;
    let warehouse = tempfile::tempdir().expect("warehouse");
    for cell in cells {
        let table = warehouse
            .path()
            .join(format!("ctl_f{:04}", cell.ordinal))
            .join("t");
        if !table.exists() {
            build_fixture(cell, &base, &table);
        }
    }
    let before = tree(warehouse.path());
    let root = warehouse.path().to_string_lossy().to_string();
    let mut observed = Vec::new();
    for cell in cells {
        let (session, name) = door(&cell.cat, &root);
        session
            .register_configured_catalogs()
            .await
            .expect("catalogs");
        let namespace = format!("{name}.ctl_f{:04}", cell.ordinal);
        let sql = match cell.head.as_str() {
            "catalog_select" => format!("SELECT id FROM {namespace}.t"),
            "catalog_show_tables" => format!("SHOW TABLES IN {namespace}"),
            other => panic!("unknown head {other}"),
        };
        observed.push(outcome(&session, &sql, &root).await);
    }
    let changed = tree(warehouse.path()) != before;
    (warehouse, observed, changed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_hadoop_catalog_row_keeps_its_declared_refusal_on_both_doors() {
    let cells = catalog_cells();
    assert_eq!(cells.len(), CATALOG_ROWS);
    let (_warehouse, observed, changed) = replay_all(&cells).await;
    assert!(!changed, "a catalog row changed the warehouse");
    if let Ok(path) = std::env::var("REPARK_HADOOP_CATALOG_OBSERVED") {
        let lines: Vec<String> = cells
            .iter()
            .zip(&observed)
            .map(|(cell, seen)| {
                serde_json::json!({"head": cell.head, "cat": cell.cat, "fixture": cell.fixture,
                    "observed": seen, "spark": cell.spark})
                .to_string()
            })
            .collect();
        std::fs::write(path, lines.join("\n") + "\n").expect("observed");
    }
    let goldens = goldens();
    assert_eq!(goldens.len(), CATALOG_ROWS);
    let mismatches: Vec<String> = cells
        .iter()
        .zip(&observed)
        .filter_map(|(cell, seen)| {
            let key = (cell.head.clone(), cell.cat.clone(), cell.fixture.clone());
            verdict(cell, goldens.get(&key), seen)
        })
        .collect();
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catalog_checker_rejects_a_wrong_value_a_wrong_text_and_a_changed_class() {
    let cells: Vec<Cell> = catalog_cells().into_iter().take(2).collect();
    let goldens = goldens();
    let (_warehouse, observed, _) = replay_all(&cells).await;
    for (cell, seen) in cells.iter().zip(&observed) {
        let key = (cell.head.clone(), cell.cat.clone(), cell.fixture.clone());
        let (card, golden) = goldens.get(&key).expect("golden").clone();
        assert_eq!(
            verdict(cell, Some(&(card.clone(), golden.clone())), seen),
            None
        );
        let wrong_text = golden.replacen("not", "never", 1);
        let changed_class = golden.replacen("ERR AnalysisException:", "ERR PySparkException:", 1);
        let wrong_value = "rows [\"u\"]".to_string();
        for mutated in [wrong_text, changed_class, wrong_value] {
            assert_ne!(mutated, golden);
            assert!(
                verdict(cell, Some(&(card.clone(), mutated.clone())), seen).is_some(),
                "{mutated}"
            );
        }
        assert!(verdict(cell, Some(&("D4".to_string(), golden)), seen).is_some());
        assert!(verdict(cell, None, seen).is_some());
    }
}
