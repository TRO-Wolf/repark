use std::io::Write;
use std::path::Path;

use iceberg::TableCreation;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

use super::*;

const CELLS: &str = "../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl";
const CATALOG_STYLE: &str = "00009-2f6c1c3e-9a7b-4d2e-8f10-5b3a7c9d0e11.metadata.json";
const PINNED_ROWS: usize = 1418;
const SKIPPED_CRC_KEPT: usize = 20;
const SKIPPED_CODEC_BY_NAME: usize = 472;

struct Cell {
    fixture: String,
    writer: String,
    names: String,
    cleanup: String,
    hint: String,
    spark: String,
}

fn path_read_cells() -> Vec<Cell> {
    let text = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(CELLS))
        .expect("cells file");
    text.lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("cell json"))
        .filter(|cell| cell["head"] == "path_select")
        .map(|cell| {
            let fixture = cell["fixture"].as_str().expect("fixture").to_string();
            let parts: Vec<String> = fixture.split('/').map(str::to_string).collect();
            let field = |index: usize| parts.get(index).cloned().unwrap_or_default();
            Cell {
                writer: field(0),
                names: field(1),
                cleanup: field(2),
                hint: field(3).trim_start_matches("hint_").to_string(),
                spark: cell["spark"].as_str().expect("spark").to_string(),
                fixture,
            }
        })
        .collect()
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
        _ => (0..5)
            .map(|index| (version_of(index), at(index), Form::Plain))
            .collect(),
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
        _ => return None,
    })
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
    let rename = match cell.names.as_str() {
        "gz_new" => Some(Form::GzInfix),
        "gz_old" => Some(Form::GzLegacy),
        "gzname_plainbytes" => Some(Form::GzInfixPlainBytes),
        "plainname_gzbytes" => Some(Form::PlainGzBytes),
        _ => None,
    };
    if let Some(form) = rename {
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
        _ => true,
    });
    for (version, payload, form) in &versions {
        let (name, bytes) = file_for(*version, payload, *form);
        std::fs::write(metadata.join(name), bytes).expect("write");
    }
    let hint = metadata.join("version-hint.text");
    if cell.hint == "dir" {
        std::fs::create_dir(&hint).expect("mkdir");
    } else if let Some(text) = hint_text(&cell.hint, newest) {
        std::fs::write(&hint, text).expect("hint");
        if cell.hint == "unreadable" {
            std::fs::set_permissions(&hint, std::fs::Permissions::from_mode(0)).expect("chmod");
        }
    }
}

async fn repark_answer(session: &crate::ReparkSession, root: &str) -> Result<Vec<i64>> {
    let batches = session
        .read_iceberg_path(root)
        .await?
        .collect()
        .await
        .map_err(engine_err)?;
    let mut ids: Vec<i64> = batches
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
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

fn spark_rows(spark: &str) -> Option<Vec<i64>> {
    let list = spark.strip_prefix("rows ")?;
    Some(serde_json::from_str(list).expect("rows list"))
}

fn spark_missing_version(spark: &str) -> Option<&str> {
    spark
        .split("Metadata file for version ")
        .nth(1)?
        .split(' ')
        .next()
}

fn mismatch(cell: &Cell, root: &str, answer: &Result<Vec<i64>>) -> Option<String> {
    match (spark_rows(&cell.spark), answer) {
        (Some(rows), Ok(ids)) if rows == *ids => None,
        (None, Err(error)) => {
            let version = spark_missing_version(&cell.spark)?;
            let expected =
                format!("Metadata file for version {version} is missing under {root}/metadata");
            (error.to_string() != expected).then(|| format!("{}: text {error}", cell.fixture))
        }
        (_, answer) => Some(format!(
            "{}: spark {} repark {answer:?}",
            cell.fixture, cell.spark
        )),
    }
}

#[tokio::test]
async fn path_read_cells_match_the_spark_column() {
    let (_warehouse, session, base) = base_history().await;
    let fixtures = tempfile::tempdir().expect("tempdir");
    let (mut pinned, mut crc_kept, mut codec_by_name) = (0, 0, 0);
    let mut mismatches = Vec::new();
    for (index, cell) in path_read_cells().iter().enumerate() {
        if cell.fixture.ends_with("/crc_kept") {
            crc_kept += 1;
            continue;
        }
        if cell.names.contains("bytes") && cell.spark.contains("RuntimeIOException") {
            codec_by_name += 1;
            continue;
        }
        let root = fixtures.path().join(format!("f{index:04}"));
        build_fixture(cell, &base, &root);
        let root = root.to_string_lossy().to_string();
        let answer = repark_answer(&session, &root).await;
        mismatches.extend(mismatch(cell, &root, &answer));
        pinned += 1;
    }
    assert!(
        mismatches.is_empty(),
        "{} mismatches: {:#?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(30)]
    );
    assert_eq!(
        (pinned, crc_kept, codec_by_name),
        (PINNED_ROWS, SKIPPED_CRC_KEPT, SKIPPED_CODEC_BY_NAME)
    );
}
