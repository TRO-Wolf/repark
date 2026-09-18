use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use repark_core::{ErrorClass, ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use serde_json::{Map, Value};
use tempfile::TempDir;

const FORMAT_VERSIONS: [&str; 2] = ["2", "3"];

const ANSI_DELIMITED_IDENTIFIER_TWINS: [(&str, &str); 2] = [
    ("rename_double_quoted", "rename_dotted"),
    ("add_double_quoted_leaf", "add_dotted_leaf"),
];

fn oracle() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../python/repark-parity/fixtures/torture/data/ice_nested_evo_1/oracle.json");
    let text = std::fs::read_to_string(&path).expect("Spark's nested DDL recording");
    serde_json::from_str(&text).expect("the recording is JSON")
}

fn cells_of<'a>(root: &'a Value, key: &str, format_version: &str) -> Vec<(&'a str, &'a Value)> {
    let Value::Object(cells) = &root[key] else {
        panic!("the recording has no `{key}` object");
    };
    let prefix = format!("v{format_version}_");
    let mut ordered: Vec<(&str, &Value)> = cells
        .iter()
        .filter(|(label, _)| label.starts_with(&prefix))
        .map(|(label, cell)| (label.as_str(), cell))
        .collect();
    ordered.sort_by_key(|(_, cell)| replay_rank(cell));
    ordered
}

fn replay_rank(cell: &Value) -> u8 {
    let statements = cell["statements"].as_array().map_or(&[][..], Vec::as_slice);
    match statements.first().and_then(Value::as_str) {
        Some(first) if first.starts_with("CREATE") => 0,
        Some(_) => 1,
        None => 2,
    }
}

struct Door {
    session: ReparkSession,
    ansi: Arc<dyn SqlDialect>,
    warehouse: PathBuf,
    _dir: TempDir,
}

impl Door {
    async fn sql(&self, sql: &str) -> repark_core::Result<datafusion::prelude::DataFrame> {
        self.session.sql_with(&self.ansi, sql).await
    }
}

async fn ansi_door(format_version: &str) -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_path_buf();
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let builder = if format_version == "2" {
        ReparkSession::builder().with_sql_dialect(Arc::clone(&ansi))
    } else {
        let spark: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
        ReparkSession::builder()
            .with_extension(Arc::new(SparkExtension))
            .with_sql_dialect(spark)
            .config("repark.sql.allowCreateFormatVersion3", "true")
    };
    let session = builder.build().expect("session");
    session
        .register_memory_catalog("ice", warehouse.to_str().expect("utf8"))
        .await
        .expect("catalog");
    let door = Door {
        session,
        ansi,
        warehouse,
        _dir: dir,
    };
    door.sql("CREATE SCHEMA IF NOT EXISTS ice.ns")
        .await
        .expect("namespace");
    door
}

fn ansi_spelling(spark_sql: &str) -> String {
    let mut sql = spark_sql
        .replace("sc.ns.", "ice.ns.")
        .replace(" USING iceberg", "");
    for version in FORMAT_VERSIONS {
        sql = sql.replace(
            &format!("TBLPROPERTIES ('format-version'='{version}')"),
            &format!("WITH (format_version = '{version}')"),
        );
    }
    sql
}

struct Refusal {
    class: ErrorClass,
    message: String,
}

async fn run(door: &Door, spark_sql: &str) -> Result<(), Refusal> {
    let sql = ansi_spelling(spark_sql);
    let frame = door.sql(&sql).await.map_err(|error| Refusal {
        class: error.exception_class(),
        message: error.to_string(),
    })?;
    frame.collect().await.map(|_| ()).map_err(|error| Refusal {
        class: ErrorClass::Base,
        message: error.to_string(),
    })
}

async fn run_all(door: &Door, statements: &[Value], with_inserts: bool) -> Option<Refusal> {
    for statement in statements {
        let sql = statement.as_str().expect("a statement is a string");
        if !with_inserts && sql.starts_with("INSERT") {
            continue;
        }
        if let Err(refusal) = run(door, sql).await {
            return Some(refusal);
        }
    }
    None
}

fn spark_simple(data_type: &Value) -> String {
    if let Value::String(name) = data_type {
        return match name.as_str() {
            "integer" => "int".to_string(),
            "long" => "bigint".to_string(),
            "short" => "smallint".to_string(),
            "byte" => "tinyint".to_string(),
            other => other.to_string(),
        };
    }
    match data_type["type"].as_str() {
        Some("struct") => {
            let children = data_type["fields"]
                .as_array()
                .expect("struct fields")
                .iter()
                .map(|field| {
                    format!(
                        "{}:{}",
                        field["name"].as_str().expect("name"),
                        spark_simple(&field["type"])
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("struct<{children}>")
        }
        Some("array") => format!("array<{}>", spark_simple(&data_type["elementType"])),
        Some("map") => format!(
            "map<{},{}>",
            spark_simple(&data_type["keyType"]),
            spark_simple(&data_type["valueType"])
        ),
        other => panic!("unmapped Spark type {other:?}"),
    }
}

fn arrow_simple(data_type: &DataType) -> String {
    match data_type {
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Date32 => "date".to_string(),
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|field| format!("{}:{}", field.name(), arrow_simple(field.data_type())))
                .collect::<Vec<_>>()
                .join(",");
            format!("struct<{children}>")
        }
        DataType::List(element) | DataType::LargeList(element) => {
            format!("array<{}>", arrow_simple(element.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => format!(
                "map<{},{}>",
                arrow_simple(pair[0].data_type()),
                arrow_simple(pair[1].data_type())
            ),
            other => panic!("map entries are not a key/value struct: {other}"),
        },
        other => other.to_string(),
    }
}

fn aliased_leaf_read(sql: &str) -> String {
    sql.replace(
        "SELECT id, s.a, s.b FROM",
        "SELECT id, s.a AS a, s.b AS b FROM",
    )
}

async fn read_columns(door: &Door, spark_query: &str) -> Result<Vec<String>, String> {
    let sql = aliased_leaf_read(&ansi_spelling(spark_query));
    let frame = door.sql(&sql).await.map_err(|error| error.to_string())?;
    Ok(frame
        .schema()
        .fields()
        .iter()
        .map(|field: &Arc<Field>| format!("{}:{}", field.name(), arrow_simple(field.data_type())))
        .collect())
}

fn spark_columns(schema: &Value) -> Vec<String> {
    schema["fields"]
        .as_array()
        .expect("schema fields")
        .iter()
        .map(|field| {
            format!(
                "{}:{}",
                field["name"].as_str().expect("name"),
                spark_simple(&field["type"])
            )
        })
        .collect()
}

fn describe_columns(rows: &Value) -> Vec<String> {
    rows.as_array()
        .expect("describe rows")
        .iter()
        .map(|row| {
            format!(
                "{}:{}",
                row["col_name"].as_str().expect("col_name"),
                row["data_type"].as_str().expect("data_type")
            )
        })
        .collect()
}

fn table_of(query: &str) -> String {
    let after = query
        .split(" FROM ")
        .nth(1)
        .or_else(|| query.strip_prefix("DESCRIBE TABLE "))
        .expect("a table reference");
    after.split(' ').next().expect("a table name").to_string()
}

fn metadata_files(root: &Path, table: &str, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            metadata_files(&path, table, found);
        } else if path.to_string_lossy().ends_with(".metadata.json")
            && path
                .parent()
                .and_then(Path::parent)
                .is_some_and(|dir| dir.ends_with(Path::new("ns").join(table)))
        {
            found.push(path);
        }
    }
}

fn metadata_version(path: &Path) -> u64 {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.split('-').next())
        .and_then(|prefix| prefix.parse().ok())
        .expect("a numbered metadata file")
}

fn without_schema_ids(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .filter(|(key, _)| !matches!(key.as_str(), "schema-id" | "identifier-field-ids"))
                .map(|(key, child)| (key.clone(), without_schema_ids(child)))
                .collect::<Map<_, _>>(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(without_schema_ids).collect()),
        other => other.clone(),
    }
}

fn current_schema(door: &Door, table: &str) -> Value {
    let mut found = Vec::new();
    metadata_files(&door.warehouse, table, &mut found);
    let Some(latest) = found.into_iter().max_by_key(|path| metadata_version(path)) else {
        return Value::Null;
    };
    let text = std::fs::read_to_string(&latest).expect("metadata file");
    let metadata: Value = serde_json::from_str(&text).expect("metadata JSON");
    let current = metadata["current-schema-id"].clone();
    metadata["schemas"]
        .as_array()
        .expect("schemas")
        .iter()
        .find(|schema| schema["schema-id"] == current)
        .map_or(Value::Null, without_schema_ids)
}

fn spark_class(error: &Value) -> ErrorClass {
    match error["python_class"].as_str() {
        Some("ParseException") => ErrorClass::Parse,
        Some("AnalysisException") => ErrorClass::Analysis,
        _ => ErrorClass::Base,
    }
}

fn spark_message_head(error: &Value) -> String {
    let message = error["message"].as_str().unwrap_or_default();
    match message.find("SQLSTATE: ") {
        Some(at) => message[..at + "SQLSTATE: 00000".len()].to_string(),
        None => message.to_string(),
    }
}

async fn ddl_cell_mismatch(door: &Door, label: &str, cell: &Value) -> Option<String> {
    let statements = cell["statements"].as_array().expect("statements");
    let refusal = run_all(door, statements, false).await;
    if label.ends_with("add_required_nested_child") {
        let spark_line = cell["error"]["message"]
            .as_str()
            .expect("Spark's refusal line");
        return match refusal {
            Some(refusal) if refusal.message.contains(spark_line) => None,
            Some(refusal) => Some(format!("{label}: refused `{}`", refusal.message)),
            None => Some(format!(
                "{label}: accepted a required child without a default"
            )),
        };
    }
    if let Some(refusal) = refusal {
        return Some(format!("{label}: refused `{}`", refusal.message));
    }
    let query = cell["query"].as_str()?;
    if label.ends_with("_describe") {
        let got = read_columns(door, &format!("SELECT * FROM {}", table_of(query))).await;
        let want = describe_columns(&cell["rows"]);
        return (got.as_ref() != Ok(&want)).then(|| format!("{label}: {got:?} != {want:?}"));
    }
    let got = read_columns(door, query).await;
    let want = spark_columns(&cell["schema"]);
    (got.as_ref() != Ok(&want)).then(|| format!("{label}: {got:?} != {want:?}"))
}

fn ansi_twin_of<'a>(root: &'a Value, format_version: &str, label: &str) -> &'a Value {
    let short = label.trim_start_matches(&format!("v{format_version}_"));
    let twin = ANSI_DELIMITED_IDENTIFIER_TWINS
        .iter()
        .find(|(spark_spelling, _)| *spark_spelling == short)
        .map_or(short, |(_, twin)| *twin);
    &root["schema_cells"][format!("v{format_version}_{twin}")]
}

async fn schema_cell_mismatch(
    door: &Door,
    label: &str,
    cell: &Value,
    expected: &Value,
) -> Option<String> {
    let statements = cell["statements"].as_array().expect("statements");
    let refusal = run_all(door, statements, false).await;
    let table = cell["table"].as_str().expect("table");
    let got = current_schema(door, table);
    let cell = expected;
    let want = without_schema_ids(&cell["metadata_schema"]);
    if got != want {
        return Some(format!("{label}: metadata schema {got} != Spark {want}"));
    }
    match (&cell["error"], refusal) {
        (Value::Null, None) => None,
        (Value::Null, Some(refusal)) => Some(format!("{label}: refused `{}`", refusal.message)),
        (error, None) => Some(format!("{label}: accepted, Spark refused {error}")),
        (error, Some(refusal)) => {
            let head = spark_message_head(error);
            let same_class = refusal.class == spark_class(error);
            (!same_class || !refusal.message.contains(&head)).then(|| {
                format!(
                    "{label}: refused {:?} `{}`, Spark {:?} `{head}`",
                    refusal.class,
                    refusal.message,
                    spark_class(error)
                )
            })
        }
    }
}

#[tokio::test]
async fn every_recorded_nested_ddl_cell_answers_spark_on_the_ansi_door() {
    let root = oracle();
    let mut mismatches = Vec::new();
    for format_version in FORMAT_VERSIONS {
        let door = ansi_door(format_version).await;
        for (label, cell) in cells_of(&root, "cells", format_version) {
            mismatches.extend(ddl_cell_mismatch(&door, label, cell).await);
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[tokio::test]
async fn every_recorded_nested_schema_cell_answers_spark_on_the_ansi_door() {
    let root = oracle();
    let mut mismatches = Vec::new();
    for format_version in FORMAT_VERSIONS {
        let door = ansi_door(format_version).await;
        for (label, cell) in cells_of(&root, "schema_cells", format_version) {
            let expected = ansi_twin_of(&root, format_version, label);
            mismatches.extend(schema_cell_mismatch(&door, label, cell, expected).await);
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
