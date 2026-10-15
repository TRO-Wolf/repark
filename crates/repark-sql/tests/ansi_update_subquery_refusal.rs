use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const REFUSAL: &str = "UPDATE with SET column = (SELECT ...) is not supported on this \
     statement: RePark cannot write the subquery value here, so it refuses before any write.";

const DOOR_PREFIX: &str = "Error during planning: ";

const SEEDED: [(i32, i32, i32, i32); 2] = [(0, 10, 10, 20), (3, 30, 30, 40)];

const PREDICATES: [(&str, &str); 3] = [
    ("where_true", " WHERE true"),
    ("where_in_list", " WHERE id IN (0, 3)"),
    ("no_where", ""),
];

const NO_MATCH_PREDICATES: [(&str, &str); 2] = [
    ("where_false", " WHERE false"),
    ("where_no_match", " WHERE id BETWEEN 100 AND 200"),
];

const DATA_DELETING_ACTIONS: [&str; 8] = [
    "drop",
    "delete",
    "truncate",
    "overwrite",
    "replace",
    "purge",
    "expire",
    "remove_orphan",
];

const VALUES: [(&str, &str); 2] = [
    ("subquery", "(SELECT {c} FROM {t}_src)"),
    (
        "subquery_with_where",
        "(SELECT {c} FROM {t}_src WHERE id = 1)",
    ),
];

const STRUCT_SOURCES: [(&str, &str); 2] = [
    (
        "reordered",
        "named_struct('b', CAST(2 AS INT), 'a', CAST(1 AS INT))",
    ),
    (
        "in_order",
        "named_struct('a', CAST(1 AS INT), 'b', CAST(2 AS INT))",
    ),
];

async fn ansi_session() -> (ReparkSession, TempDir) {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("ansi session");
    let warehouse = TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("path").to_string();
    session
        .register_memory_catalog("ice", &root)
        .await
        .expect("catalog");
    run(
        &session,
        &format!("CREATE SCHEMA ice.ns WITH (location = '{root}/ns')"),
    )
    .await;
    (session, warehouse)
}

async fn spark_session() -> (ReparkSession, TempDir) {
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("spark session");
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    (session, warehouse)
}

async fn run(session: &ReparkSession, sql: &str) {
    session
        .sql(sql)
        .await
        .expect("plans")
        .collect()
        .await
        .expect("runs");
}

async fn seed(session: &ReparkSession, table: &str, source_value: &str) {
    run(
        session,
        &format!(
            "CREATE TABLE {table} AS SELECT CAST(0 AS INT) AS id, CAST(10 AS INT) AS v, \
             named_struct('a', CAST(10 AS INT), 'b', CAST(20 AS INT)) AS st UNION ALL \
             SELECT CAST(3 AS INT), CAST(30 AS INT), \
             named_struct('a', CAST(30 AS INT), 'b', CAST(40 AS INT))"
        ),
    )
    .await;
    run(
        session,
        &format!(
            "CREATE TABLE {table}_src AS SELECT CAST(1 AS INT) AS id, CAST(7 AS INT) AS v, \
             {source_value} AS st"
        ),
    )
    .await;
}

async fn rows(session: &ReparkSession, table: &str) -> Vec<(i32, i32, i32, i32)> {
    let batches = session
        .sql(&format!(
            "SELECT id, v, st['a'] AS a, st['b'] AS b FROM {table} ORDER BY id"
        ))
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let mut found = Vec::new();
    for batch in &batches {
        let column = |index: usize| batch.column(index).as_primitive::<Int32Type>().clone();
        let (id, v, a, b) = (column(0), column(1), column(2), column(3));
        for row in 0..batch.num_rows() {
            found.push((id.value(row), v.value(row), a.value(row), b.value(row)));
        }
    }
    found
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<(), String> {
    match session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

type TableState = (Option<i64>, usize, Option<String>, BTreeSet<String>);

async fn table_state(session: &ReparkSession, table: &str) -> TableState {
    let name = table.rsplit('.').next().expect("table name");
    let catalogs = session.catalogs_snapshot();
    let loaded = catalogs
        .get("ice")
        .expect("catalog")
        .load_table(&TableIdent::new(
            NamespaceIdent::new("ns".to_string()),
            name.to_string(),
        ))
        .await
        .expect("load table");
    let metadata = loaded.metadata();
    let directory = Path::new(metadata.location().trim_start_matches("file://")).join("metadata");
    let files = std::fs::read_dir(&directory)
        .expect("metadata directory")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    (
        metadata.current_snapshot_id(),
        metadata.snapshots().count(),
        loaded.metadata_location().map(str::to_string),
        files,
    )
}

async fn refusal(session: &ReparkSession, table: &str, sql: &str) -> Result<String, String> {
    let before = table_state(session, table).await;
    let outcome = attempt(session, sql).await;
    let after = table_state(session, table).await;
    match outcome {
        Err(text) if text.contains(REFUSAL) && after == before => Ok(text),
        outcome => Err(format!("{sql}: {outcome:?}, table {before:?} -> {after:?}")),
    }
}

async fn refusals_on(
    session: &ReparkSession,
    column: &str,
    sources: &[(&str, &str)],
    predicates: &[(&str, &str)],
) -> (Vec<String>, BTreeSet<String>) {
    let mut failures = Vec::new();
    let mut texts = BTreeSet::new();
    for (source, source_value) in sources {
        for (predicate, where_sql) in predicates {
            for (form, value) in VALUES {
                let table = format!("ice.ns.{column}_{source}_{predicate}_{form}");
                seed(session, &table, source_value).await;
                let value = value.replace("{c}", column).replace("{t}", &table);
                let sql = format!("UPDATE {table} SET {column} = {value}{where_sql}");
                match refusal(session, &table, &sql).await {
                    Ok(text) => {
                        texts.insert(text);
                    }
                    Err(failure) => failures.push(failure),
                }
                let after = rows(session, &table).await;
                if after != SEEDED {
                    failures.push(format!("{sql}: rows {after:?}"));
                }
            }
        }
    }
    (failures, texts)
}

async fn refusals_for(
    column: &str,
    sources: &[(&str, &str)],
    predicates: &[(&str, &str)],
) -> Vec<String> {
    let (session, _warehouse) = ansi_session().await;
    refusals_on(&session, column, sources, predicates).await.0
}

#[tokio::test]
async fn ruling_74_a_flat_int_update_from_a_subquery_refuses_on_the_ansi_door() {
    let failures = refusals_for("v", &[("int", STRUCT_SOURCES[1].1)], &PREDICATES).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn ruling_90_a_struct_update_from_a_subquery_refuses_on_the_ansi_door() {
    let failures = refusals_for("st", &STRUCT_SOURCES, &PREDICATES).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn ruling_95_an_update_from_a_subquery_that_matches_no_row_refuses_on_the_ansi_door() {
    let mut failures =
        refusals_for("v", &[("int", STRUCT_SOURCES[1].1)], &NO_MATCH_PREDICATES).await;
    failures.extend(refusals_for("st", &STRUCT_SOURCES, &NO_MATCH_PREDICATES).await);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn family_texts(session: &ReparkSession) -> BTreeSet<String> {
    let mut failures = Vec::new();
    let mut texts = BTreeSet::new();
    for (column, sources) in [("v", &STRUCT_SOURCES[1..]), ("st", &STRUCT_SOURCES[..])] {
        for predicates in [&PREDICATES[..], &NO_MATCH_PREDICATES[..]] {
            let (found, refused) = refusals_on(session, column, sources, predicates).await;
            failures.extend(found);
            texts.extend(refused);
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    texts
}

fn stripped(texts: &BTreeSet<String>) -> BTreeSet<&str> {
    texts
        .iter()
        .map(|text| text.strip_prefix(DOOR_PREFIX).unwrap_or(text))
        .collect()
}

#[tokio::test]
async fn ruling_95_the_whole_refused_family_answers_one_text_on_both_doors() {
    let (ansi, _ansi_warehouse) = ansi_session().await;
    let (spark, _spark_warehouse) = spark_session().await;
    let ansi_texts = family_texts(&ansi).await;
    let spark_texts = family_texts(&spark).await;
    assert_eq!(
        stripped(&ansi_texts),
        BTreeSet::from([REFUSAL]),
        "{ansi_texts:?}"
    );
    assert_eq!(
        stripped(&spark_texts),
        stripped(&ansi_texts),
        "{spark_texts:?} {ansi_texts:?}"
    );
}

#[tokio::test]
async fn ruling_95_the_refusal_names_no_data_deleting_action_on_the_ansi_door() {
    let (session, _warehouse) = ansi_session().await;
    let texts = family_texts(&session).await;
    assert!(!texts.is_empty());
    for text in &texts {
        let lowered = text.to_lowercase();
        for action in DATA_DELETING_ACTIONS {
            assert!(!lowered.contains(action), "{action} in {text}");
        }
    }
}
