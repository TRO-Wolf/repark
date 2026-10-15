use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::util::pretty::pretty_format_batches;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
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

const STRUCT_SOURCES: [(&str, &str, &str); 2] = [
    (
        "reordered",
        "STRUCT<b: INT, a: INT>",
        "named_struct('b', 2, 'a', 1)",
    ),
    (
        "in_order",
        "STRUCT<a: INT, b: INT>",
        "named_struct('a', 1, 'b', 2)",
    ),
];

async fn catalog_session() -> (ReparkSession, TempDir) {
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("session");
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

async fn seed(session: &ReparkSession, table: &str, source_type: &str, source_value: &str) {
    run(
        session,
        &format!("CREATE TABLE {table} (id INT, v INT, st STRUCT<a: INT, b: INT>) USING iceberg"),
    )
    .await;
    run(
        session,
        &format!(
            "INSERT INTO {table} VALUES (0, 10, named_struct('a', 10, 'b', 20)), \
             (3, 30, named_struct('a', 30, 'b', 40))"
        ),
    )
    .await;
    run(
        session,
        &format!("CREATE TABLE {table}_src (id INT, v INT, st {source_type}) USING iceberg"),
    )
    .await;
    run(
        session,
        &format!("INSERT INTO {table}_src VALUES (1, 7, {source_value})"),
    )
    .await;
}

async fn rows(session: &ReparkSession, table: &str) -> Vec<(i32, i32, i32, i32)> {
    let batches = session
        .sql(&format!(
            "SELECT id, v, st.a, st.b FROM {table} ORDER BY id"
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

async fn refusals_for(
    column: &str,
    sources: &[(&str, &str, &str)],
    predicates: &[(&str, &str)],
) -> (Vec<String>, BTreeSet<String>) {
    let (session, _warehouse) = catalog_session().await;
    let mut failures = Vec::new();
    let mut texts = BTreeSet::new();
    for (source, source_type, source_value) in sources {
        for (predicate, where_sql) in predicates {
            for (form, value) in VALUES {
                let table = format!("ice.ns.{column}_{source}_{predicate}_{form}");
                seed(&session, &table, source_type, source_value).await;
                let value = value.replace("{c}", column).replace("{t}", &table);
                let sql = format!("UPDATE {table} SET {column} = {value}{where_sql}");
                match refusal(&session, &table, &sql).await {
                    Ok(text) => {
                        texts.insert(text);
                    }
                    Err(failure) => failures.push(failure),
                }
                let after = rows(&session, &table).await;
                if after != SEEDED {
                    failures.push(format!("{sql}: rows {after:?}"));
                }
            }
        }
    }
    (failures, texts)
}

const INT_SOURCE: [(&str, &str, &str); 1] = [(
    "int",
    "STRUCT<a: INT, b: INT>",
    "named_struct('a', 1, 'b', 2)",
)];

#[tokio::test]
async fn ruling_74_a_flat_int_update_from_a_subquery_refuses_on_the_spark_door() {
    let (failures, _) = refusals_for("v", &INT_SOURCE, &PREDICATES).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn ruling_90_a_struct_update_from_a_subquery_refuses_on_the_spark_door() {
    let (failures, _) = refusals_for("st", &STRUCT_SOURCES, &PREDICATES).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn ruling_95_an_update_from_a_subquery_that_matches_no_row_refuses_on_the_spark_door() {
    let (mut failures, _) = refusals_for("v", &INT_SOURCE, &NO_MATCH_PREDICATES).await;
    failures.extend(
        refusals_for("st", &STRUCT_SOURCES, &NO_MATCH_PREDICATES)
            .await
            .0,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn the_identity_update_from_a_subquery_still_stores_on_the_spark_door() {
    let (session, _warehouse) = catalog_session().await;
    for (column, (source, source_type, source_value), expected) in [
        ("v", STRUCT_SOURCES[1], (0, 7, 10, 20)),
        ("st", STRUCT_SOURCES[0], (0, 10, 1, 2)),
        ("st", STRUCT_SOURCES[1], (0, 10, 1, 2)),
    ] {
        let table = format!("ice.ns.identity_{column}_{source}");
        seed(&session, &table, source_type, source_value).await;
        run(
            &session,
            &format!(
                "UPDATE {table} SET {column} = (SELECT {column} FROM {table}_src) WHERE id = 0"
            ),
        )
        .await;
        assert_eq!(
            rows(&session, &table).await,
            vec![expected, SEEDED[1]],
            "{table}"
        );
    }
}

async fn printed(session: &ReparkSession, table: &str) -> String {
    let batches = session
        .sql(&format!("SELECT * FROM {table} ORDER BY id"))
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    pretty_format_batches(&batches).expect("format").to_string()
}

async fn every_column_type_refusals() -> (Vec<String>, BTreeSet<String>) {
    let (session, _warehouse) = catalog_session().await;
    let mut failures = Vec::new();
    let mut texts = BTreeSet::new();
    for (name, column_type, seed, source) in [
        ("string", "STRING", "'x'", "'y'"),
        (
            "bigint",
            "BIGINT",
            "CAST(10 AS BIGINT)",
            "CAST(7 AS BIGINT)",
        ),
        ("double", "DOUBLE", "1.5D", "2.5D"),
        ("decimal", "DECIMAL(10, 2)", "1.25BD", "2.50BD"),
        ("boolean", "BOOLEAN", "false", "true"),
        ("date", "DATE", "DATE'2020-01-01'", "DATE'2021-02-03'"),
        (
            "timestamp",
            "TIMESTAMP",
            "TIMESTAMP'2020-01-01 00:00:00'",
            "TIMESTAMP'2021-02-03 04:05:06'",
        ),
        (
            "timestamp_ntz",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_NTZ'2020-01-01 00:00:00'",
            "TIMESTAMP_NTZ'2021-02-03 04:05:06'",
        ),
        ("array", "ARRAY<INT>", "array(1, 2)", "array(3)"),
        ("map", "MAP<STRING, INT>", "map('k', 1)", "map('q', 2)"),
        (
            "nested_struct",
            "STRUCT<o: STRUCT<a: INT, b: INT>, n: INT>",
            "named_struct('o', named_struct('a', 1, 'b', 2), 'n', 3)",
            "named_struct('o', named_struct('a', 4, 'b', 5), 'n', 6)",
        ),
        (
            "array_of_struct",
            "ARRAY<STRUCT<a: INT, b: INT>>",
            "array(named_struct('a', 1, 'b', 2))",
            "array(named_struct('a', 4, 'b', 5))",
        ),
    ] {
        let table = format!("ice.ns.every_type_{name}");
        run(
            &session,
            &format!("CREATE TABLE {table} (id INT, c {column_type}) USING iceberg"),
        )
        .await;
        run(&session, &format!("INSERT INTO {table} VALUES (0, {seed})")).await;
        run(
            &session,
            &format!("CREATE TABLE {table}_src (id INT, c {column_type}) USING iceberg"),
        )
        .await;
        run(
            &session,
            &format!("INSERT INTO {table}_src VALUES (1, {source})"),
        )
        .await;
        let before = printed(&session, &table).await;
        let sql = format!("UPDATE {table} SET c = (SELECT c FROM {table}_src) WHERE true");
        match refusal(&session, &table, &sql).await {
            Ok(text) => {
                texts.insert(text);
            }
            Err(failure) => failures.push(format!("{name}: {failure}")),
        }
        if printed(&session, &table).await != before {
            failures.push(format!("{name}: rows changed"));
        }
    }
    (failures, texts)
}

#[tokio::test]
async fn ruling_90_a_an_update_from_a_subquery_refuses_for_every_column_type() {
    let (failures, _) = every_column_type_refusals().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn family_texts() -> BTreeSet<String> {
    let mut failures = Vec::new();
    let mut texts = BTreeSet::new();
    for (column, sources) in [("v", &INT_SOURCE[..]), ("st", &STRUCT_SOURCES[..])] {
        for predicates in [&PREDICATES[..], &NO_MATCH_PREDICATES[..]] {
            let (found, refused) = refusals_for(column, sources, predicates).await;
            failures.extend(found);
            texts.extend(refused);
        }
    }
    let (found, refused) = every_column_type_refusals().await;
    failures.extend(found);
    texts.extend(refused);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    texts
}

#[tokio::test]
async fn ruling_95_the_whole_refused_family_answers_one_text_on_the_spark_door() {
    let texts = family_texts().await;
    let stripped: BTreeSet<&str> = texts
        .iter()
        .map(|text| text.strip_prefix(DOOR_PREFIX).unwrap_or(text))
        .collect();
    assert_eq!(stripped, BTreeSet::from([REFUSAL]), "{texts:?}");
}

#[tokio::test]
async fn ruling_95_the_refusal_names_no_data_deleting_action_on_the_spark_door() {
    let texts = family_texts().await;
    assert!(!texts.is_empty());
    for text in &texts {
        let lowered = text.to_lowercase();
        for action in DATA_DELETING_ACTIONS {
            assert!(!lowered.contains(action), "{action} in {text}");
        }
    }
}
