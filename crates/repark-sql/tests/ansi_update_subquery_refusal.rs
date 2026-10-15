use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const REFUSAL: &str = "UPDATE with SET column = (SELECT ...) is not supported on this \
     statement: RePark cannot write the subquery value here, so it refuses before any write.";

const SEEDED: [(i32, i32, i32, i32); 2] = [(0, 10, 10, 20), (3, 30, 30, 40)];

const PREDICATES: [(&str, &str); 3] = [
    ("where_true", " WHERE true"),
    ("where_in_list", " WHERE id IN (0, 3)"),
    ("no_where", ""),
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

async fn refusals_for(column: &str, sources: &[(&str, &str)]) -> Vec<String> {
    let (session, _warehouse) = ansi_session().await;
    let mut failures = Vec::new();
    for (source, source_value) in sources {
        for (predicate, where_sql) in PREDICATES {
            for (form, value) in VALUES {
                let table = format!("ice.ns.{column}_{source}_{predicate}_{form}");
                seed(&session, &table, source_value).await;
                let value = value.replace("{c}", column).replace("{t}", &table);
                let sql = format!("UPDATE {table} SET {column} = {value}{where_sql}");
                let outcome = attempt(&session, &sql).await;
                let after = rows(&session, &table).await;
                let refused = outcome
                    .as_ref()
                    .err()
                    .is_some_and(|text| text.contains(REFUSAL));
                if !refused || after != SEEDED {
                    failures.push(format!("{sql}: {outcome:?}, rows {after:?}"));
                }
            }
        }
    }
    failures
}

#[tokio::test]
async fn ruling_74_a_flat_int_update_from_a_subquery_refuses_on_the_ansi_door() {
    let failures = refusals_for("v", &[("int", STRUCT_SOURCES[1].1)]).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn ruling_90_a_struct_update_from_a_subquery_refuses_on_the_ansi_door() {
    let failures = refusals_for("st", &STRUCT_SOURCES).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
