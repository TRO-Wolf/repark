use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

async fn door() -> (ReparkSession, TempDir) {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");
    (session, dir)
}

async fn answer(session: &ReparkSession, sql: &str) -> String {
    match session.sql(sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(batches) => pretty_format_batches(&batches).expect("render").to_string(),
            Err(error) => format!("ERR {error}"),
        },
        Err(error) => format!("ERR {error}"),
    }
}

#[tokio::test]
async fn the_spark_timestamp_ltz_spelling_refuses_on_the_ansi_door() {
    let (session, _dir) = door().await;
    assert_eq!(
        answer(
            &session,
            "CREATE TABLE ice.sales.l2 (id INT, c TIMESTAMP_LTZ)"
        )
        .await,
        "ERR Error during planning: CREATE TABLE: could not resolve the declared column types \
         (This feature is not implemented: Unsupported SQL type TIMESTAMP_LTZ)"
    );
    assert!(
        answer(&session, "SELECT * FROM ice.sales.l2")
            .await
            .starts_with("ERR ")
    );
}

#[tokio::test]
async fn the_ansi_empty_map_is_map_of_two_empty_arrays_and_the_spark_spelling_refuses() {
    let (session, _dir) = door().await;
    answer(
        &session,
        "CREATE TABLE ice.sales.m (id INT, c MAP(VARCHAR, INTEGER))",
    )
    .await;
    assert_eq!(
        answer(
            &session,
            "INSERT INTO ice.sales.m VALUES (0, MAP(ARRAY['k'], ARRAY[1])), \
             (1, MAP(ARRAY[], ARRAY[]))"
        )
        .await,
        "+-------+\n| count |\n+-------+\n| 2     |\n+-------+"
    );
    assert_eq!(
        answer(&session, "SELECT id, c FROM ice.sales.m ORDER BY id").await,
        "+----+--------+\n| id | c      |\n+----+--------+\n| 0  | {k: 1} |\n| 1  | {}     |\n+----+--------+"
    );
    assert_eq!(
        answer(&session, "SELECT MAP() AS m").await,
        "ERR Error during planning: Function 'map' expected at least one argument but received 0"
    );
}
