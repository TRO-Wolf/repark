use std::sync::Arc;

use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

struct Door {
    session: ReparkSession,
    _dir: TempDir,
}

async fn ansi_door() -> Door {
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
    Door { session, _dir: dir }
}

async fn run(door: &Door, sql: &str) {
    door.session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

async fn column_type(door: &Door, sql: &str, column: usize) -> String {
    let frame = door.session.sql(sql).await.expect("plan");
    let field = frame.schema().field(column).clone();
    format!("{} {}", field.name(), field.data_type())
}

#[tokio::test]
async fn nested_add_rename_and_drop_on_the_ansi_door() {
    let door = ansi_door().await;
    run(&door, "CREATE SCHEMA IF NOT EXISTS ice.sales").await;
    run(
        &door,
        "CREATE TABLE ice.sales.nested (id INT, s STRUCT<a INT, b VARCHAR>)",
    )
    .await;
    run(&door, "ALTER TABLE ice.sales.nested ADD COLUMN s.c BIGINT").await;
    run(
        &door,
        "ALTER TABLE ice.sales.nested RENAME COLUMN s.a TO a2",
    )
    .await;
    run(&door, "ALTER TABLE ice.sales.nested DROP COLUMN s.b").await;
    let described = column_type(&door, "SELECT s FROM ice.sales.nested", 0).await;
    assert!(
        described.contains("\"a2\": Int32") && described.contains("\"c\": Int64"),
        "{described}"
    );
    assert!(!described.contains("\"b\""), "{described}");
    let refused = door
        .session
        .sql("ALTER TABLE ice.sales.nested ADD COLUMN s.r INT NOT NULL")
        .await
        .expect_err("a required nested child without a default must refuse");
    assert!(
        refused
            .to_string()
            .contains("Incompatible change: cannot add required column"),
        "{refused}"
    );
}
