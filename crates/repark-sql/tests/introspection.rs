//! Q8 introspection is delegated through the ANSI door to stock DataFusion.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::DataType;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

/// A session whose default dialect is the ANSI door, with `information_schema` enabled through the builder.
async fn introspective_ansi_session(warehouse: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .config("datafusion.catalog.information_schema", "true")
        .build()
        .expect("session must build");
    session
        .register_memory_catalog("ice", warehouse)
        .await
        .expect("catalog must register");
    session
}

/// Create `ice.sales.orders` through this door so enumeration observes a real table.
async fn seed(session: &ReparkSession, warehouse: &str) {
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");
    session
        .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label")
        .await
        .expect("CTAS");
}

/// Collect one `Utf8` column into a sorted `Vec<String>`, asserting the Arrow type as well as values.
async fn utf8_column(session: &ReparkSession, sql: &str) -> Vec<String> {
    let frame = session.sql(sql).await.expect("plan");
    let batches = frame.collect().await.expect("collect");
    let mut out = Vec::new();
    for batch in &batches {
        assert_eq!(
            batch.column(0).data_type(),
            &DataType::Utf8,
            "the projected name column must be Utf8"
        );
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8");
        for row in 0..batch.num_rows() {
            out.push(column.value(row).to_string());
        }
    }
    out.sort();
    out
}

/// A registered Iceberg catalog enumerates through `information_schema`.
/// Mutation: drop `.config("datafusion.catalog.information_schema", …)` → both queries fail.
#[tokio::test]
async fn information_schema_enumerates_an_iceberg_catalog_through_the_ansi_door() {
    let warehouse_dir = TempDir::new().expect("warehouse");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = introspective_ansi_session(&warehouse).await;
    seed(&session, &warehouse).await;

    let schemata = utf8_column(
        &session,
        "SELECT schema_name FROM information_schema.schemata \
         WHERE catalog_name = 'ice' AND schema_name = 'sales'",
    )
    .await;
    assert_eq!(
        schemata,
        vec!["sales".to_string()],
        "the door-created namespace must enumerate in information_schema.schemata"
    );

    let tables = utf8_column(
        &session,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_catalog = 'ice' AND table_schema = 'sales' AND table_name = 'orders'",
    )
    .await;
    assert_eq!(
        tables,
        vec!["orders".to_string()],
        "the door-created table must enumerate in information_schema.tables"
    );

    // Columns too — the half `DESCRIBE` is built on.
    let columns = utf8_column(
        &session,
        "SELECT column_name FROM information_schema.columns \
         WHERE table_catalog = 'ice' AND table_schema = 'sales' AND table_name = 'orders'",
    )
    .await;
    assert_eq!(columns, vec!["id".to_string(), "label".to_string()]);
}

/// Stock `SHOW TABLES` and `DESCRIBE t` reach DataFusion through this door.
/// Mutation: adding a router intercept for either statement that does not delegate turns this red
/// (the row counts / column names would stop matching stock DataFusion's shape).
#[tokio::test]
async fn show_tables_and_describe_delegate_through_the_ansi_door() {
    let warehouse_dir = TempDir::new().expect("warehouse");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = introspective_ansi_session(&warehouse).await;
    seed(&session, &warehouse).await;

    let shown = session
        .sql("SHOW TABLES")
        .await
        .expect("SHOW TABLES must plan through the door")
        .collect()
        .await
        .expect("SHOW TABLES must execute");
    let mut found = false;
    for batch in &shown {
        // Stock DataFusion places the table name in the third column.
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("table_name is Utf8");
        for row in 0..batch.num_rows() {
            if names.value(row) == "orders" {
                found = true;
            }
        }
    }
    assert!(found, "SHOW TABLES must list the door-created table");

    let described = session
        .sql("DESCRIBE ice.sales.orders")
        .await
        .expect("DESCRIBE must plan through the door")
        .collect()
        .await
        .expect("DESCRIBE must execute");
    let mut columns: Vec<String> = Vec::new();
    for batch in &described {
        assert_eq!(
            batch.column(0).data_type(),
            &DataType::Utf8,
            "DESCRIBE's column_name is Utf8"
        );
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8");
        for row in 0..batch.num_rows() {
            columns.push(names.value(row).to_string());
        }
    }
    columns.sort();
    assert_eq!(
        columns,
        vec!["id".to_string(), "label".to_string()],
        "DESCRIBE must report the door-created columns"
    );
    assert_eq!(
        described.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2
    );
}

/// The negative half — WITHOUT the conf, the same door refuses with DataFusion's own message.
#[tokio::test]
async fn introspection_still_refuses_without_the_information_schema_conf() {
    let warehouse_dir = TempDir::new().expect("warehouse");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");

    let error = session
        .sql("SHOW TABLES")
        .await
        .expect_err("SHOW TABLES must refuse without the conf")
        .to_string();
    assert!(
        error.contains("information_schema"),
        "the refusal must name the conf that enables it: {error}"
    );
}

/// Time-travel pinned relations must not survive the statement or appear in enumeration.
#[tokio::test]
async fn time_travel_pinned_views_do_not_leak_into_the_introspection_surface() {
    let warehouse_dir = TempDir::new().expect("warehouse");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = introspective_ansi_session(&warehouse).await;
    seed(&session, &warehouse).await;

    let history = session
        .testing_list_snapshots("ice.sales.orders")
        .await
        .expect("snapshot history");
    let snapshot = history.first().expect("one snapshot after CTAS").0;

    for _ in 0..3 {
        let frame = session
            .sql(&format!(
                "SELECT id, label FROM ice.sales.orders FOR VERSION AS OF {snapshot}"
            ))
            .await
            .expect("pinned read must plan");
        // Collected after `execute` returned — i.e. after the temp name was released.
        let batches = frame.collect().await.expect("pinned read must execute");
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "the pinned read must still return its row once the ephemeral name is gone"
        );
        assert_eq!(batches[0].column(0).data_type(), &DataType::Int64);
    }

    let leftover = utf8_column(
        &session,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_name LIKE '__repark_ansi_tt%'",
    )
    .await;
    assert!(
        leftover.is_empty(),
        "time-travel temp views must be released, not left on the session: {leftover:?}"
    );
    // The core-minted half (`repark_core::read_table_at`) is also checked.
    let core_leftover = utf8_column(
        &session,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_name LIKE '__repark_tt%'",
    )
    .await;
    assert!(
        core_leftover.is_empty(),
        "the core half of each pinned relation must be released too, not left on the session: \
         {core_leftover:?}"
    );
}

/// Metadata projections hide time-travel relations from enumeration while keeping them queryable.
/// Mutation: drop the `.filter(…)` in `MetadataProjectionSchemaProvider::table_names` → the two
/// pins: rp-1-fork-repin/C-005
#[tokio::test]
async fn metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_ansi_door() {
    let warehouse_dir = TempDir::new().expect("warehouse");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = introspective_ansi_session(&warehouse).await;
    seed(&session, &warehouse).await;

    // 1. `information_schema.tables` — the real table, and nothing synthesized beside it.
    let listed = utf8_column(
        &session,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_catalog = 'ice' AND table_schema = 'sales'",
    )
    .await;
    assert_eq!(
        listed,
        vec!["orders".to_string()],
        "the namespace must enumerate its one real table, not the fork's synthesized names"
    );

    // 2. The twin path: `SHOW TABLES` (rewritten onto the same view) must agree.
    let shown = session
        .sql("SHOW TABLES")
        .await
        .expect("SHOW TABLES must plan")
        .collect()
        .await
        .expect("SHOW TABLES must execute");
    let mut dollar_names: Vec<String> = Vec::new();
    for batch in &shown {
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("table_name is Utf8");
        for row in 0..batch.num_rows() {
            if names.value(row).contains('$') {
                dollar_names.push(names.value(row).to_string());
            }
        }
    }
    assert!(
        dollar_names.is_empty(),
        "SHOW TABLES must not list metadata tables either: {dollar_names:?}"
    );

    // 3. Hidden, not removed: the name still resolves and still returns rows through this door.
    let snapshots = session
        .sql("SELECT count(*) AS n FROM ice.sales.\"orders$snapshots\"")
        .await
        .expect("a hidden metadata table must still plan")
        .collect()
        .await
        .expect("a hidden metadata table must still execute");
    let counts = snapshots[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64");
    assert!(
        !counts.is_null(0) && counts.value(0) > 0,
        "the CTAS above committed a snapshot, so the hidden metadata table must return rows; \
         got {:?}",
        counts.value(0)
    );
}
