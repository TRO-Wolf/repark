//! Reachability pins install `AnsiDialect` through `ReparkSessionBuilder` and drive DDL/DML via `session.sql`.
//! A direct router test cannot prove installation. Native sessions keep stock DataFusion semantics.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

/// A session whose DEFAULT dialect is the ANSI door, with one in-memory Iceberg catalog.
async fn ansi_session(warehouse: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("session must build");
    session
        .register_memory_catalog("ice", warehouse)
        .await
        .expect("catalog must register");
    session
}

/// End to end through `ReparkSession::sql`, schema DDL, CTAS, and a typed read reach the catalog.
/// Mutation: dropping `.with_sql_dialect(...)` from the builder turns this RED — the session
/// default becomes `DataFusionDialect`, whose CTAS makes a session-local `MemTable` and never
/// creates `ice.sales.orders`, so the catalog-visible read fails.
#[tokio::test]
async fn ansi_dialect_on_a_repark_session_runs_the_door() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;

    // The ANSI spelling — `CREATE SCHEMA … WITH (location = …)`, not Spark's NAMESPACE form.
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run through the door");
    session
        .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label")
        .await
        .expect("CTAS must run through the door");
    session
        .sql("INSERT INTO ice.sales.orders VALUES (2, 'b')")
        .await
        .expect("INSERT must run through the door")
        .collect()
        .await
        .expect("collect");

    let frame = session
        .sql("SELECT id, label FROM ice.sales.orders ORDER BY id")
        .await
        .expect("read must run through the door");
    let schema = frame.schema().as_arrow().clone();
    let batches = frame.collect().await.expect("collect");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "id type");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "label type");

    let mut pairs: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 id");
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 label");
        for row in 0..batch.num_rows() {
            pairs.push((ids.value(row), labels.value(row).to_string()));
        }
    }
    assert_eq!(
        pairs,
        vec![(1, "a".to_string()), (2, "b".to_string())],
        "the door must have created and written a real Iceberg table"
    );
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2,
        "row count"
    );
}

/// The session-installed door exposes its refusal errors to the caller.
#[tokio::test]
async fn ansi_door_refusals_surface_through_the_session() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;

    let err = session
        .sql("CREATE TABLE unqualified AS SELECT 1 AS id")
        .await
        .expect_err("an unregistered CTAS target must refuse")
        .to_string();
    assert!(
        err.contains("qualify") || err.contains("registered"),
        "the Q15 refusal must survive the session boundary: {err}"
    );

    // …and nothing was created anywhere — no session-local `MemTable` consolation prize.
    let err = session
        .sql("SELECT * FROM unqualified")
        .await
        .expect_err("nothing may have been created");
    assert!(
        err.to_string().contains("unqualified"),
        "no session-local table may exist: {err}"
    );
}

/// A11: ANSI column-def `CREATE TABLE` refuses nanosecond-precision timestamps at DDL time.
fn assert_ansi_ns_create_refusal(err: &str, column: &str) {
    assert!(
        err.contains(&format!("`{column}`")),
        "must name column `{column}`: {err}"
    );
    assert!(
        err.contains("nanosecond") && err.contains("(9)"),
        "must name nanosecond precision 9: {err}"
    );
    assert!(
        err.contains("microsecond") && err.contains("TIMESTAMP(6)"),
        "must name the supported precision: {err}"
    );
    assert!(
        !err.contains("not supported until v3"),
        "must be the DDL-time refuse, not the Iceberg v2 write-path residual: {err}"
    );
}

#[tokio::test]
async fn ansi_column_def_nanosecond_timestamp_shapes_refuse() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");

    // Named shapes that DataFusion types as `timestamp[ns]` (planner.rs:743-764).
    let shapes: &[(&str, &str, &str)] = &[
        ("bare", "ts", "ts TIMESTAMP"),
        ("explicit_9", "event_at", "event_at TIMESTAMP(9)"),
        (
            "with_time_zone",
            "when_tz",
            "when_tz TIMESTAMP WITH TIME ZONE",
        ),
        (
            "explicit_9_with_time_zone",
            "when_tz9",
            "when_tz9 TIMESTAMP(9) WITH TIME ZONE",
        ),
        (
            "without_time_zone",
            "when_ntz",
            "when_ntz TIMESTAMP WITHOUT TIME ZONE",
        ),
        (
            "explicit_9_without_time_zone",
            "when_ntz9",
            "when_ntz9 TIMESTAMP(9) WITHOUT TIME ZONE",
        ),
    ];

    for (shape, column, decl) in shapes {
        let table = format!("ts_{shape}");
        let err = match session
            .sql(&format!("CREATE TABLE ice.sales.{table} ({decl})"))
            .await
        {
            Ok(_) => panic!("shape `{shape}` must refuse"),
            Err(err) => err.to_string(),
        };
        assert_ansi_ns_create_refusal(&err, column);

        let leftover = session
            .sql(&format!("SELECT * FROM ice.sales.{table}"))
            .await;
        assert!(
            leftover.is_err(),
            "shape `{shape}` must not leave a table: {table}"
        );
    }

    // Mixed list: the ns column is named; the µs sibling is not the refuse subject.
    let err = session
        .sql("CREATE TABLE ice.sales.mixed (ok TIMESTAMP(6), late TIMESTAMP(9), label VARCHAR)")
        .await
        .expect_err("mixed ns column must refuse")
        .to_string();
    assert_ansi_ns_create_refusal(&err, "late");
    assert!(
        !err.contains("`ok`"),
        "must name the ns column, not the µs sibling: {err}"
    );
}

/// Positive control: `TIMESTAMP(6)` is microseconds and stays a successful CREATE.
#[tokio::test]
async fn ansi_column_def_timestamp_6_create_is_unchanged() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");

    session
        .sql("CREATE TABLE ice.sales.ts_us (event_at TIMESTAMP(6), label VARCHAR)")
        .await
        .expect("TIMESTAMP(6) CREATE must succeed");
    session
        .sql("CREATE TABLE ice.sales.ts_us_ntz (event_at TIMESTAMP(6) WITHOUT TIME ZONE)")
        .await
        .expect("TIMESTAMP(6) WITHOUT TIME ZONE CREATE must succeed");

    let frame = session
        .sql("SELECT event_at, label FROM ice.sales.ts_us")
        .await
        .expect("read TIMESTAMP(6) table");
    let schema = frame.schema().as_arrow().clone();
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP(6) must stay microsecond"
    );
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "VARCHAR");
    assert_eq!(
        frame
            .collect()
            .await
            .expect("collect")
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0,
        "column-def CREATE writes no rows"
    );

    let frame = session
        .sql("SELECT event_at FROM ice.sales.ts_us_ntz")
        .await
        .expect("read TIMESTAMP(6) WITHOUT TIME ZONE table");
    assert_eq!(
        frame.schema().as_arrow().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP(6) WITHOUT TIME ZONE must stay microsecond"
    );
}
