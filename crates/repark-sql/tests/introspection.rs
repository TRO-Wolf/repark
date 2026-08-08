//! Q8 INTROSPECTION, delivered through the ANSI door (design §2 Q8: "delegate — DF
//! `information_schema` + stock `SHOW TABLES` / `DESCRIBE t`").
//!
//! PR-5's R2 day-1 spike found there was nothing to delegate TO: `ReparkSession` could not enable
//! `information_schema` at all, because the builder's `.config(k, v)` map never reached
//! `SessionConfig`. That was filed as a repark-core gap — exactly as Q8 instructs ("gaps are
//! core/fork fixes, not door parsers") — and PR-6 fixes it in
//! `repark_core::session::apply_datafusion_config_keys`. This file is the DOOR half of that
//! delivery: with the conf set on the builder, introspection works through `AnsiDialect` with no
//! door-side parser at all.
//!
//! Native profile throughout — no `SessionExtension`. Q8 is a delegation claim, and delegation is
//! stock DataFusion; an extended session would prove something else.
//!
//! AWS-free by construction (in-memory Iceberg catalog over a `TempDir` warehouse).

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::DataType;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

/// A session whose default dialect is the ANSI door, with `information_schema` enabled the
/// PRODUCT way — through the builder config map (the surface the R2 fix opened).
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

/// Create `ice.sales.orders` through this door, so the enumeration below is enumerating a table
/// the ANSI door itself made (not a fixture smuggled in another way).
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

/// Collect one `Utf8` column into a sorted `Vec<String>`, asserting the Arrow TYPE as well as the
/// values (docs/testing.md: value AND type, never `show` alone).
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

/// The Q8 delivery: a REGISTERED ICEBERG CATALOG enumerates through `information_schema` — its
/// namespace in `schemata`, its table in `tables` — with the ANSI door installed as the session
/// dialect. This is the enumeration verification the design asks for, on the product path.
///
/// Honest result recorded here rather than in prose: enumeration DOES work through the fork's
/// providers. The R2 spike had already proved the machinery on a raw `SessionContext`; the only
/// thing missing was the conf, and the conf is now reachable.
///
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

/// Stock `SHOW TABLES` and `DESCRIBE t` reach DataFusion THROUGH this door — no ANSI-side
/// handler, which is the whole content of the Q8 "delegate" ruling. Both are asserted on the
/// Arrow path.
///
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
        // Stock DataFusion's SHOW TABLES shape: table_catalog, table_schema, table_name,
        // table_type — all Utf8.
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
/// This is what makes the two rows above attributable to the R2 fix rather than to a default.
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

/// The HONEST caveat, pinned: the fork's `$`-suffixed metadata tables enumerate alongside the
/// real table. Trino hides these from `SHOW TABLES`; we do not, today. Whether
/// `repark_iceberg::catalog`'s `SchemaProvider::table_names` should filter them is the OPEN
/// product question the R2 spike raised and `task/p2g-ansi-m2-ledger.md` carries forward — it is
/// a fork/core decision, not a door parser, so Q8's door row is scoped to what is proven above
/// and this row states the rest out loud. Filtering them later flips this red on purpose.
#[tokio::test]
async fn metadata_tables_currently_enumerate_alongside_the_real_table() {
    let warehouse_dir = TempDir::new().expect("warehouse");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = introspective_ansi_session(&warehouse).await;
    seed(&session, &warehouse).await;

    let batches = session
        .sql(
            "SELECT count(*) AS n FROM information_schema.tables \
             WHERE table_catalog = 'ice' AND table_schema = 'sales' \
             AND table_name LIKE 'orders$%'",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64");
    assert!(
        !counts.is_null(0) && counts.value(0) > 0,
        "metadata tables currently enumerate (open product question, see the P2G ledger); got {:?}",
        counts.value(0)
    );
}
