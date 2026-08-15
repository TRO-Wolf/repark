//! Q10 — `spark.sql.timestampType` at the Spark door + native `DataFrame` API.
//!
//! Default `TIMESTAMP_LTZ` is today's type-resolution (existing suites are the
//! default-mode gate and are not edited). This file pins the NTZ opt-in: bare
//! `TIMESTAMP` literals / casts / DDL resolve to naive µs / Iceberg `timestamp`,
//! value AND Arrow type. `to_timestamp` / `current_timestamp` stay LTZ.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion::logical_expr::{Cast, Expr, col};
use iceberg::Catalog;
use iceberg::spec::PrimitiveType;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{ReparkSession, SqlDialect};
use repark_functions::timestamp_type::SPARK_SQL_TIMESTAMP_TYPE_KEY;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

fn spark_session_with_timestamp_type(timestamp_type: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config(SPARK_SQL_TIMESTAMP_TYPE_KEY, timestamp_type)
        .build()
        .unwrap_or_else(|error| panic!("session with {timestamp_type}: {error}"))
}

fn ntz_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn ltz_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// 2024-06-15 12:00:00 as naive/UTC-wall micros.
const WALL_NOON: i64 = 1_718_452_800_000_000;

async fn collect_ts(session: &ReparkSession, sql: &str) -> (DataType, i64) {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let field_type = batches[0].schema().field(0).data_type().clone();
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    (field_type, ticks)
}

#[tokio::test]
async fn default_ltz_bare_timestamp_stays_microsecond_utc() {
    let session = spark_session_with_timestamp_type("TIMESTAMP_LTZ");
    for sql in [
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
    ] {
        let (field_type, ticks) = collect_ts(&session, sql).await;
        assert_eq!(field_type, ltz_type(), "{sql}");
        assert_eq!(ticks, WALL_NOON, "{sql}");
    }
}

#[tokio::test]
async fn ntz_opt_in_bare_timestamp_literal_and_cast_are_naive() {
    let session = spark_session_with_timestamp_type("TIMESTAMP_NTZ");
    for sql in [
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
    ] {
        let (field_type, ticks) = collect_ts(&session, sql).await;
        assert_eq!(field_type, ntz_type(), "{sql}");
        assert_eq!(ticks, WALL_NOON, "{sql}");
    }
}

#[tokio::test]
async fn ntz_opt_in_does_not_retarget_to_timestamp() {
    let session = spark_session_with_timestamp_type("TIMESTAMP_NTZ");
    let (field_type, _) =
        collect_ts(&session, "SELECT to_timestamp('2024-06-15 12:00:00') AS ts").await;
    assert_eq!(field_type, ltz_type());
}

#[tokio::test]
async fn invalid_timestamp_type_fails_loud_naming_both_values() {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let error = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config(SPARK_SQL_TIMESTAMP_TYPE_KEY, "TIMESTAMP")
        .build()
        .expect_err("invalid timestampType must refuse")
        .to_string();
    assert!(
        error.contains(SPARK_SQL_TIMESTAMP_TYPE_KEY),
        "must name the key: {error}"
    );
    assert!(
        error.contains("TIMESTAMP_LTZ") && error.contains("TIMESTAMP_NTZ"),
        "must name both legal values: {error}"
    );
}

/// Native `DataFrame` API: a standalone `Expr::Cast` of a string to the SQL `TIMESTAMP`
/// target — the shape `Column.cast("timestamp")` / `F.expr("CAST(… AS TIMESTAMP)")`
/// crosses as — on an NTZ session.
#[tokio::test]
async fn native_dataframe_api_cast_as_timestamp_follows_ntz() {
    let session = spark_session_with_timestamp_type("TIMESTAMP_NTZ");
    let frame = session
        .context()
        .sql("SELECT '2024-06-15 12:00:00' AS s")
        .await
        .expect("source")
        .select(vec![
            Expr::Cast(Cast::new(Box::new(col("s")), ntz_type())).alias("ts"),
        ])
        .expect("project");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(batches[0].schema().field(0).data_type(), &ntz_type());
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, WALL_NOON);
}

async fn create_bare_timestamp_table(
    timestamp_type: &str,
) -> (TempDir, Arc<dyn Catalog>, TableIdent) {
    let warehouse = TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap().to_string();
    let session = spark_session_with_timestamp_type(timestamp_type);
    let catalog = repark_iceberg::catalog::memory_catalog(&warehouse_path)
        .await
        .unwrap();
    session
        .register_iceberg_catalog("ice", Arc::clone(&catalog))
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse_path}/sales"))]),
        )
        .await
        .unwrap();
    session
        .sql("CREATE TABLE ice.sales.typed (ts TIMESTAMP) USING iceberg")
        .await
        .expect("CREATE");
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "typed".to_string(),
    );
    (warehouse, catalog, ident)
}

#[tokio::test]
async fn ntz_opt_in_ddl_bare_timestamp_stores_iceberg_timestamp() {
    let (_warehouse, catalog, ident) = create_bare_timestamp_table("TIMESTAMP_NTZ").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let field_type = table.metadata().current_schema().as_struct().fields()[0]
        .field_type
        .as_ref();
    assert!(
        matches!(
            field_type,
            iceberg::spec::Type::Primitive(PrimitiveType::Timestamp)
        ),
        "NTZ default TIMESTAMP must store Iceberg timestamp, got {field_type}"
    );
}

#[tokio::test]
async fn default_ltz_ddl_bare_timestamp_still_stores_timestamptz() {
    let (_warehouse, catalog, ident) = create_bare_timestamp_table("TIMESTAMP_LTZ").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let field_type = table.metadata().current_schema().as_struct().fields()[0]
        .field_type
        .as_ref();
    assert!(
        matches!(
            field_type,
            iceberg::spec::Type::Primitive(PrimitiveType::Timestamptz)
        ),
        "LTZ default TIMESTAMP must stay timestamptz, got {field_type}"
    );
}
