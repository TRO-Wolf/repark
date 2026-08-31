//! F-Y10-1 cross-door integer overflow pins.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array};
use datafusion::arrow::datatypes::DataType;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

struct Door {
    session: ReparkSession,
    _dir: TempDir,
}

async fn native_ansi_door() -> Door {
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

async fn spark_extended_door(ansi_enabled: bool) -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config(
            "spark.sql.ansi.enabled",
            if ansi_enabled { "true" } else { "false" },
        )
        .build()
        .expect("spark session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");
    Door { session, _dir: dir }
}

async fn collect_error(session: &ReparkSession, sql: &str) -> String {
    match session.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected `{sql}` to fail, but it produced rows"),
        },
    }
}

async fn int32_scalar(session: &ReparkSession, sql: &str) -> (DataType, bool, Option<i32>) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed ({sql}): {error}"));
    let schema = frame.schema().as_arrow().clone();
    let field = schema.field(0);
    let data_type = field.data_type().clone();
    let nullable = field.is_nullable();
    let batches = frame.collect().await.expect("collect");
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32Array");
    let value = array.is_valid(0).then(|| array.value(0));
    (data_type, nullable, value)
}

const OVERFLOW_SQL: &str = "SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS v";

/// pins: f-y10-1-int-overflow/C-003
#[tokio::test]
async fn cross_door_int32_add_overflow_raises_on_both_doors_by_default() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door(true).await;
    let ansi_error = collect_error(&ansi.session, OVERFLOW_SQL).await;
    let spark_error = collect_error(&spark.session, OVERFLOW_SQL).await;
    assert!(
        ansi_error.contains("ARITHMETIC_OVERFLOW"),
        "ANSI door raises on INT overflow, got: {ansi_error}"
    );
    assert!(
        spark_error.contains("ARITHMETIC_OVERFLOW"),
        "Spark door (ansi default true) raises on INT overflow, got: {spark_error}"
    );
}

/// pins: f-y10-1-int-overflow/C-003
#[tokio::test]
async fn cross_door_int32_add_overflow_wraps_on_spark_ansi_false_raises_on_ansi() {
    let ansi = native_ansi_door().await;
    let spark = spark_extended_door(false).await;
    let ansi_error = collect_error(&ansi.session, OVERFLOW_SQL).await;
    assert!(
        ansi_error.contains("ARITHMETIC_OVERFLOW"),
        "ANSI door has no wrap knob, got: {ansi_error}"
    );
    let spark_pin = int32_scalar(&spark.session, OVERFLOW_SQL).await;
    assert_eq!(
        spark_pin,
        (DataType::Int32, false, Some(-2_147_483_648)),
        "Spark ansi=false wraps INT add"
    );
}
