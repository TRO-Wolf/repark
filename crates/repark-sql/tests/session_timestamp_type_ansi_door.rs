//! Q10 — ANSI-door cell of `spark.sql.timestampType`.

use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;

fn spark_extended_ntz() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .build()
        .expect("Spark-extended NTZ session")
}

fn ntz_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

const WALL_NOON: i64 = 1_718_452_800_000_000;

async fn collect_ts(
    session: &ReparkSession,
    dialect: &Arc<dyn SqlDialect>,
    sql: &str,
) -> (DataType, i64) {
    let batches = session
        .sql_with(dialect, sql)
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
async fn ansi_door_and_spark_door_agree_on_ntz_opt_in() {
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let spark: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let session = spark_extended_ntz();
    for sql in [
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
    ] {
        let (ansi_type, ansi_ticks) = collect_ts(&session, &ansi, sql).await;
        let (spark_type, spark_ticks) = collect_ts(&session, &spark, sql).await;
        assert_eq!(ansi_type, ntz_type(), "ANSI {sql}");
        assert_eq!(ansi_ticks, WALL_NOON, "ANSI {sql}");
        assert_eq!(
            (ansi_type, ansi_ticks),
            (spark_type, spark_ticks),
            "the door must not change the NTZ answer — {sql}"
        );
    }
}
