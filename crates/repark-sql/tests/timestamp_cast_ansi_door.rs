//! The ANSI-door cell of the `CAST(TIMESTAMP AS <numeric>)` epoch-seconds matrix.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;

const NEW_YORK: &str = "America/New_York";

/// The instants include a whole half-hour before 1970 and a negative fractional second.
const INSTANTS: [&str; 2] = ["1969-12-31T23:30:00Z", "1969-12-31T23:59:59.5Z"];
const EXPECTED_SECONDS: [Option<i64>; 3] = [Some(-1800), Some(-1), None];

/// Register the instants and a trailing NULL as a nanosecond-backed, time-zone-aware `ts` column.
fn register_instants(session: &ReparkSession) {
    let values: Vec<Option<&str>> = INSTANTS
        .iter()
        .map(|value| Some(*value))
        .chain(std::iter::once(None))
        .collect();
    let column = cast(
        &StringArray::from(values),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    )
    .expect("well-formed RFC-3339 instant literals");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![column]).expect("a one-column batch");
    session
        .context()
        .register_batch("t", batch)
        .expect("register the instant table");
}

/// The `(Arrow type, values)` of `SELECT CAST(ts AS BIGINT) FROM t` through `dialect`.
async fn epoch_seconds_through(
    session: &ReparkSession,
    dialect: &Arc<dyn SqlDialect>,
) -> (DataType, Vec<Option<i64>>) {
    let batches = session
        .sql_with(dialect, "SELECT CAST(ts AS BIGINT) AS epoch_value FROM t")
        .await
        .unwrap_or_else(|error| panic!("plan through the door: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect through the door: {error}"));
    let batch = &batches[0];
    let field_type = batch.schema().field(0).data_type().clone();
    let column = batch.column(0).as_primitive::<Int64Type>();
    let values = (0..column.len())
        .map(|row| column.is_valid(row).then(|| column.value(row)))
        .collect();
    (field_type, values)
}

/// One Spark-extended session, both doors, the same seconds — value AND Arrow type.
#[tokio::test]
async fn both_doors_of_one_spark_extended_session_scale_to_epoch_seconds() {
    let spark: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::clone(&spark))
        .config(repark_core::SESSION_TIME_ZONE_KEY, NEW_YORK)
        .build()
        .expect("a Spark-extended session");
    register_instants(&session);

    let (spark_type, spark_values) = epoch_seconds_through(&session, &spark).await;
    let (ansi_type, ansi_values) = epoch_seconds_through(&session, &ansi).await;

    assert_eq!(spark_type, DataType::Int64);
    assert_eq!(ansi_type, spark_type, "the door must not change the type");
    assert_eq!(spark_values, EXPECTED_SECONDS.to_vec());
    assert_eq!(
        ansi_values, spark_values,
        "the ANSI door must not be the cell where the raw nanosecond tick survives"
    );
}

/// The honest negative: a BARE session (no Spark extension) keeps DataFusion's raw tick.
#[tokio::test]
async fn a_bare_session_keeps_the_raw_nanosecond_tick() {
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::clone(&ansi))
        .config(repark_core::SESSION_TIME_ZONE_KEY, NEW_YORK)
        .build()
        .expect("a bare session");
    register_instants(&session);

    let (field_type, values) = epoch_seconds_through(&session, &ansi).await;
    assert_eq!(field_type, DataType::Int64);
    assert_eq!(
        values,
        vec![Some(-1_800_000_000_000), Some(-500_000_000), None],
        "stock DataFusion reinterprets the nanosecond tick — the pre-fix Spark-door answer"
    );
}
