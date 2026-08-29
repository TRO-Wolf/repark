//! The ANSI-door cell of the session-timezone matrix.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;

const NEW_YORK: &str = "America/New_York";
const TOKYO: &str = "Asia/Tokyo";

/// The fixture uses `2024-06-15T12:00:00Z` and `2024-01-01T04:30:00Z`.
const INSTANTS: [&str; 2] = ["2024-06-15T12:00:00Z", "2024-01-01T04:30:00Z"];

/// The instants under test, as RFC-3339 strings so an expectation is checkable by eye.
fn utc_instants(rfc3339: &[&str]) -> ArrayRef {
    let text = StringArray::from(rfc3339.to_vec());
    cast(
        &text,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )
    .expect("well-formed RFC-3339 instant literals")
}

/// A Spark-extended session at `zone`, with the tz-aware instants registered as table `t`.
fn spark_extended_session_at(zone: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config(repark_core::SESSION_TIME_ZONE_KEY, zone)
        .build()
        .expect("a Spark-extended session at a real zone");
    register_instants(&session);
    session
}

fn register_instants(session: &ReparkSession) {
    let column = utc_instants(&INSTANTS);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )]));
    session
        .context()
        .register_batch(
            "t",
            RecordBatch::try_new(schema, vec![column]).expect("a one-column batch"),
        )
        .expect("register the instant table");
}

/// Run `sql` through `dialect` on `session`, returning `(Arrow types, i32 columns)`.
async fn int_columns_through(
    session: &ReparkSession,
    dialect: &Arc<dyn SqlDialect>,
    sql: &str,
) -> (Vec<DataType>, Vec<Vec<i32>>) {
    let batches = session
        .sql_with(dialect, sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let batch = &batches[0];
    let types = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let values = column.as_primitive::<Int32Type>();
            (0..values.len()).map(|row| values.value(row)).collect()
        })
        .collect();
    (types, columns)
}

const EXTRACT_SQL: &str = "SELECT year(ts) AS y, hour(ts) AS h FROM t ORDER BY ts";

/// Both doors use the session time zone and return the same values and Arrow types.
#[tokio::test]
async fn ansi_door_and_spark_door_agree_under_a_non_utc_session() {
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let spark: Arc<dyn SqlDialect> = Arc::new(SparkDialect);

    let new_york = spark_extended_session_at(NEW_YORK);
    let (ansi_types, ansi_columns) = int_columns_through(&new_york, &ansi, EXTRACT_SQL).await;
    let (spark_types, spark_columns) = int_columns_through(&new_york, &spark, EXTRACT_SQL).await;
    assert_eq!(ansi_types, vec![DataType::Int32; 2]);
    assert_eq!(
        ansi_columns,
        vec![vec![2023, 2024], vec![23, 8]],
        "2024-01-01T04:30Z is 2023-12-31 23:00-ish EST; 2024-06-15T12:00Z is 08:00 EDT"
    );
    assert_eq!(
        (ansi_types, ansi_columns),
        (spark_types, spark_columns),
        "the door must not change the answer — the session zone belongs to the session"
    );

    let tokyo = spark_extended_session_at(TOKYO);
    let (_, tokyo_columns) = int_columns_through(&tokyo, &ansi, EXTRACT_SQL).await;
    assert_eq!(
        tokyo_columns,
        vec![vec![2024, 2024], vec![13, 21]],
        "east of UTC, through the ANSI door, with no Spark dialect involved anywhere"
    );
}

/// A session without the extension uses stock DataFusion and reads the stored time zone.
#[tokio::test]
async fn a_native_session_without_the_spark_extension_reads_the_stored_zone() {
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let native = ReparkSession::builder()
        .with_sql_dialect(Arc::clone(&ansi))
        .config(repark_core::SESSION_TIME_ZONE_KEY, NEW_YORK)
        .build()
        .expect("a native session");
    register_instants(&native);
    assert_eq!(
        native.session_time_zone().id(),
        NEW_YORK,
        "the session still CARRIES the zone — it is the function layer that is absent"
    );
    let (_, columns) = int_columns_through(
        &native,
        &ansi,
        "SELECT date_part('hour', ts) AS h FROM t ORDER BY ts",
    )
    .await;
    assert_eq!(
        columns,
        vec![vec![4, 12]],
        "stock DataFusion extracts in the array's stored (UTC) zone; Spark semantics arrive with \
         the Spark extension, which is what installs the zone-aware extractors"
    );
}
