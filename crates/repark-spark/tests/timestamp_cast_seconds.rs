//! Pins Spark's `CAST` epoch-seconds semantics through a real Spark-door session.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, TimeUnit};
use datafusion::logical_expr::{Cast, Expr};
use datafusion::prelude::col;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};

/// These two zones must produce the same result because this cast is zone-independent.
const NEW_YORK: &str = "America/New_York";
const TOKYO: &str = "Asia/Tokyo";

/// The instants under test, RFC-3339 so an expectation is checkable without epoch arithmetic.
const WHOLE_BEFORE_EPOCH: &str = "1969-12-31T23:30:00Z";
const HALF_SECOND_BEFORE_EPOCH: &str = "1969-12-31T23:59:59.5Z";
const ONE_AND_A_QUARTER_BEFORE_EPOCH: &str = "1969-12-31T23:59:58.75Z";
const FRACTION_AFTER_EPOCH: &str = "1970-01-01T00:00:00.75Z";
const MODERN_INSTANT: &str = "2024-06-15T12:00:00Z";
const MODERN_INSTANT_WITH_FRACTION: &str = "2024-06-15T12:00:01.999999Z";

/// A tz-aware `timestamp[ns, tz=UTC]` array.
fn utc_instants(rfc3339: &[&str]) -> ArrayRef {
    let text = StringArray::from(rfc3339.to_vec());
    cast(
        &text,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    )
    .expect("well-formed RFC-3339 instant literals")
}

/// The Spark-doored session the product builds, at `zone`.
fn session_at(zone: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config(repark_core::SESSION_TIME_ZONE_KEY, zone)
        .build()
        .expect("a session at a real zone")
}

/// Register `rfc3339` as a NULLABLE tz-aware `ts` column on table `t`, with a trailing NULL row.
fn register_instants(session: &ReparkSession, rfc3339: &[&str]) {
    let present = utc_instants(rfc3339);
    let with_null = cast(
        &StringArray::from(
            rfc3339
                .iter()
                .map(|value| Some(*value))
                .chain(std::iter::once(None))
                .collect::<Vec<_>>(),
        ),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    )
    .expect("well-formed RFC-3339 instant literals");
    assert_eq!(
        with_null.len(),
        present.len() + 1,
        "the fixture adds exactly one NULL row"
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![with_null]).expect("a one-column batch");
    session
        .context()
        .register_batch("t", batch)
        .expect("register the instant table");
}

/// Run `sql` and return `(Arrow type, Int64 values)` — the value AND type halves of one column.
async fn epoch_seconds(session: &ReparkSession, sql: &str) -> (DataType, Vec<Option<i64>>) {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let batch = &batches[0];
    let field_type = batch.schema().field(0).data_type().clone();
    let column = batch.column(0).as_primitive::<Int64Type>();
    let values = (0..column.len())
        .map(|row| column.is_valid(row).then(|| column.value(row)))
        .collect();
    (field_type, values)
}

/// A single-row `SELECT CAST(<literal instant> AS BIGINT)` on the Spark door.
fn cast_literal_sql(rfc3339: &str, target: &str) -> String {
    format!("SELECT CAST(to_timestamp('{rfc3339}') AS {target}) AS epoch_value")
}

/// Spark door — the charged class: whole instants either side of 1970 cast to epoch SECONDS.
#[tokio::test]
async fn spark_door_timestamp_cast_to_bigint_is_epoch_seconds() {
    let session = session_at(NEW_YORK);
    for (instant, expected) in [
        (WHOLE_BEFORE_EPOCH, -1800_i64),
        (MODERN_INSTANT, 1_718_452_800),
    ] {
        let (field_type, values) =
            epoch_seconds(&session, &cast_literal_sql(instant, "BIGINT")).await;
        assert_eq!(
            field_type,
            DataType::Int64,
            "{instant}: Spark returns BIGINT"
        );
        assert_eq!(values, vec![Some(expected)], "{instant}");
    }
}

/// Spark door — the FLOOR edge, both signs.
#[tokio::test]
async fn spark_door_timestamp_cast_floors_toward_negative_infinity() {
    let session = session_at(NEW_YORK);
    for (instant, expected) in [
        (HALF_SECOND_BEFORE_EPOCH, -1_i64),
        (ONE_AND_A_QUARTER_BEFORE_EPOCH, -2),
        (FRACTION_AFTER_EPOCH, 0),
        (MODERN_INSTANT_WITH_FRACTION, 1_718_452_801),
    ] {
        let (_, values) = epoch_seconds(&session, &cast_literal_sql(instant, "BIGINT")).await;
        assert_eq!(
            values,
            vec![Some(expected)],
            "{instant}: Spark uses Math.floorDiv, not truncation"
        );
    }
}

/// Spark door — the session zone does NOT move the answer.
#[tokio::test]
async fn spark_door_epoch_seconds_are_zone_independent() {
    for zone in [NEW_YORK, TOKYO, "UTC"] {
        let session = session_at(zone);
        let (_, values) =
            epoch_seconds(&session, &cast_literal_sql(WHOLE_BEFORE_EPOCH, "BIGINT")).await;
        assert_eq!(
            values,
            vec![Some(-1800)],
            "{zone}: the epoch of an instant cannot depend on a session zone"
        );
    }
}

/// Spark door — a real timestamp COLUMN, with its null mask.
#[tokio::test]
async fn spark_door_timestamp_column_casts_row_by_row_with_nulls() {
    let session = session_at(TOKYO);
    register_instants(
        &session,
        &[
            WHOLE_BEFORE_EPOCH,
            HALF_SECOND_BEFORE_EPOCH,
            MODERN_INSTANT_WITH_FRACTION,
        ],
    );
    let (field_type, values) =
        epoch_seconds(&session, "SELECT CAST(ts AS BIGINT) AS epoch_value FROM t").await;
    assert_eq!(field_type, DataType::Int64);
    assert_eq!(
        values,
        vec![Some(-1800), Some(-1), Some(1_718_452_801), None],
        "row order is the registered batch order; the trailing NULL stays NULL"
    );
}

/// Spark door — narrower signed integer targets share the class.
#[tokio::test]
async fn spark_door_narrower_integer_targets_get_the_same_scaling() {
    let session = session_at(NEW_YORK);
    for (target, expected_type) in [("INT", DataType::Int32), ("SMALLINT", DataType::Int16)] {
        let batches = session
            .sql(&cast_literal_sql(WHOLE_BEFORE_EPOCH, target))
            .await
            .unwrap_or_else(|error| panic!("plan CAST AS {target}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("collect CAST AS {target}: {error}"));
        let batch = &batches[0];
        assert_eq!(
            batch.schema().field(0).data_type(),
            &expected_type,
            "{target}: the user's width survives the rewrite"
        );
        let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .expect("printable batch")
            .to_string();
        assert!(
            rendered.contains("-1800"),
            "{target}: expected Spark's -1800, got:\n{rendered}"
        );
    }
}

/// Spark door — float and decimal targets keep the FRACTION Spark keeps.
#[tokio::test]
async fn spark_door_real_targets_keep_the_fractional_second() {
    let session = session_at(NEW_YORK);
    let batches = session
        .sql(&cast_literal_sql(HALF_SECOND_BEFORE_EPOCH, "DOUBLE"))
        .await
        .expect("plan CAST AS DOUBLE")
        .collect()
        .await
        .expect("collect CAST AS DOUBLE");
    let batch = &batches[0];
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
    let column = batch.column(0).as_primitive::<Float64Type>();
    assert!(
        (column.value(0) - -0.5).abs() < f64::EPSILON,
        "expected Spark's -0.5, got {}",
        column.value(0)
    );

    let decimal = session
        .sql(&cast_literal_sql(HALF_SECOND_BEFORE_EPOCH, "DECIMAL(20,6)"))
        .await
        .expect("plan CAST AS DECIMAL")
        .collect()
        .await
        .expect("collect CAST AS DECIMAL");
    assert_eq!(
        decimal[0].schema().field(0).data_type(),
        &DataType::Decimal128(20, 6),
        "the declared precision and scale survive the rewrite"
    );
    let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&decimal)
        .expect("printable batch")
        .to_string();
    assert!(
        rendered.contains("-0.500000"),
        "expected Spark's -0.500000, got:\n{rendered}"
    );
}

/// Native `DataFrame` API — the OTHER engine-side entry point.
#[tokio::test]
async fn native_dataframe_api_cast_is_epoch_seconds() {
    let session = session_at(NEW_YORK);
    register_instants(
        &session,
        &[WHOLE_BEFORE_EPOCH, HALF_SECOND_BEFORE_EPOCH, MODERN_INSTANT],
    );
    let frame = session
        .context()
        .table("t")
        .await
        .expect("the registered table")
        .select(vec![
            // Built as a bare `Expr::Cast`.
            Expr::Cast(Cast::new(Box::new(col("ts")), DataType::Int64)).alias("epoch_value"),
        ])
        .expect("project the cast");
    let batches = frame.collect().await.expect("collect the DataFrame");
    let batch = &batches[0];
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Int64,
        "the DataFrame path returns BIGINT like the SQL door"
    );
    let column = batch.column(0).as_primitive::<Int64Type>();
    let values: Vec<Option<i64>> = (0..column.len())
        .map(|row| column.is_valid(row).then(|| column.value(row)))
        .collect();
    assert_eq!(
        values,
        vec![Some(-1800), Some(-1), Some(1_718_452_800), None],
        "the DataFrame door must not be a cell where the raw nanosecond tick survives"
    );
}

/// The REVERSE direction stays correct — the regression fence for a symmetric "fix".
#[tokio::test]
async fn the_reverse_direction_still_reads_seconds_and_round_trips() {
    let session = session_at(NEW_YORK);
    let batches = session
        .sql(
            "SELECT CAST(CAST(to_timestamp('1969-12-31T23:30:00Z') AS BIGINT) AS TIMESTAMP) \
             AS ts_value",
        )
        .await
        .expect("plan the round trip")
        .collect()
        .await
        .expect("collect the round trip");
    let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("printable batch")
        .to_string();
    assert!(
        rendered.contains("1969-12-31T23:30:00"),
        "seconds out, the same instant back; got:\n{rendered}"
    );
}

/// DATE / TIMESTAMP stay outside TZ-5's *scale*.
#[tokio::test]
async fn casts_outside_the_class_are_untouched() {
    let session = session_at(NEW_YORK);
    for (target, expected) in [
        ("DATE", DataType::Date32),
        ("STRING", DataType::Utf8),
        (
            "TIMESTAMP",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ] {
        let batches = session
            .sql(&cast_literal_sql(MODERN_INSTANT, target))
            .await
            .unwrap_or_else(|error| panic!("plan CAST AS {target}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("collect CAST AS {target}: {error}"));
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &expected,
            "{target}: DATE/TIMESTAMP stay unowned; STRING is B-TZ-4 Utf8"
        );
    }
}
