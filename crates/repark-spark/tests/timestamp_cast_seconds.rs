//! **The `CAST(TIMESTAMP AS <numeric>)` epoch-seconds class** (divergence registry row TZ-5) —
//! pinned at the engine layer, value AND Arrow type, through a real session built the way the
//! product builds one.
//!
//! # What this file is the evidence for
//!
//! Apache Spark's `Cast(TimestampType, LongType)` is the **floor of epoch SECONDS**. repark stored
//! timestamps as nanosecond ticks and let DataFusion's cast reinterpret the raw value, so
//! `CAST(ts AS BIGINT)` returned a number 10⁹ too large — correctly signed, plausibly shaped, and
//! wrong. The fix is `repark-functions`' analyzer rewrite plus the two embedded scaling UDFs
//! (`repark_functions::timestamp_cast`); this file holds it from the OUTSIDE, where a user is.
//!
//! # The entry-point matrix (docs/testing.md matrix, split the way the tz campaign split it)
//!
//! | Cell | Where it is pinned |
//! |---|---|
//! | Spark door | HERE — every `session.sql(...)` pin below, on a `SparkDialect` session |
//! | native `DataFrame` API | HERE — `native_dataframe_api_cast_is_epoch_seconds`, built as a standalone `Expr::Cast` over a registered batch (the shape `Column.cast("long")` crosses PyO3 as) |
//! | ANSI door | `crates/repark-sql/tests/timestamp_cast_ansi_door.rs` (it lives in that crate because the crate-DAG policy allows `repark-sql -> repark-spark` as a dev edge and nothing the other way) |
//! | facade | `python/repark/tests/test_timestamp_cast_parity.py` — the recorded differential corpus, whose TZ-5 disclosure row this fix flips to equality |
//!
//! # Why the negatives are half the file
//!
//! Spark floors (`Math.floorDiv`); truncation toward zero — what an arrow `Timestamp(Second)` cast
//! hop would give — agrees with it on every positive instant and on every whole negative second.
//! It disagrees only on a **negative fractional** second, which is why `-0.5 s → -1` and
//! `-1.25 s → -2` are here: they are the two rows that separate the real fix from the plausible
//! one. The positive fractional rows are the other half of that fence, so the fix cannot be
//! "always subtract one".
//!
//! The class is **zone-independent on both engines** (probed under `America/New_York`,
//! `Asia/Tokyo` and `UTC`): a cast reads the instant, never a wall clock. The two-zone pin below
//! is the standing detector for a future change that wires a session zone into this path.
//!
//! Every expectation is live-Spark-4.1.2-recorded (`task/tz5-cast-seconds-ledger.md` §2), and
//! instants are written as RFC-3339 strings so a reader can check one by eye. AWS-free by
//! construction.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, TimeUnit};
use datafusion::logical_expr::{Cast, Expr};
use datafusion::prelude::col;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};

/// The two non-UTC zones the campaign uses: a DST-observing zone WEST of UTC and a fixed-offset
/// zone EAST of it. For THIS class both must give the same answer — see the module docs.
const NEW_YORK: &str = "America/New_York";
const TOKYO: &str = "Asia/Tokyo";

/// The instants under test, RFC-3339 so an expectation is checkable without epoch arithmetic.
const WHOLE_BEFORE_EPOCH: &str = "1969-12-31T23:30:00Z";
const HALF_SECOND_BEFORE_EPOCH: &str = "1969-12-31T23:59:59.5Z";
const ONE_AND_A_QUARTER_BEFORE_EPOCH: &str = "1969-12-31T23:59:58.75Z";
const FRACTION_AFTER_EPOCH: &str = "1970-01-01T00:00:00.75Z";
const MODERN_INSTANT: &str = "2024-06-15T12:00:00Z";
const MODERN_INSTANT_WITH_FRACTION: &str = "2024-06-15T12:00:01.999999Z";

/// A tz-aware `timestamp[ns, tz=UTC]` array — nanosecond-backed on purpose: nanoseconds are the
/// unit that made the divergence 10⁹ rather than 10⁶, so a µs-only fixture would under-test it.
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

/// Register `rfc3339` as a NULLABLE tz-aware `ts` column on table `t`, with a trailing NULL row so
/// every pin below carries the null mask a production column has.
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

/// ===========================================================================================
/// Spark door — the charged class: whole instants either side of 1970 cast to epoch SECONDS.
///
/// Live Spark 4.1.2: `-1800` and `1718452800`. DataFusion's own cast answers the raw nanosecond
/// tick (`-1800000000000`), which is what reverting the analyzer rewrite restores.
/// ===========================================================================================
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

/// ===========================================================================================
/// Spark door — the FLOOR edge, both signs. The rows that separate the fix from the plausible
/// truncating one (`-0.5 s` would be `0`, `-1.25 s` would be `-1`).
/// ===========================================================================================
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

/// ===========================================================================================
/// Spark door — the session zone does NOT move the answer.
///
/// A cast reads the instant; only a wall-clock reading would need a zone. Two zones with opposite
/// UTC offsets (and one DST-observing) are the standing detector for a change that wires the
/// session zone into this path by accident.
/// ===========================================================================================
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

/// ===========================================================================================
/// Spark door — a real timestamp COLUMN, with its null mask.
///
/// A folded literal proves nothing about the per-row kernel, and a NULL row is the input a
/// scaling bug most often turns into a zero.
/// ===========================================================================================
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

/// ===========================================================================================
/// Spark door — narrower signed integer targets share the class.
///
/// The rewrite scales first and leaves the user's width to the outer cast, so `INT`/`SMALLINT`
/// answer Spark's `-1800` in `Int32`/`Int16` — and repark answers them AT ALL, which it did not
/// before (DataFusion refuses a direct `Timestamp -> Int32`).
/// ===========================================================================================
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

/// ===========================================================================================
/// Spark door — float and decimal targets keep the FRACTION Spark keeps.
///
/// `CAST(ts AS DOUBLE)` of a half-second before the epoch is `-0.5` in Spark, not `-1` and not
/// `-500000000`. This is the sibling that shares the wrong scaling but NOT the floor.
/// ===========================================================================================
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

/// ===========================================================================================
/// Native `DataFrame` API — the OTHER engine-side entry point.
///
/// `Column.cast("long")` crosses PyO3 as a standalone `Expr::Cast` over a frame, with no SQL
/// string anywhere. It is a distinct user entry point, not a synonym for the SQL door, and it is
/// the spelling a migrated PySpark job most often uses.
/// ===========================================================================================
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
            // Built as a bare `Expr::Cast`, which is exactly what `Column.cast("long")` carries
            // across PyO3 — no schema in hand, no SQL string anywhere.
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

/// ===========================================================================================
/// The REVERSE direction stays correct — the regression fence for a symmetric "fix".
///
/// `CAST(<integer> AS TIMESTAMP)` already reads SECONDS in repark, exactly as Spark does (probed
/// 2026-08-11). Scaling it too would have INTRODUCED the divergence this unit removes, so the
/// round trip is pinned rather than assumed. The remaining reverse-direction gap is the Arrow
/// export TYPE (`timestamp[ns]` with no zone vs Spark's `timestamp[us, tz=UTC]`), which belongs
/// to registry row TZ-4 and is deliberately not asserted here.
/// ===========================================================================================
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

/// ===========================================================================================
/// DATE / TIMESTAMP stay outside TZ-5's *scale*. STRING is B-TZ-4: Spark `Utf8`. DATE is
/// TZ-8 (session-zone value; type stays `Date32`). Flipped 2026-08-13 (V-3 named A5 overflow
/// — the string-shape change forced this Spark-door type pin red).
/// ===========================================================================================
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
