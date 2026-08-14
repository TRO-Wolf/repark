//! **The session-timezone extraction class** (H-1a split B, campaign decision D7) — pinned on the
//! coercion path, value AND Arrow type, under two non-UTC session zones with the DST boundaries
//! included.
//!
//! # What this file is the evidence for
//!
//! Apache Spark resolves every calendar field of a `TIMESTAMP` in `spark.sql.session.timeZone`.
//! Before this unit repark resolved them in the STORED zone, which the census measured as a
//! four-hour silent offset (divergence registry §7 row TZ-1). The fix is in
//! `repark-functions`' extractor coercion path; this file holds it from the outside, through a
//! real session built exactly the way the product builds one.
//!
//! # The four-entry-point matrix (docs/testing.md matrix row 3, split by this campaign)
//!
//! | Cell | Where it is pinned |
//! |---|---|
//! | native `DataFrame` API | HERE — `native_dataframe_api_extracts_in_the_session_zone`, built from `repark_functions::expr_fn` (a standalone `Expr`, no session attached — the facade's `F.year(col)` shape) |
//! | Spark door | HERE — every `session.sql(...)` pin below, on a `SparkDialect` session |
//! | ANSI door | `crates/repark-sql/tests/session_timezone_ansi_door.rs` (that crate owns the DEV edge to this one; the reverse edge does not exist, by crate-DAG policy) |
//! | facade | `python/repark/tests/test_session_timezone_parity.py` — the recorded differential corpus, whose disclosure rows this fix flips to equality |
//!
//! # Why the negatives are half the file
//!
//! A zone-blind engine and a zone-*drunk* engine both fail Spark. The `DATE` / `TIME` pins below
//! (and the corpus's two control rows) exist because pushing the session zone into a path that
//! carries no instant is the exact failure mode a careless fix produces: a `DATE` has no instant,
//! so nothing about it may move.
//!
//! Instants are written as RFC-3339 strings and converted once, so a reader can check an
//! expectation against the string without decoding epoch arithmetic. AWS-free by construction.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, Int32Type, Schema, TimeUnit, TimestampMicrosecondType,
};
use datafusion::logical_expr::{Cast, Expr};
use datafusion::prelude::col;
use repark_core::{ReparkSession, SqlDialect};
use repark_functions::expr_fn;
use repark_spark::{SparkDialect, SparkExtension};

/// The two non-UTC zones the whole campaign uses: one DST-observing zone WEST of UTC and one
/// fixed-offset zone EAST of it, so an offset-sign error cannot pass both.
const NEW_YORK: &str = "America/New_York";
const TOKYO: &str = "Asia/Tokyo";
/// A half-hour offset, so a fix that only ever moves whole hours is caught (`minute` moves here).
const KOLKATA: &str = "Asia/Kolkata";

/// The instants under test, as RFC-3339 strings so an expectation is checkable by eye.
///
/// They are converted with arrow's own string→timestamp cast rather than a date library: the
/// fixture then cannot disagree with the engine about what `2024-06-15T12:00:00Z` means, and this
/// test binary needs no clock dependency of its own.
fn utc_instants(rfc3339: &[&str]) -> ArrayRef {
    let text = StringArray::from(rfc3339.to_vec());
    cast(
        &text,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )
    .expect("well-formed RFC-3339 instant literals")
}

/// Micros since the epoch for one RFC-3339 instant — the same conversion, for an expectation.
fn micros(rfc3339: &str) -> i64 {
    utc_instants(&[rfc3339])
        .as_primitive::<TimestampMicrosecondType>()
        .value(0)
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

/// Register `instants` as a tz-AWARE `timestamp[us, tz=UTC]` column named `ts` on table `t` —
/// the Arrow type PySpark's own export produces for a `TIMESTAMP`, so the Rust and the Python
/// halves of this class are measuring the same shape.
fn register_instants(session: &ReparkSession, rfc3339: &[&str]) {
    let column = utc_instants(rfc3339);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![column]).expect("a one-column batch");
    session
        .context()
        .register_batch("t", batch)
        .expect("register the instant table");
}

/// Run `sql` and return `(field types, i32 columns)` — the value AND type halves of one row set.
async fn int_columns(session: &ReparkSession, sql: &str) -> (Vec<DataType>, Vec<Vec<i32>>) {
    let batches = session
        .sql(sql)
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

/// The single-column `i32` shortcut over [`int_columns`], with its Arrow type asserted to be
/// `Int32` — every Spark calendar extractor returns `INT`, so the type half is uniform.
async fn ints(session: &ReparkSession, sql: &str) -> Vec<i32> {
    let (types, columns) = int_columns(session, sql).await;
    assert_eq!(
        types,
        vec![DataType::Int32],
        "a Spark calendar extractor returns INT: `{sql}`"
    );
    columns.into_iter().next().expect("one column")
}

/// One `STRING` column (`date_format`), with its Arrow type asserted.
async fn strings(session: &ReparkSession, sql: &str) -> Vec<String> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let batch = &batches[0];
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Utf8,
        "`date_format` returns STRING: `{sql}`"
    );
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    (0..values.len())
        .map(|row| values.value(row).to_string())
        .collect()
}

/// One `DATE` column as day offsets, with its Arrow type asserted — `trunc` / `add_months` /
/// `to_date`'s shape.
async fn dates(session: &ReparkSession, sql: &str) -> Vec<i32> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let batch = &batches[0];
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Date32,
        "a Spark date-valued function returns DATE: `{sql}`"
    );
    let values = batch.column(0).as_primitive::<Date32Type>();
    (0..values.len()).map(|row| values.value(row)).collect()
}

/// Day offsets for one `'yyyy-MM-dd'` date — the same arrow cast the fixtures use for instants, so
/// an expectation is checkable by eye and the test binary owns no calendar of its own.
fn date32(text: &str) -> i32 {
    let source = StringArray::from(vec![text]);
    cast(&source, &DataType::Date32)
        .expect("a well-formed 'yyyy-MM-dd' date literal")
        .as_primitive::<Date32Type>()
        .value(0)
}

/// One `TIMESTAMP` column as `(Arrow type, micros)` — `date_trunc`'s shape.
async fn timestamps(session: &ReparkSession, sql: &str) -> (DataType, Vec<i64>) {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let batch = &batches[0];
    let kind = batch.schema().field(0).data_type().clone();
    let values = batch.column(0).as_primitive::<TimestampMicrosecondType>();
    (
        kind,
        (0..values.len()).map(|row| values.value(row)).collect(),
    )
}

// ==================================================================================================
// The extractor families — one pin per family, on the coercion path, under two non-UTC zones
// ==================================================================================================

/// FAMILY 1 — `year`. The instant `2024-01-01T04:30Z` is 2023-12-31 23:30 in New York, so the
/// calendar YEAR itself moves; the same instant is already 2024 in Tokyo. A partitioned write
/// keyed on `year(ts)` lands in a different partition on the two engines until this holds.
#[tokio::test]
async fn year_extractor_resolves_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-01-01T04:30:00Z"]);
    assert_eq!(ints(&new_york, "SELECT year(ts) FROM t").await, vec![2023]);

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["2024-01-01T04:30:00Z", "2023-12-31T16:30:00Z"]);
    assert_eq!(
        ints(&tokyo, "SELECT year(ts) FROM t").await,
        vec![2024, 2024],
        "east of UTC the SECOND instant crosses into 2024 while New York would still read 2023"
    );
}

/// FAMILY 2 — `month` / `dayofmonth` / `dayofyear`, including the leap day. `2024-02-29T02:00Z`
/// is 2024-02-28 in New York: a leap-day filter selects different rows until this holds.
#[tokio::test]
async fn month_and_day_extractors_resolve_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-02-29T02:00:00Z", "2024-03-01T02:15:00Z"]);
    let (types, columns) = int_columns(
        &new_york,
        "SELECT month(ts), dayofmonth(ts), dayofyear(ts) FROM t",
    )
    .await;
    assert_eq!(types, vec![DataType::Int32; 3]);
    assert_eq!(columns[0], vec![2, 2], "both instants are February in EST");
    assert_eq!(
        columns[1],
        vec![28, 29],
        "the leap day itself moves back one"
    );
    assert_eq!(columns[2], vec![59, 60]);

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["2024-02-29T02:00:00Z", "2024-03-01T02:15:00Z"]);
    let (_, columns) = int_columns(&tokyo, "SELECT month(ts), dayofmonth(ts) FROM t").await;
    assert_eq!(
        (columns[0].clone(), columns[1].clone()),
        (vec![2, 3], vec![29, 1]),
        "east of UTC the same pair does NOT move — the two zones together separate a session-zone \
         fix from an offset-sign one"
    );
}

/// FAMILY 3 — `hour` / `minute` / `second`. `Asia/Kolkata` is +05:30, so the MINUTE moves too:
/// a fix that only ever shifts whole hours reds here and nowhere else.
#[tokio::test]
async fn hour_minute_second_extractors_resolve_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-06-15T12:00:00Z"]);
    let (_, columns) =
        int_columns(&new_york, "SELECT hour(ts), minute(ts), second(ts) FROM t").await;
    assert_eq!(
        (columns[0].clone(), columns[1].clone(), columns[2].clone()),
        (vec![8], vec![0], vec![0]),
        "the census's four-hour silent offset, isolated: EDT is UTC-4"
    );

    let kolkata = session_at(KOLKATA);
    register_instants(&kolkata, &["2024-06-15T12:00:00Z"]);
    let (_, columns) =
        int_columns(&kolkata, "SELECT hour(ts), minute(ts), second(ts) FROM t").await;
    assert_eq!(
        (columns[0].clone(), columns[1].clone(), columns[2].clone()),
        (vec![17], vec![30], vec![0]),
        "+05:30 moves the MINUTE as well as the hour; the second never moves"
    );
}

/// FAMILY 4 — the week/quarter family (`dayofweek` 1=Sunday, `weekday` 0=Monday, ISO
/// `weekofyear` / `yearofweek`, `quarter`). These ride on the resolved DAY, so they move with it
/// — and the ISO pair is the one that moves a whole YEAR at a New Year boundary.
#[tokio::test]
async fn week_and_quarter_extractors_resolve_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-01-01T04:30:00Z"]);
    let (types, columns) = int_columns(
        &new_york,
        "SELECT dayofweek(ts), weekday(ts), weekofyear(ts), yearofweek(ts), quarter(ts) FROM t",
    )
    .await;
    assert_eq!(types, vec![DataType::Int32; 5]);
    assert_eq!(
        (
            columns[0].clone(),
            columns[1].clone(),
            columns[2].clone(),
            columns[3].clone(),
            columns[4].clone()
        ),
        (vec![1], vec![6], vec![52], vec![2023], vec![4]),
        "2023-12-31 EST is a Sunday in ISO week 52 of 2023, in Q4"
    );

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["2024-01-01T04:30:00Z"]);
    let (_, columns) = int_columns(
        &tokyo,
        "SELECT dayofweek(ts), weekday(ts), weekofyear(ts), yearofweek(ts), quarter(ts) FROM t",
    )
    .await;
    assert_eq!(
        (
            columns[0].clone(),
            columns[1].clone(),
            columns[2].clone(),
            columns[3].clone(),
            columns[4].clone()
        ),
        (vec![2], vec![0], vec![1], vec![2024], vec![1]),
        "2024-01-01 JST is a Monday in ISO week 1 of 2024, in Q1 — every field on the other side"
    );
}

/// FAMILY 5 — `date_trunc`. Spark truncates to LOCAL midnight and returns the instant that
/// denotes, which is the daily-rollup boundary a migrated aggregate depends on.
///
/// TZ-4 PR-1 annotates the return as `timestamp[us, tz=UTC]`. The ticks were already Spark's
/// instant; the type pin here is the representation half.
#[tokio::test]
async fn date_trunc_truncates_on_the_session_zone_calendar() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-06-15T03:00:00Z"]);
    let (kind, values) = timestamps(&new_york, "SELECT date_trunc('day', ts) FROM t").await;
    assert_eq!(
        kind,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "TZ-4 PR-1: date_trunc of an instant is timestamp[us, tz=UTC]"
    );
    assert_eq!(
        values,
        vec![micros("2024-06-14T04:00:00Z")],
        "2024-06-14 00:00 EDT, not UTC midnight of the next day"
    );

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["2023-12-31T15:00:00Z"]);
    let (_, values) = timestamps(&tokyo, "SELECT date_trunc('year', ts) FROM t").await;
    assert_eq!(
        values,
        vec![micros("2023-12-31T15:00:00Z")],
        "the instant IS 2024-01-01 00:00 in Tokyo, so the year start is the instant itself — \
         truncating the stored calendar lands a whole year earlier"
    );
}

/// FAMILY 6 — `date_format`. Rendering and extraction must move TOGETHER: a formatted partition
/// path and an extracted partition key that disagree is worse than either being wrong alone.
#[tokio::test]
async fn date_format_renders_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-01-01T02:00:00Z"]);
    assert_eq!(
        strings(
            &new_york,
            "SELECT date_format(ts, 'yyyy-MM-dd HH:mm') FROM t"
        )
        .await,
        vec!["2023-12-31 21:00".to_string()]
    );
    assert_eq!(
        ints(&new_york, "SELECT year(ts) FROM t").await,
        vec![2023],
        "the rendered date and the extracted year agree, in the SAME zone"
    );

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["2024-01-01T02:00:00Z"]);
    assert_eq!(
        strings(&tokyo, "SELECT date_format(ts, 'yyyy-MM-dd HH:mm') FROM t").await,
        vec!["2024-01-01 11:00".to_string()]
    );
}

/// FAMILY 7 — the DST boundaries, which is where a zone-aware engine differs from a fixed-offset
/// one. Spring forward: 02:00–03:00 local does not exist on 2024-03-10 in New York. Fall back:
/// two DISTINCT instants share local hour 1 (EDT then EST) — the row a dedup-by-hour job needs.
#[tokio::test]
async fn dst_boundaries_resolve_like_spark() {
    let new_york = session_at(NEW_YORK);
    register_instants(
        &new_york,
        &[
            "2024-03-10T07:00:00Z",
            "2024-11-03T05:30:00Z",
            "2024-11-03T06:30:00Z",
        ],
    );
    assert_eq!(
        ints(&new_york, "SELECT hour(ts) FROM t").await,
        vec![3, 1, 1],
        "spring-forward lands at 3 (EDT); fall-back collapses two instants onto local hour 1"
    );

    // The instants themselves are untouched — the repeated local hour is a CALENDAR collapse,
    // never a value collapse. A fix that shifted ticks instead of re-annotating would red here.
    let (kind, values) = timestamps(&new_york, "SELECT ts FROM t").await;
    assert_eq!(
        kind,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        values,
        vec![
            micros("2024-03-10T07:00:00Z"),
            micros("2024-11-03T05:30:00Z"),
            micros("2024-11-03T06:30:00Z"),
        ]
    );
}

/// FAMILY 8 — negative-epoch instants (gap G16). Sign handling and zone handling are independent
/// bugs; this pins that fixing the zone did not break the pre-1970 arithmetic.
#[tokio::test]
async fn pre_1970_instants_resolve_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["1969-12-31T23:30:00Z"]);
    let (_, columns) = int_columns(
        &new_york,
        "SELECT year(ts), month(ts), dayofmonth(ts), hour(ts) FROM t",
    )
    .await;
    assert_eq!(
        (
            columns[0].clone(),
            columns[1].clone(),
            columns[2].clone(),
            columns[3].clone()
        ),
        (vec![1969], vec![12], vec![31], vec![18]),
        "18:30 EST on the last day of 1969: the calendar fields agree with UTC, the HOUR does not"
    );

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["1969-12-31T23:30:00Z"]);
    let (_, columns) = int_columns(&tokyo, "SELECT year(ts), hour(ts) FROM t").await;
    assert_eq!(
        (columns[0].clone(), columns[1].clone()),
        (vec![1970], vec![8]),
        "east of UTC the same instant is already 1970 — the epoch boundary itself moves"
    );
}

// ==================================================================================================
// The entry-point matrix: the native `DataFrame` API cell beside the Spark-door cell above
// ==================================================================================================

/// MATRIX — the **native `DataFrame` API** cell, and the reason the zone is read at INVOKE time.
///
/// `repark_functions::expr_fn` builds a standalone `Expr` that embeds the UDF instance directly
/// (the shape `repark-python` gives `F.year(col("ts"))`, which has no `SessionContext` to resolve
/// against). A zone baked into the UDF at REGISTRATION would reach the SQL doors and miss this
/// path entirely, so this test is the one that fails if the seam is built the easy way.
#[tokio::test]
async fn native_dataframe_api_extracts_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-01-01T04:30:00Z", "2024-06-15T12:00:00Z"]);
    let frame = new_york
        .context()
        .table("t")
        .await
        .expect("the registered table")
        .select(vec![
            expr_fn::year(col("ts")).alias("year_part"),
            expr_fn::hour(col("ts")).alias("hour_part"),
        ])
        .expect("a DataFrame-API projection");
    let batches = frame.collect().await.expect("collect");
    let batch = &batches[0];
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
    assert_eq!(batch.schema().field(1).data_type(), &DataType::Int32);
    let years = batch.column(0).as_primitive::<Int32Type>();
    let hours = batch.column(1).as_primitive::<Int32Type>();
    assert_eq!((years.value(0), hours.value(0)), (2023, 23));
    assert_eq!((years.value(1), hours.value(1)), (2024, 8));

    // The SAME class through the Spark door on the SAME session, so the two cells are pinned to
    // each other and not merely to two hand-written expectations.
    let (_, columns) = int_columns(&new_york, "SELECT year(ts), hour(ts) FROM t").await;
    assert_eq!(
        (columns[0].clone(), columns[1].clone()),
        (vec![2023, 2024], vec![23, 8]),
        "the DataFrame API and the Spark door must not drift apart on this class"
    );
}

// ==================================================================================================
// The negatives — what must NOT move
// ==================================================================================================

/// A `DATE`'s OWN calendar carries no instant, so nothing that reads it may move with the session
/// zone — extraction and `date_format` alike. This is the over-reach guard: the cheapest wrong fix
/// (push the zone into every temporal coercion) reds here.
///
/// The claim is deliberately narrower than it was. It used to say "nothing derived from one may
/// move … `date_trunc` alike", which is **false in Spark**: `date_trunc(fmt, DATE)` promotes the
/// `DATE` to a `TIMESTAMP` first, and that promotion is a session-zone localization. The promotion
/// is pinned next door, composed, in
/// [`date_trunc_of_a_date_or_string_lands_on_the_session_zone_timeline`] — this test now only
/// claims what it actually covers.
#[tokio::test]
async fn date_arguments_never_move_with_the_session_zone() {
    for zone in [NEW_YORK, TOKYO, KOLKATA, "UTC"] {
        let session = session_at(zone);
        let (_, columns) = int_columns(
            &session,
            "SELECT year(DATE '2024-02-29'), month(DATE '2024-02-29'), \
             dayofmonth(DATE '2024-02-29'), dayofweek(DATE '2024-02-29')",
        )
        .await;
        assert_eq!(
            (
                columns[0].clone(),
                columns[1].clone(),
                columns[2].clone(),
                columns[3].clone()
            ),
            (vec![2024], vec![2], vec![29], vec![5]),
            "DATE extraction under {zone} must equal DATE extraction under UTC"
        );
        assert_eq!(
            strings(
                &session,
                "SELECT date_format(DATE '2024-02-29', 'yyyy-MM-dd')"
            )
            .await,
            vec!["2024-02-29".to_string()],
            "rendering a DATE under {zone} must not shift it"
        );
        assert_eq!(
            dates(&session, "SELECT trunc(DATE '2024-02-29', 'YEAR')").await,
            vec![date32("2024-01-01")],
            "DATE -> DATE calendar math under {zone} stays on the DATE's own calendar"
        );
    }
}

/// `date_trunc(fmt, DATE)` / `date_trunc(fmt, STRING)` — the composed claim, which is where the
/// first draft of this unit was a whole day wrong under `America/New_York`.
///
/// Spark promotes the `DATE` (or string) to a `TIMESTAMP` before truncating, and that promotion is
/// a **session-zone localization**, so the result is the INSTANT of local midnight. Every value
/// below is live Spark 4.1.2 measured on 2026-08-10 under the same session zone:
///
/// ```text
/// date_trunc('day', DATE '2024-01-01')                     NY 2024-01-01T05:00Z   Tokyo 2023-12-31T15:00Z
/// year|month|dayofmonth|hour(date_trunc('day', DATE …))    2024, 1, 1, 0          identical in both zones
/// date_format(date_trunc('day', DATE …),'yyyy-MM-dd HH:mm') '2024-01-01 00:00'    identical in both zones
/// ```
///
/// The COMPOSITION legs are the point. `date_trunc`'s output is an instant (TZ-4 PR-1: µs+UTC).
/// Extractors resolve that instant in the session zone; these legs hold that the DATE-argument
/// promotion wrote local midnight's instant, not a wall-clock tick under a naive type.
#[tokio::test]
async fn date_trunc_of_a_date_or_string_lands_on_the_session_zone_timeline() {
    for (zone, midnight, leap_midnight) in [
        (NEW_YORK, "2024-01-01T05:00:00Z", "2024-02-29T05:00:00Z"),
        (TOKYO, "2023-12-31T15:00:00Z", "2024-02-28T15:00:00Z"),
    ] {
        let session = session_at(zone);

        let (kind, values) =
            timestamps(&session, "SELECT date_trunc('day', DATE '2024-01-01')").await;
        assert_eq!(
            kind,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "TZ-4 PR-1: date_trunc of a DATE is still an instant (local midnight)"
        );
        assert_eq!(
            values,
            vec![micros(midnight)],
            "under {zone} Spark promotes the DATE to local midnight's INSTANT"
        );
        let (_, values) = timestamps(&session, "SELECT date_trunc('day', DATE '2024-02-29')").await;
        assert_eq!(
            values,
            vec![micros(leap_midnight)],
            "the leap day, under {zone}"
        );

        // COMPOSITION — a DATE argument, then every extractor family over the result.
        let (_, columns) = int_columns(
            &session,
            "SELECT year(date_trunc('day', DATE '2024-01-01')), \
             month(date_trunc('day', DATE '2024-01-01')), \
             dayofmonth(date_trunc('day', DATE '2024-01-01')), \
             hour(date_trunc('day', DATE '2024-01-01'))",
        )
        .await;
        assert_eq!(
            (
                columns[0].clone(),
                columns[1].clone(),
                columns[2].clone(),
                columns[3].clone()
            ),
            (vec![2024], vec![1], vec![1], vec![0]),
            "under {zone} the truncated day must read back as its own local midnight, not shift"
        );
        assert_eq!(
            strings(
                &session,
                "SELECT date_format(date_trunc('day', DATE '2024-01-01'), 'yyyy-MM-dd HH:mm')"
            )
            .await,
            vec!["2024-01-01 00:00".to_string()],
            "rendering the truncated day under {zone} must agree with extracting from it"
        );

        // COMPOSITION — the STRING argument twin, which takes the same zone-free path.
        let (_, columns) = int_columns(
            &session,
            "SELECT year(date_trunc('day', '2024-01-01')), \
             month(date_trunc('day', '2024-01-01')), \
             dayofmonth(date_trunc('day', '2024-01-01'))",
        )
        .await;
        assert_eq!(
            (columns[0].clone(), columns[1].clone(), columns[2].clone()),
            (vec![2024], vec![1], vec![1]),
            "a STRING argument promotes exactly like a DATE under {zone}"
        );
        assert_eq!(
            strings(
                &session,
                "SELECT date_format(date_trunc('day', '2024-01-01'), 'yyyy-MM-dd HH:mm')"
            )
            .await,
            vec!["2024-01-01 00:00".to_string()],
        );
    }
}

/// `date_trunc` across the DST **fall-back**, where the truncated local time is AMBIGUOUS.
///
/// Spark truncates with `ZonedDateTime.truncatedTo`, whose `resolveLocal` passes the SOURCE
/// instant's offset as the preferred one — so the offset is PRESERVED and the two distinct instants
/// of the repeated hour stay distinct. Live Spark 4.1.2, `America/New_York`, 2026-08-10:
///
/// ```text
/// date_trunc('minute', to_timestamp('2024-11-03T06:30:40Z'))  ->  2024-11-03T06:30:00Z
/// date_trunc('hour',   to_timestamp('2024-11-03T05:30:00Z'))  ->  2024-11-03T05:00:00Z
/// date_trunc('hour',   to_timestamp('2024-11-03T06:30:00Z'))  ->  2024-11-03T06:00:00Z
/// ```
///
/// An implementation that re-resolves the truncated local time to the EARLIEST valid offset —
/// which the first draft of this unit did, on a doc comment that claimed it was what
/// `java.time` does — collapses the pair onto `05:00Z` and puts the `'minute'` row an hour early.
/// Every instant in every repeated hour, in every DST-observing zone, at hour/minute/second
/// granularity, is behind this one pin.
#[tokio::test]
async fn date_trunc_preserves_the_source_offset_across_a_fall_back() {
    let new_york = session_at(NEW_YORK);
    register_instants(
        &new_york,
        &[
            "2024-11-03T05:30:40Z", // 01:30:40 EDT — the FIRST pass through local hour 1
            "2024-11-03T06:30:40Z", // 01:30:40 EST — the SECOND pass, same wall clock
        ],
    );
    let (_, values) = timestamps(&new_york, "SELECT date_trunc('minute', ts) FROM t").await;
    assert_eq!(
        values,
        vec![
            micros("2024-11-03T05:30:00Z"),
            micros("2024-11-03T06:30:00Z"),
        ],
        "truncating to the minute must not move an instant across the DST offset it was recorded at"
    );

    let (_, values) = timestamps(&new_york, "SELECT date_trunc('hour', ts) FROM t").await;
    assert_eq!(
        values,
        vec![
            micros("2024-11-03T05:00:00Z"),
            micros("2024-11-03T06:00:00Z"),
        ],
        "two distinct instants in the repeated hour truncate to two DISTINCT instants"
    );

    // The DAY anchor of the same fall-back day is UNambiguous (local midnight is before the
    // transition), so the preferred offset must NOT be forced there: Spark answers 04:00Z (EDT),
    // not 05:00Z (the source instant's EST offset).
    let (_, values) = timestamps(&new_york, "SELECT date_trunc('day', ts) FROM t").await;
    assert_eq!(
        values,
        vec![
            micros("2024-11-03T04:00:00Z"),
            micros("2024-11-03T04:00:00Z"),
        ],
        "an unambiguous truncated local time takes its own single valid offset, not the source's"
    );
}

/// `date_trunc` whose truncated local anchor falls inside a DST **gap** — the arm whose old
/// justification ("gaps are an hour in every zone in the IANA database") was factually false.
///
/// Both zones below were measured against live Spark 4.1.2 on 2026-08-10 and are the two
/// realistically reachable shapes: a 30-minute gap and a midnight gap.
///
/// ```text
/// Australia/Lord_Howe  date_trunc('hour', to_timestamp('2024-10-05T15:40:00Z'))  ->  2024-10-05T15:30:00Z
/// America/Santiago     date_trunc('day',  to_timestamp('2024-09-08T04:30:00Z'))  ->  2024-09-08T04:00:00Z
/// ```
///
/// Lord Howe is the 30-minute case (its DST step is half an hour, not an hour); Santiago's
/// transition is at local midnight, so the `'day'` anchor itself does not exist.
#[tokio::test]
async fn dst_gap_zones_resolve_like_spark() {
    let lord_howe = session_at("Australia/Lord_Howe");
    register_instants(&lord_howe, &["2024-10-05T15:40:00Z"]);
    let (_, values) = timestamps(&lord_howe, "SELECT date_trunc('hour', ts) FROM t").await;
    assert_eq!(
        values,
        vec![micros("2024-10-05T15:30:00Z")],
        "local 02:00 does not exist on 2024-10-06 in Lord Howe: the gap is THIRTY minutes"
    );

    let santiago = session_at("America/Santiago");
    register_instants(&santiago, &["2024-09-08T04:30:00Z"]);
    let (_, values) = timestamps(&santiago, "SELECT date_trunc('day', ts) FROM t").await;
    assert_eq!(
        values,
        vec![micros("2024-09-08T04:00:00Z")],
        "Santiago springs forward AT local midnight, so the day anchor itself is in the gap"
    );
}

/// The date-valued calendar shims this crate owns (`trunc`, `add_months`) take a TIMESTAMP
/// argument's date in the SESSION zone, exactly as Spark does — the sibling half of the extraction
/// class. Live Spark 4.1.2, `America/New_York`, 2026-08-10:
///
/// ```text
/// trunc(to_timestamp('2024-06-01T03:00:00Z'), 'MM')   ->  2024-05-01   (the instant is 2024-05-31 23:00 EDT)
/// add_months(to_timestamp('2024-06-01T03:00:00Z'), 1) ->  2024-06-30   (end-of-month preserving, from 05-31)
/// ```
///
/// Both reach the date through [`repark_functions`]' own `coerce_to_date32` + invoke, which is why
/// they were fixable here; `CAST(ts AS DATE)` / `to_date` now share that kernel
/// (`timestamp_to_date_paths_read_the_session_zone`). `datediff` stays residual.
#[tokio::test]
async fn date_valued_shims_take_the_date_in_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-06-01T03:00:00Z"]);
    assert_eq!(
        dates(&new_york, "SELECT trunc(ts, 'MM') FROM t").await,
        vec![date32("2024-05-01")],
        "the instant is 2024-05-31 23:00 EDT, so its month starts in MAY"
    );
    assert_eq!(
        dates(&new_york, "SELECT add_months(ts, 1) FROM t").await,
        vec![date32("2024-06-30")],
        "from 2024-05-31 (a month end) Spark clamps to the target month's end"
    );

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["2024-06-01T03:00:00Z"]);
    assert_eq!(
        dates(&tokyo, "SELECT trunc(ts, 'MM') FROM t").await,
        vec![date32("2024-06-01")],
        "east of UTC the same instant is already 2024-06-01 12:00 JST — the pair separates a \
         session-zone fix from an offset-sign one"
    );

    // A DATE argument still never moves: the shims' zone-free path is untouched.
    for zone in [NEW_YORK, TOKYO] {
        let session = session_at(zone);
        assert_eq!(
            dates(&session, "SELECT add_months(DATE '2024-01-31', 1)").await,
            vec![date32("2024-02-29")],
            "DATE -> DATE month arithmetic under {zone} has no instant to resolve"
        );
    }
}

/// TZ-8 CAST / `to_date`: an LTZ timestamp's date is the session-zone calendar. Live Spark
/// 4.1.2, `America/New_York`, `2024-06-15T03:00:00Z` → `2024-06-14` (23:00 EDT on the 14th).
/// The UTC control and the Tokyo forward-crossing pin the sign; NTZ stays the stored wall.
#[tokio::test]
async fn timestamp_to_date_paths_read_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-06-15T03:00:00Z"]);
    assert_eq!(
        dates(&new_york, "SELECT to_date(ts) FROM t").await,
        vec![date32("2024-06-14")],
        "the instant is 23:00 EDT on the 14th; `to_date` must not read the stored UTC date"
    );
    assert_eq!(
        dates(&new_york, "SELECT CAST(ts AS DATE) FROM t").await,
        vec![date32("2024-06-14")],
        "CAST(ts AS DATE) is the common partition-key derivation — same date as to_date"
    );

    let utc = session_at("UTC");
    register_instants(&utc, &["2024-06-15T03:00:00Z"]);
    assert_eq!(
        dates(&utc, "SELECT CAST(ts AS DATE) FROM t").await,
        vec![date32("2024-06-15")],
        "UTC control: the session-zone date equals the stored date"
    );

    let tokyo = session_at(TOKYO);
    register_instants(&tokyo, &["2023-12-31T16:30:00Z"]);
    assert_eq!(
        dates(&tokyo, "SELECT CAST(ts AS DATE) FROM t").await,
        vec![date32("2024-01-01")],
        "east of UTC the same class crosses the year boundary FORWARD"
    );

    // datediff simplifies to Date32 subtraction of CAST(ts AS DATE) (datafusion-spark
    // SparkDateDiff::simplify). It therefore rides the TZ-8 CAST rewrite — Spark 13, not 14.
    assert_eq!(
        ints(&new_york, "SELECT datediff(ts, DATE '2024-06-01') FROM t").await,
        vec![13],
        "datediff(ts, date) is CAST(ts AS DATE) − date; the CAST rewrite closes this spelling"
    );
}

/// Native `DataFrame` API cell: a standalone `Expr::Cast` (the shape `Column.cast("date")`
/// crosses PyO3 as) must hit the same rewrite as SQL `CAST(ts AS DATE)`.
#[tokio::test]
async fn native_dataframe_api_cast_to_date_reads_the_session_zone() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-06-15T03:00:00Z"]);
    let frame = new_york
        .context()
        .table("t")
        .await
        .expect("the registered table")
        .select(vec![
            Expr::Cast(Cast::new(Box::new(col("ts")), DataType::Date32)).alias("d"),
        ])
        .expect("a DataFrame-API CAST AS DATE");
    let batches = frame.collect().await.expect("collect");
    let batch = &batches[0];
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Date32);
    let values = batch.column(0).as_primitive::<Date32Type>();
    assert_eq!(values.value(0), date32("2024-06-14"));
}

/// TZ-8 residue: `last_day` / `date_add` over a TIMESTAMP still fail to plan (Spark accepts
/// them). Named residual — not datediff, which rides CAST. Do not silently absorb.
#[tokio::test]
async fn last_day_and_date_add_over_a_timestamp_still_refuse() {
    let new_york = session_at(NEW_YORK);
    register_instants(&new_york, &["2024-06-15T03:00:00Z"]);

    // `last_day` / `date_add` over a TIMESTAMP do not even PLAN here, where Spark accepts
    // them. Live Spark 4.1.2 for this instant under NY: last_day → 2024-06-30,
    // date_add(..., 1) → 2024-06-15 (session-zone date 2024-06-14 ± calendar).
    for sql in [
        "SELECT last_day(ts) FROM t",
        "SELECT date_add(ts, 1) FROM t",
    ] {
        let Err(error) = new_york.sql(sql).await else {
            panic!("`{sql}` planned; Spark's answer must now be pinned instead")
        };
        let error = error.to_string();
        assert!(
            error.contains("coerce") || error.contains("No function matches"),
            "DIVERGENCE (registry TZ-8 residual): `{sql}` must fail to PLAN until the overload \
             is added; got {error}"
        );
    }
}

/// A `TIME` carries no date and no instant either — `hour`/`minute`/`second` over one are pure
/// clock arithmetic and must be identical in every session zone.
#[tokio::test]
async fn time_arguments_never_move_with_the_session_zone() {
    for zone in [NEW_YORK, TOKYO, "UTC"] {
        let session = session_at(zone);
        let (_, columns) = int_columns(
            &session,
            "SELECT hour(TIME '13:45:07'), minute(TIME '13:45:07'), second(TIME '13:45:07')",
        )
        .await;
        assert_eq!(
            (columns[0].clone(), columns[1].clone(), columns[2].clone()),
            (vec![13], vec![45], vec![7]),
            "TIME extraction must not move under {zone}"
        );
    }
}

/// The seam's default, pinned ACROSS the crate boundary. `repark-functions` cannot name
/// `repark-core` (a capability leaf with no engine edge), so its fallback zone is a second
/// constant by necessity — and a second constant that nothing checks is a latent split-brain.
/// A session that never set the key must extract exactly as `repark_core`'s documented default
/// says it will.
#[tokio::test]
async fn default_session_extracts_in_the_core_default_zone() {
    assert_eq!(
        repark_core::DEFAULT_SESSION_TIME_ZONE,
        repark_functions::session_time_zone::DEFAULT_EXTRACTION_TIME_ZONE,
        "the engine's default zone and the extractor layer's fallback must be the same string"
    );
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let default_session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .expect("a session that never set the key");
    register_instants(&default_session, &["2024-06-15T12:00:00Z"]);
    assert_eq!(
        ints(&default_session, "SELECT hour(ts) FROM t").await,
        vec![12],
        "an unconfigured session reads instants in UTC — the documented, host-independent default"
    );

    let explicit_utc = session_at("UTC");
    register_instants(&explicit_utc, &["2024-06-15T12:00:00Z"]);
    assert_eq!(
        ints(&explicit_utc, "SELECT hour(ts) FROM t").await,
        vec![12],
        "setting the key to the default value changes nothing"
    );
}

/// The one-spelling gate, checked ACROSS the crate boundary. `repark-functions` must name the
/// authoritative conf key in its refusal message and cannot import `repark-core` to get it, so
/// the literal it carries is a mirror — and an unchecked mirror is how a second spelling is born.
/// This test is the check: it runs from the one crate that can see both constants.
#[test]
fn the_carrier_refusal_names_the_engines_own_key() {
    use datafusion::common::config::ExtensionOptions;
    use repark_functions::session_time_zone::SessionTimeZoneConfig;

    let message = SessionTimeZoneConfig::default()
        .set("time_zone", "Asia/Tokyo")
        .expect_err("the carrier is not settable")
        .to_string();
    assert!(
        message.contains(repark_core::SESSION_TIME_ZONE_KEY),
        "the refusal must name the ENGINE's key constant verbatim, not a lookalike; got {message}"
    );
}

/// A tz-NAIVE timestamp is an instant in UTC here, not a Spark `TIMESTAMP_NTZ`: repark has no
/// NTZ type, and `to_timestamp('…Z')` demonstrably stores UTC ticks under a tz-naive Arrow type
/// (registry row TZ-4 — the missing annotation is a TYPE gap). Extraction therefore treats it as
/// an instant, which is what makes the facade corpus's scalar-literal rows converge with Spark.
/// Pinned explicitly so the interpretation is a decision on the record, not an accident.
///
/// This pin exercises the case where the interpretation is RIGHT. The case where the same
/// interpretation is WRONG has its own pin next door — a pin that only exists because this one
/// alone could be read as evidence the whole family converged, which it is not.
#[tokio::test]
async fn a_tz_naive_timestamp_is_read_as_a_utc_instant() {
    let new_york = session_at(NEW_YORK);
    assert_eq!(
        ints(
            &new_york,
            "SELECT hour(to_timestamp('2024-06-15T12:00:00Z'))"
        )
        .await,
        vec![8],
        "`to_timestamp` yields a tz-naive Arrow type holding UTC ticks; the session zone applies"
    );
    let tokyo = session_at(TOKYO);
    assert_eq!(
        ints(&tokyo, "SELECT hour(to_timestamp('2024-06-15T12:00:00Z'))").await,
        vec![21]
    );
}

/// TZ-4 PR-2: a zoneless LTZ input is a session-zone wall clock. Flip evidence for TZ-7.
#[tokio::test]
async fn a_zoneless_timestamp_input_localizes_in_the_session_zone() {
    for zone in [NEW_YORK, TOKYO] {
        let session = session_at(zone);
        for sql in [
            "SELECT hour(TIMESTAMP '2024-06-15 12:00:00')",
            "SELECT hour(to_timestamp('2024-06-15 12:00:00'))",
            "SELECT hour(CAST('2024-06-15 12:00:00' AS TIMESTAMP))",
        ] {
            assert_eq!(
                ints(&session, sql).await,
                vec![12],
                "zoneless LTZ input is a wall clock in {zone}: `{sql}`"
            );
        }
    }

    let new_york = session_at(NEW_YORK);
    let (_, columns) = int_columns(
        &new_york,
        "SELECT year(TIMESTAMP '2024-01-01 00:30:00'), dayofmonth(TIMESTAMP '2024-01-01 00:30:00')",
    )
    .await;
    assert_eq!(
        (columns[0].clone(), columns[1].clone()),
        (vec![2024], vec![1]),
        "year-boundary zoneless literal stays on 2024-01-01 in New York"
    );
}

/// TZ-4 PR-2: a tz-naive column is NTZ — extractors do not apply the session zone.
#[tokio::test]
async fn a_naive_ntz_timestamp_is_not_shifted_by_the_session_zone() {
    use datafusion::arrow::array::TimestampMicrosecondArray;

    let session = session_at(NEW_YORK);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )]));
    let ticks = micros("2024-06-15T12:00:00Z");
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampMicrosecondArray::from(vec![ticks]))],
    )
    .expect("ntz batch");
    session
        .context()
        .register_batch("ntz", batch)
        .expect("register ntz");
    assert_eq!(
        ints(&session, "SELECT hour(ts) FROM ntz").await,
        vec![12],
        "NTZ hour is the spelled wall, not the New York instant (8)"
    );
}
