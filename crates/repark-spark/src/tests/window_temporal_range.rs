//! Pins Spark temporal `RANGE` frame behavior for unit-less, interval, inverted, and numeric-key
//! bounds. Assertions use live Spark 4.1.2 expectations on the Arrow path, including types.

use super::super::*;
use super::common::*;

use datafusion::arrow::array::{Date32Array, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::TimeUnit;

// =================================================================================================
// Fixtures — the §0 recon seed rows, registered as plain temp views (leaf-private)
// =================================================================================================

/// `(id, ts, v)` with a same-day pair, a next-day row, and a tied pair three days later.
///
/// The tie on `ts` (ids 4 and 5) is what makes peer handling observable; the 12-hour gap between
/// ids 1 and 2 is what makes a sub-day interval observable.
const TIMESTAMP_SEED: &[(i64, &str, i64)] = &[
    (1, "2024-01-01T00:00:00", 10),
    (2, "2024-01-01T12:00:00", 20),
    (3, "2024-01-02T00:00:00", 30),
    (4, "2024-01-04T00:00:00", 40),
    (5, "2024-01-04T00:00:00", 50),
];

/// Microseconds since the epoch for a `YYYY-MM-DDTHH:MM:SS` seed literal (UTC, no zone).
fn seed_micros(text: &str) -> i64 {
    let (date, time) = text
        .split_once('T')
        .unwrap_or_else(|| panic!("seed timestamp `{text}` must be `date T time`"));
    let mut date_parts = date.split('-').map(|part| {
        part.parse::<i64>()
            .unwrap_or_else(|error| panic!("seed date `{date}`: {error}"))
    });
    let (year, month, day) = (
        date_parts.next().unwrap_or(0),
        date_parts.next().unwrap_or(0),
        date_parts.next().unwrap_or(0),
    );
    let mut time_parts = time.split(':').map(|part| {
        part.parse::<i64>()
            .unwrap_or_else(|error| panic!("seed time `{time}`: {error}"))
    });
    let (hour, minute, second) = (
        time_parts.next().unwrap_or(0),
        time_parts.next().unwrap_or(0),
        time_parts.next().unwrap_or(0),
    );
    let days = days_from_civil(year, month, day);
    ((days * 86_400) + (hour * 3_600) + (minute * 60) + second) * 1_000_000
}

/// Days since 1970-01-01 for a proleptic-Gregorian civil date (Howard Hinnant's `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Register the timestamp seed as `wt` (`id BIGINT, ts TIMESTAMP, v BIGINT`).
fn register_timestamp_seed(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                TIMESTAMP_SEED.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                TIMESTAMP_SEED
                    .iter()
                    .map(|row| seed_micros(row.1))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                TIMESTAMP_SEED.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch("wt", batch).unwrap();
}

/// Register the DATE seed as `wd` (`id BIGINT, d DATE, v BIGINT`): 2024-01-01, -02, -04.
fn register_date_seed(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Date32, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let days: Vec<i32> = [(2024, 1, 1), (2024, 1, 2), (2024, 1, 4)]
        .into_iter()
        .map(|(year, month, day)| {
            i32::try_from(days_from_civil(year, month, day)).unwrap_or_default()
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Date32Array::from(days)),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    )
    .unwrap();
    ctx.register_batch("wd", batch).unwrap();
}

/// Ties + close values so `INTERVAL 'n' UNIT` magnitude over INT is observable.
///
/// `(id, v) = (1,10), (2,20), (3,20), (4,21), (5,30)` — Z-4 / W-4 live Spark seed.
fn register_numeric_ties_seed(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 20, 21, 30])),
        ],
    )
    .unwrap();
    ctx.register_batch("wi", batch).unwrap();
}

/// Collect one `BIGINT`-typed measured column, asserting its Arrow type and returning its cells.
///
/// The query must project exactly `(id, <measured>)` ordered by `id`, so the returned vector is
/// row-ordered and comparable to the recorded Spark half directly.
async fn collect_measured_int64(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<Option<i64>> {
    let frame = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute failed for `{sql}`: {error}"));
    assert_eq!(
        frame.schema().field(1).data_type(),
        &DataType::Int64,
        "`{sql}` must measure an Int64 column (Spark BIGINT)"
    );
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect failed for `{sql}`: {error}"));
    let mut cells = Vec::new();
    for batch in &batches {
        let array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column 1 of `{sql}` is not Int64Array"));
        for index in 0..array.len() {
            cells.push(if array.is_null(index) {
                None
            } else {
                Some(array.value(index))
            });
        }
    }
    cells
}

/// The `Some(..)` form of a recorded all-non-null Spark half.
fn present(values: &[i64]) -> Vec<Option<i64>> {
    values.iter().copied().map(Some).collect()
}

// =================================================================================================
// Pin 1 — the refuse arm: a unit-less RANGE offset over a TIMESTAMP order key
// =================================================================================================

/// Spark 4.1.2 refuses `RANGE BETWEEN <n> PRECEDING` over a `TIMESTAMP` order key with
/// `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` (§0 recon, live oracle). Bare datetime bounds must
/// refuse instead of being interpreted as months.
#[tokio::test]
async fn temporal_range_bare_offset_over_timestamp_key_refuses_like_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);

    for offset in ["1", "2", "30"] {
        let sql = format!(
            "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN {offset} PRECEDING \
             AND CURRENT ROW) AS s FROM wt ORDER BY id"
        );
        let error = execute(&ctx, &catalogs, &sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must refuse, not answer a one-month window"));
        let message = error.to_string();
        assert!(
            message.contains("DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE"),
            "`{sql}` must carry Spark's error class, got: {message}"
        );
        assert!(
            message.contains("does not support the data type \"INT\""),
            "`{sql}` must carry Spark's INT-in-range-frame wording, got: {message}"
        );
    }

    // The FOLLOWING side and the shorthand (no BETWEEN) spelling reach the same bound slot.
    for frame in [
        "RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING",
        "RANGE 1 PRECEDING",
    ] {
        let sql = format!("SELECT id, sum(v) OVER (ORDER BY ts {frame}) AS s FROM wt ORDER BY id");
        let error = execute(&ctx, &catalogs, &sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must refuse"));
        assert!(
            error
                .to_string()
                .contains("DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE"),
            "`{sql}` must carry Spark's error class, got: {error}"
        );
    }
}

// =================================================================================================
// Pin 2 — the restate arm: a unit-less RANGE offset over a DATE order key means DAYS
// =================================================================================================

/// Spark reads `RANGE BETWEEN 1 PRECEDING` over a `DATE` key as **one day** and answers
/// `[10, 30, 30]` on the seed (§0 recon, live oracle). DataFusion's coercion read it as one
/// month and answered `[10, 30, 60]` — the same query, a silently wider window, no error. The
/// door restates the bound as `INTERVAL '1' DAY` and re-plans, so the values match Spark.
///
/// The `30 PRECEDING` case is the second half of the claim: it must mean thirty **days** (which
/// does span the whole seed, `[10, 30, 60]`), not thirty months — identical output to the
/// one-month reading is exactly why a single offset could not carry this pin alone.
#[tokio::test]
async fn temporal_range_bare_offset_over_date_key_means_days() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_date_seed(&ctx);

    let one_day = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s \
         FROM wd ORDER BY id",
    )
    .await;
    assert_eq!(
        one_day,
        present(&[10, 30, 30]),
        "`1 PRECEDING` over a DATE key is Spark's ONE DAY, not DataFusion's one month"
    );

    // Spelled as an interval, the same frame — the two spellings must agree after the restatement.
    let spelled_out = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wd ORDER BY id",
    )
    .await;
    assert_eq!(
        spelled_out, one_day,
        "`1 PRECEDING` and `INTERVAL '1' DAY PRECEDING` must be the same frame over a DATE key"
    );

    let thirty_days = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY d RANGE BETWEEN 30 PRECEDING AND CURRENT ROW) AS s \
         FROM wd ORDER BY id",
    )
    .await;
    assert_eq!(
        thirty_days,
        present(&[10, 30, 60]),
        "`30 PRECEDING` over a DATE key spans the whole seed as THIRTY DAYS"
    );
}

// =================================================================================================
// Pin 3 — the already-correct interval path is undisturbed (asc, desc, ties, NULL keys)
// =================================================================================================

/// Interval-bounded temporal `RANGE` matches Spark.
///
/// Four classes in one pin because they share the seed and the claim is "the fix touched no
/// interval-bounded frame": ascending, descending, a tie on the order key, and NULL order keys.
/// Every golden is the recorded live-Spark half from the §0 recon.
#[tokio::test]
async fn temporal_range_interval_bounds_still_match_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);

    let ascending = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        ascending,
        present(&[10, 30, 60, 90, 90]),
        "ascending one-day trailing window (ids 4/5 tie, so both see 40+50)"
    );

    let descending = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts DESC RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        descending,
        present(&[60, 50, 30, 90, 90]),
        "descending reverses which side of the current row the interval reaches"
    );

    // Peers only: a zero-width interval frame collapses to the tie group on `ts`.
    let peers_only = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '0' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        peers_only,
        present(&[10, 20, 30, 90, 90]),
        "a zero interval is the peer group: only the tied ids 4/5 see more than themselves"
    );

    // Sub-day unit: the 12-hour gap between ids 1 and 2 is inside a 12-hour window, the 24-hour
    // gap to id 3 is not — so HOUR and DAY must disagree, which is what proves the unit is read.
    let twelve_hours = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '12' HOUR PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        twelve_hours,
        present(&[10, 30, 50, 90, 90]),
        "a 12-hour window excludes the 24-hour-older row a 1-day window includes"
    );
    assert_ne!(
        twelve_hours, ascending,
        "HOUR and DAY must not collapse to the same frame — that would prove nothing"
    );
}

/// NULL order keys under a temporal `RANGE`: Spark groups the NULL rows as their own peers and
/// answers `[10, 60, 40, 60]` on the null-bearing seed (§0 recon, live oracle).
#[tokio::test]
async fn temporal_range_null_order_keys_match_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(seed_micros("2024-01-01T00:00:00")),
                None,
                Some(seed_micros("2024-01-02T00:00:00")),
                None,
            ])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40])),
        ],
    )
    .unwrap();
    ctx.register_batch("wn", batch).unwrap();

    let with_nulls = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wn ORDER BY id",
    )
    .await;
    assert_eq!(
        with_nulls,
        present(&[10, 60, 40, 60]),
        "NULL order keys are their own peer group ahead of the values (Spark ASC NULLS FIRST)"
    );
}

// =================================================================================================
// Pin 4 — the fix is scoped: numeric order keys and mixed statements are left alone
// =================================================================================================

/// A unit-less `RANGE` offset over a **numeric** order key is the ordinary value-offset frame and
/// must be untouched — neither refused nor re-scaled to days.
///
/// The mixed case is the reason [`crate::window_range`]'s restatement is statement-wide-or-nothing:
/// one statement carrying both a DATE-keyed and an INT-keyed bare-number frame must leave the
/// INT frame exactly as it was, because the AST restatement cannot tell the two bound sites
/// apart. The DATE frame then keeps its recorded divergence rather than the INT frame acquiring
/// a new one — a narrower fix, never a wider bug.
#[tokio::test]
async fn temporal_range_numeric_order_keys_are_untouched() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);
    register_date_seed(&ctx);

    // `v` is BIGINT: 10, 20, 30, 40, 50. A ±10 value window is the ordinary numeric RANGE.
    let numeric = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS s \
         FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        numeric,
        present(&[10, 30, 50, 70, 90]),
        "a numeric order key keeps DataFusion's value-offset RANGE semantics"
    );

    // Mixed statement: the INT-keyed frame must still be a value offset, not a day offset.
    let mixed = execute(
        &ctx,
        &catalogs,
        "SELECT id, \
         sum(v) OVER (ORDER BY v RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS by_value, \
         sum(v) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS by_date \
         FROM wd ORDER BY id",
    )
    .await
    .expect("a mixed numeric/DATE statement must still plan");
    let batches = mixed.collect().await.expect("mixed statement must collect");
    let by_value = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("by_value is Int64Array");
    assert_eq!(
        (0..by_value.len())
            .map(|index| by_value.value(index))
            .collect::<Vec<_>>(),
        vec![10, 30, 50],
        "the INT-keyed frame in a mixed statement is never re-scaled to days"
    );
}

// =================================================================================================
// Temporal RANGE refusal and residual behavior pins.
// =================================================================================================

/// Plan or execute error text for a statement that must stay loud.
async fn execute_error(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("`{sql}` must stay loud, not answer"),
        },
    }
}

/// R3 HIGH: `INTERVAL '-1' DAY PRECEDING` over a TIMESTAMP key is Spark's empty frame
/// (sum NULL, count 0), not a wrapped `count(*)` = -1 / debug panic. DATE already answered
/// empty at the pin and must stay empty (the restatement is TIMESTAMP-only).
#[tokio::test]
async fn temporal_range_negative_offset_is_spark_empty_frame() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);
    register_date_seed(&ctx);

    let negative_sum = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        negative_sum,
        vec![None, None, None, None, None],
        "negative PRECEDING over TIMESTAMP is Spark's empty sum, not a panic"
    );

    let negative_count = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-1' DAY PRECEDING \
         AND CURRENT ROW) AS c FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        negative_count,
        present(&[0, 0, 0, 0, 0]),
        "negative PRECEDING over TIMESTAMP is Spark's empty count, not wrapping -1"
    );

    let negative_following = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND \
         INTERVAL '-1' DAY FOLLOWING) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        negative_following,
        vec![None, None, None, None, None],
        "negative FOLLOWING to CURRENT ROW is the same inverted empty frame"
    );

    let date_count = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '-1' DAY PRECEDING \
         AND CURRENT ROW) AS c FROM wd ORDER BY id",
    )
    .await;
    assert_eq!(
        date_count,
        present(&[0, 0, 0]),
        "DATE + negative interval already answered empty and must not be refused"
    );
}

/// Spark refuses same-kind magnitude inversions with
/// `SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON`; kind inversions remain Spark-empty.
#[tokio::test]
async fn temporal_range_value_inverted_frames_do_not_wrap() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);

    for sql in [
        "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-2' DAY PRECEDING \
         AND INTERVAL '-1' DAY PRECEDING) AS c FROM wt ORDER BY id",
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-2' DAY PRECEDING \
         AND INTERVAL '-1' DAY PRECEDING) AS s FROM wt ORDER BY id",
        "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-1' DAY PRECEDING \
         AND INTERVAL '0' DAY FOLLOWING) AS c FROM wt ORDER BY id",
        "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '2' DAY FOLLOWING \
         AND INTERVAL '1' DAY FOLLOWING) AS c FROM wt ORDER BY id",
    ] {
        let message = execute_error(&ctx, &catalogs, sql).await;
        assert!(
            message.contains("SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON"),
            "same-kind magnitude invert must refuse like Spark, not wrap; `{sql}` got: {message}"
        );
        assert!(
            message.contains("lower bound of a window frame must be less than or equal"),
            "`{sql}` must carry Spark's start<=end wording, got: {message}"
        );
    }
}

/// Q-003: a statement that mixes a negative TIMESTAMP interval with a numeric unit-less
/// `RANGE` bound cannot use the statement-wide restatement. Refuse so wrapping cannot
/// ride the mix.
#[tokio::test]
async fn temporal_range_mixed_negative_timestamp_and_numeric_bare_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);
    let message = execute_error(
        &ctx,
        &catalogs,
        "SELECT id, \
         sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-1' DAY PRECEDING \
         AND CURRENT ROW) AS by_ts, \
         sum(v) OVER (ORDER BY v RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS by_value \
         FROM wt ORDER BY id",
    )
    .await;
    assert!(
        message.contains("UNSUPPORTED.NEGATIVE_RANGE_OFFSET"),
        "mixed negative-TIMESTAMP + numeric-bare must refuse, got: {message}"
    );
}

/// R2: `INTERVAL '1 12:00:00' DAY TO SECOND` (and the brief's `'1 0:0:0'`) is Spark's
/// one-day-plus-hours trailing window, not an Arrow parse error.
#[tokio::test]
async fn temporal_range_day_to_second_literal_matches_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);

    let day_and_twelve = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1 12:00:00' DAY TO SECOND \
         PRECEDING AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        day_and_twelve,
        present(&[10, 30, 60, 90, 90]),
        "DAY TO SECOND 1 12:00:00 is a 36-hour trailing window (same rows as 1 DAY on this seed)"
    );

    let day_exactly = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1 0:0:0' DAY TO SECOND \
         PRECEDING AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        day_exactly,
        present(&[10, 30, 60, 90, 90]),
        "DAY TO SECOND 1 0:0:0 is exactly one day"
    );
}

/// R1 (W-4): unquoted `INTERVAL 1 DAY` is quoted before first plan and matches the
/// quoted `INTERVAL '1' DAY` table (Spark 4.1.2).
#[tokio::test]
async fn temporal_range_unquoted_interval_literal_matches_quoted() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);
    let unquoted = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        unquoted,
        present(&[10, 30, 60, 90, 90]),
        "unquoted INTERVAL 1 DAY must match Spark / the quoted INTERVAL '1' DAY table"
    );
    let quoted = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        unquoted, quoted,
        "quoted and unquoted INTERVAL 1 DAY must agree"
    );
}

/// Both-bounds-FOLLOWING includes the current row (120 vs Spark 90). The planned frame is typed,
/// but this seam cannot exclude the current row without inventing row-level compensation.
#[tokio::test]
async fn temporal_range_following_to_following_still_includes_current_row() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);
    let sums = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY FOLLOWING \
         AND INTERVAL '2' DAY FOLLOWING) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        sums,
        vec![Some(30), None, Some(120), None, None],
        "R4 stays the recorded silent off-by-one (Spark is 30/NULL/90/NULL/NULL) until a \
         DataFusion range-search fix; do not absorb a move off this pin"
    );
}

/// R5 (W-4): `INTERVAL 'n' UNIT` over a numeric key is Spark's numeric `n` RANGE
/// (unit ignored). Unique `v` gaps of 10 make `n=1` look like "self only"; the
/// magnitude pin below is the one that distinguishes that guess.
#[tokio::test]
async fn temporal_range_interval_bound_over_int_key_is_numeric_n() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);

    let one_day = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(
        one_day,
        present(&[10, 20, 30, 40, 50]),
        "INTERVAL '1' DAY over unique v (gaps of 10) is numeric 1 PRECEDING"
    );

    let one_hour = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '1' HOUR PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    let one_month = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '1' MONTH PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    let unitless = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) \
         AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(one_day, one_hour, "R5: unit is ignored (DAY == HOUR)");
    assert_eq!(one_day, one_month, "R5: unit is ignored (DAY == MONTH)");
    assert_eq!(one_day, unitless, "R5: INTERVAL '1' UNIT == 1 PRECEDING");

    let unquoted = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL 1 DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wt ORDER BY id",
    )
    .await;
    assert_eq!(unquoted, one_day, "R1+R5: unquoted INTERVAL 1 DAY over INT");
}

/// R5 magnitude: ties + `n=1` vs `n=10` so "self only" cannot pass.
#[tokio::test]
async fn temporal_range_interval_bound_over_int_key_uses_numeric_magnitude() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_numeric_ties_seed(&ctx);

    let one = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wi ORDER BY id",
    )
    .await;
    assert_eq!(
        one,
        present(&[10, 40, 40, 61, 30]),
        "INTERVAL '1' DAY over ties seed is numeric 1 (not self-only)"
    );

    let ten = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '10' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wi ORDER BY id",
    )
    .await;
    assert_eq!(
        ten,
        present(&[10, 50, 50, 61, 91]),
        "INTERVAL '10' DAY over ties seed is numeric 10 PRECEDING"
    );

    let zero = collect_measured_int64(
        &ctx,
        &catalogs,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '0' DAY PRECEDING \
         AND CURRENT ROW) AS s FROM wi ORDER BY id",
    )
    .await;
    assert_eq!(
        zero,
        present(&[10, 40, 40, 21, 30]),
        "INTERVAL '0' DAY over ties seed is the peer group"
    );
}

/// Mixed TIMESTAMP-interval + INT-interval cannot use the statement-wide R5 rewrite.
/// The numeric site stays the recorded Arrow cast (same mixed-statement rule as G5b).
#[tokio::test]
async fn temporal_range_mixed_datetime_and_numeric_interval_leaves_numeric_loud() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);
    let message = execute_error(
        &ctx,
        &catalogs,
        "SELECT id, \
         sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS by_ts, \
         sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
         AND CURRENT ROW) AS by_value \
         FROM wt ORDER BY id",
    )
    .await;
    assert!(
        message.contains("Cannot cast string") && message.contains("1 DAY"),
        "mixed datetime-interval + numeric-interval must keep the numeric Arrow cast, got: {message}"
    );
}

/// Window-ns type pin (W-4 / #79): the temporal seed is µs end-to-end, not ns.
#[tokio::test]
async fn temporal_range_timestamp_seed_is_microseconds() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_timestamp_seed(&ctx);
    let frame = execute(&ctx, &catalogs, "SELECT ts FROM wt LIMIT 1")
        .await
        .expect("SELECT ts from the temporal seed must plan");
    let data_type = frame.schema().field(0).data_type();
    let DataType::Timestamp(TimeUnit::Microsecond, _) = data_type else {
        panic!("temporal seed ts must be Timestamp(Microsecond, _), got {data_type}");
    };
}
