use super::*;

use arrow::array::DurationMicrosecondArray;
use datafusion::prelude::{SessionConfig, SessionContext};

const LONG_MAX_DAYS: i64 = 106_751_991;

fn ctx(ansi: bool) -> SessionContext {
    let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi);
    let ctx = SessionContext::new_with_config(config);
    crate::register_all(&ctx);
    ctx
}

async fn micros(ctx: &SessionContext, sql: &str) -> Vec<Option<i64>> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .unwrap()
            .clone();
        out.extend(column.iter());
    }
    out
}

async fn error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
    }
}

async fn nullable(ctx: &SessionContext, sql: &str) -> bool {
    let frame = ctx.sql(sql).await.unwrap();
    frame.schema().field(0).is_nullable()
}

#[tokio::test]
async fn overflow_raises_in_both_modes() {
    for ansi in [false, true] {
        let ctx = ctx(ansi);
        for sql in [
            "SELECT make_dt_interval(2147483647) AS r",
            "SELECT make_dt_interval(106751992) AS r",
            "SELECT make_dt_interval(106751991, 5) AS r",
            "SELECT make_dt_interval(106751991, 4, 1) AS r",
            "SELECT make_dt_interval(106751991, 4, 0, CAST(54.775808 AS DECIMAL(8, 6))) AS r",
            "SELECT make_dt_interval(-106751991, -4, 0, CAST(-54.775809 AS DECIMAL(8, 6))) AS r",
            "SELECT make_dt_interval(106751991, 5, -60, 0) AS r",
            "SELECT make_dt_interval(-2147483648) AS r",
        ] {
            let text = error(&ctx, sql).await;
            assert!(
                text.contains("[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION]"),
                "{sql} (ansi {ansi}): {text}"
            );
        }
    }
}

#[tokio::test]
async fn overflow_raises_over_a_column() {
    let ctx = ctx(false);
    ctx.sql("CREATE TABLE t (d INT NOT NULL) AS VALUES (1), (2147483647)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = error(&ctx, "SELECT make_dt_interval(d) AS r FROM t").await;
    assert!(
        text.contains("[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION]"),
        "{text}"
    );
}

#[tokio::test]
async fn in_range_values_answer() {
    let ctx = ctx(true);
    let cases: [(&str, i64); 7] = [
        ("SELECT make_dt_interval() AS r", 0),
        (
            "SELECT make_dt_interval(0, 2147483647) AS r",
            2_147_483_647 * MICROS_PER_HOUR,
        ),
        (
            "SELECT make_dt_interval(0, 0, 2147483647) AS r",
            2_147_483_647 * MICROS_PER_MINUTE,
        ),
        (
            "SELECT make_dt_interval(106751991) AS r",
            LONG_MAX_DAYS * MICROS_PER_DAY,
        ),
        (
            "SELECT make_dt_interval(106751991, 4, 0, CAST(54.775807 AS DECIMAL(8, 6))) AS r",
            i64::MAX,
        ),
        (
            "SELECT make_dt_interval(-106751991, -4, 0, CAST(-54.775808 AS DECIMAL(8, 6))) AS r",
            i64::MIN,
        ),
        (
            "SELECT make_dt_interval(1, 2, 3, CAST(4.5 AS DOUBLE)) AS r",
            MICROS_PER_DAY + 2 * MICROS_PER_HOUR + 3 * MICROS_PER_MINUTE + 4_500_000,
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(micros(&ctx, sql).await, vec![Some(expected)], "{sql}");
    }
}

#[tokio::test]
async fn non_finite_and_null_seconds_answer_null() {
    for ansi in [false, true] {
        let ctx = ctx(ansi);
        for sql in [
            "SELECT make_dt_interval(0, 0, 0, CAST('NaN' AS DOUBLE)) AS r",
            "SELECT make_dt_interval(0, 0, 0, CAST('Infinity' AS DOUBLE)) AS r",
            "SELECT make_dt_interval(0, 0, 0, NULL) AS r",
            "SELECT make_dt_interval(NULL, 0, 0, 0) AS r",
        ] {
            assert_eq!(micros(&ctx, sql).await, vec![None], "{sql} (ansi {ansi})");
        }
    }
}

#[tokio::test]
async fn seconds_beyond_decimal_18_6_follow_the_mode() {
    let sql = "SELECT make_dt_interval(0, 0, 0, CAST(1E12 AS DOUBLE)) AS r";
    assert_eq!(micros(&ctx(false), sql).await, vec![None]);
    let text = error(&ctx(true), sql).await;
    assert!(
        text.contains("[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]"),
        "{text}"
    );
}

#[tokio::test]
async fn nullability_follows_the_seconds_cast() {
    let ctx = ctx(false);
    let cases = [
        ("SELECT make_dt_interval(1) AS r", false),
        (
            "SELECT make_dt_interval(0, 0, 0, CAST(5 AS INT)) AS r",
            false,
        ),
        (
            "SELECT make_dt_interval(0, 0, 0, CAST(1 AS DECIMAL(12, 0))) AS r",
            false,
        ),
        (
            "SELECT make_dt_interval(0, 0, 0, CAST(1 AS DECIMAL(18, 7))) AS r",
            false,
        ),
        (
            "SELECT make_dt_interval(0, 0, 0, CAST(1 AS DECIMAL(13, 0))) AS r",
            true,
        ),
        (
            "SELECT make_dt_interval(0, 0, 0, CAST(1 AS DECIMAL(19, 7))) AS r",
            true,
        ),
        (
            "SELECT make_dt_interval(0, 0, 0, CAST(1 AS BIGINT)) AS r",
            true,
        ),
        (
            "SELECT make_dt_interval(0, 0, 0, CAST(1.5 AS DOUBLE)) AS r",
            true,
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(nullable(&ctx, sql).await, expected, "{sql}");
    }
}

#[tokio::test]
async fn five_arguments_name_wrong_num_args() {
    let text = error(&ctx(false), "SELECT make_dt_interval(1, 2, 3, 4, 5) AS r").await;
    assert!(
        text.contains("[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]"),
        "{text}"
    );
}

#[test]
fn float_seconds_round_half_up_on_the_shortest_decimal() {
    assert_eq!(float_unscaled(0.123_456_5), Some(123_457));
    assert_eq!(float_unscaled(0.123_456_499_9), Some(123_456));
    assert_eq!(float_unscaled(1.000_000_5), Some(1_000_001));
    assert_eq!(float_unscaled(-5.0e-7), Some(-1));
    assert_eq!(float_unscaled(2.5e-7), Some(0));
    assert_eq!(float_unscaled(1e-300), Some(0));
    assert_eq!(
        float_unscaled(999_999_999_999.999_9),
        Some(999_999_999_999_999_900)
    );
    assert_eq!(float_unscaled(1e12), None);
    assert_eq!(float_unscaled(-1e12), None);
}

#[test]
fn decimal_seconds_rescale_with_half_up() {
    assert_eq!(rescale(1_234_567, 7), Some(123_457));
    assert_eq!(rescale(-1_234_565, 7), Some(-123_457));
    assert_eq!(rescale(9_999_999_999_999_999_995, 7), None);
    assert_eq!(
        rescale(999_999_999_999_999_999, 6),
        Some(999_999_999_999_999_999)
    );
    assert_eq!(rescale(45, 1), Some(4_500_000));
}

#[test]
fn day_time_micros_adds_in_spark_order() {
    assert!(day_time_micros(LONG_MAX_DAYS, 5, -60, 0).is_err());
    assert_eq!(
        day_time_micros(LONG_MAX_DAYS, 4, 0, 54_775_807).ok(),
        Some(i64::MAX)
    );
    assert_eq!(
        day_time_micros(0, 2_147_483_647, 0, 0).ok(),
        Some(2_147_483_647 * MICROS_PER_HOUR)
    );
}

#[test]
fn decimal_text_renders_plain() {
    assert_eq!(
        decimal_text(-9_999_999_999_999_999_995, 7),
        "-999999999999.9999995"
    );
    assert_eq!(decimal_text(5, 2), "0.05");
    assert_eq!(decimal_text(12, 0), "12");
}
