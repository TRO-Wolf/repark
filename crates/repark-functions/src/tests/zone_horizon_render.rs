use arrow::array::timezone::Tz;
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::ansi::with_spark_ansi_config;
use crate::datetime::{local_datetime_from_micros, micros_from_local_datetime, offset_at_instant};
use crate::session_time_zone::with_session_time_zone;
use crate::spark_string_timestamp::string_to_timestamp_micros;

const NEW_YORK: &str = "America/New_York";
const SYDNEY: &str = "Australia/Sydney";
const LORD_HOWE: &str = "Australia/Lord_Howe";
const KOLKATA: &str = "Asia/Kolkata";
const WALL_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

const SPARK_WALLS: [(&str, &str, &str); 40] = [
    (NEW_YORK, "2099-03-08 06:59:59", "2099-03-08 01:59:59"),
    (NEW_YORK, "2099-03-08 07:00:00", "2099-03-08 03:00:00"),
    (NEW_YORK, "2099-07-15 12:34:56", "2099-07-15 08:34:56"),
    (NEW_YORK, "2100-01-15 12:34:56", "2100-01-15 07:34:56"),
    (NEW_YORK, "2100-03-14 06:59:59", "2100-03-14 01:59:59"),
    (NEW_YORK, "2100-03-14 07:00:00", "2100-03-14 03:00:00"),
    (NEW_YORK, "2100-07-15 12:34:56", "2100-07-15 08:34:56"),
    (NEW_YORK, "2100-11-07 05:59:59", "2100-11-07 01:59:59"),
    (NEW_YORK, "2100-11-07 06:00:00", "2100-11-07 01:00:00"),
    (NEW_YORK, "2104-03-09 06:59:59", "2104-03-09 01:59:59"),
    (NEW_YORK, "2104-03-09 07:00:00", "2104-03-09 03:00:00"),
    (NEW_YORK, "2104-11-02 05:59:59", "2104-11-02 01:59:59"),
    (NEW_YORK, "2104-11-02 06:00:00", "2104-11-02 01:00:00"),
    (NEW_YORK, "2500-03-14 07:00:00", "2500-03-14 03:00:00"),
    (NEW_YORK, "2500-11-07 06:00:00", "2500-11-07 01:00:00"),
    (NEW_YORK, "9999-03-14 06:59:59", "9999-03-14 01:59:59"),
    (NEW_YORK, "9999-03-14 07:00:00", "9999-03-14 03:00:00"),
    (NEW_YORK, "9999-11-07 06:00:00", "9999-11-07 01:00:00"),
    (SYDNEY, "2099-07-15 12:34:56", "2099-07-15 22:34:56"),
    (SYDNEY, "2100-01-15 12:34:56", "2100-01-15 23:34:56"),
    (SYDNEY, "2100-04-03 15:59:59", "2100-04-04 02:59:59"),
    (SYDNEY, "2100-04-03 16:00:00", "2100-04-04 02:00:00"),
    (SYDNEY, "2100-07-15 12:34:56", "2100-07-15 22:34:56"),
    (SYDNEY, "2100-10-02 15:59:59", "2100-10-03 01:59:59"),
    (SYDNEY, "2100-10-02 16:00:00", "2100-10-03 03:00:00"),
    (SYDNEY, "2104-04-05 16:00:00", "2104-04-06 02:00:00"),
    (SYDNEY, "2104-10-04 16:00:00", "2104-10-05 03:00:00"),
    (SYDNEY, "2500-04-03 16:00:00", "2500-04-04 02:00:00"),
    (SYDNEY, "9999-10-02 16:00:00", "9999-10-03 03:00:00"),
    (LORD_HOWE, "2100-04-03 14:59:59", "2100-04-04 01:59:59"),
    (LORD_HOWE, "2100-04-03 15:00:00", "2100-04-04 01:30:00"),
    (LORD_HOWE, "2100-07-15 12:34:56", "2100-07-15 23:04:56"),
    (LORD_HOWE, "2100-10-02 15:29:59", "2100-10-03 01:59:59"),
    (LORD_HOWE, "2100-10-02 15:30:00", "2100-10-03 02:30:00"),
    (LORD_HOWE, "2104-04-05 15:00:00", "2104-04-06 01:30:00"),
    (LORD_HOWE, "2500-10-02 15:30:00", "2500-10-03 02:30:00"),
    (LORD_HOWE, "9999-04-03 15:00:00", "9999-04-04 01:30:00"),
    (KOLKATA, "2100-07-15 12:34:56", "2100-07-15 18:04:56"),
    (KOLKATA, "9999-01-15 12:34:56", "9999-01-15 18:04:56"),
    ("UTC", "2100-07-15 12:34:56", "2100-07-15 12:34:56"),
];

const SPARK_INSTANTS: [(&str, &str, i64); 12] = [
    (NEW_YORK, "2100-07-15 12:00:00", 4_119_350_400_000_000),
    (NEW_YORK, "2100-03-14 02:30:00", 4_108_692_600_000_000),
    (NEW_YORK, "2100-11-07 01:30:00", 4_129_248_600_000_000),
    (NEW_YORK, "2500-07-15 12:00:00", 16_742_131_200_000_000),
    (NEW_YORK, "2500-03-14 02:30:00", 16_731_473_400_000_000),
    (SYDNEY, "2100-07-15 12:00:00", 4_119_300_000_000_000),
    (SYDNEY, "2100-04-04 02:30:00", 4_110_449_400_000_000),
    (SYDNEY, "2100-10-03 02:30:00", 4_126_177_800_000_000),
    (LORD_HOWE, "2100-07-15 12:00:00", 4_119_298_200_000_000),
    (LORD_HOWE, "2100-04-04 01:45:00", 4_110_446_700_000_000),
    (LORD_HOWE, "2100-10-03 02:15:00", 4_126_175_100_000_000),
    (KOLKATA, "2100-07-15 12:00:00", 4_119_316_200_000_000),
];

fn zone(name: &str) -> Tz {
    name.parse::<Tz>().expect("test zone parses")
}

fn naive(text: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(text, WALL_FORMAT).expect("a wall clock")
}

fn utc_micros(text: &str) -> i64 {
    naive(text).and_utc().timestamp_micros()
}

fn fixed_now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
        .expect("fixed clock parses")
        .with_timezone(&Utc)
}

fn context(session_zone: &str) -> SessionContext {
    let config = with_spark_ansi_config(
        with_session_time_zone(SessionConfig::new(), session_zone),
        true,
    );
    let ctx = SessionContext::new_with_config(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

async fn row(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx
        .sql(sql)
        .await
        .expect("the statement plans")
        .collect()
        .await
        .expect("the statement runs");
    let batch: RecordBatch = batches.into_iter().next().expect("one batch");
    batch
        .columns()
        .iter()
        .map(|column| {
            let text = cast(column.as_ref(), &DataType::Utf8).expect("a printable column");
            let text = text.as_string::<i32>();
            if text.is_null(0) {
                "NULL".to_string()
            } else {
                text.value(0).to_string()
            }
        })
        .collect()
}

#[test]
fn every_recorded_instant_renders_at_the_wall_clock_spark_gives() {
    for (session, instant, expected) in SPARK_WALLS {
        let wall = local_datetime_from_micros(utc_micros(instant), zone(session))
            .map(|wall| wall.format(WALL_FORMAT).to_string());
        assert_eq!(wall.as_deref(), Some(expected), "{session} {instant}");
    }
}

#[test]
fn every_recorded_wall_clock_is_placed_at_the_instant_spark_gives() {
    for (session, wall, expected) in SPARK_INSTANTS {
        assert_eq!(
            micros_from_local_datetime(naive(wall), zone(session), None),
            Some(expected),
            "{session} {wall}"
        );
        assert_eq!(
            string_to_timestamp_micros(wall, zone(session), fixed_now()),
            Some(expected),
            "{session} {wall}"
        );
    }
}

#[test]
fn the_wall_of_an_instant_reads_back_as_the_instant_in_real_zones() {
    let first = utc_micros("2100-01-01 00:00:00");
    let last = utc_micros("2501-01-01 00:00:00");
    let step = (86_400 * 11 + 3_600 * 7 + 1_861) * 1_000_000_i64;
    for session in [
        NEW_YORK,
        SYDNEY,
        LORD_HOWE,
        KOLKATA,
        "UTC",
        "Europe/London",
        "America/Santiago",
        "Pacific/Auckland",
    ] {
        let named = zone(session);
        let mut checked = 0_u32;
        let mut plain = 0_u32;
        let mut instant = first;
        while instant < last {
            let wall = local_datetime_from_micros(instant, named).expect("a wall clock");
            let offset = offset_at_instant(instant, named);
            assert_eq!(
                micros_from_local_datetime(wall, named, offset),
                Some(instant),
                "{session} {instant}"
            );
            let unhinted = micros_from_local_datetime(wall, named, None);
            let literal = string_to_timestamp_micros(
                &wall.format(WALL_FORMAT).to_string(),
                named,
                fixed_now(),
            );
            assert_eq!(unhinted, literal, "{session} {instant}");
            plain += u32::from(unhinted == Some(instant));
            checked += 1;
            instant += step;
        }
        assert!(checked > 12_000, "{session} {checked}");
        assert!(checked - plain < 20, "{session} {plain} of {checked}");
    }
}

#[test]
fn a_time_only_string_takes_today_from_the_final_rule() {
    let now = DateTime::parse_from_rfc3339("2100-07-01T03:30:00Z")
        .expect("fixed clock parses")
        .with_timezone(&Utc);
    assert_eq!(
        string_to_timestamp_micros("23:45:00", zone(NEW_YORK), now),
        Some(utc_micros("2100-07-01 03:45:00"))
    );
    assert_eq!(
        string_to_timestamp_micros(
            "23:45:00",
            zone(NEW_YORK),
            now + chrono::TimeDelta::hours(1)
        ),
        Some(utc_micros("2100-07-02 03:45:00"))
    );
}

#[tokio::test]
async fn the_three_expressions_of_the_card_answer_noon() {
    let sql = |year: i32| {
        format!(
            "SELECT CAST(t AS STRING), hour(t), date_format(t, 'HH:mm') \
             FROM (SELECT TIMESTAMP '{year}-07-01 12:00:00' AS t)"
        )
    };
    for session in [NEW_YORK, SYDNEY, LORD_HOWE, KOLKATA, "UTC", "-08:00"] {
        let ctx = context(session);
        for year in [2099, 2100, 2104, 2500, 9999] {
            assert_eq!(
                row(&ctx, &sql(year)).await,
                vec![
                    format!("{year}-07-01 12:00:00"),
                    "12".to_string(),
                    "12:00".to_string()
                ],
                "{session} {year}"
            );
        }
    }
}

#[tokio::test]
async fn every_extractor_reads_the_final_rule_in_new_york() {
    let ctx = context(NEW_YORK);
    let cells = [
        ("CAST(t AS STRING)", "2100-07-15 08:34:56"),
        ("CAST(CAST(t AS DATE) AS STRING)", "2100-07-15"),
        ("hour(t)", "8"),
        ("minute(t)", "34"),
        ("second(t)", "56"),
        ("year(t)", "2100"),
        ("month(t)", "7"),
        ("dayofmonth(t)", "15"),
        ("dayofweek(t)", "5"),
        ("dayofyear(t)", "196"),
        ("weekofyear(t)", "28"),
        ("quarter(t)", "3"),
        ("weekday(t)", "3"),
        (
            "date_format(t, 'yyyy-MM-dd HH:mm:ss')",
            "2100-07-15 08:34:56",
        ),
        ("to_char(t, 'yyyy-MM-dd HH:mm:ss')", "2100-07-15 08:34:56"),
        ("unix_micros(date_trunc('DAY', t))", "4119307200000000"),
        ("unix_micros(date_trunc('MONTH', t))", "4118097600000000"),
        ("unix_micros(date_trunc('YEAR', t))", "4102462800000000"),
        ("CAST(trunc(t, 'MM') AS STRING)", "2100-07-01"),
        ("from_unixtime(4119338096)", "2100-07-15 08:34:56"),
        (
            "to_json(named_struct('t', t))",
            "{\"t\":\"2100-07-15T08:34:56.000-04:00\"}",
        ),
    ];
    for (expression, expected) in cells {
        let sql =
            format!("SELECT {expression} FROM (SELECT TIMESTAMP '2100-07-15 12:34:56 UTC' AS t)");
        assert_eq!(
            row(&ctx, &sql).await,
            vec![expected.to_string()],
            "{expression}"
        );
    }
}

#[tokio::test]
async fn every_constructor_reads_the_final_rule_in_new_york() {
    let ctx = context(NEW_YORK);
    let cells = [
        (
            "unix_micros(TIMESTAMP '2100-07-15 12:00:00')",
            "4119350400000000",
        ),
        (
            "unix_micros(to_timestamp('2100-07-15 12:00:00', 'yyyy-MM-dd HH:mm:ss'))",
            "4119350400000000",
        ),
        ("unix_timestamp('2100-07-15 12:00:00')", "4119350400"),
        (
            "to_unix_timestamp('2100-07-15 12:00:00', 'yyyy-MM-dd HH:mm:ss')",
            "4119350400",
        ),
        (
            "unix_micros(make_timestamp(2100, 7, 15, 12, 0, 0))",
            "4119350400000000",
        ),
        (
            "unix_micros(make_timestamp(2100, 7, 15, 12, 0, 0, 'America/New_York'))",
            "4119350400000000",
        ),
        (
            "unix_micros(CAST(DATE '2100-07-15' AS TIMESTAMP))",
            "4119307200000000",
        ),
    ];
    for (expression, expected) in cells {
        let sql = format!("SELECT {expression}");
        assert_eq!(
            row(&ctx, &sql).await,
            vec![expected.to_string()],
            "{expression}"
        );
    }
}

#[tokio::test]
async fn a_batch_reads_the_tables_only_when_every_instant_is_inside_them() {
    let ctx = context(NEW_YORK);
    let fields = "min(hour(t)), max(hour(t)), min(dayofweek(t)), max(year(t)), count(*)";
    let inside = "(TIMESTAMP '2024-07-01 12:00:00'), (TIMESTAMP '2099-07-01 12:00:00')";
    let mixed = "(TIMESTAMP '2024-07-01 12:00:00'), (TIMESTAMP '2100-07-01 12:00:00')";
    let outside = "(TIMESTAMP '2100-07-01 12:00:00'), (TIMESTAMP '2500-07-01 12:00:00')";
    for (values, weekday, year) in [
        (inside, "2", "2099"),
        (mixed, "2", "2100"),
        (outside, "5", "2500"),
    ] {
        let sql = format!("SELECT {fields} FROM (VALUES {values}) AS v(t)");
        assert_eq!(
            row(&ctx, &sql).await,
            vec!["12", "12", weekday, year, "2"],
            "{values}"
        );
    }
}

#[tokio::test]
async fn an_offset_with_seconds_prints_main_text_before_2100() {
    let cells = [
        (
            NEW_YORK,
            "1850-06-15 12:00:00 UTC",
            "{\"t\":\"1850-06-15T07:03:58.000-04:56\"}",
        ),
        (
            NEW_YORK,
            "1883-11-18 17:00:00 UTC",
            "{\"t\":\"1883-11-18T12:00:00.000-05:00\"}",
        ),
        (
            NEW_YORK,
            "0001-06-15 12:00:00 UTC",
            "{\"t\":\"0001-06-15T07:03:58.000-04:56\"}",
        ),
        (
            "Europe/Paris",
            "1850-06-15 12:00:00 UTC",
            "{\"t\":\"1850-06-15T12:09:21.000+00:09\"}",
        ),
        (
            "Europe/Paris",
            "1883-11-18 17:00:00 UTC",
            "{\"t\":\"1883-11-18T17:09:21.000+00:09\"}",
        ),
        (
            "Europe/Paris",
            "0001-06-15 12:00:00 UTC",
            "{\"t\":\"0001-06-15T12:09:21.000+00:09\"}",
        ),
        (
            KOLKATA,
            "1850-06-15 12:00:00 UTC",
            "{\"t\":\"1850-06-15T17:53:28.000+05:53\"}",
        ),
        (
            KOLKATA,
            "1883-11-18 17:00:00 UTC",
            "{\"t\":\"1883-11-18T22:21:10.000+05:21\"}",
        ),
        (
            KOLKATA,
            "0001-06-15 12:00:00 UTC",
            "{\"t\":\"0001-06-15T17:53:28.000+05:53\"}",
        ),
        (
            SYDNEY,
            "1850-06-15 12:00:00 UTC",
            "{\"t\":\"1850-06-15T22:04:52.000+10:05\"}",
        ),
        (
            SYDNEY,
            "1883-11-18 17:00:00 UTC",
            "{\"t\":\"1883-11-19T03:04:52.000+10:05\"}",
        ),
        (
            SYDNEY,
            "0001-06-15 12:00:00 UTC",
            "{\"t\":\"0001-06-15T22:04:52.000+10:05\"}",
        ),
    ];
    for (session, instant, expected) in cells {
        let ctx = context(session);
        let direct = format!("SELECT to_json(named_struct('t', TIMESTAMP '{instant}'))");
        assert_eq!(
            row(&ctx, &direct).await,
            vec![expected.to_string()],
            "{session} {instant}"
        );
        let round = format!(
            "SELECT to_json(from_json(to_json(named_struct('t', TIMESTAMP '{instant}')), 't TIMESTAMP'))"
        );
        let answered = row(&ctx, &round).await;
        assert_ne!(answered, vec!["{}".to_string()], "{session} {instant}");
    }
}

#[tokio::test]
async fn a_zone_without_a_transition_in_2099_reads_the_table_end() {
    for session in ["Africa/Casablanca", "Africa/El_Aaiun"] {
        let ctx = context(session);
        let sql = "SELECT CAST(TIMESTAMP '2112-09-11 12:34:56 UTC' AS STRING), hour(TIMESTAMP '2112-09-11 12:34:56 UTC')";
        assert_eq!(
            row(&ctx, sql).await,
            vec!["2112-09-11 13:34:56".to_string(), "13".to_string()],
            "{session}"
        );
    }
}

#[tokio::test]
async fn unix_timestamp_of_an_overlap_answers_the_earlier_offset() {
    let ctx = context(NEW_YORK);
    for (wall, expected) in [
        ("2024-11-03 01:30:00", "1730611800"),
        ("2100-11-07 01:30:00", "4129248600"),
    ] {
        let sql = format!("SELECT unix_timestamp('{wall}')");
        assert_eq!(row(&ctx, &sql).await, vec![expected.to_string()], "{wall}");
    }
}
