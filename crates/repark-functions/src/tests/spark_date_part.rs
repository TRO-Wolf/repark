use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Date32Array, DictionaryArray, Int32Array, IntervalDayTimeArray,
    IntervalYearMonthArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, IntervalDayTime, Schema, TimeUnit};
use chrono::DateTime;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::ansi::with_spark_ansi_config;
use crate::registration::analyze_eagerly;
use crate::session_time_zone::with_session_time_zone;

const NEW_YORK: &str = "America/New_York";
const KOLKATA: &str = "Asia/Kolkata";
const UTC: &str = "UTC";

fn spark_context(zone: &str, ansi: bool) -> SessionContext {
    let config = with_spark_ansi_config(with_session_time_zone(SessionConfig::new(), zone), ansi);
    let ctx = crate::spark_type_planner::planned_context(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

fn utc_micros(rfc3339: &str) -> i64 {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("valid rfc3339 timestamp")
        .timestamp_micros()
}

fn register_timestamps(
    ctx: &SessionContext,
    table: &str,
    values: &[Option<&str>],
    tz: Option<&str>,
) {
    let ticks: Vec<Option<i64>> = values.iter().map(|v| v.map(utc_micros)).collect();
    let ids = Int32Array::from_iter_values(0..i32::try_from(values.len()).expect("row count"));
    let ts = match tz {
        Some(zone) => TimestampMicrosecondArray::from(ticks).with_timezone(zone),
        None => TimestampMicrosecondArray::from(ticks),
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, tz.map(Arc::from)),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(ts)]).expect("batch");
    ctx.register_batch(table, batch).expect("register table");
}

async fn run_int32_row(ctx: &SessionContext, sql: &str) -> Vec<Option<i32>> {
    let df = ctx.sql(sql).await.expect("plan sql");
    let analyzed = analyze_eagerly(&ctx.state(), df.into_unoptimized_plan()).expect("analyze");
    let batches = ctx
        .execute_logical_plan(analyzed)
        .await
        .expect("execute")
        .collect()
        .await
        .expect("collect");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    (0..batch.num_columns())
        .map(|index| {
            let col = batch.column(index);
            assert_eq!(col.data_type(), &DataType::Int32, "col {index}");
            if col.is_null(0) {
                None
            } else {
                Some(col.as_primitive::<Int32Type>().value(0))
            }
        })
        .collect()
}

async fn run_int32_col(ctx: &SessionContext, sql: &str) -> Vec<Option<i32>> {
    let df = ctx.sql(sql).await.expect("plan sql");
    let analyzed = analyze_eagerly(&ctx.state(), df.into_unoptimized_plan()).expect("analyze");
    let batches = ctx
        .execute_logical_plan(analyzed)
        .await
        .expect("execute")
        .collect()
        .await
        .expect("collect");
    let mut out = Vec::new();
    for batch in batches {
        let col = batch.column(0);
        assert_eq!(col.data_type(), &DataType::Int32);
        let arr = col.as_primitive::<Int32Type>();
        for row in 0..arr.len() {
            out.push(if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row))
            });
        }
    }
    out
}

#[tokio::test]
async fn date_part_datepart_and_extract_read_session_zone_on_card_row() {
    for ansi in [false, true] {
        let ny = spark_context(NEW_YORK, ansi);
        register_timestamps(&ny, "t", &[Some("2024-01-01T02:00:00Z")], Some("UTC"));
        let expected_ny = vec![
            Some(2023),
            Some(2023),
            Some(4),
            Some(12),
            Some(52),
            Some(31),
            Some(1),
            Some(1),
            Some(7),
            Some(7),
            Some(365),
            Some(21),
            Some(0),
            Some(0),
        ];
        let dp = run_int32_row(
            &ny,
            "SELECT date_part('year', ts), date_part('yearofweek', ts), date_part('quarter', ts), \
             date_part('month', ts), date_part('week', ts), date_part('day', ts), \
             date_part('dayofweek', ts), date_part('dow', ts), date_part('dayofweek_iso', ts), \
             date_part('dow_iso', ts), date_part('doy', ts), date_part('hour', ts), \
             date_part('minute', ts), date_part('second', ts) FROM t",
        )
        .await;
        assert_eq!(dp, expected_ny);

        let dpa = run_int32_row(
            &ny,
            "SELECT datepart('year', ts), datepart('yearofweek', ts), datepart('quarter', ts), \
             datepart('month', ts), datepart('week', ts), datepart('day', ts), \
             datepart('dayofweek', ts), datepart('dow', ts), datepart('dayofweek_iso', ts), \
             datepart('dow_iso', ts), datepart('doy', ts), datepart('hour', ts), \
             datepart('minute', ts), datepart('second', ts) FROM t",
        )
        .await;
        assert_eq!(dpa, expected_ny);

        let ex = run_int32_row(
            &ny,
            "SELECT extract(YEAR FROM ts), extract(QUARTER FROM ts), \
             extract(MONTH FROM ts), extract(WEEK FROM ts), extract(DAY FROM ts), \
             extract(DAYOFWEEK FROM ts), extract(DOW FROM ts), \
             extract(DOY FROM ts), extract(HOUR FROM ts), extract(MINUTE FROM ts), \
             extract(SECOND FROM ts) FROM t",
        )
        .await;
        assert_eq!(
            ex,
            vec![
                Some(2023),
                Some(4),
                Some(12),
                Some(52),
                Some(31),
                Some(1),
                Some(1),
                Some(365),
                Some(21),
                Some(0),
                Some(0),
            ]
        );

        let utc = spark_context(UTC, ansi);
        register_timestamps(&utc, "t", &[Some("2024-01-01T02:00:00Z")], Some("UTC"));
        let utc_vals = run_int32_row(
            &utc,
            "SELECT date_part('year', ts), date_part('month', ts), date_part('day', ts), \
             date_part('dayofweek', ts), extract(DOW FROM ts), extract(HOUR FROM ts) FROM t",
        )
        .await;
        assert_eq!(
            utc_vals,
            vec![Some(2024), Some(1), Some(1), Some(2), Some(2), Some(2)]
        );
    }
}

#[tokio::test]
async fn all_spark_field_aliases_resolve_in_session_zone() {
    let ctx = spark_context(NEW_YORK, true);
    register_timestamps(&ctx, "t", &[Some("2024-01-01T02:04:05Z")], Some("UTC"));
    let cases = [
        ("year", 2023),
        ("y", 2023),
        ("years", 2023),
        ("yr", 2023),
        ("yrs", 2023),
        ("yearofweek", 2023),
        ("quarter", 4),
        ("qtr", 4),
        ("month", 12),
        ("mon", 12),
        ("mons", 12),
        ("months", 12),
        ("week", 52),
        ("w", 52),
        ("weeks", 52),
        ("day", 31),
        ("d", 31),
        ("days", 31),
        ("dayofweek", 1),
        ("dow", 1),
        ("dayofweek_iso", 7),
        ("dow_iso", 7),
        ("doy", 365),
        ("hour", 21),
        ("h", 21),
        ("hours", 21),
        ("hr", 21),
        ("hrs", 21),
        ("minute", 4),
        ("m", 4),
        ("min", 4),
        ("mins", 4),
        ("minutes", 4),
        ("second", 5),
        ("s", 5),
        ("sec", 5),
        ("secs", 5),
        ("seconds", 5),
    ];
    let exprs: Vec<String> = cases
        .iter()
        .map(|(field, _)| format!("date_part('{field}', ts)"))
        .collect();
    let actual = run_int32_row(&ctx, &format!("SELECT {} FROM t", exprs.join(", "))).await;
    let expected: Vec<Option<i32>> = cases.iter().map(|(_, value)| Some(*value)).collect();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn kolkata_half_hour_zone_shifts_date_and_minute() {
    let kolkata = spark_context(KOLKATA, true);
    register_timestamps(
        &kolkata,
        "t",
        &[Some("2024-01-01T18:45:00Z"), Some("1969-12-31T23:30:00Z")],
        Some("UTC"),
    );
    assert_eq!(
        run_int32_col(&kolkata, "SELECT date_part('day', ts) FROM t ORDER BY id").await,
        vec![Some(2), Some(1)]
    );
    assert_eq!(
        run_int32_col(&kolkata, "SELECT date_part('hour', ts) FROM t ORDER BY id").await,
        vec![Some(0), Some(5)]
    );
    assert_eq!(
        run_int32_col(
            &kolkata,
            "SELECT extract(MINUTE FROM ts) FROM t ORDER BY id"
        )
        .await,
        vec![Some(15), Some(0)]
    );
    assert_eq!(
        run_int32_col(&kolkata, "SELECT extract(YEAR FROM ts) FROM t ORDER BY id").await,
        vec![Some(2024), Some(1970)]
    );
}

#[tokio::test]
async fn new_york_dst_lmt_leap_post_2038_and_post_2099_zones() {
    let ny = spark_context(NEW_YORK, true);
    register_timestamps(
        &ny,
        "t",
        &[
            Some("2024-03-01T02:15:00Z"),
            Some("2024-03-10T06:59:59Z"),
            Some("2024-03-10T07:00:00Z"),
            Some("2024-11-03T05:30:00Z"),
            Some("2024-11-03T06:30:00Z"),
            Some("1880-06-15T12:00:00Z"),
            Some("2040-01-01T02:30:00Z"),
            Some("2100-07-01T03:00:00Z"),
            Some("2500-01-01T02:00:00Z"),
        ],
        Some("UTC"),
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT date_part('hour', ts) FROM t ORDER BY id").await,
        vec![
            Some(21),
            Some(1),
            Some(3),
            Some(1),
            Some(1),
            Some(7),
            Some(21),
            Some(23),
            Some(21),
        ]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT extract(DAY FROM ts) FROM t ORDER BY id").await,
        vec![
            Some(29),
            Some(10),
            Some(10),
            Some(3),
            Some(3),
            Some(15),
            Some(31),
            Some(30),
            Some(31),
        ]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT date_part('minute', ts) FROM t ORDER BY id").await,
        vec![
            Some(15),
            Some(59),
            Some(0),
            Some(30),
            Some(30),
            Some(3),
            Some(30),
            Some(0),
            Some(0),
        ]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT date_part('second', ts) FROM t ORDER BY id").await,
        vec![
            Some(0),
            Some(59),
            Some(0),
            Some(0),
            Some(0),
            Some(58),
            Some(0),
            Some(0),
            Some(0),
        ]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT extract(YEAR FROM ts) FROM t ORDER BY id").await,
        vec![
            Some(2024),
            Some(2024),
            Some(2024),
            Some(2024),
            Some(2024),
            Some(1880),
            Some(2039),
            Some(2100),
            Some(2499),
        ]
    );
    assert_eq!(
        run_int32_row(
            &ny,
            "SELECT date_part('hour', TIMESTAMP '2024-03-10 02:30:00'), \
             extract(HOUR FROM TIMESTAMP '2024-03-10 02:30:00')"
        )
        .await,
        vec![Some(3), Some(3)]
    );
}

#[tokio::test]
async fn dictionary_encoded_non_utc_stored_zone_and_all_time_units() {
    let ny = spark_context(NEW_YORK, true);

    let values = Arc::new(
        TimestampMicrosecondArray::from(vec![utc_micros("2024-01-01T02:00:00Z")])
            .with_timezone("UTC"),
    );
    let keys = Int32Array::from(vec![Some(0), None, Some(0)]);
    let dict = DictionaryArray::<Int32Type>::try_new(keys, values).expect("dict array");
    let dict_schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        dict.data_type().clone(),
        true,
    )]));
    let dict_batch = RecordBatch::try_new(dict_schema, vec![Arc::new(dict)]).expect("dict batch");
    ny.register_batch("t_dict", dict_batch)
        .expect("register dict");
    assert_eq!(
        run_int32_col(&ny, "SELECT date_part('hour', ts) FROM t_dict").await,
        vec![Some(21), None, Some(21)]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT extract(DOW FROM ts) FROM t_dict").await,
        vec![Some(1), None, Some(1)]
    );

    register_timestamps(
        &ny,
        "t_paris",
        &[Some("2024-01-01T02:00:00Z")],
        Some("Europe/Paris"),
    );
    assert_eq!(
        run_int32_row(
            &ny,
            "SELECT date_part('year', ts), date_part('hour', ts), extract(DAY FROM ts) FROM t_paris"
        )
        .await,
        vec![Some(2023), Some(21), Some(31)]
    );

    let instant_secs = utc_micros("2024-01-01T02:00:00Z") / 1_000_000;
    let units_schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts_s",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        ),
        Field::new(
            "ts_ms",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ]));
    let units_batch = RecordBatch::try_new(
        units_schema,
        vec![
            Arc::new(TimestampSecondArray::from(vec![instant_secs]).with_timezone("UTC")),
            Arc::new(
                TimestampMillisecondArray::from(vec![instant_secs * 1_000]).with_timezone("UTC"),
            ),
            Arc::new(
                TimestampNanosecondArray::from(vec![instant_secs * 1_000_000_000])
                    .with_timezone("UTC"),
            ),
        ],
    )
    .expect("units batch");
    ny.register_batch("t_units", units_batch)
        .expect("register units");
    assert_eq!(
        run_int32_row(
            &ny,
            "SELECT date_part('hour', ts_s), date_part('hour', ts_ms), date_part('hour', ts_ns), \
             extract(YEAR FROM ts_s), extract(YEAR FROM ts_ms), extract(YEAR FROM ts_ns) \
             FROM t_units"
        )
        .await,
        vec![
            Some(21),
            Some(21),
            Some(21),
            Some(2023),
            Some(2023),
            Some(2023)
        ]
    );
}

#[tokio::test]
async fn where_filter_groupby_and_join_use_session_zone_without_utc_preimage() {
    let ny = spark_context(NEW_YORK, true);
    register_timestamps(
        &ny,
        "events",
        &[
            Some("2024-01-01T02:00:00Z"),
            Some("2024-01-01T05:00:00Z"),
            Some("2024-03-01T02:15:00Z"),
            None,
        ],
        Some("UTC"),
    );

    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT id FROM events WHERE date_part('year', ts) = 2023 ORDER BY id"
        )
        .await,
        vec![Some(0)]
    );
    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT id FROM events WHERE extract(YEAR FROM ts) = 2023 ORDER BY id"
        )
        .await,
        vec![Some(0)]
    );
    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT id FROM events WHERE date_part('hour', ts) = 21 ORDER BY id"
        )
        .await,
        vec![Some(0), Some(2)]
    );
    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT id FROM events WHERE extract(HOUR FROM ts) = 21 ORDER BY id"
        )
        .await,
        vec![Some(0), Some(2)]
    );
    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT date_part('day', ts) AS d FROM events WHERE ts IS NOT NULL \
             GROUP BY date_part('day', ts) ORDER BY d"
        )
        .await,
        vec![Some(1), Some(29), Some(31)]
    );
    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT extract(DAY FROM ts) AS d FROM events WHERE ts IS NOT NULL \
             GROUP BY extract(DAY FROM ts) ORDER BY d"
        )
        .await,
        vec![Some(1), Some(29), Some(31)]
    );
    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT a.id FROM events a JOIN (SELECT 31 AS k UNION ALL SELECT 29 AS k) b \
             ON date_part('day', a.ts) = b.k ORDER BY a.id"
        )
        .await,
        vec![Some(0), Some(2)]
    );
    assert_eq!(
        run_int32_col(
            &ny,
            "SELECT a.id FROM events a JOIN (SELECT 31 AS k UNION ALL SELECT 29 AS k) b \
             ON extract(DAY FROM a.ts) = b.k ORDER BY a.id"
        )
        .await,
        vec![Some(0), Some(2)]
    );
}

#[tokio::test]
async fn ntz_date_null_invalid_and_non_temporal_controls() {
    let ny = spark_context(NEW_YORK, true);
    register_timestamps(&ny, "t_ntz", &[Some("2024-01-01T02:00:00Z")], None);
    assert_eq!(
        run_int32_row(
            &ny,
            "SELECT date_part('year', ts), date_part('day', ts), date_part('hour', ts), \
             date_part('dow', ts), extract(HOUR FROM ts), extract(DOW FROM ts), \
             extract(DAYOFWEEK FROM ts) FROM t_ntz"
        )
        .await,
        vec![
            Some(2024),
            Some(1),
            Some(2),
            Some(2),
            Some(2),
            Some(2),
            Some(2)
        ]
    );

    let date_schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
    let date_batch = RecordBatch::try_new(
        date_schema,
        vec![Arc::new(Date32Array::from(vec![Some(19_722), None]))],
    )
    .expect("date batch");
    ny.register_batch("t_date", date_batch)
        .expect("register date");
    assert_eq!(
        run_int32_col(&ny, "SELECT date_part('year', d) FROM t_date").await,
        vec![Some(2023), None]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT extract(DOW FROM d) FROM t_date").await,
        vec![Some(1), None]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT extract(DAYOFWEEK FROM d) FROM t_date").await,
        vec![Some(1), None]
    );
    assert_eq!(
        run_int32_col(&ny, "SELECT date_part('hour', d) FROM t_date").await,
        vec![Some(0), None]
    );

    assert_eq!(
        run_int32_row(
            &ny,
            "SELECT date_part('hour', NULL) AS a, extract(HOUR FROM NULL) AS b, \
             date_part('hour', CAST(NULL AS TIMESTAMP)) AS c, \
             date_part('hour', CAST(NULL AS DATE)) AS d"
        )
        .await,
        vec![None, None, None, None]
    );

    for bad in [
        "SELECT date_part(' year ', TIMESTAMP '2024-01-01 02:00:00 UTC')",
        "SELECT date_part('', TIMESTAMP '2024-01-01 02:00:00 UTC')",
        "SELECT date_part('bogus', TIMESTAMP '2024-01-01 02:00:00 UTC')",
    ] {
        let err = match ny.sql(bad).await {
            Err(err) => err.to_string(),
            Ok(df) => df
                .collect()
                .await
                .expect_err("invalid field must error")
                .to_string(),
        };
        assert!(err.contains("INVALID_EXTRACT_FIELD"), "{bad}: {err}");
    }

    assert_eq!(
        run_int32_row(&ny, "SELECT extract(YEAR FROM '2020-01-01')").await,
        vec![Some(2020)]
    );
    assert_eq!(
        run_int32_row(&ny, "SELECT extract(YEAR FROM INTERVAL '2' YEAR)").await,
        vec![Some(2)]
    );
}

async fn run_err(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(err) => err.to_string(),
        Ok(df) => df
            .collect()
            .await
            .expect_err(&format!("expected error for: {sql}"))
            .to_string(),
    }
}

#[tokio::test]
async fn a1_interval_inputs_follow_spark_semantics() {
    let ny_ansi = spark_context(NEW_YORK, true);

    assert_eq!(
        run_int32_row(
            &ny_ansi,
            "SELECT extract(YEAR FROM INTERVAL '2' YEAR), extract(MONTH FROM INTERVAL '14' MONTH), \
             extract(DAY FROM INTERVAL '5' DAY), extract(HOUR FROM INTERVAL '3' HOUR), \
             extract(MINUTE FROM INTERVAL '45' MINUTE), extract(SECOND FROM INTERVAL '12' SECOND)"
        )
        .await,
        vec![Some(2), Some(2), Some(5), Some(3), Some(45), Some(12)]
    );

    for refused_sql in [
        "SELECT date_part('day', INTERVAL '2' YEAR)",
        "SELECT date_part('year', INTERVAL '14' MONTH)",
        "SELECT date_part('year', INTERVAL '5' DAY)",
        "SELECT date_part('year', INTERVAL '2' YEAR)",
    ] {
        let err = run_err(&ny_ansi, refused_sql).await;
        assert!(err.contains("MonthDayNano"), "{refused_sql}: {err}");
    }
    assert!(
        run_err(&ny_ansi, "SELECT date_part('epoch', INTERVAL '5' DAY)")
            .await
            .contains("INVALID_EXTRACT_FIELD")
    );
    assert!(
        run_err(&ny_ansi, "SELECT extract(EPOCH FROM INTERVAL '5' DAY)")
            .await
            .contains("INVALID_EXTRACT_FIELD")
    );

    let facade_udf = crate::datetime::spark_date_part::facade_date_part_udf();
    let df = ny_ansi
        .sql("SELECT INTERVAL '2' YEAR AS iy, INTERVAL '3' HOUR AS ihr")
        .await
        .expect("plan intervals");
    let facade_batches = df
        .select(vec![
            facade_udf
                .call(vec![
                    datafusion::prelude::lit("y"),
                    datafusion::prelude::col("iy"),
                ])
                .alias("y"),
            facade_udf
                .call(vec![
                    datafusion::prelude::lit("hrs"),
                    datafusion::prelude::col("ihr"),
                ])
                .alias("h"),
        ])
        .expect("select facade")
        .collect()
        .await
        .expect("collect facade");
    assert_eq!(
        facade_batches[0]
            .column(0)
            .as_primitive::<Int32Type>()
            .value(0),
        2
    );
    assert_eq!(
        facade_batches[0]
            .column(1)
            .as_primitive::<Int32Type>()
            .value(0),
        3
    );

    let iv_schema = Arc::new(Schema::new(vec![
        Field::new(
            "ym",
            DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
            true,
        ),
        Field::new(
            "dt",
            DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
            true,
        ),
    ]));
    let iv_batch = RecordBatch::try_new(
        iv_schema,
        vec![
            Arc::new(IntervalYearMonthArray::from(vec![Some(26), None])),
            Arc::new(IntervalDayTimeArray::from(vec![
                Some(IntervalDayTime::new(5, 3 * 3_600_000)),
                None,
            ])),
        ],
    )
    .expect("interval batch");
    ny_ansi
        .register_batch("t_iv", iv_batch)
        .expect("register intervals");
    assert_eq!(
        run_int32_col(&ny_ansi, "SELECT date_part('year', ym) FROM t_iv").await,
        vec![Some(2), None]
    );
    assert_eq!(
        run_int32_col(&ny_ansi, "SELECT date_part('month', ym) FROM t_iv").await,
        vec![Some(2), None]
    );
    assert_eq!(
        run_int32_col(&ny_ansi, "SELECT date_part('day', dt) FROM t_iv").await,
        vec![Some(5), None]
    );
    assert_eq!(
        run_int32_col(&ny_ansi, "SELECT date_part('hour', dt) FROM t_iv").await,
        vec![Some(3), None]
    );
    assert!(
        run_err(&ny_ansi, "SELECT date_part('day', ym) FROM t_iv")
            .await
            .contains("INVALID_EXTRACT_FIELD")
    );
    assert!(
        run_err(&ny_ansi, "SELECT date_part('year', dt) FROM t_iv")
            .await
            .contains("INVALID_EXTRACT_FIELD")
    );
}

#[tokio::test]
async fn a1_string_inputs_follow_spark_semantics() {
    let ny_ansi = spark_context(NEW_YORK, true);
    let ny_off = spark_context(NEW_YORK, false);
    let kolkata = spark_context(KOLKATA, true);

    assert_eq!(
        run_int32_row(
            &ny_ansi,
            "SELECT date_part('year', '2024-01-01'), date_part('hour', '2024-01-01'), \
             date_part('hour', '2024-01-01 02:30:45'), date_part('hour', '2024-01-01T02:30:45Z'), \
             date_part('day', '2024-01-01T02:30:45Z'), date_part('year', '2024-01-01T02:30:45Z'), \
             date_part('hour', '2024-01-01 02:30:45+05:30'), \
             date_part('hour', '2024-03-10 02:30:00'), \
             date_part('year', CAST(NULL AS STRING))"
        )
        .await,
        vec![
            Some(2024),
            Some(0),
            Some(2),
            Some(21),
            Some(1),
            Some(2024),
            Some(16),
            Some(3),
            None,
        ]
    );
    assert_eq!(
        run_int32_row(
            &kolkata,
            "SELECT date_part('hour', '2024-01-01T02:30:45Z'), \
             date_part('minute', '2024-01-01T02:30:45Z'), \
             date_part('hour', '2024-01-01 02:30:45+05:30')"
        )
        .await,
        vec![Some(8), Some(0), Some(2)]
    );

    let s_schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let s_batch = RecordBatch::try_new(
        s_schema,
        vec![Arc::new(StringArray::from(vec![
            "2024-01-01T02:30:45Z",
            "not-a-date",
        ]))],
    )
    .expect("string batch");
    ny_off
        .register_batch("t_str", s_batch.clone())
        .expect("register str off");
    ny_ansi
        .register_batch("t_str", s_batch)
        .expect("register str on");
    assert_eq!(
        run_int32_col(&ny_off, "SELECT date_part('hour', s) FROM t_str").await,
        vec![Some(21), None]
    );
    assert_eq!(
        run_int32_col(&ny_off, "SELECT date_part('day', s) FROM t_str").await,
        vec![Some(1), None]
    );
    assert!(
        run_err(&ny_ansi, "SELECT date_part('hour', s) FROM t_str")
            .await
            .contains("CAST_INVALID_INPUT")
    );
    assert!(
        run_err(&ny_ansi, "SELECT date_part('year', s) FROM t_str")
            .await
            .contains("CAST_INVALID_INPUT")
    );
}

#[tokio::test]
async fn a2_non_spark_fields_refuse_loudly_across_sources_and_empty_tables() {
    let ny = spark_context(NEW_YORK, true);
    register_timestamps(&ny, "t_ts", &[Some("2024-01-01T02:00:00Z")], Some("UTC"));
    register_timestamps(&ny, "t_empty", &[], Some("UTC"));

    let rejected = [
        "year_iso",
        "isoyear",
        "quarters",
        "isoweek",
        "isodow",
        "dayofyear",
        "millisecond",
        "milliseconds",
        "ms",
        "microsecond",
        "microseconds",
        "us",
        "nanosecond",
        "nanoseconds",
        "ns",
        "epoch",
        "century",
        "decade",
        "millennium",
        " year ",
        "'year'",
        "\"year\"",
        "",
        "bogus",
    ];
    for field in rejected {
        let escaped = field.replace('\'', "''");
        for sql in [
            format!("SELECT date_part('{escaped}', ts) FROM t_ts"),
            format!("SELECT date_part('{escaped}', ts) FROM t_empty"),
            format!("SELECT date_part('{escaped}', NULL)"),
            format!("SELECT date_part('{escaped}', DATE '2024-01-01')"),
            format!("SELECT date_part('{escaped}', '2024-01-01 02:00:00')"),
        ] {
            let err = run_err(&ny, &sql).await;
            assert!(err.contains("INVALID_EXTRACT_FIELD"), "{sql}: {err}");
        }
    }
    for sql in [
        "SELECT extract(ISOYEAR FROM ts) FROM t_ts",
        "SELECT extract(ISODOW FROM ts) FROM t_ts",
        "SELECT extract(EPOCH FROM ts) FROM t_ts",
        "SELECT extract(MILLISECOND FROM NULL)",
        "SELECT extract(MICROSECOND FROM NULL)",
    ] {
        let err = run_err(&ny, sql).await;
        assert!(err.contains("INVALID_EXTRACT_FIELD"), "{sql}: {err}");
    }
}

#[tokio::test]
async fn a3_non_foldable_field_refuses_before_optimizer_while_foldable_exprs_answer() {
    let ny = spark_context(NEW_YORK, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["hour"])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![utc_micros("2024-01-01T02:00:00Z")])
                    .with_timezone("UTC"),
            ),
        ],
    )
    .expect("fold batch");
    ny.register_batch("t_fold", batch).expect("register fold");

    for sql in [
        "SELECT date_part(f, ts) FROM (SELECT 'hour' AS f, ts FROM t_fold) s",
        "SELECT date_part(f, ts) FROM t_fold",
        "SELECT date_part(concat(f, ''), ts) FROM t_fold",
        "SELECT date_part(CASE WHEN f = 'hour' THEN 'hour' ELSE 'hour' END, ts) FROM t_fold",
        "SELECT date_part(CASE WHEN rand() >= 0 THEN 'hour' ELSE 'day' END, ts) FROM t_fold",
    ] {
        let err = run_err(&ny, sql).await;
        assert!(err.contains("NON_FOLDABLE_ARGUMENT"), "{sql}: {err}");
    }

    assert_eq!(
        run_int32_row(
            &ny,
            "SELECT date_part(lower('HOUR'), ts), date_part(concat('ho', 'ur'), ts), \
             date_part(CASE WHEN 1 = 1 THEN 'hour' ELSE 'day' END, ts) FROM t_fold"
        )
        .await,
        vec![Some(21), Some(21), Some(21)]
    );
    let err = run_err(&ny, "SELECT date_part(lower('EPOCH'), ts) FROM t_fold").await;
    assert!(err.contains("INVALID_EXTRACT_FIELD"), "lower epoch: {err}");
}
