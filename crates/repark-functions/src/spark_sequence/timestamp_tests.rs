use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, Int64Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

const FIRST: i64 = 1_767_323_045_123_456;
const SECOND: i64 = 1_000_000;
const DAY: i64 = 86_400 * SECOND;

fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    let micros = vec![FIRST, -1];
    let nanos = vec![FIRST * 1_000, -1_000];
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2]))),
        (
            "ntz",
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
        ),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC")),
        ),
        (
            "ns",
            Arc::new(TimestampNanosecondArray::from(nanos.clone())),
        ),
        (
            "tzns",
            Arc::new(TimestampNanosecondArray::from(nanos).with_timezone("UTC")),
        ),
        (
            "fine",
            Arc::new(TimestampNanosecondArray::from(vec![
                FIRST * 1_000 + 789,
                -1,
            ])),
        ),
        (
            "ms",
            Arc::new(TimestampMillisecondArray::from(vec![FIRST / 1_000, -1])),
        ),
        (
            "s",
            Arc::new(TimestampSecondArray::from(vec![FIRST / SECOND, -1])),
        ),
    ];
    let batch = RecordBatch::try_from_iter(columns).expect("source batch");
    ctx.register_batch("t", batch).expect("register source");
    ctx
}

async fn lists(ctx: &SessionContext, expression: &str) -> (DataType, Vec<Option<Vec<i64>>>) {
    let sql = format!("SELECT {expression} AS v FROM t ORDER BY id");
    let frame = ctx
        .sql(&sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"));
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
    let batch = concat_batches(&schema, &batches).expect("one batch");
    let column = batch.column(0).as_list::<i32>();
    let DataType::List(element) = batch.column(0).data_type() else {
        panic!("{sql}: not a list");
    };
    let mut rows = Vec::new();
    for row in 0..column.len() {
        if column.is_null(row) {
            rows.push(None);
            continue;
        }
        let ticks = cast(&column.value(row), &DataType::Int64).expect("ticks");
        let ticks = ticks
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 ticks");
        rows.push(Some(ticks.values().to_vec()));
    }
    (element.data_type().clone(), rows)
}

async fn refusal(ctx: &SessionContext, expression: &str) -> String {
    let sql = format!("SELECT {expression} AS v FROM t ORDER BY id");
    match ctx.sql(&sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("sequence should refuse")
            .to_string(),
    }
}

fn spark_recorded() -> Vec<(&'static str, Vec<Option<Vec<i64>>>)> {
    vec![
        (
            "sequence({c}, {c})",
            vec![Some(vec![FIRST]), Some(vec![-1])],
        ),
        (
            "sequence({c}, {c} + INTERVAL 2 SECOND, INTERVAL 1 SECOND)",
            vec![
                Some(vec![FIRST, FIRST + SECOND, FIRST + 2 * SECOND]),
                Some(vec![-1, 999_999, 1_999_999]),
            ],
        ),
        (
            "sequence({c} + INTERVAL 2 SECOND, {c}, INTERVAL -1 SECOND)",
            vec![
                Some(vec![FIRST + 2 * SECOND, FIRST + SECOND, FIRST]),
                Some(vec![1_999_999, 999_999, -1]),
            ],
        ),
        (
            "sequence({c}, {c} + INTERVAL 2 DAY, INTERVAL 1 DAY)",
            vec![
                Some(vec![FIRST, FIRST + DAY, FIRST + 2 * DAY]),
                Some(vec![-1, 86_399_999_999, 172_799_999_999]),
            ],
        ),
        (
            "sequence({c}, {c} + INTERVAL 2 MONTH, INTERVAL 1 MONTH)",
            vec![
                Some(vec![FIRST, 1_770_001_445_123_456, 1_772_420_645_123_456]),
                Some(vec![-1, 2_678_399_999_999, 5_097_599_999_999]),
            ],
        ),
    ]
}

fn scaled(rows: &[Option<Vec<i64>>], factor: i64) -> Vec<Option<Vec<i64>>> {
    rows.iter()
        .map(|row| {
            row.as_ref()
                .map(|ticks| ticks.iter().map(|tick| tick * factor).collect())
        })
        .collect()
}

#[tokio::test]
async fn microsecond_forms_keep_their_zone_and_match_spark() {
    let ctx = ctx();
    for (column, zone) in [("ntz", None), ("ts", Some("UTC".into()))] {
        for (template, recorded) in spark_recorded() {
            let expression = template.replace("{c}", column);
            let (element, rows) = lists(&ctx, &expression).await;
            assert_eq!(
                element,
                DataType::Timestamp(TimeUnit::Microsecond, zone.clone()),
                "{expression}"
            );
            assert_eq!(rows, recorded, "{expression}");
        }
    }
}

#[tokio::test]
async fn nanosecond_forms_equal_the_microsecond_twin() {
    let ctx = ctx();
    for (column, zone) in [("ns", None), ("tzns", Some("UTC".into()))] {
        for (template, recorded) in spark_recorded() {
            let expression = template.replace("{c}", column);
            let (element, rows) = lists(&ctx, &expression).await;
            assert_eq!(
                element,
                DataType::Timestamp(TimeUnit::Nanosecond, zone.clone()),
                "{expression}"
            );
            assert_eq!(rows, scaled(&recorded, 1_000), "{expression}");
        }
    }
}

#[tokio::test]
async fn nanosecond_start_keeps_its_sub_microsecond_digits() {
    let ctx = ctx();
    let fine = FIRST * 1_000 + 789;
    let second = 1_000 * SECOND;
    let (element, rows) = lists(
        &ctx,
        "sequence(fine, fine + INTERVAL 2 SECOND, INTERVAL 1 SECOND)",
    )
    .await;
    assert_eq!(element, DataType::Timestamp(TimeUnit::Nanosecond, None));
    assert_eq!(
        rows,
        vec![
            Some(vec![fine, fine + second, fine + 2 * second]),
            Some(vec![-1, second - 1, 2 * second - 1]),
        ]
    );
    let (_, rows) = lists(&ctx, "sequence(fine, fine)").await;
    assert_eq!(rows, vec![Some(vec![fine]), Some(vec![-1])]);
    let (_, rows) = lists(
        &ctx,
        "sequence(fine, arrow_cast(arrow_cast(fine, 'Int64') + 1999999999, \
         'Timestamp(Nanosecond, None)'), INTERVAL 1 SECOND)",
    )
    .await;
    assert_eq!(
        rows,
        vec![Some(vec![fine, fine + second]), Some(vec![-1, second - 1])]
    );
}

#[tokio::test]
async fn nanosecond_gap_under_a_microsecond_keeps_the_step_sign_rule() {
    let ctx = ctx();
    let fine = FIRST * 1_000 + 789;
    let above = "arrow_cast(arrow_cast(fine, 'Int64') + 100, 'Timestamp(Nanosecond, None)')";
    let below = "arrow_cast(arrow_cast(fine, 'Int64') - 100, 'Timestamp(Nanosecond, None)')";
    for expression in [
        format!("sequence(fine, {above})"),
        format!("sequence(fine, {above}, INTERVAL 1 SECOND)"),
        format!("sequence(fine, {above}, INTERVAL 1 MONTH)"),
        format!("sequence(fine, {below})"),
        format!("sequence(fine, {below}, INTERVAL -1 SECOND)"),
        format!("sequence(fine, {below}, INTERVAL -1 MONTH)"),
    ] {
        let (_, rows) = lists(&ctx, &expression).await;
        assert_eq!(rows, vec![Some(vec![fine]), Some(vec![-1])], "{expression}");
    }
    for expression in [
        format!("sequence(fine, {above}, INTERVAL -1 SECOND)"),
        format!("sequence(fine, {below}, INTERVAL 1 SECOND)"),
    ] {
        let error = refusal(&ctx, &expression).await;
        assert!(
            error.contains("requirement failed: Illegal sequence boundaries"),
            "{expression}: {error}"
        );
    }
}

#[tokio::test]
async fn mixed_units_follow_the_first_argument() {
    let ctx = ctx();
    let (element, rows) = lists(&ctx, "sequence(ns, ntz)").await;
    assert_eq!(element, DataType::Timestamp(TimeUnit::Nanosecond, None));
    assert_eq!(rows, vec![Some(vec![FIRST * 1_000]), Some(vec![-1_000])]);
    let (element, rows) = lists(&ctx, "sequence(ntz, fine)").await;
    assert_eq!(element, DataType::Timestamp(TimeUnit::Microsecond, None));
    assert_eq!(rows, vec![Some(vec![FIRST]), Some(vec![-1])]);
    let (element, rows) = lists(&ctx, "sequence(ts, tzns)").await;
    assert_eq!(
        element,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(rows, vec![Some(vec![FIRST]), Some(vec![-1])]);
}

#[tokio::test]
async fn second_and_millisecond_inputs_keep_their_unit() {
    let ctx = ctx();
    let (element, rows) = lists(
        &ctx,
        "sequence(ms, ms + INTERVAL 2 SECOND, INTERVAL 1 SECOND)",
    )
    .await;
    assert_eq!(element, DataType::Timestamp(TimeUnit::Millisecond, None));
    let first = FIRST / 1_000;
    assert_eq!(
        rows,
        vec![
            Some(vec![first, first + 1_000, first + 2_000]),
            Some(vec![-1, 999, 1_999]),
        ]
    );
    let (element, rows) = lists(&ctx, "sequence(s, s + INTERVAL 2 SECOND)").await;
    assert_eq!(element, DataType::Timestamp(TimeUnit::Second, None));
    let first = FIRST / SECOND;
    assert_eq!(
        rows,
        vec![
            Some(vec![first, first + 1, first + 2]),
            Some(vec![-1, 0, 1])
        ]
    );
    let error = refusal(
        &ctx,
        "sequence(ms, ms + INTERVAL 1 SECOND, INTERVAL '0.0005' SECOND)",
    )
    .await;
    assert!(
        error.contains("sequence timestamp step is finer than the element unit"),
        "{error}"
    );
}

#[tokio::test]
async fn scalar_nanosecond_bounds_build_a_nanosecond_list() {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    let start = "arrow_cast(1767323045123456789, 'Timestamp(Nanosecond, None)')";
    let stop = "arrow_cast(1767323047123456789, 'Timestamp(Nanosecond, None)')";
    let sql = format!("SELECT sequence({start}, {stop}, INTERVAL 1 SECOND)");
    let batches = ctx
        .sql(&sql)
        .await
        .expect("plan scalar")
        .collect()
        .await
        .expect("execute scalar");
    let column = batches[0].column(0).as_list::<i32>();
    let DataType::List(element) = batches[0].column(0).data_type() else {
        panic!("not a list");
    };
    assert_eq!(
        element.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    let ticks = cast(&column.value(0), &DataType::Int64).expect("ticks");
    let ticks = ticks
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 ticks");
    assert_eq!(
        ticks.values(),
        &[
            1_767_323_045_123_456_789,
            1_767_323_046_123_456_789,
            1_767_323_047_123_456_789
        ]
    );
}

#[tokio::test]
async fn null_bound_answers_a_null_list_in_the_input_unit() {
    let ctx = ctx();
    let (element, rows) = lists(
        &ctx,
        "sequence(ns, CASE WHEN id = 1 THEN ns END, INTERVAL 1 SECOND)",
    )
    .await;
    assert_eq!(element, DataType::Timestamp(TimeUnit::Nanosecond, None));
    assert_eq!(rows, vec![Some(vec![FIRST * 1_000]), None]);
}
