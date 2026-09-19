use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::ansi::with_spark_ansi_config;
use crate::session_time_zone::with_session_time_zone;

fn context(zone: &str, ansi: bool) -> SessionContext {
    let config = with_spark_ansi_config(with_session_time_zone(SessionConfig::new(), zone), ansi);
    let ctx = SessionContext::new_with_config(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<RecordBatch> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches.into_iter().next().expect("one batch"))
}

fn ltz_micros(batch: &RecordBatch) -> Vec<Option<i64>> {
    let column = batch.column(0);
    assert_eq!(
        column.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
    );
    let micros = column.as_primitive::<TimestampMicrosecondType>();
    (0..micros.len())
        .map(|row| (!micros.is_null(row)).then(|| micros.value(row)))
        .collect()
}

const COLUMN_SOURCE: &str = "(VALUES ('2020'), ('2999-01-01'), ('2020-06-01 10:00:00 UTC'), \
                             ('garbage'), (NULL)) AS t(s)";

#[tokio::test]
async fn literal_cast_folds_spark_grammar_to_microsecond_utc() {
    let ctx = context("America/New_York", true);
    let batch = run(&ctx, "SELECT CAST('2999-01-01' AS TIMESTAMP)")
        .await
        .expect("literal casts");
    assert_eq!(ltz_micros(&batch), vec![Some(32_472_162_000_000_000)]);
}

#[tokio::test]
async fn column_cast_runs_the_kernel_with_ansi_off() {
    let ctx = context("America/New_York", false);
    let batch = run(
        &ctx,
        &format!("SELECT CAST(s AS TIMESTAMP) FROM {COLUMN_SOURCE}"),
    )
    .await
    .expect("column casts");
    assert_eq!(
        ltz_micros(&batch),
        vec![
            Some(1_577_854_800_000_000),
            Some(32_472_162_000_000_000),
            Some(1_591_005_600_000_000),
            None,
            None,
        ]
    );
}

#[tokio::test]
async fn column_cast_raises_cast_invalid_input_with_ansi_on() {
    let ctx = context("UTC", true);
    let error = run(
        &ctx,
        &format!("SELECT CAST(s AS TIMESTAMP) FROM {COLUMN_SOURCE}"),
    )
    .await
    .expect_err("ANSI refuses the malformed row");
    assert!(
        error
            .to_string()
            .contains("[CAST_INVALID_INPUT] The value 'garbage' of the type \"STRING\""),
        "{error}"
    );
}

#[tokio::test]
async fn literal_cast_of_a_malformed_string_raises_with_ansi_on() {
    let ctx = context("UTC", true);
    let error = run(&ctx, "SELECT CAST('2020-06-01T00:00Z' AS TIMESTAMP)")
        .await
        .expect_err("ANSI refuses a zone after minutes");
    assert!(
        error.to_string().contains("The value '2020-06-01T00:00Z'"),
        "{error}"
    );
}

#[tokio::test]
async fn try_cast_answers_null_for_malformed_rows_with_ansi_on() {
    let ctx = context("UTC", true);
    let batch = run(
        &ctx,
        &format!("SELECT TRY_CAST(s AS TIMESTAMP) FROM {COLUMN_SOURCE}"),
    )
    .await
    .expect("try_cast never raises on a value");
    assert_eq!(
        ltz_micros(&batch),
        vec![
            Some(1_577_836_800_000_000),
            Some(32_472_144_000_000_000),
            Some(1_591_005_600_000_000),
            None,
            None,
        ]
    );
    let literal = run(&ctx, "SELECT TRY_CAST('2020-6-1' AS TIMESTAMP)")
        .await
        .expect("try_cast literal folds");
    assert_eq!(ltz_micros(&literal), vec![Some(1_590_969_600_000_000)]);
}

#[tokio::test]
async fn one_argument_to_timestamp_and_try_to_timestamp_follow_the_cast() {
    let ctx = context("UTC", true);
    let batch = run(&ctx, "SELECT to_timestamp('2020-06-01 1:2:3')")
        .await
        .expect("to_timestamp parses");
    assert_eq!(ltz_micros(&batch), vec![Some(1_590_973_323_000_000)]);
    let tolerant = run(
        &ctx,
        &format!("SELECT try_to_timestamp(s) FROM {COLUMN_SOURCE}"),
    )
    .await
    .expect("try_to_timestamp never raises on a value");
    assert_eq!(ltz_micros(&tolerant)[3], None);
    assert_eq!(ltz_micros(&tolerant)[0], Some(1_577_836_800_000_000));
}

#[tokio::test]
async fn dictionary_encoded_string_columns_run_the_kernel() {
    use arrow::array::DictionaryArray;
    use arrow::datatypes::{Field, Int32Type, Schema};
    use datafusion::datasource::MemTable;

    let values: DictionaryArray<Int32Type> = vec![
        Some("2020"),
        Some("2020-06-01 10:00:00"),
        Some("2999-01-01"),
        Some("garbage"),
        None,
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).expect("batch");
    let expected = vec![
        Some(1_577_854_800_000_000),
        Some(1_591_020_000_000_000),
        Some(32_472_162_000_000_000),
        None,
        None,
    ];
    for statement in [
        "SELECT CAST(s AS TIMESTAMP) FROM d",
        "SELECT TRY_CAST(s AS TIMESTAMP) FROM d",
        "SELECT try_to_timestamp(s) FROM d",
    ] {
        let ctx = context("America/New_York", false);
        ctx.register_table(
            "d",
            Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]]).expect("mem"),
            ),
        )
        .expect("register");
        let answer = run(&ctx, statement).await.expect(statement);
        assert_eq!(ltz_micros(&answer), expected, "{statement}");
    }
}
