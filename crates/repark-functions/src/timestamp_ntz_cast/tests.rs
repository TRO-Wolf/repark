use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::TimestampMicrosecondType;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::*;

fn array_ticks(array: &ArrayRef) -> Vec<Option<i64>> {
    let micros = array.as_primitive::<TimestampMicrosecondType>();
    (0..micros.len())
        .map(|row| micros.is_valid(row).then(|| micros.value(row)))
        .collect()
}

fn ctx_at(zone: &str) -> SessionContext {
    let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
    let ctx = SessionContext::new_with_config(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

#[test]
fn the_parse_table_answers_the_measured_literal_walls() {
    assert_eq!(
        parse_timestamp_ntz_wall("2024-01-01 12:34:56.123456"),
        Some(1_704_112_496_123_456)
    );
    assert_eq!(
        parse_timestamp_ntz_wall("2024-01-01 12:00:00+05:00"),
        Some(1_704_110_400_000_000)
    );
    assert_eq!(
        parse_timestamp_ntz_wall("2024-01-01"),
        Some(1_704_067_200_000_000)
    );
    assert_eq!(
        parse_timestamp_ntz_wall("2024-01-01 12:34:56.1234567"),
        Some(1_704_112_496_123_456)
    );
    assert_eq!(
        parse_timestamp_ntz_wall("2024-01-01T12:00:00"),
        Some(1_704_110_400_000_000)
    );
    assert_eq!(parse_timestamp_ntz_wall("x"), None);
    assert_eq!(parse_timestamp_ntz_wall(""), None);
    assert_eq!(parse_timestamp_ntz_wall("2024-13-01"), None);
}

#[tokio::test]
async fn the_cast_answers_strings_dates_and_nulls() {
    let ctx = ctx_at("UTC");
    let batches = ctx
        .sql(
            "SELECT __repark_cast_timestamp_ntz__('2024-01-01 12:00:00+05:00') AS a, \
             __repark_cast_timestamp_ntz__(DATE '2024-01-02') AS b, \
             __repark_cast_timestamp_ntz__(NULL) AS c",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ntz_timestamp_type()
    );
    assert_eq!(
        array_ticks(batches[0].column(0)),
        vec![Some(1_704_110_400_000_000)]
    );
    assert_eq!(
        array_ticks(batches[0].column(1)),
        vec![Some(1_704_153_600_000_000)]
    );
    assert!(batches[0].column(2).is_null(0));
}

#[tokio::test]
async fn an_instant_cast_is_its_session_zone_wall() {
    for (zone, expected) in [
        ("UTC", 1_704_110_400_000_000),
        ("America/New_York", 1_704_092_400_000_000),
    ] {
        let ctx = ctx_at(zone);
        let batches = ctx
            .sql("SELECT __repark_cast_timestamp_ntz__(TIMESTAMP '2024-01-01 12:00:00Z') AS v")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        assert_eq!(
            array_ticks(batches[0].column(0)),
            vec![Some(expected)],
            "{zone}"
        );
    }
}

#[tokio::test]
async fn malformed_strings_raise_under_ansi_and_null_under_try_cast() {
    let ctx = ctx_at("UTC");
    let failure = ctx
        .sql("SELECT __repark_cast_timestamp_ntz__('x') AS v")
        .await
        .expect("plan")
        .collect()
        .await
        .expect_err("ansi garbage raises");
    let text = failure.to_string();
    assert!(text.contains("[CAST_INVALID_INPUT]"), "{text}");
    assert!(text.contains("\"TIMESTAMP_NTZ\""), "{text}");
    let batches = ctx
        .sql("SELECT __repark_try_cast_timestamp_ntz__('x') AS v")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    assert!(batches[0].column(0).is_null(0));
}

#[tokio::test]
async fn numeric_sources_refuse_with_sparks_class_and_names() {
    let ctx = ctx_at("UTC");
    let failure = ctx
        .sql("SELECT __repark_cast_timestamp_ntz__(1) AS v")
        .await
        .expect_err("int source refuses")
        .to_string();
    assert!(
        failure.contains(
            "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \
             \"CAST(1 AS TIMESTAMP_NTZ)\" due to data type mismatch: cannot cast \"INT\" to \
             \"TIMESTAMP_NTZ\". SQLSTATE: 42K09"
        ),
        "{failure}"
    );
    let tried = ctx
        .sql("SELECT __repark_try_cast_timestamp_ntz__(1) AS v")
        .await
        .expect_err("try_cast int source refuses")
        .to_string();
    assert!(tried.contains("TRY_CAST(1 AS TIMESTAMP_NTZ)"), "{tried}");
}

#[tokio::test]
async fn the_literal_is_a_naive_wall_named_like_spark() {
    let ctx = ctx_at("UTC");
    let frame = ctx
        .sql("SELECT __repark_timestamp_ntz__(1704110400000000) AS v")
        .await
        .expect("plan");
    assert_eq!(
        frame.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        array_ticks(batches[0].column(0)),
        vec![Some(1_704_110_400_000_000)]
    );
    let named = ctx
        .sql("SELECT __repark_timestamp_ntz__(1704110400000000)")
        .await
        .expect("plan");
    assert_eq!(
        named.schema().field(0).name(),
        "TIMESTAMP_NTZ '2024-01-01 12:00:00'"
    );
}

#[test]
fn retargeting_an_already_wrapped_store_expr_keeps_a_single_wrap() {
    use datafusion::common::Column;
    let bare = Expr::Column(Column::from_name("c"));
    let schema = DFSchema::empty();
    let once = retarget_top(bare.clone(), &schema, true);
    assert!(is_ntz_cast_call(&once), "{once:?}");
    let twice = retarget_top(once.clone(), &schema, true);
    assert_eq!(twice, once);
    assert_eq!(retarget_top(bare, &schema, false).to_string(), "c");
}
