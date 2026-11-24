use super::*;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::TimestampMicrosecondType;
use datafusion::prelude::SessionContext;

fn ctx() -> SessionContext {
    let ctx = crate::spark_type_planner::planned_context(datafusion::prelude::SessionConfig::new());
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

#[tokio::test]
async fn now_and_current_timestamp_are_microsecond_utc_non_null() {
    let ctx = ctx();
    for sql in ["SELECT now() AS ts", "SELECT current_timestamp() AS ts"] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        let schema = batches[0].schema();
        let field = schema.field(0);
        assert_eq!(field.data_type(), &ltz_timestamp_type(), "{sql}: type");
        assert!(!field.is_nullable(), "{sql}: Spark marks now() not-null");
    }
}

#[tokio::test]
async fn to_timestamp_of_a_zone_suffixed_string_is_microsecond_utc() {
    let ctx = ctx();
    let sql = "SELECT to_timestamp('2024-03-10T01:30:00-05:00') AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &ltz_timestamp_type());
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, 1_710_052_200_000_000);
}

#[tokio::test]
async fn zoneless_to_timestamp_under_utc_session_keeps_wall_as_utc() {
    let ctx = ctx();
    let sql = "SELECT to_timestamp('2024-06-15 12:00:00') AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ltz_timestamp_type()
    );
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, 1_718_452_800_000_000);
}

fn ctx_at(zone: &str) -> SessionContext {
    use datafusion::prelude::SessionConfig;
    let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
    let ctx = crate::spark_type_planner::planned_context(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

#[tokio::test]
async fn zoneless_inputs_localize_in_the_session_zone() {
    let ctx = ctx_at("America/New_York");
    let expected = 1_718_467_200_000_000_i64;
    for sql in [
        "SELECT to_timestamp('2024-06-15 12:00:00') AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
    ] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ltz_timestamp_type(),
            "{sql}"
        );
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ticks, expected, "{sql}");
    }
}

#[tokio::test]
async fn zone_suffixed_to_timestamp_is_not_localized_again() {
    let ctx = ctx_at("America/New_York");
    let sql = "SELECT to_timestamp('2024-06-15T12:00:00Z') AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, 1_718_452_800_000_000);
}

#[tokio::test]
async fn integer_cast_to_timestamp_is_microsecond_utc_seconds() {
    let ctx = ctx();
    let sql = "SELECT CAST(CAST(-1800 AS BIGINT) AS TIMESTAMP) AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &ltz_timestamp_type());
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, -1_800_000_000);
}

fn ctx_ntz_at(zone: &str) -> SessionContext {
    use datafusion::prelude::SessionConfig;
    let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
    let config = crate::timestamp_type::with_spark_timestamp_type(
        config,
        crate::timestamp_type::SparkTimestampType::Ntz,
    );
    let ctx = crate::spark_type_planner::planned_context(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

#[tokio::test]
async fn ntz_opt_in_bare_timestamp_is_naive_microseconds() {
    let ctx = ctx_ntz_at("America/New_York");
    let expected = 1_718_452_800_000_000_i64;
    for sql in [
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
    ] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ntz_timestamp_type(),
            "{sql}"
        );
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ticks, expected, "{sql}");
    }
}

#[tokio::test]
async fn ntz_opt_in_does_not_retarget_to_timestamp_or_now() {
    let ctx = ctx_ntz_at("UTC");
    let batches = ctx
        .sql("SELECT to_timestamp('2024-06-15 12:00:00') AS ts")
        .await
        .expect("to_timestamp")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ltz_timestamp_type(),
        "to_timestamp stays LTZ; the knob is the SQL type name TIMESTAMP"
    );
    let batches = ctx
        .sql("SELECT current_timestamp() AS ts")
        .await
        .expect("current_timestamp")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ltz_timestamp_type(),
        "current_timestamp is an instant, not the SQL type name"
    );
}

#[tokio::test]
async fn ntz_opt_in_integer_cast_is_naive_microseconds_seconds() {
    let ctx = ctx_ntz_at("UTC");
    let sql = "SELECT CAST(CAST(-1800 AS BIGINT) AS TIMESTAMP) AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ntz_timestamp_type()
    );
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, -1_800_000_000);
}

#[tokio::test]
async fn deferred_session_zone_skips_zoneless_fold_and_keeps_zoned_fold() {
    let config = crate::session_time_zone::with_deferred_session_time_zone(
        datafusion::prelude::SessionConfig::new(),
    );
    let ctx = crate::spark_type_planner::planned_context(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    for (sql, expected_fn) in [
        (
            "SELECT TIMESTAMP '2024-01-01 02:00:00' AS ts",
            "to_timestamp",
        ),
        (
            "SELECT CAST('2024-01-01 02:00:00' AS TIMESTAMP) AS ts",
            "to_timestamp",
        ),
        (
            "SELECT TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP) AS ts",
            "try_to_timestamp",
        ),
    ] {
        let unoptimized = ctx.sql(sql).await.expect(sql).into_unoptimized_plan();
        let plan = crate::analyze_eagerly(&ctx.state(), unoptimized).expect(sql);
        let display = format!("{plan}");
        assert!(display.contains(expected_fn), "{sql}: {display}");
        assert!(
            !display.contains("TimestampMicrosecond("),
            "{sql}: {display}"
        );
    }
    for sql in [
        "SELECT TIMESTAMP '2024-01-01 02:00:00Z' AS ts",
        "SELECT CAST('2024-01-01 02:00:00+00:00' AS TIMESTAMP) AS ts",
        "SELECT TRY_CAST('2024-01-01 02:00:00Z' AS TIMESTAMP) AS ts",
    ] {
        let unoptimized = ctx.sql(sql).await.expect(sql).into_unoptimized_plan();
        let plan = crate::analyze_eagerly(&ctx.state(), unoptimized).expect(sql);
        let display = format!("{plan}");
        assert!(
            display.contains("TimestampMicrosecond(1704074400000000"),
            "{sql}: {display}"
        );
        assert!(!display.contains("to_timestamp"), "{sql}: {display}");
    }
}

#[tokio::test]
async fn deferred_timestamp_type_leaves_bare_casts_and_folds_zoned_ltz_marker() {
    let config = crate::session_time_zone::with_deferred_session_time_zone(
        datafusion::prelude::SessionConfig::new(),
    );
    let config = crate::timestamp_type::with_deferred_spark_timestamp_type(config);
    let ctx = crate::spark_type_planner::planned_context(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    for sql in [
        "SELECT TIMESTAMP '2024-01-01 02:00:00' AS ts",
        "SELECT CAST('2024-01-01 02:00:00' AS TIMESTAMP) AS ts",
        "SELECT TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP) AS ts",
        "SELECT CAST('2024-01-01 02:00:00Z' AS TIMESTAMP) AS ts",
    ] {
        let unoptimized = ctx.sql(sql).await.expect(sql).into_unoptimized_plan();
        let plan = crate::analyze_eagerly(&ctx.state(), unoptimized).expect(sql);
        let display = format!("{plan}");
        assert!(!display.contains("to_timestamp"), "{sql}: {display}");
        assert!(
            !display.contains("TimestampMicrosecond("),
            "{sql}: {display}"
        );
    }
    let zoned_sql = "SELECT __repark_cast_timestamp_ltz__('2024-01-01 02:00:00Z') AS ts";
    let unoptimized = ctx
        .sql(zoned_sql)
        .await
        .expect(zoned_sql)
        .into_unoptimized_plan();
    let plan = crate::analyze_eagerly(&ctx.state(), unoptimized).expect(zoned_sql);
    let display = format!("{plan}");
    assert!(
        display.contains("TimestampMicrosecond(1704074400000000"),
        "{display}"
    );
    assert!(!display.contains(TIMESTAMP_LTZ_CAST_NAME), "{display}");
}

#[tokio::test]
async fn explicit_ltz_cast_marker_stays_ltz_under_ntz_timestamp_type() {
    let ctx = ctx_ntz_at("America/New_York");
    for sql in [
        "SELECT __repark_cast_timestamp_ltz__('2024-01-01 02:00:00') AS ts",
        "SELECT __repark_cast_timestamp_ltz__(TIMESTAMP '2024-01-01 02:00:00') AS ts",
        "SELECT __repark_try_cast_timestamp_ltz__('2024-01-01 02:00:00') AS ts",
    ] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ltz_timestamp_type(),
            "{sql}"
        );
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ticks, 1_704_092_400_000_000, "{sql}");
    }
    let bad = ctx
        .sql("SELECT __repark_try_cast_timestamp_ltz__('bad') AS ts")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    assert_eq!(bad[0].schema().field(0).data_type(), &ltz_timestamp_type());
    assert!(bad[0].column(0).is_null(0));
}

#[tokio::test]
async fn ntz_opt_in_try_cast_to_timestamp_answers_ntz() {
    let ctx = ctx_ntz_at("America/New_York");
    let ok = ctx
        .sql("SELECT TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP) AS ts")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    assert_eq!(ok[0].schema().field(0).data_type(), &ntz_timestamp_type());
    assert_eq!(
        ok[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_704_074_400_000_000
    );
    let bad = ctx
        .sql("SELECT TRY_CAST('bad' AS TIMESTAMP) AS ts")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    assert_eq!(bad[0].schema().field(0).data_type(), &ntz_timestamp_type());
    assert!(bad[0].column(0).is_null(0));
}

#[tokio::test]
async fn ntz_opt_in_cast_of_ltz_instant_converts_to_session_wall() {
    let ctx = ctx_ntz_at("America/New_York");
    let batches = ctx
        .sql(
            "SELECT CAST(__repark_cast_timestamp_ltz__('2024-01-01 02:00:00Z') AS TIMESTAMP) AS ts",
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
        batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_704_056_400_000_000
    );
}

#[tokio::test]
async fn ntz_opt_in_union_rebuilds_stale_schema_to_ntz() {
    let ctx = ctx_ntz_at("America/New_York");
    let sql = "SELECT TIMESTAMP '2024-01-01 02:00:00' AS ts \
               UNION ALL SELECT __repark_timestamp_ntz__(1704074400000000) AS ts";
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ntz_timestamp_type()
    );
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch.column(0).as_primitive::<TimestampMicrosecondType>();
            (0..col.len()).map(|row| col.value(row)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values, vec![1_704_074_400_000_000, 1_704_074_400_000_000]);
}
