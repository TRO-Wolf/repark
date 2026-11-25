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
    use datafusion::execution::SessionStateBuilder;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::SessionConfig;

    let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
    let config = crate::timestamp_type::with_spark_timestamp_type(
        config,
        crate::timestamp_type::SparkTimestampType::Ntz,
    );
    let mut rules = insert_ntz_timestamp_cast_before_coercion(Analyzer::new().rules).unwrap();
    rules.extend(crate::analyzer_rules());
    let state = SessionStateBuilder::new()
        .with_type_planner(crate::spark_type_planner())
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    crate::register_all(&ctx);
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

#[tokio::test]
async fn ntz_opt_in_dml_into_ltz_table_preserves_ltz_store_cast() {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;

    let ctx = ctx_ntz_at("America/New_York");
    let target_schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        ltz_timestamp_type(),
        true,
    )]));
    let table = Arc::new(MemTable::try_new(target_schema, vec![vec![]]).expect("mem table"));
    ctx.register_table("ltz_target", table).expect("register");

    let sql = "INSERT INTO ltz_target SELECT TIMESTAMP '2024-01-01 02:00:00' AS v";
    let logical = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .expect("logical plan");
    let analyzed = ctx
        .state()
        .analyzer()
        .execute_and_check(logical, ctx.state().config_options(), |_, _| {})
        .expect("analyzed");
    let LogicalPlan::Dml(dml) = analyzed else {
        panic!("expected Dml plan, got {analyzed:?}");
    };
    assert_eq!(
        dml.input.schema().field(0).data_type(),
        &ltz_timestamp_type()
    );

    ctx.sql(sql)
        .await
        .expect("insert")
        .collect()
        .await
        .expect("run");
    let rows = ctx
        .sql("SELECT v FROM ltz_target")
        .await
        .expect("select")
        .collect()
        .await
        .expect("collect");
    assert_eq!(rows[0].schema().field(0).data_type(), &ltz_timestamp_type());
    assert_eq!(
        rows[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_704_092_400_000_000
    );
}

#[test]
fn string_carries_timezone_recognizes_spark_zone_spellings() {
    for zoned in [
        "2024-01-01 02:00:00Z",
        "2024-01-01 02:00:00+05:30",
        "2024-01-01 02:00:00+05:30:00",
        "2024-01-01 02:00:00+053000",
        "2024-01-01 02:00:00-05",
        "2024-01-01 02:00:00 America/New_York",
        "2024-01-01 02:00:00 UTC",
        "2024-01-01 02:00:00 GMT+02:00",
        "2024-01-01 02:00:00 PST",
        "2024-01-01T02:00:00[UTC]",
    ] {
        assert!(string_carries_timezone(zoned), "expected zoned: {zoned}");
    }
    for zoneless in [
        "2024-01-01 02:00:00",
        "2024-01-01",
        "2024-01-01 02:00:00 BOGUS",
        "",
    ] {
        assert!(
            !string_carries_timezone(zoneless),
            "expected zoneless: {zoneless}"
        );
    }
}

#[tokio::test]
async fn ntz_opt_in_mixed_ntz_ltz_comparison_and_case_localize_in_session_zone() {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    let ctx = ctx_ntz_at("America/New_York");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("ts", ntz_timestamp_type(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_704_074_400_000_000,
                1_704_092_400_000_000,
            ])),
        ],
    )
    .unwrap();
    ctx.register_batch("t_ntz", batch).unwrap();

    let rows = ctx
        .sql(
            "SELECT id FROM t_ntz \
             WHERE ts = __repark_cast_timestamp_ltz__('2024-01-01 02:00:00')",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let ids: Vec<i32> = rows
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![1]);

    let merge_case = ctx
        .sql(
            "SELECT CASE WHEN id = 1 THEN \
             arrow_cast((CAST((ts) AS TIMESTAMP)), 'Timestamp(µs, \"UTC\")') \
             ELSE __repark_cast_timestamp_ltz__('2020-01-01 00:00:00') END AS v \
             FROM t_ntz WHERE id = 1",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        merge_case[0].schema().field(0).data_type(),
        &ltz_timestamp_type()
    );
    assert_eq!(
        merge_case[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_704_092_400_000_000
    );
}

#[tokio::test]
async fn ntz_opt_in_string_casts_preserve_wall_across_gap_offset_and_raise_ansi() {
    let ctx = ctx_ntz_at("America/New_York");
    for (sql, expected) in [
        (
            "SELECT CAST('2024-03-10 02:30:00.654321' AS TIMESTAMP) AS v",
            1_710_037_800_654_321_i64,
        ),
        (
            "SELECT TRY_CAST('2024-03-10 02:30:00.654321' AS TIMESTAMP) AS v",
            1_710_037_800_654_321_i64,
        ),
        (
            "SELECT CAST('2024-01-01 02:00:00+05:30' AS TIMESTAMP) AS v",
            1_704_074_400_000_000_i64,
        ),
    ] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ntz_timestamp_type(),
            "{sql}"
        );
        assert_eq!(
            batches[0]
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            expected,
            "{sql}"
        );
    }
    let err = ctx
        .sql("SELECT CAST('bad' AS TIMESTAMP) AS v")
        .await
        .expect("plan")
        .collect()
        .await
        .expect_err("ansi malformed raises")
        .to_string();
    assert!(err.contains("[CAST_INVALID_INPUT]"), "{err}");
    assert!(err.contains("\"TIMESTAMP_NTZ\""), "{err}");
}

#[tokio::test]
async fn date_trunc_over_ntz_returns_ltz_instant_in_session_zone() {
    for (zone, expected) in [
        ("America/New_York", 1_710_054_000_000_000_i64),
        ("UTC", 1_710_036_000_000_000_i64),
    ] {
        let ctx = ctx_at(zone);
        let batches = ctx
            .sql("SELECT date_trunc('hour', __repark_timestamp_ntz__(1710037845123456)) AS v")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ltz_timestamp_type(),
            "{zone}"
        );
        assert_eq!(
            batches[0]
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            expected,
            "{zone}"
        );
    }
}

#[tokio::test]
async fn nullability_pins_for_try_cast_date_ltz_ntz_date_and_legacy_unix_timestamp() {
    let ctx = ctx_at("America/New_York");
    let try_batches = ctx
        .sql("SELECT TRY_CAST('2024-03-10 02:30:00' AS TIMESTAMP) AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(try_batches[0].schema().field(0).is_nullable());

    let date_ltz = ctx
        .sql("SELECT __repark_cast_timestamp_ltz__(DATE '2024-01-01') AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!date_ltz[0].schema().field(0).is_nullable());

    let ntz_to_date = ctx
        .sql("SELECT CAST(__repark_timestamp_ntz__(1704074400000000) AS DATE) AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(ntz_to_date[0].schema().field(0).is_nullable());

    let ltz_to_date = ctx
        .sql("SELECT CAST(TIMESTAMP '2024-01-01 02:00:00' AS DATE) AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!ltz_to_date[0].schema().field(0).is_nullable());

    let ansi_unix = ctx
        .sql("SELECT unix_timestamp(TIMESTAMP '2024-01-01 02:00:00') AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!ansi_unix[0].schema().field(0).is_nullable());

    let ntz_ctx = ctx_ntz_at("America/New_York");
    let ntz_date_ltz = ntz_ctx
        .sql("SELECT __repark_cast_timestamp_ltz__(DATE '2024-01-01') AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!ntz_date_ltz[0].schema().field(0).is_nullable());

    let legacy_cfg = crate::ansi::with_spark_ansi_config(
        crate::session_time_zone::with_session_time_zone(
            datafusion::prelude::SessionConfig::new(),
            "America/New_York",
        ),
        false,
    );
    let legacy_ctx = crate::spark_type_planner::planned_context(legacy_cfg);
    crate::register_all(&legacy_ctx);
    for rule in crate::analyzer_rules() {
        legacy_ctx.add_analyzer_rule(rule);
    }
    let legacy_unix = legacy_ctx
        .sql("SELECT unix_timestamp(TIMESTAMP '2024-01-01 02:00:00') AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(legacy_unix[0].schema().field(0).is_nullable());
}
