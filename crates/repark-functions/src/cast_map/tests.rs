use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionConfig;

use super::*;
use crate::ansi::with_spark_ansi_config;

fn context(ansi: bool) -> SessionContext {
    let ctx = SessionContext::new_with_config(with_spark_ansi_config(SessionConfig::new(), ansi));
    register(&ctx);
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let rewritten = rewrite_map_casts(sql).unwrap_or_else(|| sql.to_owned());
    ctx.sql(&rewritten).await?.collect().await
}

fn first_cell(batches: &[RecordBatch]) -> String {
    array_value_to_string(batches[0].column(0).as_ref(), 0).unwrap()
}

fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", key, false),
                    Field::new("value", value, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

#[test]
fn map_target_parses_any_case_spacing_and_nesting() {
    let flat = map_type(DataType::Utf8, DataType::Int64);
    for text in [
        "MAP<STRING, BIGINT>",
        "map<string,bigint>",
        "Map < String , Long >",
    ] {
        assert_eq!(map_cast_target(text), Some(flat.clone()), "{text}");
    }
    let nested = map_cast_target("MAP<STRING, MAP<STRING, ARRAY<INT>>>").unwrap();
    assert_eq!(
        spark_sql_name(&nested),
        "MAP<STRING, MAP<STRING, ARRAY<INT>>>"
    );
    let structured = map_cast_target("STRUCT<m: MAP<STRING, DECIMAL(10,2)>>").unwrap();
    assert_eq!(
        spark_sql_name(&structured),
        "STRUCT<m: MAP<STRING, DECIMAL(10,2)>>"
    );
    assert_eq!(
        map_cast_token("array<map<string,long>>").as_deref(),
        Some("ARRAY<MAP<STRING, BIGINT>>")
    );
}

#[test]
fn non_map_or_malformed_targets_are_not_map_casts() {
    for text in [
        "ARRAY<INT>",
        "STRUCT<a: INT>",
        "INT",
        "MAP<STRING>",
        "MAP<STRING, INT",
        "MAP<STRING, INT> extra",
        "MAP<STRING, NOPE>",
        "MAP<STRING, DECIMAL(40,2)>",
    ] {
        assert_eq!(map_cast_target(text), None, "{text}");
    }
}

#[test]
fn rewrite_leaves_sql_without_a_map_cast_untouched() {
    for sql in [
        "SELECT 1",
        "SELECT CAST(a AS INT) FROM t",
        "SELECT map('a', 1) AS m",
        "SELECT 'CAST(x AS MAP<STRING, INT>)' AS s",
        "SELECT 1 -- CAST(x AS MAP<STRING, INT>)",
        "SELECT CAST(NULL AS ARRAY<INT>) AS a",
    ] {
        assert_eq!(rewrite_map_casts(sql), None, "{sql}");
    }
}

#[test]
fn rewrite_replaces_only_the_map_cast_and_keeps_the_rest_verbatim() {
    let rewritten =
        rewrite_map_casts("SELECT 'it''s', CAST(m AS MAP<STRING, BIGINT>) AS v, é FROM t").unwrap();
    assert!(rewritten.starts_with("SELECT 'it''s', __repark_cast_map__(m, '"));
    assert!(rewritten.ends_with("', false) AS v, é FROM t"));
    let nested =
        rewrite_map_casts("SELECT try_cast(CAST(m AS MAP<STRING,INT>) AS MAP<STRING,BIGINT>)")
            .unwrap();
    assert_eq!(nested.matches(CAST_MAP_NAME).count(), 2);
    assert!(nested.ends_with("', true)"));
    let empty = rewrite_map_casts("SELECT CAST(map() AS MAP<STRING, INT>)").unwrap();
    assert!(empty.starts_with("SELECT __repark_empty_map_cast__(NULL, '"));
}

#[tokio::test]
async fn null_empty_and_widened_maps_answer() {
    let ctx = context(true);
    let null = run(&ctx, "SELECT CAST(NULL AS MAP<STRING, INT>) AS m")
        .await
        .unwrap();
    assert!(null[0].column(0).is_null(0));
    assert_eq!(
        null[0].schema().field(0).data_type(),
        &map_type(DataType::Utf8, DataType::Int32)
    );
    assert!(null[0].schema().field(0).is_nullable());
    let empty = run(&ctx, "SELECT CAST(map() AS MAP<STRING, INT>) AS m")
        .await
        .unwrap();
    assert_eq!(first_cell(&empty), "{}");
    assert!(!empty[0].schema().field(0).is_nullable());
    let widened = run(&ctx, "SELECT CAST(map('a', 1) AS MAP<STRING, BIGINT>) AS m")
        .await
        .unwrap();
    assert_eq!(first_cell(&widened), "{a: 1}");
    assert_eq!(
        widened[0].schema().field(0).data_type(),
        &map_type(DataType::Utf8, DataType::Int64)
    );
    let keys = run(&ctx, "SELECT CAST(map(1, 'x') AS MAP<STRING, STRING>) AS m")
        .await
        .unwrap();
    assert_eq!(first_cell(&keys), "{1: x}");
}

#[tokio::test]
async fn invalid_value_raises_under_ansi_and_nulls_otherwise() {
    let sql = "SELECT CAST(map('a', 'x') AS MAP<STRING, INT>) AS m";
    let error = run(&context(true), sql).await.unwrap_err().to_string();
    assert!(
        error.contains(
            "[CAST_INVALID_INPUT] The value 'x' of the type \"STRING\" cannot be cast to \"INT\""
        ),
        "{error}"
    );
    let legacy = run(&context(false), sql).await.unwrap();
    assert_eq!(first_cell(&legacy), "{a: }");
    let tried = run(
        &context(true),
        "SELECT try_cast(map('a', 'x') AS MAP<STRING, INT>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&tried), "{a: }");
}

#[tokio::test]
async fn try_cast_and_null_sources_plan_nullable() {
    let ctx = context(true);
    for (sql, nullable) in [
        (
            "SELECT try_cast(map('a', 'x') AS MAP<STRING, INT>) AS m",
            true,
        ),
        ("SELECT CAST(NULL AS MAP<STRING, BIGINT>) AS m", true),
        ("SELECT CAST(map() AS MAP<STRING, INT>) AS m", false),
    ] {
        let rewritten = rewrite_map_casts(sql).unwrap();
        let frame = ctx.sql(&rewritten).await.unwrap();
        assert_eq!(frame.schema().field(0).is_nullable(), nullable, "{sql}");
    }
}

#[tokio::test]
async fn non_map_source_refuses_at_planning() {
    let error = run(&context(true), "SELECT CAST(1 AS MAP<STRING, INT>) AS m")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]")
            && error.contains("\" to \"MAP<STRING, INT>\". SQLSTATE: 42K09"),
        "{error}"
    );
}

#[tokio::test]
async fn unaliased_cast_keeps_the_spark_projection_name() {
    let batches = run(&context(true), "SELECT CAST(NULL AS MAP<STRING, INT>)")
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().field(0).name(),
        "CAST(NULL AS MAP<STRING, INT>)"
    );
}

#[test]
fn comment_hint_is_an_ordinary_comment() {
    for sql in [
        "SELECT 1 /*! CAST(x AS MAP<STRING,INT>) */, 2",
        "SELECT 1 /* CAST(x AS MAP<STRING,INT>) */, 2",
    ] {
        assert_eq!(rewrite_map_casts(sql), None, "{sql}");
    }
}

#[tokio::test]
async fn colliding_keys_after_the_key_cast_stay_as_spark_stores_them() {
    let batches = run(
        &context(true),
        "SELECT CAST(map('2', 'a', '1', 'b', '02', 'c') AS MAP<INT, STRING>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&batches), "{2: a, 1: b, 2: c}");
}

#[tokio::test]
async fn key_legality_follows_the_session_mode_and_try_cast() {
    let refusal = "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]";
    let legacy = run(
        &context(false),
        "SELECT CAST(map('1', 'a') AS MAP<INT, STRING>) AS m",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(legacy.contains(refusal), "{legacy}");
    for ansi in [true, false] {
        let tried = run(
            &context(ansi),
            "SELECT try_cast(map('x', 1) AS MAP<INT, INT>) AS m",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(tried.contains(refusal), "{tried}");
        let widened = run(
            &context(ansi),
            "SELECT try_cast(map(1, 'a') AS MAP<BIGINT, STRING>) AS m",
        )
        .await
        .unwrap();
        assert_eq!(first_cell(&widened), "{1: a}");
    }
}

#[tokio::test]
async fn leaf_casts_follow_spark_overflow_and_trimming() {
    let overflow = "SELECT CAST(map('a', 128) AS MAP<STRING, TINYINT>) AS m";
    let error = run(&context(true), overflow).await.unwrap_err().to_string();
    assert!(
        error.contains("[CAST_OVERFLOW] The value 128L of the type \"BIGINT\" cannot be cast to \"TINYINT\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. SQLSTATE: 22003"),
        "{error}"
    );
    let wrapped = run(&context(false), overflow).await.unwrap();
    assert_eq!(first_cell(&wrapped), "{a: -128}");
    for ansi in [true, false] {
        let trimmed = run(
            &context(ansi),
            "SELECT CAST(map('a', ' 1') AS MAP<STRING, INT>) AS m",
        )
        .await
        .unwrap();
        assert_eq!(first_cell(&trimmed), "{a: 1}");
    }
    let legacy_fraction = run(
        &context(false),
        "SELECT CAST(map('a', '1.5') AS MAP<STRING, INT>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&legacy_fraction), "{a: 1}");
    let boolean = run(
        &context(true),
        "SELECT CAST(map('a', ' true ') AS MAP<STRING, BOOLEAN>) AS m",
    )
    .await
    .unwrap();
    assert_eq!(first_cell(&boolean), "{a: true}");
}

#[tokio::test]
async fn overflow_names_the_source_type_as_spark_does() {
    let error = run(
        &context(true),
        "SELECT CAST(map('a', CAST(128 AS INT)) AS MAP<STRING, TINYINT>) AS m",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains(
            "[CAST_OVERFLOW] The value 128 of the type \"INT\" cannot be cast to \"TINYINT\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. SQLSTATE: 22003"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn ascii_controls_and_del_trim_but_c1_controls_do_not() {
    for ansi in [true, false] {
        for (sql, expected) in [
            (
                "SELECT CAST(map('a', concat(chr(127), '1')) AS MAP<STRING, INT>) AS m",
                "{a: 1}",
            ),
            (
                "SELECT CAST(map('a', concat(chr(9), '1')) AS MAP<STRING, INT>) AS m",
                "{a: 1}",
            ),
            (
                "SELECT CAST(map('a', concat(chr(127), 'true')) AS MAP<STRING, BOOLEAN>) AS m",
                "{a: true}",
            ),
        ] {
            let batches = run(&context(ansi), sql).await.unwrap();
            assert_eq!(first_cell(&batches), expected, "{sql} ansi={ansi}");
        }
    }
    let c1 = "SELECT CAST(map('a', concat(chr(133), '1')) AS MAP<STRING, INT>) AS m";
    let error = run(&context(true), c1).await.unwrap_err().to_string();
    assert!(
        error.contains("[CAST_INVALID_INPUT] The value '\u{85}1'"),
        "{error}"
    );
    let legacy = run(&context(false), c1).await.unwrap();
    assert_eq!(first_cell(&legacy), "{a: }");
}

#[tokio::test]
async fn try_cast_key_overflow_refuses_loudly() {
    for ansi in [true, false] {
        let error = run(
            &context(ansi),
            "SELECT try_cast(map(CAST(128 AS INT), 'a') AS MAP<TINYINT, STRING>) AS m",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("a map key cast produced NULL"), "{error}");
    }
}

fn integral_context(ansi: bool) -> SessionContext {
    let mut rules =
        insert_string_integral_before_coercion(datafusion::optimizer::Analyzer::new().rules)
            .unwrap();
    rules.push(Arc::new(SparkCoercionCastMark));
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(with_spark_ansi_config(SessionConfig::new(), ansi))
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    register(&ctx);
    ctx
}

async fn integral(ansi: bool, text: &str, target: &str) -> Result<String> {
    let sql = format!("SELECT CAST(s AS {target}) AS r FROM (VALUES ('{text}')) AS t(s)");
    let batches = integral_context(ansi).sql(&sql).await?.collect().await?;
    Ok(first_cell(&batches))
}

#[tokio::test]
async fn string_integral_cast_trims_both_modes_and_nulls_only_with_ansi_off() {
    for ansi in [true, false] {
        for (text, target, expected) in [
            (" 42 ", "INT", "42"),
            ("\t7\n", "TINYINT", "7"),
            ("\u{0}3\u{7f}", "SMALLINT", "3"),
            ("+5", "BIGINT", "5"),
            ("-0", "INT", "0"),
            ("-128", "TINYINT", "-128"),
            (" 9223372036854775807", "BIGINT", "9223372036854775807"),
        ] {
            assert_eq!(
                integral(ansi, text, target).await.unwrap(),
                expected,
                "{text:?} {target} ansi={ansi}"
            );
        }
    }
    for (text, target, expected) in [
        ("1.9", "INT", "1"),
        ("-1.9", "INT", "-1"),
        (".", "INT", "0"),
        ("-.", "INT", "0"),
        ("abc", "INT", ""),
        ("", "BIGINT", ""),
        ("1e3", "INT", ""),
        ("128", "TINYINT", ""),
        ("\u{a0}3", "INT", ""),
        ("1..2", "SMALLINT", ""),
    ] {
        assert_eq!(
            integral(false, text, target).await.unwrap(),
            expected,
            "{text:?} {target}"
        );
    }
    for (text, target, spark) in [
        ("1.0", "INT", "INT"),
        ("128", "TINYINT", "TINYINT"),
        ("", "BIGINT", "BIGINT"),
        ("4 2", "SMALLINT", "SMALLINT"),
    ] {
        let error = integral(true, text, target).await.unwrap_err().to_string();
        assert!(
            error.contains(&format!(
                "[CAST_INVALID_INPUT] The value '{text}' of the type \"STRING\" cannot be cast to \"{spark}\" because it is malformed."
            )),
            "{error}"
        );
    }
}

#[tokio::test]
async fn string_integral_cast_keeps_the_name_type_and_leaves_other_casts() {
    let ctx = integral_context(false);
    let frame = ctx
        .sql("SELECT CAST(s AS INT), TRY_CAST(s AS INT) AS r, CAST(s AS VARCHAR) AS d FROM (VALUES (' 4')) AS t(s)")
        .await
        .unwrap();
    let plan = ctx.state().optimize(frame.logical_plan()).unwrap();
    let names: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["t.s", "r", "d"].map(String::from));
    assert_eq!(plan.schema().field(0).data_type(), &DataType::Int32);
    let shown = plan.display_indent().to_string();
    assert!(shown.contains("__repark_string_to_int__(t.s)"), "{shown}");
    assert!(shown.contains("TRY_CAST(t.s AS Int32)"), "{shown}");
    let batches = frame.collect().await.unwrap();
    assert_eq!(first_cell(&batches), "4");
    assert!(batches[0].column(1).is_null(0));
}

#[tokio::test]
async fn arrow_cast_operand_keeps_arrow_parsing() {
    let sql = format!(
        "SELECT CAST({ARROW_CAST_OPERAND_NAME}(s) AS INT) AS r FROM (VALUES (' 4')) AS t(s)"
    );
    for ansi in [true, false] {
        let error = integral_context(ansi)
            .sql(&sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("Cannot cast string ' 4'"), "{error}");
    }
}

#[tokio::test]
async fn a_cast_planned_inside_values_keeps_arrow_parsing() {
    for ansi in [true, false] {
        let error = integral_context(ansi)
            .sql("SELECT * FROM (VALUES (CAST(' 4' AS INT))) AS t(r)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("Cannot cast string ' 4'"), "{error}");
    }
}

#[tokio::test]
async fn an_outer_projection_sees_the_nullable_cast_with_ansi_off() {
    let ctx = integral_context(false);
    let sql = "SELECT r FROM (SELECT CAST(a AS INT) AS r FROM (VALUES ('abc')) AS t(a)) AS u WHERE r IS NULL";
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn an_in_list_keeps_arrow_parsing() {
    let sql = "SELECT n FROM (VALUES (1), (2)) AS t(n) WHERE CAST(n AS INT) IN (' 2', '3 ')";
    for ansi in [true, false] {
        let error = integral_context(ansi)
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("Cannot cast string ' 2'"), "{error}");
    }
}

#[tokio::test]
async fn a_projection_built_without_name_checks_keeps_its_names() {
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{LogicalPlan, Projection, cast, col};
    use datafusion::optimizer::AnalyzerRule;
    let ctx = integral_context(false);
    let source = ctx
        .sql("SELECT s FROM (VALUES (' 4')) AS t(s)")
        .await
        .unwrap()
        .into_unoptimized_plan();
    let exprs = vec![col("t.s"), cast(col("t.s"), DataType::Int32)];
    let input = Arc::new(source);
    let mut fields: Vec<_> = input
        .schema()
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect();
    fields.push((None, Arc::new(Field::new("n", DataType::Int32, true))));
    let metadata = std::collections::HashMap::new();
    let schema = Arc::new(DFSchema::new_with_metadata(fields, metadata).unwrap());
    let plan =
        LogicalPlan::Projection(Projection::try_new_with_schema(exprs, input, schema).unwrap());
    let analyzed = SparkStringIntegralCast
        .analyze(plan, ctx.state().config_options())
        .unwrap();
    let names: Vec<String> = analyzed
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["s", "n"].map(String::from));
    assert!(
        analyzed
            .display_indent()
            .to_string()
            .contains("__repark_string_to_int__")
    );
}

#[tokio::test]
async fn a_join_on_a_string_key_against_an_integer_key_plans_and_trims() {
    let ctx = integral_context(false);
    let sql = "SELECT l.id FROM (VALUES (1), (2)) AS l(id) \
               FULL JOIN (VALUES (' 2'), ('3')) AS r(id) ON l.id = CAST(r.id AS BIGINT)";
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn a_cast_the_coercion_inserts_keeps_arrow_parsing() {
    for (sql, text) in [
        (
            "SELECT coalesce(s, 0) FROM (VALUES ('abc')) AS t(s)",
            "'abc'",
        ),
        (
            "SELECT l.id FROM (VALUES (1)) AS l(id) JOIN (VALUES (' 1')) AS r(id) ON l.id = r.id",
            "' 1'",
        ),
    ] {
        let error = integral_context(false)
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!("Cannot cast string {text}")),
            "{sql}: {error}"
        );
    }
}

#[tokio::test]
async fn a_user_cast_on_the_left_of_an_in_list_trims() {
    let ctx = integral_context(false);
    let sql =
        "SELECT s FROM (VALUES (' 5 '), ('abc'), ('2')) AS t(s) WHERE CAST(s AS INT) IN (5, 1)";
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn a_second_analysis_leaves_the_marked_coercion_cast() {
    let ctx = integral_context(false);
    let frame = ctx
        .sql("SELECT coalesce(s, 0) AS c FROM (VALUES ('7')) AS t(s)")
        .await
        .unwrap();
    let once = ctx.state().analyzer().execute_and_check(
        frame.logical_plan().clone(),
        ctx.state().config_options(),
        |_, _| {},
    );
    let once = once.unwrap();
    let twice = ctx
        .state()
        .analyzer()
        .execute_and_check(once.clone(), ctx.state().config_options(), |_, _| {})
        .unwrap();
    assert_eq!(once, twice);
    assert!(
        !twice
            .display_indent()
            .to_string()
            .contains("__repark_string_to_")
    );
}
