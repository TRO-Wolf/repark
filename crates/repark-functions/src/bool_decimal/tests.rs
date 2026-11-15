use super::*;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionConfig;

fn ctx_with(ansi: Option<bool>) -> SessionContext {
    let config = match ansi {
        Some(enabled) => crate::ansi::with_spark_ansi_config(SessionConfig::new(), enabled),
        None => SessionConfig::new(),
    };
    let ctx = SessionContext::new_with_config(config);
    ctx.add_analyzer_rule(Arc::new(BoolDecimalCast { spark_door: true }));
    ctx
}

async fn flags(ctx: &SessionContext, sql: &str) -> Vec<bool> {
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    crate::analyze_eagerly(&ctx.state(), plan)
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|field| field.is_nullable())
        .collect()
}

async fn values(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        for row in 0..batch.num_rows() {
            out.push(array_value_to_string(column, row).unwrap());
        }
    }
    out
}

#[test]
fn a_float_cast_to_decimal_of_a_nonnull_operand_is_nullable_as_spark() {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::logical_expr::{Cast, TryCast, col};
    let target = DataType::Decimal128(20, 0);
    for nullable in [false, true] {
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "x",
            DataType::Float64,
            nullable,
        )]))
        .unwrap();
        for expr in [
            Expr::Cast(Cast::new(Box::new(col("x")), target.clone())),
            Expr::TryCast(TryCast::new(Box::new(col("x")), target.clone())),
        ] {
            let rewritten = rewrite_cast(expr, &schema, false, true).data;
            let Expr::ScalarFunction(function) = &rewritten else {
                panic!("not rewritten: {rewritten}");
            };
            assert_eq!(function.func.name(), FLOAT_TO_DECIMAL_NAME);
            let (_, field) = rewritten.to_field(&schema).unwrap();
            assert!(field.is_nullable(), "{rewritten} on nullable={nullable}");
        }
    }
}

#[tokio::test]
async fn wide_targets_are_nonnull_with_one_and_zero() {
    let ctx = ctx_with(Some(true));
    assert_eq!(
        flags(&ctx, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
        vec![false]
    );
    assert_eq!(
        values(&ctx, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
        vec!["1.00".to_string()]
    );
    assert_eq!(
        values(&ctx, "SELECT CAST(false AS DECIMAL(10,2)) AS v").await,
        vec!["0.00".to_string()]
    );
    assert_eq!(
        values(&ctx, "SELECT CAST(true AS DECIMAL(1,0)) AS v").await,
        vec!["1".to_string()]
    );
}

#[tokio::test]
async fn narrow_target_nulls_or_raises_by_ansi() {
    let legacy = ctx_with(Some(false));
    assert_eq!(
        flags(&legacy, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
        vec![true]
    );
    assert_eq!(
        values(&legacy, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
        vec![String::new()]
    );
    let ansi = ctx_with(Some(true));
    assert_eq!(
        flags(&ansi, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
        vec![true]
    );
    let error = ansi
        .sql("SELECT CAST(true AS DECIMAL(2,2)) AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("NUMERIC_VALUE_OUT_OF_RANGE"), "{error}");
}

#[tokio::test]
async fn native_door_defaults_to_ansi() {
    let native = ctx_with(None);
    let error = native
        .sql("SELECT CAST(true AS DECIMAL(2,2)) AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("NUMERIC_VALUE_OUT_OF_RANGE"), "{error}");
    assert_eq!(
        values(&native, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
        vec!["1.00".to_string()]
    );
}

#[test]
fn the_kernel_rounds_the_jdk_17_text_half_up() {
    use repark_common::java_double::java_double_unscaled;
    let widened = |value: f32| f64::from(value);
    for (value, scale, want) in [
        (1.255, 2, Some(126)),
        (1.005, 2, Some(101)),
        (2.675, 2, Some(268)),
        (9.995, 2, Some(1000)),
        (-1.255, 2, Some(-126)),
        (0.125, 2, Some(13)),
        (-0.0, 2, Some(0)),
        (1e-20, 18, Some(0)),
        (1.254_999_999_999_999_7, 2, Some(125)),
        (0.300_000_000_000_000_04, 18, Some(300_000_000_000_000_040)),
        (1.005, 18, Some(1_005_000_000_000_000_000)),
        (1e23, 0, Some(99_999_999_999_999_990_000_000)),
        (
            2.828_793_848_061_59e17,
            18,
            Some(282_879_384_806_159_008 * 10_i128.pow(18)),
        ),
        (widened(1.255), 2, Some(125)),
        (widened(1.255), 18, Some(1_254_999_995_231_628_400)),
        (1.255, 30, Some(1255 * 10_i128.pow(27))),
        (1e300, 2, None),
        (f64::NAN, 2, None),
        (f64::INFINITY, 0, None),
    ] {
        assert_eq!(
            java_double_unscaled(value, scale),
            want,
            "{value:?} at scale {scale}"
        );
    }
}

fn half_up_from_text(text: &str, scale: i8) -> i128 {
    let (negative, body) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (mantissa, exponent) = body
        .split_once('E')
        .map_or((body, 0), |(m, e)| (m, e.parse::<i32>().unwrap()));
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: String = format!("{whole}{fraction}");
    let point = i32::try_from(whole.len()).unwrap() + exponent + i32::from(scale);
    let padded = if point > i32::try_from(digits.len()).unwrap() {
        format!(
            "{digits}{}",
            "0".repeat(usize::try_from(point).unwrap() - digits.len())
        )
    } else {
        digits
    };
    let (kept, next) = if point <= 0 {
        (
            String::from("0"),
            if point == 0 {
                padded.chars().next()
            } else {
                None
            },
        )
    } else {
        let cut = usize::try_from(point).unwrap();
        (padded[..cut].to_string(), padded.chars().nth(cut))
    };
    let mut value: i128 = kept.parse().unwrap();
    if next.is_some_and(|digit| digit >= '5') {
        value += 1;
    }
    if negative { -value } else { value }
}

#[test]
fn the_fast_path_agrees_with_the_text_on_a_dense_sweep() {
    use repark_common::java_double::java_double_unscaled;
    let mut value = 0.000_1_f64;
    let mut checked = 0;
    while value < 2_000.0 {
        for scale in [0_i8, 1, 2, 3, 4, 6, 9] {
            for signed in [value, -value] {
                let text = crate::java_double::java_double_text(signed);
                assert_eq!(
                    java_double_unscaled(signed, scale),
                    Some(half_up_from_text(&text, scale)),
                    "{text} at scale {scale}"
                );
                checked += 1;
            }
        }
        value = value * 1.000_37 + 0.000_013;
    }
    for thousandths in 0..300_000_u32 {
        let value = f64::from(thousandths) / 1000.0;
        for scale in [0_i8, 1, 2, 15] {
            let text = crate::java_double::java_double_text(value);
            assert_eq!(
                java_double_unscaled(value, scale),
                Some(half_up_from_text(&text, scale)),
                "{text} at scale {scale}"
            );
            checked += 1;
        }
    }
    assert!(checked > 1_000_000, "{checked}");
}

async fn first(ctx: &SessionContext, sql: &str) -> String {
    values(ctx, sql)
        .await
        .into_iter()
        .next()
        .unwrap_or_default()
}

#[tokio::test]
async fn a_float_cast_try_cast_and_arrow_cast_to_decimal_round_the_text() {
    let ctx = ctx_with(Some(true));
    for (sql, want) in [
        (
            "SELECT CAST(CAST('1.255' AS DOUBLE) AS DECIMAL(10,2))",
            "1.26",
        ),
        (
            "SELECT CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2))",
            "1.01",
        ),
        (
            "SELECT CAST(CAST('-1.255' AS DOUBLE) AS DECIMAL(10,2))",
            "-1.26",
        ),
        (
            "SELECT CAST(CAST('1.255' AS FLOAT) AS DECIMAL(10,2))",
            "1.25",
        ),
        (
            "SELECT TRY_CAST(CAST('1.255' AS DOUBLE) AS DECIMAL(10,2))",
            "1.26",
        ),
        (
            "SELECT arrow_cast(CAST('1.255' AS DOUBLE), 'Decimal128(10, 2)')",
            "1.26",
        ),
        (
            "SELECT TRY_CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(10,2))",
            "",
        ),
        (
            "SELECT TRY_CAST(CAST('1e10' AS DOUBLE) AS DECIMAL(10,2))",
            "",
        ),
    ] {
        assert_eq!(first(&ctx, sql).await, want, "{sql}");
    }
    assert_eq!(
        first(
            &ctx,
            "SELECT CAST(v AS DECIMAL(10,2)) FROM (VALUES (CAST('1.255' AS DOUBLE))) t(v)"
        )
        .await,
        "1.26"
    );
}

#[tokio::test]
async fn a_float_cast_to_decimal_keeps_todays_refusals() {
    for ansi in [Some(true), Some(false), None] {
        let ctx = ctx_with(ansi);
        for (sql, text) in [
            (
                "SELECT CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(10,2))",
                "Cannot cast to Decimal128(10, 2). Overflowing on NaN",
            ),
            (
                "SELECT CAST(CAST('-Infinity' AS DOUBLE) AS DECIMAL(10,2))",
                "Overflowing on -inf",
            ),
            (
                "SELECT CAST(CAST('123456789.125' AS DOUBLE) AS DECIMAL(10,2))",
                "123456789.13 is too large to store in a Decimal128 of precision 10",
            ),
            (
                "SELECT CAST(CAST('1e300' AS DOUBLE) AS DECIMAL(38,2))",
                "Overflowing on 1e300",
            ),
        ] {
            let error = ctx
                .sql(sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(text), "{sql}: {error}");
        }
    }
}

fn native_ctx() -> SessionContext {
    let ctx = SessionContext::new_with_config(crate::ansi::with_spark_ansi_config(
        SessionConfig::new(),
        true,
    ));
    ctx.add_analyzer_rule(Arc::new(BoolDecimalCast { spark_door: false }));
    ctx
}

async fn analyzed(ctx: &SessionContext, sql: &str) -> String {
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    crate::analyze_eagerly(&ctx.state(), plan)
        .unwrap()
        .display_indent()
        .to_string()
}

#[tokio::test]
async fn the_native_door_leaves_float_and_decimal_casts_to_their_cast() {
    let ctx = native_ctx();
    for sql in [
        "SELECT CAST(v AS DECIMAL(10,2)) FROM (VALUES (CAST('1.255' AS DOUBLE))) t(v)",
        "SELECT TRY_CAST(v AS DECIMAL(38,18)) FROM (VALUES (CAST('1.255' AS DOUBLE))) t(v)",
        "SELECT CAST(v AS DECIMAL(10,2)) FROM (VALUES (CAST('1.255' AS FLOAT))) t(v)",
        "SELECT arrow_cast(v, 'Decimal128(10, 2)') FROM (VALUES (CAST('1.255' AS DOUBLE))) t(v)",
        "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST('1.25' AS DECIMAL(38,18)))) t(d)",
    ] {
        let shown = analyzed(&ctx, sql).await;
        assert!(!shown.contains("__repark_"), "{sql}: {shown}");
    }
    assert_eq!(
        first(
            &ctx,
            "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST('-22.129547339983247' AS DECIMAL(38,18)))) t(d)"
        )
        .await,
        "-22.129547339983244"
    );
    assert_eq!(
        values(
            &ctx,
            "SELECT CAST(true AS DECIMAL(10,2)) FROM (VALUES (CAST('1.255' AS DOUBLE))) t(v)"
        )
        .await,
        vec!["1.00".to_string()]
    );
}

#[tokio::test]
async fn a_decimal_cast_to_double_rounds_once_as_spark() {
    let ctx = ctx_with(Some(true));
    for (sql, expected) in [
        (
            "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST('-22.129547339983247' AS DECIMAL(38,18)))) t(d)",
            "-22.129547339983247",
        ),
        (
            "SELECT TRY_CAST(d AS DOUBLE) FROM (VALUES (CAST('-22.129547339983247' AS DECIMAL(38,18)))) t(d)",
            "-22.129547339983247",
        ),
        (
            "SELECT CAST(CAST('-22.129547339983247' AS DECIMAL(38,18)) AS DOUBLE)",
            "-22.129547339983247",
        ),
        (
            "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST('1.25' AS DECIMAL(10,2)))) t(d)",
            "1.25",
        ),
    ] {
        assert!(
            analyzed(&ctx, sql).await.contains(DECIMAL_TO_DOUBLE_NAME),
            "{sql}"
        );
        assert_eq!(first(&ctx, sql).await, expected, "{sql}");
    }
}

#[tokio::test]
async fn a_decimal_cast_to_double_keeps_its_nullability_and_nulls() {
    let ctx = ctx_with(Some(true));
    let sql = "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST('1.5' AS DECIMAL(10,2))), (NULL)) t(d)";
    assert_eq!(flags(&ctx, sql).await, vec![true]);
    assert_eq!(
        values(&ctx, sql).await,
        vec!["1.5".to_string(), String::new()]
    );
    for query in [
        "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST('1.5' AS DECIMAL(10,2)))) t(d)",
        "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST(1 AS DECIMAL(10,2)))) t(d)",
        "SELECT CAST(d AS DOUBLE) FROM (VALUES (CAST(1 AS DECIMAL(10,2))), (NULL)) t(d)",
    ] {
        assert_eq!(
            flags(&ctx, query).await,
            flags(&native_ctx(), query).await,
            "{query}"
        );
    }
}

#[test]
fn only_decimal128_to_double_is_rewritten() {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::logical_expr::{Cast, col};
    let schema = DFSchema::try_from(Schema::new(vec![
        Field::new("d", DataType::Decimal128(38, 18), true),
        Field::new("w", DataType::Decimal256(40, 2), true),
        Field::new("i", DataType::Int64, true),
    ]))
    .unwrap();
    let cast = |name: &str, target: DataType| Expr::Cast(Cast::new(Box::new(col(name)), target));
    assert!(decimal_double_cast(&cast("d", DataType::Float64), &schema).is_some());
    assert!(decimal_double_cast(&cast("d", DataType::Float32), &schema).is_none());
    assert!(decimal_double_cast(&cast("w", DataType::Float64), &schema).is_none());
    assert!(decimal_double_cast(&cast("i", DataType::Float64), &schema).is_none());
    assert!(!rewrite_cast(cast("d", DataType::Float64), &schema, false, false).transformed);
}

fn preimage_ctx() -> SessionContext {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    let ctx = ctx_with(None);
    let top38 = 10i128.pow(38) - 1;
    let d10: Vec<Option<i128>> = vec![
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(50),
        Some(51),
        Some(-50),
        Some(450),
        Some(9_999_999_999),
        Some(-9_999_999_999),
    ];
    let d38: Vec<Option<i128>> = vec![
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(500_000_000_000_000_000),
        Some(1_000_000_000_000_000_400),
        Some(-22_129_547_339_983_247_000),
        Some(4_500_000_000_000_000_000),
        Some(top38),
        Some(-top38),
    ];
    let ids: Vec<i32> = (0..10).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("d10", DataType::Decimal128(10, 2), true),
        Field::new("d38", DataType::Decimal128(38, 18), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(datafusion::arrow::array::Int32Array::from(ids)),
            Arc::new(
                Decimal128Array::from(d10)
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(d38)
                    .with_precision_and_scale(38, 18)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx
}

async fn optimized(ctx: &SessionContext, sql: &str) -> String {
    ctx.sql(sql)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap()
        .display_indent()
        .to_string()
}

async fn ids(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let mut out = values(ctx, sql).await;
    out.sort();
    out
}

const PREIMAGE_OPERATORS: [&str; 8] = [
    "=",
    "<>",
    "<",
    "<=",
    ">",
    ">=",
    "IS DISTINCT FROM",
    "IS NOT DISTINCT FROM",
];

#[tokio::test]
async fn a_decimal_column_against_a_double_constant_plans_no_conversion() {
    let ctx = preimage_ctx();
    for column in ["d10", "d38"] {
        for operator in PREIMAGE_OPERATORS {
            for sql in [
                format!("SELECT id FROM t WHERE CAST({column} AS DOUBLE) {operator} 0.5"),
                format!("SELECT id FROM t WHERE 0.5 {operator} CAST({column} AS DOUBLE)"),
                format!("SELECT id FROM t WHERE CAST({column} AS DOUBLE) {operator} -0.0"),
                format!(
                    "SELECT id FROM t WHERE CAST({column} AS DOUBLE) {operator} CAST(0.505 AS DOUBLE)"
                ),
            ] {
                let plan = optimized(&ctx, &sql).await;
                assert!(!plan.contains(DECIMAL_TO_DOUBLE_NAME), "{sql}\n{plan}");
                assert!(plan.contains(column), "{sql}\n{plan}");
            }
        }
        let listed =
            format!("SELECT id FROM t WHERE CAST({column} AS DOUBLE) IN (0.5, 4.5, -0.01)");
        assert!(
            !optimized(&ctx, &listed)
                .await
                .contains(DECIMAL_TO_DOUBLE_NAME),
            "{listed}"
        );
    }
}

#[tokio::test]
async fn a_constant_the_type_cannot_bound_keeps_the_per_row_conversion() {
    let ctx = preimage_ctx();
    for (column, above) in [("d10", "1e8"), ("d38", "1e20"), ("d38", "1e300")] {
        for constant in [
            "CAST('NaN' AS DOUBLE)",
            "CAST('inf' AS DOUBLE)",
            "CAST('-inf' AS DOUBLE)",
            above,
        ] {
            for operator in PREIMAGE_OPERATORS {
                for sql in [
                    format!(
                        "SELECT id FROM t WHERE CAST({column} AS DOUBLE) {operator} {constant}"
                    ),
                    format!(
                        "SELECT id FROM t WHERE {constant} {operator} CAST({column} AS DOUBLE)"
                    ),
                ] {
                    let plan = optimized(&ctx, &sql).await;
                    assert!(plan.contains(DECIMAL_TO_DOUBLE_NAME), "{sql}\n{plan}");
                }
            }
        }
    }
    let four = "SELECT id FROM t WHERE CAST(d10 AS DOUBLE) IN (0.5, 4.5, -0.01, 1.0)";
    assert!(optimized(&ctx, four).await.contains(DECIMAL_TO_DOUBLE_NAME));
}

#[tokio::test]
async fn the_rewritten_comparison_answers_as_the_per_row_form() {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    let ctx = preimage_ctx();
    let constants = [
        0.5,
        0.505,
        -0.5,
        0.0,
        -0.0,
        0.01,
        -0.01,
        0.005,
        4.5,
        1e-18,
        -1e-18,
        5e-19,
        1.000_000_000_000_000_4,
        -22.129_547_339_983_247,
        99_999_999.99,
        -99_999_999.99,
        -1e300,
        1e20,
        1e100,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    for constant in constants {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![constant]))],
        )
        .unwrap();
        ctx.deregister_table("k").unwrap();
        ctx.register_table(
            "k",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let written = format!("CAST('{constant:?}' AS DOUBLE)");
        for column in ["d10", "d38"] {
            for operator in PREIMAGE_OPERATORS {
                for (folded, held) in [
                    (
                        format!(
                            "SELECT id FROM t WHERE CAST({column} AS DOUBLE) {operator} {written}"
                        ),
                        format!(
                            "SELECT t.id FROM t, k WHERE CAST(t.{column} AS DOUBLE) {operator} k.c"
                        ),
                    ),
                    (
                        format!(
                            "SELECT id FROM t WHERE {written} {operator} CAST({column} AS DOUBLE)"
                        ),
                        format!(
                            "SELECT t.id FROM t, k WHERE k.c {operator} CAST(t.{column} AS DOUBLE)"
                        ),
                    ),
                ] {
                    assert!(
                        optimized(&ctx, &held)
                            .await
                            .contains(DECIMAL_TO_DOUBLE_NAME)
                    );
                    assert_eq!(ids(&ctx, &folded).await, ids(&ctx, &held).await, "{folded}");
                }
            }
        }
    }
}
