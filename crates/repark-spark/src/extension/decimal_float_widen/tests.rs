use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::{SparkDecimalFloatWiden, insert_decimal_float_widen_before_coercion};

fn ctx() -> SessionContext {
    let rules = insert_decimal_float_widen_before_coercion(Analyzer::new().rules).unwrap();
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    SessionContext::new_with_state(state)
}

async fn setup() -> SessionContext {
    let ctx = ctx();
    ctx.sql(
        "CREATE VIEW src AS SELECT * FROM (VALUES (1, CAST(0 AS DECIMAL(10,2)), CAST(1e-20 AS DOUBLE), CAST(0.5 AS DECIMAL(38,18)), CAST(0.5 AS FLOAT))) t(id, dc, dd, dbig, ff)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    ctx
}

async fn answer(ctx: &SessionContext, sql: &str) -> (DataType, Vec<String>) {
    let frame = ctx.sql(sql).await.unwrap();
    let physical = frame.clone().create_physical_plan().await.unwrap();
    let data_type = physical.schema().field(0).data_type().clone();
    let mut values = Vec::new();
    for batch in frame.collect().await.unwrap() {
        for row in 0..batch.num_rows() {
            values.push(array_value_to_string(batch.column(0), row).unwrap());
        }
    }
    values.sort();
    (data_type, values)
}

#[tokio::test]
async fn set_operations_unify_a_decimal_and_a_double_to_double() {
    let ctx = setup().await;
    for (sql, expected) in [
        (
            "SELECT dc AS c FROM src UNION ALL SELECT dd FROM src",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dd AS c FROM src UNION ALL SELECT dc FROM src",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION SELECT dd FROM src",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION ALL SELECT ff FROM src",
            vec!["0.0", "0.5"],
        ),
        (
            "SELECT dc AS c FROM src INTERSECT SELECT dd FROM src",
            vec![],
        ),
        (
            "SELECT dc AS c FROM src EXCEPT SELECT dd FROM src",
            vec!["0.0"],
        ),
    ] {
        let (data_type, values) = answer(&ctx, sql).await;
        assert_eq!(data_type, DataType::Float64, "{sql}");
        assert_eq!(values, expected, "{sql}");
    }
}

#[tokio::test]
async fn expressions_unify_a_decimal_and_a_double_to_double() {
    let ctx = setup().await;
    for (sql, data_type, expected) in [
        ("SELECT dc = dd FROM src", DataType::Boolean, "false"),
        ("SELECT dd = dc FROM src", DataType::Boolean, "false"),
        (
            "SELECT dd IN (dc, dbig) FROM src",
            DataType::Boolean,
            "false",
        ),
        (
            "SELECT dd BETWEEN dc AND dc FROM src",
            DataType::Boolean,
            "false",
        ),
        (
            "SELECT dc BETWEEN dc AND dd FROM src",
            DataType::Boolean,
            "true",
        ),
        (
            "SELECT count(*) FROM src WHERE dd IN (SELECT dc FROM src)",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT count(*) FROM src WHERE dc IN (SELECT dd FROM src)",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT CASE WHEN id = 2 THEN dc ELSE dd END FROM src",
            DataType::Float64,
            "1e-20",
        ),
        (
            "SELECT CASE dc WHEN dd THEN 1 ELSE 0 END FROM src",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT CASE dc WHEN dd THEN 1 WHEN dbig THEN 2 ELSE 0 END FROM src",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT coalesce(dd, dc) FROM src",
            DataType::Float64,
            "1e-20",
        ),
        (
            "SELECT greatest(dc, dd) FROM src",
            DataType::Float64,
            "1e-20",
        ),
        (
            "SELECT least(dbig, dd) FROM src",
            DataType::Float64,
            "1e-20",
        ),
        ("SELECT nullif(dd, dc) FROM src", DataType::Float64, "1e-20"),
        (
            "SELECT nullif(dc, dd) FROM src",
            DataType::Decimal128(10, 2),
            "0.00",
        ),
        ("SELECT nullif(ff, dbig) FROM src", DataType::Float32, ""),
        ("SELECT nullif(ff, dc) FROM src", DataType::Float32, "0.5"),
        (
            "SELECT count(*) FROM src a JOIN src b ON a.dc = b.dd",
            DataType::Int64,
            "0",
        ),
    ] {
        let (found, values) = answer(&ctx, sql).await;
        assert_eq!(found, data_type, "{sql}");
        assert_eq!(values, vec![expected.to_string()], "{sql}");
    }
}

#[tokio::test]
async fn a_user_semi_join_keeps_the_left_type() {
    let ctx = setup().await;
    let (data_type, values) = answer(
        &ctx,
        "SELECT a.dc FROM src a LEFT SEMI JOIN src b ON a.dc = b.dd",
    )
    .await;
    assert_eq!(data_type, DataType::Decimal128(10, 2));
    assert!(values.is_empty());
}

#[tokio::test]
async fn a_plan_without_a_decimal_and_float_pair_is_returned_unchanged() {
    let ctx = setup().await;
    for sql in [
        "SELECT dc + 1, dd * 2, dc = dbig, dd = 1.5 FROM src",
        "SELECT dc AS c FROM src UNION ALL SELECT dbig FROM src",
        "SELECT dd AS c FROM src UNION ALL SELECT ff FROM src",
    ] {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let shown = plan.display_indent().to_string();
        let after = SparkDecimalFloatWiden
            .analyze(plan, ctx.state().config_options())
            .unwrap();
        assert_eq!(after.display_indent().to_string(), shown, "{sql}");
    }
}

#[test]
fn the_rule_sits_before_type_coercion_and_refuses_without_it() {
    let rules = insert_decimal_float_widen_before_coercion(Analyzer::new().rules).unwrap();
    let names: Vec<&str> = rules.iter().map(|rule| rule.name()).collect();
    let widen = names
        .iter()
        .position(|name| *name == "spark_decimal_float_widen")
        .unwrap();
    assert_eq!(names.get(widen + 1), Some(&"type_coercion"));
    let without: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = Analyzer::new()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != "type_coercion")
        .collect();
    assert!(insert_decimal_float_widen_before_coercion(without).is_err());
}

#[tokio::test]
async fn a_union_of_three_or_more_branches_with_the_decimal_first_is_double() {
    let ctx = setup().await;
    for (sql, expected) in [
        (
            "SELECT dc AS c FROM src UNION ALL SELECT dd FROM src UNION ALL SELECT dd FROM src",
            vec!["0.0", "1e-20", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION ALL SELECT dd FROM src UNION ALL SELECT dd FROM src UNION ALL SELECT dc FROM src",
            vec!["0.0", "0.0", "1e-20", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION ALL (SELECT dd FROM src UNION ALL SELECT dc FROM src)",
            vec!["0.0", "0.0", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION ALL SELECT dd FROM src UNION (SELECT dd FROM src UNION ALL SELECT dc FROM src)",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION SELECT dd FROM src UNION SELECT dd FROM src",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION ALL SELECT dd FROM src UNION ALL SELECT dd FROM src UNION ALL SELECT dd FROM src UNION ALL SELECT dc FROM src",
            vec!["0.0", "0.0", "1e-20", "1e-20", "1e-20"],
        ),
    ] {
        let (data_type, values) = answer(&ctx, sql).await;
        assert_eq!(data_type, DataType::Float64, "{sql}");
        assert_eq!(values, expected, "{sql}");
    }
    let (_, counted) = answer(
        &ctx,
        "SELECT count(*) FROM (SELECT dc AS c FROM src UNION ALL SELECT dd FROM src UNION ALL SELECT dd FROM src) WHERE c = CAST(1e-20 AS DOUBLE)",
    )
    .await;
    assert_eq!(counted, vec!["2"]);
}

mod in_ranges {
    use std::sync::Arc;

    use datafusion::arrow::array::{Decimal128Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;

    use super::super::{
        DECIMAL_IN_RANGE_LIMIT, DecimalInForm, DecimalInRanges, decimal_in_form,
        decimal_range_of_double,
    };

    const POOL: [f64; 18] = [
        0.5,
        1.25,
        -0.0,
        4.5,
        0.505,
        f64::NAN,
        -0.5,
        0.1,
        f64::INFINITY,
        1e-18,
        2.75,
        1e30,
        99_999_999.99,
        0.0,
        f64::NEG_INFINITY,
        0.01,
        1e20,
        0.1 + 0.2,
    ];
    const SCALES: [(u8, i8); 4] = [(10, 2), (20, 6), (38, 18), (38, 10)];
    const LENGTHS: [usize; 11] = [1, 2, 3, 4, 5, 8, 16, 31, 32, 33, 64];

    fn constants(length: usize, precision: u8, start: usize) -> Vec<f64> {
        let wide = |index: usize| -> f64 {
            let base = 1.0 + f64::from(u32::try_from(index).unwrap_or(0)) * 0.37;
            if precision > 15 {
                base
            } else {
                POOL[index % POOL.len()]
            }
        };
        (0..length)
            .map(|index| {
                if index % 3 == 0 {
                    POOL[(start + index) % POOL.len()]
                } else {
                    wide(index + start)
                }
            })
            .collect()
    }

    fn column(precision: u8, scale: i8, items: &[f64]) -> Vec<Option<i128>> {
        let top = 10i128.pow(u32::from(precision)) - 1;
        let mut values = vec![None, Some(-top), Some(top), Some(0), Some(1), Some(-1)];
        for &item in items {
            if let Some((lower, upper)) = decimal_range_of_double(item, precision, scale) {
                for value in [lower - 1, lower, upper - 1, upper] {
                    if value.abs() <= top {
                        values.push(Some(value));
                    }
                }
            }
        }
        values
    }

    fn context(precision: u8, scale: i8, values: &[Option<i128>], rule: bool) -> SessionContext {
        let ctx = SessionContext::new();
        repark_functions::bool_decimal::install_bool_decimal_cast(&ctx, true);
        if rule {
            ctx.add_optimizer_rule(Arc::new(DecimalInRanges));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("d", DataType::Decimal128(precision, scale), true),
        ]));
        let ids = Int64Array::from_iter_values(0..i64::try_from(values.len()).unwrap());
        let decimals = Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(decimals)]).unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    fn literal(item: f64) -> String {
        if item.is_nan() {
            "CAST('NaN' AS DOUBLE)".to_string()
        } else if item.is_infinite() {
            format!(
                "CAST('{}Infinity' AS DOUBLE)",
                if item < 0.0 { "-" } else { "" }
            )
        } else {
            format!("CAST('{item:e}' AS DOUBLE)")
        }
    }

    fn oracle(
        values: &[Option<i128>],
        scale: i8,
        items: &[f64],
        null_item: bool,
        negated: bool,
    ) -> Vec<String> {
        values
            .iter()
            .map(|value| {
                let Some(unscaled) = value else {
                    return String::new();
                };
                let double = repark_common::java_double::java_decimal_to_double(*unscaled, scale);
                let member = items.contains(&double);
                let answer = match (member, null_item) {
                    (true, _) => Some(!negated),
                    (false, true) => None,
                    (false, false) => Some(negated),
                };
                answer.map_or_else(String::new, |answer| answer.to_string())
            })
            .collect()
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> (Vec<String>, String) {
        let frame = ctx.sql(sql).await.unwrap();
        let optimized = frame.clone().into_optimized_plan().unwrap();
        let mut out = Vec::new();
        for batch in frame.collect().await.unwrap() {
            for row in 0..batch.num_rows() {
                out.push(array_value_to_string(batch.column(1), row).unwrap());
            }
        }
        (out, format!("{}", optimized.display_indent()))
    }

    #[test]
    fn precision_fifteen_and_below_always_takes_the_member_form() {
        for precision in 1..=15u8 {
            for scale in 0..=i8::try_from(precision).unwrap() {
                for length in LENGTHS {
                    let items = constants(length, precision, usize::from(precision));
                    assert!(
                        matches!(
                            decimal_in_form(&items, precision, scale, DECIMAL_IN_RANGE_LIMIT),
                            DecimalInForm::Members(_)
                        ),
                        "({precision},{scale}) {items:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn the_range_limit_is_thirty_two() {
        let distinct = |count: u32, base: f64| -> Vec<f64> {
            (0..count)
                .map(|index| base + f64::from(index) * 0.37)
                .collect()
        };
        for (precision, scale, base) in [(38u8, 18i8, 1.0), (38, 10, 1e7), (20, 10, 1e7)] {
            assert!(matches!(
                decimal_in_form(&distinct(32, base), precision, scale, DECIMAL_IN_RANGE_LIMIT),
                DecimalInForm::Ranges { ranges, .. } if ranges.len() == 32
            ));
            assert_eq!(
                decimal_in_form(
                    &distinct(33, base),
                    precision,
                    scale,
                    DECIMAL_IN_RANGE_LIMIT
                ),
                DecimalInForm::PerRow
            );
        }
    }

    #[test]
    fn zero_signs_nan_and_infinity_follow_spark() {
        assert_eq!(decimal_range_of_double(-0.0, 10, 2), Some((0, 1)));
        assert_eq!(decimal_range_of_double(0.0, 10, 2), Some((0, 1)));
        assert_eq!(decimal_range_of_double(f64::NAN, 10, 2), None);
        assert_eq!(decimal_range_of_double(f64::INFINITY, 10, 2), None);
        assert_eq!(decimal_range_of_double(0.505, 10, 2), None);
        assert_eq!(decimal_range_of_double(1e30, 10, 2), None);
        assert_eq!(
            decimal_range_of_double(99_999_999.99, 10, 2),
            Some((9_999_999_999, 10_000_000_000))
        );
        assert_eq!(
            decimal_in_form(&[f64::NAN, f64::INFINITY], 10, 2, DECIMAL_IN_RANGE_LIMIT),
            DecimalInForm::Members(Vec::new())
        );
    }

    #[tokio::test]
    async fn the_rewritten_in_answers_as_the_per_row_form_row_for_row() {
        for (precision, scale) in SCALES {
            for length in LENGTHS {
                for start in [0usize, 7] {
                    let mut items = constants(length, precision, start);
                    if start == 7 {
                        items.push(f64::NAN);
                    }
                    let values = column(precision, scale, &items);
                    let mut list: Vec<String> = items.iter().map(|&item| literal(item)).collect();
                    if start == 7 {
                        list.push("CAST(NULL AS DOUBLE)".to_string());
                    }
                    let form = decimal_in_form(&items, precision, scale, DECIMAL_IN_RANGE_LIMIT);
                    for negation in ["", "NOT "] {
                        let sql = format!(
                            "SELECT id, CAST(d AS DOUBLE) {negation}IN ({}) FROM t ORDER BY id",
                            list.join(", ")
                        );
                        let (per_row, _) =
                            rows(&context(precision, scale, &values, false), &sql).await;
                        let (ranged, plan) =
                            rows(&context(precision, scale, &values, true), &sql).await;
                        let expected =
                            oracle(&values, scale, &items, start == 7, !negation.is_empty());
                        let negative_zero = items
                            .iter()
                            .any(|item| *item == 0.0 && item.is_sign_negative());
                        if length > 3 || !negative_zero {
                            assert_eq!(ranged, expected, "({precision},{scale}) {sql}");
                        }
                        if negative_zero {
                            assert_eq!(ranged.len(), per_row.len(), "{sql}");
                        } else {
                            assert_eq!(per_row, expected, "({precision},{scale}) {sql}");
                        }
                        let kept = plan.contains("__repark_decimal_to_double__");
                        if length <= 3 {
                            continue;
                        }
                        match &form {
                            DecimalInForm::PerRow => assert!(kept, "{plan}"),
                            DecimalInForm::Members(_) => {
                                assert!(!kept && !plan.contains(">="), "{plan}");
                            }
                            DecimalInForm::Ranges { ranges, .. } => {
                                assert!(!kept && plan.contains(">="), "{plan}");
                                assert!(ranges.len() <= DECIMAL_IN_RANGE_LIMIT);
                            }
                        }
                        if precision <= 15 {
                            assert!(matches!(form, DecimalInForm::Members(_)), "{plan}");
                        }
                    }
                }
            }
        }
    }
}

fn decimal_ctx(legacy: bool) -> SessionContext {
    let rules = insert_decimal_float_widen_before_coercion(Analyzer::new().rules).unwrap();
    let mut config = SessionConfig::new();
    repark_functions::decimal_legacy::write_decimal_truncate_legacy(config.options_mut(), legacy);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    SessionContext::new_with_state(state)
}

async fn decimal_setup(legacy: bool) -> SessionContext {
    let ctx = decimal_ctx(legacy);
    ctx.sql(
        "CREATE VIEW p AS SELECT * FROM (VALUES \
         (1, CAST('0.49' AS DECIMAL(10,2)), CAST('0.4' AS DECIMAL(38,1)), CAST('0' AS DECIMAL(38,0)), \
          CAST('1.000000000000000000000000000049' AS DECIMAL(38,30))), \
         (2, CAST('1.45' AS DECIMAL(10,2)), CAST('1.4' AS DECIMAL(38,1)), CAST('1' AS DECIMAL(38,0)), \
          CAST('2.5' AS DECIMAL(38,30)))) t(id, c, d, a, w)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    ctx
}

#[tokio::test]
async fn nested_unions_round_once_per_binary_node_bottom_up() {
    let ctx = decimal_setup(false).await;
    let (data_type, values) = answer(
        &ctx,
        "SELECT c AS v FROM p UNION ALL SELECT d FROM p UNION ALL SELECT a FROM p",
    )
    .await;
    assert_eq!(data_type, DataType::Decimal128(38, 0));
    assert_eq!(values, vec!["0", "0", "1", "1", "1", "2"]);
    let (_, values) = answer(
        &ctx,
        "SELECT a AS v FROM p UNION ALL SELECT c FROM p UNION ALL SELECT d FROM p",
    )
    .await;
    assert_eq!(values, vec!["0", "0", "0", "1", "1", "1"]);
}

async fn walked(ctx: &SessionContext, sql: &str) -> String {
    let mut found = String::new();
    for batch in ctx
        .sql(&format!("EXPLAIN VERBOSE {sql}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
    {
        for row in 0..batch.num_rows() {
            if array_value_to_string(batch.column(0), row).unwrap()
                == "logical_plan after spark_decimal_float_widen"
            {
                found = array_value_to_string(batch.column(1), row).unwrap();
            }
        }
    }
    found
}

#[tokio::test]
async fn a_comparison_keeps_a_literal_digits_and_an_in_list_takes_the_integer_form() {
    let ctx = decimal_setup(false).await;
    let equal = walked(&ctx, "SELECT id FROM p WHERE w = 1").await;
    assert!(!equal.contains("Decimal128(38, 28)"), "{equal}");
    let listed = walked(
        &ctx,
        "SELECT id FROM p WHERE w IN (CAST(1 AS INT), CAST(7 AS INT))",
    )
    .await;
    assert!(
        listed.contains("CAST(p.w AS Decimal128(38, 28))"),
        "{listed}"
    );
    let (data_type, _) = answer(&ctx, "SELECT coalesce(a, c) FROM p").await;
    assert_eq!(data_type, DataType::Decimal128(38, 0));
}

#[tokio::test]
async fn the_legacy_flag_leaves_datafusion_s_decimal_type() {
    let ctx = decimal_setup(true).await;
    let (data_type, values) = answer(&ctx, "SELECT a AS v FROM p UNION ALL SELECT c FROM p").await;
    assert_eq!(data_type, DataType::Decimal128(38, 2));
    assert_eq!(values, vec!["0.00", "0.49", "1.00", "1.45"]);
}
