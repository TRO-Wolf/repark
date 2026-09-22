use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Between, BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator,
    Union, Values,
};
use datafusion::optimizer::AnalyzerRule;
use repark_functions::spark_result_types::{
    needs_count_star_expansion, transform_keeping_count_star,
};

pub const SPARK_MAX_DECIMAL_PRECISION: u8 = 38;

#[derive(Debug, Default)]
pub struct SparkIntegralLiteral;

impl AnalyzerRule for SparkIntegralLiteral {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if !plan_may_narrow(&plan)? {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_integral_literal"
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "The error contract is documented in map.md under the owner comment ban."
)]
pub fn insert_literal_rule_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(DataFusionError::Plan(
            "spark integral literals require the default type_coercion analyzer rule".to_string(),
        ));
    };
    rules.insert(position, Arc::new(SparkIntegralLiteral));
    Ok(rules)
}

#[must_use]
pub fn spark_door_post_coercion_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    repark_functions::analyzer_rules()
        .into_iter()
        .filter(|rule| rule.name() != "spark_integer_literal")
        .collect()
}

fn plan_may_narrow(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|leaf| {
                if is_narrowable_literal(leaf) || needs_count_star_expansion(leaf) {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            if found {
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(found)
}

fn node_has_higher_order(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        expr.apply(|leaf| {
            if matches!(leaf, Expr::HigherOrderFunction(_)) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn is_narrowable_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            ScalarValue::Int64(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Decimal128(_, _, _)
                | ScalarValue::Decimal256(_, _, _),
            _
        )
    )
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Values(values) = plan {
        return rewrite_values(values);
    }
    if matches!(plan, LogicalPlan::Limit(_)) {
        return Ok(Transformed::no(plan));
    }
    if let LogicalPlan::Union(union) = plan {
        let stale = union.inputs.iter().any(|input| {
            let merged = union.schema.fields();
            let mine = input.schema().fields();
            mine.len() != merged.len()
                || mine
                    .iter()
                    .zip(merged.iter())
                    .any(|(mine, merged)| mine.data_type() != merged.data_type())
        });
        if !stale {
            return Ok(Transformed::no(LogicalPlan::Union(union)));
        }
        let rebuilt = Union::try_new_with_loose_types(union.inputs)?;
        return Ok(Transformed::yes(LogicalPlan::Union(rebuilt)));
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let narrowed = transform_keeping_count_star(expr, &spark_integral_literal)?;
        let widened = narrowed
            .data
            .transform_down(|node| Ok(widen_decimal_against_float(node, &schema)))?;
        let combined = Transformed::new(
            widened.data,
            narrowed.transformed || widened.transformed,
            TreeNodeRecursion::Continue,
        );
        Ok(combined.update_data(|node| saved_name.restore(node)))
    })?;
    let narrowed_flag = transformed.transformed;
    let narrowed = transformed.map_data(LogicalPlan::recompute_schema)?.data;
    if narrowed_flag {
        let resolved = narrowed.resolve_lambda_variables()?;
        return Ok(Transformed::new(
            resolved.data,
            true,
            TreeNodeRecursion::Continue,
        ));
    }
    if !node_has_higher_order(&narrowed)? {
        return Ok(Transformed::no(narrowed));
    }
    let resolved = narrowed.resolve_lambda_variables()?;
    Ok(Transformed::new(
        resolved.data,
        resolved.transformed,
        TreeNodeRecursion::Continue,
    ))
}

fn rewrite_values(values: Values) -> Result<Transformed<LogicalPlan>> {
    if !values_may_narrow(&values)? {
        return Ok(Transformed::no(LogicalPlan::Values(values)));
    }
    let mut changed = false;
    let mut rows = Vec::with_capacity(values.values.len());
    for row in &values.values {
        let mut narrowed_row = Vec::with_capacity(row.len());
        for expr in row {
            let narrowed = expr.clone().transform_up(spark_integral_literal)?;
            changed |= narrowed.transformed;
            narrowed_row.push(narrowed.data);
        }
        rows.push(narrowed_row);
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Values(values)));
    }
    let rebuilt = LogicalPlanBuilder::values(rows)?.build()?;
    Ok(Transformed::yes(rebuilt.resolve_lambda_variables().data()?))
}

fn values_may_narrow(values: &Values) -> Result<bool> {
    let mut found = false;
    for row in &values.values {
        for expr in row {
            expr.apply(|leaf| {
                if is_narrowable_literal(leaf) {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            if found {
                return Ok(true);
            }
        }
    }
    Ok(found)
}

pub(crate) fn spark_integral_literal(expr: Expr) -> Result<Transformed<Expr>> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), meta) => {
            if let Ok(narrow) = i32::try_from(value) {
                Ok(Transformed::yes(Expr::Literal(
                    ScalarValue::Int32(Some(narrow)),
                    meta,
                )))
            } else {
                Ok(Transformed::no(Expr::Literal(
                    ScalarValue::Int64(Some(value)),
                    meta,
                )))
            }
        }
        Expr::Literal(ScalarValue::Int64(None), meta) => Ok(Transformed::yes(Expr::Literal(
            ScalarValue::Int32(None),
            meta,
        ))),
        Expr::Literal(ScalarValue::UInt64(Some(value)), meta) => {
            let digits = decimal_digits(value);
            Ok(Transformed::yes(Expr::Literal(
                ScalarValue::Decimal128(Some(i128::from(value)), digits, 0),
                meta,
            )))
        }
        Expr::Literal(ScalarValue::UInt64(None), meta) => Ok(Transformed::yes(Expr::Literal(
            ScalarValue::Decimal128(None, 20, 0),
            meta,
        ))),
        Expr::Literal(ScalarValue::Decimal128(value, precision, scale), meta) => {
            check_decimal_precision(precision)?;
            Ok(Transformed::no(Expr::Literal(
                ScalarValue::Decimal128(value, precision, scale),
                meta,
            )))
        }
        Expr::Literal(ScalarValue::Decimal256(value, precision, scale), meta) => {
            check_decimal_precision(precision)?;
            Ok(Transformed::no(Expr::Literal(
                ScalarValue::Decimal256(value, precision, scale),
                meta,
            )))
        }
        other => Ok(Transformed::no(other)),
    }
}

fn widen_decimal_against_float(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(binary) if is_float_comparison(binary.op) => {
            widen_comparison(binary, schema)
        }
        Expr::InList(in_list) => widen_in_list(in_list, schema),
        Expr::Between(between) => widen_between(between, schema),
        other => Transformed::no(other),
    }
}

fn is_float_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

fn is_float_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

fn is_decimal_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Decimal128(_, _, _) | ScalarValue::Decimal256(_, _, _), _) => {
            true
        }
        Expr::Negative(inner) => is_decimal_literal(inner),
        _ => false,
    }
}

fn widen_comparison(binary: BinaryExpr, schema: &DFSchema) -> Transformed<Expr> {
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let operator = binary.op;
    if is_decimal_literal(&binary.left) && is_float_type(&right_type) {
        let widened = Expr::Cast(Cast::new(binary.left, DataType::Float64));
        return Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(widened),
            operator,
            binary.right,
        )));
    }
    if is_decimal_literal(&binary.right) && is_float_type(&left_type) {
        let widened = Expr::Cast(Cast::new(binary.right, DataType::Float64));
        return Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            binary.left,
            operator,
            Box::new(widened),
        )));
    }
    Transformed::no(Expr::BinaryExpr(binary))
}

fn widen_in_list(in_list: InList, schema: &DFSchema) -> Transformed<Expr> {
    let InList {
        expr,
        list,
        negated,
    } = in_list;
    let Ok(expr_type) = expr.get_type(schema) else {
        return Transformed::no(Expr::InList(InList::new(expr, list, negated)));
    };
    if !is_float_type(&expr_type) {
        return Transformed::no(Expr::InList(InList::new(expr, list, negated)));
    }
    let mut changed = false;
    let mut widened_list = Vec::with_capacity(list.len());
    for item in list {
        if is_decimal_literal(&item) {
            widened_list.push(Expr::Cast(Cast::new(Box::new(item), DataType::Float64)));
            changed = true;
        } else {
            widened_list.push(item);
        }
    }
    let rebuilt = Expr::InList(InList::new(expr, widened_list, negated));
    if changed {
        Transformed::yes(rebuilt)
    } else {
        Transformed::no(rebuilt)
    }
}

fn widen_between(between: Between, schema: &DFSchema) -> Transformed<Expr> {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    let Ok(expr_type) = expr.get_type(schema) else {
        return Transformed::no(Expr::Between(Between::new(expr, negated, low, high)));
    };
    if !is_float_type(&expr_type) {
        return Transformed::no(Expr::Between(Between::new(expr, negated, low, high)));
    }
    let mut changed = false;
    let mut bound = |side: Box<Expr>| {
        if is_decimal_literal(&side) {
            changed = true;
            Box::new(Expr::Cast(Cast::new(side, DataType::Float64)))
        } else {
            side
        }
    };
    let rebuilt = Expr::Between(Between::new(expr, negated, bound(low), bound(high)));
    if changed {
        Transformed::yes(rebuilt)
    } else {
        Transformed::no(rebuilt)
    }
}

fn decimal_digits(mut value: u64) -> u8 {
    let mut digits: u8 = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn check_decimal_precision(precision: u8) -> Result<()> {
    if precision > SPARK_MAX_DECIMAL_PRECISION {
        return Err(DataFusionError::Plan(format!(
            "[DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION] Decimal precision {precision} exceeds max precision 38. SQLSTATE: 22003"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, i256};
    use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;

    fn literal(expr: Expr) -> (ScalarValue, bool) {
        let transformed = spark_integral_literal(expr).unwrap();
        let Expr::Literal(scalar, _) = transformed.data else {
            panic!("expected a literal");
        };
        (scalar, transformed.transformed)
    }

    #[test]
    fn int64_fitting_i32_narrows() {
        let (scalar, transformed) = literal(Expr::Literal(ScalarValue::Int64(Some(1)), None));
        assert!(transformed);
        assert_eq!(scalar, ScalarValue::Int32(Some(1)));
    }

    #[test]
    fn int64_needing_64_bits_stays() {
        for value in [i64::from(i32::MAX) + 1, i64::MAX, i64::MIN] {
            let (scalar, transformed) =
                literal(Expr::Literal(ScalarValue::Int64(Some(value)), None));
            assert!(!transformed);
            assert_eq!(scalar, ScalarValue::Int64(Some(value)));
        }
    }

    #[test]
    fn int64_null_narrows() {
        let (scalar, transformed) = literal(Expr::Literal(ScalarValue::Int64(None), None));
        assert!(transformed);
        assert_eq!(scalar, ScalarValue::Int32(None));
    }

    #[test]
    fn uint64_becomes_decimal19() {
        let (scalar, transformed) = literal(Expr::Literal(
            ScalarValue::UInt64(Some(9_223_372_036_854_775_808)),
            None,
        ));
        assert!(transformed);
        assert_eq!(
            scalar,
            ScalarValue::Decimal128(Some(9_223_372_036_854_775_808), 19, 0)
        );
    }

    #[test]
    fn uint64_max_becomes_decimal20() {
        let (scalar, transformed) =
            literal(Expr::Literal(ScalarValue::UInt64(Some(u64::MAX)), None));
        assert!(transformed);
        assert_eq!(
            scalar,
            ScalarValue::Decimal128(Some(i128::from(u64::MAX)), 20, 0)
        );
    }

    #[test]
    fn decimal128_over_precision_refuses() {
        let error =
            spark_integral_literal(Expr::Literal(ScalarValue::Decimal128(Some(1), 39, 0), None))
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("[DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION]")
        );
        assert!(error.to_string().contains("exceeds max precision 38"));
        assert!(error.to_string().contains("SQLSTATE: 22003"));
    }

    #[test]
    fn decimal256_over_precision_refuses() {
        let error = spark_integral_literal(Expr::Literal(
            ScalarValue::Decimal256(Some(i256::from(1)), 39, 0),
            None,
        ))
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("[DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION]")
        );
    }

    #[test]
    fn decimal128_max_precision_stays() {
        let (_, transformed) =
            literal(Expr::Literal(ScalarValue::Decimal128(Some(1), 38, 0), None));
        assert!(!transformed);
    }

    #[test]
    fn parenthesized_negative_int_min_stays_bigint() {
        let inner = Expr::Literal(ScalarValue::Int64(Some(i64::from(i32::MAX) + 1)), None);
        let transformed = spark_integral_literal(Expr::Negative(Box::new(inner.clone()))).unwrap();
        assert!(!transformed.transformed);
        assert_eq!(transformed.data, Expr::Negative(Box::new(inner)));
    }

    #[derive(Debug, Default)]
    struct StubRule(&'static str);

    impl AnalyzerRule for StubRule {
        fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
            Ok(plan)
        }

        fn name(&self) -> &str {
            self.0
        }
    }

    #[test]
    fn insert_order_places_rule_before_type_coercion() {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
            Arc::new(StubRule("first")),
            Arc::new(TypeCoercion::new()),
            Arc::new(StubRule("last")),
        ];
        let ordered = insert_literal_rule_before_coercion(rules).unwrap();
        let names: Vec<&str> = ordered.iter().map(|rule| rule.name()).collect();
        assert_eq!(
            names,
            vec!["first", "spark_integral_literal", "type_coercion", "last"]
        );
    }

    #[test]
    fn insert_order_keeps_hof_preparation_ahead_of_the_rule() {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
            Arc::new(StubRule("first")),
            Arc::new(StubRule("higher_order_preparation")),
            Arc::new(StubRule("spark_decimal_precision")),
            Arc::new(TypeCoercion::new()),
        ];
        let ordered = insert_literal_rule_before_coercion(rules).unwrap();
        let names: Vec<&str> = ordered.iter().map(|rule| rule.name()).collect();
        assert_eq!(
            names,
            vec![
                "first",
                "higher_order_preparation",
                "spark_decimal_precision",
                "spark_integral_literal",
                "type_coercion",
            ]
        );
    }

    #[test]
    fn insert_without_coercion_errors() {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![Arc::new(StubRule("only"))];
        assert!(insert_literal_rule_before_coercion(rules).is_err());
    }

    #[tokio::test]
    async fn udf_arguments_narrow_before_coercion() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        repark_functions::register_all(&ctx);
        let plan = ctx
            .state()
            .create_logical_plan("SELECT factorial(5) AS v")
            .await
            .unwrap();
        let config = ctx.state().config_options().clone();
        let analyzed = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        let observed = format!("{:?}", analyzed.expressions());
        assert!(
            observed.contains("Int32(5)"),
            "factorial argument must narrow before coercion, got {observed}"
        );
        let coerced = TypeCoercion::new().analyze(analyzed, &config).unwrap();
        assert_eq!(coerced.schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn narrowable_kinds_cover_the_rewrite_match() {
        assert!(is_narrowable_literal(&Expr::Literal(
            ScalarValue::Int64(Some(1)),
            None
        )));
        assert!(is_narrowable_literal(&Expr::Literal(
            ScalarValue::UInt64(Some(1)),
            None
        )));
        assert!(is_narrowable_literal(&Expr::Literal(
            ScalarValue::Decimal128(Some(1), 39, 0),
            None
        )));
        assert!(is_narrowable_literal(&Expr::Literal(
            ScalarValue::Decimal256(Some(i256::from(1)), 39, 0),
            None
        )));
        assert!(!is_narrowable_literal(&Expr::Literal(
            ScalarValue::Int32(Some(1)),
            None
        )));
        assert!(!is_narrowable_literal(&Expr::Negative(Box::new(
            Expr::Literal(ScalarValue::Int64(Some(1)), None)
        ))));
    }

    #[tokio::test]
    async fn rewrite_plan_reports_no_transform_without_narrowing() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT 'a' AS v")
            .await
            .unwrap();
        let rewritten = rewrite_plan(plan).unwrap();
        assert!(!rewritten.transformed);
        assert_eq!(
            rewritten.data.schema().field(0).data_type(),
            &DataType::Utf8
        );
    }

    #[tokio::test]
    async fn rewrite_plan_leaves_a_clean_union_unbuilt() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT 'a' AS v UNION ALL SELECT 'b'")
            .await
            .unwrap();
        let rewritten = rewrite_plan(plan).unwrap();
        assert!(!rewritten.transformed);
    }

    #[tokio::test]
    async fn values_fed_hof_body_narrows_without_late_rule() {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use repark_functions::lambda_rebind::HigherOrderPreparation;
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.dialect = datafusion::config::Dialect::Databricks;
        let ctx = SessionContext::new_with_config(config);
        repark_functions::register_all(&ctx);
        let plan = ctx
            .state()
            .create_logical_plan(
                "SELECT transform(a, x -> x + 1) AS r FROM (VALUES (array(1, 2, 3))) AS t(a)",
            )
            .await
            .unwrap();
        let config = ctx.state().config_options().clone();
        let plan = HigherOrderPreparation.analyze(plan, &config).unwrap();
        let plan = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        let plan = TypeCoercion::new().analyze(plan, &config).unwrap();
        let DataType::List(element) = plan.schema().field(0).data_type() else {
            panic!("transform answers a list");
        };
        assert_eq!(element.data_type(), &DataType::Int32);
    }

    #[tokio::test]
    async fn union_schema_follows_narrowed_branches() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT 1 AS q UNION ALL SELECT 2")
            .await
            .unwrap();
        assert_eq!(plan.schema().field(0).data_type(), &DataType::Int64);
        let config = ctx.state().config_options().clone();
        let analyzed = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        assert_eq!(analyzed.schema().field(0).data_type(), &DataType::Int32);
    }

    #[tokio::test]
    async fn count_star_keeps_the_int64_expansion_and_its_name() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT count(*), count(1), count(5) FROM (VALUES (1)) AS t(x)")
            .await
            .unwrap();
        let names: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let config = ctx.state().config_options().clone();
        let analyzed = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        let rendered = format!("{}", analyzed.display_indent());
        assert!(
            rendered.contains("aggr=[[count(Int64(1)), count(Int32(5)) AS count(Int64(5))]]"),
            "{rendered}"
        );
        let analyzed_names: Vec<String> = analyzed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(analyzed_names, names);
    }

    fn decimal_float_ctx() -> datafusion::prelude::SessionContext {
        use datafusion::arrow::array::{Decimal128Array, Float32Array, Float64Array, Int64Array};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use std::sync::Arc;
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        let ctx = SessionContext::new_with_config(config);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("d", DataType::Float64, true),
            Field::new("f", DataType::Float32, true),
            Field::new("dec", DataType::Decimal128(6, 2), true),
        ]));
        let decimals = Decimal128Array::from(vec![Some(1050), Some(-325)])
            .with_precision_and_scale(6, 2)
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(0.0)])),
                Arc::new(Float32Array::from(vec![Some(1.5), Some(0.1)])),
                Arc::new(decimals),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("v", Arc::new(table)).unwrap();
        ctx
    }

    async fn analyzed_render(ctx: &datafusion::prelude::SessionContext, sql: &str) -> String {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let config = ctx.state().config_options().clone();
        let analyzed = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        format!("{}", analyzed.display_indent())
    }

    #[tokio::test]
    async fn decimal_eq_double_casts_the_literal() {
        let ctx = decimal_float_ctx();
        let rendered = analyzed_render(&ctx, "SELECT id FROM v WHERE d = 0.0").await;
        assert!(
            rendered.contains("CAST(Decimal128(Some(0),1,1) AS Float64)"),
            "{rendered}"
        );
        assert!(!rendered.contains("CAST(v.d AS"), "{rendered}");
    }

    #[tokio::test]
    async fn decimal_eq_float_casts_the_literal_to_double() {
        let ctx = decimal_float_ctx();
        let rendered = analyzed_render(&ctx, "SELECT id FROM v WHERE f = 0.1").await;
        assert!(
            rendered.contains("CAST(Decimal128(Some(1),1,1) AS Float64)"),
            "{rendered}"
        );
        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM v WHERE f = 0.1")
            .await
            .unwrap();
        let config = ctx.state().config_options().clone();
        let analyzed = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        let coerced = TypeCoercion::new().analyze(analyzed, &config).unwrap();
        let rendered = format!("{}", coerced.display_indent());
        assert!(rendered.contains("CAST(v.f AS Float64)"), "{rendered}");
        assert!(!rendered.contains(" AS Decimal"), "{rendered}");
    }

    #[tokio::test]
    async fn decimal_comparison_forms_all_widen_the_literal() {
        let ctx = decimal_float_ctx();
        let cases: Vec<(&str, &str)> = vec![
            (
                "SELECT id FROM v WHERE 0.0 = d",
                "CAST(Decimal128(Some(0),1,1) AS Float64) = v.d",
            ),
            (
                "SELECT id FROM v WHERE d > 1.0",
                "v.d > CAST(Decimal128(Some(10),2,1) AS Float64)",
            ),
            (
                "SELECT id FROM v WHERE d <= 1.0",
                "v.d <= CAST(Decimal128(Some(10),2,1) AS Float64)",
            ),
            (
                "SELECT id FROM v WHERE d <> 0.0",
                "v.d != CAST(Decimal128(Some(0),1,1) AS Float64)",
            ),
            (
                "SELECT id FROM v WHERE d = -0.5",
                "v.d = CAST(Decimal128(Some(-5),1,1) AS Float64)",
            ),
            (
                "SELECT id FROM v WHERE d IN (0.0, 1.5)",
                "v.d IN ([CAST(Decimal128(Some(0),1,1) AS Float64), CAST(Decimal128(Some(15),2,1) AS Float64)])",
            ),
            (
                "SELECT id FROM v WHERE d NOT IN (0.0, 1.5)",
                "v.d NOT IN ([CAST(Decimal128(Some(0),1,1) AS Float64), CAST(Decimal128(Some(15),2,1) AS Float64)])",
            ),
            (
                "SELECT id FROM v WHERE d BETWEEN 0.0 AND 2.0",
                "v.d BETWEEN CAST(Decimal128(Some(0),1,1) AS Float64) AND CAST(Decimal128(Some(20),2,1) AS Float64)",
            ),
            (
                "SELECT id FROM v WHERE d NOT BETWEEN 0.0 AND 2.0",
                "v.d NOT BETWEEN CAST(Decimal128(Some(0),1,1) AS Float64) AND CAST(Decimal128(Some(20),2,1) AS Float64)",
            ),
            (
                "SELECT id FROM v WHERE f BETWEEN 0.0 AND 1.5",
                "v.f BETWEEN CAST(Decimal128(Some(0),1,1) AS Float64) AND CAST(Decimal128(Some(15),2,1) AS Float64)",
            ),
        ];
        for (sql, needle) in cases {
            let rendered = analyzed_render(&ctx, sql).await;
            assert!(rendered.contains(needle), "{sql}: {rendered}");
            assert!(!rendered.contains(" AS Decimal"), "{sql}: {rendered}");
        }
    }

    #[tokio::test]
    async fn decimal_against_decimal_or_integral_is_untouched() {
        let ctx = decimal_float_ctx();
        for sql in [
            "SELECT id FROM v WHERE dec = 0.0",
            "SELECT id FROM v WHERE dec = 0",
            "SELECT id FROM v WHERE dec > 10",
            "SELECT id FROM v WHERE dec IN (0.0, 1.00)",
            "SELECT id FROM v WHERE id = 5",
            "SELECT 0.0 AS v",
        ] {
            let rendered = analyzed_render(&ctx, sql).await;
            assert!(!rendered.contains("Float64"), "{sql}: {rendered}");
        }
    }

    #[tokio::test]
    async fn decimal_widening_is_idempotent() {
        let ctx = decimal_float_ctx();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM v WHERE d = 0.0 AND f > 1.0")
            .await
            .unwrap();
        let config = ctx.state().config_options().clone();
        let once = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        let rendered = format!("{}", once.display_indent());
        assert!(
            rendered.contains("v.d = CAST(Decimal128(Some(0),1,1) AS Float64)"),
            "{rendered}"
        );
        assert!(
            rendered.contains("v.f > CAST(Decimal128(Some(10),2,1) AS Float64)"),
            "{rendered}"
        );
        let twice = SparkIntegralLiteral.analyze(once.clone(), &config).unwrap();
        assert_eq!(rendered, format!("{}", twice.display_indent()));
    }

    #[tokio::test]
    async fn int32_count_of_one_widens_without_an_int64_literal() {
        use datafusion::functions_aggregate::count::count;
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let plan = ctx
            .read_empty()
            .unwrap()
            .aggregate(
                vec![],
                vec![count(Expr::Literal(ScalarValue::Int32(Some(1)), None))],
            )
            .unwrap()
            .into_unoptimized_plan();
        let config = ctx.state().config_options().clone();
        let analyzed = SparkIntegralLiteral.analyze(plan, &config).unwrap();
        let rendered = format!("{}", analyzed.display_indent());
        assert!(
            rendered.contains("count(Int64(1)) AS count(Int32(1))"),
            "{rendered}"
        );
        assert_eq!(analyzed.schema().field(0).name(), "count(Int32(1))");
    }
}
