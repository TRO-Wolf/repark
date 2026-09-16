use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Union, Values};
use datafusion::optimizer::AnalyzerRule;

pub const SPARK_MAX_DECIMAL_PRECISION: u8 = 38;

#[derive(Debug, Default)]
pub struct SparkIntegralLiteral;

impl AnalyzerRule for SparkIntegralLiteral {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_integral_literal"
    }
}

pub(crate) fn insert_literal_rule_before_coercion(
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

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Values(values) = plan {
        return rewrite_values(values);
    }
    if matches!(plan, LogicalPlan::Limit(_)) {
        return Ok(Transformed::no(plan));
    }
    if let LogicalPlan::Union(union) = plan {
        let rebuilt = Union::try_new_with_loose_types(union.inputs)?;
        return Ok(Transformed::yes(LogicalPlan::Union(rebuilt)));
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(spark_integral_literal)?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    let narrowed_flag = transformed.transformed;
    let narrowed = transformed.map_data(LogicalPlan::recompute_schema)?.data;
    let resolved = narrowed.resolve_lambda_variables()?;
    Ok(Transformed::new(
        resolved.data,
        narrowed_flag || resolved.transformed,
        TreeNodeRecursion::Continue,
    ))
}

fn rewrite_values(values: Values) -> Result<Transformed<LogicalPlan>> {
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
        Expr::Negative(inner) => Ok(fold_negative_int_min(*inner)),
        other => Ok(Transformed::no(other)),
    }
}

fn fold_negative_int_min(inner: Expr) -> Transformed<Expr> {
    if let Expr::Literal(ScalarValue::Int64(Some(value)), meta) = &inner
        && *value == i64::from(i32::MAX) + 1
    {
        return Transformed::yes(Expr::Literal(
            ScalarValue::Int32(Some(i32::MIN)),
            meta.clone(),
        ));
    }
    Transformed::no(Expr::Negative(Box::new(inner)))
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
    fn negative_int_min_folds() {
        let transformed = spark_integral_literal(Expr::Negative(Box::new(Expr::Literal(
            ScalarValue::Int64(Some(i64::from(i32::MAX) + 1)),
            None,
        ))))
        .unwrap();
        assert!(transformed.transformed);
        assert_eq!(
            transformed.data,
            Expr::Literal(ScalarValue::Int32(Some(i32::MIN)), None)
        );
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
    fn insert_without_coercion_errors() {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![Arc::new(StubRule("only"))];
        assert!(insert_literal_rule_before_coercion(rules).is_err());
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
}
