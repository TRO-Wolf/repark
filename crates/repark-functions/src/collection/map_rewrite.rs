//! Rewrite DataFusion's `map(make_array(keys), make_array(values))` onto the Spark map kernel.
//!
//! DataFusion plans `map(k1, v1, k2, v2)` as `map(make_array(k1, k2), make_array(v1, v2))`. Its
//! kernel builds a map only when both lists are constants or both are per-row. A constant list
//! beside a per-row list panics or answers `map requires key and value lists to have the same
//! length`. A constant map stays on the DataFusion kernel; every other map takes the Spark
//! per-row kernel, which also raises Spark's `NULL_MAP_KEY` and `DUPLICATED_MAP_KEY`.

use std::collections::HashSet;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::create_map::sql_map_udf;

const CONSTANT_CONSTRUCTORS: [&str; 5] = ["array", "make_array", "map", "named_struct", "struct"];

/// Send every non-constant two-list `map` call to the Spark per-row map kernel.
#[derive(Debug, Default)]
pub struct SparkMapRewrite;

impl AnalyzerRule for SparkMapRewrite {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_map_rewrite"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(rewrite_expr(node)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr) -> Transformed<Expr> {
    let Some((keys, values)) = paired_lists(&expr) else {
        return Transformed::no(expr);
    };
    if is_constant_map(keys, values) {
        return Transformed::no(expr);
    }
    let pairs = keys
        .iter()
        .zip(values)
        .flat_map(|(key, value)| [key.clone(), value.clone()])
        .collect();
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        sql_map_udf(),
        pairs,
    )))
}

fn paired_lists(expr: &Expr) -> Option<(&[Expr], &[Expr])> {
    let Expr::ScalarFunction(map) = expr else {
        return None;
    };
    let [Expr::ScalarFunction(keys), Expr::ScalarFunction(values)] = map.args.as_slice() else {
        return None;
    };
    (map.func.name() == "map"
        && keys.func.name() == "make_array"
        && values.func.name() == "make_array"
        && !keys.args.is_empty()
        && keys.args.len() == values.args.len())
    .then_some((keys.args.as_slice(), values.args.as_slice()))
}

fn is_constant_map(keys: &[Expr], values: &[Expr]) -> bool {
    let mut spellings = HashSet::with_capacity(keys.len());
    keys.iter().all(|key| {
        is_constant(key) && !is_null_literal(key) && spellings.insert(key.schema_name().to_string())
    }) && values.iter().all(is_constant)
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if value.is_null())
}

fn is_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(..) => true,
        Expr::Cast(cast) => is_constant(&cast.expr),
        Expr::TryCast(cast) => is_constant(&cast.expr),
        Expr::Negative(inner) => is_constant(inner),
        Expr::ScalarFunction(function) => {
            CONSTANT_CONSTRUCTORS.contains(&function.func.name())
                && function.args.iter().all(is_constant)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::tree_node::TreeNode;
    use datafusion::functions_nested::map::map;
    use datafusion::logical_expr::{Expr, cast, col, lit};

    use super::{SparkMapRewrite, rewrite_expr};
    use crate::collection::SQL_MAP_NAME;

    fn rewritten(expr: Expr) -> Expr {
        expr.transform_up(|node| Ok(rewrite_expr(node)))
            .expect("rewrite")
            .data
    }

    fn kernel_arguments(expr: &Expr) -> Option<&[Expr]> {
        match expr {
            Expr::ScalarFunction(function) if function.func.name() == SQL_MAP_NAME => {
                Some(function.args.as_slice())
            }
            _ => None,
        }
    }

    #[test]
    fn a_column_value_beside_a_constant_key_takes_the_per_row_kernel() {
        let expr = rewritten(map(vec![lit("a")], vec![col("id")]));
        assert_eq!(kernel_arguments(&expr), Some(&[lit("a"), col("id")][..]));
    }

    #[test]
    fn pairs_keep_their_order_in_the_kernel_arguments() {
        let expr = rewritten(map(vec![col("k"), lit("b")], vec![col("v"), lit(2_i64)]));
        assert_eq!(
            kernel_arguments(&expr),
            Some(&[col("k"), col("v"), lit("b"), lit(2_i64)][..])
        );
    }

    #[test]
    fn a_constant_map_keeps_the_datafusion_constructor() {
        for constant in [
            map(vec![lit("a")], vec![lit(1_i64)]),
            map(vec![lit("a"), lit("b")], vec![lit(1_i64), lit(2_i64)]),
            map(vec![lit("a")], vec![cast(lit(1_i64), DataType::Int32)]),
            map(vec![lit("a")], vec![map(vec![lit("b")], vec![lit(1_i64)])]),
            map(
                vec![lit("a")],
                vec![Expr::Literal(
                    datafusion::common::ScalarValue::Int64(None),
                    None,
                )],
            ),
        ] {
            assert_eq!(rewritten(constant.clone()), constant);
        }
    }

    #[test]
    fn a_null_key_or_a_repeated_key_takes_the_per_row_kernel() {
        let null_key = Expr::Literal(datafusion::common::ScalarValue::Utf8(None), None);
        assert!(kernel_arguments(&rewritten(map(vec![null_key], vec![lit(1_i64)]))).is_some());
        assert!(
            kernel_arguments(&rewritten(map(
                vec![lit("a"), lit("a")],
                vec![lit(1_i64), lit(2_i64)]
            )))
            .is_some()
        );
    }

    #[test]
    fn a_nested_non_constant_map_rewrites_every_level() {
        let expr = rewritten(map(
            vec![lit("a")],
            vec![map(vec![lit("b")], vec![col("id")])],
        ));
        let outer = kernel_arguments(&expr).expect("outer kernel");
        assert!(kernel_arguments(&outer[1]).is_some(), "{outer:?}");
    }

    #[test]
    fn a_map_over_columns_or_another_function_is_untouched() {
        let over_arrays =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                datafusion::functions_nested::map::map_udf(),
                vec![col("keys"), col("values")],
            ));
        assert_eq!(rewritten(over_arrays.clone()), over_arrays);
        let other = col("id") + lit(1_i64);
        assert_eq!(rewritten(other.clone()), other);
    }

    #[test]
    fn the_rule_has_a_stable_name() {
        assert_eq!(
            datafusion::optimizer::AnalyzerRule::name(&SparkMapRewrite),
            "spark_map_rewrite"
        );
    }
}
