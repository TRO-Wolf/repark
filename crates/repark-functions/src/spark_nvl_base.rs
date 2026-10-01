use std::ops::Not;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::functions::expr_fn::coalesce;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Case, Expr, GroupingSet, LogicalPlan, TypeSignature};
use datafusion::optimizer::AnalyzerRule;

use crate::spark_nvl_udf::{SparkNullIfZero, SparkNullif, SparkNvl, SparkNvl2, SparkZeroIfNull};

#[expect(
    clippy::missing_errors_doc,
    reason = "The error contract is documented in map.md under the owner comment ban."
)]
pub fn insert_base_route_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(DataFusionError::Plan(
            "spark nvl base route requires the default type_coercion analyzer rule".to_string(),
        ));
    };
    rules.insert(position, Arc::new(SparkNvlBaseRoute));
    Ok(rules)
}

#[derive(Debug, Default)]
pub struct SparkNvlBaseRoute;

impl AnalyzerRule for SparkNvlBaseRoute {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if crate::ansi::spark_ansi_enabled_from_options(config) {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_nvl_base_route"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let transformed = plan.map_expressions(|expr| {
        if let Expr::GroupingSet(set) = &expr {
            return rewrite_grouping_set(set);
        }
        expr.transform_up(rewrite_expr)
    })?;
    if !transformed.transformed {
        return Ok(transformed);
    }
    let fallback = transformed.data.clone();
    match transformed.map_data(LogicalPlan::recompute_schema) {
        Ok(recomputed) => Ok(recomputed),
        Err(_) => Ok(Transformed::new(
            fallback,
            true,
            TreeNodeRecursion::Continue,
        )),
    }
}

fn rewrite_grouping_set(set: &GroupingSet) -> Result<Transformed<Expr>> {
    let mut changed = false;
    let mut rewrite_inner = |inner: &Expr| -> Result<Expr> {
        let rewritten = inner.clone().transform_up(rewrite_expr)?;
        changed |= rewritten.transformed;
        Ok(rewritten.data)
    };
    let rebuilt = match set {
        GroupingSet::Rollup(inner) => {
            let mut lowered = Vec::with_capacity(inner.len());
            for expr in inner {
                lowered.push(rewrite_inner(expr)?);
            }
            GroupingSet::Rollup(lowered)
        }
        GroupingSet::Cube(inner) => {
            let mut lowered = Vec::with_capacity(inner.len());
            for expr in inner {
                lowered.push(rewrite_inner(expr)?);
            }
            GroupingSet::Cube(lowered)
        }
        GroupingSet::GroupingSets(sets) => {
            let mut lowered = Vec::with_capacity(sets.len());
            for inner in sets {
                let mut lowered_inner = Vec::with_capacity(inner.len());
                for expr in inner {
                    lowered_inner.push(rewrite_inner(expr)?);
                }
                lowered.push(lowered_inner);
            }
            GroupingSet::GroupingSets(lowered)
        }
    };
    Ok(Transformed::new(
        Expr::GroupingSet(rebuilt),
        changed,
        TreeNodeRecursion::Continue,
    ))
}

fn rewrite_expr(expr: Expr) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(function) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !is_custom_family_call(function) {
        return Ok(Transformed::no(expr));
    }
    let routed = match function.func.name() {
        "nvl" | "ifnull" => route_nvl(function),
        "nvl2" => route_nvl2(function),
        "nullif" => route_nullif(function),
        "zeroifnull" => route_zeroifnull(function),
        "nullifzero" => route_nullifzero(function),
        _ => return Ok(Transformed::no(expr)),
    }?;
    Ok(Transformed::yes(routed))
}

fn is_custom_family_call(function: &ScalarFunction) -> bool {
    matches!(
        function.func.name(),
        "nvl" | "ifnull" | "nvl2" | "nullif" | "zeroifnull" | "nullifzero"
    ) && matches!(
        function.func.signature().type_signature,
        TypeSignature::UserDefined
    )
}

fn core_nvl(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::nvl(),
        args,
    ))
}

fn core_nvl2(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::nvl2(),
        args,
    ))
}

fn core_nullif(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::nullif(),
        args,
    ))
}

fn zero_literal() -> Expr {
    Expr::Literal(ScalarValue::Int32(Some(0)), None)
}

fn route_nvl(function: &ScalarFunction) -> Result<Expr> {
    if function
        .func
        .inner()
        .downcast_ref::<SparkNvl>()
        .is_some_and(SparkNvl::is_facade_built)
    {
        validate_literals(&datafusion::functions::core::coalesce(), function)?;
        return Ok(coalesce(function.args.clone()));
    }
    validate_literals(&datafusion::functions::core::nvl(), function)?;
    Ok(core_nvl(function.args.clone()))
}

fn route_nvl2(function: &ScalarFunction) -> Result<Expr> {
    if let [test, first, second] = function.args.as_slice()
        && function
            .func
            .inner()
            .downcast_ref::<SparkNvl2>()
            .is_some_and(SparkNvl2::is_facade_built)
    {
        return Ok(Expr::Case(Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(test.clone().is_null().not()),
                Box::new(first.clone()),
            )],
            else_expr: Some(Box::new(second.clone())),
        }));
    }
    if let [test, first, second] = function.args.as_slice()
        && function
            .func
            .inner()
            .downcast_ref::<SparkNvl2>()
            .is_some_and(SparkNvl2::is_fexpr_built)
        && let (
            Expr::Literal(test_value, _),
            Expr::Literal(first_value, _),
            Expr::Literal(second_value, _),
        ) = (test, first, second)
        && matches!(test_value, ScalarValue::Int32(Some(_)))
        && matches!(first_value, ScalarValue::Utf8(Some(_)))
        && matches!(second_value, ScalarValue::Binary(Some(_)))
    {
        return Err(fexpr_nvl2_string_binary_refusal(test_value));
    }
    validate_literals(&datafusion::functions::core::nvl2(), function)?;
    Ok(core_nvl2(function.args.clone()))
}

fn fexpr_nvl2_string_binary_refusal(test: &ScalarValue) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION] Cannot resolve \"CAST({test:?} AS BINARY)\" due to data type mismatch: cannot cast \"INT\" to \"BINARY\" with ANSI mode on.\nIf you have to cast \"INT\" to \"BINARY\", you can set \"spark.sql.ansi.enabled\" as 'false'. SQLSTATE: 42K09"
    ))
}

fn route_nullif(function: &ScalarFunction) -> Result<Expr> {
    if let [first, second] = function.args.as_slice()
        && function
            .func
            .inner()
            .downcast_ref::<SparkNullif>()
            .is_some_and(SparkNullif::is_fexpr_built)
        && let Some((off, _)) = crate::spark_nvl_fexpr::utc_fold_nullif_literals(first, second)
    {
        return Ok(off);
    }
    validate_literals(&datafusion::functions::core::nullif(), function)?;
    Ok(core_nullif(function.args.clone()))
}

fn validate_literals(
    core: &datafusion::logical_expr::ScalarUDF,
    function: &ScalarFunction,
) -> Result<()> {
    let Some(types) = crate::spark_nvl_core::literal_types(&function.args) else {
        return Ok(());
    };
    crate::spark_nvl_core::validate_core_call(core, &types)
}

fn unresolved_routine(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[UNRESOLVED_ROUTINE] Cannot resolve routine `{name}` on search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883; line 1 pos 0"
    ))
}

fn route_zeroifnull(function: &ScalarFunction) -> Result<Expr> {
    if let [arg] = function.args.as_slice()
        && function
            .func
            .inner()
            .downcast_ref::<SparkZeroIfNull>()
            .is_some_and(SparkZeroIfNull::is_facade_built)
    {
        return Ok(coalesce(vec![arg.clone(), zero_literal()]));
    }
    Err(unresolved_routine(function.func.name()))
}

fn route_nullifzero(function: &ScalarFunction) -> Result<Expr> {
    if let [arg] = function.args.as_slice()
        && function
            .func
            .inner()
            .downcast_ref::<SparkNullIfZero>()
            .is_some_and(SparkNullIfZero::is_facade_built)
    {
        return Ok(core_nullif(vec![arg.clone(), zero_literal()]));
    }
    Err(unresolved_routine(function.func.name()))
}

#[cfg(test)]
mod tests {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::lit;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::spark_nvl_udf::{nvl_family_expr, nvl_family_facade_expr};

    fn ctx_with_ansi(ansi_on: bool) -> SessionContext {
        let rules = insert_base_route_before_coercion(Analyzer::new().rules)
            .expect("default analyzer contains type_coercion");
        let config =
            crate::ansi::with_spark_ansi_config(datafusion::prelude::SessionConfig::new(), ansi_on);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        for udf in crate::spark_nvl_udf::functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        ctx
    }

    async fn collect_one(
        ctx: &SessionContext,
        sql: &str,
    ) -> datafusion::arrow::record_batch::RecordBatch {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"))
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("{sql} returned no batch"))
    }

    #[test]
    fn seats_before_type_coercion() {
        let rules = insert_base_route_before_coercion(Analyzer::new().rules)
            .expect("default analyzer contains type_coercion");
        let names: Vec<&str> = rules.iter().map(|rule| rule.name()).collect();
        let position = names
            .iter()
            .position(|name| *name == "spark_nvl_base_route")
            .expect("base route is installed");
        assert_eq!(names[position + 1], "type_coercion");
    }

    #[test]
    fn refuses_a_missing_type_coercion_rule() {
        let error = insert_base_route_before_coercion(Vec::new())
            .expect_err("missing insertion point must refuse")
            .to_string();
        assert!(error.contains("type_coercion"), "{error}");
    }

    #[tokio::test]
    async fn ansi_off_sql_nvl_answers_like_core() {
        use datafusion::arrow::array::Array;
        let ctx = ctx_with_ansi(false);
        let batch = collect_one(&ctx, "SELECT nvl('a', 5) AS v").await;
        assert_eq!(
            batch.schema().field(0).data_type(),
            &datafusion::arrow::datatypes::DataType::Utf8
        );
        assert_eq!(batch.num_rows(), 1);
        assert!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .is_some_and(|column| column.value(0) == "a"),
            "ansi-off nvl answers 'a'"
        );
    }

    #[tokio::test]
    async fn ansi_off_sql_zeroifnull_refuses_like_an_unknown_routine() {
        let ctx = ctx_with_ansi(false);
        let error = ctx
            .sql("SELECT zeroifnull('a') AS v")
            .await
            .expect("ansi-off zeroifnull binds")
            .collect()
            .await
            .expect_err("ansi-off zeroifnull must refuse")
            .to_string();
        assert!(
            error.contains(
                "[UNRESOLVED_ROUTINE] Cannot resolve routine `zeroifnull` on search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883; line 1 pos 0"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn ansi_on_leaves_custom_nvl_untouched() {
        let ctx = ctx_with_ansi(true);
        let error = ctx
            .sql("SELECT nvl('a', 5) AS v")
            .await
            .expect("custom nvl still binds")
            .collect()
            .await
            .expect_err("ansi casts still fail")
            .to_string();
        assert!(!error.contains("UNRESOLVED_ROUTINE"), "{error}");
    }

    #[test]
    fn facade_marker_routes_zeroifnull_to_coalesce() {
        let call = nvl_family_facade_expr("zeroifnull", &[lit("a")]).expect("facade call");
        let routed = call.transform_up(rewrite_expr).expect("route").data;
        let Expr::ScalarFunction(function) = routed else {
            panic!("facade zeroifnull must route to a call");
        };
        assert_eq!(function.func.name(), "coalesce");
        let call = nvl_family_expr("zeroifnull", &[lit("a")]).expect("registry call");
        let error = call
            .transform_up(rewrite_expr)
            .expect_err("registry zeroifnull must refuse")
            .to_string();
        assert!(
            error.contains("[UNRESOLVED_ROUTINE] Cannot resolve routine `zeroifnull`"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn ansi_off_nested_coal_nvl_answers_like_base() {
        use datafusion::arrow::array::Array;
        let ctx = ctx_with_ansi(false);
        let batch = collect_one(
            &ctx,
            "SELECT coalesce(nvl(TIMESTAMP'2024-01-02 03:04:05', 'x'), 'y') AS v",
        )
        .await;
        assert!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .is_some_and(|column| column.value(0) == "2024-01-02T03:04:05"),
            "nested coalesce answers the stamp text"
        );
    }

    #[test]
    fn fexpr_nullif_utc_pair_folds_to_null() {
        use datafusion::arrow::array::timezone::Tz;

        use crate::csv::default_timestamp_micros;
        let zone: Tz = "UTC".parse().expect("utc zone");
        let micros = default_timestamp_micros("2024-01-02 03:04:05", Some(zone)).expect("parse");
        let stamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(micros), Some(Arc::from("UTC"))),
            None,
        );
        let text = Expr::Literal(
            ScalarValue::Utf8(Some("2024-01-02 03:04:05".to_owned())),
            None,
        );
        let call =
            crate::expr_fn::call(crate::spark_nvl_udf::nullif_fexpr_udf(), vec![stamp, text]);
        let routed = call.transform_up(rewrite_expr).expect("route").data;
        let Expr::Literal(ScalarValue::TimestampMicrosecond(None, _), _) = routed else {
            panic!("utc-equal fexpr nullif must fold to null, got {routed:?}");
        };
    }

    #[test]
    fn validate_reports_base_bind_failure() {
        let call = nvl_family_expr("nullif", &[lit("a"), lit(true)]).expect("registry call");
        let error = call
            .transform_up(rewrite_expr)
            .expect_err("mismatch must refuse")
            .to_string();
        assert_eq!(
            error,
            "Error during planning: For function 'nullif' Utf8 and Boolean is not comparable"
        );
    }

    #[tokio::test]
    async fn unresolvable_routed_plan_keeps_base_bind_text() {
        let ctx = ctx_with_ansi(false);
        let error = ctx
            .sql("SELECT nullif('a', true) AS v")
            .await
            .expect("ansi-off nullif binds")
            .collect()
            .await
            .expect_err("mismatched nullif must refuse")
            .to_string();
        assert!(error.contains("not comparable"), "{error}");
    }

    #[test]
    fn fexpr_nvl2_string_binary_pair_refuses_like_base() {
        let binary = Expr::Literal(ScalarValue::Binary(Some(vec![1])), None);
        let call = crate::expr_fn::call(
            crate::spark_nvl_fexpr::nvl2_fexpr_udf(),
            vec![lit(1), lit("a"), binary],
        );
        let error = call
            .transform_up(rewrite_expr)
            .expect_err("binary pair must refuse")
            .to_string();
        assert!(error.contains("CAST_WITH_CONF_SUGGESTION"), "{error}");
        assert!(error.contains("CAST(Int32(1) AS BINARY)"), "{error}");
    }

    #[test]
    fn nvl2_binary_pair_without_fexpr_marker_routes_to_core() {
        let binary = Expr::Literal(ScalarValue::Binary(Some(vec![1])), None);
        let call = nvl_family_expr("nvl2", &[lit(1), lit("a"), binary]).expect("registry call");
        let routed = call.transform_up(rewrite_expr).expect("route").data;
        let Expr::ScalarFunction(function) = routed else {
            panic!("registry nvl2 must route to a call");
        };
        assert_eq!(function.func.name(), "nvl2");
        let null_binary = Expr::Literal(ScalarValue::Binary(Some(vec![1])), None);
        let null_test = crate::expr_fn::call(
            crate::spark_nvl_fexpr::nvl2_fexpr_udf(),
            vec![lit(ScalarValue::Null), lit("a"), null_binary],
        );
        assert!(null_test.transform_up(rewrite_expr).is_ok());
    }

    #[test]
    fn facade_marker_routes_nvl2_to_case() {
        let call =
            nvl_family_facade_expr("nvl2", &[lit(1), lit("a"), lit(5)]).expect("facade call");
        let routed = call.transform_up(rewrite_expr).expect("route").data;
        assert!(
            matches!(routed, Expr::Case(_)),
            "facade nvl2 must route to CASE, got {routed:?}"
        );
        let call = nvl_family_expr("nvl2", &[lit(1), lit("a"), lit(5)]).expect("registry call");
        let routed = call.transform_up(rewrite_expr).expect("route").data;
        let Expr::ScalarFunction(function) = routed else {
            panic!("registry nvl2 must route to a call");
        };
        assert_eq!(function.func.name(), "nvl2");
    }
}
