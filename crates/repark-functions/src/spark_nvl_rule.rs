use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::functions::expr_fn::{coalesce, get_field, nullif as df_nullif};
use datafusion::logical_expr::conditional_expressions::CaseBuilder;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    BinaryExpr, Cast, Expr, ExprSchemable, GroupingSet, LogicalPlan, Operator, TypeSignature,
    Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::spark_nvl::{
    CompareLeaf, CompareRefusal, binary_op_diff_types, coalesce_data_diff_types,
    compare_for_nullif, if_data_diff_types, invalid_ordering_type, widen_full,
};
use crate::spark_nvl_eager::{nullif_compare_expr, nvl_pick_expr};
use crate::spark_nvl_udf::{
    ifnull_expr, nullif_pick_udf, nvl_cast_expr, nvl_expr, nvl2_expr, zero_scalar, zeroifnull_expr,
};

#[must_use]
pub fn append_nvl_family_rule(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    rules.push(Arc::new(SparkNvlFamilyRewrite));
    rules
}

#[derive(Debug, Default)]
pub struct SparkNvlFamilyRewrite;

impl AnalyzerRule for SparkNvlFamilyRewrite {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_nvl_family_rewrite"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        if let Expr::GroupingSet(set) = &expr {
            return rewrite_grouping_set(set, &name_preserver, &schema);
        }
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_grouping_set(
    set: &GroupingSet,
    name_preserver: &NamePreserver,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let mut changed = false;
    let mut rewrite_inner = |inner: &Expr| -> Result<Expr> {
        let saved_name = name_preserver.save(inner);
        let rewritten = inner
            .clone()
            .transform_up(|node| rewrite_expr(node, schema))?;
        changed |= rewritten.transformed;
        Ok(rewritten.update_data(|node| saved_name.restore(node)).data)
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

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(function) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if (function.func.name() == "nvl" || function.func.name() == "ifnull")
        && function.args.len() == 2
    {
        let rewritten = rewrite_nvl(
            function.func.name(),
            function.args[0].clone(),
            function.args[1].clone(),
            schema,
        )?;
        return Ok(Transformed::yes(rewritten));
    }
    if function.func.name() == "nvl2" && function.args.len() == 3 {
        let rewritten = rewrite_nvl2(
            function.args[0].clone(),
            function.args[1].clone(),
            function.args[2].clone(),
            schema,
        )?;
        return Ok(Transformed::yes(rewritten));
    }
    if function.func.name() == "zeroifnull" && function.args.len() == 1 {
        let rewritten = rewrite_zeroifnull(function.args[0].clone(), schema)?;
        return Ok(Transformed::yes(rewritten));
    }
    if is_spark_nullif_call(function) && function.args.len() == 2 {
        let rewritten = rewrite_nullif(
            function.args[0].clone(),
            function.args[1].clone(),
            None,
            schema,
        )?;
        return Ok(Transformed::yes(rewritten));
    }
    if function.func.name() == "nullifzero" && function.args.len() == 1 {
        let zero = Expr::Literal(ScalarValue::Int32(Some(0)), None);
        let rewritten = rewrite_nullif(function.args[0].clone(), zero, Some("0"), schema)?;
        return Ok(Transformed::yes(rewritten));
    }
    Ok(Transformed::no(expr))
}

fn is_spark_nullif_call(function: &ScalarFunction) -> bool {
    function.func.name() == "nullif"
        && matches!(
            function.func.signature().type_signature,
            TypeSignature::UserDefined
        )
}

fn is_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(scalar, _) => scalar.is_null(),
        Expr::Cast(cast) => is_null_literal(&cast.expr),
        _ => false,
    }
}

fn is_non_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(scalar, _) => !scalar.is_null(),
        Expr::Cast(cast) => is_non_null_literal(&cast.expr),
        _ => false,
    }
}

fn case_when_present(test: Expr, first: Expr, second: Expr) -> Result<Expr> {
    CaseBuilder::new(
        None,
        vec![test.is_not_null()],
        vec![first],
        Some(Box::new(second)),
    )
    .end()
}

fn maybe_nvl_cast(expr: Expr, from_type: &DataType, widen: &DataType) -> Expr {
    if from_type == widen {
        expr
    } else {
        nvl_cast_expr(expr, widen)
    }
}

fn rewrite_nvl(spelling: &str, first: Expr, second: Expr, schema: &DFSchema) -> Result<Expr> {
    let (Ok(first_type), Ok(second_type)) = (first.get_type(schema), second.get_type(schema))
    else {
        return Ok(if spelling == "nvl" {
            nvl_expr(first, second)
        } else {
            ifnull_expr(first, second)
        });
    };
    let widen = widen_full(&first_type, &second_type)
        .ok_or_else(|| coalesce_data_diff_types(&first_type, &second_type))?;
    if is_null_literal(&first) {
        return Ok(maybe_nvl_cast(second, &second_type, &widen));
    }
    if is_non_null_literal(&first) {
        return Ok(maybe_nvl_cast(first, &first_type, &widen));
    }
    if is_truly_volatile(&first) {
        let widened_first = maybe_nvl_cast(first, &first_type, &widen);
        let widened_second = maybe_nvl_cast(second, &second_type, &widen);
        let first_nullable = logical_first_nullable(&widened_first, schema);
        return Ok(nvl_pick_expr(widened_first, widened_second, first_nullable));
    }
    Ok(coalesce(vec![
        maybe_nvl_cast(first, &first_type, &widen),
        maybe_nvl_cast(second, &second_type, &widen),
    ]))
}

fn logical_first_nullable(first: &Expr, schema: &DFSchema) -> bool {
    if first_is_try_shape(first) {
        return true;
    }
    first.nullable(schema).unwrap_or(true)
}

fn first_is_try_shape(first: &Expr) -> bool {
    let mut current = first;
    loop {
        match current {
            Expr::Alias(alias) => current = alias.expr.as_ref(),
            Expr::Cast(cast) => current = cast.expr.as_ref(),
            Expr::ScalarFunction(call)
                if call.func.name() == "__repark_nvl_cast" && call.args.len() == 1 =>
            {
                current = &call.args[0];
            }
            _ => break,
        }
    }
    matches!(current, Expr::TryCast(_))
        || matches!(current, Expr::ScalarFunction(call) if call.func.name().to_ascii_lowercase().starts_with("try_"))
}

fn rewrite_nvl2(test: Expr, first: Expr, second: Expr, schema: &DFSchema) -> Result<Expr> {
    let (Ok(first_type), Ok(second_type)) = (first.get_type(schema), second.get_type(schema))
    else {
        return Ok(nvl2_expr(test, first, second));
    };
    let widen = widen_full(&first_type, &second_type)
        .ok_or_else(|| if_data_diff_types(&first_type, &second_type))?;
    if is_null_literal(&test) {
        return Ok(maybe_nvl_cast(second, &second_type, &widen));
    }
    if is_non_null_literal(&test) {
        return Ok(maybe_nvl_cast(first, &first_type, &widen));
    }
    case_when_present(
        test,
        maybe_nvl_cast(first, &first_type, &widen),
        maybe_nvl_cast(second, &second_type, &widen),
    )
}

fn widened_zero(widen: &DataType) -> Result<Expr> {
    let zero = Expr::Literal(zero_scalar(widen)?, None);
    Ok(Expr::Cast(Cast::new(Box::new(zero), widen.clone())))
}

fn rewrite_zeroifnull(arg: Expr, schema: &DFSchema) -> Result<Expr> {
    let Ok(arg_type) = arg.get_type(schema) else {
        return Ok(zeroifnull_expr(arg));
    };
    let widen = widen_full(&arg_type, &DataType::Int32)
        .ok_or_else(|| coalesce_data_diff_types(&arg_type, &DataType::Int32))?;
    let zero = widened_zero(&widen)?;
    if is_null_literal(&arg) {
        return Ok(zero);
    }
    if is_non_null_literal(&arg) {
        return Ok(maybe_nvl_cast(arg, &arg_type, &widen));
    }
    if is_truly_volatile(&arg) {
        let widened = maybe_nvl_cast(arg, &arg_type, &widen);
        let first_nullable = logical_first_nullable(&widened, schema);
        return Ok(nvl_pick_expr(widened, zero, first_nullable));
    }
    Ok(coalesce(vec![maybe_nvl_cast(arg, &arg_type, &widen), zero]))
}

fn rewrite_nullif(
    first: Expr,
    second: Expr,
    second_sql: Option<&str>,
    schema: &DFSchema,
) -> Result<Expr> {
    let (Ok(first_type), Ok(second_type)) = (first.get_type(schema), second.get_type(schema))
    else {
        return Ok(blind_pick(first, second));
    };
    if first_type == DataType::Null && second_type == DataType::Null {
        return Ok(Expr::Literal(ScalarValue::Null, None));
    }
    let first_sql = "...";
    let from_nullifzero = second_sql.is_some();
    let second_sql = second_sql.unwrap_or("...");
    let leaves = match compare_for_nullif(&first_type, &second_type) {
        Ok(leaves) => leaves,
        Err(CompareRefusal::BinaryOp) => {
            return Err(binary_op_diff_types(
                first_sql,
                second_sql,
                &first_type,
                &second_type,
            ));
        }
        Err(CompareRefusal::Ordering(common)) => {
            return Err(invalid_ordering_type(first_sql, second_sql, &common));
        }
    };
    if leaves.is_empty() {
        return Ok(Expr::Literal(ScalarValue::Null, None));
    }
    if is_null_literal(&first) || is_null_literal(&second) {
        return Ok(first);
    }
    if let [leaf] = leaves.as_slice()
        && leaf.path_a.is_empty()
        && leaf.path_b.is_empty()
    {
        let string_cast = leaf_string_needs_cast(&leaf.type_a, &leaf.type_b, &leaf.common);
        if !from_nullifzero && leaf.common == first_type && !string_cast {
            return Ok(single_nullif(&first, &second, leaf));
        }
        if crate::spark_nvl_eager::kernel_covers_scalar(&leaf.common)
            && (from_nullifzero || leaf.common != first_type || string_cast)
        {
            return Ok(nullif_compare_expr(first, second, &leaf.common));
        }
    }
    let mut cond: Option<Expr> = None;
    for leaf in &leaves {
        let left = shape_operand(&first, &leaf.path_a, &leaf.type_a, &leaf.common);
        let right = shape_operand(&second, &leaf.path_b, &leaf.type_b, &leaf.common);
        let equals = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Eq,
            Box::new(right),
        ));
        cond = Some(match cond {
            None => equals,
            Some(built) => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(built),
                Operator::And,
                Box::new(equals),
            )),
        });
    }
    let Some(cond) = cond else {
        return Ok(Expr::Literal(ScalarValue::Null, None));
    };
    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
        nullif_pick_udf(),
        vec![cond, first],
    )))
}

fn leaf_string_needs_cast(type_a: &DataType, type_b: &DataType, common: &DataType) -> bool {
    (is_string_leaf(type_a) && type_a != common) || (is_string_leaf(type_b) && type_b != common)
}

fn is_string_leaf(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, values) => is_string_leaf(values),
        _ => false,
    }
}

fn single_nullif(first: &Expr, second: &Expr, leaf: &CompareLeaf) -> Expr {
    let left = shape_operand(first, &leaf.path_a, &leaf.type_a, &leaf.common);
    let right = shape_operand(second, &leaf.path_b, &leaf.type_b, &leaf.common);
    df_nullif(left, right)
}

fn is_truly_volatile(expr: &Expr) -> bool {
    expr.exists(|node| {
        Ok(matches!(node, Expr::ScalarFunction(call)
            if call.func.signature().volatility == Volatility::Volatile
                && !is_deterministic_vehicle(call.func.name())))
    })
    .unwrap_or(true)
}

fn is_deterministic_vehicle(name: &str) -> bool {
    use crate::timestamp_ns_cast::{TIMESTAMP_NS_CAST_NAME, TIMESTAMPTZ_NS_CAST_NAME};
    use crate::timestamp_ntz_cast::{TIMESTAMP_NTZ_CAST_NAME, TRY_TIMESTAMP_NTZ_CAST_NAME};
    matches!(
        name,
        TIMESTAMP_NTZ_CAST_NAME
            | TRY_TIMESTAMP_NTZ_CAST_NAME
            | TIMESTAMP_NS_CAST_NAME
            | TIMESTAMPTZ_NS_CAST_NAME
            | "to_timestamp_ltz"
            | "to_timestamp_ntz"
            | "try_to_timestamp"
            | "__repark_timestamp_to_string__"
            | "__repark_timestamp_to_date__"
            | "to_date"
            | "date"
            | "from_unixtime"
            | "date_format"
            | "to_char"
            | "to_varchar"
    )
}

fn blind_pick(first: Expr, second: Expr) -> Expr {
    let cond = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(first.clone()),
        Operator::Eq,
        Box::new(second),
    ));
    Expr::ScalarFunction(ScalarFunction::new_udf(
        nullif_pick_udf(),
        vec![cond, first],
    ))
}

fn needs_spark_shaped_cast(from_type: &DataType, common: &DataType) -> bool {
    matches!(
        from_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
    ) && matches!(common, DataType::Timestamp(_, _))
}

fn shape_operand(expr: &Expr, path: &[String], from_type: &DataType, common: &DataType) -> Expr {
    let mut shaped = expr.clone();
    for name in path {
        shaped = get_field(shaped, name.as_str());
    }
    if from_type == common {
        shaped
    } else if needs_spark_shaped_cast(from_type, common) {
        nvl_cast_expr(shaped, common)
    } else {
        Expr::Cast(Cast::new(Box::new(shaped), common.clone()))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn ctx() -> SessionContext {
        let rules = append_nvl_family_rule(Analyzer::new().rules);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        for udf in crate::spark_nvl_udf::functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        for udf in crate::spark_nvl_eager::functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        ctx
    }

    async fn one_row(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        pretty.lines().map(str::to_owned).collect()
    }

    async fn cell_is_null(ctx: &SessionContext, sql: &str) -> bool {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        assert_eq!(batches[0].num_rows(), 1, "expected a single row for {sql}");
        batches[0].column(0).is_null(0)
    }

    async fn plan_err(ctx: &SessionContext, sql: &str) -> String {
        ctx.sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sql} planned, want refusal"))
            .to_string()
    }

    #[tokio::test]
    async fn rule_keeps_first_argument_type() {
        let ctx = ctx();
        let rows = one_row(&ctx, "SELECT nullif(1, 2) AS v").await;
        assert!(rows.iter().any(|row| row.contains('1')), "{rows:?}");
        assert!(
            cell_is_null(&ctx, "SELECT nullif(1, 1) AS v").await,
            "nullif(1, 1) answers null"
        );
    }

    #[tokio::test]
    async fn rule_refuses_like_spark() {
        let ctx = ctx();
        let message = plan_err(&ctx, "SELECT nullif(1, DATE '2024-01-01') AS v").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]"),
            "{message}"
        );
        let message = plan_err(&ctx, "SELECT nullifzero(DATE '2024-01-01') AS v").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]"),
            "{message}"
        );
        assert!(message.contains("(... = 0)"), "{message}");
        assert!(message.contains("(\"DATE\" and \"INT\")"), "{message}");
    }

    #[tokio::test]
    async fn rewrite_preserves_cardinality_fold() {
        use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
        use datafusion::prelude::SessionConfig;
        let settings = crate::cardinality::ReparkSqlSettings {
            max_array_elements: 100,
            ..crate::cardinality::ReparkSqlSettings::default()
        };
        let config = crate::cardinality::with_repark_sql_config(SessionConfig::new(), settings);
        let mut rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> =
            vec![Arc::new(TypeCoercion::new())];
        rules.extend(crate::cardinality::analyzer_rules());
        rules = append_nvl_family_rule(rules);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        for udf in crate::spark_nvl_udf::functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        for udf in crate::spark_nvl_eager::functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        let sql = "SELECT array_repeat(1, nullif(101, 0)) AS v";
        let frame = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"));
        let message = crate::analyze_eagerly(&ctx.state(), frame.logical_plan().clone())
            .expect_err("const over-ceiling must refuse")
            .to_string();
        assert!(
            message.contains(crate::cardinality::MAX_ARRAY_ELEMENTS_KEY),
            "{message}"
        );
    }

    #[test]
    fn nullif_compare_form_bounded_by_first_argument() {
        use datafusion::logical_expr::lit;
        let compare =
            crate::spark_nvl_eager::nullif_compare_expr(lit(101i32), lit(0i64), &DataType::Int64);
        let args = vec![lit(1i64), compare];
        let err = crate::cardinality::refuse_literal_expansion("array_repeat", &args, 100)
            .expect_err("mixed-width compare form must refuse over the ceiling")
            .to_string();
        assert!(
            err.contains(crate::cardinality::MAX_ARRAY_ELEMENTS_KEY),
            "{err}"
        );
    }

    #[test]
    fn nullif_decimal_double_seconds_fold_to_null() {
        use datafusion::arrow::datatypes::DataType as ArrowType;
        use datafusion::logical_expr::lit;
        let decimal = Expr::Literal(ScalarValue::Decimal128(Some(1010), 4, 1), None);
        let double = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(decimal),
            ArrowType::Float64,
        ));
        for second in [
            double,
            Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(lit(101i32)),
                ArrowType::Decimal128(5, 0),
            )),
            lit(101i64),
        ] {
            let count = df_nullif(lit(101i32), second);
            crate::cardinality::refuse_literal_expansion("array_repeat", &[lit(1i64), count], 100)
                .expect("proven-equal nullif must skip the ceiling");
        }
    }

    #[test]
    fn nullif_string_sides_skip_or_refuse() {
        use datafusion::logical_expr::lit;
        let string_first = df_nullif(lit("101"), lit(0i32));
        crate::cardinality::refuse_literal_expansion(
            "array_repeat",
            &[lit(1i64), string_first],
            100,
        )
        .expect("string-first nullif stays unknown to the ceiling");
        for text in ["101", " 101 ", "+101"] {
            let string_second = df_nullif(lit(101i32), lit(text));
            crate::cardinality::refuse_literal_expansion(
                "array_repeat",
                &[lit(1i64), string_second],
                100,
            )
            .unwrap_or_else(|error| {
                panic!("equal string-second nullif skips the ceiling for {text:?}: {error}")
            });
        }
        for text in ["102", "abc", "101.0", ""] {
            let string_second = df_nullif(lit(101i32), lit(text));
            let outcome = crate::cardinality::refuse_literal_expansion(
                "array_repeat",
                &[lit(1i64), string_second],
                100,
            );
            let err = match outcome {
                Err(error) => error.to_string(),
                Ok(()) => panic!("unequal string-second nullif refuses for {text:?}"),
            };
            assert!(
                err.contains(crate::cardinality::MAX_ARRAY_ELEMENTS_KEY),
                "{err}"
            );
        }
    }

    #[tokio::test]
    async fn rule_compares_structs_by_position() {
        let ctx = ctx();
        assert!(
            cell_is_null(&ctx, "SELECT nullif(STRUCT(1 AS a), STRUCT(1 AS b)) AS v").await,
            "positional struct compare answers null on equal fields"
        );
        let rows = one_row(&ctx, "SELECT nullif(STRUCT(1 AS a), STRUCT(2 AS b)) AS v").await;
        assert!(rows.iter().any(|row| row.contains('1')), "{rows:?}");
    }
}
