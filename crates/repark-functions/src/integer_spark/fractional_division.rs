use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use std::sync::Arc;

use datafusion::common::{Column, DFSchema, Result, TableReference};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator, Projection,
    WriteOp,
};
use datafusion::optimizer::AnalyzerRule;

use super::{INTEGER_ADD_NAME, INTEGER_MUL_NAME, INTEGER_SUB_NAME, contains_lambda_variable};

#[derive(Debug, Default)]
pub struct SparkFractionalDivision;

impl AnalyzerRule for SparkFractionalDivision {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if !plan_has_division(&plan)? {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    fn name(&self) -> &'static str {
        "spark_fractional_division"
    }
}

fn plan_has_division(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            if expr.exists(|leaf| {
                Ok(matches!(leaf, Expr::BinaryExpr(binary) if binary.op == Operator::Divide))
            })? {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(found)
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(rewrite_expr(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    let retyped = if transformed.transformed && matches!(transformed.data, LogicalPlan::Values(_)) {
        transformed.map_data(rebuild_values)?
    } else {
        transformed.map_data(LogicalPlan::recompute_schema)?
    };
    retyped.transform_data(conform_integer_store)
}

fn rebuild_values(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Values(values) => LogicalPlanBuilder::values(values.values)?.build(),
        other => Ok(other),
    }
}

fn conform_integer_store(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Dml(mut dml) = plan else {
        return Ok(Transformed::no(plan));
    };
    let target = dml.target.schema();
    let source = dml.input.schema();
    let stale = matches!(dml.op, WriteOp::Insert(_) | WriteOp::Update)
        && target.fields().len() == source.fields().len()
        && target
            .fields()
            .iter()
            .zip(source.fields())
            .any(|(target, source)| {
                fractional_into_integer(source.data_type(), target.data_type())
            });
    if !stale {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    }
    let (exprs, input) = match dml.input.as_ref() {
        LogicalPlan::Projection(projection) => {
            (projection.expr.clone(), Arc::clone(&projection.input))
        }
        other => (
            source
                .iter()
                .map(|(qualifier, field)| {
                    Expr::Column(Column::new(qualifier.cloned(), field.name()))
                })
                .collect(),
            Arc::new(other.clone()),
        ),
    };
    let conformed = exprs
        .into_iter()
        .zip(source.iter())
        .zip(target.fields())
        .map(|((expr, (qualifier, field)), target)| {
            conform_store_expr(
                expr,
                qualifier,
                field.data_type(),
                target.data_type(),
                field.name(),
            )
        })
        .collect();
    dml.input = Arc::new(LogicalPlan::Projection(Projection::try_new(
        conformed, input,
    )?));
    Ok(Transformed::yes(LogicalPlan::Dml(dml)))
}

fn conform_store_expr(
    expr: Expr,
    qualifier: Option<&TableReference>,
    source: &DataType,
    target: &DataType,
    name: &str,
) -> Expr {
    if !fractional_into_integer(source, target) {
        return expr;
    }
    let inner = match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    };
    Expr::Cast(Cast::new(Box::new(inner), target.clone())).alias_qualified(qualifier.cloned(), name)
}

fn fractional_into_integer(source: &DataType, target: &DataType) -> bool {
    source.is_floating() && target.is_integer()
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Divide => {
            fractional_division(binary, schema)
        }
        Expr::ScalarFunction(function) => unarm_fractional_operands(function, schema),
        other => Transformed::no(other),
    }
}

fn fractional_division(binary: BinaryExpr, schema: &DFSchema) -> Transformed<Expr> {
    let integral = matches!(
        (binary.left.get_type(schema), binary.right.get_type(schema)),
        (Ok(left), Ok(right)) if left.is_integer() && right.is_integer()
    );
    if !integral
        || contains_lambda_variable(&binary.left)
        || contains_lambda_variable(&binary.right)
    {
        return Transformed::no(Expr::BinaryExpr(binary));
    }
    Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Cast(Cast::new(binary.left, DataType::Float64))),
        Operator::Divide,
        Box::new(Expr::Cast(Cast::new(binary.right, DataType::Float64))),
    )))
}

fn unarm_fractional_operands(function: ScalarFunction, schema: &DFSchema) -> Transformed<Expr> {
    let Some(operator) = integer_operator(function.func.name()) else {
        return Transformed::no(Expr::ScalarFunction(function));
    };
    if !function
        .args
        .iter()
        .any(|arg| is_fractional_operand(arg, schema))
    {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    match <[Expr; 2]>::try_from(function.args) {
        Ok([left, right]) => Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(strip_cast(left)),
            operator,
            Box::new(strip_cast(right)),
        ))),
        Err(args) => Transformed::no(Expr::ScalarFunction(ScalarFunction::new_udf(
            function.func,
            args,
        ))),
    }
}

fn integer_operator(name: &str) -> Option<Operator> {
    match name {
        INTEGER_ADD_NAME => Some(Operator::Plus),
        INTEGER_SUB_NAME => Some(Operator::Minus),
        INTEGER_MUL_NAME => Some(Operator::Multiply),
        _ => None,
    }
}

fn is_fractional_operand(arg: &Expr, schema: &DFSchema) -> bool {
    let Expr::Cast(cast) = arg else {
        return false;
    };
    matches!(cast.expr.get_type(schema), Ok(data_type) if data_type.is_numeric() && !data_type.is_integer())
}

fn strip_cast(expr: Expr) -> Expr {
    match expr {
        Expr::Cast(cast) => *cast.expr,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    fn prod_like_ctx() -> SessionContext {
        let mut rules = crate::lambda_rebind::analyzer_rules_with_higher_order_preparation(
            Analyzer::new().rules,
        )
        .expect("prod-like assembly keeps a type_coercion seat");
        rules.extend(crate::analyzer_rules());
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), true);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        crate::register_all(&ctx);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("i", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .expect("fixture batch");
        ctx.register_batch("t", batch).expect("register t");
        ctx
    }

    async fn batches(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql} plans: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("{sql} runs: {error}"))
    }

    async fn f64_column(ctx: &SessionContext, sql: &str, index: usize) -> Vec<Option<f64>> {
        let mut values = Vec::new();
        for batch in batches(ctx, sql).await {
            assert_eq!(
                batch.schema().field(index).data_type(),
                &DataType::Float64,
                "{sql} column {index} must be DOUBLE like Spark"
            );
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64Array");
            values
                .extend((0..array.len()).map(|row| array.is_valid(row).then(|| array.value(row))));
        }
        values
    }

    async fn i64_column(ctx: &SessionContext, sql: &str) -> Vec<Option<i64>> {
        i64_column_at(ctx, sql, 0).await
    }

    async fn i64_column_at(ctx: &SessionContext, sql: &str, index: usize) -> Vec<Option<i64>> {
        let mut values = Vec::new();
        for batch in batches(ctx, sql).await {
            assert_eq!(
                batch.schema().field(index).data_type(),
                &DataType::Int64,
                "{sql} column {index} must be BIGINT like Spark"
            );
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array");
            values
                .extend((0..array.len()).map(|row| array.is_valid(row).then(|| array.value(row))));
        }
        values
    }

    async fn i32_column(ctx: &SessionContext, sql: &str) -> Vec<Option<i32>> {
        let mut values = Vec::new();
        for batch in batches(ctx, sql).await {
            assert_eq!(
                batch.schema().field(0).data_type(),
                &DataType::Int32,
                "{sql}"
            );
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array");
            values
                .extend((0..array.len()).map(|row| array.is_valid(row).then(|| array.value(row))));
        }
        values
    }

    async fn error_text(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .expect_err("Spark raises here")
                .to_string(),
        }
    }

    fn doubles(values: &[f64]) -> Vec<Option<f64>> {
        values.iter().copied().map(Some).collect()
    }

    #[tokio::test]
    async fn derived_table_arithmetic_over_division_is_double() {
        let ctx = prod_like_ctx();
        for column in ["id", "i"] {
            let inner = format!("SELECT id, {column} / 2 AS h FROM t");
            let sql = format!("SELECT h * 2, h + 1, h - 1, h FROM ({inner}) s ORDER BY id");
            assert_eq!(f64_column(&ctx, &sql, 0).await, doubles(&[1.0, 2.0, 3.0]));
            assert_eq!(f64_column(&ctx, &sql, 1).await, doubles(&[1.5, 2.0, 2.5]));
            assert_eq!(f64_column(&ctx, &sql, 2).await, doubles(&[-0.5, 0.0, 0.5]));
            assert_eq!(f64_column(&ctx, &sql, 3).await, doubles(&[0.5, 1.0, 1.5]));
        }
    }

    #[tokio::test]
    async fn cte_and_nested_scopes_over_division_are_double() {
        let ctx = prod_like_ctx();
        for column in ["id", "i"] {
            let inner = format!("SELECT id, {column} / 2 AS h FROM t");
            let cte = format!("WITH s AS ({inner}) SELECT h * 2, h + 1 FROM s ORDER BY id");
            assert_eq!(f64_column(&ctx, &cte, 0).await, doubles(&[1.0, 2.0, 3.0]));
            assert_eq!(f64_column(&ctx, &cte, 1).await, doubles(&[1.5, 2.0, 2.5]));
            let nested =
                format!("SELECT h * 2, h - 1 FROM (SELECT id, h FROM ({inner}) s1) s2 ORDER BY id");
            assert_eq!(
                f64_column(&ctx, &nested, 0).await,
                doubles(&[1.0, 2.0, 3.0])
            );
            assert_eq!(
                f64_column(&ctx, &nested, 1).await,
                doubles(&[-0.5, 0.0, 0.5])
            );
        }
    }

    #[tokio::test]
    async fn same_scope_arithmetic_over_division_is_double() {
        let ctx = prod_like_ctx();
        for column in ["id", "i"] {
            let sql = format!("SELECT {column} / 2 + 1, {column} / 2 * 2 FROM t ORDER BY id");
            assert_eq!(f64_column(&ctx, &sql, 0).await, doubles(&[1.5, 2.0, 2.5]));
            assert_eq!(f64_column(&ctx, &sql, 1).await, doubles(&[1.0, 2.0, 3.0]));
        }
    }

    #[tokio::test]
    async fn filter_and_sort_over_derived_division_see_the_fraction() {
        let ctx = prod_like_ctx();
        for column in ["id", "i"] {
            let inner = format!("SELECT id, {column} / 2 AS h FROM t");
            let filtered = format!("SELECT id FROM ({inner}) s WHERE h + 1 = 1.5");
            assert_eq!(i64_column(&ctx, &filtered).await, vec![Some(1)]);
            let sorted = format!("SELECT id FROM ({inner}) s ORDER BY h + 0 DESC");
            assert_eq!(
                i64_column(&ctx, &sorted).await,
                vec![Some(3), Some(2), Some(1)]
            );
        }
    }

    #[tokio::test]
    async fn sum_over_int_division_is_double() {
        let ctx = prod_like_ctx();
        let sql = "SELECT sum(h) FROM (SELECT i / CAST(2 AS INT) AS h FROM t) s";
        assert_eq!(f64_column(&ctx, sql, 0).await, doubles(&[3.0]));
    }

    #[tokio::test]
    async fn chained_derived_arithmetic_stays_double() {
        let ctx = prod_like_ctx();
        let sql = "SELECT g + 1 FROM (SELECT id, h * 2 AS g FROM \
                   (SELECT id, id / 2 AS h FROM t) s1) s2 ORDER BY id";
        assert_eq!(f64_column(&ctx, sql, 0).await, doubles(&[2.0, 3.0, 4.0]));
    }

    #[tokio::test]
    async fn bigint_max_plus_a_derived_fraction_does_not_overflow() {
        let ctx = prod_like_ctx();
        let sql = "SELECT h + 9223372036854775807 FROM (SELECT id / 2 AS h FROM t) s";
        assert_eq!(
            f64_column(&ctx, sql, 0).await,
            doubles(&[9.223_372_036_854_776e18; 3])
        );
    }

    #[tokio::test]
    async fn integral_derived_arithmetic_stays_integral() {
        let ctx = prod_like_ctx();
        let bigint = "SELECT a + 1 FROM (SELECT id, id * 2 AS a FROM t) s ORDER BY id";
        assert_eq!(
            i64_column(&ctx, bigint).await,
            vec![Some(3), Some(5), Some(7)]
        );
        let int = "SELECT a + 1 FROM (SELECT id, i * 2 AS a FROM t) s ORDER BY id";
        assert_eq!(i32_column(&ctx, int).await, vec![Some(3), Some(5), Some(7)]);
        let long_overflow = error_text(
            &ctx,
            "SELECT a + 1 FROM (SELECT 9223372036854775807 AS a) s",
        )
        .await;
        assert!(
            long_overflow.contains("ARITHMETIC_OVERFLOW")
                && long_overflow.contains("long overflow"),
            "{long_overflow}"
        );
        let int_overflow = error_text(
            &ctx,
            "SELECT a * 2147483647 FROM (SELECT i * 2 AS a FROM t) s",
        )
        .await;
        assert!(
            int_overflow.contains("ARITHMETIC_OVERFLOW")
                && int_overflow.contains("integer overflow")
                && int_overflow.contains("try_multiply"),
            "{int_overflow}"
        );
    }

    #[tokio::test]
    async fn integer_beside_division_keeps_type_and_checking() {
        let ctx = prod_like_ctx();
        let sql =
            "SELECT h + 1, a + 1 FROM (SELECT id, id / 2 AS h, id * 2 AS a FROM t) s ORDER BY id";
        assert_eq!(f64_column(&ctx, sql, 0).await, doubles(&[1.5, 2.0, 2.5]));
        assert_eq!(
            i64_column_at(&ctx, sql, 1).await,
            vec![Some(3), Some(5), Some(7)]
        );
        let overflow = error_text(&ctx, "SELECT id / 2, 9223372036854775807 + id FROM t").await;
        assert!(
            overflow.contains("ARITHMETIC_OVERFLOW") && overflow.contains("long overflow"),
            "{overflow}"
        );
    }

    fn with_store(ctx: &SessionContext, name: &str, value_type: DataType) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", value_type, true),
        ]));
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![]])
            .expect("empty store table");
        ctx.register_table(name, Arc::new(table))
            .expect("register store table");
    }

    async fn stored_i64(ctx: &SessionContext, insert: &str, table: &str) -> Vec<Option<i64>> {
        batches(ctx, insert).await;
        i64_column_at(ctx, &format!("SELECT v FROM {table} ORDER BY id"), 0).await
    }

    #[tokio::test]
    async fn fractional_quotient_stores_into_bigint_like_spark() {
        let ctx = prod_like_ctx();
        let inner = "SELECT id, id / 2 AS h FROM t";
        for (name, insert, want) in [
            ("plain", "SELECT id, id / 2 FROM t".to_string(), [0, 1, 1]),
            ("neg", "SELECT id, -id / 2 FROM t".to_string(), [0, -1, -1]),
            (
                "derived",
                format!("SELECT id, h + 1 FROM ({inner}) s"),
                [1, 2, 2],
            ),
            (
                "derived_mul",
                format!("SELECT id, h * 3 FROM ({inner}) s"),
                [1, 3, 4],
            ),
            (
                "cte",
                format!("WITH s AS ({inner}) SELECT id, h + 1 FROM s"),
                [1, 2, 2],
            ),
            ("ctl", "SELECT id, id * 2 FROM t".to_string(), [2, 4, 6]),
        ] {
            let table = format!("store_{name}");
            with_store(&ctx, &table, DataType::Int64);
            let stored = stored_i64(&ctx, &format!("INSERT INTO {table} {insert}"), &table).await;
            assert_eq!(stored, want.map(Some).to_vec(), "{name}");
        }
        with_store(&ctx, "store_values", DataType::Int64);
        let stored = stored_i64(
            &ctx,
            "INSERT INTO store_values VALUES (10, 7 / 2), (11, -7 / 2)",
            "store_values",
        )
        .await;
        assert_eq!(stored, vec![Some(3), Some(-3)]);
    }

    #[tokio::test]
    async fn fractional_quotient_stores_into_int_unchanged() {
        let ctx = prod_like_ctx();
        with_store(&ctx, "store_int", DataType::Int32);
        batches(
            &ctx,
            "INSERT INTO store_int SELECT id, h + 1 FROM (SELECT id, id / 2 AS h FROM t) s",
        )
        .await;
        let sql = "SELECT CAST(v AS BIGINT) FROM store_int ORDER BY id";
        assert_eq!(i64_column(&ctx, sql).await, vec![Some(1), Some(2), Some(2)]);
    }

    #[tokio::test]
    async fn out_of_range_quotient_refuses_and_stores_nothing() {
        let ctx = prod_like_ctx();
        with_store(&ctx, "store_range", DataType::Int64);
        let message = error_text(
            &ctx,
            "INSERT INTO store_range SELECT id, id * 1e19 / 1 FROM t",
        )
        .await;
        assert!(message.contains("Int64"), "{message}");
        assert_eq!(
            i64_column(&ctx, "SELECT count(*) FROM store_range").await,
            vec![Some(0)]
        );
    }
}
