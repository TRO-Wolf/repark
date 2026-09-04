//! Spark expression-semantics analyzer rule.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::functions::expr_fn::nullif;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator, lit};
use datafusion::optimizer::AnalyzerRule;

/// G6-3 / G6-5 cast-legality deny matrix and refusal.
mod cast_legality;
mod like_escape;
mod overlay;

/// Spark operator semantics over type-coerced logical plans; the rule is stateless.
#[derive(Debug, Default)]
pub struct SparkExprSemantics;

impl AnalyzerRule for SparkExprSemantics {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let ansi_enabled = crate::ansi::spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, ansi_enabled))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)] // `AnalyzerRule::name` ties the lifetime to &self
    fn name(&self) -> &str {
        "spark_expr_semantics"
    }
}

/// Rewrite one plan node while preserving output field names.
fn rewrite_plan(plan: LogicalPlan, ansi_enabled: bool) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema, ansi_enabled))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

/// Rewrite one expression bottom-up without revisiting injected subtrees.
fn rewrite_expr(expr: Expr, schema: &DFSchema, ansi_enabled: bool) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(ref binary) if binary.op == Operator::Divide => {
            rewrite_division(expr, schema, ansi_enabled)
        }
        Expr::BinaryExpr(ref binary) if binary.op == Operator::Modulo => {
            rewrite_modulo(expr, schema, ansi_enabled)
        }
        Expr::ScalarFunction(ref function)
            if function.func.name() == "array_element" && function.args.len() == 2 =>
        {
            Ok(rewrite_array_subscript(expr, schema))
        }
        Expr::ScalarFunction(function) if function.func.name() == "substr" => {
            Ok(Transformed::yes(Expr::ScalarFunction(
                ScalarFunction::new_udf(crate::string::substring_udf(), function.args),
            )))
        }
        Expr::ScalarFunction(function)
            if function.func.name() == "overlay" && function.args.len() == 4 =>
        {
            Ok(overlay::rewrite(function))
        }
        Expr::Cast(_) => rewrite_timestamp_casts(expr, schema),
        Expr::TryCast(ref try_cast) => {
            if let Ok(source_type) = try_cast.expr.get_type(schema) {
                cast_legality::refuse_spark_illegal_cast(
                    cast_legality::CastKeyword::TryCast,
                    &try_cast.expr,
                    &source_type,
                    try_cast.field.data_type(),
                )?;
            }
            Ok(Transformed::no(expr))
        }
        other => like_escape::rewrite(other),
    }
}

/// Dispatch a `CAST` through legality, numeric, string, and date rewrites.
/// # Errors
/// Returns a plan error for Spark-illegal DATE↔integer pairs.
fn rewrite_timestamp_casts(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if let Expr::Cast(cast) = &expr
        && let Ok(source_type) = cast.expr.get_type(schema)
    {
        cast_legality::refuse_spark_illegal_cast(
            cast_legality::CastKeyword::Cast,
            &cast.expr,
            &source_type,
            cast.field.data_type(),
        )?;
    }
    let numeric = rewrite_timestamp_to_numeric_cast(expr, schema);
    if numeric.transformed {
        return Ok(numeric);
    }
    let string = rewrite_timestamp_to_string_cast(numeric.data, schema);
    if string.transformed {
        return Ok(string);
    }
    Ok(rewrite_timestamp_to_date_cast(string.data, schema))
}

/// `CAST(<timestamp> AS STRING)` → Spark's session-zone space-separated `Utf8` (B-TZ-4).
fn rewrite_timestamp_to_string_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = expr else {
        return Transformed::no(expr);
    };
    let Ok(source_type) = cast.expr.get_type(schema) else {
        return Transformed::no(Expr::Cast(cast));
    };
    if !matches!(source_type, DataType::Timestamp(..)) {
        return Transformed::no(Expr::Cast(cast));
    }
    if !matches!(
        cast.field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Transformed::no(Expr::Cast(cast));
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::timestamp_cast::spark_timestamp_to_string_udf(),
        vec![*cast.expr],
    )))
}

/// `CAST(<timestamp> AS DATE)` → Spark's session-zone `Date32` (TZ-8).
fn rewrite_timestamp_to_date_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = expr else {
        return Transformed::no(expr);
    };
    let Ok(source_type) = cast.expr.get_type(schema) else {
        return Transformed::no(Expr::Cast(cast));
    };
    if !matches!(source_type, DataType::Timestamp(..)) {
        return Transformed::no(Expr::Cast(cast));
    }
    if !matches!(cast.field.data_type(), DataType::Date32) {
        return Transformed::no(Expr::Cast(cast));
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::timestamp_cast::spark_timestamp_to_date_udf(),
        vec![*cast.expr],
    )))
}

/// `CAST(<timestamp> AS <numeric>)` → Spark's epoch SECONDS (registry row TZ-5).
fn rewrite_timestamp_to_numeric_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = expr else {
        return Transformed::no(expr);
    };
    let Ok(source_type) = cast.expr.get_type(schema) else {
        return Transformed::no(Expr::Cast(cast));
    };
    if !matches!(source_type, DataType::Timestamp(..)) {
        return Transformed::no(Expr::Cast(cast));
    }
    let target = cast.field.data_type().clone();
    let Some(scaled) = epoch_seconds_for_target(&target, *cast.expr.clone()) else {
        return Transformed::no(Expr::Cast(cast));
    };
    Transformed::yes(Expr::Cast(Cast::new(Box::new(scaled), target)))
}

/// Return the epoch-seconds expression for a supported numeric target.
fn epoch_seconds_for_target(target: &DataType, timestamp: Expr) -> Option<Expr> {
    let udf = match target {
        DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
            crate::timestamp_cast::spark_epoch_seconds_floor_udf()
        }
        DataType::Float64
        | DataType::Float32
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => crate::timestamp_cast::spark_epoch_seconds_real_udf(),
        _ => return None,
    };
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        udf,
        vec![timestamp],
    )))
}

/// True when `expr` is a signed integer literal equal to `-1` (Spark overlay default len).
/// Rewrite division with Spark's integer promotion and zero-divisor policy.
fn rewrite_division(
    expr: Expr,
    schema: &DFSchema,
    ansi_enabled: bool,
) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(Transformed::no(expr));
    };
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        return Ok(Transformed::no(Expr::BinaryExpr(binary)));
    };
    let integer_division = left_type.is_integer() && right_type.is_integer();
    let mut left = *binary.left;
    let mut right = *binary.right;
    let divisor_type = if integer_division {
        left = Expr::Cast(Cast::new(Box::new(left), DataType::Float64));
        right = Expr::Cast(Cast::new(Box::new(right), DataType::Float64));
        DataType::Float64
    } else {
        right_type
    };
    let right = guard_zero_divisor(right, &divisor_type, ansi_enabled)?;
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Divide,
        Box::new(right),
    ))))
}

/// Rewrite modulo with Spark's zero-divisor policy without changing operand types.
fn rewrite_modulo(expr: Expr, schema: &DFSchema, ansi_enabled: bool) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(Transformed::no(expr));
    };
    let Ok(divisor_type) = binary.right.get_type(schema) else {
        return Ok(Transformed::no(Expr::BinaryExpr(binary)));
    };
    let right = guard_zero_divisor(*binary.right, &divisor_type, ansi_enabled)?;
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        binary.left,
        Operator::Modulo,
        Box::new(right),
    ))))
}

/// Guard a numeric divisor for `/` and `%` (shared DEC-7 / A2 path).
fn guard_zero_divisor(divisor: Expr, divisor_type: &DataType, ansi_enabled: bool) -> Result<Expr> {
    if !divisor_type.is_numeric() {
        return Ok(divisor);
    }
    if ansi_enabled {
        if is_ansi_zero_guard(&divisor) {
            return Ok(divisor);
        }
        return Ok(crate::ansi::guard_nonzero_divisor(divisor));
    }
    if is_zero_guard(&divisor) {
        return Ok(divisor);
    }
    let zero = ScalarValue::new_zero(divisor_type)?;
    Ok(nullif(divisor, lit(zero)))
}

/// Return whether `expr` is this rule's ANSI zero guard.
fn is_ansi_zero_guard(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function)
            if function.func.name() == crate::ansi::ANSI_NONZERO_DIVISOR_NAME
                && function.args.len() == 1
    )
}

/// Return whether `expr` is a zero `nullif` guard.
fn is_zero_guard(expr: &Expr) -> bool {
    if let Expr::ScalarFunction(function) = expr
        && function.func.name() == "nullif"
        && function.args.len() == 2
        && let Expr::Literal(value, _) = &function.args[1]
        && !value.is_null()
    {
        return matches!(ScalarValue::new_zero(&value.data_type()), Ok(zero) if *value == zero);
    }
    false
}

/// Rewrite planner-lowered `array_element` to Spark's 0-based `[]` UDF.
fn rewrite_array_subscript(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return Transformed::no(expr);
    };
    let types = (
        function.args[0].get_type(schema),
        function.args[1].get_type(schema),
    );
    let (Ok(array_type), Ok(index_type)) = types else {
        return Transformed::no(Expr::ScalarFunction(function));
    };
    if list_element_type(&array_type).is_none() || !index_type.is_integer() {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::collection::spark_array_get_udf(),
        function.args,
    )))
}

/// Return the element type of a list-shaped `DataType`.
fn list_element_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type().clone())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Float64Array, Int64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// Build a context with the analyzer installed.
    fn ctx() -> SessionContext {
        ctx_with_ansi(true)
    }

    /// Non-ANSI mode returns NULL for `/0` and `% 0`.
    fn ctx_legacy() -> SessionContext {
        ctx_with_ansi(false)
    }

    fn ctx_with_ansi(ansi_enabled: bool) -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi_enabled);
        let ctx = SessionContext::new_with_config(config);
        ctx.add_analyzer_rule(Arc::new(SparkExprSemantics));
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        batches.into_iter().next().unwrap()
    }

    async fn f64_column(ctx: &SessionContext, sql: &str) -> Vec<Option<f64>> {
        let batch = batch(ctx, sql).await;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| panic!("expected Float64 for {sql}, got {:?}", batch.schema()));
        (0..column.len())
            .map(|row| column.is_valid(row).then(|| column.value(row)))
            .collect()
    }

    async fn i64_column(ctx: &SessionContext, sql: &str) -> Vec<Option<i64>> {
        let batch = batch(ctx, sql).await;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("expected Int64 for {sql}, got {:?}", batch.schema()));
        (0..column.len())
            .map(|row| column.is_valid(row).then(|| column.value(row)))
            .collect()
    }

    /// Integer division is Spark true division.
    #[tokio::test]
    async fn integer_division_is_double() {
        let ctx = ctx();
        assert_eq!(f64_column(&ctx, "SELECT 5/2").await, vec![Some(2.5)]);
        assert_eq!(f64_column(&ctx, "SELECT 7/2").await, vec![Some(3.5)]);
        assert_eq!(f64_column(&ctx, "SELECT -7/2").await, vec![Some(-3.5)]);
    }

    /// ANSI mode raises `DIVIDE_BY_ZERO` for numeric `/ 0`.
    #[tokio::test]
    async fn division_by_zero_raises_under_default_ansi() {
        let ctx = ctx();
        for sql in [
            "SELECT 1/0",
            "SELECT 1.0/0.0",
            "SELECT CAST(1 AS DOUBLE)/CAST(0 AS DOUBLE)",
            "SELECT a / b FROM (VALUES (1, 0), (9, 3)) AS t(a, b)",
        ] {
            let error = match ctx.sql(sql).await {
                Err(error) => error.to_string(),
                Ok(frame) => frame.collect().await.expect_err(sql).to_string(),
            };
            assert!(
                error.contains("DIVIDE_BY_ZERO"),
                "{sql}: expected DIVIDE_BY_ZERO, got {error}"
            );
        }
    }

    /// Non-ANSI division by zero returns NULL.
    #[tokio::test]
    async fn division_by_zero_is_null_when_ansi_false() {
        let ctx = ctx_legacy();
        assert_eq!(f64_column(&ctx, "SELECT 1/0").await, vec![None]);
        assert_eq!(f64_column(&ctx, "SELECT 1.0/0.0").await, vec![None]);
        assert_eq!(
            f64_column(&ctx, "SELECT CAST(1 AS DOUBLE)/CAST(0 AS DOUBLE)").await,
            vec![None]
        );
        assert_eq!(
            f64_column(
                &ctx,
                "SELECT a / b FROM (VALUES (1, 0), (9, 3)) AS t(a, b) ORDER BY a"
            )
            .await,
            vec![None, Some(3.0)]
        );
    }

    /// ANSI modulo by zero raises while nonzero modulo keeps its type.
    #[tokio::test]
    async fn modulo_by_zero_raises_under_default_ansi() {
        let ctx = ctx();
        let error = match ctx.sql("SELECT 5 % 0").await {
            Err(error) => error.to_string(),
            Ok(frame) => frame.collect().await.expect_err("5 % 0").to_string(),
        };
        assert!(
            error.contains("DIVIDE_BY_ZERO"),
            "expected DIVIDE_BY_ZERO, got {error}"
        );
        assert_eq!(i64_column(&ctx, "SELECT 7 % 3").await, vec![Some(1)]);
    }

    /// Non-ANSI modulo by zero returns NULL.
    #[tokio::test]
    async fn modulo_by_zero_is_null_when_ansi_false() {
        let ctx = ctx_legacy();
        assert_eq!(i64_column(&ctx, "SELECT 5 % 0").await, vec![None]);
        assert_eq!(i64_column(&ctx, "SELECT 7 % 3").await, vec![Some(1)]);
        assert_eq!(f64_column(&ctx, "SELECT 5.0 % 0.0").await, vec![None]);
    }

    /// Decimal division remains decimal.
    #[tokio::test]
    async fn decimal_division_stays_decimal() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2))",
        )
        .await;
        assert!(
            matches!(
                batch.schema().field(0).data_type(),
                DataType::Decimal128(..)
            ),
            "expected decimal, got {:?}",
            batch.schema().field(0).data_type()
        );
    }

    /// Return the first output column type.
    async fn result_type(ctx: &SessionContext, sql: &str) -> DataType {
        batch(ctx, sql).await.schema().field(0).data_type().clone()
    }

    /// Correlated integer division is promoted to Spark true division.
    #[tokio::test]
    async fn correlated_outer_ref_division_is_promoted_to_double() {
        let ctx = ctx();
        ctx.sql("CREATE OR REPLACE VIEW l_outer AS SELECT * FROM (VALUES (1, 5)) AS t(id, a)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE OR REPLACE VIEW l_inner AS SELECT * FROM (VALUES (10)) AS t(b)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let sql = "SELECT o.id, (SELECT i.b / o.a FROM l_inner i) AS ratio FROM l_outer o";
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        let analyzed = crate::analyze_eagerly(&state, plan).unwrap();
        let rendered = analyzed.display_indent_schema().to_string();
        assert!(
            rendered.contains("__repark_ansi_nonzero_divisor__(CAST(outer_ref(o.a) AS Float64))")
                || rendered.contains("__repark_ansi_nonzero_divisor__"),
            "correlated outer-ref divisor must be promoted to Float64 and ANSI-guarded \
             (no integer truncation); got:\n{rendered}"
        );
    }

    /// Scalar-subquery division with an outer integer column is double true division.
    #[tokio::test]
    async fn division_over_subquery_and_column_is_double_matching_spark() {
        let ctx = ctx();
        ctx.sql(
            "CREATE OR REPLACE VIEW l_outer2 AS \
             SELECT * FROM (VALUES (1, 5), (2, 7)) AS t(id, a)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        ctx.sql("CREATE OR REPLACE VIEW l_inner2 AS SELECT * FROM (VALUES (10)) AS t(b)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let sql = "SELECT (SELECT sum(i.b) FROM l_inner2 i) / o.a AS ratio \
                   FROM l_outer2 o ORDER BY o.id";
        assert_eq!(
            f64_column(&ctx, sql).await,
            vec![Some(2.0), Some(10.0 / 7.0)]
        );
    }

    /// Non-ANSI scalar-subquery division returns NULL for a zero divisor.
    #[tokio::test]
    async fn division_over_subquery_zero_divisor_is_null_when_ansi_false() {
        let ctx = ctx_legacy();
        ctx.sql(
            "CREATE OR REPLACE VIEW l_outer2 AS \
             SELECT * FROM (VALUES (1, 5), (2, 7), (3, 0)) AS t(id, a)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        ctx.sql("CREATE OR REPLACE VIEW l_inner2 AS SELECT * FROM (VALUES (10)) AS t(b)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let sql = "SELECT (SELECT sum(i.b) FROM l_inner2 i) / o.a AS ratio \
                   FROM l_outer2 o ORDER BY o.id";
        assert_eq!(
            f64_column(&ctx, sql).await,
            vec![Some(2.0), Some(10.0 / 7.0), None]
        );
    }

    /// Non-decimal numeric division yields `Float64`.
    #[tokio::test]
    async fn all_nondecimal_division_is_double() {
        let ctx = ctx();
        for sql in [
            "SELECT CAST(5 AS BIGINT) / CAST(2 AS BIGINT)",
            "SELECT CAST(5 AS SMALLINT) / CAST(2 AS SMALLINT)",
            "SELECT CAST(5 AS DOUBLE) / CAST(2 AS DOUBLE)",
            "SELECT 5 / CAST(2 AS DOUBLE)",
        ] {
            assert_eq!(f64_column(&ctx, sql).await, vec![Some(2.5)], "{sql}");
        }
    }

    /// Decimal division remains decimal unless a float operand requires `Float64`.
    #[tokio::test]
    async fn division_result_type_class_matches_spark_decimal_rule() {
        let ctx = ctx();
        for sql in [
            "SELECT CAST(1 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2))",
            "SELECT 7 / CAST(2 AS DECIMAL(10,2))",
            "SELECT CAST(7 AS DECIMAL(10,2)) / 2",
        ] {
            assert!(
                matches!(result_type(&ctx, sql).await, DataType::Decimal128(..)),
                "expected decimal class for `{sql}`, got {:?}",
                result_type(&ctx, sql).await
            );
        }
        for sql in [
            "SELECT CAST(7 AS DOUBLE) / CAST(2 AS DECIMAL(10,2))",
            "SELECT CAST(7 AS DECIMAL(10,2)) / CAST(2 AS DOUBLE)",
            "SELECT 7 / 2",
        ] {
            assert!(
                matches!(result_type(&ctx, sql).await, DataType::Float64),
                "expected double for `{sql}`, got {:?}",
                result_type(&ctx, sql).await
            );
        }
    }

    /// Narrow integer indices are cast by the embedded UDF.
    #[tokio::test]
    async fn array_subscript_accepts_narrow_integer_indices() {
        let ctx = ctx();
        assert_eq!(
            i64_column(&ctx, "SELECT [10, 20, 30][CAST(1 AS INT)]").await,
            vec![Some(20)]
        );
    }

    /// The `[]` subscript is 0-based and NULLs invalid indices.
    #[tokio::test]
    async fn array_subscript_is_zero_based() {
        let ctx = ctx();
        let sql = "SELECT [10, 20, 30][idx] FROM (VALUES (0), (1), (2), (3), (-1)) \
                   AS t(idx) ORDER BY idx";
        let batch = batch(&ctx, sql).await;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values: Vec<Option<i64>> = (0..column.len())
            .map(|row| column.is_valid(row).then(|| column.value(row)))
            .collect();
        assert_eq!(
            values,
            vec![None, Some(10), Some(20), Some(30), None],
            "Spark []: 0-based, invalid index → NULL"
        );
    }

    /// Map subscripts lower to `get_field` and remain unchanged.
    #[tokio::test]
    async fn map_subscript_is_untouched() {
        let ctx = ctx();
        let values = i64_column(&ctx, "SELECT map(['k'], [7])['k']").await;
        assert_eq!(values, vec![Some(7)]);
    }

    /// Rewrites preserve the original output column name.
    #[tokio::test]
    async fn division_rewrite_preserves_field_names() {
        let plain = SessionContext::new();
        let spark = ctx();
        let sql = "SELECT a / b FROM (VALUES (1, 2)) AS t(a, b)";
        let plain_name = plain
            .sql(sql)
            .await
            .unwrap()
            .schema()
            .field(0)
            .name()
            .clone();
        let spark_name = spark
            .sql(sql)
            .await
            .unwrap()
            .schema()
            .field(0)
            .name()
            .clone();
        assert_eq!(spark_name, plain_name);
    }

    /// Spark `overlay(..., -1)` matches the three-argument form.
    #[tokio::test]
    async fn overlay_len_minus_one_matches_three_arg() {
        use datafusion::arrow::array::StringArray;

        let ctx = ctx();
        let three = batch(&ctx, "SELECT overlay('abcdef', 'XY', 2) AS o").await;
        let four = batch(&ctx, "SELECT overlay('abcdef', 'XY', 2, -1) AS o").await;
        let three_val = three
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string")
            .value(0);
        let four_val = four
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string")
            .value(0);
        assert_eq!(three_val, "aXYdef");
        assert_eq!(four_val, three_val);
        let two = batch(&ctx, "SELECT overlay('abcdef', 'XY', 2, 2) AS o").await;
        assert_eq!(
            two.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string")
                .value(0),
            "aXYdef"
        );
    }

    /// Timestamp casts use epoch seconds before and after 1970.
    #[tokio::test]
    async fn timestamp_cast_to_bigint_is_epoch_seconds() {
        let ctx = ctx();
        assert_eq!(
            i64_column(
                &ctx,
                "SELECT CAST(TIMESTAMP '1969-12-31 23:30:00' AS BIGINT)"
            )
            .await,
            vec![Some(-1800)]
        );
        assert_eq!(
            i64_column(
                &ctx,
                "SELECT CAST(TIMESTAMP '2024-06-15 12:00:00' AS BIGINT)"
            )
            .await,
            vec![Some(1_718_452_800)]
        );
        assert_eq!(
            i64_column(
                &ctx,
                "SELECT CAST(TIMESTAMP '1970-01-01 00:00:00' AS BIGINT)"
            )
            .await,
            vec![Some(0)]
        );
    }

    /// Negative fractional timestamps floor toward negative infinity.
    #[tokio::test]
    async fn timestamp_cast_to_bigint_floors_on_both_sides_of_the_epoch() {
        let ctx = ctx();
        for (sql, expected) in [
            ("TIMESTAMP '1969-12-31 23:59:59.5'", -1_i64),
            ("TIMESTAMP '1969-12-31 23:59:58.75'", -2),
            ("TIMESTAMP '1969-12-31 23:59:59'", -1),
            ("TIMESTAMP '1970-01-01 00:00:00.75'", 0),
            ("TIMESTAMP '2024-06-15 12:00:01.999999'", 1_718_452_801),
        ] {
            assert_eq!(
                i64_column(&ctx, &format!("SELECT CAST({sql} AS BIGINT)")).await,
                vec![Some(expected)],
                "{sql}"
            );
        }
    }

    /// NULL timestamps remain NULL with `Int64` output.
    #[tokio::test]
    async fn timestamp_cast_of_null_is_null_int64() {
        let ctx = ctx();
        let sql = "SELECT CAST(CAST(NULL AS TIMESTAMP) AS BIGINT)";
        assert_eq!(i64_column(&ctx, sql).await, vec![None]);
        assert_eq!(result_type(&ctx, sql).await, DataType::Int64);
    }

    /// Narrow integer targets scale first, then apply the outer cast.
    #[tokio::test]
    async fn narrower_integer_targets_get_the_same_scaling() {
        let ctx = ctx();
        for (target, arrow_type) in [("INT", DataType::Int32), ("SMALLINT", DataType::Int16)] {
            let sql = format!("SELECT CAST(TIMESTAMP '1969-12-31 23:30:00' AS {target})");
            assert_eq!(result_type(&ctx, &sql).await, arrow_type, "{target}");
            let batch = batch(&ctx, &sql).await;
            let rendered =
                datafusion::arrow::util::pretty::pretty_format_batches(&[batch]).unwrap();
            assert!(
                rendered.to_string().contains("-1800"),
                "{target}: expected Spark's -1800, got:\n{rendered}"
            );
        }
    }

    /// Float and decimal targets retain fractional seconds.
    #[tokio::test]
    async fn real_targets_keep_the_fractional_second() {
        let ctx = ctx();
        assert_eq!(
            f64_column(
                &ctx,
                "SELECT CAST(TIMESTAMP '1969-12-31 23:59:59.5' AS DOUBLE)"
            )
            .await,
            vec![Some(-0.5)]
        );
        assert_eq!(
            f64_column(
                &ctx,
                "SELECT CAST(TIMESTAMP '1969-12-31 23:30:00' AS DOUBLE)"
            )
            .await,
            vec![Some(-1800.0)]
        );
        let decimal = "SELECT CAST(TIMESTAMP '1969-12-31 23:59:59.5' AS DECIMAL(20,6))";
        assert!(
            matches!(
                result_type(&ctx, decimal).await,
                DataType::Decimal128(20, 6)
            ),
            "decimal target keeps its declared precision/scale"
        );
        let rendered =
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch(&ctx, decimal).await])
                .unwrap()
                .to_string();
        assert!(
            rendered.contains("-0.500000"),
            "expected Spark's -0.500000, got:\n{rendered}"
        );
    }

    /// Matching the source timestamp type keeps the rewrite idempotent.
    #[tokio::test]
    async fn the_timestamp_cast_rewrite_is_idempotent() {
        let ctx = ctx();
        let sql = "SELECT CAST(TIMESTAMP '1969-12-31 23:30:00' AS BIGINT) AS epoch_value";
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        let once = crate::analyze_eagerly(&state, plan).unwrap();
        let twice = crate::analyze_eagerly(&state, once.clone()).unwrap();
        assert_eq!(
            once.display_indent_schema().to_string(),
            twice.display_indent_schema().to_string(),
            "a second analyze must be a fixpoint for this rewrite"
        );
        assert_eq!(
            once.display_indent_schema()
                .to_string()
                .matches("__repark_epoch_seconds_floor__")
                .count(),
            1,
            "exactly one scaling UDF, however many times the analyzer runs"
        );
        assert_eq!(i64_column(&ctx, sql).await, vec![Some(-1800)]);
    }

    /// Timestamp-to-timestamp casts remain identity.
    #[tokio::test]
    async fn timestamp_to_timestamp_cast_is_untouched() {
        let ctx = ctx();
        let sql = "SELECT CAST(TIMESTAMP '2024-06-15 12:00:00' AS TIMESTAMP)";
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        let analyzed = crate::analyze_eagerly(&state, plan).unwrap();
        let rendered = analyzed.display_indent_schema().to_string();
        assert!(
            !rendered.contains("__repark_epoch_seconds"),
            "{sql}: no scaling UDF may be injected"
        );
        assert!(
            !rendered.contains("__repark_timestamp_to_string__"),
            "{sql}: identity TIMESTAMP is not the string rewrite"
        );
        assert!(
            !rendered.contains("__repark_timestamp_to_date__"),
            "{sql}: identity TIMESTAMP is not the date rewrite"
        );
    }

    /// Timestamp-to-date casts emit Spark `Date32`.
    #[tokio::test]
    async fn timestamp_cast_to_date_is_spark_date32() {
        let ctx = ctx();
        let sql = "SELECT CAST(TIMESTAMP '2024-06-15 12:00:00' AS DATE)";
        assert_eq!(result_type(&ctx, sql).await, DataType::Date32, "{sql}");
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        let once = crate::analyze_eagerly(&state, plan).unwrap();
        let twice = crate::analyze_eagerly(&state, once.clone()).unwrap();
        let rendered = once.display_indent_schema().to_string();
        assert_eq!(
            rendered,
            twice.display_indent_schema().to_string(),
            "a second analyze must be a fixpoint for the date rewrite"
        );
        assert_eq!(
            rendered.matches("__repark_timestamp_to_date__").count(),
            1,
            "exactly one date-cast UDF, however many times the analyzer runs"
        );
        assert!(
            !rendered.contains("__repark_epoch_seconds"),
            "DATE is not a numeric-scaling target"
        );
        assert!(
            !rendered.contains("__repark_timestamp_to_string__"),
            "DATE is not the string rewrite"
        );
        let printed =
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch(&ctx, sql).await])
                .unwrap()
                .to_string();
        assert!(
            printed.contains("2024-06-15"),
            "NTZ wall date of the spelled TIMESTAMP; got:\n{printed}"
        );
    }

    /// Timestamp-to-string casts emit Spark `Utf8` through one embedded UDF.
    #[tokio::test]
    async fn timestamp_cast_to_string_is_spark_utf8() {
        use datafusion::arrow::array::StringArray;

        let ctx = ctx();
        let sql = "SELECT CAST(TIMESTAMP '2024-06-15 12:00:00' AS STRING)";
        assert_eq!(result_type(&ctx, sql).await, DataType::Utf8, "{sql}");
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        let once = crate::analyze_eagerly(&state, plan).unwrap();
        let twice = crate::analyze_eagerly(&state, once.clone()).unwrap();
        let rendered = once.display_indent_schema().to_string();
        assert_eq!(
            rendered,
            twice.display_indent_schema().to_string(),
            "a second analyze must be a fixpoint for the string rewrite"
        );
        assert_eq!(
            rendered.matches("__repark_timestamp_to_string__").count(),
            1,
            "exactly one string-cast UDF, however many times the analyzer runs"
        );
        assert!(
            !rendered.contains("__repark_epoch_seconds"),
            "STRING is not a numeric-scaling target"
        );
        let batch = batch(&ctx, sql).await;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("expected Utf8 for {sql}, got {:?}", batch.schema()));
        assert_eq!(column.value(0), "2024-06-15 12:00:00");
    }

    /// Integer-to-timestamp casts already read seconds as Spark does.
    #[tokio::test]
    async fn integer_to_timestamp_cast_is_untouched_and_reads_seconds() {
        let ctx = ctx();
        let sql = "SELECT CAST(CAST(-1800 AS BIGINT) AS TIMESTAMP)";
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        let analyzed = crate::analyze_eagerly(&state, plan).unwrap();
        assert!(
            !analyzed
                .display_indent_schema()
                .to_string()
                .contains("__repark_epoch_seconds"),
            "the reverse direction takes no scaling UDF"
        );
        let rendered =
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch(&ctx, sql).await])
                .unwrap()
                .to_string();
        assert!(
            rendered.contains("1969-12-31T23:30:00"),
            "-1800 must read as -1800 SECONDS; got:\n{rendered}"
        );
    }

    /// Epoch-second round trips preserve the instant.
    #[tokio::test]
    async fn epoch_seconds_round_trip_returns_the_instant() {
        let ctx = ctx();
        let sql = "SELECT CAST(CAST(TIMESTAMP '1969-12-31 23:30:00' AS BIGINT) AS TIMESTAMP)";
        let rendered =
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch(&ctx, sql).await])
                .unwrap()
                .to_string();
        assert!(
            rendered.contains("1969-12-31T23:30:00"),
            "round trip must return the instant; got:\n{rendered}"
        );
    }

    /// Timestamp columns use the same per-row path as literals.
    #[tokio::test]
    async fn a_timestamp_column_casts_row_by_row() {
        let ctx = ctx();
        let sql = "SELECT CAST(ts AS BIGINT) AS epoch_value FROM (VALUES \
                   (TIMESTAMP '1969-12-31 23:30:00'), \
                   (TIMESTAMP '1969-12-31 23:59:59.5'), \
                   (CAST(NULL AS TIMESTAMP)), \
                   (TIMESTAMP '2024-06-15 12:00:00')) AS t(ts) ORDER BY ts ASC NULLS FIRST";
        assert_eq!(
            i64_column(&ctx, sql).await,
            vec![None, Some(-1800), Some(-1), Some(1_718_452_800)],
            "NULLS FIRST is spelled explicitly so this row pins the CAST, not the engine's \
             default null ordering (a different class, with its own pins)"
        );
    }

    // The helper preserves planning errors and execution errors as distinct outcomes.

    async fn sql_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame.collect().await.expect_err(sql).to_string(),
        }
    }

    /// G6-3: all four pairs now refuse with Spark's class and Spark's named remedy.
    #[tokio::test]
    async fn date_to_integer_casts_refuse_with_sparks_class() {
        let ctx = ctx();
        for (target, spark_name) in [
            ("INT", "INT"),
            ("BIGINT", "BIGINT"),
            ("TINYINT", "TINYINT"),
            ("SMALLINT", "SMALLINT"),
        ] {
            let sql = format!("SELECT CAST(DATE '2020-01-01' AS {target}) AS n");
            let message = sql_error(&ctx, &sql).await;
            assert!(
                message.contains("[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]"),
                "{sql}: {message}"
            );
            assert!(
                message.contains(&format!("cannot cast \"DATE\" to \"{spark_name}\"")),
                "{sql}: {message}"
            );
            assert!(message.contains("`UNIX_DATE`"), "{sql}: {message}");
        }
    }

    /// The gate checks the type pair and ignores `spark.sql.ansi.enabled`, matching Spark.
    #[tokio::test]
    async fn the_legality_gate_fires_in_both_ansi_modes() {
        for ctx in [ctx(), ctx_legacy()] {
            let message = sql_error(&ctx, "SELECT CAST(DATE '2020-01-01' AS INT) AS n").await;
            assert!(
                message.contains("[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]"),
                "{message}"
            );
        }
    }

    /// The `try_cast` arm is total over VALUES, never over TYPES.
    #[tokio::test]
    async fn try_cast_date_to_int_refuses_and_spells_try_cast() {
        let ctx = ctx();
        let message = sql_error(&ctx, "SELECT try_cast(DATE '2020-01-01' AS INT) AS n").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]"),
            "{message}"
        );
        assert!(message.contains("TRY_CAST("), "{message}");
    }

    /// G6-2 must not move: `try_cast` over a LEGAL pair whose VALUE fails still nulls out.
    #[tokio::test]
    async fn try_cast_over_a_legal_pair_still_nulls_the_bad_value() {
        let ctx = ctx();
        assert_eq!(
            i64_column(&ctx, "SELECT CAST(try_cast('abc' AS INT) AS BIGINT) AS n").await,
            vec![None]
        );
    }

    /// G6-5, the reverse direction, and the remedy Spark names for it.
    #[tokio::test]
    async fn int_to_date_casts_refuse_with_the_reverse_remedy() {
        let ctx = ctx();
        // A bare integer literal is `Int64`, so the message names BIGINT.
        for (sql, spark_name) in [
            ("SELECT CAST(18262 AS DATE) AS d", "BIGINT"),
            ("SELECT CAST(CAST(18262 AS INT) AS DATE) AS d", "INT"),
        ] {
            let message = sql_error(&ctx, sql).await;
            assert!(
                message.contains(&format!("cannot cast \"{spark_name}\" to \"DATE\"")),
                "{sql}: {message}"
            );
            assert!(
                message.contains("`DATE_FROM_UNIX_DATE`"),
                "{sql}: {message}"
            );
        }
    }

    /// TZ-5 shares the function the gate now heads, and `1577836800` is the value G6-4 pins.
    #[tokio::test]
    async fn a_timestamp_source_never_reaches_the_legality_gate() {
        let ctx = ctx();
        assert_eq!(
            i64_column(
                &ctx,
                "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS BIGINT) AS n"
            )
            .await,
            vec![Some(1_577_836_800)]
        );
    }

    /// `unix_date` is the named remedy; simplify later lowers it to `CAST(a AS Int32)`.
    #[tokio::test]
    async fn the_remedy_the_error_names_still_works() {
        // `unix_date`/`datediff` are `datafusion-spark` UDFs; only `register_all` installs them.
        let ctx = ctx();
        crate::register_all(&ctx);
        assert_eq!(
            i64_column(
                &ctx,
                "SELECT CAST(unix_date(DATE '2020-01-01') AS BIGINT) AS n"
            )
            .await,
            vec![Some(18262)],
            "a gate in the OPTIMIZER would refuse the function its own message recommends"
        );
        assert_eq!(
            i64_column(
                &ctx,
                "SELECT CAST(datediff(DATE '2020-01-05', DATE '2020-01-01') AS BIGINT) AS n"
            )
            .await,
            vec![Some(4)],
            "`SparkDateDiff::simplify` manufactures a cast too — its source types as Int64"
        );
    }

    /// Non-integer targets remain refused by the Arrow/DataFusion kernel, not this gate.
    #[tokio::test]
    async fn date_to_non_integer_targets_keep_the_datafusion_needle() {
        let ctx = ctx();
        for target in ["DOUBLE", "BOOLEAN", "DECIMAL(10,0)"] {
            let sql = format!("SELECT CAST(DATE '2020-01-01' AS {target}) AS n");
            let message = sql_error(&ctx, &sql).await;
            assert!(
                !message.contains("CAST_WITH_FUNC_SUGGESTION"),
                "{sql} must NOT be claimed by the legality gate: {message}"
            );
        }
    }

    /// DATE → STRING and DATE → TIMESTAMP are legal in Spark and must still answer.
    #[tokio::test]
    async fn legal_date_targets_still_answer() {
        let ctx = ctx();
        let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&[batch(
            &ctx,
            "SELECT CAST(DATE '2020-01-01' AS STRING) AS s, \
             CAST(DATE '2020-01-01' AS TIMESTAMP) AS t",
        )
        .await])
        .unwrap()
        .to_string();
        assert!(rendered.contains("2020-01-01"), "{rendered}");
    }
}
