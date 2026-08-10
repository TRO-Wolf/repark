//! Spark expression-semantics analyzer rule.
//!
//! DataFusion's native SQL semantics diverge from Spark's on a small set of core operators, and
//! the raw-SQL passthrough (`spark.sql(...)` → `ctx.sql`) inherited every divergence silently
//! (audit AR-1: findings #1, #4-adjacent, #5, #16 — `task/audit-2026-07-10.md`). This rule
//! rewrites every analyzed logical plan — passthrough SQL, `DataFrame` ops, and CTAS/DML inner
//! queries alike — to Spark's semantics:
//!
//! - **Integer `/` is always-double division** (Spark's `/` maps to `Divide` → `DoubleType`;
//!   DataFusion truncates on integer operands: `5/2 = 2`): both operands are cast to `Float64`
//!   when both are integers.
//! - **Division / modulo by zero yields NULL** — Spark's default (non-ANSI) semantics for every
//!   numeric type, where DataFusion errors on integer `/ 0` and yields `±inf`/`NaN` on float:
//!   the divisor is wrapped in `nullif(divisor, 0)`. `RePark` targets Spark's **non-ANSI**
//!   mode on this surface (decision recorded in `task/lessons.md`, 2026-07-12).
//! - **The `[]` array subscript is 0-based** with invalid-index → NULL (Spark `GetArrayItem`;
//!   DataFusion's is 1-based with negative-from-end). The SQL planner lowers `arr[i]` to
//!   `array_element(arr, i)`, which this rule rewrites onto the embedded
//!   `__repark_array_get__` UDF ([`crate::collection`]) carrying Spark's semantics. Map
//!   subscripts lower to `get_field` and are untouched. A *directly spelled*
//!   `array_element(arr, i)` call is rewritten identically — Spark has no function of that
//!   name, so the DataFusion-native 1-based spelling is knowingly sacrificed for `[]` parity
//!   (Spark code wanting 1-based access spells `element_at`, which [`crate::collection`]
//!   provides).
//! - **`overlay(str, replace, pos, -1)`** drops the literal `-1` 4th arg so free-SQL matches
//!   Spark's default (replace-length / 3-arg). DataFusion's 4-arg `-1` replaces the remainder
//!   of the string (F2 octo C1-Q-002).
//!
//! Registered by the session *after* the built-in analyzer rules (via the Spark door's
//! `SessionExtension`), so it sees type-coerced plans and must emit exactly-typed expressions —
//! no re-coercion runs afterwards. Every rewrite is **idempotent**: the passthrough analyzes
//! eagerly (so schema consumers — the PyO3 Arrow export, CTAS schema derivation — see the
//! post-rewrite types) and physical planning analyzes again.
//!
//! A `get_type` failure on an operand leaves that expression untouched (the `Transformed::no`
//! bail in [`rewrite_division`] / [`rewrite_modulo`]) rather than failing the plan. That bail is
//! **defensive only** — it is not reached by any valid analyzed plan today: correlated / outer-
//! query references arrive as `Expr::OuterReferenceColumn`, which carries its type (DataFusion's
//! SQL planner wraps them before any analyzer rule runs), so a correlated `int / int` resolves
//! and IS rewritten — to `CAST(_ AS Float64) / nullif(CAST(_ AS Float64), 0)`, exactly like the
//! non-correlated case (verified against live Spark 4.1.2, Group L 2026-07-23); and DataFusion
//! 52.5's SQL surface has no higher-order / lambda form that would introduce a variable outside
//! the node's input schema. (Correcting the earlier note that claimed correlated refs keep
//! DataFusion's integer truncation — they do not.)
//!
//! Fixpoint note (Group L-write 2026-07-23): a single analyze is NOT always a fixpoint. This rule
//! runs AFTER `TypeCoercion` within one analyzer invocation, so when it rewrites an `int / int`
//! branch to `Float64`, a parent set operation (`UNION`) that `TypeCoercion` already coerced
//! against the pre-rewrite `Int64` branches keeps its `Int64` output — only a SECOND analyze
//! (whose `TypeCoercion` re-runs over the now-`Float64` branches) propagates `Float64` up through
//! the `UNION`. Execution reaches that fixpoint for free (physical planning re-analyzes), so facade
//! SELECT paths — which double-analyze via the `PyDataFrame` constructor — were always correct.
//! The single-analyze **WRITE path** (CTAS schema derivation) was not: it derived the table schema
//! from the once-analyzed `UNION` (`Int64`) while the executed data was `Float64`, so
//! `CREATE TABLE t AS SELECT 5/2 AS q UNION ALL SELECT 7/2` failed loud at the parquet writer
//! (`Field q has type Int64, array has type Float64`). Fixed by analyzing the write-schema plan to
//! the fixpoint in `repark_sql::execute_ctas` (never a silent wrong answer — it always failed
//! loud).

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::functions::expr_fn::nullif;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator, lit};
use datafusion::optimizer::AnalyzerRule;

/// ===========================================================================================
/// The analyzer rule: Spark operator semantics over type-coerced logical plans.
///
/// See the module docs for the exact rewrites. Stateless — one instance serves every session.
/// ===========================================================================================
#[derive(Debug, Default)]
pub struct SparkExprSemantics;

impl AnalyzerRule for SparkExprSemantics {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)] // `AnalyzerRule::name` ties the lifetime to &self
    fn name(&self) -> &str {
        "spark_expr_semantics"
    }
}

/// Rewrite one plan node's expressions against its merged input schema, preserving the output
/// field names the un-rewritten expressions produced (the same `NamePreserver` discipline the
/// built-in `TypeCoercion` rule uses — a rewrite must never rename `SELECT a / b`'s column).
fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    // A rewrite can change expression types (int / int → Float64), so every node's cached
    // output schema must be recomputed — unconditionally, because a parent whose own
    // expressions were untouched (a Sort over a rewritten Projection) still caches its child's
    // pre-rewrite schema, and the optimizer asserts schema stability after every rule.
    transformed.map_data(LogicalPlan::recompute_schema)
}

/// The per-expression rewrite. Bottom-up: operands are already rewritten when their parent is
/// visited, and replacement subtrees are not revisited (no self-recursion on the injected
/// `array_element` / `nullif` nodes).
fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(ref binary) if binary.op == Operator::Divide => {
            rewrite_division(expr, schema)
        }
        Expr::BinaryExpr(ref binary) if binary.op == Operator::Modulo => {
            rewrite_modulo(expr, schema)
        }
        Expr::ScalarFunction(ref function)
            if function.func.name() == "array_element" && function.args.len() == 2 =>
        {
            Ok(rewrite_array_subscript(expr, schema))
        }
        // The SQL planner lowers `substr(...)` / `SUBSTRING(x FROM y FOR z)` through an
        // `ExprPlanner` that embeds DataFusion's *built-in* UDF directly, bypassing the
        // registry where `crate::string` shadows it — swap the node onto the Spark shim.
        // (The shim's own instance is named "substring", so this never self-matches.)
        Expr::ScalarFunction(function) if function.func.name() == "substr" => {
            Ok(Transformed::yes(Expr::ScalarFunction(
                ScalarFunction::new_udf(crate::string::substring_udf(), function.args),
            )))
        }
        // Spark `overlay(str, replace, pos, -1)` — default len means replace-length (same as
        // the 3-arg form). DataFusion's 4-arg `-1` replaces the remainder of the string; drop
        // a literal -1 4th arg so free-SQL matches Spark (F2 octo C1-Q-002).
        Expr::ScalarFunction(function)
            if function.func.name() == "overlay" && function.args.len() == 4 =>
        {
            if is_negative_one_literal(&function.args[3]) {
                let mut args = function.args;
                args.truncate(3);
                Ok(Transformed::yes(Expr::ScalarFunction(
                    ScalarFunction::new_udf(function.func, args),
                )))
            } else {
                Ok(Transformed::no(Expr::ScalarFunction(function)))
            }
        }
        other => Ok(Transformed::no(other)),
    }
}

/// True when `expr` is a signed integer literal equal to `-1` (Spark overlay default len).
fn is_negative_one_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            ScalarValue::Int64(Some(-1))
                | ScalarValue::Int32(Some(-1))
                | ScalarValue::Int16(Some(-1))
                | ScalarValue::Int8(Some(-1)),
            _
        )
    )
}

/// `a / b` → Spark division: integer ÷ integer promotes both sides to `Float64` (Spark's `/` is
/// always-double), and any numeric divisor is null-guarded (`x / 0` → NULL, non-ANSI).
fn rewrite_division(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(Transformed::no(expr));
    };
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        // Genuinely unresolvable operand: leave untouched. Defensive only — correlated / outer
        // refs are typed `OuterReferenceColumn` and DO resolve here (see the module docs), so
        // this bail is not reached by a valid analyzed plan today.
        return Ok(Transformed::no(Expr::BinaryExpr(binary)));
    };
    // Spark keeps decimal `/` in decimal (its own result-type rule) and promotes every other
    // numeric `/` to double; so we cast to Float64 **only** when BOTH operands are integers —
    // never when either is decimal (else `int / decimal`, which Spark keeps decimal, would widen
    // to double). This is why the rewrite reads both operand types rather than casting blindly.
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
    let right = guard_zero_divisor(right, &divisor_type)?;
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Divide,
        Box::new(right),
    ))))
}

/// `a % b` → Spark modulo: the divisor is null-guarded (`x % 0` → NULL, non-ANSI). Operand
/// types are untouched — Spark's `%` keeps the coerced operand type.
fn rewrite_modulo(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(Transformed::no(expr));
    };
    let Ok(divisor_type) = binary.right.get_type(schema) else {
        // Defensive bail (see the module docs / `rewrite_division`); not reached today. Even if it
        // were, Spark `%` keeps the operand type, so leaving DataFusion's `Modulo` in place is the
        // benign choice — only the `nullif` zero-guard (a nullability nicety) would be skipped.
        return Ok(Transformed::no(Expr::BinaryExpr(binary)));
    };
    let right = guard_zero_divisor(*binary.right, &divisor_type)?;
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        binary.left,
        Operator::Modulo,
        Box::new(right),
    ))))
}

/// Wrap a numeric divisor in `nullif(divisor, 0)` so a zero divisor yields NULL (Spark
/// non-ANSI). Applied even to provably nonzero literals: Spark's `Divide`/`Remainder` are
/// *always nullable*, so the guard also reproduces Spark's result-schema nullability (constant
/// folding erases the runtime cost). Non-numeric divisors (intervals) pass through, as does a
/// divisor that is already a `nullif(_, 0)` guard — the idempotency case: the analyzer runs
/// once eagerly on the passthrough plan and again at physical planning.
fn guard_zero_divisor(divisor: Expr, divisor_type: &DataType) -> Result<Expr> {
    if !divisor_type.is_numeric() || is_zero_guard(&divisor) {
        return Ok(divisor);
    }
    let zero = ScalarValue::new_zero(divisor_type)?;
    Ok(nullif(divisor, lit(zero)))
}

/// True when `expr` is already `nullif(_, <zero literal>)` — either this rule's own guard from
/// an earlier analyzer run, or the user's hand-written equivalent (identical semantics).
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

/// `array_element(arr, i)` (the planner's lowering of `arr[i]`) → Spark's 0-based `[]` via the
/// embedded `__repark_array_get__` UDF (negative and out-of-range indices yield NULL, Spark
/// non-ANSI; DataFusion's own negative-from-end behaviour is deliberately unreachable through
/// this spelling). The replacement UDF has a different name, so re-analysis never re-shifts.
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

/// The element type of a list-shaped `DataType`, or `None` when it isn't one (a map subscript
/// lowers to `get_field`, never here — but stay total).
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
    use datafusion::prelude::SessionContext;

    /// A context with the rule installed — the same wiring the session performs.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
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

    /// Spark `/` on integers is always-double true division — the audit's S0 (`5/2` was `2`).
    #[tokio::test]
    async fn integer_division_is_double() {
        let ctx = ctx();
        assert_eq!(f64_column(&ctx, "SELECT 5/2").await, vec![Some(2.5)]);
        assert_eq!(f64_column(&ctx, "SELECT 7/2").await, vec![Some(3.5)]);
        assert_eq!(f64_column(&ctx, "SELECT -7/2").await, vec![Some(-3.5)]);
    }

    /// Division by zero yields NULL for every numeric type (Spark non-ANSI) — never an error
    /// (DataFusion's integer behaviour) and never `inf` (its float behaviour).
    #[tokio::test]
    async fn division_by_zero_is_null() {
        let ctx = ctx();
        assert_eq!(f64_column(&ctx, "SELECT 1/0").await, vec![None]);
        assert_eq!(f64_column(&ctx, "SELECT 1.0/0.0").await, vec![None]);
        assert_eq!(
            f64_column(&ctx, "SELECT CAST(1 AS DOUBLE)/CAST(0 AS DOUBLE)").await,
            vec![None]
        );
        // A column divisor (not a literal) carrying a zero row: a=1 divides by 0 → NULL.
        assert_eq!(
            f64_column(
                &ctx,
                "SELECT a / b FROM (VALUES (1, 0), (9, 3)) AS t(a, b) ORDER BY a"
            )
            .await,
            vec![None, Some(3.0)]
        );
    }

    /// Modulo by zero yields NULL (Spark non-ANSI); nonzero modulo keeps the integer type.
    #[tokio::test]
    async fn modulo_by_zero_is_null_and_keeps_type() {
        let ctx = ctx();
        assert_eq!(i64_column(&ctx, "SELECT 5 % 0").await, vec![None]);
        assert_eq!(i64_column(&ctx, "SELECT 7 % 3").await, vec![Some(1)]);
        assert_eq!(f64_column(&ctx, "SELECT 5.0 % 0.0").await, vec![None]);
    }

    /// Decimal ÷ decimal stays decimal — the integer promotion must not touch it (Spark keeps
    /// decimal division in decimal; its precision rules are tracked separately).
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

    /// The first output column's data type — used to pin the result-type *class* of `/`.
    async fn result_type(ctx: &SessionContext, sql: &str) -> DataType {
        batch(ctx, sql).await.schema().field(0).data_type().clone()
    }

    /// BUG-004 closure (Group L 2026-07-23). A correlated / outer-reference `int / int` is NOT
    /// left as DataFusion integer truncation: the outer column arrives as a typed
    /// `OuterReferenceColumn`, so `get_type` resolves and the division is rewritten to Spark
    /// true-division — the outer-ref divisor is promoted to `Float64` and null-guarded, exactly
    /// like the non-correlated case. Asserted on the analyzed plan because DataFusion cannot
    /// physically execute a correlated scalar subquery. (Reverting the integer-promotion branch
    /// drops the `Float64` cast, reddening this pin — the divergence the audit feared.)
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
        // The outer-ref divisor resolved (no `get_type` bail) AND was promoted to Float64 true
        // division with the zero-divisor guard — the same rewrite as a non-correlated `int / int`.
        assert!(
            rendered.contains("nullif(CAST(outer_ref(o.a) AS Float64), Float64(0))"),
            "correlated outer-ref divisor must be promoted to Float64 and null-guarded \
             (no integer truncation); got:\n{rendered}"
        );
    }

    /// A `/` combining a scalar-subquery result with an outer integer column executes and matches
    /// live Spark 4.1.2 exactly: double true-division, zero divisor → NULL. Oracle (Group L
    /// 2026-07-23, `sum(b)=10`, `a ∈ {5,7,0}`): `[2.0, 1.4285714285714286, NULL]`.
    #[tokio::test]
    async fn division_over_subquery_and_column_is_double_matching_spark() {
        let ctx = ctx();
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

    /// Every non-decimal numeric `/` is double (Spark), whatever the integer width or whether an
    /// operand is already floating: bigint, smallint, and `DOUBLE` operands all yield `2.5`.
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

    /// Spark's `/` result type is decimal iff ≥1 operand is decimal AND none is float, else double
    /// (oracle 2026-07-23). We pin the result-type *class* on every mix. The exact decimal
    /// precision is a documented divergence — repark uses DataFusion's decimal-division rule
    /// (`decimal(10,2) / decimal(10,2)` → `Decimal128(16,6)` vs Spark `decimal(23,13)`;
    /// `int / decimal(10,2)` → `Decimal128(26,4)` vs Spark `decimal(14,11)`) — so it is
    /// deliberately NOT asserted here, keeping this a real cross-engine class pin rather than a
    /// repark-vs-repark tautology. (An unconditional Float64 cast — the rejected design-gate
    /// alternative — reddens the decimal arms by widening them to double.)
    #[tokio::test]
    async fn division_result_type_class_matches_spark_decimal_rule() {
        let ctx = ctx();
        // decimal present, no float → decimal (decimal absorbs integers).
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
        // any float present → double (float dominates decimal); all-integer → double.
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

    /// A narrower integer index reaches the embedded UDF un-coerced (the rewrite runs after
    /// `TypeCoercion`), so the invoke must cast it — `arr[CAST(0 AS INT)]` is an Int32 index.
    #[tokio::test]
    async fn array_subscript_accepts_narrow_integer_indices() {
        let ctx = ctx();
        assert_eq!(
            i64_column(&ctx, "SELECT [10, 20, 30][CAST(1 AS INT)]").await,
            vec![Some(20)]
        );
    }

    /// The `[]` subscript is Spark 0-based: `[0]` is the first element, out-of-range and
    /// negative indices are NULL (never DataFusion's negative-from-end).
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
        // ORDER BY idx ascending: -1, 0, 1, 2, 3.
        assert_eq!(
            values,
            vec![None, Some(10), Some(20), Some(30), None],
            "Spark []: 0-based, invalid index → NULL"
        );
    }

    /// A map subscript lowers to `get_field`, not `array_element` — the rewrite must not
    /// touch it.
    #[tokio::test]
    async fn map_subscript_is_untouched() {
        let ctx = ctx();
        let values = i64_column(&ctx, "SELECT map(['k'], [7])['k']").await;
        assert_eq!(values, vec![Some(7)]);
    }

    /// The rewrite preserves the un-rewritten output column name (`NamePreserver`): a bare
    /// `SELECT a / b` keeps its `a / b`-derived name instead of leaking the injected casts.
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

    /// Spark `overlay(..., -1)` uses replace-length (same as 3-arg); DF remainder is wrong.
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
        // Explicit positive len still replaces that many chars.
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
}
