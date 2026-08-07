//! Spark AST-level defaults for the DataFusion passthrough path.
//!
//! Spark and DataFusion disagree on where an `ORDER BY` puts NULLs **when the query does not
//! say**: Spark defaults to `ASC → NULLS FIRST` / `DESC → NULLS LAST`, DataFusion to the exact
//! opposite — so under a `LIMIT` the two engines return *different rows*, not just a different
//! order (audit AR-1 finding #4). The distinction between "user wrote `NULLS LAST`" and "parser
//! defaulted" only exists in the AST (`OrderByOptions::nulls_first: Option<bool>`); by logical
//! plan time it is baked in. So [`execute_passthrough`] re-parses the statement exactly as
//! `ctx.sql` would, injects Spark's defaults into every unspecified `ORDER BY` — top-level,
//! subqueries, set operations, window specs (inline `OVER (…)` and named `WINDOW` clauses) —
//! and only then plans it. Explicit `NULLS FIRST`/`NULLS LAST` is always honoured.

use std::ops::ControlFlow;

use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, NamedWindowExpr, OrderByExpr, OrderByKind, Query, SetExpr, Statement, VisitMut,
    VisitorMut, WindowType,
};

use crate::local_fs_ddl;
use repark_core::CatalogRegistry;

/// ===========================================================================================
/// Plan + execute one passthrough statement with Spark's ORDER BY null-placement defaults.
///
/// The Spark-parity replacement for `ctx.sql(sql)` on every path the router does not
/// intercept: parse with the session dialect (exactly as `ctx.sql` does), apply the AST
/// defaults, then plan and execute. DataFusion-native statements that have no generic AST
/// (`COPY`, `CREATE EXTERNAL TABLE`) carry no Spark ORDER BY surface and pass through
/// unchanged — subject to the SEC-02 local-filesystem DDL gate (see [`local_fs_ddl`]).
/// ===========================================================================================
///
/// # Errors
/// Propagates parse, planning, and execution errors as [`datafusion::error::DataFusionError`].
pub(crate) async fn execute_passthrough(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let mut statement = state.sql_to_statement(sql, &dialect)?;
    if let DfStatement::Statement(inner) = &mut statement {
        apply_spark_order_by_defaults(inner);
    }
    let plan = state.statement_to_plan(statement).await?;
    // r24 SB1 / SEC-02: refuse local CREATE EXTERNAL / COPY TO when conf is false (warehouse
    // grandfather still allowed). Must run before eager collect so a blocked COPY never writes.
    local_fs_ddl::refuse_local_filesystem_plan(ctx, catalogs, &plan)?;
    // PySpark applies a command (INSERT / UPDATE / DELETE, and `COPY … TO`) eagerly at `sql()`;
    // DataFusion plans it lazily — the write commits only when the returned DataFrame is collected —
    // so a bare `spark.sql("INSERT …")` / `spark.sql("COPY …")` a migrated caller never collects
    // silently loses the write (audit F-BR-2; `LogicalPlan::Copy` was the disclosed same-class
    // residual, task/lessons.md 2026-07-16). Detect the command shape on the freshly-planned
    // statement (before analysis, which never introduces or removes the `Dml` / `Copy` wrapper), so
    // the command can be applied here, once. A pure query (SELECT) is neither a `Dml` nor a `Copy`
    // plan and keeps its lazy DataFrame untouched — the N4 metadata-only path and WG-4's streaming
    // laziness both ride that unchanged lazy plan.
    let is_eager_command = matches!(&plan, LogicalPlan::Dml(_) | LogicalPlan::Copy(_));
    // Analyze eagerly: the Spark semantics rules can change expression types (int `/` →
    // double), and this DataFrame's logical schema feeds real consumers — the PyO3 Arrow
    // export and CTAS schema derivation — which must see the post-analysis types, not the raw
    // planner output (a mismatch reinterprets the collected buffers).
    let plan = repark_functions::analyze_eagerly(&state, plan)?;
    let dataframe = ctx.execute_logical_plan(plan).await?;
    if !is_eager_command {
        return Ok(dataframe);
    }
    // A command (DML or `COPY … TO`): apply it now — the Iceberg write / file sink commits on this
    // collect, exactly once — then re-wrap the already-computed affected-row `count` batches in an
    // in-memory DataFrame. A later `.collect()` on what we return re-reads those batches instead of
    // re-running the command, so the write is never applied twice (the trap the naive
    // eager-collect-but-return-the-same-plan fix creates). `read_batches` is empty-safe (a
    // `Schema::empty()` fallback); both `Dml` and `Copy` yield a single `count` batch, so the
    // returned DataFrame carries DataFusion's command shape.
    let batches = dataframe.collect().await?;
    ctx.read_batches(batches)
}

/// Inject Spark's null-placement defaults into every `ORDER BY` whose placement is unspecified,
/// across the whole statement (queries, subqueries, window specs).
pub(crate) fn apply_spark_order_by_defaults(statement: &mut Statement) {
    let mut visitor = OrderByDefaults;
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = statement.visit(&mut visitor);
}

/// The visitor: `post_visit_query` covers query-level `ORDER BY` and the named `WINDOW` clauses
/// of every SELECT in the query body; `post_visit_expr` covers inline `OVER (ORDER BY …)`.
struct OrderByDefaults;

impl VisitorMut for OrderByDefaults {
    type Break = std::convert::Infallible;

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(order_by) = &mut query.order_by
            && let OrderByKind::Expressions(expressions) = &mut order_by.kind
        {
            expressions.iter_mut().for_each(apply_default);
        }
        apply_to_set_expr(&mut query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = expr
            && let Some(WindowType::WindowSpec(spec)) = &mut function.over
        {
            spec.order_by.iter_mut().for_each(apply_default);
        }
        ControlFlow::Continue(())
    }
}

/// Fix the named `WINDOW w AS (…)` clauses of the `SELECT` nodes in a query body. Nested
/// `Query` nodes are handled by their own `post_visit_query` call; set operations are walked
/// to reach the `SELECT` nodes on either side.
fn apply_to_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            for window in &mut select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &mut window.1 {
                    spec.order_by.iter_mut().for_each(apply_default);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            apply_to_set_expr(left);
            apply_to_set_expr(right);
        }
        _ => {}
    }
}

/// Spark's default: ascending → NULLS FIRST, descending → NULLS LAST. Only fills the gap —
/// an explicit `NULLS FIRST`/`NULLS LAST` is untouched.
fn apply_default(order_by: &mut OrderByExpr) {
    if order_by.options.nulls_first.is_none() {
        order_by.options.nulls_first = Some(order_by.options.asc.unwrap_or(true));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    use repark_core::CatalogRegistry;

    use crate::execute;

    /// A context whose `v` table carries (2, NULL, 1) — the null-placement fixture — wired the
    /// way `repark-core` builds sessions (Spark analyzer rules installed).
    fn ctx() -> (SessionContext, CatalogRegistry) {
        let ctx = SessionContext::new();
        for rule in repark_functions::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(2), None, Some(1)]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        (ctx, CatalogRegistry::new())
    }

    async fn i32_rows(
        ctx: &SessionContext,
        catalogs: &CatalogRegistry,
        sql: &str,
    ) -> Vec<Option<i32>> {
        let batches = execute(ctx, catalogs, sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..column.len() {
                rows.push(column.is_valid(row).then(|| column.value(row)));
            }
        }
        rows
    }

    /// Spark defaults: ASC → NULLS FIRST, DESC → NULLS LAST (DataFusion's are the opposite —
    /// the audit proved the passthrough returned [1, 2, NULL] for ASC).
    #[tokio::test]
    async fn order_by_defaults_are_spark() {
        let (ctx, catalogs) = ctx();
        assert_eq!(
            i32_rows(&ctx, &catalogs, "SELECT v FROM t ORDER BY v").await,
            vec![None, Some(1), Some(2)]
        );
        assert_eq!(
            i32_rows(&ctx, &catalogs, "SELECT v FROM t ORDER BY v ASC").await,
            vec![None, Some(1), Some(2)]
        );
        assert_eq!(
            i32_rows(&ctx, &catalogs, "SELECT v FROM t ORDER BY v DESC").await,
            vec![Some(2), Some(1), None]
        );
    }

    /// An explicit NULLS FIRST / NULLS LAST always wins over the injected default.
    #[tokio::test]
    async fn explicit_null_placement_is_honoured() {
        let (ctx, catalogs) = ctx();
        assert_eq!(
            i32_rows(&ctx, &catalogs, "SELECT v FROM t ORDER BY v ASC NULLS LAST").await,
            vec![Some(1), Some(2), None]
        );
        assert_eq!(
            i32_rows(
                &ctx,
                &catalogs,
                "SELECT v FROM t ORDER BY v DESC NULLS FIRST"
            )
            .await,
            vec![None, Some(2), Some(1)]
        );
    }

    /// Under LIMIT the default changes *which rows* survive — the audit's data-changing case.
    #[tokio::test]
    async fn order_by_limit_returns_spark_rows() {
        let (ctx, catalogs) = ctx();
        assert_eq!(
            i32_rows(&ctx, &catalogs, "SELECT v FROM t ORDER BY v LIMIT 2").await,
            vec![None, Some(1)]
        );
    }

    /// The default reaches subqueries and window `OVER (ORDER BY …)` specs: with NULLS FIRST
    /// the NULL row takes `row_number` 1.
    #[tokio::test]
    async fn window_order_by_gets_spark_default() {
        let (ctx, catalogs) = ctx();
        let batches = execute(
            &ctx,
            &catalogs,
            "SELECT rn FROM (SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t) \
             WHERE v IS NULL",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        // DataFusion's raw row_number is UInt64 (the Int32 Spark cast is the facade's job).
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(column.value(0), 1, "the NULL row must rank first (Spark)");
    }

    /// The rewrites survive the double analyzer run this path creates (eager analysis here +
    /// physical planning's own) — the idempotency contract: the subscript must not shift
    /// twice, the divisor must not be double-guarded into a type error.
    #[tokio::test]
    async fn passthrough_rewrites_are_idempotent_across_reanalysis() {
        let (ctx, catalogs) = ctx();
        let batches = execute(
            &ctx,
            &catalogs,
            "SELECT [10, 20, 30][0] AS first, [10, 20, 30][-1] AS neg, 9/3 AS d",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let first = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(first.value(0), 10, "0-based, shifted exactly once");
        assert!(batches[0].column(1).is_null(0), "negative index is NULL");
        let division = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert!((division.value(0) - 3.0).abs() < f64::EPSILON);
    }

    /// The Spark division semantics ride the passthrough end to end (the audit's S0 through
    /// `execute` rather than a bare context): `SELECT 5/2` is 2.5, not 2.
    #[tokio::test]
    async fn passthrough_integer_division_is_double() {
        let (ctx, catalogs) = ctx();
        let batches = execute(&ctx, &catalogs, "SELECT 5/2 AS r")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert!((column.value(0) - 2.5).abs() < f64::EPSILON);
    }
}
