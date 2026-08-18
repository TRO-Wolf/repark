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

use datafusion::config::Dialect;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::parser::{ResetStatement, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{
    Expr, NamedWindowExpr, OrderByExpr, OrderByKind, Query, SetExpr, Statement, VisitMut,
    VisitorMut, WindowType,
};

use crate::{local_fs_ddl, window_range};
use repark_core::CatalogRegistry;

/// ===========================================================================================
/// Plan + execute one passthrough statement with Spark's ORDER BY null-placement defaults.
///
/// The Spark-parity replacement for `ctx.sql(sql)` on every path the router does not
/// intercept: parse with the session dialect (exactly as `ctx.sql` does), apply the AST
/// defaults, then plan and execute. DataFusion-native statements that have no generic AST
/// (`COPY`, `CREATE EXTERNAL TABLE`) carry no Spark ORDER BY surface and pass through
/// unchanged — subject to the SEC-02 local-filesystem DDL gate (see [`local_fs_ddl`]).
///
/// **This is also where the G3-E8 subquery-predicate valve attaches** (F-A). Every route into
/// DataFusion's DML — the router's `DELETE`/`UPDATE` arms, the `_ =>` arm, and
/// `execute_unparsable_fallthrough` — lands here, and the statement below is the one that will
/// actually be planned. A guard wired to the router's own `DatabricksDialect` parse is fail-OPEN
/// for any form the two parsers disagree about: Spark's FROM-less `DELETE <table> WHERE …` fails
/// the router parse, falls through, is re-parsed here under the session dialect, and — before
/// this call — emptied the table. Attach DML guards at THIS parse.
/// ===========================================================================================
///
/// # Errors
/// Propagates parse, planning, and execution errors as [`datafusion::error::DataFusionError`],
/// plus the G3-E8 refusal for a `DELETE`/`UPDATE` carrying a subquery predicate.
pub(crate) async fn execute_passthrough(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    // G15 type-position (`CAST(x AS STRING COLLATE name)`) fails `sql_to_statement`.
    // Scan the executing-parse text first so that spelling is G15, not ParserError.
    crate::collation::refuse_type_position_collation_in_sql(sql)?;
    let mut statement = state.sql_to_statement(sql, &dialect)?;
    let mut may_have_bare_range_bound = false;
    match &mut statement {
        DfStatement::Statement(inner) => {
            // G15 — collation at the EXECUTING parse (G3-E8 altitude). A COLLATE
            // spelling must not reach DataFusion's unsupported-AST path or be
            // silently dropped. Router-parsable SELECT/ORDER BY still land here
            // when tests call execute_passthrough directly (Q-001 pin).
            crate::refuse_collation_in_statement(inner)?;
            // G3-E8 identity path — allow-listed DELETE / UPDATE via
            // try_allowed_delete_in / try_allowed_update_in → execute_predicate_dml
            // (attach is spelling-generic; the allow-list exports uncorrelated IN /
            // NOT IN, [NOT] EXISTS ± correlation, correlated IN, and identity
            // UPDATE … SET <scalar> WHERE col IN). Fail-closed: every other
            // subquery spelling still hits the valve below (never DataFusion DML).
            if let Some(allowed) =
                repark_iceberg::write::predicate_dml::try_allowed_delete_in(inner)?
            {
                let object_name = match inner.as_ref() {
                    Statement::Delete(delete) => crate::delete_target_object_name(delete),
                    _ => None,
                };
                crate::refuse_mor_unpartitioned_multi_spec_dml(
                    catalogs,
                    object_name,
                    crate::MorDmlKind::Delete,
                )
                .await?;
                let handle = crate::catalog_handle(catalogs, &allowed.catalog_name)?;
                repark_iceberg::write::predicate_dml::execute_predicate_dml(
                    ctx,
                    handle,
                    &allowed.spec,
                )
                .await?;
                return ctx.read_empty();
            }
            if let Some(allowed) =
                repark_iceberg::write::predicate_dml::try_allowed_update_in(inner)?
            {
                let object_name = match inner.as_ref() {
                    Statement::Update(update) => {
                        crate::object_name_from_table_with_joins(&update.table)
                    }
                    _ => None,
                };
                crate::refuse_mor_unpartitioned_multi_spec_dml(
                    catalogs,
                    object_name,
                    crate::MorDmlKind::Update,
                )
                .await?;
                let handle = crate::catalog_handle(catalogs, &allowed.catalog_name)?;
                repark_iceberg::write::predicate_dml::execute_predicate_dml(
                    ctx,
                    handle,
                    &allowed.spec,
                )
                .await?;
                return ctx.read_empty();
            }
            // G3-E8 — on the EXECUTING parse, before anything else touches the statement.
            crate::refuse_dml_subquery_predicate_in_statement(inner)?;
            apply_spark_order_by_defaults(inner);
            // R1: DataFusion's convert_frame_bound_to_scalar_value accepts only
            // SingleQuotedString inside INTERVAL. Quote `INTERVAL 1 DAY` before first plan.
            window_range::quote_unquoted_interval_range_bounds(inner);
            may_have_bare_range_bound = window_range::statement_has_bare_range_bound(inner);
        }
        DfStatement::Reset(ResetStatement::Variable(name)) => {
            crate::collation::refuse_collation_reset_variable(&name.to_string())?;
        }
        _ => {}
    }
    let plan = state.statement_to_plan(statement).await?;
    // G5b: a unit-less `RANGE` offset over a datetime order key is Spark's refusal (TIMESTAMP)
    // or means DAYS (DATE), never DataFusion's silent MONTHS. The AST probe above keeps every
    // statement without such a bound — effectively all of them — on the single-plan path.
    let plan = if may_have_bare_range_bound {
        conform_temporal_range_frames(&state, sql, &dialect, plan).await?
    } else {
        plan
    };
    // r24 SB1 / SEC-02: refuse local CREATE EXTERNAL / COPY TO when conf is false (warehouse
    // grandfather still allowed). Must run before eager collect so a blocked COPY never writes.
    local_fs_ddl::refuse_local_filesystem_plan(ctx, catalogs, &plan)?;
    // SE-1 D1 round 4 (Y-3 / Y-4), re-routed in round 5 (Z-2): `CREATE VIEW cat.ns.v AS …` and
    // `SELECT … INTO cat.ns.t` reach the router's `_ =>` catch-all, so the CTAS tighten refuse
    // never sees them — while the Iceberg schema provider's `register_table` sink persists a
    // real table (measured). The refuse now lives in the shared belt's `guard`, which this door
    // calls on the plan the sink will actually register (the Spark door keeps its own
    // plan/execute halves — AST rewrites, temporal range conform, the eager-command fold).
    repark_core::PreExecute::new(ctx, catalogs).guard(&plan)?;
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

/// ===========================================================================================
/// Apply Spark's bare-`RANGE`-offset rules to a freshly-planned statement (G5b).
///
/// Refuses the TIMESTAMP arm on the planned tree; for the DATE arm restates the offsets as
/// `INTERVAL '<n>' DAY` in the AST and re-plans, because a window expression's schema name
/// embeds its frame and an in-place plan rewrite would strand every parent column reference.
/// See [`window_range`] for the full rationale.
/// ===========================================================================================
///
/// # Errors
/// Propagates the Spark refusal, and any parse / planning error of the restated statement.
async fn conform_temporal_range_frames(
    state: &SessionState,
    sql: &str,
    dialect: &Dialect,
    plan: LogicalPlan,
) -> Result<LogicalPlan> {
    match window_range::classify_planned_range_frames(&plan)? {
        window_range::RangeFrameVerdict::Unchanged => Ok(plan),
        window_range::RangeFrameVerdict::RestateBareBoundsAsDays => {
            restate_range_frames_and_replan(
                state,
                sql,
                dialect,
                window_range::rewrite_bare_range_bounds_to_days,
            )
            .await
        }
        window_range::RangeFrameVerdict::RestateIntervalBoundsAsNumeric => {
            restate_range_frames_and_replan(
                state,
                sql,
                dialect,
                window_range::rewrite_interval_range_bounds_to_numeric,
            )
            .await
        }
    }
}

/// Re-parse, re-apply Spark ORDER BY defaults + R1 quoting, run `rewrite`, re-plan.
///
/// The restatement path starts from the original SQL text (window names embed the
/// frame), so unquoted `INTERVAL 1 DAY` must be quoted again before the second plan.
async fn restate_range_frames_and_replan(
    state: &SessionState,
    sql: &str,
    dialect: &Dialect,
    rewrite: impl FnOnce(&mut Statement),
) -> Result<LogicalPlan> {
    let mut restated = state.sql_to_statement(sql, dialect)?;
    if let DfStatement::Statement(inner) = &mut restated {
        apply_spark_order_by_defaults(inner);
        window_range::quote_unquoted_interval_range_bounds(inner);
        rewrite(inner);
    }
    state.statement_to_plan(restated).await
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
