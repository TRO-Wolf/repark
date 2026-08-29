//! Spark AST defaults for DataFusion passthrough statements.
//!
//! The passthrough injects Spark's `ASC NULLS FIRST` and `DESC NULLS LAST` defaults into every
//! unspecified order, including nested queries and window specifications. Explicit null placement
//! remains unchanged. The same boundary owns DML, collation, range-frame, and filesystem valves.

use std::ops::ControlFlow;

use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::Dialect;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{Expr as DataFusionExpr, ExprSchemable, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::parser::{ResetStatement, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{
    DataType, Expr, NamedWindowExpr, OrderByExpr, OrderByKind, Query, SetExpr, Statement, VisitMut,
    VisitorMut, WindowType,
};

use crate::{local_fs_ddl, window_range};
use repark_core::CatalogRegistry;

/// ===========================================================================================
/// Plan + execute one passthrough statement with Spark's ORDER BY null-placement defaults.
///
/// The Spark-parity replacement for `ctx.sql(sql)`: parse with the session dialect, apply AST
/// defaults, then plan and execute. DataFusion-native statements without a generic AST pass
/// through unchanged, subject to the SEC-02 local-filesystem gate.
///
/// The G3-E8 subquery-predicate valve attaches to this executing parse. Router and session
/// dialects can disagree, so an earlier parse cannot safely guard DML.
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
                    ctx,
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
                    ctx,
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
            // SQP-1: rewrite `CAST(x AS BINARY)` to `BYTEA` — `BINARY` alone is `Unsupported SQL
            // type` at planning. Type legality is checked on the planned tree below.
            rewrite_binary_casts(inner);
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
    // SQP-1: refuse an illegal `→ BINARY` cast before the eager analyze so repark's clean
    // `DATATYPE_MISMATCH` message wins over DataFusion's silent int→bytes / opaque optimizer error.
    refuse_illegal_binary_cast(&plan)?;
    // Refuse local CREATE EXTERNAL and COPY TO before eager execution unless explicitly allowed.
    local_fs_ddl::refuse_local_filesystem_plan(ctx, catalogs, &plan)?;
    // Apply the shared create guard to the plan the sink will register.
    repark_core::PreExecute::new(ctx, catalogs).guard(&plan)?;
    // Spark applies commands eagerly. Materialize DML and COPY once, while leaving queries lazy.
    let is_eager_command = matches!(&plan, LogicalPlan::Dml(_) | LogicalPlan::Copy(_));
    // Eager analysis exposes Spark-adjusted types to Arrow export and CTAS schema derivation.
    let plan = repark_functions::analyze_eagerly(&state, plan)?;
    let dataframe = ctx.execute_logical_plan(plan).await?;
    if !is_eager_command {
        return Ok(dataframe);
    }
    // Return materialized command results so later collection cannot re-run the write.
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
        // Keep the BINARY→BYTEA rewrite in lockstep: this re-parse starts from the original SQL,
        // so a statement with both a bare RANGE bound and a BINARY cast would else fail at planning.
        rewrite_binary_casts(inner);
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

/// Rewrite every `CAST(x AS BINARY)` / `TRY_CAST` / `::BINARY` target to `BYTEA` (SQP-1):
/// sqlparser's [`DataType::Binary`] is `Unsupported SQL type BINARY` at planning, while `BYTEA`
/// plans to the Arrow `Binary` that Spark's `BINARY` is. `VARBINARY` is left alone — Spark rejects
/// it too (`UNSUPPORTED_DATATYPE`, B12). The DDL `BINARY` column path is intercepted before this parse.
fn rewrite_binary_casts(statement: &mut Statement) {
    let mut visitor = BinaryCastToBytea;
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = statement.visit(&mut visitor);
}

/// Rewrites a `BINARY` cast target to `BYTEA` at every cast kind (`CAST`, `TRY_CAST`, `::`) and
/// every nesting depth.
struct BinaryCastToBytea;

impl VisitorMut for BinaryCastToBytea {
    type Break = std::convert::Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Cast { data_type, .. } = expr
            && matches!(data_type, DataType::Binary(_))
        {
            *data_type = DataType::Bytea;
        }
        ControlFlow::Continue(())
    }
}

/// Refuse a cast to Arrow `Binary` whose input type Spark refuses (SQP-1 / B2–B7). Spark allows
/// only STRING / BINARY / NULL → BINARY; every other source is an analysis error. After
/// [`rewrite_binary_casts`], DataFusion would otherwise **silently** cast an integer to bytes — a
/// wrong answer, not a refusal — so the check is repark's. It runs on the planned tree because the
/// input type is only known after name resolution: `CAST(c AS BINARY)` is legal over a STRING
/// column (B13), illegal over an INT column, and the two are the same parse tree. The walk reuses
/// the `insert_overwrite` cast-walk shape so no cast hides in a subquery, predicate or aggregate.
///
/// # Errors
/// [`DataFusionError::Plan`] carrying Spark's `DATATYPE_MISMATCH` and the source type name; it
/// folds to `AnalysisException` at the PyO3 boundary.
fn refuse_illegal_binary_cast(plan: &LogicalPlan) -> Result<()> {
    match find_illegal_binary_cast(plan) {
        Some(offender) => Err(illegal_binary_cast_error(&offender)),
        None => Ok(()),
    }
}

/// An illegal `→ BINARY` cast the walk found: the refused source type, and whether it was a
/// `TRY_CAST`. The kind changes the message — Spark quotes `CAST_WITH_CONF_SUGGESTION` only for a
/// plain `CAST` of an integer; `TRY_CAST` of any source quotes `CAST_WITHOUT_SUGGESTION` (measured
/// B2, `<pyspark-4.1.2-oracle>`).
struct IllegalBinaryCast {
    source: ArrowDataType,
    is_try_cast: bool,
}

/// The first cast-to-`Binary` in `plan` whose source Spark refuses, or `None`.
fn find_illegal_binary_cast(plan: &LogicalPlan) -> Option<IllegalBinaryCast> {
    let mut offender = None;
    let _ = plan.apply(|node| {
        let schema = crate::insert_overwrite::expr_typing_schema(node);
        let _ = node.apply_expressions(|expr| {
            if let Some(found) = expr_illegal_binary_cast_source(expr, schema.as_ref()) {
                offender = Some(found);
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if offender.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    offender
}

/// The illegal `→ Binary` cast inside `expr` (or a subquery hanging off it), or `None`. Subquery
/// plans hang off the expression, not off [`LogicalPlan`] children, so they are recursed into
/// explicitly — the same reason [`crate::insert_overwrite`] does.
fn expr_illegal_binary_cast_source(
    expr: &DataFusionExpr,
    schema: &DFSchema,
) -> Option<IllegalBinaryCast> {
    let mut offender = None;
    let _ = expr.apply(|node| {
        let cast_input = match node {
            DataFusionExpr::Cast(cast) => Some((cast.expr.as_ref(), cast.field.data_type(), false)),
            DataFusionExpr::TryCast(cast) => {
                Some((cast.expr.as_ref(), cast.field.data_type(), true))
            }
            _ => None,
        };
        if let Some((input, &ArrowDataType::Binary, is_try_cast)) = cast_input
            && let Ok(source) = input.get_type(schema)
            && !is_binary_castable_source(&source)
        {
            offender = Some(IllegalBinaryCast {
                source,
                is_try_cast,
            });
            return Ok(TreeNodeRecursion::Stop);
        }
        if let DataFusionExpr::ScalarSubquery(subquery)
        | DataFusionExpr::Exists(Exists { subquery, .. })
        | DataFusionExpr::InSubquery(InSubquery { subquery, .. }) = node
            && let Some(found) = find_illegal_binary_cast(&subquery.subquery)
        {
            offender = Some(found);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    offender
}

/// True for the source types Spark allows to cast to `BINARY`: the string family, the binary
/// family (re-cast of a binary value), and `NULL` (B8). Everything else refuses.
fn is_binary_castable_source(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Utf8View
            | ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::BinaryView
            | ArrowDataType::Null
    )
}

/// Build Spark's refusal for an illegal `→ BINARY` cast, naming the source type. A plain `CAST` of
/// an integer quotes `CAST_WITH_CONF_SUGGESTION` and the "with ANSI mode on" clause (ANSI-off would
/// big-endian-encode the int — B11, tabled); `TRY_CAST` of any source and a plain `CAST` of any
/// non-integer quote `CAST_WITHOUT_SUGGESTION` (measured `TRY_CAST(1 AS BINARY)`, B2).
fn illegal_binary_cast_error(offender: &IllegalBinaryCast) -> DataFusionError {
    let source_name = spark_source_type_name(&offender.source);
    if is_spark_integer(&offender.source) && !offender.is_try_cast {
        DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION] due to data type mismatch: cannot cast \
             \"{source_name}\" to \"BINARY\" with ANSI mode on. SQLSTATE: 42K09"
        ))
    } else {
        DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] due to data type mismatch: cannot cast \
             \"{source_name}\" to \"BINARY\". SQLSTATE: 42K09"
        ))
    }
}

/// The Arrow integer types (INT / BIGINT / …) whose `→ BINARY` refusal carries
/// `CAST_WITH_CONF_SUGGESTION` (they cast under ANSI OFF).
fn is_spark_integer(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
    )
}

/// The Spark SQL type name a `→ BINARY` refusal quotes for `source`. An unlisted type falls back
/// to Arrow's own spelling so the message is never empty.
fn spark_source_type_name(source: &ArrowDataType) -> String {
    match source {
        ArrowDataType::Int8 => "TINYINT".to_string(),
        ArrowDataType::Int16 => "SMALLINT".to_string(),
        ArrowDataType::Int32 => "INT".to_string(),
        ArrowDataType::Int64 => "BIGINT".to_string(),
        ArrowDataType::Float32 => "FLOAT".to_string(),
        ArrowDataType::Float64 => "DOUBLE".to_string(),
        ArrowDataType::Boolean => "BOOLEAN".to_string(),
        ArrowDataType::Date32 | ArrowDataType::Date64 => "DATE".to_string(),
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => format!("DECIMAL({precision},{scale})"),
        ArrowDataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        ArrowDataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, BinaryArray, Int32Array, Int64Array, RecordBatch, UInt64Array,
    };
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

    /// Spark defaults: ASC → NULLS FIRST and DESC → NULLS LAST.
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

    /// Under LIMIT the default determines which rows survive.
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
    /// twice, the divisor must not be double-guarded into a type error, and the SQP-1
    /// `BINARY`→`BYTEA` cast rewrite must plan to `Binary` under the same double analysis.
    /// pins: sqp-1-spark-string-literals/C-010
    #[tokio::test]
    async fn passthrough_rewrites_are_idempotent_across_reanalysis() {
        let (ctx, catalogs) = ctx();
        let batches = execute(
            &ctx,
            &catalogs,
            "SELECT [10, 20, 30][0] AS first, [10, 20, 30][-1] AS neg, 9/3 AS d, \
             CAST('ab' AS BINARY) AS b",
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
        let binary = batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("CAST(... AS BINARY) must plan to Arrow Binary across the double analysis");
        assert_eq!(binary.value(0), b"ab");
    }

    /// The passthrough evaluates `SELECT 5/2` as `2.5`, not integer `2`.
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
