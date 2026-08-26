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
            // SQP-1: `CAST(x AS BINARY)` becomes `CAST(x AS BYTEA)` so DataFusion plans it to
            // Arrow `Binary` — `BINARY` alone is `Unsupported SQL type` at planning. The DDL path
            // (`create_table.rs`) is intercepted before this parse, so `b BINARY` columns keep
            // `BINARY`. Type legality is checked on the planned tree below.
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
    // SQP-1: refuse `CAST(x AS BINARY)` where Spark refuses — a numeric / bool / date / decimal
    // source. This runs on the planned tree because a bare `CAST(c AS BINARY)` over a STRING
    // column is legal (B13) and only the resolved input type tells the two apart. DataFusion
    // would otherwise SILENTLY cast an integer to bytes (a wrong answer Spark analysis-refuses),
    // or fail decimal/bool/date with an opaque optimizer error — so the refuse is repark's, in
    // Spark's `DATATYPE_MISMATCH` words. Runs before the eager analyze/optimize so the clean
    // message wins.
    refuse_illegal_binary_cast(&plan)?;
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
        // Keep the BINARY→BYTEA rewrite in lockstep with the primary parse: this re-parse starts
        // from the original SQL text, so a statement carrying both a bare RANGE bound and a
        // BINARY cast would otherwise reach planning with `BINARY` and fail.
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

/// ===========================================================================================
/// Rewrite every `CAST(x AS BINARY)` / `TRY_CAST(x AS BINARY)` / `x::BINARY` target to `BYTEA`
/// (SQP-1). sqlparser parses `BINARY` to [`DataType::Binary`], which DataFusion's planner rejects
/// as `Unsupported SQL type BINARY`; `BYTEA` plans to Arrow `Binary`, the type Spark's `BINARY`
/// is. `VARBINARY` is deliberately left alone — Spark rejects it too (`UNSUPPORTED_DATATYPE`,
/// B12). The DDL `BINARY` column path is intercepted before this parse, so it never reaches here.
/// ===========================================================================================
fn rewrite_binary_casts(statement: &mut Statement) {
    let mut visitor = BinaryCastToBytea;
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = statement.visit(&mut visitor);
}

/// Rewrites a cast target of `BINARY` to `BYTEA`, at every cast kind (`CAST`, `TRY_CAST`,
/// `SAFE_CAST`, `::`) and every nesting depth.
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

/// ===========================================================================================
/// Refuse a cast to Arrow `Binary` whose input type Spark refuses (SQP-1 / B2–B7).
///
/// Spark allows only STRING / BINARY / NULL → BINARY; every other source (INT, BIGINT, DECIMAL,
/// BOOLEAN, DATE, …) is an analysis error. After [`rewrite_binary_casts`], DataFusion would
/// otherwise **silently** cast an integer to bytes — a wrong answer, not a refusal — so the check
/// is repark's. It runs on the planned tree because the input type is only known after name
/// resolution: `CAST(c AS BINARY)` over a STRING column is legal (B13), over an INT column is not,
/// and the two are the same parse tree. The walk reuses the `insert_overwrite` cast-walk shape
/// (every expression position, subqueries included) so no cast hides in a predicate or aggregate.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] carrying Spark's `DATATYPE_MISMATCH` condition and the source type
/// name — `CAST_WITH_CONF_SUGGESTION` for an integer (ANSI-off would allow it, B11), else
/// `CAST_WITHOUT_SUGGESTION`. A `Plan` error with a bracketed Spark class folds to
/// `AnalysisException` at the PyO3 boundary, the class Spark raises.
fn refuse_illegal_binary_cast(plan: &LogicalPlan) -> Result<()> {
    match find_illegal_binary_cast(plan) {
        Some(source) => Err(illegal_binary_cast_error(&source)),
        None => Ok(()),
    }
}

/// The input type of the first cast-to-`Binary` in `plan` whose source Spark refuses, or `None`.
fn find_illegal_binary_cast(plan: &LogicalPlan) -> Option<ArrowDataType> {
    let mut offender = None;
    let _ = plan.apply(|node| {
        let schema = crate::insert_overwrite::expr_typing_schema(node);
        let _ = node.apply_expressions(|expr| {
            if let Some(source) = expr_illegal_binary_cast_source(expr, schema.as_ref()) {
                offender = Some(source);
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

/// The source type of an illegal `→ Binary` cast inside `expr` (or a subquery hanging off it), or
/// `None`. Subquery plans hang off the expression, not off [`LogicalPlan`] children, so they are
/// recursed into explicitly — the same reason [`crate::insert_overwrite`] does.
fn expr_illegal_binary_cast_source(
    expr: &DataFusionExpr,
    schema: &DFSchema,
) -> Option<ArrowDataType> {
    let mut offender = None;
    let _ = expr.apply(|node| {
        let cast_input = match node {
            DataFusionExpr::Cast(cast) => Some((cast.expr.as_ref(), cast.field.data_type())),
            DataFusionExpr::TryCast(cast) => Some((cast.expr.as_ref(), cast.field.data_type())),
            _ => None,
        };
        if let Some((input, &ArrowDataType::Binary)) = cast_input
            && let Ok(source) = input.get_type(schema)
            && !is_binary_castable_source(&source)
        {
            offender = Some(source);
            return Ok(TreeNodeRecursion::Stop);
        }
        if let DataFusionExpr::ScalarSubquery(subquery)
        | DataFusionExpr::Exists(Exists { subquery, .. })
        | DataFusionExpr::InSubquery(InSubquery { subquery, .. }) = node
            && let Some(source) = find_illegal_binary_cast(&subquery.subquery)
        {
            offender = Some(source);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    offender
}

/// True for the source types Spark allows to cast to `BINARY`: the string family, the binary
/// family (a re-cast of a binary value, B8), and `NULL` (B8). Everything else refuses.
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

/// Build Spark's refusal for an illegal `→ BINARY` cast, naming the source type. Integer sources
/// quote `CAST_WITH_CONF_SUGGESTION` and Spark's "with ANSI mode on" clause (turning ANSI off
/// would big-endian-encode the int — B11, tabled); every other source quotes
/// `CAST_WITHOUT_SUGGESTION`. Recorded from the live oracle (B2 / B4 `<pyspark-4.1.2-oracle>`).
fn illegal_binary_cast_error(source: &ArrowDataType) -> DataFusionError {
    let source_name = spark_source_type_name(source);
    if is_spark_integer(source) {
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

/// True for the Arrow integer types Spark names INT / BIGINT / … — the sources whose refusal
/// carries `CAST_WITH_CONF_SUGGESTION` (they cast under ANSI OFF).
fn is_spark_integer(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
    )
}

/// The Spark SQL type name a `→ BINARY` refusal quotes for `source`. Covers the types a cast can
/// realistically carry; an unlisted type falls back to Arrow's own spelling so the message is
/// never empty.
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
    /// twice, the divisor must not be double-guarded into a type error, and the SQP-1
    /// `BINARY`→`BYTEA` cast rewrite must plan to `Binary` under the same double analysis.
    ///
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
