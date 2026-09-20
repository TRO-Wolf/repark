//! Spark AST defaults for DataFusion passthrough statements.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::config::Dialect;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Cast, Expr as DataFusionExpr, ExprSchemable, LogicalPlan, WriteOp};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::parser::{ResetStatement, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{
    DataType, Expr, NamedWindowExpr, OrderByExpr, OrderByKind, Query, SetExpr, Statement, VisitMut,
    VisitorMut, WindowType,
};

use crate::{local_fs_ddl, window_range};
use repark_core::CatalogRegistry;
use repark_iceberg::write::insert_defaults;

/// Plan + execute one passthrough statement with Spark's ORDER BY null-placement defaults.
/// # Errors
/// Propagates parse, planning, and execution errors, plus the G3-E8 subquery-predicate refusal.
pub(crate) async fn execute_passthrough(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    execute_passthrough_inner(ctx, catalogs, sql, false)
        .await
        .map_err(crate::keyword_lower::map_door_keyword_errors)
}

pub(crate) async fn execute_insert_source(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    execute_passthrough_inner(ctx, catalogs, sql, true)
        .await
        .map_err(crate::keyword_lower::map_door_keyword_errors)
}

async fn execute_passthrough_inner(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    insert_source: bool,
) -> Result<DataFrame> {
    let state = ctx.state();
    let session_dialect = state.config().options().sql_parser.dialect;
    let dialect = crate::dialect_for_executing_parse(sql, session_dialect);
    // G15 type-position (`CAST(x AS STRING COLLATE name)`) fails `sql_to_statement`.
    crate::collation::refuse_type_position_collation_in_sql(sql)?;
    let mut statement = state.sql_to_statement(sql, &dialect)?;
    let mut may_have_bare_range_bound = false;
    let mut insert_columns: Option<Vec<String>> = None;
    let mut preloaded: Option<Box<iceberg::table::Table>> = None;
    let mut timestamp_cells = Vec::new();
    match &mut statement {
        DfStatement::Statement(inner) => {
            // G15 — collation at the EXECUTING parse (G3-E8 altitude).
            crate::refuse_collation_in_statement(inner)?;
            crate::refuse_declared_function_in_statement(inner)?;
            if let Some(done) = try_execute_identity_dml(ctx, catalogs, inner).await? {
                return Ok(done);
            }
            // G3-E8 — on the EXECUTING parse, before anything else touches the statement.
            crate::refuse_dml_subquery_predicate_in_statement(inner)?;
            apply_spark_order_by_defaults(inner);
            crate::time_window::wrap_time_window_grouping(inner)?;
            // SQP-1: rewrite `CAST` to `BYTEA`.
            rewrite_binary_casts(inner);
            crate::bare_unit::rewrite_bare_datetime_units(inner)?;
            crate::bare_nullary::demote_refusing_nullary_calls(inner);
            crate::keyword_lower::lower_spark_keywords(inner);
            // R1: DataFusion accepts only SingleQuotedString inside INTERVAL frame bounds.
            window_range::quote_unquoted_interval_range_bounds(inner);
            may_have_bare_range_bound = window_range::statement_has_bare_range_bound(inner);
            timestamp_cells = crate::insert_timestamp_ns::timestamp_typed_values_cells(inner);
            insert_columns = insert_defaults::insert_column_list(inner);
            preloaded = if let Some((catalog_name, ident)) = insert_defaults::insert_target(inner)
                && let Some(catalog) = catalogs.get(&catalog_name)
            {
                Box::pin(insert_defaults::rewrite_insert_markers(
                    catalog, &ident, inner,
                ))
                .await?
                .preloaded
                .map(Box::new)
            } else {
                None
            };
        }
        DfStatement::Explain(explain) => {
            if let DfStatement::Statement(inner) = explain.statement.as_mut() {
                crate::keyword_lower::lower_timestamp_ns_casts(inner.as_mut());
            }
        }
        DfStatement::Reset(ResetStatement::Variable(name)) => {
            crate::collation::refuse_collation_reset_variable(&name.to_string())?;
        }
        _ => {}
    }
    let plan = repark_core::column_resolution::plan_statement_with_column_repair(
        &state,
        statement,
        crate::spark_door_case_insensitive(state.config().options()),
    )
    .await?;
    // G5b: a unit-less RANGE offset over datetime is Spark refusal or DAYS, never silent MONTHS.
    let plan = if may_have_bare_range_bound {
        conform_temporal_range_frames(&state, sql, &dialect, plan).await?
    } else {
        plan
    };
    let plan = crate::insert_timestamp_ns::before_analysis(plan, &timestamp_cells)?;
    // Refuse local CREATE EXTERNAL and COPY TO before eager execution unless explicitly allowed.
    local_fs_ddl::refuse_local_filesystem_plan(ctx, catalogs, &plan)?;
    // Apply the shared create guard to the plan the sink will register.
    repark_core::PreExecute::new(ctx, catalogs).guard(&plan)?;
    // Spark applies commands eagerly.
    let is_eager_command = matches!(&plan, LogicalPlan::Dml(_) | LogicalPlan::Copy(_));
    // Eager analysis exposes Spark-adjusted types to Arrow export and CTAS schema derivation.
    let plan = repark_functions::analyze_eagerly(&state, plan)?;
    let plan = conform_insert_narrowed_ints(ctx, plan).await?;
    let target = insert_defaults::dml_target(&plan)
        .and_then(|(name, ident)| catalogs.get(&name).map(|catalog| (catalog, ident)));
    let plan = match target {
        Some((catalog, ident)) => {
            Box::pin(insert_defaults::fill_insert_plan(
                catalog,
                &ident,
                insert_columns.as_deref(),
                plan,
                preloaded.map(|table| *table),
            ))
            .await?
        }
        None => plan,
    };
    let plan = crate::insert_timestamp_ns::after_analysis(plan)?;
    if insert_source {
        return ctx.execute_logical_plan(insert_input(&plan)?).await;
    }
    let dataframe = ctx.execute_logical_plan(plan).await?;
    if !is_eager_command {
        return Ok(dataframe);
    }
    // Return materialized command results so later collection cannot re-run the write.
    let batches = dataframe.collect().await?;
    ctx.read_batches(batches)
}

async fn conform_insert_narrowed_ints(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<LogicalPlan> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(plan);
    };
    if !matches!(dml.op, WriteOp::Insert(_)) {
        return Ok(LogicalPlan::Dml(dml));
    }
    let input = Arc::clone(&dml.input);
    let LogicalPlan::Projection(projection) = input.as_ref() else {
        return Ok(LogicalPlan::Dml(dml));
    };
    let Ok(provider) = ctx.table_provider(dml.table_name.clone()).await else {
        return Ok(LogicalPlan::Dml(dml));
    };
    let target = provider.schema();
    if target.fields().len() != projection.expr.len() {
        return Ok(LogicalPlan::Dml(dml));
    }
    let input_schema = projection.input.schema();
    let mut exprs: Vec<DataFusionExpr> = Vec::with_capacity(projection.expr.len());
    let mut changed = false;
    for (expr, target_field) in projection.expr.iter().zip(target.fields()) {
        let Ok(source_type) = expr.get_type(input_schema.as_ref()) else {
            exprs.push(expr.clone());
            continue;
        };
        if source_type == ArrowDataType::Int32 && *target_field.data_type() == ArrowDataType::Int64
        {
            let (inner, name) = match expr {
                DataFusionExpr::Alias(Alias { expr, name, .. }) => {
                    (expr.as_ref().clone(), name.clone())
                }
                other => (other.clone(), target_field.name().clone()),
            };
            let cast = DataFusionExpr::Cast(Cast::new(Box::new(inner), ArrowDataType::Int64));
            exprs.push(cast.alias(name));
            changed = true;
        } else {
            exprs.push(expr.clone());
        }
    }
    if !changed {
        return Ok(LogicalPlan::Dml(dml));
    }
    let conformed = LogicalPlan::Projection(datafusion::logical_expr::Projection::try_new(
        exprs,
        Arc::clone(&projection.input),
    )?);
    Ok(LogicalPlan::Dml(datafusion::logical_expr::DmlStatement {
        input: Arc::new(conformed),
        ..dml
    }))
}

fn insert_input(plan: &LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Insert(_)) => {
            Ok(dml.input.as_ref().clone())
        }
        other => Err(DataFusionError::Plan(format!(
            "INSERT with write options planned a `{}`, not an insert",
            other.display()
        ))),
    }
}

async fn try_execute_identity_dml(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    inner: &Statement,
) -> Result<Option<DataFrame>> {
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let (mut allowed, kind, object_name) = if let Some(allowed) =
        repark_iceberg::write::predicate_dml::try_allowed_delete_in(inner)?
    {
        let object_name = match inner {
            Statement::Delete(delete) => crate::delete_target_object_name(delete),
            _ => None,
        };
        (allowed, crate::MorDmlKind::Delete, object_name)
    } else if let Some(allowed) =
        repark_iceberg::write::predicate_dml::try_allowed_update_in(inner)?
    {
        let object_name = match inner {
            Statement::Update(update) => crate::object_name_from_table_with_joins(&update.table),
            _ => None,
        };
        (allowed, crate::MorDmlKind::Update, object_name)
    } else if let Some(allowed) =
        repark_iceberg::write::predicate_dml::plain::try_allowed_plain_identity_or_update(inner)?
    {
        if catalogs.get(&allowed.catalog_name).is_none() {
            return Ok(None);
        }
        let handle = crate::catalog_handle(catalogs, &allowed.catalog_name)?;
        if repark_iceberg::write::predicate_dml::plain::plain_identity_needs_fork(
            handle,
            &allowed.spec,
        )
        .await
        {
            return Ok(None);
        }
        let object_name = match inner {
            Statement::Delete(delete) => crate::delete_target_object_name(delete),
            Statement::Update(update) => crate::object_name_from_table_with_joins(&update.table),
            _ => None,
        };
        let kind = if allowed.spec.assignments.is_some() {
            crate::MorDmlKind::Update
        } else {
            crate::MorDmlKind::Delete
        };
        (allowed, kind, object_name)
    } else {
        return Ok(None);
    };
    allowed.spec.case_insensitive = case_insensitive;
    if case_insensitive {
        canonicalize_identity_selection(catalogs, &allowed.catalog_name, &mut allowed.spec).await?;
    }
    crate::refuse_mor_unpartitioned_multi_spec_dml(ctx, catalogs, object_name, kind).await?;
    let handle = crate::catalog_handle(catalogs, &allowed.catalog_name)?;
    repark_iceberg::write::predicate_dml::execute_predicate_dml(ctx, handle, &allowed.spec).await?;
    Ok(Some(ctx.read_empty()?))
}

async fn canonicalize_identity_selection(
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    spec: &mut repark_iceberg::write::predicate_dml::PredicateDmlSpec,
) -> Result<()> {
    let handle = crate::catalog_handle(catalogs, catalog_name)?;
    let table = handle
        .load_table(&spec.target)
        .await
        .map_err(crate::iceberg_err)?;
    let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(crate::iceberg_err)?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let scopes = [(spec.target_alias.as_str(), fields.as_slice())];
    let home = Some(spec.target_alias.as_str());
    spec.selection_sql = repark_core::column_resolution::rewrite_fragment_case(
        &spec.selection_sql,
        &scopes,
        true,
        home,
    )?;
    if let Some(assignments) = spec.assignments.as_mut() {
        for (target, value) in assignments {
            *target =
                repark_core::column_resolution::rewrite_fragment_case(target, &scopes, true, home)?;
            *value =
                repark_core::column_resolution::rewrite_fragment_case(value, &scopes, true, home)?;
        }
    }
    Ok(())
}

/// Apply Spark's bare-`RANGE`-offset rules to a freshly-planned statement (G5b).
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
async fn restate_range_frames_and_replan(
    state: &SessionState,
    sql: &str,
    dialect: &Dialect,
    rewrite: impl FnOnce(&mut Statement),
) -> Result<LogicalPlan> {
    let mut restated = state.sql_to_statement(sql, dialect)?;
    if let DfStatement::Statement(inner) = &mut restated {
        apply_spark_order_by_defaults(inner);
        // Keep the BINARY→BYTEA rewrite in lockstep: this re-parse starts from the original SQL.
        rewrite_binary_casts(inner);
        crate::bare_unit::rewrite_bare_datetime_units(inner)?;
        crate::bare_nullary::demote_refusing_nullary_calls(inner);
        crate::keyword_lower::lower_spark_keywords(inner);
        window_range::quote_unquoted_interval_range_bounds(inner);
        rewrite(inner);
    }
    repark_core::column_resolution::plan_statement_with_column_repair(
        state,
        restated,
        crate::spark_door_case_insensitive(state.config().options()),
    )
    .await
}

/// Inject Spark null-placement defaults into every ORDER BY whose placement is unspecified.
pub(crate) fn apply_spark_order_by_defaults(statement: &mut Statement) {
    let mut visitor = OrderByDefaults;
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = statement.visit(&mut visitor);
}

/// The visitor covers query-level ORDER BY, named WINDOW clauses, and inline OVER ORDER BY.
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

/// Fix the named `WINDOW w AS (…)` clauses of the `SELECT` nodes in a query body.
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

/// Spark's default: ascending → NULLS FIRST, descending → NULLS LAST.
fn apply_default(order_by: &mut OrderByExpr) {
    if order_by.options.nulls_first.is_none() {
        order_by.options.nulls_first = Some(order_by.options.asc.unwrap_or(true));
    }
}

/// Rewrite every `CAST` / `TRY_CAST` / `::BINARY` target to `BYTEA`.
fn rewrite_binary_casts(statement: &mut Statement) {
    let mut visitor = BinaryCastToBytea;
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = statement.visit(&mut visitor);
}

/// Rewrites a `BINARY` cast target to `BYTEA` at every cast kind and every nesting depth.
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, BinaryArray, Int32Array, RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    use repark_core::CatalogRegistry;

    use crate::execute;

    /// A context whose `v` table carries.
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

    /// The default reaches subqueries and window `OVER` specs.
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

    /// pins: sqp-1-spark-string-literals/C-010
    /// The rewrites survive the double analyzer run this path creates.
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
            .downcast_ref::<Int32Array>()
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
