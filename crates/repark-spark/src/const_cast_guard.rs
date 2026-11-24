use std::collections::HashSet;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    Aggregate, Distinct, DistinctOn, Expr, Filter, Join, JoinType, Limit, LogicalPlan, Projection,
    Sort, TableScan, Union, Values, Window, WriteOp,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Query, Visit, Visitor};
use repark_core::CatalogRegistry;
use repark_iceberg::write::merge::{
    InsertAction, MatchedAction, MergeSpec, NotMatchedBySourceAction,
};
use repark_iceberg::write::predicate_dml::PredicateDmlSpec;

use crate::catalog_ops::namespace_schema_name;
use crate::spark_ast;
use crate::view_ddl::frame_settings::extension_settings;
use crate::view_ddl::temp_settings::TempViewSettings;
use crate::view_ddl::temp_view::plan_and_settings;

mod eval;
#[cfg(test)]
mod tests;

use eval::{EvalContext, eval_expr, has_spark_error_class};

const MAX_PLAN_DEPTH: usize = 48;
const MAX_BRANCH_ENVS: usize = 64;

type RowEnv = Vec<Option<ScalarValue>>;

pub(crate) fn validate_write_plan(ctx: &SessionContext, plan: &LogicalPlan) -> Result<()> {
    let options = ctx.state().config().options().clone();
    let target = unwrap_write_root(plan);
    let _ = walk_plan(target, &options, None, 0, false)?;
    Ok(())
}

pub(crate) fn validate_write_query<'a>(
    ctx: &'a SessionContext,
    catalogs: &'a CatalogRegistry,
    source: &'a Query,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let sql = source.to_string();
        match spark_ast::execute_passthrough(ctx, catalogs, &sql).await {
            Ok(frame) => validate_write_plan(ctx, frame.logical_plan()),
            Err(error) if is_cast_error(&error) => Err(error),
            Err(_) => Ok(()),
        }
    })
}

struct ExplicitCastFinder(bool);

impl Visitor for ExplicitCastFinder {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        match expr {
            SqlExpr::Cast { .. } => {
                self.0 = true;
                ControlFlow::Break(())
            }
            SqlExpr::Function(function) => {
                let name = function
                    .name
                    .0
                    .last()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if matches!(
                    name.as_str(),
                    "cast"
                        | "to_timestamp"
                        | "to_timestamp_ltz"
                        | "to_timestamp_ntz"
                        | "double"
                        | "float"
                        | "int"
                        | "integer"
                        | "bigint"
                        | "smallint"
                        | "tinyint"
                        | "decimal"
                        | "boolean"
                        | "date"
                        | "timestamp"
                        | "timestamp_ntz"
                ) {
                    self.0 = true;
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

pub(crate) fn sql_expr_has_explicit_cast(expr: &SqlExpr) -> bool {
    let mut finder = ExplicitCastFinder(false);
    let _ = expr.visit(&mut finder);
    finder.0
}

pub(crate) async fn validate_identity_dml_predicate(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    spec: &PredicateDmlSpec,
) -> Result<()> {
    let target_sql = qualified_table_sql(catalog_name, &spec.target);
    let alias = quote_backtick(&spec.target_alias);
    let probe = format!(
        "SELECT 1 FROM {target_sql} AS {alias} WHERE ({})",
        spec.selection_sql
    );
    validate_probe_sql(ctx, catalogs, &probe).await
}

pub(crate) fn validate_merge_spec<'a>(
    ctx: &'a SessionContext,
    catalogs: &'a CatalogRegistry,
    catalog_name: &'a str,
    spec: &'a MergeSpec,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(validate_merge_spec_inner(ctx, catalogs, catalog_name, spec))
}

async fn validate_merge_spec_inner(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    spec: &MergeSpec,
) -> Result<()> {
    let source_alias = quote_backtick(&spec.source_alias);
    let source_probe = format!("SELECT * FROM {} AS {source_alias}", spec.source_from_sql);
    validate_probe_sql(ctx, catalogs, &source_probe).await?;

    let target_sql = qualified_table_sql(catalog_name, &spec.target);
    let target_alias = quote_backtick(&spec.target_alias);
    let on_probe = format!(
        "SELECT 1 FROM {target_sql} AS {target_alias} LEFT JOIN {} AS {source_alias} ON ({})",
        spec.source_from_sql, spec.on_sql
    );
    validate_probe_sql(ctx, catalogs, &on_probe).await?;

    let joined_from = format!(
        "{target_sql} AS {target_alias} LEFT JOIN {} AS {source_alias} ON true",
        spec.source_from_sql
    );
    for clause in &spec.matched {
        if clause_predicate_is_const_false(
            ctx,
            catalogs,
            &joined_from,
            clause.predicate_sql.as_deref(),
        )
        .await?
        {
            continue;
        }
        if let MatchedAction::Update { assignments } = &clause.action {
            let exprs: Vec<&str> = assignments
                .iter()
                .map(|(_, value)| value.as_str())
                .filter(|value| !value.trim().eq_ignore_ascii_case("DEFAULT"))
                .collect();
            validate_merge_exprs(ctx, catalogs, &joined_from, &exprs).await?;
        }
    }
    for clause in &spec.not_matched {
        if clause_predicate_is_const_false(
            ctx,
            catalogs,
            &joined_from,
            clause.predicate_sql.as_deref(),
        )
        .await?
        {
            continue;
        }
        if let InsertAction::Explicit { values_sql, .. } = &clause.action {
            let exprs: Vec<&str> = values_sql
                .iter()
                .map(String::as_str)
                .filter(|value| !value.trim().eq_ignore_ascii_case("DEFAULT"))
                .collect();
            validate_merge_exprs(ctx, catalogs, &joined_from, &exprs).await?;
        }
    }
    let target_only_from = format!("{target_sql} AS {target_alias}");
    for clause in &spec.not_matched_by_source {
        if clause_predicate_is_const_false(
            ctx,
            catalogs,
            &target_only_from,
            clause.predicate_sql.as_deref(),
        )
        .await?
        {
            continue;
        }
        if let NotMatchedBySourceAction::Update { assignments } = &clause.action {
            let exprs: Vec<&str> = assignments
                .iter()
                .map(|(_, value)| value.as_str())
                .filter(|value| !value.trim().eq_ignore_ascii_case("DEFAULT"))
                .collect();
            validate_merge_exprs(ctx, catalogs, &target_only_from, &exprs).await?;
        }
    }
    Ok(())
}

async fn clause_predicate_is_const_false(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    from_sql: &str,
    predicate_sql: Option<&str>,
) -> Result<bool> {
    let Some(predicate) = predicate_sql else {
        return Ok(false);
    };
    let probe = format!("SELECT ({predicate}) AS __pred FROM {from_sql}");
    let frame = match spark_ast::execute_passthrough(ctx, catalogs, &probe).await {
        Ok(frame) => frame,
        Err(error) if is_cast_error(&error) => return Err(error),
        Err(_) => return Ok(false),
    };
    let options = ctx.state().config().options().clone();
    let envs = walk_plan(frame.logical_plan(), &options, None, 0, false)?;
    let all_false = !envs.is_empty()
        && envs
            .iter()
            .all(|env| matches!(env.first(), Some(Some(ScalarValue::Boolean(Some(false))))));
    Ok(all_false)
}

async fn validate_merge_exprs(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    from_sql: &str,
    exprs: &[&str],
) -> Result<()> {
    if exprs.is_empty() {
        return Ok(());
    }
    let select_list = exprs
        .iter()
        .enumerate()
        .map(|(index, expr)| format!("({expr}) AS __merge_expr_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let probe = format!("SELECT {select_list} FROM {from_sql}");
    validate_probe_sql(ctx, catalogs, &probe).await
}

async fn validate_probe_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    probe_sql: &str,
) -> Result<()> {
    match spark_ast::execute_passthrough(ctx, catalogs, probe_sql).await {
        Ok(frame) => validate_write_plan(ctx, frame.logical_plan()),
        Err(error) if is_cast_error(&error) => Err(error),
        Err(_) => Ok(()),
    }
}

fn is_cast_error(error: &DataFusionError) -> bool {
    has_spark_error_class(&error.to_string())
}

fn qualified_table_sql(catalog_name: &str, ident: &iceberg::TableIdent) -> String {
    let namespace = namespace_schema_name(ident.namespace());
    format!(
        "{}.{}.{}",
        quote_backtick(catalog_name),
        quote_backtick(&namespace),
        quote_backtick(ident.name())
    )
}

fn quote_backtick(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn unwrap_write_root(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Analyze(analyze) => unwrap_write_root(&analyze.input),
        LogicalPlan::Dml(dml) => match dml.op {
            WriteOp::Update => unwrap_update_filter(&dml.input),
            _ => &dml.input,
        },
        LogicalPlan::Copy(copy) => &copy.input,
        other => other,
    }
}

fn unwrap_update_filter(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Projection(projection) => unwrap_update_filter(&projection.input),
        LogicalPlan::SubqueryAlias(alias) => unwrap_update_filter(&alias.input),
        other => other,
    }
}

pub(super) fn walk_plan(
    plan: &LogicalPlan,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    if depth > MAX_PLAN_DEPTH {
        return Ok(vec![unknown_row(plan.schema())]);
    }
    if let Some((inner, maybe_settings)) = extension_settings(plan) {
        let child_options = overlay_options(options, maybe_settings.as_ref());
        return walk_plan(inner, &child_options, needed, depth + 1, may_be_empty);
    }
    match plan {
        LogicalPlan::Dml(dml) => match dml.op {
            WriteOp::Update => {
                let target = unwrap_update_filter(&dml.input);
                walk_plan(target, options, None, depth + 1, false)
            }
            _ => walk_plan(&dml.input, options, None, depth + 1, false),
        },
        LogicalPlan::Copy(copy) => walk_plan(&copy.input, options, None, depth + 1, false),
        LogicalPlan::Limit(limit) => walk_limit(limit, options, needed, depth, may_be_empty),
        LogicalPlan::Projection(projection) => {
            walk_projection(projection, options, needed, depth, may_be_empty)
        }
        LogicalPlan::Filter(filter) => walk_filter(filter, options, needed, depth),
        LogicalPlan::SubqueryAlias(alias) => {
            walk_plan(&alias.input, options, needed, depth + 1, may_be_empty)
        }
        LogicalPlan::Subquery(subquery) => {
            walk_plan(&subquery.subquery, options, needed, depth + 1, may_be_empty)
        }
        LogicalPlan::Repartition(repartition) => {
            walk_plan(&repartition.input, options, needed, depth + 1, may_be_empty)
        }
        LogicalPlan::Sort(sort) => walk_sort(sort, options, needed, depth, may_be_empty),
        LogicalPlan::Distinct(distinct) => {
            walk_distinct(distinct, options, needed, depth, may_be_empty)
        }
        LogicalPlan::Union(union) => walk_union(union, options, needed, depth, may_be_empty),
        LogicalPlan::Join(join) => walk_join(join, options, needed, depth),
        LogicalPlan::Aggregate(aggregate) => {
            walk_aggregate(aggregate, options, depth, may_be_empty)
        }
        LogicalPlan::Window(window) => walk_window(window, options, depth, may_be_empty),
        LogicalPlan::Values(values) => walk_values(values, options, depth, may_be_empty),
        LogicalPlan::EmptyRelation(empty) => Ok(vec![unknown_row(&empty.schema)]),
        LogicalPlan::TableScan(scan) => walk_table_scan(scan, options, needed, depth, may_be_empty),
        LogicalPlan::Analyze(analyze) => {
            walk_plan(&analyze.input, options, needed, depth + 1, may_be_empty)
        }
        _ => {
            let inputs = plan.inputs();
            if inputs.len() == 1
                && inputs[0].schema().fields().len() == plan.schema().fields().len()
            {
                return walk_plan(inputs[0], options, needed, depth + 1, may_be_empty);
            }
            for input in inputs {
                let _ = walk_plan(input, options, None, depth + 1, may_be_empty)?;
            }
            Ok(vec![unknown_row(plan.schema())])
        }
    }
}

fn walk_filter(
    filter: &Filter,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
) -> Result<Vec<RowEnv>> {
    let mut child_needed = needed.cloned();
    if let Some(set) = child_needed.as_mut() {
        collect_expr_columns(&filter.predicate, filter.input.schema(), set);
    }
    let envs = walk_plan(
        &filter.input,
        options,
        child_needed.as_ref(),
        depth + 1,
        true,
    )?;
    for env in &envs {
        let eval_ctx = EvalContext {
            schema: filter.input.schema(),
            env,
            options,
            plan_depth: depth,
        };
        let _ = eval_expr(&eval_ctx, &filter.predicate)?;
    }
    Ok(envs)
}

fn walk_sort(
    sort: &Sort,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    let mut child_needed = needed.cloned();
    if let Some(set) = child_needed.as_mut() {
        for expr in &sort.expr {
            collect_expr_columns(&expr.expr, sort.input.schema(), set);
        }
    }
    let envs = walk_plan(
        &sort.input,
        options,
        child_needed.as_ref(),
        depth + 1,
        may_be_empty,
    )?;
    for env in &envs {
        let eval_ctx = EvalContext {
            schema: sort.input.schema(),
            env,
            options,
            plan_depth: depth,
        };
        for expr in &sort.expr {
            let _ = eval_expr(&eval_ctx, &expr.expr)?;
        }
    }
    Ok(envs)
}

fn walk_window(
    window: &Window,
    options: &ConfigOptions,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    let envs = walk_plan(&window.input, options, None, depth + 1, may_be_empty)?;
    let mut out = Vec::with_capacity(envs.len());
    for env in envs {
        let eval_ctx = EvalContext {
            schema: window.input.schema(),
            env: &env,
            options,
            plan_depth: depth,
        };
        for expr in &window.window_expr {
            let _ = eval_expr(&eval_ctx, expr)?;
        }
        let mut combined = env;
        combined.resize(window.schema.fields().len(), None);
        out.push(combined);
    }
    Ok(out)
}

fn walk_values(
    values: &Values,
    options: &ConfigOptions,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    if may_be_empty {
        let empty_schema = DFSchema::empty();
        let eval_ctx = EvalContext {
            schema: &empty_schema,
            env: &[],
            options,
            plan_depth: depth,
        };
        for row in &values.values {
            for cell in row {
                let _ = eval_expr(&eval_ctx, cell)?;
            }
        }
    }
    Ok(vec![unknown_row(&values.schema)])
}

fn walk_limit(
    limit: &Limit,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    if let Some(fetch) = limit.fetch.as_deref()
        && is_zero_limit(fetch, options)
    {
        return Ok(Vec::new());
    }
    walk_plan(&limit.input, options, needed, depth + 1, may_be_empty)
}

fn is_zero_limit(expr: &Expr, options: &ConfigOptions) -> bool {
    let empty_schema = DFSchema::empty();
    let eval_ctx = EvalContext {
        schema: &empty_schema,
        env: &[],
        options,
        plan_depth: 0,
    };
    let Ok(Some(value)) = eval_expr(&eval_ctx, expr) else {
        return false;
    };
    matches!(
        value,
        ScalarValue::Int64(Some(0))
            | ScalarValue::Int32(Some(0))
            | ScalarValue::Int16(Some(0))
            | ScalarValue::Int8(Some(0))
            | ScalarValue::UInt64(Some(0))
            | ScalarValue::UInt32(Some(0))
            | ScalarValue::UInt16(Some(0))
            | ScalarValue::UInt8(Some(0))
    )
}

fn walk_projection(
    projection: &Projection,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    let mut child_needed = HashSet::new();
    for (index, expr) in projection.expr.iter().enumerate() {
        if needed.is_none_or(|set| set.contains(&index)) {
            collect_expr_columns(expr, projection.input.schema(), &mut child_needed);
        }
    }
    let child_envs = walk_plan(
        &projection.input,
        options,
        Some(&child_needed),
        depth + 1,
        may_be_empty,
    )?;
    let mut out = Vec::with_capacity(child_envs.len());
    for env in &child_envs {
        let eval_ctx = EvalContext {
            schema: projection.input.schema(),
            env,
            options,
            plan_depth: depth,
        };
        let mut row = Vec::with_capacity(projection.expr.len());
        for (index, expr) in projection.expr.iter().enumerate() {
            if needed.is_some_and(|set| !set.contains(&index)) {
                row.push(None);
                continue;
            }
            row.push(eval_expr(&eval_ctx, expr)?);
        }
        out.push(row);
    }
    Ok(out)
}

fn walk_distinct(
    distinct: &Distinct,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    match distinct {
        Distinct::All(input) => walk_plan(input, options, needed, depth + 1, may_be_empty),
        Distinct::On(DistinctOn {
            on_expr,
            select_expr,
            sort_expr,
            input,
            ..
        }) => {
            let envs = walk_plan(input, options, None, depth + 1, may_be_empty)?;
            let mut out = Vec::with_capacity(envs.len());
            for env in &envs {
                let eval_ctx = EvalContext {
                    schema: input.schema(),
                    env,
                    options,
                    plan_depth: depth,
                };
                for expr in on_expr {
                    let _ = eval_expr(&eval_ctx, expr)?;
                }
                if let Some(sorts) = sort_expr {
                    for expr in sorts {
                        let _ = eval_expr(&eval_ctx, &expr.expr)?;
                    }
                }
                let mut row = Vec::with_capacity(select_expr.len());
                for expr in select_expr {
                    row.push(eval_expr(&eval_ctx, expr)?);
                }
                out.push(row);
            }
            Ok(out)
        }
    }
}

fn walk_union(
    union: &Union,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    let width = union.schema.fields().len();
    let mut combined = Vec::new();
    for input in &union.inputs {
        let branch_envs = walk_plan(input, options, needed, depth + 1, may_be_empty)?;
        for mut env in branch_envs {
            if env.len() != width {
                env.resize(width, None);
            }
            if !combined.contains(&env) && combined.len() < MAX_BRANCH_ENVS {
                combined.push(env);
            }
        }
    }
    Ok(combined)
}

fn walk_join(
    join: &Join,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
) -> Result<Vec<RowEnv>> {
    let left_len = join.left.schema().fields().len();
    let right_len = join.right.schema().fields().len();
    let (left_needed, right_needed) = split_join_needed(join, needed, left_len, right_len);

    let left_envs = walk_plan(&join.left, options, left_needed.as_ref(), depth + 1, true)?;
    let right_envs = walk_plan(&join.right, options, right_needed.as_ref(), depth + 1, true)?;

    let mut merged_schema = join.left.schema().as_ref().clone();
    merged_schema.merge(join.right.schema());

    let pairs = combine_join_envs(join.join_type, &left_envs, &right_envs, left_len, right_len);
    let mut out = Vec::with_capacity(pairs.len());
    for (left_env, right_env) in pairs {
        let left_ctx = EvalContext {
            schema: join.left.schema(),
            env: &left_env,
            options,
            plan_depth: depth,
        };
        let right_ctx = EvalContext {
            schema: join.right.schema(),
            env: &right_env,
            options,
            plan_depth: depth,
        };
        for (left_on, right_on) in &join.on {
            let _ = eval_expr(&left_ctx, left_on)?;
            let _ = eval_expr(&right_ctx, right_on)?;
        }
        let mut combined_env = Vec::with_capacity(left_len + right_len);
        combined_env.extend(left_env.iter().cloned());
        combined_env.extend(right_env.iter().cloned());
        if let Some(filter) = &join.filter {
            let filter_ctx = EvalContext {
                schema: &merged_schema,
                env: &combined_env,
                options,
                plan_depth: depth,
            };
            let _ = eval_expr(&filter_ctx, filter)?;
        }
        let row = project_join_output(
            join.join_type,
            left_env,
            right_env,
            join.schema.fields().len(),
        );
        if !out.contains(&row) && out.len() < MAX_BRANCH_ENVS {
            out.push(row);
        }
    }
    Ok(out)
}

fn split_join_needed(
    join: &Join,
    needed: Option<&HashSet<usize>>,
    left_len: usize,
    right_len: usize,
) -> (Option<HashSet<usize>>, Option<HashSet<usize>>) {
    let Some(needed) = needed else {
        return (None, None);
    };
    let mut left = HashSet::new();
    let mut right = HashSet::new();
    match join.join_type {
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            for &idx in needed {
                if idx < left_len {
                    left.insert(idx);
                }
            }
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            for &idx in needed {
                if idx < right_len {
                    right.insert(idx);
                }
            }
        }
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            for &idx in needed {
                if idx < left_len {
                    left.insert(idx);
                } else if idx < left_len + right_len {
                    right.insert(idx - left_len);
                }
            }
        }
    }
    for (left_on, right_on) in &join.on {
        collect_expr_columns(left_on, join.left.schema(), &mut left);
        collect_expr_columns(right_on, join.right.schema(), &mut right);
    }
    if let Some(filter) = &join.filter {
        let mut merged = join.left.schema().as_ref().clone();
        merged.merge(join.right.schema());
        let mut filter_cols = HashSet::new();
        collect_expr_columns(filter, &merged, &mut filter_cols);
        for idx in filter_cols {
            if idx < left_len {
                left.insert(idx);
            } else if idx < left_len + right_len {
                right.insert(idx - left_len);
            }
        }
    }
    (Some(left), Some(right))
}

fn combine_join_envs(
    join_type: JoinType,
    left_envs: &[RowEnv],
    right_envs: &[RowEnv],
    left_len: usize,
    right_len: usize,
) -> Vec<(RowEnv, RowEnv)> {
    let mut pairs = Vec::new();
    if !left_envs.is_empty() && !right_envs.is_empty() {
        for left in left_envs {
            for right in right_envs {
                if pairs.len() >= MAX_BRANCH_ENVS {
                    return pairs;
                }
                pairs.push((left.clone(), right.clone()));
            }
        }
        return pairs;
    }
    match join_type {
        JoinType::Left | JoinType::LeftAnti | JoinType::LeftMark | JoinType::Full
            if !left_envs.is_empty() =>
        {
            for left in left_envs {
                pairs.push((left.clone(), vec![None; right_len]));
            }
        }
        JoinType::Right | JoinType::RightAnti | JoinType::RightMark | JoinType::Full
            if !right_envs.is_empty() =>
        {
            for right in right_envs {
                pairs.push((vec![None; left_len], right.clone()));
            }
        }
        _ => {}
    }
    pairs
}

fn project_join_output(
    join_type: JoinType,
    mut left: RowEnv,
    mut right: RowEnv,
    out_len: usize,
) -> RowEnv {
    let mut row = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => left,
        JoinType::LeftMark => {
            left.push(None);
            left
        }
        JoinType::RightSemi | JoinType::RightAnti => right,
        JoinType::RightMark => {
            right.push(None);
            right
        }
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            left.extend(right);
            left
        }
    };
    row.resize(out_len, None);
    row
}

fn walk_aggregate(
    aggregate: &Aggregate,
    options: &ConfigOptions,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    let mut child_needed = HashSet::new();
    for expr in &aggregate.group_expr {
        collect_expr_columns(expr, aggregate.input.schema(), &mut child_needed);
    }
    for expr in &aggregate.aggr_expr {
        collect_expr_columns(expr, aggregate.input.schema(), &mut child_needed);
    }
    let envs = walk_plan(
        &aggregate.input,
        options,
        Some(&child_needed),
        depth + 1,
        may_be_empty,
    )?;
    let out_len = aggregate.schema.fields().len();
    let mut out = Vec::with_capacity(envs.len());
    for env in &envs {
        let eval_ctx = EvalContext {
            schema: aggregate.input.schema(),
            env,
            options,
            plan_depth: depth,
        };
        let mut row = Vec::with_capacity(out_len);
        for expr in &aggregate.group_expr {
            row.push(eval_expr(&eval_ctx, expr)?);
        }
        for expr in &aggregate.aggr_expr {
            let _ = eval_expr(&eval_ctx, expr)?;
        }
        row.resize(out_len, None);
        out.push(row);
    }
    Ok(out)
}

fn walk_table_scan(
    scan: &TableScan,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    may_be_empty: bool,
) -> Result<Vec<RowEnv>> {
    if scan.fetch == Some(0) {
        return Ok(Vec::new());
    }
    let provider = source_as_provider(&scan.source).ok();
    if let Some(provider) = provider
        && let Some((view_plan, view_settings)) = plan_and_settings(provider.as_ref())
    {
        let child_options = overlay_options(options, view_settings.as_ref());
        let child_needed = map_scan_needed(scan.projection.as_deref(), needed);
        let inner_envs = walk_plan(
            &view_plan,
            &child_options,
            child_needed.as_ref(),
            depth + 1,
            may_be_empty || !scan.filters.is_empty(),
        )?;
        let projected_envs: Vec<RowEnv> = inner_envs
            .into_iter()
            .map(|env| {
                project_scan_env(
                    &env,
                    scan.projection.as_deref(),
                    scan.projected_schema.fields().len(),
                )
            })
            .collect();
        for env in &projected_envs {
            let eval_ctx = EvalContext {
                schema: &scan.projected_schema,
                env,
                options,
                plan_depth: depth,
            };
            for filter in &scan.filters {
                let _ = eval_expr(&eval_ctx, filter)?;
            }
        }
        return Ok(projected_envs);
    }
    let unknown = unknown_row(&scan.projected_schema);
    let eval_ctx = EvalContext {
        schema: &scan.projected_schema,
        env: &unknown,
        options,
        plan_depth: depth,
    };
    for filter in &scan.filters {
        let _ = eval_expr(&eval_ctx, filter)?;
    }
    Ok(vec![unknown])
}

fn map_scan_needed(
    projection: Option<&[usize]>,
    needed: Option<&HashSet<usize>>,
) -> Option<HashSet<usize>> {
    match (projection, needed) {
        (Some(proj), Some(needed)) => {
            let mut out = HashSet::new();
            for &idx in needed {
                if let Some(&src) = proj.get(idx) {
                    out.insert(src);
                }
            }
            Some(out)
        }
        (Some(proj), None) => Some(proj.iter().copied().collect()),
        (None, Some(needed)) => Some(needed.clone()),
        (None, None) => None,
    }
}

fn project_scan_env(env: &RowEnv, projection: Option<&[usize]>, out_len: usize) -> RowEnv {
    let mut row = match projection {
        Some(proj) => proj
            .iter()
            .map(|&idx| env.get(idx).cloned().flatten())
            .collect(),
        None => env.clone(),
    };
    row.resize(out_len, None);
    row
}

fn overlay_options(base: &ConfigOptions, settings: Option<&TempViewSettings>) -> ConfigOptions {
    let Some(settings) = settings else {
        return base.clone();
    };
    let mut overlaid = base.clone();
    settings.apply(&mut overlaid);
    overlaid
}

fn collect_expr_columns(expr: &Expr, schema: &DFSchema, out: &mut HashSet<usize>) {
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node
            && let Some(index) = schema
                .index_of_column(column)
                .ok()
                .or_else(|| schema.index_of_column_by_name(column.relation.as_ref(), &column.name))
        {
            out.insert(index);
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

fn unknown_row(schema: &DFSchema) -> RowEnv {
    vec![None; schema.fields().len()]
}
