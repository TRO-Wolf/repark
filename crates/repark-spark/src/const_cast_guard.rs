use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{
    Aggregate, Distinct, DistinctOn, Expr, Filter, Limit, LogicalPlan, Projection, Sort, TableScan,
    Union, Values, Window, WriteOp,
};
use datafusion::prelude::SessionContext;

use crate::view_ddl::frame_settings::extension_settings;
use crate::view_ddl::temp_settings::TempViewSettings;
use crate::view_ddl::temp_view::{live_replanning_plan, static_view_plan, view_cache_key};

mod eval;
mod join;
mod probes;
mod spark_grammar;
#[cfg(test)]
mod tests;

use eval::{EvalContext, eval_expr};
use join::walk_join;
pub(crate) use probes::{
    sql_expr_has_explicit_cast, validate_identity_dml_predicate, validate_merge_spec,
    validate_write_query,
};

pub(super) const MAX_PLAN_DEPTH: usize = 48;
pub(super) const MAX_BRANCH_ENVS: usize = 64;

pub(super) type RowEnv = Vec<Option<ScalarValue>>;

pub(super) enum ResolvedView {
    Plan(Box<LogicalPlan>, Option<TempViewSettings>),
    CastError(String),
    Unknown,
}

pub(super) type ViewPlanCache = HashMap<u64, ResolvedView>;

pub(crate) fn validate_write_plan<'a>(
    ctx: &'a SessionContext,
    plan: &'a LogicalPlan,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let options = ctx.state().config().options().clone();
        let target = unwrap_write_root(plan);
        let mut views = ViewPlanCache::new();
        resolve_live_views(target, &options, 0, &mut views).await;
        let _ = walk_plan(target, &options, None, 0, false, &views)?;
        Ok(())
    })
}

pub(super) fn resolve_live_views<'a>(
    plan: &'a LogicalPlan,
    options: &'a ConfigOptions,
    depth: usize,
    views: &'a mut ViewPlanCache,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_PLAN_DEPTH {
            return;
        }
        if let Some((inner, maybe_settings)) = extension_settings(plan) {
            let child_options = overlay_options(options, maybe_settings.as_ref());
            resolve_live_views(inner, &child_options, depth + 1, views).await;
            return;
        }
        match plan {
            LogicalPlan::Limit(limit)
                if limit
                    .fetch
                    .as_deref()
                    .is_some_and(|expr| is_zero_limit(expr, options, views)) =>
            {
                return;
            }
            LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Update) => {
                let target = unwrap_update_filter(&dml.input);
                resolve_live_views(target, options, depth + 1, views).await;
                return;
            }
            LogicalPlan::TableScan(scan) if scan.fetch != Some(0) => {
                resolve_scan_view(scan, options, depth, views).await;
            }
            _ => {}
        }

        let mut subqueries = Vec::new();
        let _ = plan.apply_expressions(|expr| {
            collect_expr_subqueries(expr, &mut subqueries);
            Ok(TreeNodeRecursion::Continue)
        });
        for subquery in &subqueries {
            resolve_live_views(subquery, options, depth + 1, views).await;
        }
        for input in plan.inputs() {
            resolve_live_views(input, options, depth + 1, views).await;
        }
    })
}

async fn resolve_scan_view(
    scan: &TableScan,
    options: &ConfigOptions,
    depth: usize,
    views: &mut ViewPlanCache,
) {
    let Ok(provider) = source_as_provider(&scan.source) else {
        return;
    };
    if let Some(key) = view_cache_key(provider.as_ref()) {
        if views.contains_key(&key) {
            return;
        }
        views.insert(key, ResolvedView::Unknown);
        match live_replanning_plan(provider.as_ref()).await {
            Ok(Some((view_plan, settings))) => {
                let child_options = overlay_options(options, Some(&settings));
                resolve_live_views(&view_plan, &child_options, depth + 1, views).await;
                views.insert(key, ResolvedView::Plan(Box::new(view_plan), Some(settings)));
            }
            Err(error) if probes::is_cast_error(&error) => {
                views.insert(key, ResolvedView::CastError(error.to_string()));
            }
            Ok(None) | Err(_) => {}
        }
        return;
    }
    if let Some((view_plan, maybe_settings)) = static_view_plan(provider.as_ref()) {
        let child_options = overlay_options(options, maybe_settings.as_ref());
        resolve_live_views(&view_plan, &child_options, depth + 1, views).await;
    }
}

fn collect_expr_subqueries(expr: &Expr, out: &mut Vec<LogicalPlan>) {
    let _ = expr.apply(|node| {
        match node {
            Expr::ScalarSubquery(subquery) => out.push((*subquery.subquery).clone()),
            Expr::Exists(Exists { subquery, .. })
            | Expr::InSubquery(InSubquery { subquery, .. }) => {
                out.push((*subquery.subquery).clone());
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
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
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    if depth > MAX_PLAN_DEPTH {
        return Ok(vec![unknown_row(plan.schema())]);
    }
    if let Some((inner, maybe_settings)) = extension_settings(plan) {
        let child_options = overlay_options(options, maybe_settings.as_ref());
        return walk_plan(
            inner,
            &child_options,
            needed,
            depth + 1,
            may_be_empty,
            views,
        );
    }
    match plan {
        LogicalPlan::Dml(dml) => match dml.op {
            WriteOp::Update => {
                let target = unwrap_update_filter(&dml.input);
                walk_plan(target, options, None, depth + 1, false, views)
            }
            _ => walk_plan(&dml.input, options, None, depth + 1, false, views),
        },
        LogicalPlan::Copy(copy) => walk_plan(&copy.input, options, None, depth + 1, false, views),
        LogicalPlan::Limit(limit) => walk_limit(limit, options, needed, depth, may_be_empty, views),
        LogicalPlan::Projection(projection) => {
            walk_projection(projection, options, needed, depth, may_be_empty, views)
        }
        LogicalPlan::Filter(filter) => walk_filter(filter, options, needed, depth, views),
        LogicalPlan::SubqueryAlias(alias) => walk_plan(
            &alias.input,
            options,
            needed,
            depth + 1,
            may_be_empty,
            views,
        ),
        LogicalPlan::Subquery(subquery) => walk_plan(
            &subquery.subquery,
            options,
            needed,
            depth + 1,
            may_be_empty,
            views,
        ),
        LogicalPlan::Repartition(repartition) => walk_plan(
            &repartition.input,
            options,
            needed,
            depth + 1,
            may_be_empty,
            views,
        ),
        LogicalPlan::Sort(sort) => walk_sort(sort, options, needed, depth, may_be_empty, views),
        LogicalPlan::Distinct(distinct) => {
            walk_distinct(distinct, options, needed, depth, may_be_empty, views)
        }
        LogicalPlan::Union(union) => walk_union(union, options, needed, depth, may_be_empty, views),
        LogicalPlan::Join(join) => walk_join(join, options, needed, depth, views),
        LogicalPlan::Aggregate(aggregate) => {
            walk_aggregate(aggregate, options, depth, may_be_empty, views)
        }
        LogicalPlan::Window(window) => walk_window(window, options, depth, may_be_empty, views),
        LogicalPlan::Values(values) => walk_values(values, options, depth, may_be_empty, views),
        LogicalPlan::EmptyRelation(empty) => Ok(vec![unknown_row(&empty.schema)]),
        LogicalPlan::TableScan(scan) => {
            walk_table_scan(scan, options, needed, depth, may_be_empty, views)
        }
        LogicalPlan::Analyze(analyze) => walk_plan(
            &analyze.input,
            options,
            needed,
            depth + 1,
            may_be_empty,
            views,
        ),
        _ => {
            let inputs = plan.inputs();
            if inputs.len() == 1
                && inputs[0].schema().fields().len() == plan.schema().fields().len()
            {
                return walk_plan(inputs[0], options, needed, depth + 1, may_be_empty, views);
            }
            for input in inputs {
                let _ = walk_plan(input, options, None, depth + 1, may_be_empty, views)?;
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
    views: &ViewPlanCache,
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
        views,
    )?;
    for env in &envs {
        let eval_ctx = EvalContext {
            schema: filter.input.schema(),
            env,
            options,
            plan_depth: depth,
            views,
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
    views: &ViewPlanCache,
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
        views,
    )?;
    for env in &envs {
        let eval_ctx = EvalContext {
            schema: sort.input.schema(),
            env,
            options,
            plan_depth: depth,
            views,
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
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    let envs = walk_plan(&window.input, options, None, depth + 1, may_be_empty, views)?;
    let mut out = Vec::with_capacity(envs.len());
    for env in envs {
        let eval_ctx = EvalContext {
            schema: window.input.schema(),
            env: &env,
            options,
            plan_depth: depth,
            views,
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
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    if may_be_empty {
        let empty_schema = DFSchema::empty();
        let eval_ctx = EvalContext {
            schema: &empty_schema,
            env: &[],
            options,
            plan_depth: depth,
            views,
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
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    if let Some(fetch) = limit.fetch.as_deref()
        && is_zero_limit(fetch, options, views)
    {
        return Ok(Vec::new());
    }
    walk_plan(
        &limit.input,
        options,
        needed,
        depth + 1,
        may_be_empty,
        views,
    )
}

pub(super) fn is_zero_limit(expr: &Expr, options: &ConfigOptions, views: &ViewPlanCache) -> bool {
    let empty_schema = DFSchema::empty();
    let eval_ctx = EvalContext {
        schema: &empty_schema,
        env: &[],
        options,
        plan_depth: 0,
        views,
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
    views: &ViewPlanCache,
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
        views,
    )?;
    let mut out = Vec::with_capacity(child_envs.len());
    for env in &child_envs {
        let eval_ctx = EvalContext {
            schema: projection.input.schema(),
            env,
            options,
            plan_depth: depth,
            views,
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
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    match distinct {
        Distinct::All(input) => walk_plan(input, options, needed, depth + 1, may_be_empty, views),
        Distinct::On(DistinctOn {
            on_expr,
            select_expr,
            sort_expr,
            input,
            ..
        }) => {
            let envs = walk_plan(input, options, None, depth + 1, may_be_empty, views)?;
            let mut out = Vec::with_capacity(envs.len());
            for env in &envs {
                let eval_ctx = EvalContext {
                    schema: input.schema(),
                    env,
                    options,
                    plan_depth: depth,
                    views,
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
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    let width = union.schema.fields().len();
    let mut combined = Vec::new();
    for input in &union.inputs {
        let branch_envs = walk_plan(input, options, needed, depth + 1, may_be_empty, views)?;
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

fn walk_aggregate(
    aggregate: &Aggregate,
    options: &ConfigOptions,
    depth: usize,
    may_be_empty: bool,
    views: &ViewPlanCache,
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
        views,
    )?;
    if envs.is_empty() && aggregate.group_expr.is_empty() {
        return Ok(vec![unknown_row(&aggregate.schema)]);
    }
    let out_len = aggregate.schema.fields().len();
    let mut out = Vec::with_capacity(envs.len());
    for env in &envs {
        let eval_ctx = EvalContext {
            schema: aggregate.input.schema(),
            env,
            options,
            plan_depth: depth,
            views,
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
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    if scan.fetch == Some(0) {
        return Ok(Vec::new());
    }
    let resolved = source_as_provider(&scan.source).ok().and_then(|provider| {
        if let Some(key) = view_cache_key(provider.as_ref()) {
            return match views.get(&key) {
                Some(ResolvedView::Plan(plan, settings)) => {
                    Some(Ok((plan.as_ref().clone(), settings.clone())))
                }
                Some(ResolvedView::CastError(msg)) => {
                    Some(Err(DataFusionError::Execution(msg.clone())))
                }
                _ => None,
            };
        }
        static_view_plan(provider.as_ref()).map(Ok)
    });
    if let Some(result) = resolved {
        let (view_plan, view_settings) = result?;
        let child_options = overlay_options(options, view_settings.as_ref());
        let child_needed = map_scan_needed(scan.projection.as_deref(), needed);
        let inner_envs = walk_plan(
            &view_plan,
            &child_options,
            child_needed.as_ref(),
            depth + 1,
            may_be_empty || !scan.filters.is_empty(),
            views,
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
                views,
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
        views,
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

pub(super) fn overlay_options(
    base: &ConfigOptions,
    settings: Option<&TempViewSettings>,
) -> ConfigOptions {
    let Some(settings) = settings else {
        return base.clone();
    };
    let mut overlaid = base.clone();
    settings.apply(&mut overlaid);
    overlaid
}

pub(super) fn collect_expr_columns(expr: &Expr, schema: &DFSchema, out: &mut HashSet<usize>) {
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
