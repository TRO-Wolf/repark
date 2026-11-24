use std::collections::HashSet;

use datafusion::common::config::ConfigOptions;
use datafusion::common::{NullEquality, ScalarValue};
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{Distinct, Join, JoinType, LogicalPlan};

use crate::view_ddl::frame_settings::extension_settings;
use crate::view_ddl::temp_view::{static_view_plan, view_cache_key};

use super::eval::{EvalContext, eval_expr, scalars_equal};
use super::{
    MAX_BRANCH_ENVS, MAX_PLAN_DEPTH, ResolvedView, RowEnv, ViewPlanCache, collect_expr_columns,
    is_zero_limit, overlay_options, walk_plan,
};

pub(super) fn walk_join(
    join: &Join,
    options: &ConfigOptions,
    needed: Option<&HashSet<usize>>,
    depth: usize,
    views: &ViewPlanCache,
) -> Result<Vec<RowEnv>> {
    let left_len = join.left.schema().fields().len();
    let right_len = join.right.schema().fields().len();
    let (left_needed, right_needed) = split_join_needed(join, needed, left_len, right_len);

    let left_envs = walk_plan(
        &join.left,
        options,
        left_needed.as_ref(),
        depth + 1,
        true,
        views,
    )?;
    let right_envs = walk_plan(
        &join.right,
        options,
        right_needed.as_ref(),
        depth + 1,
        true,
        views,
    )?;

    let mut merged_schema = join.left.schema().as_ref().clone();
    merged_schema.merge(join.right.schema());

    let both_non_empty = side_guaranteed_non_empty(&join.left, options, views, depth + 1)
        && side_guaranteed_non_empty(&join.right, options, views, depth + 1);
    let pairs = combine_join_envs(join.join_type, &left_envs, &right_envs, left_len, right_len);
    let mut out = Vec::with_capacity(pairs.len());
    for (left_env, right_env) in pairs {
        let on_always_true = eval_join_equi_on(join, &left_env, &right_env, options, depth, views)?;
        let mut combined_env = Vec::with_capacity(left_len + right_len);
        combined_env.extend(left_env.iter().cloned());
        combined_env.extend(right_env.iter().cloned());
        let filter_val = if let Some(filter) = &join.filter {
            let filter_ctx = EvalContext {
                schema: &merged_schema,
                env: &combined_env,
                options,
                plan_depth: depth,
                views,
            };
            eval_expr(&filter_ctx, filter)?
        } else {
            None
        };
        if join.join_type == JoinType::Inner
            && join.on.is_empty()
            && matches!(filter_val, Some(ScalarValue::Boolean(Some(false))))
        {
            continue;
        }
        let filter_always_true =
            join.filter.is_none() || matches!(filter_val, Some(ScalarValue::Boolean(Some(true))));
        let preserve_nullable_side = both_non_empty && on_always_true && filter_always_true;
        let row = project_join_output(
            join.join_type,
            left_env,
            right_env,
            join.schema.fields().len(),
            preserve_nullable_side,
        );
        if !out.contains(&row) && out.len() < MAX_BRANCH_ENVS {
            out.push(row);
        }
    }
    Ok(out)
}

fn eval_join_equi_on(
    join: &Join,
    left_env: &[Option<ScalarValue>],
    right_env: &[Option<ScalarValue>],
    options: &ConfigOptions,
    depth: usize,
    views: &ViewPlanCache,
) -> Result<bool> {
    let left_ctx = EvalContext {
        schema: join.left.schema(),
        env: left_env,
        options,
        plan_depth: depth,
        views,
    };
    let right_ctx = EvalContext {
        schema: join.right.schema(),
        env: right_env,
        options,
        plan_depth: depth,
        views,
    };
    let mut always_true = true;
    for (left_on, right_on) in &join.on {
        let left_val = eval_expr(&left_ctx, left_on)?;
        let right_val = eval_expr(&right_ctx, right_on)?;
        let eq_true = match (left_val.as_ref(), right_val.as_ref()) {
            (Some(lv), Some(rv)) if !lv.is_null() && !rv.is_null() => {
                scalars_equal(options, lv, rv)
            }
            (Some(lv), Some(rv))
                if lv.is_null()
                    && rv.is_null()
                    && join.null_equality == NullEquality::NullEqualsNull =>
            {
                true
            }
            _ => false,
        };
        if !eq_true {
            always_true = false;
        }
    }
    Ok(always_true)
}

fn side_guaranteed_non_empty(
    plan: &LogicalPlan,
    options: &ConfigOptions,
    views: &ViewPlanCache,
    depth: usize,
) -> bool {
    if depth > MAX_PLAN_DEPTH {
        return false;
    }
    if let Some((inner, maybe_settings)) = extension_settings(plan) {
        let child_options = overlay_options(options, maybe_settings.as_ref());
        return side_guaranteed_non_empty(inner, &child_options, views, depth + 1);
    }
    match plan {
        LogicalPlan::EmptyRelation(empty) => empty.produce_one_row,
        LogicalPlan::Projection(p) => {
            side_guaranteed_non_empty(&p.input, options, views, depth + 1)
        }
        LogicalPlan::SubqueryAlias(a) => {
            side_guaranteed_non_empty(&a.input, options, views, depth + 1)
        }
        LogicalPlan::Subquery(s) => {
            side_guaranteed_non_empty(&s.subquery, options, views, depth + 1)
        }
        LogicalPlan::Sort(s) => side_guaranteed_non_empty(&s.input, options, views, depth + 1),
        LogicalPlan::Window(w) => side_guaranteed_non_empty(&w.input, options, views, depth + 1),
        LogicalPlan::Repartition(r) => {
            side_guaranteed_non_empty(&r.input, options, views, depth + 1)
        }
        LogicalPlan::Distinct(Distinct::All(input)) => {
            side_guaranteed_non_empty(input, options, views, depth + 1)
        }
        LogicalPlan::Aggregate(a) => {
            a.group_expr.is_empty()
                || side_guaranteed_non_empty(&a.input, options, views, depth + 1)
        }
        LogicalPlan::Values(v) => !v.values.is_empty(),
        LogicalPlan::Limit(l) => {
            !l.fetch
                .as_deref()
                .is_some_and(|expr| is_zero_limit(expr, options, views))
                && side_guaranteed_non_empty(&l.input, options, views, depth + 1)
        }
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .any(|input| side_guaranteed_non_empty(input, options, views, depth + 1)),
        LogicalPlan::TableScan(scan) => {
            if scan.fetch == Some(0) || !scan.filters.is_empty() {
                return false;
            }
            let Ok(provider) = source_as_provider(&scan.source) else {
                return false;
            };
            if let Some(key) = view_cache_key(provider.as_ref()) {
                return match views.get(&key) {
                    Some(ResolvedView::Plan(view_plan, maybe_settings)) => {
                        let child_options = overlay_options(options, maybe_settings.as_ref());
                        side_guaranteed_non_empty(view_plan, &child_options, views, depth + 1)
                    }
                    _ => false,
                };
            }
            if let Some((view_plan, maybe_settings)) = static_view_plan(provider.as_ref()) {
                let child_options = overlay_options(options, maybe_settings.as_ref());
                return side_guaranteed_non_empty(&view_plan, &child_options, views, depth + 1);
            }
            false
        }
        _ => false,
    }
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
    preserve_nullable_side: bool,
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
        JoinType::Inner => {
            left.extend(right);
            left
        }
        JoinType::Left => {
            if preserve_nullable_side {
                left.extend(right);
            } else {
                left.extend(std::iter::repeat_n(None, right.len()));
            }
            left
        }
        JoinType::Right => {
            if preserve_nullable_side {
                left.extend(right);
                left
            } else {
                let mut combined = vec![None; left.len()];
                combined.extend(right);
                combined
            }
        }
        JoinType::Full => {
            if preserve_nullable_side {
                left.extend(right);
                left
            } else {
                vec![None; left.len() + right.len()]
            }
        }
    };
    row.resize(out_len, None);
    row
}
