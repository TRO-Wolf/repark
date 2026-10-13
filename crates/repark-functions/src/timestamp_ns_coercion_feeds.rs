use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::{DmlStatement, Expr, Filter, LogicalPlan, Projection, WriteOp};

use crate::timestamp_ns_cast::{is_timestamp_ns_cast, timestamp_ns_target};
use crate::timestamp_ns_coercion_sites::{feed_subtree_holds_site, rewrite_feed_root};

#[derive(Debug)]
pub enum FeedOutcome {
    Unchanged,
    Rewritten(Box<LogicalPlan>),
    Refused(&'static str),
}

fn rewritten(plan: LogicalPlan) -> FeedOutcome {
    FeedOutcome::Rewritten(Box::new(plan))
}

#[allow(clippy::missing_errors_doc)]
pub fn rewrite_feeds(plan: LogicalPlan) -> Result<FeedOutcome> {
    let backup = plan.clone();
    match dml_position_feeds(plan)? {
        FeedOutcome::Unchanged => kernel_argument_feeds(backup),
        FeedOutcome::Rewritten(rewritten) => match kernel_argument_feeds((*rewritten).clone())? {
            FeedOutcome::Unchanged => Ok(FeedOutcome::Rewritten(rewritten)),
            other => Ok(other),
        },
        refused @ FeedOutcome::Refused(_) => Ok(refused),
    }
}

#[allow(clippy::missing_errors_doc)]
fn kernel_argument_feeds(plan: LogicalPlan) -> Result<FeedOutcome> {
    kernel_pass(plan)
}

#[allow(clippy::missing_errors_doc)]
fn kernel_pass(node: LogicalPlan) -> Result<FeedOutcome> {
    match node {
        LogicalPlan::Projection(projection) => {
            let (exprs, input, outcome) = kernel_exprs(projection.expr.clone(), &projection.input)?;
            if matches!(outcome, KernelOutcome::Refused(_)) {
                return Ok(outcome.into_feed());
            }
            let child = kernel_pass(input.as_ref().clone())?;
            match child {
                FeedOutcome::Refused(_) => Ok(child),
                FeedOutcome::Rewritten(next) => {
                    let rebuilt = Projection::try_new(exprs, Arc::new(*next))?;
                    Ok(rewritten(LogicalPlan::Projection(rebuilt)))
                }
                FeedOutcome::Unchanged => {
                    if matches!(outcome, KernelOutcome::Changed) {
                        let rebuilt = Projection::try_new(exprs, input)?;
                        Ok(rewritten(LogicalPlan::Projection(rebuilt)))
                    } else {
                        Ok(FeedOutcome::Unchanged)
                    }
                }
            }
        }
        LogicalPlan::Filter(filter) => {
            let (exprs, input, outcome) =
                kernel_exprs(vec![filter.predicate.clone()], &filter.input)?;
            if matches!(outcome, KernelOutcome::Refused(_)) {
                return Ok(outcome.into_feed());
            }
            let child = kernel_pass(input.as_ref().clone())?;
            match child {
                FeedOutcome::Refused(_) => Ok(child),
                FeedOutcome::Rewritten(next) => {
                    let Some(predicate) = exprs.into_iter().next() else {
                        return Ok(FeedOutcome::Unchanged);
                    };
                    let rebuilt = Filter::try_new(predicate, Arc::new(*next))?;
                    Ok(rewritten(LogicalPlan::recompute_schema(
                        LogicalPlan::Filter(rebuilt),
                    )?))
                }
                FeedOutcome::Unchanged => {
                    if matches!(outcome, KernelOutcome::Changed) {
                        let Some(predicate) = exprs.into_iter().next() else {
                            return Ok(FeedOutcome::Unchanged);
                        };
                        let rebuilt = Filter::try_new(predicate, input)?;
                        Ok(rewritten(LogicalPlan::recompute_schema(
                            LogicalPlan::Filter(rebuilt),
                        )?))
                    } else {
                        Ok(FeedOutcome::Unchanged)
                    }
                }
            }
        }
        LogicalPlan::Sort(mut holder) => {
            let child = kernel_pass(holder.input.as_ref().clone())?;
            match child {
                FeedOutcome::Rewritten(next) => {
                    holder.input = Arc::new(*next);
                    Ok(rewritten(LogicalPlan::recompute_schema(
                        LogicalPlan::Sort(holder),
                    )?))
                }
                other => Ok(other),
            }
        }
        LogicalPlan::Limit(mut holder) => {
            let child = kernel_pass(holder.input.as_ref().clone())?;
            match child {
                FeedOutcome::Rewritten(next) => {
                    holder.input = Arc::new(*next);
                    Ok(rewritten(LogicalPlan::recompute_schema(
                        LogicalPlan::Limit(holder),
                    )?))
                }
                other => Ok(other),
            }
        }
        LogicalPlan::SubqueryAlias(mut holder) => {
            let child = kernel_pass(holder.input.as_ref().clone())?;
            match child {
                FeedOutcome::Rewritten(next) => {
                    holder.input = Arc::new(*next);
                    Ok(rewritten(LogicalPlan::recompute_schema(
                        LogicalPlan::SubqueryAlias(holder),
                    )?))
                }
                other => Ok(other),
            }
        }
        LogicalPlan::Dml(dml) => match kernel_pass(dml.input.as_ref().clone())? {
            FeedOutcome::Rewritten(next) => Ok(rewritten(retarget_dml_input(&dml, *next))),
            other => Ok(other),
        },
        _ => Ok(FeedOutcome::Unchanged),
    }
}

fn retarget_dml_input(dml: &DmlStatement, input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Dml(DmlStatement {
        input: Arc::new(input),
        ..dml.clone()
    })
}

enum KernelOutcome {
    Same,
    Changed,
    Refused(&'static str),
}

impl KernelOutcome {
    fn into_feed(self) -> FeedOutcome {
        match self {
            Self::Refused(reason) => FeedOutcome::Refused(reason),
            _ => FeedOutcome::Unchanged,
        }
    }
}

enum KernelExprOutcome {
    Same,
    Changed,
    Refused(&'static str),
}

#[allow(clippy::missing_errors_doc)]
fn kernel_exprs(
    exprs: Vec<Expr>,
    input: &Arc<LogicalPlan>,
) -> Result<(Vec<Expr>, Arc<LogicalPlan>, KernelOutcome)> {
    let mut schema = DFSchema::empty();
    schema.merge(input.schema());
    let mut current = input.as_ref().clone();
    let mut changed = false;
    let mut owned = exprs;
    for index in 0..owned.len() {
        let (next, item) = rewrite_expr_kernels(&owned[index], &owned, &schema, &mut current)?;
        owned[index] = next;
        match item {
            KernelExprOutcome::Same => {}
            KernelExprOutcome::Changed => changed = true,
            KernelExprOutcome::Refused(reason) => {
                return Ok((owned, Arc::new(current), KernelOutcome::Refused(reason)));
            }
        }
    }
    let outcome = if changed || current != *input.as_ref() {
        KernelOutcome::Changed
    } else {
        KernelOutcome::Same
    };
    Ok((owned, Arc::new(current), outcome))
}

#[allow(clippy::missing_errors_doc)]
fn rewrite_expr_kernels(
    expr: &Expr,
    host: &[Expr],
    schema: &DFSchema,
    input: &mut LogicalPlan,
) -> Result<(Expr, KernelExprOutcome)> {
    let mut refused: Option<&'static str> = None;
    let out = expr.clone().transform_down(|node| {
        if refused.is_some() {
            return Ok(Transformed::no(node));
        }
        if !is_timestamp_ns_cast(&node) {
            return Ok(Transformed::no(node));
        }
        let Expr::ScalarFunction(mut function) = node else {
            return Ok(Transformed::no(node));
        };
        let Some(arg) = function.args.first().cloned() else {
            return Ok(Transformed::no(Expr::ScalarFunction(function)));
        };
        let start = peel_root(&arg).clone();
        if let Expr::Column(column) = &start {
            if count_reads(host, column.name.as_str()) != 1 {
                return Ok(Transformed::no(Expr::ScalarFunction(function)));
            }
            match descend(input.clone(), column)? {
                FeedOutcome::Rewritten(next) => {
                    *input = *next;
                }
                FeedOutcome::Refused(reason) => {
                    refused = Some(reason);
                }
                FeedOutcome::Unchanged => {}
            }
            return Ok(Transformed::no(Expr::ScalarFunction(function)));
        }
        if matches!(start, Expr::Cast(_) | Expr::TryCast(_)) {
            return Ok(Transformed::no(Expr::ScalarFunction(function)));
        }
        match rewrite_root_in_place(&start, schema) {
            RootOutcome::Miss => Ok(Transformed::no(Expr::ScalarFunction(function))),
            RootOutcome::Refused(reason) => {
                refused = Some(reason);
                Ok(Transformed::no(Expr::ScalarFunction(function)))
            }
            RootOutcome::Rewritten(root) => {
                function.args[0] = match &arg {
                    Expr::Alias(alias) => root.alias(alias.name.clone()),
                    _ => root,
                };
                Ok(Transformed::yes(Expr::ScalarFunction(function)))
            }
        }
    })?;
    if let Some(reason) = refused {
        return Ok((expr.clone(), KernelExprOutcome::Refused(reason)));
    }
    if out.transformed {
        Ok((out.data, KernelExprOutcome::Changed))
    } else {
        Ok((out.data, KernelExprOutcome::Same))
    }
}

#[allow(clippy::missing_errors_doc)]
fn dml_position_feeds(plan: LogicalPlan) -> Result<FeedOutcome> {
    let positions = ns_positions(&plan);
    if positions.is_empty() {
        return Ok(FeedOutcome::Unchanged);
    }
    let mut current = plan;
    let mut rewrote_any = false;
    for index in positions {
        match rewrite_dml_position(&current, index)? {
            FeedOutcome::Unchanged => {}
            FeedOutcome::Rewritten(next) => {
                current = *next;
                rewrote_any = true;
            }
            refused @ FeedOutcome::Refused(_) => return Ok(refused),
        }
    }
    if rewrote_any {
        Ok(rewritten(current))
    } else {
        Ok(FeedOutcome::Unchanged)
    }
}

fn ns_positions(plan: &LogicalPlan) -> Vec<usize> {
    let LogicalPlan::Dml(dml) = plan else {
        return Vec::new();
    };
    if !matches!(dml.op, WriteOp::Insert(_) | WriteOp::Update) {
        return Vec::new();
    }
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Vec::new();
    };
    let targets = dml.target.schema();
    if projection.expr.len() != targets.fields().len() {
        return Vec::new();
    }
    targets
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| timestamp_ns_target(field.data_type()).is_some())
        .map(|(index, _)| index)
        .collect()
}

#[allow(clippy::missing_errors_doc)]
fn rewrite_dml_position(plan: &LogicalPlan, index: usize) -> Result<FeedOutcome> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(FeedOutcome::Unchanged);
    };
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(FeedOutcome::Unchanged);
    };
    let Some(item) = projection.expr.get(index) else {
        return Ok(FeedOutcome::Unchanged);
    };
    let start = peel_anchor(item);
    if let Expr::Column(column) = start {
        if count_reads(&projection.expr, column.name.as_str()) != 1 {
            return Ok(FeedOutcome::Unchanged);
        }
        let input = projection.input.as_ref().clone();
        let column = column.clone();
        return match descend(input, &column)? {
            FeedOutcome::Rewritten(input) => {
                let rebuilt = Projection::try_new(projection.expr.clone(), Arc::new(*input))?;
                Ok(rewritten(retarget_dml(dml, rebuilt)))
            }
            other => Ok(other),
        };
    }
    if matches!(start, Expr::Cast(_) | Expr::TryCast(_)) {
        return Ok(FeedOutcome::Unchanged);
    }
    let mut schema = DFSchema::empty();
    schema.merge(projection.input.schema());
    let item = item.clone();
    let feed = peel_root(&item);
    match rewrite_root_in_place(feed, &schema) {
        RootOutcome::Miss => Ok(FeedOutcome::Unchanged),
        RootOutcome::Refused(reason) => Ok(FeedOutcome::Refused(reason)),
        RootOutcome::Rewritten(root) => {
            let mut expr = projection.expr.clone();
            expr[index] = realias(root, &item);
            let rebuilt = Projection::try_new(expr, Arc::clone(&projection.input))?;
            Ok(rewritten(retarget_dml(dml, rebuilt)))
        }
    }
}

fn retarget_dml(dml: &DmlStatement, projection: Projection) -> LogicalPlan {
    LogicalPlan::Dml(DmlStatement {
        input: Arc::new(LogicalPlan::Projection(projection)),
        ..dml.clone()
    })
}

fn peel_root(feed: &Expr) -> &Expr {
    match feed {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    }
}

fn peel_anchor(item: &Expr) -> &Expr {
    let mut current = item;
    loop {
        match current {
            Expr::Alias(alias) => current = alias.expr.as_ref(),
            kernel if is_timestamp_ns_cast(kernel) => {
                let Expr::ScalarFunction(function) = kernel else {
                    break;
                };
                let Some(arg) = function.args.first() else {
                    break;
                };
                current = arg;
            }
            _ => break,
        }
    }
    current
}

enum RootOutcome {
    Miss,
    Rewritten(Expr),
    Refused(&'static str),
}

fn rewrite_root_in_place(feed: &Expr, schema: &DFSchema) -> RootOutcome {
    let Some(rewritten) = rewrite_feed_root(feed, schema) else {
        return RootOutcome::Miss;
    };
    if feed_subtree_holds_site(&rewritten, schema) {
        return RootOutcome::Refused("feed holds a site below its root");
    }
    RootOutcome::Rewritten(rewritten)
}

fn realias(root: Expr, feed: &Expr) -> Expr {
    match feed {
        Expr::Alias(alias) => root.alias(alias.name.clone()),
        _ => root,
    }
}

#[allow(clippy::missing_errors_doc)]
fn descend(node: LogicalPlan, column: &Column) -> Result<FeedOutcome> {
    match node {
        LogicalPlan::Projection(projection) => {
            let Some((at, cross)) = find_producer(&projection.expr, column) else {
                return Ok(FeedOutcome::Unchanged);
            };
            let mut expr = projection.expr;
            let input = projection.input;
            if cross {
                if count_reads(&expr, column.name.as_str()) != 1 {
                    return Ok(FeedOutcome::Unchanged);
                }
                match descend(input.as_ref().clone(), column)? {
                    FeedOutcome::Rewritten(input) => {
                        let rebuilt = Projection::try_new(expr, Arc::new(*input))?;
                        Ok(rewritten(LogicalPlan::Projection(rebuilt)))
                    }
                    other => Ok(other),
                }
            } else {
                let mut schema = DFSchema::empty();
                schema.merge(input.schema());
                let feed = expr[at].clone();
                let held = feed.clone();
                match rewrite_root_in_place(peel_root(&held), &schema) {
                    RootOutcome::Miss => Ok(FeedOutcome::Unchanged),
                    RootOutcome::Refused(reason) => Ok(FeedOutcome::Refused(reason)),
                    RootOutcome::Rewritten(root) => {
                        expr[at] = realias(root, &feed);
                        let rebuilt = Projection::try_new(expr, input)?;
                        Ok(rewritten(LogicalPlan::Projection(rebuilt)))
                    }
                }
            }
        }
        LogicalPlan::Filter(mut node) => {
            if reads_column(&node.predicate, column.name.as_str()) {
                return Ok(FeedOutcome::Unchanged);
            }
            match descend(node.input.as_ref().clone(), column)? {
                FeedOutcome::Rewritten(input) => {
                    node.input = Arc::new(*input);
                    Ok(rewritten(LogicalPlan::recompute_schema(
                        LogicalPlan::Filter(node),
                    )?))
                }
                other => Ok(other),
            }
        }
        LogicalPlan::Sort(mut node) => {
            if node
                .expr
                .iter()
                .any(|order| reads_column(&order.expr, column.name.as_str()))
            {
                return Ok(FeedOutcome::Unchanged);
            }
            match descend(node.input.as_ref().clone(), column)? {
                FeedOutcome::Rewritten(input) => {
                    node.input = Arc::new(*input);
                    Ok(rewritten(LogicalPlan::recompute_schema(
                        LogicalPlan::Sort(node),
                    )?))
                }
                other => Ok(other),
            }
        }
        LogicalPlan::Limit(mut node) => {
            if node
                .skip
                .iter()
                .chain(node.fetch.iter())
                .any(|bound| reads_column(bound, column.name.as_str()))
            {
                return Ok(FeedOutcome::Unchanged);
            }
            match descend(node.input.as_ref().clone(), column)? {
                FeedOutcome::Rewritten(input) => {
                    node.input = Arc::new(*input);
                    Ok(rewritten(LogicalPlan::recompute_schema(
                        LogicalPlan::Limit(node),
                    )?))
                }
                other => Ok(other),
            }
        }
        LogicalPlan::SubqueryAlias(mut node) => match descend(node.input.as_ref().clone(), column)?
        {
            FeedOutcome::Rewritten(input) => {
                node.input = Arc::new(*input);
                Ok(rewritten(LogicalPlan::recompute_schema(
                    LogicalPlan::SubqueryAlias(node),
                )?))
            }
            other => Ok(other),
        },
        _ => Ok(FeedOutcome::Unchanged),
    }
}

fn find_producer(exprs: &[Expr], column: &Column) -> Option<(usize, bool)> {
    for (at, item) in exprs.iter().enumerate() {
        match item {
            Expr::Column(candidate) if candidate.name == column.name => {
                return Some((at, true));
            }
            Expr::Alias(alias) if alias.name == column.name => {
                let cross = matches!(alias.expr.as_ref(), Expr::Column(candidate) if candidate.name == column.name);
                return Some((at, cross));
            }
            _ => {}
        }
    }
    None
}

fn count_reads(exprs: &[Expr], name: &str) -> usize {
    let mut count = 0;
    for expr in exprs {
        let _ = expr.apply(|node| {
            if let Expr::Column(candidate) = node
                && candidate.name == name
            {
                count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    count
}

fn reads_column(expr: &Expr, name: &str) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Expr::Column(candidate) = node
            && candidate.name == name
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}
