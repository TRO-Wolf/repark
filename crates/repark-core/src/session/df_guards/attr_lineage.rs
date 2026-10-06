use datafusion::common::Column;
use datafusion::logical_expr::expr::{Alias, Case};
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, Operator};
use repark_common::names::NameRule;

use super::attr_id::{ATTR_KEY, AttrId};
use super::sort_names::below_transparent;

#[must_use]
pub fn sort_hits_meet_at_join(plan: &LogicalPlan, positions: &[usize]) -> bool {
    let mut node = plan;
    let mut at = positions.to_vec();
    loop {
        node = match node {
            LogicalPlan::Filter(filter) => filter.input.as_ref(),
            LogicalPlan::Sort(sort) => sort.input.as_ref(),
            LogicalPlan::Limit(limit) => limit.input.as_ref(),
            LogicalPlan::Repartition(repartition) => repartition.input.as_ref(),
            LogicalPlan::Distinct(Distinct::All(input)) => input.as_ref(),
            LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
            LogicalPlan::Projection(projection) => {
                let input = projection.input.schema();
                let mut next = Vec::with_capacity(at.len());
                for position in &at {
                    let Some(index) = projection
                        .expr
                        .get(*position)
                        .and_then(plain_source)
                        .and_then(|column| input.maybe_index_of_column(column))
                    else {
                        return false;
                    };
                    next.push(index);
                }
                at = next;
                projection.input.as_ref()
            }
            LogicalPlan::Join(_) => {
                at.sort_unstable();
                at.dedup();
                return at.len() > 1;
            }
            _ => return false,
        };
    }
}

fn plain_source(expr: &Expr) -> Option<&Column> {
    let mut node = expr;
    loop {
        match node {
            Expr::Column(column) => return Some(column),
            Expr::Alias(alias) => node = alias.expr.as_ref(),
            _ => return None,
        }
    }
}

#[must_use]
pub fn sort_sourced_twin_engine(
    plan: &LogicalPlan,
    hits: &[usize],
    written: &str,
    rule: NameRule,
) -> Option<String> {
    let mut node = plan;
    let projection = loop {
        let below = below_transparent(node);
        if !std::ptr::eq(below, node) {
            node = below;
            continue;
        }
        if let LogicalPlan::Projection(projection) = node {
            break projection;
        }
        return None;
    };
    if projection
        .input
        .schema()
        .fields()
        .iter()
        .any(|field| rule.matches(written, field.name()))
    {
        return None;
    }
    let sources = projection_source_ids(node);
    let mut found = None;
    for hit in hits {
        let Some(Some(id)) = sources.get(*hit) else {
            continue;
        };
        let Some(name) = traced_source_name(projection.input.as_ref(), id) else {
            continue;
        };
        if !rule.matches(written, &name) {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = Some(projection.schema().fields().get(*hit)?.name().clone());
    }
    found
}

fn traced_source_name(plan: &LogicalPlan, id: &AttrId) -> Option<String> {
    let position = plan
        .schema()
        .fields()
        .iter()
        .position(|field| AttrId::native(field).as_ref() == Some(id))?;
    let LogicalPlan::Projection(projection) = plan else {
        return plan
            .schema()
            .fields()
            .get(position)
            .map(|field| field.name().clone());
    };
    let sources = projection_source_ids(plan);
    let next = sources.get(position)?.clone()?;
    traced_source_name(projection.input.as_ref(), &next)
}

#[must_use]
pub fn projection_source_ids(plan: &LogicalPlan) -> Vec<Option<AttrId>> {
    let LogicalPlan::Projection(projection) = plan else {
        return vec![None; plan.schema().fields().len()];
    };
    let input = projection.input.schema();
    projection
        .expr
        .iter()
        .map(|expr| match source_column(expr) {
            Some(column) => input
                .maybe_index_of_column(column)
                .and_then(|index| AttrId::native(input.field(index))),
            None => None,
        })
        .collect()
}

fn source_column(expr: &Expr) -> Option<&Column> {
    let mut node = expr;
    let mut mints = 0;
    loop {
        match node {
            Expr::Column(column) => return Some(column),
            Expr::Alias(alias) => {
                if alias_has_attr(alias) {
                    mints += 1;
                    if mints > 1 {
                        return None;
                    }
                }
                node = alias.expr.as_ref();
            }
            Expr::ScalarFunction(func) if func.func.name() == "coalesce" => {
                return coalesce_source_column(&func.args);
            }
            Expr::Case(case) => return case_source_column(case),
            _ => return None,
        }
    }
}

fn const_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(..) => true,
        Expr::Cast(cast) => const_expr(&cast.expr),
        Expr::Alias(alias) if !alias_has_attr(alias) => const_expr(&alias.expr),
        Expr::ScalarFunction(func) => func.args.iter().all(const_expr),
        _ => false,
    }
}

fn unwrapped_column(expr: &Expr) -> Option<&Column> {
    let mut node = expr;
    loop {
        match node {
            Expr::Column(column) => return Some(column),
            Expr::Alias(alias) if !alias_has_attr(alias) => node = alias.expr.as_ref(),
            _ => return None,
        }
    }
}

fn when_column(expr: &Expr) -> Option<&Column> {
    if let Some(column) = unwrapped_column(expr) {
        return Some(column);
    }
    let Expr::Cast(cast) = expr else {
        return None;
    };
    unwrapped_column(&cast.expr)
}

fn coalesce_source_column(args: &[Expr]) -> Option<&Column> {
    if args.len() < 2 {
        return None;
    }
    let mut found: Option<&Column> = None;
    for arg in args {
        if const_expr(arg) {
            continue;
        }
        let column = unwrapped_column(arg)?;
        if found.is_some() {
            return None;
        }
        found = Some(column);
    }
    found
}

fn case_source_column(case: &Case) -> Option<&Column> {
    if case.expr.is_some() {
        return None;
    }
    if let Some(column) = fill_case_column(case) {
        return Some(column);
    }
    let mut found: Option<&Column> = None;
    for (when, then) in &case.when_then_expr {
        let column = case_when_column(when)?;
        if found.is_some_and(|prior| prior != column) {
            return None;
        }
        found = Some(column);
        if !const_expr(then) {
            return None;
        }
    }
    match case.else_expr.as_deref() {
        None => {}
        Some(other) if const_expr(other) => {}
        Some(other) => {
            let column = unwrapped_column(other)?;
            if found.is_some_and(|prior| prior != column) {
                return None;
            }
            found = Some(column);
        }
    }
    found
}

fn fill_case_column(case: &Case) -> Option<&Column> {
    let [(when, then)] = case.when_then_expr.as_slice() else {
        return None;
    };
    let (Expr::IsNotNull(inner) | Expr::IsNull(inner)) = when.as_ref() else {
        return None;
    };
    let column = unwrapped_column(inner)?;
    let then_column = unwrapped_column(then)?;
    if then_column != column {
        return None;
    }
    let other = case.else_expr.as_deref()?;
    if !const_expr(other) {
        return None;
    }
    Some(column)
}

fn case_when_column(when: &Expr) -> Option<&Column> {
    let Expr::BinaryExpr(binary) = when else {
        return None;
    };
    if !matches!(binary.op, Operator::Eq) {
        return None;
    }
    let (left, right) = (binary.left.as_ref(), binary.right.as_ref());
    if let Some(column) = when_column(left)
        && const_expr(right)
    {
        return Some(column);
    }
    if let Some(column) = when_column(right)
        && const_expr(left)
    {
        return Some(column);
    }
    None
}

fn alias_has_attr(alias: &Alias) -> bool {
    alias
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.inner().contains_key(ATTR_KEY))
}
