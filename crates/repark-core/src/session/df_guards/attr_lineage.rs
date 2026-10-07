use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::expr::{Alias, Case};
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, Operator, Projection};
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

fn sort_projection(plan: &LogicalPlan) -> Option<&Projection> {
    let mut node = plan;
    loop {
        let below = below_transparent(node);
        if !std::ptr::eq(below, node) {
            node = below;
            continue;
        }
        if let LogicalPlan::Projection(projection) = node {
            return Some(projection);
        }
        return None;
    }
}

fn visible_name(field: &str) -> &str {
    let Some(rest) = field.strip_prefix("__repark_") else {
        return field;
    };
    let mut parts = rest.splitn(4, '_');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(tag), Some(plan), Some(index), Some(display))
            if tag.len() == 1
                && (tag == "l" || tag == "r")
                && !plan.is_empty()
                && plan.bytes().all(|byte| byte.is_ascii_alphanumeric())
                && !index.is_empty()
                && index.bytes().all(|byte| byte.is_ascii_digit())
                && !display.is_empty() =>
        {
            display
        }
        _ => field,
    }
}

fn visible_positions(schema: &DFSchema, written: &str, rule: NameRule) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| rule.matches(written, visible_name(field.name())))
        .map(|(position, _)| position)
        .collect()
}

fn single_child(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Projection(projection) => Some(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => Some(filter.input.as_ref()),
        LogicalPlan::Sort(sort) => Some(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => Some(limit.input.as_ref()),
        LogicalPlan::Repartition(repartition) => Some(repartition.input.as_ref()),
        LogicalPlan::Distinct(Distinct::All(input)) => Some(input.as_ref()),
        LogicalPlan::Distinct(Distinct::On(on)) => Some(on.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => Some(alias.input.as_ref()),
        _ => None,
    }
}

fn nearest_visible_below(
    input: &LogicalPlan,
    written: &str,
    rule: NameRule,
) -> Option<(usize, usize)> {
    let mut node = input;
    let mut depth = 1;
    loop {
        let child = single_child(node)?;
        depth += 1;
        let matches = visible_positions(child.schema(), written, rule);
        if matches.len() > 1 {
            return None;
        }
        if let [position] = matches.as_slice() {
            return Some((depth, *position));
        }
        node = child;
    }
}

fn lineage_passes_through(
    projection: &Projection,
    hit: usize,
    depth: usize,
    position: usize,
) -> bool {
    let Some(top) = projection.expr.get(hit).and_then(plain_projection_source) else {
        return false;
    };
    let mut node = projection.input.as_ref();
    let Some(mut at) = node.schema().maybe_index_of_column(top) else {
        return false;
    };
    let mut level = 1;
    loop {
        let Some(child) = single_child(node) else {
            return false;
        };
        level += 1;
        if let LogicalPlan::Projection(inner) = node {
            let Some(column) = inner.expr.get(at).and_then(plain_projection_source) else {
                return false;
            };
            let Some(below) = child.schema().maybe_index_of_column(column) else {
                return false;
            };
            at = below;
        }
        if level == depth {
            return at == position;
        }
    }
}

#[must_use]
pub fn sort_input_carries_twice(plan: &LogicalPlan, written: &str, rule: NameRule) -> bool {
    sort_projection(plan).is_some_and(|projection| {
        visible_positions(projection.input.schema(), written, rule).len() > 1
    })
}

#[must_use]
pub fn sort_sourced_twin_engine(
    plan: &LogicalPlan,
    hits: &[usize],
    written: &str,
    rule: NameRule,
) -> Option<String> {
    let projection = sort_projection(plan)?;
    if !visible_positions(projection.input.schema(), written, rule).is_empty() {
        return None;
    }
    let Some((depth, position)) = nearest_visible_below(projection.input.as_ref(), written, rule)
    else {
        return None;
    };
    let mut found = None;
    for hit in hits {
        if !lineage_passes_through(projection, *hit, depth, position) {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = Some(projection.schema.fields().get(*hit)?.name().clone());
    }
    found
}

fn plain_projection_source(expr: &Expr) -> Option<&Column> {
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
