use std::ops::ControlFlow;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, JoinConstraint as PlanConstraint, Result};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{Expr, Join, JoinType, LogicalPlan};
use datafusion::sql::sqlparser::ast::{
    ExceptSelectItem, Expr as SqlExpr, GroupByExpr, Ident, JoinConstraint, JoinOperator,
    ObjectName, ObjectNamePart, OrderByKind, Query, ReplaceSelectElement, ReplaceSelectItem,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Visit, VisitMut, Visitor,
    VisitorMut,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::fold::constraint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
    Both,
}

struct Merged {
    key: Ident,
    expr: SqlExpr,
    plain: bool,
}

fn side_of(operator: &JoinOperator) -> Option<Side> {
    match operator {
        JoinOperator::Join(_)
        | JoinOperator::Inner(_)
        | JoinOperator::Left(_)
        | JoinOperator::LeftOuter(_)
        | JoinOperator::Semi(_)
        | JoinOperator::LeftSemi(_)
        | JoinOperator::Anti(_)
        | JoinOperator::LeftAnti(_) => Some(Side::Left),
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => Some(Side::Right),
        JoinOperator::FullOuter(_) => Some(Side::Both),
        _ => None,
    }
}

fn qualifier_of(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(alias) => Some(alias.name.to_string()),
            None => match name.0.last() {
                Some(ObjectNamePart::Identifier(ident)) => Some(ident.to_string()),
                _ => None,
            },
        },
        TableFactor::Derived {
            alias: Some(alias), ..
        } => Some(alias.name.to_string()),
        _ => None,
    }
}

fn using_idents(columns: &[ObjectName]) -> Option<Vec<&Ident>> {
    columns
        .iter()
        .map(|column| match column.0.as_slice() {
            [ObjectNamePart::Identifier(ident)] => Some(ident),
            _ => None,
        })
        .collect()
}

fn same_key(left: &Ident, right: &Ident, insensitive: bool) -> bool {
    if insensitive && left.quote_style.is_none() && right.quote_style.is_none() {
        left.value.eq_ignore_ascii_case(&right.value)
    } else {
        left.value == right.value
    }
}

fn parsed(text: &str) -> Option<SqlExpr> {
    Parser::new(&DatabricksDialect {})
        .try_with_sql(text)
        .ok()?
        .parse_expr()
        .ok()
}

fn merged_keys(from: &[TableWithJoins], insensitive: bool) -> Vec<Merged> {
    let [table] = from else {
        return Vec::new();
    };
    let Some(first) = qualifier_of(&table.relation) else {
        return Vec::new();
    };
    let mut held: Option<Vec<(Ident, String, bool)>> = None;
    for join in &table.joins {
        let Some(side) = side_of(&join.join_operator) else {
            return Vec::new();
        };
        let Some(JoinConstraint::Using(columns)) = constraint(&join.join_operator) else {
            return Vec::new();
        };
        let Some(idents) = using_idents(columns) else {
            return Vec::new();
        };
        let right = qualifier_of(&join.relation);
        let mut next = Vec::new();
        for ident in idents {
            let before = match &held {
                None => Some((format!("{first}.{ident}"), true)),
                Some(keys) => keys
                    .iter()
                    .find(|(key, _, _)| same_key(key, ident, insensitive))
                    .map(|(_, text, plain)| (text.clone(), *plain)),
            };
            let Some((before, plain)) = before else {
                continue;
            };
            let entry = match (side, &right) {
                (Side::Left, _) => (before, plain),
                (Side::Right, Some(right)) => (format!("{right}.{ident}"), false),
                (Side::Both, Some(right)) => {
                    (format!("coalesce({before}, {right}.{ident})"), false)
                }
                _ => return Vec::new(),
            };
            next.push((ident.clone(), entry.0, entry.1));
        }
        held = Some(next);
    }
    held.unwrap_or_default()
        .into_iter()
        .filter_map(|(key, text, plain)| parsed(&text).map(|expr| Merged { key, expr, plain }))
        .collect()
}

struct KeyRefs<'a> {
    keys: &'a [Merged],
    skipped: &'a [Ident],
    insensitive: bool,
    depth: usize,
}

impl VisitorMut for KeyRefs<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        let SqlExpr::Identifier(ident) = expr else {
            return ControlFlow::Continue(());
        };
        if self
            .skipped
            .iter()
            .any(|skipped| same_key(skipped, ident, self.insensitive))
        {
            return ControlFlow::Continue(());
        }
        if let Some(merged) = self
            .keys
            .iter()
            .find(|merged| same_key(&merged.key, ident, self.insensitive))
        {
            *expr = merged.expr.clone();
        }
        ControlFlow::Continue(())
    }
}

fn names_side_key(parts: &[Ident], keys: &[Merged], insensitive: bool) -> bool {
    parts.len() > 1
        && parts.last().is_some_and(|last| {
            keys.iter()
                .any(|merged| !merged.plain && same_key(&merged.key, last, insensitive))
        })
}

fn projection_conflicts(select: &Select, keys: &[Merged], insensitive: bool) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::QualifiedWildcard(..) => true,
        SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
            names_side_key(parts, keys, insensitive)
        }
        _ => false,
    })
}

fn rewrite_select(
    select: &mut Select,
    keys: &[Merged],
    outputs: bool,
    insensitive: bool,
) -> (bool, bool) {
    let outputs = outputs && !projection_conflicts(select, keys, insensitive);
    let active = outputs || keys.iter().all(|merged| merged.plain);
    let mut replaced = false;
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) if outputs => {
                if let Some(merged) = keys
                    .iter()
                    .find(|merged| !merged.plain && same_key(&merged.key, ident, insensitive))
                {
                    replaced = true;
                    *item = SelectItem::ExprWithAlias {
                        expr: merged.expr.clone(),
                        alias: ident.clone(),
                    };
                }
            }
            SelectItem::Wildcard(options) if outputs && options.opt_replace.is_none() => {
                let items = keys
                    .iter()
                    .filter(|merged| !merged.plain)
                    .map(|merged| {
                        Box::new(ReplaceSelectElement {
                            expr: merged.expr.clone(),
                            column_name: merged.key.clone(),
                            as_keyword: true,
                        })
                    })
                    .collect::<Vec<_>>();
                if !items.is_empty() {
                    replaced = true;
                    options.opt_replace = Some(ReplaceSelectItem { items });
                }
            }
            _ => {}
        }
    }
    if active {
        let mut refs = KeyRefs {
            keys,
            skipped: &[],
            insensitive,
            depth: 0,
        };
        let _ = VisitMut::visit(&mut select.projection, &mut refs);
        let _ = VisitMut::visit(&mut select.selection, &mut refs);
        let _ = VisitMut::visit(&mut select.group_by, &mut refs);
        let _ = VisitMut::visit(&mut select.having, &mut refs);
        let _ = VisitMut::visit(&mut select.qualify, &mut refs);
    }
    (active, replaced)
}

struct SideOrder<'a> {
    keys: &'a [Merged],
    insensitive: bool,
    found: bool,
}

impl Visitor for SideOrder<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        if let SqlExpr::CompoundIdentifier(parts) = expr
            && names_side_key(parts, self.keys, self.insensitive)
        {
            self.found = true;
        }
        ControlFlow::Continue(())
    }
}

fn holds_compound(expr: &SqlExpr) -> bool {
    struct Compound(bool);
    impl Visitor for Compound {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
            if matches!(expr, SqlExpr::CompoundIdentifier(_)) {
                self.0 = true;
            }
            ControlFlow::Continue(())
        }
    }
    let mut found = Compound(false);
    let _ = Visit::visit(expr, &mut found);
    found.0
}

fn plain_rows(select: &Select) -> bool {
    let grouped = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => !exprs.is_empty() || !modifiers.is_empty(),
        GroupByExpr::All(_) => true,
    };
    select.distinct.is_none() && !grouped && select.having.is_none() && select.qualify.is_none()
}

const WRAPPER: &str = "SELECT * FROM (SELECT 1) AS __repark_using_w";

fn wrap_side_order(query: &mut Query) -> Option<()> {
    let OrderByKind::Expressions(ordered) = &mut query.order_by.as_mut()?.kind else {
        return None;
    };
    let mut carried = Vec::new();
    for (index, item) in ordered.iter_mut().enumerate() {
        if !holds_compound(&item.expr) {
            continue;
        }
        let name = Ident::new(format!("__repark_using_o{index}"));
        let expr = std::mem::replace(&mut item.expr, SqlExpr::Identifier(name.clone()));
        carried.push((expr, name));
    }
    let (first, rest) = carried.split_first()?;
    let Statement::Query(mut outer) = Parser::new(&DatabricksDialect {})
        .try_with_sql(WRAPPER)
        .ok()?
        .parse_statement()
        .ok()?
    else {
        return None;
    };
    let SetExpr::Select(outer_select) = outer.body.as_mut() else {
        return None;
    };
    let [SelectItem::Wildcard(options)] = outer_select.projection.as_mut_slice() else {
        return None;
    };
    options.opt_except = Some(ExceptSelectItem {
        first_element: first.1.clone(),
        additional_elements: rest.iter().map(|(_, name)| name.clone()).collect(),
    });
    let [table] = outer_select.from.as_mut_slice() else {
        return None;
    };
    let TableFactor::Derived { subquery, .. } = &mut table.relation else {
        return None;
    };
    let SetExpr::Select(inner) = query.body.as_mut() else {
        return None;
    };
    for (expr, alias) in &carried {
        inner.projection.push(SelectItem::ExprWithAlias {
            expr: expr.clone(),
            alias: alias.clone(),
        });
    }
    std::mem::swap(&mut subquery.body, &mut query.body);
    std::mem::swap(&mut outer.body, &mut query.body);
    Some(())
}

fn select_has_using(select: &Select) -> bool {
    select.from.iter().any(|table| {
        table.joins.iter().any(|join| {
            matches!(
                constraint(&join.join_operator),
                Some(JoinConstraint::Using(_) | JoinConstraint::Natural)
            )
        })
    })
}

struct UsingSelects {
    insensitive: bool,
    found: bool,
}

impl UsingSelects {
    fn top_select(&mut self, query: &mut Query) {
        let Query { body, order_by, .. } = query;
        let SetExpr::Select(select) = body.as_mut() else {
            return;
        };
        if !select_has_using(select) {
            return;
        }
        self.found = true;
        let keys = merged_keys(&select.from, self.insensitive);
        if keys.is_empty() {
            return;
        }
        let mut side = SideOrder {
            keys: &keys,
            insensitive: self.insensitive,
            found: false,
        };
        let _ = Visit::visit(order_by, &mut side);
        let outputs = !side.found || plain_rows(select);
        let shadowed = select
            .projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let (active, replaced) = rewrite_select(select, &keys, outputs, self.insensitive);
        if side.found && replaced && wrap_side_order(query).is_some() {
            return;
        }
        if active {
            let mut refs = KeyRefs {
                keys: &keys,
                skipped: &shadowed,
                insensitive: self.insensitive,
                depth: 0,
            };
            let _ = VisitMut::visit(&mut query.order_by, &mut refs);
        }
    }
}

impl VisitorMut for UsingSelects {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if matches!(query.body.as_ref(), SetExpr::Select(_)) {
            self.top_select(query);
            return ControlFlow::Continue(());
        }
        let mut pending = vec![query.body.as_mut()];
        while let Some(node) = pending.pop() {
            match node {
                SetExpr::Select(select) if select_has_using(select) => {
                    self.found = true;
                    let keys = merged_keys(&select.from, self.insensitive);
                    if !keys.is_empty() {
                        rewrite_select(select, &keys, true, self.insensitive);
                    }
                }
                SetExpr::SetOperation { left, right, .. } => {
                    pending.push(right.as_mut());
                    pending.push(left.as_mut());
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn rewrite_using_keys(statement: &mut Statement, insensitive: bool) -> bool {
    let mut selects = UsingSelects {
        insensitive,
        found: false,
    };
    let _ = VisitMut::visit(statement, &mut selects);
    selects.found
}

fn columns_of(expr: &Expr) -> Vec<Column> {
    let mut columns = Vec::new();
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node {
            columns.push(column.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    columns
}

fn merged_below(child: &Join, wanted: &Column) -> Option<Expr> {
    if !matches!(child.join_constraint, PlanConstraint::Using) {
        return None;
    }
    child.on.iter().find_map(|(left, right)| {
        let held = columns_of(left)
            .into_iter()
            .chain(columns_of(right))
            .any(|column| &column == wanted);
        if !held {
            return None;
        }
        match child.join_type {
            JoinType::Right => Some(right.clone()),
            JoinType::Full => Some(coalesce(vec![left.clone(), right.clone()])),
            _ => Some(left.clone()),
        }
    })
}

fn rekey_join(join: Join) -> Transformed<LogicalPlan> {
    let LogicalPlan::Join(child) = join.left.as_ref() else {
        return Transformed::no(LogicalPlan::Join(join));
    };
    if !matches!(join.join_constraint, PlanConstraint::Using) {
        return Transformed::no(LogicalPlan::Join(join));
    }
    let mut changed = false;
    let on = join
        .on
        .iter()
        .map(|(left, right)| {
            let merged = match left {
                Expr::Column(column) => merged_below(child, column),
                _ => None,
            };
            match merged {
                Some(merged) if &merged != left => {
                    changed = true;
                    (merged, right.clone())
                }
                _ => (left.clone(), right.clone()),
            }
        })
        .collect::<Vec<_>>();
    if !changed {
        return Transformed::no(LogicalPlan::Join(join));
    }
    Transformed::yes(LogicalPlan::Join(Join { on, ..join }))
}

#[allow(clippy::missing_errors_doc)]
pub(super) fn rekey_chained_using(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| match node {
        LogicalPlan::Join(join) => Ok(rekey_join(join)),
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
}
