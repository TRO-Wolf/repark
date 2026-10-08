use std::ops::ControlFlow;

use datafusion::execution::session_state::SessionState;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Visit, VisitMut,
    Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Location, Span};

use super::fold::constraint;

pub(super) const EXPLICIT_LINE: u64 = u64::MAX - 11;

fn inner_mut(statement: &mut DfStatement) -> Option<&mut Statement> {
    match statement {
        DfStatement::Statement(inner) => Some(inner.as_mut()),
        DfStatement::Explain(explain) => inner_mut(explain.statement.as_mut()),
        _ => None,
    }
}

fn inner_ref(statement: &DfStatement) -> Option<&Statement> {
    match statement {
        DfStatement::Statement(inner) => Some(inner.as_ref()),
        DfStatement::Explain(explain) => inner_ref(explain.statement.as_ref()),
        _ => None,
    }
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

struct FindUsing(bool);

impl Visitor for FindUsing {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        let mut pending = vec![query.body.as_ref()];
        while let Some(node) = pending.pop() {
            match node {
                SetExpr::Select(select) if select_has_using(select) => {
                    self.0 = true;
                    return ControlFlow::Break(());
                }
                SetExpr::SetOperation { left, right, .. } => {
                    pending.push(right.as_ref());
                    pending.push(left.as_ref());
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn names_using(statement: &DfStatement) -> bool {
    let Some(inner) = inner_ref(statement) else {
        return false;
    };
    let mut found = FindUsing(false);
    let _ = Visit::visit(inner, &mut found);
    found.0
}

pub(super) fn spanned_state(state: &SessionState) -> SessionState {
    let mut spanned = state.clone();
    spanned.config_mut().options_mut().sql_parser.collect_spans = true;
    spanned
}

struct MarkExplicit;

impl VisitorMut for MarkExplicit {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if let SqlExpr::CompoundIdentifier(parts) = expr {
            let mark = Span::new(
                Location::new(EXPLICIT_LINE, 1),
                Location::new(EXPLICIT_LINE, 2),
            );
            for part in parts {
                part.span = mark;
            }
        }
        ControlFlow::Continue(())
    }
}

const ORDER_MARK: &str = "__repark_using_order";

struct Slot(Option<SqlExpr>);

impl VisitorMut for Slot {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if matches!(expr, SqlExpr::Identifier(ident) if ident.value == ORDER_MARK)
            && let Some(held) = self.0.take()
        {
            *expr = held;
        }
        ControlFlow::Continue(())
    }
}

fn order_marked(held: SqlExpr) -> Option<SqlExpr> {
    let mut wrapped = Parser::new(&DatabricksDialect {})
        .try_with_sql(&format!("coalesce(coalesce({ORDER_MARK}))"))
        .ok()?
        .parse_expr()
        .ok()?;
    let mut slot = Slot(Some(held));
    let _ = VisitMut::visit(&mut wrapped, &mut slot);
    slot.0.is_none().then_some(wrapped)
}

struct OrderRefs {
    depth: usize,
}

impl VisitorMut for OrderRefs {
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
        if self.depth == 0
            && matches!(expr, SqlExpr::CompoundIdentifier(parts) if parts.len() > 1)
            && let Some(marked) = order_marked(expr.clone())
        {
            *expr = marked;
        }
        ControlFlow::Continue(())
    }
}

struct SideRefs<'a> {
    keys: &'a [Ident],
    any: bool,
    depth: usize,
    found: bool,
}

impl Visitor for SideRefs<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        if self.depth == 0
            && let SqlExpr::CompoundIdentifier(parts) = expr
            && let Some(last) = parts.last()
            && parts.len() > 1
            && (self.any
                || self
                    .keys
                    .iter()
                    .any(|key| key.value.eq_ignore_ascii_case(&last.value)))
        {
            self.found = true;
        }
        ControlFlow::Continue(())
    }
}

fn grouped(select: &Select) -> bool {
    let keyed = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => !exprs.is_empty() || !modifiers.is_empty(),
        GroupByExpr::All(_) => true,
    };
    keyed || select.having.is_some()
}

fn grouped_side_refs(select: &Select, query: Option<&Query>) -> bool {
    if !grouped(select) {
        return false;
    }
    let (keys, any) = select_keys(select);
    let mut refs = SideRefs {
        keys: &keys,
        any,
        depth: 0,
        found: false,
    };
    let _ = Visit::visit(&select.projection, &mut refs);
    let _ = Visit::visit(&select.having, &mut refs);
    if let Some(query) = query {
        let _ = Visit::visit(&query.order_by, &mut refs);
    }
    refs.found
}

struct KeyMix<'a> {
    keys: &'a [Ident],
    any: bool,
    depth: usize,
    bare: bool,
    side: bool,
}

impl Visitor for KeyMix<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        let named = |ident: &Ident| {
            self.any
                || self
                    .keys
                    .iter()
                    .any(|key| key.value.eq_ignore_ascii_case(&ident.value))
        };
        match expr {
            SqlExpr::Identifier(ident) if named(ident) => self.bare = true,
            SqlExpr::CompoundIdentifier(parts)
                if parts.len() > 1 && parts.last().is_some_and(named) =>
            {
                self.side = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn select_keys(select: &Select) -> (Vec<Ident>, bool) {
    let mut keys = Vec::new();
    let mut any = false;
    for join in select.from.iter().flat_map(|table| table.joins.iter()) {
        match constraint(&join.join_operator) {
            Some(JoinConstraint::Using(columns)) => {
                keys.extend(
                    using_idents(columns)
                        .unwrap_or_default()
                        .into_iter()
                        .cloned(),
                );
            }
            Some(JoinConstraint::Natural) => any = true,
            _ => {}
        }
    }
    (keys, any)
}

fn mixes_key_and_side(select: &Select, query: Option<&Query>) -> bool {
    let (keys, any) = select_keys(select);
    let mut refs = KeyMix {
        keys: &keys,
        any,
        depth: 0,
        bare: false,
        side: false,
    };
    let _ = Visit::visit(&select.projection, &mut refs);
    let _ = Visit::visit(&select.selection, &mut refs);
    let _ = Visit::visit(&select.group_by, &mut refs);
    let _ = Visit::visit(&select.having, &mut refs);
    if let Some(query) = query {
        let _ = Visit::visit(&query.order_by, &mut refs);
    }
    refs.bare && refs.side
}

struct MarkOrder {
    starred: bool,
}

impl VisitorMut for MarkOrder {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let top = matches!(query.body.as_ref(), SetExpr::Select(_));
        let mut pending = vec![query.body.as_ref()];
        while let Some(node) = pending.pop() {
            match node {
                SetExpr::Select(select) if select_has_using(select) => {
                    self.starred |= select
                        .projection
                        .iter()
                        .any(|item| matches!(item, SelectItem::QualifiedWildcard(..)))
                        || select.from.len() > 1
                        || grouped_side_refs(select, top.then_some(&*query))
                        || select.qualify.is_some()
                        || mixes_key_and_side(select, top.then_some(&*query));
                }
                SetExpr::SetOperation { left, right, .. } => {
                    pending.push(right.as_ref());
                    pending.push(left.as_ref());
                }
                _ => {}
            }
        }
        let using =
            matches!(query.body.as_ref(), SetExpr::Select(select) if select_has_using(select));
        if using {
            let _ = VisitMut::visit(&mut query.order_by, &mut OrderRefs { depth: 0 });
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn mark_explicit(statement: &mut DfStatement) -> bool {
    let Some(inner) = inner_mut(statement) else {
        return false;
    };
    let _ = VisitMut::visit(inner, &mut MarkExplicit);
    let mut order = MarkOrder { starred: false };
    let _ = VisitMut::visit(inner, &mut order);
    !order.starred
}

fn qualifier_of(factor: &TableFactor) -> Option<Vec<Ident>> {
    match factor {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(alias) => Some(vec![alias.name.clone()]),
            None => name
                .0
                .iter()
                .map(|part| match part {
                    ObjectNamePart::Identifier(ident) => Some(ident.clone()),
                    ObjectNamePart::Function(_) => None,
                })
                .collect(),
        },
        TableFactor::Derived {
            alias: Some(alias), ..
        } => Some(vec![alias.name.clone()]),
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

fn shared_keys(from: &[TableWithJoins], insensitive: bool) -> Option<(Vec<Ident>, Vec<Ident>)> {
    let [table] = from else {
        return None;
    };
    let first = qualifier_of(&table.relation)?;
    let mut held: Option<Vec<Ident>> = None;
    for join in &table.joins {
        if !matches!(
            join.join_operator,
            JoinOperator::Join(_)
                | JoinOperator::Inner(_)
                | JoinOperator::Left(_)
                | JoinOperator::LeftOuter(_)
                | JoinOperator::Right(_)
                | JoinOperator::RightOuter(_)
                | JoinOperator::FullOuter(_)
        ) {
            return None;
        }
        let Some(JoinConstraint::Using(columns)) = constraint(&join.join_operator) else {
            return None;
        };
        let idents = using_idents(columns)?;
        held = Some(match held {
            None => idents.into_iter().cloned().collect(),
            Some(keys) => keys
                .into_iter()
                .filter(|key| idents.iter().any(|ident| same_key(key, ident, insensitive)))
                .collect(),
        });
    }
    let keys = held?;
    (!keys.is_empty()).then_some((first, keys))
}

struct KeyRefs<'a> {
    first: &'a [Ident],
    keys: &'a [Ident],
    skipped: &'a [Ident],
    insensitive: bool,
    depth: usize,
    lambdas: Vec<Vec<Ident>>,
    changed: bool,
}

impl KeyRefs<'_> {
    fn shadowed(&self, ident: &Ident) -> bool {
        self.lambdas
            .iter()
            .flatten()
            .chain(self.skipped.iter())
            .any(|held| same_key(held, ident, self.insensitive))
    }
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

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if let SqlExpr::Lambda(lambda) = expr {
            self.lambdas.push(
                lambda
                    .params
                    .iter()
                    .map(|param| param.name.clone())
                    .collect(),
            );
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if matches!(expr, SqlExpr::Lambda(_)) {
            self.lambdas.pop();
            return ControlFlow::Continue(());
        }
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        let SqlExpr::Identifier(ident) = expr else {
            return ControlFlow::Continue(());
        };
        if self.shadowed(ident)
            || !self
                .keys
                .iter()
                .any(|key| same_key(key, ident, self.insensitive))
        {
            return ControlFlow::Continue(());
        }
        let mut parts = self.first.to_vec();
        parts.push(ident.clone());
        for part in &mut parts {
            part.span = Span::empty();
        }
        *expr = SqlExpr::CompoundIdentifier(parts);
        self.changed = true;
        ControlFlow::Continue(())
    }
}

struct QualifyKeys {
    insensitive: bool,
    changed: bool,
}

impl QualifyKeys {
    fn select(
        &mut self,
        select: &mut Select,
        skipped: &[Ident],
    ) -> Option<(Vec<Ident>, Vec<Ident>)> {
        let (first, keys) = shared_keys(&select.from, self.insensitive)?;
        let mut refs = KeyRefs {
            first: &first,
            keys: &keys,
            skipped,
            insensitive: self.insensitive,
            depth: 0,
            lambdas: Vec::new(),
            changed: false,
        };
        let _ = VisitMut::visit(&mut select.projection, &mut refs);
        let _ = VisitMut::visit(&mut select.selection, &mut refs);
        let _ = VisitMut::visit(&mut select.group_by, &mut refs);
        let _ = VisitMut::visit(&mut select.having, &mut refs);
        let _ = VisitMut::visit(&mut select.qualify, &mut refs);
        self.changed |= refs.changed;
        Some((first, keys))
    }
}

impl VisitorMut for QualifyKeys {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let mut ordered = None;
        let mut pending = vec![(query.body.as_mut(), true)];
        while let Some((node, top)) = pending.pop() {
            match node {
                SetExpr::Select(select) if select_has_using(select) => {
                    let shadowed = select
                        .projection
                        .iter()
                        .filter_map(|item| match item {
                            SelectItem::ExprWithAlias { alias, .. } => Some(alias.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    if let Some(found) = self.select(select, &[])
                        && top
                    {
                        ordered = Some((found, shadowed));
                    }
                }
                SetExpr::SetOperation { left, right, .. } => {
                    pending.push((right.as_mut(), false));
                    pending.push((left.as_mut(), false));
                }
                _ => {}
            }
        }
        if let Some(((first, keys), shadowed)) = ordered {
            let mut refs = KeyRefs {
                first: &first,
                keys: &keys,
                skipped: &shadowed,
                insensitive: self.insensitive,
                depth: 0,
                lambdas: Vec::new(),
                changed: false,
            };
            let _ = VisitMut::visit(&mut query.order_by, &mut refs);
            self.changed |= refs.changed;
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn qualify_keys(statement: &mut DfStatement, insensitive: bool) -> bool {
    let Some(inner) = inner_mut(statement) else {
        return false;
    };
    let mut keys = QualifyKeys {
        insensitive,
        changed: false,
    };
    let _ = VisitMut::visit(inner, &mut keys);
    keys.changed
}
