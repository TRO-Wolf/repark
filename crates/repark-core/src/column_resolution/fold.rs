use std::collections::HashMap;
use std::ops::ControlFlow;

use datafusion::common::Column;
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, Expr as SqlExpr, FromTable, GroupByExpr, Ident, JoinConstraint, JoinOperator,
    ObjectName, ObjectNamePart, OrderByKind, Query, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, TableWithJoins, UpdateTableFromKind, VisitMut, VisitorMut,
};

use super::{WrittenRefs, ambiguous_message, part_value, reference_parts};

#[derive(Default)]
pub(super) struct Known {
    tables: HashMap<Vec<String>, Vec<String>>,
    derived: HashMap<String, Vec<String>>,
    loose: Vec<(Vec<String>, String)>,
}

impl Known {
    pub(super) fn with_tables(tables: HashMap<Vec<String>, Vec<String>>) -> Self {
        Self {
            tables,
            ..Self::default()
        }
    }

    pub(super) fn absorb(&mut self, valid_fields: &[Column]) {
        for field in valid_fields {
            let key = field
                .relation
                .as_ref()
                .map_or_else(String::new, |relation| {
                    relation.table().to_ascii_lowercase()
                });
            let names = self.derived.entry(key).or_default();
            if !names.contains(&field.name) {
                names.push(field.name.clone());
            }
            let display = field
                .relation
                .as_ref()
                .map(reference_parts)
                .unwrap_or_default();
            if !self
                .loose
                .iter()
                .any(|(scope, name)| *scope == display && *name == field.name)
            {
                self.loose.push((display, field.name.clone()));
            }
        }
    }
}

pub(super) fn normalized_parts(name: &ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) if ident.quote_style.is_some() => {
                Some(ident.value.clone())
            }
            ObjectNamePart::Identifier(ident) => Some(ident.value.to_lowercase()),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

struct Relation {
    name: String,
    display: Vec<String>,
    fields: Option<Vec<String>>,
}

enum Lookup {
    Unknown,
    Hits(Vec<(Vec<String>, String)>),
}

fn relations_of(
    tables: &[TableWithJoins],
    known: &Known,
    ctes: &[String],
    into: &mut Vec<Relation>,
) {
    for table in tables {
        relation_of(&table.relation, known, ctes, into);
        for join in &table.joins {
            relation_of(&join.relation, known, ctes, into);
        }
    }
}

fn table_relation(
    name: &ObjectName,
    alias: Option<&Ident>,
    known: &Known,
    ctes: &[String],
) -> Relation {
    let written = name
        .0
        .iter()
        .filter_map(|part| part_value(part).map(str::to_string))
        .collect::<Vec<_>>();
    let last = written.last().cloned().unwrap_or_default();
    let shadowed = written.len() == 1 && ctes.iter().any(|cte| cte.eq_ignore_ascii_case(&last));
    let (relation_name, display) = match alias {
        Some(alias) => (alias.value.clone(), vec![alias.value.clone()]),
        None => (last, written),
    };
    let catalog = if shadowed {
        None
    } else {
        normalized_parts(name).and_then(|parts| known.tables.get(&parts).cloned())
    };
    let fields = catalog.or_else(|| {
        known
            .derived
            .get(&relation_name.to_ascii_lowercase())
            .cloned()
    });
    Relation {
        name: relation_name,
        display,
        fields,
    }
}

fn relation_of(factor: &TableFactor, known: &Known, ctes: &[String], into: &mut Vec<Relation>) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            into.push(table_relation(
                name,
                alias.as_ref().map(|alias| &alias.name),
                known,
                ctes,
            ));
        }
        TableFactor::Derived { alias, .. } => {
            let name = alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_default();
            let fields = if name.is_empty() {
                None
            } else {
                known.derived.get(&name.to_ascii_lowercase()).cloned()
            };
            into.push(Relation {
                display: vec![name.clone()],
                name,
                fields,
            });
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => relations_of(std::slice::from_ref(table_with_joins), known, ctes, into),
        _ => into.push(Relation {
            name: String::new(),
            display: Vec::new(),
            fields: None,
        }),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Slot {
    Projection,
    AliasReference,
    Plain,
}

struct Level {
    selects: Vec<Vec<Relation>>,
    aliases: Vec<String>,
    ctes: Vec<String>,
    slots: HashMap<*const SqlExpr, (usize, Slot)>,
    active: Vec<(*const SqlExpr, usize, Slot)>,
}

impl Level {
    fn of(query: &Query, known: &Known, outer_ctes: &[String]) -> Self {
        let mut ctes = outer_ctes.to_vec();
        if let Some(with) = query.with.as_ref() {
            ctes.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.clone()),
            );
        }
        let mut level = Self {
            selects: Vec::new(),
            aliases: Vec::new(),
            ctes,
            slots: HashMap::new(),
            active: Vec::new(),
        };
        level.collect(&query.body, known);
        if let Some(order_by) = query.order_by.as_ref()
            && let OrderByKind::Expressions(exprs) = &order_by.kind
        {
            for order in exprs {
                level
                    .slots
                    .insert(std::ptr::from_ref(&order.expr), (0, Slot::AliasReference));
            }
        }
        level
    }

    fn collect(&mut self, body: &SetExpr, known: &Known) {
        match body {
            SetExpr::Select(select) => {
                let index = self.selects.len();
                let mut relations = Vec::new();
                relations_of(&select.from, known, &self.ctes, &mut relations);
                self.selects.push(relations);
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            self.slot(expr, index, Slot::Projection);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            self.slot(expr, index, Slot::Projection);
                            self.aliases.push(alias.value.clone());
                        }
                        _ => {}
                    }
                }
                if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                    for expr in exprs {
                        self.slot(expr, index, Slot::AliasReference);
                    }
                }
                for expr in select.having.iter().chain(select.qualify.iter()) {
                    self.slot(expr, index, Slot::AliasReference);
                }
                for order in &select.sort_by {
                    self.slot(&order.expr, index, Slot::AliasReference);
                }
                for expr in select.selection.iter().chain(select.prewhere.iter()) {
                    self.slot(expr, index, Slot::Plain);
                }
                for table in &select.from {
                    for join in &table.joins {
                        if let Some(JoinConstraint::On(expr)) = constraint(&join.join_operator) {
                            self.slot(expr, index, Slot::Plain);
                        }
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.collect(left, known);
                self.collect(right, known);
            }
            _ => {}
        }
    }

    fn slot(&mut self, expr: &SqlExpr, index: usize, slot: Slot) {
        self.slots.insert(std::ptr::from_ref(expr), (index, slot));
    }

    fn select(&self) -> usize {
        self.active.last().map_or(0, |(_, index, _)| *index)
    }

    fn relations(&self) -> &[Relation] {
        self.selects.get(self.select()).map_or(&[], Vec::as_slice)
    }

    fn shields(&self, ident: &str) -> bool {
        matches!(self.active.last(), Some((_, _, Slot::AliasReference)))
            && self
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(ident))
    }
}

fn constraint_mut(operator: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => Some(constraint),
        _ => None,
    }
}

fn constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => Some(constraint),
        _ => None,
    }
}

fn scan(relations: &[Relation], qualifier: Option<&str>, name: &str) -> Option<Lookup> {
    let mut hits = Vec::new();
    let mut opaque = false;
    let mut named = false;
    for relation in relations {
        if let Some(qualifier) = qualifier
            && !relation.name.eq_ignore_ascii_case(qualifier)
        {
            continue;
        }
        named = true;
        match &relation.fields {
            Some(fields) => hits.extend(
                fields
                    .iter()
                    .filter(|field| field.eq_ignore_ascii_case(name))
                    .map(|field| (relation.display.clone(), field.clone())),
            ),
            None => opaque = true,
        }
    }
    if !hits.is_empty() {
        return Some(Lookup::Hits(hits));
    }
    if opaque || (qualifier.is_some() && named) {
        return Some(Lookup::Unknown);
    }
    None
}

struct CaseFold<'a> {
    known: &'a Known,
    written: &'a WrittenRefs,
    levels: Vec<Level>,
    base: Vec<Relation>,
    target: Vec<Relation>,
    changed: bool,
    error: Option<DataFusionError>,
}

impl CaseFold<'_> {
    fn lookup(&self, qualifier: Option<&str>, name: &str) -> Lookup {
        for level in self.levels.iter().rev() {
            if let Some(found) = scan(level.relations(), qualifier, name) {
                return found;
            }
        }
        if let Some(found) = scan(&self.base, qualifier, name) {
            return found;
        }
        if self.levels.is_empty() && self.base.is_empty() {
            let hits = self
                .known
                .loose
                .iter()
                .filter(|(scope, field)| {
                    field.eq_ignore_ascii_case(name)
                        && qualifier.is_none_or(|qualifier| {
                            scope
                                .last()
                                .is_some_and(|table| table.eq_ignore_ascii_case(qualifier))
                        })
                })
                .cloned()
                .collect::<Vec<_>>();
            if !hits.is_empty() {
                return Lookup::Hits(hits);
            }
        }
        Lookup::Unknown
    }

    fn requested_spelling(&self, qualifier: Option<&str>, name: &str) -> String {
        if let Some(scope) = qualifier {
            return self
                .written
                .qualified
                .iter()
                .find(|(written_scope, written_name)| {
                    written_scope.eq_ignore_ascii_case(scope)
                        && written_name.eq_ignore_ascii_case(name)
                })
                .map_or_else(
                    || name.to_string(),
                    |(_, written_name)| written_name.clone(),
                );
        }
        self.written
            .projection
            .iter()
            .chain(self.written.bare.iter())
            .find(|written| written.eq_ignore_ascii_case(name))
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    fn rewrite_ident(&mut self, qualifier: Option<&str>, ident: &mut Ident) {
        if self.error.is_some()
            || self
                .levels
                .last()
                .is_some_and(|level| level.shields(ident.value.as_str()))
        {
            return;
        }
        let Lookup::Hits(hits) = self.lookup(qualifier, ident.value.as_str()) else {
            return;
        };
        self.apply(qualifier, ident, hits);
    }

    fn rewrite_target(&mut self, ident: &mut Ident) {
        if self.error.is_some() {
            return;
        }
        let Some(Lookup::Hits(hits)) = scan(&self.target, None, ident.value.as_str()) else {
            return;
        };
        self.apply(None, ident, hits);
    }

    fn apply(
        &mut self,
        qualifier: Option<&str>,
        ident: &mut Ident,
        hits: Vec<(Vec<String>, String)>,
    ) {
        let first = hits[0].1.clone();
        if hits.iter().any(|(_, stored)| *stored != first) {
            let spelling = self.requested_spelling(qualifier, ident.value.as_str());
            let scopes = hits
                .into_iter()
                .map(|(scope, _)| self.written.visible(scope))
                .collect::<Vec<_>>();
            self.error = Some(DataFusionError::Plan(ambiguous_message(
                qualifier,
                spelling.as_str(),
                &scopes,
            )));
            return;
        }
        let current = if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_ascii_lowercase()
        };
        if first != current {
            ident.value = first;
            ident.quote_style = Some('`');
            self.changed = true;
        }
    }

    fn fold_usings(&mut self, body: &mut SetExpr, index: &mut usize) {
        match body {
            SetExpr::Select(select) => {
                let current = *index;
                *index += 1;
                for table in &mut select.from {
                    for join in &mut table.joins {
                        let Some(JoinConstraint::Using(columns)) =
                            constraint_mut(&mut join.join_operator)
                        else {
                            continue;
                        };
                        for column in columns {
                            if let [ObjectNamePart::Identifier(ident)] = column.0.as_mut_slice() {
                                self.rewrite_in_select(current, ident);
                            }
                        }
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.fold_usings(left, index);
                self.fold_usings(right, index);
            }
            _ => {}
        }
    }

    fn rewrite_in_select(&mut self, index: usize, ident: &mut Ident) {
        let Some(level) = self.levels.last_mut() else {
            return;
        };
        level.active.push((std::ptr::null(), index, Slot::Plain));
        self.rewrite_ident(None, ident);
        if let Some(level) = self.levels.last_mut() {
            level.active.pop();
        }
    }

    fn outer_ctes(&self) -> Vec<String> {
        self.levels
            .last()
            .map(|level| level.ctes.clone())
            .unwrap_or_default()
    }
}

impl VisitorMut for CaseFold<'_> {
    type Break = std::convert::Infallible;

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Update(update) => {
                let mut tables = vec![update.table.clone()];
                if let Some(
                    UpdateTableFromKind::BeforeSet(from) | UpdateTableFromKind::AfterSet(from),
                ) = update.from.as_ref()
                {
                    tables.extend(from.iter().cloned());
                }
                relations_of(&tables, self.known, &[], &mut self.base);
                relation_of(&update.table.relation, self.known, &[], &mut self.target);
            }
            Statement::Delete(delete) => {
                let (FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables)) =
                    &delete.from;
                relations_of(tables, self.known, &[], &mut self.base);
            }
            Statement::Insert(insert) => {
                if let TableObject::TableName(name) = &insert.table {
                    self.target
                        .push(table_relation(name, None, self.known, &[]));
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let level = Level::of(query, self.known, &self.outer_ctes());
        self.levels.push(level);
        let mut index = 0;
        self.fold_usings(&mut query.body, &mut index);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.levels.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        let pointer = std::ptr::from_ref::<SqlExpr>(expr);
        if let Some(level) = self.levels.last_mut()
            && let Some((index, slot)) = level.slots.get(&pointer).copied()
        {
            level.active.push((pointer, index, slot));
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        let pointer = std::ptr::from_ref::<SqlExpr>(expr);
        if self.error.is_none() {
            match expr {
                SqlExpr::Identifier(ident) => {
                    self.rewrite_ident(None, ident);
                }
                SqlExpr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                    let qualifier = parts[parts.len() - 2].value.clone();
                    let last = parts.len() - 1;
                    self.rewrite_ident(Some(qualifier.as_str()), &mut parts[last]);
                }
                _ => {}
            }
        }
        if let Some(level) = self.levels.last_mut()
            && level
                .active
                .last()
                .is_some_and(|(active, _, _)| *active == pointer)
        {
            level.active.pop();
        }
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Insert(insert) => {
                for column in &mut insert.columns {
                    if let Some(ObjectNamePart::Identifier(ident)) = column.0.last_mut() {
                        self.rewrite_target(ident);
                    }
                }
            }
            Statement::Update(update) => {
                for assignment in &mut update.assignments {
                    if let AssignmentTarget::ColumnName(name) = &mut assignment.target
                        && let Some(ObjectNamePart::Identifier(ident)) = name.0.last_mut()
                    {
                        self.rewrite_target(ident);
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn fold_statement(
    statement: &mut Statement,
    known: &Known,
    written: &WrittenRefs,
) -> Result<bool> {
    let mut fold = CaseFold {
        known,
        written,
        levels: Vec::new(),
        base: Vec::new(),
        target: Vec::new(),
        changed: false,
        error: None,
    };
    let _ = statement.visit(&mut fold);
    if let Some(error) = fold.error {
        return Err(error);
    }
    Ok(fold.changed)
}
