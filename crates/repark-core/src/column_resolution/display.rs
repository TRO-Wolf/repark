use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, GroupByExpr, Ident, ObjectName, OrderBy, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor,
};
use repark_common::spark_error;

pub(super) fn display_rewrite(
    original: &Statement,
    folded: &Statement,
    planned: &[String],
) -> Option<Statement> {
    let Statement::Query(original_query) = original else {
        return None;
    };
    let Statement::Query(folded_query) = folded else {
        return None;
    };
    let written = set_written(&original_query.body, planned.len())?;
    let mut rewritten = folded_query.as_ref().clone();
    if !rewrite_top_query(&mut rewritten, &written, planned) {
        return None;
    }
    Some(Statement::Query(Box::new(rewritten)))
}

fn set_written(body: &SetExpr, width: usize) -> Option<Vec<Option<String>>> {
    let mut node = body;
    loop {
        match node {
            SetExpr::Select(select) => return projection_written(&select.projection, width),
            SetExpr::SetOperation { left, .. } => {
                node = left.as_ref();
            }
            _ => return None,
        }
    }
}

fn projection_written(items: &[SelectItem], width: usize) -> Option<Vec<Option<String>>> {
    if items
        .iter()
        .any(|item| matches!(item, SelectItem::ExprWithAliases { .. }))
    {
        return None;
    }
    let stars = items
        .iter()
        .filter(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        })
        .count();
    if stars > 1 {
        return None;
    }
    let explicit = items.len() - stars;
    if stars == 0 && width != explicit {
        return None;
    }
    let star_slots = if stars == 1 {
        width.checked_sub(explicit)?
    } else {
        0
    };
    let mut out = Vec::with_capacity(width);
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => out.push(plain_ref_spelling(expr)),
            SelectItem::ExprWithAlias { alias, .. } => out.push(Some(alias.value.clone())),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                out.extend((0..star_slots).map(|_| None));
            }
            SelectItem::ExprWithAliases { .. } => return None,
        }
    }
    if out.len() != width {
        return None;
    }
    Some(out)
}

fn plain_ref_spelling(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) if !parts.is_empty() => {
            Some(parts[parts.len() - 1].value.clone())
        }
        _ => None,
    }
}

fn rewrite_top_query(query: &mut Query, written: &[Option<String>], planned: &[String]) -> bool {
    let mut changed = false;
    let mut first_aliases: HashMap<String, String> = HashMap::new();
    let mut node = query.body.as_mut();
    loop {
        match node {
            SetExpr::Select(select) => {
                let aliases = rewrite_select_projection(select, written, planned);
                changed = !aliases.is_empty();
                changed = requote_alias_references(select, &aliases) || changed;
                first_aliases = aliases;
                break;
            }
            SetExpr::SetOperation { left, .. } => {
                node = left.as_mut();
            }
            _ => break,
        }
    }
    if let Some(order_by) = query.order_by.as_mut() {
        changed = requote_order_by(order_by, &first_aliases) || changed;
    }
    changed
}

fn rewrite_select_projection(
    select: &mut Select,
    written: &[Option<String>],
    planned: &[String],
) -> HashMap<String, String> {
    let empty: HashMap<String, String> = HashMap::new();
    if select.projection.len() != written.len() || written.len() != planned.len() {
        return empty;
    }
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for spelling in written.iter().flatten() {
        *counts.entry(spelling.as_str()).or_default() += 1;
    }
    let explicit: HashSet<&str> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.as_str()),
            _ => None,
        })
        .collect();
    let mut used: HashSet<String> = HashSet::new();
    let mut actions: Vec<Option<String>> = vec![None; written.len()];
    for (index, item) in select.projection.iter().enumerate() {
        let planned_name = planned[index].as_str();
        let candidate = match (&written[index], item) {
            (Some(spelling), SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. })
                if spelling.as_str() != planned_name =>
            {
                Some(spelling.as_str())
            }
            _ => None,
        };
        let Some(spelling) = candidate else {
            used.insert(planned[index].clone());
            continue;
        };
        if counts.get(spelling).is_some_and(|count| *count > 1) {
            used.insert(planned[index].clone());
            continue;
        }
        if used.contains(spelling) {
            used.insert(planned[index].clone());
            continue;
        }
        if matches!(item, SelectItem::UnnamedExpr(_)) && explicit.contains(spelling) {
            used.insert(planned[index].clone());
            continue;
        }
        actions[index] = Some(spelling.to_string());
        used.insert(spelling.to_string());
    }
    if actions.iter().all(Option::is_none) {
        return empty;
    }
    let mut aliases = HashMap::new();
    for (index, item) in select.projection.iter_mut().enumerate() {
        let Some(spelling) = actions[index].clone() else {
            continue;
        };
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let inner = expr.clone();
                *item = SelectItem::ExprWithAlias {
                    expr: inner,
                    alias: Ident::with_quote('"', spelling.clone()),
                };
                aliases.insert(spelling.to_ascii_lowercase(), spelling);
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                alias.quote_style = Some('"');
                aliases.insert(spelling.to_ascii_lowercase(), spelling);
            }
            _ => {}
        }
    }
    aliases
}

fn requote_alias_references(select: &mut Select, aliases: &HashMap<String, String>) -> bool {
    let mut changed = false;
    if let GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
        for expr in exprs {
            changed = requote_alias_refs(expr, aliases) || changed;
        }
    }
    for expr in select.having.iter_mut().chain(select.qualify.iter_mut()) {
        changed = requote_alias_refs(expr, aliases) || changed;
    }
    for order in &mut select.sort_by {
        changed = requote_alias_refs(&mut order.expr, aliases) || changed;
    }
    changed
}

fn requote_order_by(order_by: &mut OrderBy, aliases: &HashMap<String, String>) -> bool {
    let mut changed = false;
    if let OrderByKind::Expressions(exprs) = &mut order_by.kind {
        for order in exprs {
            changed = requote_alias_refs(&mut order.expr, aliases) || changed;
        }
    }
    changed
}

fn requote_alias_refs(expr: &mut SqlExpr, aliases: &HashMap<String, String>) -> bool {
    match expr {
        SqlExpr::Identifier(_) => requote_bare_ident(expr, aliases),
        SqlExpr::Nested(inner)
        | SqlExpr::UnaryOp { expr: inner, .. }
        | SqlExpr::Cast { expr: inner, .. }
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::IsTrue(inner)
        | SqlExpr::IsNotTrue(inner)
        | SqlExpr::IsFalse(inner)
        | SqlExpr::IsNotFalse(inner)
        | SqlExpr::IsUnknown(inner)
        | SqlExpr::IsNotUnknown(inner)
        | SqlExpr::InSubquery { expr: inner, .. }
        | SqlExpr::Collate { expr: inner, .. } => requote_alias_refs(inner, aliases),
        SqlExpr::BinaryOp { left, right, .. }
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right) => {
            requote_alias_refs(left, aliases) | requote_alias_refs(right, aliases)
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            requote_alias_refs(expr, aliases)
                | requote_alias_refs(low, aliases)
                | requote_alias_refs(high, aliases)
        }
        SqlExpr::Like { expr, pattern, .. }
        | SqlExpr::ILike { expr, pattern, .. }
        | SqlExpr::RLike { expr, pattern, .. }
        | SqlExpr::SimilarTo { expr, pattern, .. } => {
            requote_alias_refs(expr, aliases) | requote_alias_refs(pattern, aliases)
        }
        SqlExpr::InList { expr, list, .. } => {
            let mut changed = requote_alias_refs(expr, aliases);
            for item in list {
                changed = requote_alias_refs(item, aliases) || changed;
            }
            changed
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut changed = false;
            if let Some(operand) = operand {
                changed = requote_alias_refs(operand, aliases) || changed;
            }
            for when in conditions {
                changed = requote_alias_refs(&mut when.condition, aliases) || changed;
                changed = requote_alias_refs(&mut when.result, aliases) || changed;
            }
            if let Some(else_result) = else_result {
                changed = requote_alias_refs(else_result, aliases) || changed;
            }
            changed
        }
        SqlExpr::Function(function) => requote_function_args(function, aliases),
        SqlExpr::Tuple(items) => {
            let mut changed = false;
            for item in items {
                changed = requote_alias_refs(item, aliases) || changed;
            }
            changed
        }
        _ => false,
    }
}

fn requote_function_args(
    function: &mut datafusion::sql::sqlparser::ast::Function,
    aliases: &HashMap<String, String>,
) -> bool {
    use datafusion::sql::sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let mut changed = false;
    let args = match &mut function.args {
        FunctionArguments::List(list) => &mut list.args,
        _ => return false,
    };
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => {
                changed = requote_alias_refs(expr, aliases) || changed;
            }
            _ => {}
        }
    }
    changed
}

fn requote_bare_ident(expr: &mut SqlExpr, aliases: &HashMap<String, String>) -> bool {
    let SqlExpr::Identifier(ident) = expr else {
        return false;
    };
    if ident.quote_style == Some('"') {
        return false;
    }
    let Some(spelling) = aliases.get(ident.value.to_ascii_lowercase().as_str()) else {
        return false;
    };
    if ident.value == *spelling {
        ident.quote_style = Some('"');
        return true;
    }
    ident.value.clone_from(spelling);
    ident.quote_style = Some('"');
    true
}

pub(super) fn should_strict_check(statement: &Statement) -> bool {
    struct Probe {
        found: bool,
    }
    impl Visitor for Probe {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
            match expr {
                SqlExpr::Identifier(ident)
                    if ident.quote_style.is_none() && has_upper_ascii(&ident.value) =>
                {
                    self.found = true;
                    ControlFlow::Break(())
                }
                SqlExpr::CompoundIdentifier(parts)
                    if parts
                        .iter()
                        .any(|part| part.quote_style.is_none() && has_upper_ascii(&part.value)) =>
                {
                    self.found = true;
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }
    let mut probe = Probe { found: false };
    let _ = statement.visit(&mut probe);
    probe.found
}

fn has_upper_ascii(name: &str) -> bool {
    name.bytes().any(|byte| byte.is_ascii_uppercase())
}

pub(super) fn strict_single_table(statement: &Statement) -> Option<(ObjectName, Option<String>)> {
    struct Probe {
        with: bool,
        tables: Vec<(ObjectName, Option<String>)>,
        other: bool,
    }
    impl Visitor for Probe {
        type Break = std::convert::Infallible;
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            if query.with.is_some() {
                self.with = true;
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            match factor {
                TableFactor::Table { name, alias, .. } => {
                    self.tables.push((
                        name.clone(),
                        alias.as_ref().map(|alias| alias.name.value.clone()),
                    ));
                }
                _ => {
                    self.other = true;
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut probe = Probe {
        with: false,
        tables: Vec::new(),
        other: false,
    };
    let _ = statement.visit(&mut probe);
    if probe.with || probe.other || probe.tables.len() != 1 {
        return None;
    }
    probe.tables.pop()
}

#[allow(clippy::missing_errors_doc)]
pub(super) fn strict_case_check(
    fields: &[String],
    relation: &str,
    statement: &Statement,
) -> Result<()> {
    struct Probe<'a> {
        fields: &'a [String],
        relation: &'a str,
        error: Option<DataFusionError>,
    }
    impl Probe<'_> {
        fn bare(&mut self, ident: &Ident) {
            if self.error.is_some() || ident.quote_style.is_some() {
                return;
            }
            if fields_exact(self.fields, &ident.value) {
                return;
            }
            if fields_icase(self.fields, &ident.value) {
                self.error = Some(strict_error(&ident.value, self.fields));
            }
        }
        fn qualified(&mut self, qualifier: &str, ident: &Ident) {
            if self.error.is_some() || ident.quote_style.is_some() {
                return;
            }
            if !qualifier.eq_ignore_ascii_case(self.relation) {
                return;
            }
            if qualifier != self.relation {
                self.error = Some(strict_error(
                    &format!("{qualifier}.{}", ident.value),
                    self.fields,
                ));
                return;
            }
            if fields_exact(self.fields, &ident.value) {
                return;
            }
            if fields_icase(self.fields, &ident.value) {
                self.error = Some(strict_error(
                    &format!("{qualifier}.{}", ident.value),
                    self.fields,
                ));
            }
        }
    }
    impl Visitor for Probe<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
            if self.error.is_some() {
                return ControlFlow::Break(());
            }
            match expr {
                SqlExpr::Identifier(ident) => self.bare(ident),
                SqlExpr::CompoundIdentifier(parts)
                    if parts.len() == 2 && parts.iter().all(|part| part.quote_style.is_none()) =>
                {
                    let qualifier = parts[0].value.clone();
                    let field = parts[1].clone();
                    self.qualified(qualifier.as_str(), &field);
                }
                _ => {}
            }
            if self.error.is_some() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    let mut probe = Probe {
        fields,
        relation,
        error: None,
    };
    let _ = statement.visit(&mut probe);
    if let Some(error) = probe.error {
        return Err(error);
    }
    Ok(())
}

fn fields_exact(fields: &[String], name: &str) -> bool {
    fields.iter().any(|field| field == name)
}

fn fields_icase(fields: &[String], name: &str) -> bool {
    fields.iter().any(|field| field.eq_ignore_ascii_case(name))
}

fn strict_error(requested: &str, fields: &[String]) -> DataFusionError {
    let column_name = format!("`{requested}`");
    let suggestions = fields
        .iter()
        .map(|field| format!("`{field}`"))
        .collect::<Vec<String>>()
        .join(", ");
    DataFusionError::Plan(spark_error::message(
        spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
        &[
            ("columnName", column_name.as_str()),
            ("suggestions", suggestions.as_str()),
        ],
    ))
}
