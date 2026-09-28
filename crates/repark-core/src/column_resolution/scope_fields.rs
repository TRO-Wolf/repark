use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, GroupByExpr, Ident, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableAliasColumnDef, TableFactor, TableWithJoins,
    VisitorMut, WildcardAdditionalOptions,
};

use super::{fold::Known, part_value};

pub(super) type ScopeEnv = [(String, Option<Vec<String>>)];

pub(super) fn normalized_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

pub(super) fn query_outputs(query: &Query, known: &Known, ctes: &ScopeEnv) -> Option<Vec<String>> {
    let mut ctes = ctes.to_vec();
    if let Some(with) = query.with.as_ref() {
        for cte in &with.cte_tables {
            let fields = if cte.alias.columns.is_empty() {
                query_outputs(&cte.query, known, &ctes)
            } else {
                Some(
                    cte.alias
                        .columns
                        .iter()
                        .map(|column| normalized_ident(&column.name))
                        .collect(),
                )
            };
            ctes.push((cte.alias.name.value.clone(), fields));
        }
    }
    set_outputs(&query.body, known, &ctes)
}

fn set_outputs(body: &SetExpr, known: &Known, ctes: &ScopeEnv) -> Option<Vec<String>> {
    match body {
        SetExpr::Select(select) => select_outputs(select, known, ctes),
        SetExpr::SetOperation { left, .. } => set_outputs(left, known, ctes),
        SetExpr::Query(query) => query_outputs(query, known, ctes),
        _ => None,
    }
}

fn plain(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

fn stored_or_normalized(
    scope: &[(String, Option<Vec<String>>)],
    qualifier: Option<&str>,
    ident: &Ident,
) -> String {
    let mut held: Option<&str> = None;
    for (relation, fields) in scope {
        if qualifier.is_some_and(|scope| !relation.eq_ignore_ascii_case(scope)) {
            continue;
        }
        let Some(fields) = fields else { continue };
        for field in fields {
            if field.eq_ignore_ascii_case(&ident.value) {
                match held {
                    None => held = Some(field),
                    Some(prior) if prior == field => {}
                    Some(_) => return normalized_ident(ident),
                }
            }
        }
    }
    held.map_or_else(|| normalized_ident(ident), str::to_string)
}

fn select_outputs(select: &Select, known: &Known, ctes: &ScopeEnv) -> Option<Vec<String>> {
    let mut names = Vec::new();
    let mut factors: Option<Vec<(String, Option<Vec<String>>)>> = None;
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                let scope = factors.get_or_insert_with(|| scope_factors(&select.from, known, ctes));
                names.push(stored_or_normalized(scope, None, ident));
            }
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
                let scope = factors.get_or_insert_with(|| scope_factors(&select.from, known, ctes));
                let last = parts.last()?;
                let qualifier = parts.get(parts.len() - 2).map(|part| part.value.as_str());
                names.push(stored_or_normalized(scope, qualifier, last));
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                names.push(normalized_ident(alias));
            }
            SelectItem::Wildcard(options) if plain(options) => {
                let scope = factors.get_or_insert_with(|| scope_factors(&select.from, known, ctes));
                for (_, fields) in scope {
                    names.extend(fields.clone()?);
                }
            }
            SelectItem::QualifiedWildcard(kind, options) if plain(options) => {
                let SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                    return None;
                };
                let qualifier = name.0.last().and_then(part_value)?;
                let scope = factors.get_or_insert_with(|| scope_factors(&select.from, known, ctes));
                let mut matched = false;
                for (relation, fields) in scope {
                    if relation.eq_ignore_ascii_case(qualifier) {
                        matched = true;
                        names.extend(fields.clone()?);
                    }
                }
                if !matched {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(names)
}

fn scope_factors(
    from: &[TableWithJoins],
    known: &Known,
    ctes: &ScopeEnv,
) -> Vec<(String, Option<Vec<String>>)> {
    let mut out = Vec::new();
    for table in from {
        push_factor(&table.relation, known, ctes, &mut out);
        for join in &table.joins {
            push_factor(&join.relation, known, ctes, &mut out);
        }
    }
    out
}

fn push_factor(
    factor: &TableFactor,
    known: &Known,
    ctes: &ScopeEnv,
    out: &mut Vec<(String, Option<Vec<String>>)>,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let written = name
                .0
                .iter()
                .filter_map(|part| part_value(part).map(str::to_string))
                .collect::<Vec<_>>();
            let last = written.last().cloned().unwrap_or_default();
            let (relation, fields) = match alias {
                Some(alias) => (alias.name.value.clone(), None),
                None => (last.clone(), None),
            };
            let shadowed =
                written.len() == 1 && ctes.iter().any(|(cte, _)| cte.eq_ignore_ascii_case(&last));
            let fields = if shadowed {
                ctes.iter()
                    .rfind(|(cte, _)| cte.eq_ignore_ascii_case(&last))
                    .and_then(|(_, fields)| fields.clone())
            } else {
                fields.or_else(|| {
                    super::fold::normalized_parts(name)
                        .and_then(|parts| known.tables.get(&parts).cloned())
                })
            };
            let fields =
                fields.or_else(|| known.derived.get(&relation.to_ascii_lowercase()).cloned());
            out.push((relation, fields));
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let relation = alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_default();
            let columns = alias
                .as_ref()
                .filter(|alias| !alias.columns.is_empty())
                .map(|alias| {
                    alias
                        .columns
                        .iter()
                        .map(|column| normalized_ident(&column.name))
                        .collect::<Vec<_>>()
                });
            let fields = columns
                .or_else(|| known.derived.get(&relation.to_ascii_lowercase()).cloned())
                .or_else(|| query_outputs(subquery, known, ctes));
            out.push((relation, fields));
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            push_factor(&table_with_joins.relation, known, ctes, out);
            for join in &table_with_joins.joins {
                push_factor(&join.relation, known, ctes, out);
            }
        }
        _ => out.push((String::new(), None)),
    }
}

fn select_references_aliases(select: &Select) -> bool {
    select.prewhere.is_some()
        || select.having.is_some()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || !select.sort_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || select.into.is_some()
        || !select.named_window.is_empty()
        || match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        }
}

fn aliasable(item: &SelectItem) -> bool {
    let (SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. }) = item else {
        return false;
    };
    !matches!(expr, SqlExpr::QualifiedWildcard(..))
}

fn inject_column_aliases(query: &mut Query, columns: &[TableAliasColumnDef]) -> bool {
    if columns.is_empty()
        || columns.iter().any(|column| column.data_type.is_some())
        || query.order_by.is_some()
        || !query.pipe_operators.is_empty()
    {
        return false;
    }
    let mut node = query.body.as_mut();
    loop {
        match node {
            SetExpr::Select(select) => {
                if select_references_aliases(select)
                    || select.projection.len() != columns.len()
                    || !select.projection.iter().all(aliasable)
                {
                    return false;
                }
                for (item, column) in select.projection.iter_mut().zip(columns) {
                    let expr = match item {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            expr.clone()
                        }
                        _ => return false,
                    };
                    *item = SelectItem::ExprWithAlias {
                        expr,
                        alias: column.name.clone(),
                    };
                }
                return true;
            }
            SetExpr::SetOperation { left, .. } => node = left.as_mut(),
            _ => return false,
        }
    }
}

pub(super) struct InjectAliases {
    pub(super) changed: bool,
}

impl VisitorMut for InjectAliases {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                if !cte.alias.columns.is_empty()
                    && inject_column_aliases(&mut cte.query, &cte.alias.columns)
                {
                    cte.alias.columns.clear();
                    self.changed = true;
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = factor
            && !alias.columns.is_empty()
            && inject_column_aliases(subquery, &alias.columns)
        {
            alias.columns.clear();
            self.changed = true;
        }
        ControlFlow::Continue(())
    }
}
