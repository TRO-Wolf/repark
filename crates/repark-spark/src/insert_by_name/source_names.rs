use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use std::fmt::Display;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    CastKind, Cte, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
    ObjectNamePart, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    SetQuantifier, TableAliasColumnDef, TableFactor, Values, VisitMut, WildcardAdditionalOptions,
    visit_expressions_mut,
};
use repark_core::CatalogRegistry;

use super::SourceName;
use super::spark_names::expression_name;

const QUERY_DEPTH: usize = 16;

pub(super) struct Named {
    display: String,
    resolved: String,
}

pub(super) enum Item {
    Named(Named),
    Expanded(Vec<Named>),
    Wildcard { trusted: bool, text: String },
    Opaque(String),
}

enum Leftmost<'a> {
    Select(&'a Select),
    Values(&'a Values),
}

fn normalize_ident(value: &str, quoted: bool, case_sensitive: bool) -> String {
    if quoted || case_sensitive {
        value.to_string()
    } else {
        value.to_ascii_lowercase()
    }
}

fn ident_named(ident: &Ident, case_sensitive: bool) -> Named {
    Named {
        display: ident.value.clone(),
        resolved: normalize_ident(&ident.value, ident.quote_style.is_some(), case_sensitive),
    }
}

fn text_named(display: String, case_sensitive: bool) -> Named {
    Named {
        resolved: normalize_ident(&display, false, case_sensitive),
        display,
    }
}

fn source_name(
    named: Named,
    column: String,
    item: Option<usize>,
    underived: Option<String>,
) -> SourceName {
    SourceName {
        display: named.display,
        resolved: named.resolved,
        column,
        item,
        underived,
    }
}

fn planned(column: &str, item: Option<usize>, underived: Option<String>) -> SourceName {
    source_name(
        Named {
            display: column.to_string(),
            resolved: column.to_string(),
        },
        column.to_string(),
        item,
        underived,
    )
}

pub(super) fn query_items(source: &Query, case_sensitive: bool) -> Option<Vec<Item>> {
    items_in(source, &[], case_sensitive, 0)
}

pub(super) fn named_items(items: Vec<Item>) -> Option<Vec<SourceName>> {
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| match item {
            Item::Named(named) => {
                let column = named.resolved.clone();
                Some(source_name(named, column, Some(index), None))
            }
            _ => None,
        })
        .collect()
}

fn items_in<'a>(
    query: &'a Query,
    outer: &[&'a Cte],
    case_sensitive: bool,
    depth: usize,
) -> Option<Vec<Item>> {
    if depth > QUERY_DEPTH {
        return None;
    }
    let mut scope = outer.to_vec();
    if let Some(with) = &query.with {
        scope.extend(with.cte_tables.iter());
    }
    match leftmost(&query.body)? {
        Leftmost::Values(values) => Some(
            (1..=values.rows.first()?.len())
                .map(|index| Item::Named(text_named(format!("col{index}"), case_sensitive)))
                .collect(),
        ),
        Leftmost::Select(select) => {
            let qualifiers = relation_qualifiers(select);
            Some(
                select
                    .projection
                    .iter()
                    .map(|item| {
                        select_item(item, select, &qualifiers, &scope, case_sensitive, depth)
                    })
                    .collect(),
            )
        }
    }
}

fn select_item(
    item: &SelectItem,
    select: &Select,
    qualifiers: &[String],
    scope: &[&Cte],
    case_sensitive: bool,
    depth: usize,
) -> Item {
    let unresolved = || Item::Wildcard {
        trusted: from_named_tables(select, scope),
        text: user_text(item),
    };
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Item::Named(ident_named(alias, case_sensitive)),
        SelectItem::UnnamedExpr(expr) => expr_named(expr, qualifiers, case_sensitive)
            .map_or_else(|| Item::Opaque(user_text(expr)), Item::Named),
        SelectItem::Wildcard(options) => {
            wildcard(select, None, options, scope, case_sensitive, depth).unwrap_or_else(unresolved)
        }
        SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::ObjectName(name),
            options,
        ) => wildcard(select, Some(name), options, scope, case_sensitive, depth)
            .unwrap_or_else(unresolved),
        _ => Item::Wildcard {
            trusted: false,
            text: user_text(item),
        },
    }
}

fn user_text<T: VisitMut + Clone + Display>(node: &T) -> String {
    let mut shown = node.clone();
    let _ = visit_expressions_mut(&mut shown, |expr| {
        if let Some(literal) = suffix_literal(expr) {
            *expr = literal;
        }
        ControlFlow::<()>::Continue(())
    });
    shown.to_string()
}

fn suffix_literal(expr: &Expr) -> Option<Expr> {
    let Expr::Function(function) = expr else {
        return None;
    };
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return None;
    };
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    match list.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(literal))]
            if name.value.eq_ignore_ascii_case(crate::SUFFIX_LITERAL_NAME) =>
        {
            Some(literal.clone())
        }
        _ => None,
    }
}

fn peel(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(child) = expr {
        expr = child;
    }
    expr
}

fn expr_named(expr: &Expr, qualifiers: &[String], case_sensitive: bool) -> Option<Named> {
    let expr = peel(expr);
    let column = match expr {
        Expr::Cast {
            kind: CastKind::Cast,
            expr: child,
            array: false,
            format: None,
            ..
        } => peel(child),
        other => other,
    };
    match column {
        Expr::Identifier(ident) => Some(ident_named(ident, case_sensitive)),
        Expr::CompoundIdentifier(parts) => {
            parts.last().map(|ident| ident_named(ident, case_sensitive))
        }
        _ => expression_name(expr, qualifiers).map(|name| text_named(name, case_sensitive)),
    }
}

fn plain_options(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

fn single_ident(name: &ObjectName) -> Option<&Ident> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Some(ident),
        _ => None,
    }
}

fn qualifier_matches(qualifier: Option<&ObjectName>, relation: Option<&Ident>) -> bool {
    let Some(qualifier) = qualifier else {
        return true;
    };
    match (single_ident(qualifier), relation) {
        (Some(written), Some(relation)) => written.value.eq_ignore_ascii_case(&relation.value),
        _ => false,
    }
}

fn cte_named<'a>(name: &ObjectName, scope: &[&'a Cte]) -> Option<&'a Cte> {
    let ident = single_ident(name)?;
    scope
        .iter()
        .rev()
        .find(|cte| cte.alias.name.value.eq_ignore_ascii_case(&ident.value))
        .copied()
}

fn wildcard(
    select: &Select,
    qualifier: Option<&ObjectName>,
    options: &WildcardAdditionalOptions,
    scope: &[&Cte],
    case_sensitive: bool,
    depth: usize,
) -> Option<Item> {
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !plain_options(options) || !from.joins.is_empty() {
        return None;
    }
    let names = match &from.relation {
        TableFactor::Derived {
            lateral: false,
            subquery,
            alias,
            ..
        } => {
            if !qualifier_matches(qualifier, alias.as_ref().map(|alias| &alias.name)) {
                return None;
            }
            let columns = alias.as_ref().map_or(&[][..], |alias| &alias.columns);
            relation_names(subquery, columns, scope, case_sensitive, depth)?
        }
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => {
            let cte = cte_named(name, scope)?;
            let own = alias.as_ref().map_or(&cte.alias.name, |alias| &alias.name);
            if !qualifier_matches(qualifier, Some(own)) {
                return None;
            }
            let columns = alias
                .as_ref()
                .filter(|alias| !alias.columns.is_empty())
                .map_or(&cte.alias.columns[..], |alias| &alias.columns);
            relation_names(&cte.query, columns, scope, case_sensitive, depth)?
        }
        _ => return None,
    };
    Some(Item::Expanded(names))
}

fn relation_names(
    query: &Query,
    columns: &[TableAliasColumnDef],
    scope: &[&Cte],
    case_sensitive: bool,
    depth: usize,
) -> Option<Vec<Named>> {
    if !columns.is_empty() {
        return Some(
            columns
                .iter()
                .map(|column| ident_named(&column.name, case_sensitive))
                .collect(),
        );
    }
    let mut relation = Vec::new();
    for item in items_in(query, scope, case_sensitive, depth + 1)? {
        match item {
            Item::Named(named) => relation.push(named),
            Item::Expanded(expanded) => relation.extend(expanded),
            Item::Wildcard { .. } | Item::Opaque(_) => return None,
        }
    }
    Some(relation)
}

fn factors(select: &Select) -> impl Iterator<Item = &TableFactor> {
    select.from.iter().flat_map(|from| {
        std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
    })
}

fn from_named_tables(select: &Select, scope: &[&Cte]) -> bool {
    !select.from.is_empty()
        && factors(select).all(|factor| {
            matches!(factor, TableFactor::Table { name, args: None, .. }
                if cte_named(name, scope).is_none())
        })
}

fn relation_qualifiers(select: &Select) -> Vec<String> {
    factors(select)
        .filter_map(|factor| match factor {
            TableFactor::Table { name, alias, .. } => alias.as_ref().map_or_else(
                || {
                    name.0
                        .last()
                        .and_then(ObjectNamePart::as_ident)
                        .map(|ident| ident.value.clone())
                },
                |alias| Some(alias.name.value.clone()),
            ),
            TableFactor::Derived { alias, .. } => {
                alias.as_ref().map(|alias| alias.name.value.clone())
            }
            _ => None,
        })
        .collect()
}

pub(super) async fn probe_source_names(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    source: &Query,
    case_sensitive: bool,
    accepts_any: bool,
) -> Result<Vec<SourceName>> {
    let items = match query_items(source, case_sensitive) {
        Some(items) if items.iter().all(|item| matches!(item, Item::Named(_))) => {
            return Ok(named_items(items).unwrap_or_default());
        }
        items => items,
    };
    let frame = match planner_frame(ctx, catalogs, source).await {
        Ok(frame) => frame,
        Err(error) if accepts_any => {
            return match repeated_names(ctx, catalogs, source, items, case_sensitive).await {
                Some(names) => Ok(names),
                None => Err(error),
            };
        }
        Err(error) => return Err(error),
    };
    let planner: Vec<String> = frame
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    Ok(items
        .and_then(|items| place(items, &planner))
        .unwrap_or_else(|| {
            planner
                .iter()
                .map(|column| planned(column, None, Some(user_text(source))))
                .collect()
        }))
}

async fn planner_frame(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    source: &Query,
) -> Result<datafusion::dataframe::DataFrame> {
    let probe_sql = format!("SELECT * FROM ({source}) AS _repark_by_name_src LIMIT 0");
    crate::spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await
}

async fn repeated_names(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    source: &Query,
    items: Option<Vec<Item>>,
    case_sensitive: bool,
) -> Option<Vec<SourceName>> {
    let Leftmost::Select(select) = leftmost(&source.body)? else {
        return None;
    };
    let mut names = Vec::new();
    for (index, (item, projected)) in items?.into_iter().zip(&select.projection).enumerate() {
        let expanded = match item {
            Item::Named(one) => vec![one],
            Item::Expanded(many) => many,
            Item::Wildcard { trusted: true, .. } => {
                star_names(ctx, catalogs, source, select, projected, case_sensitive).await?
            }
            Item::Wildcard { .. } | Item::Opaque(_) => return None,
        };
        names.extend(expanded.into_iter().map(|one| {
            let column = one.resolved.clone();
            source_name(one, column, Some(index), None)
        }));
    }
    let repeats = names.iter().enumerate().any(|(later, name)| {
        names[..later]
            .iter()
            .any(|earlier| super::same_name(&earlier.resolved, &name.resolved, case_sensitive))
    });
    repeats.then_some(names)
}

async fn star_names(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    source: &Query,
    select: &Select,
    star: &SelectItem,
    case_sensitive: bool,
) -> Option<Vec<Named>> {
    let mut alone = select.clone();
    alone.projection = vec![star.clone()];
    let mut query = source.clone();
    *query.body = SetExpr::Select(Box::new(alone));
    query.order_by = None;
    let frame = planner_frame(ctx, catalogs, &query).await.ok()?;
    Some(
        frame
            .schema()
            .as_arrow()
            .fields()
            .iter()
            .map(|field| text_named(field.name().clone(), case_sensitive))
            .collect(),
    )
}

pub(super) fn place(items: Vec<Item>, planner: &[String]) -> Option<Vec<SourceName>> {
    let fixed: usize = items
        .iter()
        .map(|item| match item {
            Item::Named(_) | Item::Opaque(_) => 1,
            Item::Expanded(names) => names.len(),
            Item::Wildcard { .. } => 0,
        })
        .sum();
    let open = items
        .iter()
        .filter(|item| matches!(item, Item::Wildcard { .. }))
        .count();
    let spare = planner.len().checked_sub(fixed)?;
    if open > 1 || (open == 0 && spare != 0) {
        return None;
    }
    let mut columns = planner.iter();
    let mut placed = Vec::with_capacity(planner.len());
    for (index, item) in items.into_iter().enumerate() {
        match item {
            Item::Named(named) => {
                columns.next()?;
                let column = named.resolved.clone();
                placed.push(source_name(named, column, Some(index), None));
            }
            Item::Opaque(text) => placed.push(planned(columns.next()?, Some(index), Some(text))),
            Item::Expanded(expanded) => {
                for named in expanded {
                    let column = columns.next()?.clone();
                    placed.push(source_name(named, column, Some(index), None));
                }
            }
            Item::Wildcard { trusted, text } => {
                for column in columns.by_ref().take(spare) {
                    placed.push(planned(
                        column,
                        Some(index),
                        (!trusted).then(|| text.clone()),
                    ));
                }
            }
        }
    }
    Some(placed)
}

pub(super) fn refuse_underived(
    table: &iceberg::table::Table,
    sources: &[SourceName],
    table_display: &str,
    case_sensitive: bool,
) -> Result<()> {
    let schema = table.metadata().current_schema();
    let fields = schema.as_struct().fields();
    for name in sources {
        let Some(text) = &name.underived else {
            continue;
        };
        if !fields
            .iter()
            .any(|field| super::same_name(&field.name, &name.resolved, case_sensitive))
        {
            return Err(DataFusionError::NotImplemented(format!(
                "INSERT into {table_display} cannot yet name the source column of `{text}` as \
                 Spark names it, and the table has no column it matches; alias that column in \
                 the source"
            )));
        }
    }
    Ok(())
}

fn names_by_position(quantifier: SetQuantifier) -> bool {
    !matches!(
        quantifier,
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
    )
}

fn leftmost(mut body: &SetExpr) -> Option<Leftmost<'_>> {
    loop {
        body = match body {
            SetExpr::Select(select) => return Some(Leftmost::Select(select)),
            SetExpr::Values(values) => return Some(Leftmost::Values(values)),
            SetExpr::Query(query) => &query.body,
            SetExpr::SetOperation {
                left,
                set_quantifier,
                ..
            } if names_by_position(*set_quantifier) => left,
            _ => return None,
        };
    }
}

pub(super) fn leftmost_is_values(source: &Query) -> bool {
    matches!(leftmost(&source.body), Some(Leftmost::Values(_)))
}

fn leftmost_select_mut(mut body: &mut SetExpr) -> Option<&mut Select> {
    loop {
        body = match body {
            SetExpr::Select(select) => return Some(select),
            SetExpr::Query(query) => &mut query.body,
            SetExpr::SetOperation {
                left,
                set_quantifier,
                ..
            } if names_by_position(*set_quantifier) => left,
            _ => return None,
        };
    }
}

pub(super) fn aliased_source(source: &Query, names: &[SourceName]) -> Query {
    let mut aliased = source.clone();
    let Some(select) = leftmost_select_mut(&mut aliased.body) else {
        return aliased;
    };
    for (index, item) in select.projection.iter_mut().enumerate() {
        let mut owned = names.iter().filter(|name| name.item == Some(index));
        let (Some(name), None) = (owned.next(), owned.next()) else {
            continue;
        };
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            *item = SelectItem::ExprWithAlias {
                expr: expr.clone(),
                alias: Ident::with_quote('`', name.column.clone()),
            };
        }
    }
    aliased
}
