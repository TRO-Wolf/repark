use std::collections::HashSet;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CastKind, Expr, Ident, Insert, Select, SelectItem, SetExpr, Statement,
};
use repark_core::CatalogRegistry;

pub(crate) mod partition_append;
pub(crate) mod replace_where;

pub(crate) struct PreparedInsert {
    pub(crate) sql: String,
    pub(crate) insert: Insert,
    pub(crate) owned_append: bool,
}

pub(crate) async fn prepare_positional_insert(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
) -> Result<Option<PreparedInsert>> {
    let deduplicated = deduplicate_source_names(insert);
    let current = deduplicated.as_ref().unwrap_or(insert);
    if let Some(rewritten) =
        partition_append::rewrite_partition_clause(ctx, catalogs, current, insert.source.as_deref())
            .await?
    {
        return Ok(Some(rewritten));
    }
    deduplicated.map(reparsed).transpose()
}

pub(crate) fn reparsed(insert: Insert) -> Result<PreparedInsert> {
    reparse_insert_sql(Statement::Insert(insert).to_string())
}

pub(crate) fn reparse_insert_sql(sql: String) -> Result<PreparedInsert> {
    match crate::normalize::parse_single_normalized(&sql)? {
        Some((Statement::Insert(insert), _, _)) => Ok(PreparedInsert {
            sql,
            insert,
            owned_append: false,
        }),
        _ => Err(DataFusionError::Internal(format!(
            "rewritten INSERT did not parse back as one INSERT: {sql}"
        ))),
    }
}

pub(crate) fn deduplicate_source_names(insert: &Insert) -> Option<Insert> {
    let mut source = insert.source.as_ref()?.as_ref().clone();
    if !deduplicate_body(&mut source.body) {
        return None;
    }
    let mut rewritten = insert.clone();
    rewritten.source = Some(Box::new(source));
    Some(rewritten)
}

fn deduplicate_body(body: &mut SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => deduplicate_select(select),
        SetExpr::Query(query) => deduplicate_body(&mut query.body),
        SetExpr::SetOperation { left, right, .. } => {
            let left_changed = deduplicate_body(left);
            let right_changed = deduplicate_body(right);
            left_changed || right_changed
        }
        _ => false,
    }
}

fn deduplicate_select(select: &mut Select) -> bool {
    let starred = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    });
    let mut seen = HashSet::new();
    let mut changed = false;
    for (index, item) in select.projection.iter_mut().enumerate() {
        let Some(key) = output_key(item) else {
            continue;
        };
        if seen.insert(key) && !(starred && column_named(item)) {
            continue;
        }
        let alias = Ident::new(format!("__repark_col_{}", index + 1));
        match item {
            SelectItem::UnnamedExpr(expr) => {
                *item = SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias,
                };
            }
            SelectItem::ExprWithAlias { alias: current, .. } => *current = alias,
            _ => continue,
        }
        changed = true;
    }
    changed
}

fn output_key(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(expression_key(expr)),
        SelectItem::ExprWithAlias { alias, .. } => Some(folded(alias)),
        _ => None,
    }
}

fn column_named(item: &SelectItem) -> bool {
    match item {
        SelectItem::ExprWithAlias { .. } => true,
        SelectItem::UnnamedExpr(expr) => column_expression(expr),
        _ => false,
    }
}

fn column_expression(expr: &Expr) -> bool {
    match expr {
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            expr: inner,
            ..
        }
        | Expr::Nested(inner) => column_expression(inner),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        _ => false,
    }
}

fn expression_key(expr: &Expr) -> String {
    match expr {
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            expr: inner,
            ..
        }
        | Expr::Nested(inner) => expression_key(inner),
        Expr::Identifier(ident) => folded(ident),
        Expr::CompoundIdentifier(parts) => parts.last().map(folded).unwrap_or_default(),
        other => other.to_string(),
    }
}

fn folded(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}
