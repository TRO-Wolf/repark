use datafusion::sql::sqlparser::ast::{
    Expr, Ident, Query, Select, SelectItem, SetExpr, SetQuantifier, Value,
};

use super::SourceName;

fn normalize_ident(value: &str, quoted: bool, case_sensitive: bool) -> String {
    if quoted || case_sensitive {
        value.to_string()
    } else {
        value.to_ascii_lowercase()
    }
}

pub(super) fn syntactic_source_names(
    source: &Query,
    case_sensitive: bool,
) -> Option<Vec<SourceName>> {
    leftmost_select(&source.body)?
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(SourceName {
                display: alias.value.clone(),
                resolved: normalize_ident(
                    &alias.value,
                    alias.quote_style.is_some(),
                    case_sensitive,
                ),
            }),
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(SourceName {
                display: ident.value.clone(),
                resolved: normalize_ident(
                    &ident.value,
                    ident.quote_style.is_some(),
                    case_sensitive,
                ),
            }),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                parts.last().map(|ident| SourceName {
                    display: ident.value.clone(),
                    resolved: normalize_ident(
                        &ident.value,
                        ident.quote_style.is_some(),
                        case_sensitive,
                    ),
                })
            }
            SelectItem::UnnamedExpr(Expr::Value(literal)) => {
                literal_name(&literal.value).map(|name| SourceName {
                    display: name.clone(),
                    resolved: normalize_ident(&name, false, case_sensitive),
                })
            }
            _ => None,
        })
        .collect()
}

fn literal_name(value: &Value) -> Option<String> {
    match value {
        Value::Number(text, false) | Value::SingleQuotedString(text) => Some(text.clone()),
        _ => None,
    }
}

fn names_by_position(quantifier: SetQuantifier) -> bool {
    !matches!(
        quantifier,
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
    )
}

fn leftmost_select(mut body: &SetExpr) -> Option<&Select> {
    loop {
        body = match body {
            SetExpr::Select(select) => return Some(select),
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
    if select.projection.len() != names.len() {
        return aliased;
    }
    let projection: Option<Vec<SelectItem>> = select
        .projection
        .iter()
        .zip(names)
        .map(|(item, name)| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Some(SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias: Ident::with_quote('`', name.resolved.clone()),
                })
            }
            _ => None,
        })
        .collect();
    if let Some(projection) = projection {
        select.projection = projection;
    }
    aliased
}
