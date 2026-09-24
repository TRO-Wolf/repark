use datafusion::sql::sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Value};

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
    let SetExpr::Select(select) = source.body.as_ref() else {
        return None;
    };
    select
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
