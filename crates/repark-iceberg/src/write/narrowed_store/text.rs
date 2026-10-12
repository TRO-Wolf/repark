use datafusion::common::Column;
use datafusion::logical_expr::Expr;

const GENERATED_PREFIXES: [&str; 2] = ["__repark_", "_repark_"];

pub(super) fn named(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Alias(alias) => named(&alias.expr),
        Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => column_text(column),
        _ => None,
    }
}

pub(super) fn generated(name: &str) -> bool {
    GENERATED_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix) || name.contains(&format!(".{prefix}")))
}

fn column_text(column: &Column) -> Option<String> {
    let name = column.name.as_str();
    let plain = !name.is_empty()
        && name
            .chars()
            .all(|letter| letter.is_alphanumeric() || letter == '_');
    if !plain
        || GENERATED_PREFIXES
            .iter()
            .any(|prefix| name.contains(prefix))
    {
        return None;
    }
    match &column.relation {
        Some(relation) if generated(relation.table()) => Some(name.to_string()),
        _ => Some(column.flat_name()),
    }
}
