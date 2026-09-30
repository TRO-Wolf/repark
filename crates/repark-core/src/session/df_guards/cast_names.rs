use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::Expr;
use repark_common::names::NameRule;

#[allow(clippy::missing_errors_doc)]
pub fn bind_projection_expr(expr: Expr, frame_schema: &DFSchema, rule: NameRule) -> Result<Expr> {
    let written = match &expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Cast(cast) => cast_child_name(&cast.expr),
        Expr::TryCast(cast) => cast_child_name(&cast.expr),
        _ => None,
    };
    let bound = super::subquery::resolve_bound_expr_with(expr, frame_schema, rule)?;
    Ok(match (written, &bound) {
        (Some(written), Expr::Column(held)) if held.name != written => {
            let relation = held.relation.clone();
            bound.alias_qualified(relation, written)
        }
        (Some(written), Expr::Cast(_) | Expr::TryCast(_)) => bound.alias(written),
        _ => bound,
    })
}

fn cast_child_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Cast(cast) => cast_child_name(&cast.expr),
        Expr::TryCast(cast) => cast_child_name(&cast.expr),
        _ => None,
    }
}
