use datafusion::error::Result;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SelectItem, SetExpr,
};

use super::timestamp_column_refusal;

fn function_exprs(expr: &Expr) -> Vec<&Expr> {
    let Expr::Function(function) = expr else {
        return Vec::new();
    };
    let FunctionArguments::List(list) = &function.args else {
        return Vec::new();
    };
    list.args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(inner),
                ..
            } => Some(inner),
            _ => None,
        })
        .collect()
}

pub(crate) fn check_timestamp_expr(expr: &Expr, in_subquery: bool) -> Result<()> {
    let mut stack: Vec<(&Expr, bool)> = vec![(expr, in_subquery)];
    while let Some((current, nested)) = stack.pop() {
        match current {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) if !nested => {
                return Err(timestamp_column_refusal());
            }
            Expr::Subquery(query) => push_query_exprs(&mut stack, query),
            Expr::Exists { subquery, .. } => push_query_exprs(&mut stack, subquery),
            Expr::InSubquery { expr, subquery, .. } => {
                stack.push((expr, true));
                push_query_exprs(&mut stack, subquery);
            }
            _ => {
                for inner in child_exprs(current) {
                    stack.push((inner, nested));
                }
                for inner in function_exprs(current) {
                    stack.push((inner, nested));
                }
            }
        }
    }
    Ok(())
}

fn push_query_exprs<'expr>(stack: &mut Vec<(&'expr Expr, bool)>, query: &'expr Query) {
    let mut bodies: Vec<&'expr SetExpr> = vec![query.body.as_ref()];
    while let Some(body) = bodies.pop() {
        match body {
            SetExpr::Select(select) => {
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(inner)
                        | SelectItem::ExprWithAlias { expr: inner, .. } => {
                            stack.push((inner, true));
                        }
                        _ => {}
                    }
                }
                if let Some(selection) = select.selection.as_ref() {
                    stack.push((selection, true));
                }
                if let Some(having) = select.having.as_ref() {
                    stack.push((having, true));
                }
            }
            SetExpr::Query(inner) => bodies.push(inner.body.as_ref()),
            SetExpr::SetOperation { left, right, .. } => {
                bodies.push(left.as_ref());
                bodies.push(right.as_ref());
            }
            _ => {}
        }
    }
}

fn child_exprs(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Nested(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            vec![inner.as_ref()]
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Collate { expr, .. } => vec![expr.as_ref()],
        Expr::Between {
            expr, low, high, ..
        } => vec![expr.as_ref(), low.as_ref(), high.as_ref()],
        Expr::InList { expr, list, .. } => {
            let mut out = vec![expr.as_ref()];
            out.extend(list.iter());
            out
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut out = Vec::new();
            if let Some(operand) = operand.as_ref() {
                out.push(operand.as_ref());
            }
            for when in conditions {
                out.push(&when.condition);
                out.push(&when.result);
            }
            if let Some(other) = else_result.as_ref() {
                out.push(other.as_ref());
            }
            out
        }
        Expr::Tuple(items) => items.iter().collect(),
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } => vec![expr.as_ref(), pattern.as_ref()],
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut out = vec![expr.as_ref()];
            if let Some(from) = substring_from.as_ref() {
                out.push(from.as_ref());
            }
            if let Some(count) = substring_for.as_ref() {
                out.push(count.as_ref());
            }
            out
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            let mut out = vec![expr.as_ref()];
            if let Some(what) = trim_what.as_ref() {
                out.push(what.as_ref());
            }
            if let Some(characters) = trim_characters.as_ref() {
                out.extend(characters.iter());
            }
            out
        }
        Expr::AtTimeZone { timestamp, .. } => vec![timestamp.as_ref()],
        _ => Vec::new(),
    }
}
