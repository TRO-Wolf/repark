use chrono::DateTime;
use datafusion::error::Result;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SelectItem, SetExpr, Value,
};

use crate::SessionTimeZone;

use super::sql_text::parse_timestamp_string_to_ms;
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

fn function_last_name(expr: &Expr) -> Option<String> {
    let Expr::Function(function) = expr else {
        return None;
    };
    function
        .name
        .to_string()
        .split('.')
        .next_back()
        .map(str::to_lowercase)
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

fn rewrite_query_leaves(query: &mut Query, zone: &SessionTimeZone) {
    let mut bodies: Vec<&mut SetExpr> = vec![query.body.as_mut()];
    while let Some(body) = bodies.pop() {
        match body {
            SetExpr::Select(select) => {
                for item in &mut select.projection {
                    match item {
                        SelectItem::UnnamedExpr(inner)
                        | SelectItem::ExprWithAlias { expr: inner, .. } => {
                            rewrite_timestamp_leaves(inner, zone);
                        }
                        _ => {}
                    }
                }
                if let Some(selection) = select.selection.as_mut() {
                    rewrite_timestamp_leaves(selection, zone);
                }
                if let Some(having) = select.having.as_mut() {
                    rewrite_timestamp_leaves(having, zone);
                }
            }
            SetExpr::Query(inner) => bodies.push(inner.body.as_mut()),
            SetExpr::SetOperation { left, right, .. } => {
                bodies.push(left.as_mut());
                bodies.push(right.as_mut());
            }
            _ => {}
        }
    }
}

pub(crate) fn rewrite_timestamp_leaves(expr: &mut Expr, zone: &SessionTimeZone) {
    match expr {
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            if matches!(
                data_type,
                datafusion::sql::sqlparser::ast::DataType::Timestamp(_, _)
            ) {
                rewrite_cast_input(inner, zone);
            }
            rewrite_timestamp_leaves(inner, zone);
        }
        Expr::TypedString(typed) => {
            if typed
                .data_type
                .to_string()
                .eq_ignore_ascii_case("timestamp")
            {
                rewrite_value_leaf(&mut typed.value, zone);
            }
        }
        Expr::Subquery(query) => rewrite_query_leaves(query, zone),
        Expr::Exists { subquery, .. } => rewrite_query_leaves(subquery, zone),
        Expr::InSubquery { expr, subquery, .. } => {
            rewrite_timestamp_leaves(expr, zone);
            rewrite_query_leaves(subquery, zone);
        }
        _ => {
            if let Some(name) = function_last_name(expr)
                && name == "to_timestamp"
            {
                rewrite_to_timestamp_arg(expr, zone);
            }
            for inner in child_exprs_mut(expr) {
                rewrite_timestamp_leaves(inner, zone);
            }
            for inner in function_exprs_mut(expr) {
                rewrite_timestamp_leaves(inner, zone);
            }
        }
    }
}

fn rewrite_cast_input(inner: &mut Expr, zone: &SessionTimeZone) {
    if let Expr::Value(wrapped) = inner {
        rewrite_value_leaf(wrapped, zone);
    }
}

fn rewrite_value_leaf(
    wrapped: &mut datafusion::sql::sqlparser::ast::ValueWithSpan,
    zone: &SessionTimeZone,
) {
    if let Value::SingleQuotedString(text) = &mut wrapped.value {
        rewrite_string_leaf(text, zone);
    }
}

fn rewrite_to_timestamp_arg(expr: &mut Expr, zone: &SessionTimeZone) {
    let Expr::Function(function) = expr else {
        return;
    };
    let FunctionArguments::List(list) = &mut function.args else {
        return;
    };
    if list.args.len() != 1 {
        return;
    }
    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(wrapped))) = &mut list.args[0] {
        rewrite_value_leaf(wrapped, zone);
    }
}

fn rewrite_string_leaf(text: &mut String, zone: &SessionTimeZone) {
    if let Some(millis) = parse_timestamp_string_to_ms(text, zone) {
        *text = utc_offset_literal(millis);
    }
}

fn utc_offset_literal(millis: i64) -> String {
    DateTime::from_timestamp_millis(millis).map_or_else(
        || millis.to_string(),
        |moment| moment.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string(),
    )
}

fn child_exprs_mut(expr: &mut Expr) -> Vec<&mut Expr> {
    match expr {
        Expr::Nested(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            vec![inner.as_mut()]
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => vec![left.as_mut(), right.as_mut()],
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Collate { expr, .. } => vec![expr.as_mut()],
        Expr::Between {
            expr, low, high, ..
        } => vec![expr.as_mut(), low.as_mut(), high.as_mut()],
        Expr::InList { expr, list, .. } => {
            let mut out = vec![expr.as_mut()];
            out.extend(list.iter_mut());
            out
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut out = Vec::new();
            if let Some(operand) = operand.as_mut() {
                out.push(operand.as_mut());
            }
            for when in conditions.iter_mut() {
                out.push(&mut when.condition);
                out.push(&mut when.result);
            }
            if let Some(other) = else_result.as_mut() {
                out.push(other.as_mut());
            }
            out
        }
        Expr::Tuple(items) => items.iter_mut().collect(),
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } => vec![expr.as_mut(), pattern.as_mut()],
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut out = vec![expr.as_mut()];
            if let Some(from) = substring_from.as_mut() {
                out.push(from.as_mut());
            }
            if let Some(count) = substring_for.as_mut() {
                out.push(count.as_mut());
            }
            out
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            let mut out = vec![expr.as_mut()];
            if let Some(what) = trim_what.as_mut() {
                out.push(what.as_mut());
            }
            if let Some(characters) = trim_characters.as_mut() {
                out.extend(characters.iter_mut());
            }
            out
        }
        Expr::AtTimeZone { timestamp, .. } => vec![timestamp.as_mut()],
        _ => Vec::new(),
    }
}

fn function_exprs_mut(expr: &mut Expr) -> Vec<&mut Expr> {
    let Expr::Function(function) = expr else {
        return Vec::new();
    };
    let FunctionArguments::List(list) = &mut function.args else {
        return Vec::new();
    };
    list.args
        .iter_mut()
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
