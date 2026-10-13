use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Case, Expr, ExprSchemable};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;

use crate::timestamp_ns_cast::timestamp_ns_cast_expr;

enum OperandKind {
    Ns(Option<Arc<str>>),
    UntypedNull,
    Other,
}

fn classify_operand(operand: &Expr, schema: &DFSchema) -> OperandKind {
    if matches!(operand, Expr::Literal(ScalarValue::Null, _)) {
        return OperandKind::UntypedNull;
    }
    let Ok(data_type) = operand.get_type(schema) else {
        return OperandKind::Other;
    };
    match data_type {
        DataType::Null => OperandKind::UntypedNull,
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => OperandKind::Ns(zone),
        _ => OperandKind::Other,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SiteRewrite {
    Naive,
    Zoned(Arc<str>),
    Skip,
}

fn site_target(rewrite: &SiteRewrite) -> Option<DataType> {
    match rewrite {
        SiteRewrite::Naive => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        SiteRewrite::Zoned(zone) => Some(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some(Arc::clone(zone)),
        )),
        SiteRewrite::Skip => None,
    }
}

fn df_coerced(node: &Expr, schema: &DFSchema) -> Option<Expr> {
    let mut ask = TypeCoercionRewriter::new(schema);
    node.clone().rewrite(&mut ask).ok().map(|out| out.data)
}

fn coerced_member_types(node: &Expr, schema: &DFSchema) -> Option<Vec<DataType>> {
    let answered = df_coerced(node, schema)?;
    if let Expr::Case(case) = &answered {
        let mut types = Vec::with_capacity(case.when_then_expr.len() + 1);
        for (_, then_branch) in &case.when_then_expr {
            types.push(then_branch.get_type(schema).ok()?);
        }
        if let Some(else_branch) = &case.else_expr {
            types.push(else_branch.get_type(schema).ok()?);
        }
        return Some(types);
    }
    let args: &[Expr] = match &answered {
        Expr::ScalarFunction(function) => &function.args,
        Expr::AggregateFunction(aggregate) => &aggregate.params.args,
        Expr::WindowFunction(window) => &window.params.args,
        _ => return None,
    };
    args.iter()
        .map(|arg| arg.get_type(schema).ok())
        .collect::<Option<Vec<DataType>>>()
}

fn member_nulls(
    node: &Expr,
    start: usize,
    kinds: &[OperandKind],
    target: &DataType,
    schema: &DFSchema,
) -> Vec<bool> {
    let mut members = vec![false; kinds.len()];
    let Some(types) = coerced_member_types(node, schema) else {
        return members;
    };
    for (offset, kind) in kinds.iter().enumerate() {
        if !matches!(kind, OperandKind::UntypedNull) {
            continue;
        }
        if types.get(start + offset) == Some(target) {
            members[offset] = true;
        }
    }
    members
}

fn site_rewrite(operands: &[OperandKind]) -> SiteRewrite {
    let mut common: Option<Option<Arc<str>>> = None;
    let mut nulls = false;
    for operand in operands {
        match operand {
            OperandKind::Other => return SiteRewrite::Skip,
            OperandKind::UntypedNull => nulls = true,
            OperandKind::Ns(zone) => match &common {
                None => common = Some(zone.clone()),
                Some(seen) if seen == zone => {}
                Some(_) => return SiteRewrite::Skip,
            },
        }
    }
    if !nulls {
        return SiteRewrite::Skip;
    }
    match common {
        Some(None) => SiteRewrite::Naive,
        Some(Some(zone)) => SiteRewrite::Zoned(zone),
        None => SiteRewrite::Skip,
    }
}

fn is_excluded_call(name: &str) -> bool {
    name.eq_ignore_ascii_case("nvl") || name.eq_ignore_ascii_case("ifnull")
}

pub(crate) fn rewrite_scalar(function: ScalarFunction, schema: &DFSchema) -> Transformed<Expr> {
    if is_excluded_call(function.func.name()) {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let skip_first = function.func.name().eq_ignore_ascii_case("if") && function.args.len() == 3;
    let start = usize::from(skip_first);
    let kinds: Vec<OperandKind> = function
        .args
        .iter()
        .skip(start)
        .map(|arg| classify_operand(arg, schema))
        .collect();
    let Some(target) = site_target(&site_rewrite(&kinds)) else {
        return Transformed::no(Expr::ScalarFunction(function));
    };
    let members = member_nulls(
        &Expr::ScalarFunction(function.clone()),
        start,
        &kinds,
        &target,
        schema,
    );
    if !members.iter().any(|member| *member) {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let zoned = matches!(target, DataType::Timestamp(TimeUnit::Nanosecond, Some(_)));
    let mut args = Vec::with_capacity(function.args.len());
    for (index, arg) in function.args.into_iter().enumerate() {
        if index >= start && members[index - start] {
            args.push(timestamp_ns_cast_expr(arg, zoned));
        } else {
            args.push(arg);
        }
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction {
        func: function.func,
        args,
    }))
}

pub(crate) fn rewrite_case(case: Case, schema: &DFSchema) -> Transformed<Expr> {
    let mut branches: Vec<&Expr> = Vec::with_capacity(case.when_then_expr.len() + 1);
    for (_, then_branch) in &case.when_then_expr {
        branches.push(then_branch);
    }
    if let Some(else_branch) = &case.else_expr {
        branches.push(else_branch);
    }
    let kinds: Vec<OperandKind> = branches
        .iter()
        .map(|branch| classify_operand(branch, schema))
        .collect();
    let Some(target) = site_target(&site_rewrite(&kinds)) else {
        return Transformed::no(Expr::Case(case));
    };
    let members = member_nulls(&Expr::Case(case.clone()), 0, &kinds, &target, schema);
    if !members.iter().any(|member| *member) {
        return Transformed::no(Expr::Case(case));
    }
    let zoned = matches!(target, DataType::Timestamp(TimeUnit::Nanosecond, Some(_)));
    let mut when_then = Vec::with_capacity(case.when_then_expr.len());
    for (index, (when, then)) in case.when_then_expr.into_iter().enumerate() {
        if members[index] {
            when_then.push((when, Box::new(timestamp_ns_cast_expr(*then, zoned))));
        } else {
            when_then.push((when, then));
        }
    }
    let else_expr = case.else_expr.map(|expr| {
        if members[when_then.len()] {
            Box::new(timestamp_ns_cast_expr(*expr, zoned))
        } else {
            expr
        }
    });
    Transformed::yes(Expr::Case(Case {
        expr: case.expr,
        when_then_expr: when_then,
        else_expr,
    }))
}

pub(crate) fn rewrite_feed_root(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    let out = match expr {
        Expr::ScalarFunction(function) => rewrite_scalar(function.clone(), schema),
        Expr::Case(case) => rewrite_case(case.clone(), schema),
        _ => return None,
    };
    out.transformed.then_some(out.data)
}

pub(crate) fn feed_subtree_holds_site(root: &Expr, schema: &DFSchema) -> bool {
    let mut found = false;
    let _ = root.apply(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        if rewrite_feed_root(node, schema).is_some() {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}
