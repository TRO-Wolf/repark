use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{
    AggregateFunction, AggregateFunctionParams, ScalarFunction, WindowFunction,
    WindowFunctionParams,
};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Case, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, Union, Values,
};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SiteRewrite {
    Naive,
    Zoned,
    Skip,
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
        Some(Some(_)) => SiteRewrite::Zoned,
        None => SiteRewrite::Skip,
    }
}

fn is_excluded_call(name: &str) -> bool {
    name.eq_ignore_ascii_case("nvl") || name.eq_ignore_ascii_case("ifnull")
}

fn rewrite_scalar(function: ScalarFunction, schema: &DFSchema) -> Transformed<Expr> {
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
    let rewrite = site_rewrite(&kinds);
    if rewrite == SiteRewrite::Skip {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let zoned = rewrite == SiteRewrite::Zoned;
    let mut args = Vec::with_capacity(function.args.len());
    for (index, arg) in function.args.into_iter().enumerate() {
        if index >= start && matches!(kinds[index - start], OperandKind::UntypedNull) {
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

fn rewrite_aggregate(aggregate: AggregateFunction, schema: &DFSchema) -> Transformed<Expr> {
    if is_excluded_call(aggregate.func.name()) {
        return Transformed::no(Expr::AggregateFunction(aggregate));
    }
    let kinds: Vec<OperandKind> = aggregate
        .params
        .args
        .iter()
        .map(|arg| classify_operand(arg, schema))
        .collect();
    let rewrite = site_rewrite(&kinds);
    if rewrite == SiteRewrite::Skip {
        return Transformed::no(Expr::AggregateFunction(aggregate));
    }
    let zoned = rewrite == SiteRewrite::Zoned;
    let mut args = Vec::with_capacity(aggregate.params.args.len());
    for (arg, kind) in aggregate.params.args.into_iter().zip(kinds.iter()) {
        if matches!(kind, OperandKind::UntypedNull) {
            args.push(timestamp_ns_cast_expr(arg, zoned));
        } else {
            args.push(arg);
        }
    }
    Transformed::yes(Expr::AggregateFunction(AggregateFunction {
        func: aggregate.func,
        params: AggregateFunctionParams {
            args,
            ..aggregate.params
        },
    }))
}

fn rewrite_window(window: Box<WindowFunction>, schema: &DFSchema) -> Transformed<Expr> {
    if is_excluded_call(window.fun.name()) {
        return Transformed::no(Expr::WindowFunction(window));
    }
    let window = *window;
    let kinds: Vec<OperandKind> = window
        .params
        .args
        .iter()
        .map(|arg| classify_operand(arg, schema))
        .collect();
    let rewrite = site_rewrite(&kinds);
    if rewrite == SiteRewrite::Skip {
        return Transformed::no(Expr::WindowFunction(Box::new(window)));
    }
    let zoned = rewrite == SiteRewrite::Zoned;
    let mut args = Vec::with_capacity(window.params.args.len());
    for (arg, kind) in window.params.args.into_iter().zip(kinds.iter()) {
        if matches!(kind, OperandKind::UntypedNull) {
            args.push(timestamp_ns_cast_expr(arg, zoned));
        } else {
            args.push(arg);
        }
    }
    Transformed::yes(Expr::WindowFunction(Box::new(WindowFunction {
        fun: window.fun,
        params: WindowFunctionParams {
            args,
            ..window.params
        },
    })))
}

fn rewrite_case(case: Case, schema: &DFSchema) -> Transformed<Expr> {
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
    let rewrite = site_rewrite(&kinds);
    if rewrite == SiteRewrite::Skip {
        return Transformed::no(Expr::Case(case));
    }
    let zoned = rewrite == SiteRewrite::Zoned;
    let mut when_then = Vec::with_capacity(case.when_then_expr.len());
    for (index, (when, then)) in case.when_then_expr.into_iter().enumerate() {
        if matches!(kinds[index], OperandKind::UntypedNull) {
            when_then.push((when, Box::new(timestamp_ns_cast_expr(*then, zoned))));
        } else {
            when_then.push((when, then));
        }
    }
    let else_expr = case.else_expr.map(|expr| {
        if matches!(kinds[when_then.len()], OperandKind::UntypedNull) {
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

fn rewrite_site(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::ScalarFunction(function) => rewrite_scalar(function, schema),
        Expr::AggregateFunction(aggregate) => rewrite_aggregate(aggregate, schema),
        Expr::WindowFunction(window) => rewrite_window(window, schema),
        Expr::Case(case) => rewrite_case(case, schema),
        other => Transformed::no(other),
    }
}

fn rewrite_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let widths: Vec<usize> = union
        .inputs
        .iter()
        .map(|input| input.schema().fields().len())
        .collect();
    let Some(first) = widths.first() else {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    };
    if widths.iter().any(|width| width != first) {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let width = *first;
    let mut wraps: Vec<Vec<(usize, bool)>> = vec![Vec::new(); union.inputs.len()];
    for column in 0..width {
        let kinds: Vec<OperandKind> = union
            .inputs
            .iter()
            .map(|input| match input.schema().fields()[column].data_type() {
                DataType::Null => OperandKind::UntypedNull,
                DataType::Timestamp(TimeUnit::Nanosecond, zone) => OperandKind::Ns(zone.clone()),
                _ => OperandKind::Other,
            })
            .collect();
        let rewrite = site_rewrite(&kinds);
        if rewrite == SiteRewrite::Skip {
            continue;
        }
        let zoned = rewrite == SiteRewrite::Zoned;
        for (index, kind) in kinds.iter().enumerate() {
            if matches!(kind, OperandKind::UntypedNull) {
                wraps[index].push((column, zoned));
            }
        }
    }
    if wraps.iter().all(Vec::is_empty) {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for (input, columns) in union.inputs.into_iter().zip(wraps) {
        if columns.is_empty() {
            inputs.push(input);
            continue;
        }
        let fields = input.schema().fields().clone();
        let found: Vec<Column> = input.schema().columns();
        let mut exprs = Vec::with_capacity(fields.len());
        for (position, field) in fields.iter().enumerate() {
            let base = Expr::Column(found[position].clone());
            if let Some((_, zoned)) = columns.iter().find(|(at, _)| *at == position) {
                exprs.push(timestamp_ns_cast_expr(base, *zoned).alias(field.name()));
            } else {
                exprs.push(base);
            }
        }
        inputs.push(Arc::new(LogicalPlan::Projection(Projection::try_new(
            exprs, input,
        )?)));
    }
    let rebuilt = Union::try_new_with_loose_types(inputs)?;
    Ok(Transformed::yes(LogicalPlan::Union(rebuilt)))
}

fn rewrite_values(values: Values) -> Result<Transformed<LogicalPlan>> {
    let schema = DFSchema::empty();
    let mut changed = false;
    let mut rows = Vec::with_capacity(values.values.len());
    for row in &values.values {
        let mut new_row = Vec::with_capacity(row.len());
        for expr in row {
            let out = expr
                .clone()
                .transform_down(|node| Ok(rewrite_site(node, &schema)))?;
            changed |= out.transformed;
            new_row.push(out.data);
        }
        rows.push(new_row);
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Values(values)));
    }
    let rebuilt = LogicalPlanBuilder::values(rows)?.build()?;
    Ok(Transformed::yes(rebuilt))
}

fn rewrite_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Values(values) = plan {
        return rewrite_values(values);
    }
    if let LogicalPlan::Union(union) = plan {
        return rewrite_union(union);
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let preserver = NamePreserver::new(&plan);
    let mapped = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        let out = expr.transform_down(|node| Ok(rewrite_site(node, &schema)))?;
        Ok(out.update_data(|node| saved.restore(node)))
    })?;
    if !mapped.transformed {
        return Ok(Transformed::no(mapped.data));
    }
    let rebuilt = mapped.map_data(LogicalPlan::recompute_schema)?.data;
    Ok(Transformed::yes(rebuilt))
}

#[allow(clippy::missing_errors_doc)]
pub fn rewrite_sites(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let out = plan.transform_up_with_subqueries(rewrite_node)?;
    if out.transformed {
        Ok(Some(out.data))
    } else {
        Ok(None)
    }
}
