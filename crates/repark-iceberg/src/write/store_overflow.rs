use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, TableReference};
use datafusion::error::Result;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, WriteOp,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::scalar::ScalarValue;

use super::store_assign::refuse_unless_write_store_assignable;
use super::store_fold::{
    check_folded_store_input, expr_references_column, store_guard_expr, wrap_store_expr,
};
use crate::write::store_cast::{
    is_overflow_store_pair, spark_store_type_name, store_cast_udf_for_target, store_int_target,
};

pub const STORE_DIVISOR_GUARD_NAME: &str = "__repark_ansi_nonzero_divisor__";
pub const STORE_DECIMAL_DIV_NAME: &str = "__repark_spark_decimal_div__";
const ARROW_CAST_NAME: &str = "arrow_cast";

pub(crate) const CONVERTIBLE_EVAL_HEADS: [&str; 3] = [
    "[DIVIDE_BY_ZERO]",
    "[ARITHMETIC_OVERFLOW]",
    "[NUMERIC_VALUE_OUT_OF_RANGE]",
];

#[derive(Debug, Default, Clone, Copy)]
pub struct StoreOverflowCast;

impl AnalyzerRule for StoreOverflowCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(transform_store_dml)
            .data()
    }

    fn name(&self) -> &'static str {
        "repark_store_overflow_cast"
    }
}

pub(crate) async fn analyzed_store_source(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<LogicalPlan> {
    let state = ctx.state();
    let raw = state.create_logical_plan(sql).await?;
    state
        .analyzer()
        .execute_and_check(raw, state.config_options(), |_, _| {})
}

pub(crate) struct StoreIssue {
    pub(crate) column: String,
    pub(crate) source_name: String,
    pub(crate) target: DataType,
    pub(crate) target_name: String,
}

fn store_issue(column: &str, source: &DataType, target: &DataType) -> Option<StoreIssue> {
    if !is_overflow_store_pair(source, target) {
        return None;
    }
    let (Some(source_name), Some(target_name)) =
        (spark_store_type_name(source), spark_store_type_name(target))
    else {
        return None;
    };
    store_cast_udf_for_target(target)?;
    Some(StoreIssue {
        column: column.to_string(),
        source_name,
        target: target.clone(),
        target_name,
    })
}

fn transform_store_dml(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Dml(mut dml) = plan else {
        return Ok(Transformed::no(plan));
    };
    let fold = match dml.op {
        WriteOp::Insert(
            datafusion::logical_expr::dml::InsertOp::Append
            | datafusion::logical_expr::dml::InsertOp::Overwrite,
        ) => true,
        WriteOp::Update => false,
        _ => return Ok(Transformed::no(LogicalPlan::Dml(dml))),
    };
    if matches!(dml.input.as_ref(), LogicalPlan::Values(_)) {
        let LogicalPlan::Values(values) = dml.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Dml(dml)));
        };
        let Some(rewritten) = conform_values_rows(values, dml.target.schema().as_ref(), fold)?
        else {
            return Ok(Transformed::no(LogicalPlan::Dml(dml)));
        };
        dml.input = Arc::new(rewritten);
        return Ok(Transformed::yes(LogicalPlan::Dml(dml)));
    }
    if let LogicalPlan::Projection(projection) = dml.input.as_ref()
        && let LogicalPlan::Values(values) = projection.input.as_ref()
        && let Some(rewritten) = conform_values_rows(values, dml.target.schema().as_ref(), fold)?
    {
        dml.input = Arc::new(LogicalPlan::Projection(Projection::try_new(
            projection.expr.clone(),
            Arc::new(rewritten),
        )?));
        return Ok(Transformed::yes(LogicalPlan::Dml(dml)));
    }
    let (exprs, input) = match dml.input.as_ref() {
        LogicalPlan::Projection(projection) => {
            (projection.expr.clone(), Arc::clone(&projection.input))
        }
        other => (
            other
                .schema()
                .iter()
                .map(|(qualifier, field)| {
                    Expr::Column(Column::new(qualifier.cloned(), field.name()))
                })
                .collect(),
            Arc::new(other.clone()),
        ),
    };
    let Some((rewritten, input)) =
        conform_store_exprs(&exprs, &input, dml.target.schema().as_ref(), fold)?
    else {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    };
    dml.input = Arc::new(LogicalPlan::Projection(Projection::try_new(
        rewritten, input,
    )?));
    Ok(Transformed::yes(LogicalPlan::Dml(dml)))
}

pub(crate) fn split_aliases(mut expr: &Expr) -> (&Expr, Vec<(Option<TableReference>, String)>) {
    let mut frames = Vec::new();
    while let Expr::Alias(alias) = expr {
        frames.push((alias.relation.clone(), alias.name.clone()));
        expr = alias.expr.as_ref();
    }
    (expr, frames)
}

fn store_output_name(inner: &Expr, frames: &[(Option<TableReference>, String)]) -> String {
    if let Some((_, name)) = frames.first() {
        return name.clone();
    }
    if let Expr::Column(column) = inner {
        return column.name.clone();
    }
    inner.schema_name().to_string()
}

fn rewrap_aliases(mut inner: Expr, frames: &[(Option<TableReference>, String)]) -> Expr {
    for (relation, name) in frames.iter().rev() {
        inner = Expr::Alias(Alias::new(inner, relation.clone(), name.clone()));
    }
    inner
}

fn conform_values_rows(
    values: &datafusion::logical_expr::Values,
    target: &ArrowSchema,
    fold: bool,
) -> Result<Option<LogicalPlan>> {
    if target.fields().len() != values.schema.fields().len() {
        return Ok(None);
    }
    let mut changed = false;
    let mut rows = Vec::with_capacity(values.values.len());
    for row in &values.values {
        if row.len() != target.fields().len() {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(row.len());
        for (expr, target_field) in row.iter().zip(target.fields()) {
            let (inner, frames) = split_aliases(expr);
            let Expr::Cast(cast) = inner else {
                out.push(expr.clone());
                continue;
            };
            let Ok(source) = cast.expr.get_type(&DFSchema::empty()) else {
                out.push(expr.clone());
                continue;
            };
            let Some(issue) = store_issue(target_field.name(), &source, cast.field.data_type())
            else {
                out.push(expr.clone());
                continue;
            };
            if fold {
                check_folded_store_input(&issue, &cast.expr, None, true)?;
            }
            let swapped = swap_guards_in_expr((*cast.expr).clone(), &issue);
            out.push(rewrap_aliases(wrap_store_expr(&issue, swapped), &frames));
            changed = true;
        }
        rows.push(out);
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(LogicalPlanBuilder::values(rows)?.build()?))
}

fn target_field_by_name<'a>(
    target: &'a ArrowSchema,
    name: &str,
) -> Option<&'a datafusion::arrow::datatypes::Field> {
    target
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .or_else(|| {
            target
                .fields()
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(name))
        })
        .map(AsRef::as_ref)
}

fn conform_store_exprs(
    exprs: &[Expr],
    input: &Arc<LogicalPlan>,
    target: &ArrowSchema,
    fold: bool,
) -> Result<Option<(Vec<Expr>, Arc<LogicalPlan>)>> {
    let schema = input.schema().clone();
    let mut input = Arc::clone(input);
    let mut changed = false;
    let mut out = Vec::with_capacity(exprs.len());
    for (position, expr) in exprs.iter().enumerate() {
        let (inner, frames) = split_aliases(expr);
        let name = store_output_name(inner, &frames);
        let target_field = target_field_by_name(target, &name).or_else(|| {
            frames
                .is_empty()
                .then(|| target.fields().get(position).map(AsRef::as_ref))
                .flatten()
        });
        let Some(target_field) = target_field else {
            out.push(expr.clone());
            continue;
        };
        let flags = ConformStoreFlags {
            fold,
            in_values: false,
            unwrap_cast: true,
        };
        let Some(rewritten) = conform_one_store_expr(
            inner,
            schema.as_ref(),
            &mut input,
            target_field.name(),
            target_field.data_type(),
            flags,
        )?
        else {
            out.push(expr.clone());
            continue;
        };
        out.push(rewrap_aliases(rewritten, &frames));
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some((out, input)))
}

#[derive(Debug, Clone, Copy)]
struct ConformStoreFlags {
    fold: bool,
    in_values: bool,
    unwrap_cast: bool,
}

fn conform_one_store_expr(
    inner: &Expr,
    schema: &DFSchema,
    input: &mut Arc<LogicalPlan>,
    column: &str,
    target_type: &DataType,
    flags: ConformStoreFlags,
) -> Result<Option<Expr>> {
    if let Expr::ScalarFunction(function) = inner
        && store_int_target(function.func.name()).is_some()
    {
        return Ok(None);
    }
    let nested = rewrite_nested_arrow_casts(
        inner.clone(),
        schema,
        input,
        column,
        target_type,
        flags.fold,
    )?;
    let nested_changed = nested != *inner;
    let inner = &nested;
    let (value, source, cast_target) = store_cast_parts(inner, schema, flags.unwrap_cast);
    if let Some(cast_target) = cast_target
        && &cast_target != target_type
    {
        return Ok(nested_changed.then(|| nested.clone()));
    }
    let Some(issue) = store_issue(column, &source, target_type) else {
        return Ok(nested_changed.then(|| nested.clone()));
    };
    if flags.fold {
        check_folded_store_input(&issue, &value, Some(input.as_ref()), flags.in_values)?;
    }
    swap_lineage_guards_for_expr(input, &value, &issue);
    Ok(Some(wrap_store_expr(
        &issue,
        swap_guards_in_expr(value, &issue),
    )))
}

fn store_cast_parts(
    inner: &Expr,
    schema: &DFSchema,
    unwrap_cast: bool,
) -> (Expr, DataType, Option<DataType>) {
    if unwrap_cast && let Expr::Cast(cast) = inner {
        let Ok(source) = cast.expr.get_type(schema) else {
            return (inner.clone(), DataType::Null, None);
        };
        return (
            (*cast.expr).clone(),
            source,
            Some(cast.field.data_type().clone()),
        );
    }
    if let Expr::ScalarFunction(function) = inner
        && function.func.name() == ARROW_CAST_NAME
        && function.args.len() == 2
        && let Expr::Literal(ScalarValue::Utf8(Some(name)), _) = &function.args[1]
        && let Some(target) = arrow_cast_target_name(name)
        && let Ok(source) = function.args[0].get_type(schema)
    {
        return (function.args[0].clone(), source, Some(target));
    }
    let Ok(source) = inner.get_type(schema) else {
        return (inner.clone(), DataType::Null, None);
    };
    (inner.clone(), source, None)
}

fn arrow_cast_target_name(name: &str) -> Option<DataType> {
    match name {
        "Int8" => Some(DataType::Int8),
        "Int16" => Some(DataType::Int16),
        "Int32" => Some(DataType::Int32),
        "Int64" => Some(DataType::Int64),
        _ => None,
    }
}

fn rewrite_nested_arrow_casts(
    inner: Expr,
    schema: &DFSchema,
    input: &mut Arc<LogicalPlan>,
    column: &str,
    target_type: &DataType,
    fold: bool,
) -> Result<Expr> {
    inner
        .transform_up(|node| {
            Ok(match node {
                Expr::ScalarFunction(function) if function.func.name() == ARROW_CAST_NAME => {
                    match nested_arrow_cast_rewrite(
                        &function,
                        schema,
                        input,
                        column,
                        target_type,
                        fold,
                    )? {
                        Some(rewritten) => Transformed::yes(rewritten),
                        None => Transformed::no(Expr::ScalarFunction(function)),
                    }
                }
                node => Transformed::no(node),
            })
        })
        .data()
}

fn nested_arrow_cast_rewrite(
    function: &ScalarFunction,
    schema: &DFSchema,
    input: &mut Arc<LogicalPlan>,
    column: &str,
    target_type: &DataType,
    fold: bool,
) -> Result<Option<Expr>> {
    if function.args.len() != 2 {
        return Ok(None);
    }
    let Expr::Literal(ScalarValue::Utf8(Some(name)), _) = &function.args[1] else {
        return Ok(None);
    };
    let Some(nested_target) = arrow_cast_target_name(name) else {
        return Ok(None);
    };
    if nested_target != *target_type {
        return Ok(None);
    }
    let Ok(source) = function.args[0].get_type(schema) else {
        return Ok(None);
    };
    let Some(issue) = store_issue(column, &source, target_type) else {
        return Ok(None);
    };
    if fold {
        check_folded_store_input(&issue, &function.args[0], Some(input.as_ref()), false)?;
    }
    swap_lineage_guards_for_expr(input, &function.args[0], &issue);
    Ok(Some(wrap_store_expr(
        &issue,
        swap_guards_in_expr(function.args[0].clone(), &issue),
    )))
}

fn swap_lineage_guards_for_expr(input: &mut Arc<LogicalPlan>, expr: &Expr, issue: &StoreIssue) {
    let mut columns = Vec::new();
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node {
            columns.push(column.name.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    columns.sort();
    columns.dedup();
    for column in columns {
        swap_defining_project_guards(Arc::make_mut(input), &column, issue);
    }
}

fn expr_list_references_column(exprs: &[Expr], column: &str) -> bool {
    exprs
        .iter()
        .any(|expr| expr_references_column(expr, column))
}

fn swap_defining_project_guards(plan: &mut LogicalPlan, column: &str, issue: &StoreIssue) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            let position = projection
                .expr
                .iter()
                .position(|candidate| match candidate {
                    Expr::Alias(alias) => alias.name == *column,
                    Expr::Column(candidate) => candidate.name == *column,
                    other => other.schema_name().to_string() == *column,
                });
            let Some(index) = position else {
                return false;
            };
            let next = match split_aliases(&projection.expr[index]).0 {
                Expr::Column(next) => Some(next.name.clone()),
                _ => None,
            };
            if let Some(next) = next {
                return swap_defining_project_guards(
                    Arc::make_mut(&mut projection.input),
                    &next,
                    issue,
                );
            }
            let mut defining = projection.expr[index].clone();
            if swap_guards_in_expr_owned(&mut defining, issue) {
                projection.expr[index] = defining;
                return true;
            }
            false
        }
        LogicalPlan::SubqueryAlias(alias) => {
            swap_defining_project_guards(Arc::make_mut(&mut alias.input), column, issue)
        }
        LogicalPlan::Filter(filter) => {
            if expr_list_references_column(std::slice::from_ref(&filter.predicate), column) {
                return false;
            }
            swap_defining_project_guards(Arc::make_mut(&mut filter.input), column, issue)
        }
        LogicalPlan::Sort(sort) => {
            let keys: Vec<Expr> = sort.expr.iter().map(|key| key.expr.clone()).collect();
            if expr_list_references_column(&keys, column) {
                return false;
            }
            swap_defining_project_guards(Arc::make_mut(&mut sort.input), column, issue)
        }
        LogicalPlan::Limit(limit) => {
            swap_defining_project_guards(Arc::make_mut(&mut limit.input), column, issue)
        }
        _ => false,
    }
}

fn swap_guards_in_expr(expr: Expr, issue: &StoreIssue) -> Expr {
    let mut owned = expr;
    swap_guards_in_expr_owned(&mut owned, issue);
    owned
}

fn swap_guards_in_expr_owned(expr: &mut Expr, issue: &StoreIssue) -> bool {
    let mut changed = false;
    let current = std::mem::replace(expr, Expr::Literal(ScalarValue::Null, None));
    let fallback = current.clone();
    let rewritten = current
        .transform_up(|node| {
            Ok(match node {
                Expr::ScalarFunction(function) => {
                    Transformed::yes(swap_guard_call(function, issue, &mut changed))
                }
                Expr::ScalarSubquery(mut query) => {
                    let (plan, swapped) = swap_guards_in_plan((*query.subquery).clone(), issue);
                    changed |= swapped;
                    query.subquery = Arc::new(plan);
                    Transformed::yes(Expr::ScalarSubquery(query))
                }
                Expr::Exists(mut exists) => {
                    let (plan, swapped) =
                        swap_guards_in_plan((*exists.subquery.subquery).clone(), issue);
                    changed |= swapped;
                    exists.subquery.subquery = Arc::new(plan);
                    Transformed::yes(Expr::Exists(exists))
                }
                Expr::InSubquery(mut query) => {
                    let (plan, swapped) =
                        swap_guards_in_plan((*query.subquery.subquery).clone(), issue);
                    changed |= swapped;
                    query.subquery.subquery = Arc::new(plan);
                    Transformed::yes(Expr::InSubquery(query))
                }
                node => Transformed::no(node),
            })
        })
        .data();
    match rewritten {
        Ok(done) => {
            *expr = done;
        }
        Err(_) => {
            *expr = fallback;
        }
    }
    changed
}

fn swap_guard_call(mut function: ScalarFunction, issue: &StoreIssue, changed: &mut bool) -> Expr {
    if function.func.name() == STORE_DIVISOR_GUARD_NAME && function.args.len() == 1 {
        *changed = true;
        let mut args = std::mem::take(&mut function.args);
        let divisor = std::mem::replace(&mut args[0], Expr::Literal(ScalarValue::Null, None));
        return store_guard_expr(divisor, issue);
    }
    if function.func.name() == STORE_DECIMAL_DIV_NAME && function.args.len() == 2 {
        let mut args = std::mem::take(&mut function.args);
        let divisor = std::mem::replace(&mut args[1], Expr::Literal(ScalarValue::Null, None));
        args[1] = store_guard_expr(divisor, issue);
        *changed = true;
        function.args = args;
        return Expr::ScalarFunction(function);
    }
    Expr::ScalarFunction(function)
}

fn swap_guards_in_plan(plan: LogicalPlan, issue: &StoreIssue) -> (LogicalPlan, bool) {
    let mut changed = false;
    let fallback = plan.clone();
    let rewritten = plan
        .transform_up(|node| {
            node.map_expressions(|expr| {
                let mut owned = expr;
                let swapped = swap_guards_in_expr_owned(&mut owned, issue);
                changed |= swapped;
                Ok(if swapped {
                    Transformed::yes(owned)
                } else {
                    Transformed::no(owned)
                })
            })
        })
        .data();
    match rewritten {
        Ok(plan) => (plan, changed),
        Err(_) => (fallback, false),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn wrap_store_outputs(
    plan: LogicalPlan,
    targets: &[(String, DataType)],
    by_name: bool,
    fold: bool,
    gate_op: Option<&str>,
) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(plan);
    };
    if let Some(op) = gate_op {
        judge_wrap_store_assignable(&projection, targets, by_name, op)?;
    }
    let mut input = Arc::clone(&projection.input);
    let schema = input.schema().clone();
    let mut changed = false;
    let mut out = Vec::with_capacity(projection.expr.len());
    for (position, expr) in projection.expr.iter().enumerate() {
        let (inner, frames) = split_aliases(expr);
        let name = store_output_name(inner, &frames);
        let target = if by_name {
            targets
                .iter()
                .find(|(candidate, _)| candidate == &name)
                .or_else(|| {
                    targets
                        .iter()
                        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&name))
                })
        } else {
            targets.get(position)
        };
        let Some((column, target_type)) = target else {
            out.push(expr.clone());
            continue;
        };
        let flags = ConformStoreFlags {
            fold,
            in_values: false,
            unwrap_cast: false,
        };
        let conformed = conform_one_store_expr(
            inner,
            schema.as_ref(),
            &mut input,
            column,
            target_type,
            flags,
        )?;
        let Some(rewritten) = conformed else {
            out.push(expr.clone());
            continue;
        };
        if frames.is_empty() {
            out.push(rewritten.alias(column));
        } else {
            out.push(rewrap_aliases(rewritten, &frames));
        }
        changed = true;
    }
    if !changed {
        return Ok(LogicalPlan::Projection(projection));
    }
    Ok(LogicalPlan::Projection(Projection::try_new(out, input)?))
}

fn judge_wrap_store_assignable(
    projection: &Projection,
    targets: &[(String, DataType)],
    by_name: bool,
    op: &str,
) -> Result<()> {
    let schema = projection.input.schema();
    for (position, expr) in projection.expr.iter().enumerate() {
        let (inner, frames) = split_aliases(expr);
        let name = store_output_name(inner, &frames);
        let target = if by_name {
            targets
                .iter()
                .find(|(candidate, _)| candidate == &name)
                .or_else(|| {
                    targets
                        .iter()
                        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&name))
                })
        } else {
            targets.get(position)
        };
        let Some((column, target_type)) = target else {
            continue;
        };
        let Ok(source) = inner.get_type(schema.as_ref()) else {
            continue;
        };
        refuse_unless_write_store_assignable(op, column, &source, target_type)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(StoreOverflowCast));
        crate::write::store_cast::register_store_cast_udfs(&ctx);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(Arc::clone(&target), vec![vec![]]).unwrap()),
        )
        .unwrap();
        let source = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("d", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&source),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![1e19, 42.0])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "s",
            Arc::new(MemTable::try_new(source, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> Result<(), String> {
        match ctx.sql(sql).await {
            Err(error) => Err(error.to_string()),
            Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
        }
    }

    fn overflow_head(message: &str, from: &str, to: &str, column: &str) {
        assert!(
            message.contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{message}"
        );
        assert!(message.contains(&format!("\"{from}\" type")), "{message}");
        assert!(message.contains(&format!("\"{to}\" type")), "{message}");
        assert!(message.contains(&format!("`{column}`")), "{message}");
        assert!(message.contains("SQLSTATE: 22003"), "{message}");
    }

    #[tokio::test]
    async fn insert_select_const_overflow_refuses_at_plan() {
        let ctx = ctx();
        for value in ["1e19", "-1e19", "1e100"] {
            let message = run(&ctx, &format!("INSERT INTO t SELECT 10, {value}"))
                .await
                .expect_err("const overflow must refuse");
            overflow_head(&message, "DOUBLE", "BIGINT", "v");
        }
    }

    #[tokio::test]
    async fn insert_select_const_overflow_refuses_over_empty_source() {
        let ctx = ctx();
        let empty = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("d", DataType::Float64, true),
        ]));
        ctx.register_table(
            "e",
            Arc::new(MemTable::try_new(empty, vec![vec![]]).unwrap()),
        )
        .unwrap();
        let message = run(&ctx, "INSERT INTO t SELECT id, 1e19 FROM e")
            .await
            .expect_err("const overflow over empty source must refuse");
        overflow_head(&message, "DOUBLE", "BIGINT", "v");
    }

    #[tokio::test]
    async fn insert_values_overflow_refuses_at_plan() {
        let ctx = ctx();
        let message = run(&ctx, "INSERT INTO t VALUES (10, 1e19)")
            .await
            .expect_err("values overflow must refuse");
        overflow_head(&message, "DOUBLE", "BIGINT", "v");
    }

    #[tokio::test]
    async fn insert_column_overflow_refuses_at_collect() {
        let ctx = ctx();
        let message = run(&ctx, "INSERT INTO t SELECT id, d FROM s")
            .await
            .expect_err("column overflow must refuse");
        overflow_head(&message, "DOUBLE", "BIGINT", "v");
    }

    #[tokio::test]
    async fn insert_through_a_subquery_folds_like_spark() {
        let ctx = ctx();
        let message = run(
            &ctx,
            "INSERT INTO t SELECT id, v FROM (SELECT 1 AS id, 1e19 AS v) s",
        )
        .await
        .expect_err("subquery const must refuse");
        overflow_head(&message, "DOUBLE", "BIGINT", "v");
    }

    #[tokio::test]
    async fn in_range_fractional_values_store_truncated() {
        let ctx = ctx();
        run(&ctx, "INSERT INTO t SELECT 10, 42.0")
            .await
            .expect("42.0 stores");
        run(&ctx, "INSERT INTO t SELECT 11, 1.5")
            .await
            .expect("1.5 stores");
        run(&ctx, "INSERT INTO t SELECT 12, -1.5")
            .await
            .expect("-1.5 stores");
        let rows = ctx
            .sql("SELECT id, v FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let values: Vec<i64> = rows
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![42, 1, -1]);
    }

    #[tokio::test]
    async fn two_to_the_63_stores_max_like_spark() {
        let ctx = ctx();
        run(&ctx, "INSERT INTO t SELECT 10, 9.223372036854776e18")
            .await
            .expect("2^63 stores");
        let rows = ctx
            .sql("SELECT v FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let value = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(value, i64::MAX);
    }

    #[tokio::test]
    async fn int_pairs_and_strings_keep_their_old_text() {
        let ctx = ctx();
        let narrow = run(&ctx, "INSERT INTO t SELECT 10, CAST(1 AS BIGINT)").await;
        assert!(narrow.is_ok(), "widening stores");
        let message = run(&ctx, "INSERT INTO t SELECT 'abc', 'def'")
            .await
            .expect_err("strings must refuse");
        assert!(
            !message.contains("CAST_OVERFLOW_IN_TABLE_INSERT"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memtable_update_is_lazy_like_spark() {
        let ctx = ctx();
        run(&ctx, "INSERT INTO t VALUES (1, 0)").await.unwrap();
        run(&ctx, "UPDATE t SET v = 1e19 WHERE false")
            .await
            .expect("where-false stays success");
        let message = run(&ctx, "UPDATE t SET v = 1e19")
            .await
            .expect_err("matched const refuses");
        overflow_head(&message, "DOUBLE", "BIGINT", "v");
    }

    #[tokio::test]
    async fn divisor_guards_swap_under_a_store() {
        use datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
        };
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct DummyGuard {
            signature: Signature,
        }
        impl ScalarUDFImpl for DummyGuard {
            fn name(&self) -> &str {
                STORE_DIVISOR_GUARD_NAME
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                Ok(arg_types[0].clone())
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                Ok(args.args[0].clone())
            }
        }
        let guard = Arc::new(datafusion::logical_expr::ScalarUDF::from(DummyGuard {
            signature: Signature::any(1, datafusion::logical_expr::Volatility::Immutable),
        }));
        let issue = store_issue("v", &DataType::Float64, &DataType::Int64).unwrap();
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(Expr::Literal(ScalarValue::Float64(Some(0.0)), None)),
            datafusion::logical_expr::Operator::Divide,
            Box::new(Expr::ScalarFunction(ScalarFunction::new_udf(
                guard,
                vec![Expr::Literal(ScalarValue::Float64(Some(0.0)), None)],
            ))),
        ));
        let swapped = swap_guards_in_expr(expr, &issue);
        let text = format!("{swapped:?}");
        assert!(text.contains("StoreIntGuard"), "{text}");
        assert!(!text.contains("DummyGuard"), "{text}");
    }
}
