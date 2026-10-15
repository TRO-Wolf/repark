use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, TableReference};
use datafusion::error::{DataFusionError, Result};
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
use super::update_cast::refuse_unassignable_leaves;
use crate::write::store_cast::{
    is_overflow_store_pair, spark_store_type_name, store_cast_udf_for_target, store_int_target,
};
use crate::write::struct_pairing::{
    StatementPairing, StructPairing, holds_struct_type, pair_store_value,
    spark_names_equal_ignoring_case, statement_pairing, store_pair_expr,
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
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| transform_store_dml(plan, config))
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

#[must_use]
pub fn table_label(table: &TableReference) -> String {
    let quote = |part: &str| format!("`{}`", part.replace('`', "``"));
    match table {
        TableReference::Bare { table } => quote(table),
        TableReference::Partial { schema, table } => {
            format!("{}.{}", quote(schema), quote(table))
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => format!("{}.{}.{}", quote(catalog), quote(schema), quote(table)),
    }
}

fn transform_store_dml(
    plan: LogicalPlan,
    config: &ConfigOptions,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Dml(mut dml) = plan else {
        return Ok(Transformed::no(plan));
    };
    let pairing = statement_pairing(config, &dml.op);
    let case_sensitive = config
        .extensions
        .get::<StatementPairing>()
        .is_some_and(|carried| carried.case_sensitive);
    let table = match dml.op {
        WriteOp::Update => "``".to_string(),
        _ => table_label(&dml.table_name),
    };
    let site = PairSite {
        pairing,
        table: &table,
    };
    let fold = match dml.op {
        WriteOp::Insert(
            datafusion::logical_expr::dml::InsertOp::Append
            | datafusion::logical_expr::dml::InsertOp::Overwrite,
        ) => true,
        WriteOp::Update => false,
        _ => return Ok(Transformed::no(LogicalPlan::Dml(dml))),
    };
    let target_schema = dml.target.schema();
    if let LogicalPlan::Values(values) = dml.input.as_ref() {
        let Some(targets) = positional_values_targets(values, target_schema.as_ref()) else {
            return Ok(Transformed::no(LogicalPlan::Dml(dml)));
        };
        let Some(rewritten) = conform_values_rows(values, &targets, fold, site, case_sensitive)?
        else {
            return Ok(Transformed::no(LogicalPlan::Dml(dml)));
        };
        dml.input = Arc::new(rewritten);
        return Ok(Transformed::yes(LogicalPlan::Dml(dml)));
    }
    if let LogicalPlan::Projection(projection) = dml.input.as_ref()
        && let LogicalPlan::Values(values) = projection.input.as_ref()
        && let Some(targets) = projected_values_targets(projection, values, target_schema.as_ref())
        && let Some(rewritten) = conform_values_rows(values, &targets, fold, site, case_sensitive)?
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
        conform_store_exprs(&exprs, &input, dml.target.schema().as_ref(), fold, site)?
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

fn positional_values_targets<'a>(
    values: &datafusion::logical_expr::Values,
    target: &'a ArrowSchema,
) -> Option<Vec<Option<&'a Field>>> {
    (target.fields().len() == values.schema.fields().len()).then(|| {
        target
            .fields()
            .iter()
            .map(|field| Some(field.as_ref()))
            .collect()
    })
}

fn projected_values_targets<'a>(
    projection: &Projection,
    values: &datafusion::logical_expr::Values,
    target: &'a ArrowSchema,
) -> Option<Vec<Option<&'a Field>>> {
    if projection.expr.len() != target.fields().len() {
        return None;
    }
    let mut targets = vec![None; values.schema.fields().len()];
    for (expr, field) in projection.expr.iter().zip(target.fields()) {
        let (mut inner, _) = split_aliases(expr);
        if let Expr::Cast(cast) = inner {
            inner = cast.expr.as_ref();
        }
        let Expr::Column(column) = inner else {
            continue;
        };
        let index = values.schema.index_of_column_by_name(None, &column.name)?;
        targets[index] = Some(field.as_ref());
    }
    Some(targets)
}

fn values_row_type(expr: &Expr) -> Option<DataType> {
    let (inner, _) = split_aliases(expr);
    let written = match inner {
        Expr::Cast(cast) => cast.expr.as_ref(),
        other => other,
    };
    written.get_type(&DFSchema::empty()).ok()
}

fn struct_names_agree(first: &DataType, other: &DataType, case_sensitive: bool) -> bool {
    match (first, other) {
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right.iter()).all(|(left, right)| {
                    let named_alike = if case_sensitive {
                        left.name() == right.name()
                    } else {
                        spark_names_equal_ignoring_case(left.name(), right.name())
                    };
                    named_alike
                        && struct_names_agree(left.data_type(), right.data_type(), case_sensitive)
                })
        }
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::FixedSizeList(left, _), DataType::FixedSizeList(right, _)) => {
            struct_names_agree(left.data_type(), right.data_type(), case_sensitive)
        }
        (DataType::Map(left, _), DataType::Map(right, _)) => {
            match (left.data_type(), right.data_type()) {
                (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
                    left.iter().zip(right.iter()).all(|(left, right)| {
                        struct_names_agree(left.data_type(), right.data_type(), case_sensitive)
                    })
                }
                _ => true,
            }
        }
        _ => true,
    }
}

fn refuse_disagreeing_values_rows(
    values: &datafusion::logical_expr::Values,
    case_sensitive: bool,
) -> Result<()> {
    let width = values.values.first().map_or(0, Vec::len);
    for column in 0..width {
        let mut first: Option<DataType> = None;
        for row in &values.values {
            let Some(written) = row.get(column).and_then(values_row_type) else {
                continue;
            };
            if !holds_struct_type(&written) {
                continue;
            }
            match &first {
                None => first = Some(written),
                Some(seen) if !struct_names_agree(seen, &written, case_sensitive) => {
                    return Err(DataFusionError::Plan(format!(
                        "[INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE] Invalid inline \
                         table. Found incompatible types in the column `col{}` for inline table. \
                         SQLSTATE: 42000",
                        column + 1
                    )));
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

fn conform_values_rows(
    values: &datafusion::logical_expr::Values,
    targets: &[Option<&Field>],
    fold: bool,
    site: PairSite<'_>,
    case_sensitive: bool,
) -> Result<Option<LogicalPlan>> {
    refuse_disagreeing_values_rows(values, case_sensitive)?;
    let mut changed = false;
    let mut rows = Vec::with_capacity(values.values.len());
    for row in &values.values {
        if row.len() != targets.len() {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(row.len());
        for (expr, target_field) in row.iter().zip(targets) {
            let Some(target_field) = target_field else {
                out.push(expr.clone());
                continue;
            };
            let (inner, frames) = split_aliases(expr);
            if let Some(paired) = pair_store_output(
                inner,
                &DFSchema::empty(),
                target_field.name(),
                target_field.data_type(),
                site,
                true,
            )? {
                out.push(rewrap_aliases(paired, &frames));
                changed = true;
                continue;
            }
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
    site: PairSite<'_>,
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
            site,
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
struct PairSite<'a> {
    pairing: StructPairing,
    table: &'a str,
}

#[derive(Debug, Clone, Copy)]
struct ConformStoreFlags<'a> {
    fold: bool,
    in_values: bool,
    unwrap_cast: bool,
    site: PairSite<'a>,
}

fn pair_store_output(
    inner: &Expr,
    schema: &DFSchema,
    column: &str,
    target: &DataType,
    site: PairSite<'_>,
    unwrap_cast: bool,
) -> Result<Option<Expr>> {
    if !holds_struct_type(target) {
        return Ok(None);
    }
    let (value, cast) = match inner {
        Expr::Cast(cast) => (cast.expr.as_ref(), Some(cast)),
        other => (other, None),
    };
    let Ok(source) = value.get_type(schema) else {
        return Ok(None);
    };
    let pairs = pair_store_value(site.pairing, site.table, column, &source, target)?;
    match cast {
        Some(_) if !unwrap_cast => {
            if let Ok(written) = inner.get_type(schema)
                && let Ok(judged) =
                    pair_store_value(site.pairing, site.table, column, &written, target)
            {
                refuse_unassignable_leaves(site.table, &judged)?;
            }
        }
        _ => refuse_unassignable_leaves(site.table, &pairs)?,
    }
    if pairs.is_identity() {
        return Ok(None);
    }
    let kernel = store_pair_expr(value.clone(), target, site.pairing, site.table, column);
    Ok(Some(match cast {
        Some(cast) => {
            let mut cast = cast.clone();
            cast.expr = Box::new(kernel);
            Expr::Cast(cast)
        }
        None => kernel,
    }))
}

fn conform_one_store_expr(
    inner: &Expr,
    schema: &DFSchema,
    input: &mut Arc<LogicalPlan>,
    column: &str,
    target_type: &DataType,
    flags: ConformStoreFlags<'_>,
) -> Result<Option<Expr>> {
    if let Some(paired) = pair_store_output(
        inner,
        schema,
        column,
        target_type,
        flags.site,
        flags.unwrap_cast,
    )? {
        return Ok(Some(paired));
    }
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
    pairing: StructPairing,
    table: &str,
) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(plan);
    };
    if !by_name && projection.expr.len() != targets.len() {
        return Ok(LogicalPlan::Projection(projection));
    }
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
            site: PairSite { pairing, table },
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
mod tests;
