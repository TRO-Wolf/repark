use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, ExprSchema, Result};
use datafusion::logical_expr::{
    DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, Values, WriteOp,
};
use repark_functions::timestamp_ns_cast::{
    is_temporal_source, timestamp_ns_cast_expr, timestamp_ns_target,
};

pub(crate) fn before_analysis(plan: LogicalPlan) -> Result<LogicalPlan> {
    let (dml, projection, targets) = match ns_insert_parts(plan) {
        NsInsert::Parts(dml, projection, targets) => (*dml, projection, targets),
        NsInsert::Untouched(untouched) => return Ok(*untouched),
    };
    let source_schema = Arc::clone(projection.input.schema());
    let mut exprs = Vec::with_capacity(projection.expr.len());
    let mut values_columns: Vec<(String, bool)> = Vec::new();
    let mut changed = false;
    for (expr, target) in projection.expr.iter().zip(&targets) {
        let Some(zoned) = *target else {
            exprs.push(expr.clone());
            continue;
        };
        let (inner, name) = split_alias(expr);
        if let Some(base) = peel_ns_casts(inner) {
            if is_string_column(base, &source_schema) {
                exprs.push(expr.clone());
                continue;
            }
            exprs.push(realias(timestamp_ns_cast_expr(base.clone(), zoned), name));
            changed = true;
            continue;
        }
        if let Expr::Column(column) = inner {
            values_columns.push((column.name.clone(), zoned));
        }
        exprs.push(expr.clone());
    }
    let input = match projection.input.as_ref() {
        LogicalPlan::Values(values) if !values_columns.is_empty() => {
            match conform_values_rows(values, &values_columns) {
                Some(rewritten) => {
                    changed = true;
                    Arc::new(LogicalPlan::Values(rewritten))
                }
                None => Arc::clone(&projection.input),
            }
        }
        _ => Arc::clone(&projection.input),
    };
    rebuild(dml, exprs, input, changed)
}

pub(crate) fn after_analysis(plan: LogicalPlan) -> Result<LogicalPlan> {
    let (dml, projection, targets) = match ns_insert_parts(plan) {
        NsInsert::Parts(dml, projection, targets) => (*dml, projection, targets),
        NsInsert::Untouched(untouched) => return Ok(*untouched),
    };
    let source_schema = Arc::clone(projection.input.schema());
    let mut exprs = Vec::with_capacity(projection.expr.len());
    let mut changed = false;
    for ((expr, target), field) in projection
        .expr
        .iter()
        .zip(&targets)
        .zip(dml.target.schema().fields())
    {
        let Some(zoned) = *target else {
            exprs.push(expr.clone());
            continue;
        };
        let Ok(found) = expr.get_type(source_schema.as_ref()) else {
            exprs.push(expr.clone());
            continue;
        };
        if found == *field.data_type() || !is_temporal_source(&found) {
            exprs.push(expr.clone());
            continue;
        }
        let (inner, name) = split_alias(expr);
        let name = name.unwrap_or_else(|| field.name().clone());
        exprs.push(realias(
            timestamp_ns_cast_expr(inner.clone(), zoned),
            Some(name),
        ));
        changed = true;
    }
    let input = Arc::clone(&projection.input);
    rebuild(dml, exprs, input, changed)
}

enum NsInsert {
    Parts(Box<DmlStatement>, Projection, Vec<Option<bool>>),
    Untouched(Box<LogicalPlan>),
}

fn ns_insert_parts(plan: LogicalPlan) -> NsInsert {
    let LogicalPlan::Dml(dml) = plan else {
        return NsInsert::Untouched(Box::new(plan));
    };
    if !matches!(dml.op, WriteOp::Insert(_)) {
        return NsInsert::Untouched(Box::new(LogicalPlan::Dml(dml)));
    }
    let targets: Vec<Option<bool>> = dml
        .target
        .schema()
        .fields()
        .iter()
        .map(|field| timestamp_ns_target(field.data_type()))
        .collect();
    let projection = match dml.input.as_ref() {
        LogicalPlan::Projection(projection)
            if projection.expr.len() == targets.len() && targets.iter().any(Option::is_some) =>
        {
            projection.clone()
        }
        _ => return NsInsert::Untouched(Box::new(LogicalPlan::Dml(dml))),
    };
    NsInsert::Parts(Box::new(dml), projection, targets)
}

fn rebuild(
    dml: DmlStatement,
    exprs: Vec<Expr>,
    input: Arc<LogicalPlan>,
    changed: bool,
) -> Result<LogicalPlan> {
    if !changed {
        return Ok(LogicalPlan::Dml(dml));
    }
    let conformed = LogicalPlan::Projection(Projection::try_new(exprs, input)?);
    Ok(LogicalPlan::Dml(DmlStatement {
        input: Arc::new(conformed),
        ..dml
    }))
}

fn split_alias(expr: &Expr) -> (&Expr, Option<String>) {
    match expr {
        Expr::Alias(alias) => (alias.expr.as_ref(), Some(alias.name.clone())),
        other => (other, None),
    }
}

fn realias(expr: Expr, name: Option<String>) -> Expr {
    match name {
        Some(name) => expr.alias(name),
        None => expr,
    }
}

fn peel_ns_casts(expr: &Expr) -> Option<&Expr> {
    let Expr::Cast(cast) = expr else {
        return None;
    };
    timestamp_ns_target(cast.field.data_type())?;
    let mut base = cast.expr.as_ref();
    while let Expr::Cast(inner) = base
        && timestamp_ns_target(inner.field.data_type()).is_some()
    {
        base = inner.expr.as_ref();
    }
    Some(base)
}

fn is_string_column(expr: &Expr, schema: &DFSchema) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };
    schema.field_from_column(column).is_ok_and(|field| {
        matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    })
}

fn conform_values_rows(values: &Values, columns: &[(String, bool)]) -> Option<Values> {
    let mut rows = values.values.clone();
    let mut changed = false;
    for (name, zoned) in columns {
        let Some(index) = values.schema.index_of_column_by_name(None, name) else {
            continue;
        };
        for row in &mut rows {
            if let Some(base) = peel_ns_casts(&row[index]) {
                row[index] = timestamp_ns_cast_expr(base.clone(), *zoned);
                changed = true;
            }
        }
    }
    changed.then(|| Values {
        schema: Arc::clone(&values.schema),
        values: rows,
    })
}
