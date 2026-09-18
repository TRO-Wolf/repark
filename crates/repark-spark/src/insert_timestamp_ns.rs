use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, ExprSchema, Result};
use datafusion::logical_expr::{
    DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, Values, WriteOp,
};
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, Expr as SqlExpr, SetExpr, Statement,
};
use repark_functions::timestamp_ns_cast::{
    is_temporal_source, timestamp_ns_cast_expr, timestamp_ns_target,
};

pub(crate) fn before_analysis(
    plan: LogicalPlan,
    timestamp_cells: &[(usize, usize)],
) -> Result<LogicalPlan> {
    let Some(targets) = ns_insert_targets(&plan) else {
        return Ok(plan);
    };
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(plan);
    };
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(LogicalPlan::Dml(dml));
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
            match conform_values_rows(values, &values_columns, timestamp_cells) {
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
    let Some(targets) = ns_insert_targets(&plan) else {
        return Ok(plan);
    };
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(plan);
    };
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(LogicalPlan::Dml(dml));
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

fn ns_insert_targets(plan: &LogicalPlan) -> Option<Vec<Option<bool>>> {
    let LogicalPlan::Dml(dml) = plan else {
        return None;
    };
    if !matches!(dml.op, WriteOp::Insert(_)) {
        return None;
    }
    let schema = dml.target.schema();
    let fields = schema.fields();
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return None;
    };
    if projection.expr.len() != fields.len()
        || !fields
            .iter()
            .any(|field| timestamp_ns_target(field.data_type()).is_some())
    {
        return None;
    }
    Some(
        fields
            .iter()
            .map(|field| timestamp_ns_target(field.data_type()))
            .collect(),
    )
}

pub(crate) fn timestamp_typed_values_cells(statement: &Statement) -> Vec<(usize, usize)> {
    let Statement::Insert(insert) = statement else {
        return Vec::new();
    };
    let Some(source) = insert.source.as_deref() else {
        return Vec::new();
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Vec::new();
    };
    let mut cells = Vec::new();
    for (row, parens) in values.rows.iter().enumerate() {
        for (column, cell) in parens.content.iter().enumerate() {
            if is_timestamp_typed(cell) {
                cells.push((row, column));
            }
        }
    }
    cells
}

fn is_timestamp_typed(cell: &SqlExpr) -> bool {
    match cell {
        SqlExpr::Nested(inner) => is_timestamp_typed(inner),
        SqlExpr::Cast { data_type, .. } => matches!(data_type, SqlDataType::Timestamp(_, _)),
        SqlExpr::TypedString(typed) => matches!(typed.data_type, SqlDataType::Timestamp(_, _)),
        _ => false,
    }
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

fn conform_values_rows(
    values: &Values,
    columns: &[(String, bool)],
    timestamp_cells: &[(usize, usize)],
) -> Option<Values> {
    let mut rewrites: Vec<(usize, usize, Expr)> = Vec::new();
    for (name, zoned) in columns {
        let Some(index) = values.schema.index_of_column_by_name(None, name) else {
            continue;
        };
        for (row_index, row) in values.values.iter().enumerate() {
            if timestamp_cells.binary_search(&(row_index, index)).is_ok() {
                continue;
            }
            if let Some(base) = peel_ns_casts(&row[index]) {
                rewrites.push((
                    row_index,
                    index,
                    timestamp_ns_cast_expr(base.clone(), *zoned),
                ));
            }
        }
    }
    if rewrites.is_empty() {
        return None;
    }
    let mut rows = values.values.clone();
    for (row, column, expr) in rewrites {
        rows[row][column] = expr;
    }
    Some(Values {
        schema: Arc::clone(&values.schema),
        values: rows,
    })
}
