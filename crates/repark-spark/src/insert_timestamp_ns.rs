use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ExprSchema, Result};
use datafusion::logical_expr::{
    Cast, DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, Values, WriteOp,
};
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, Expr as SqlExpr, SetExpr, Statement,
};
use repark_functions::timestamp_ns_cast::{
    is_temporal_source, timestamp_ns_cast_expr, timestamp_ns_conform_expr, timestamp_ns_target,
};
use repark_iceberg::write::ntz_store::holds_nested_ns_wall;

#[derive(Clone, Copy)]
enum Store {
    Leaf(bool),
    Nested,
}

pub(crate) fn before_analysis(
    plan: LogicalPlan,
    timestamp_cells: &[(usize, usize)],
) -> Result<LogicalPlan> {
    let Some(targets) = ns_store_targets(&plan) else {
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
        let Some(Store::Leaf(zoned)) = *target else {
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
    let Some(targets) = ns_store_targets(&plan) else {
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
    let mut nested_columns: Vec<&str> = Vec::new();
    let mut changed = false;
    for ((expr, target), field) in projection
        .expr
        .iter()
        .zip(&targets)
        .zip(dml.target.schema().fields())
    {
        let zoned = match *target {
            Some(Store::Leaf(zoned)) => zoned,
            Some(Store::Nested) => {
                let conformed = conform_nested(expr, field.data_type(), source_schema.as_ref())?;
                if let (Expr::Column(column), false) = (split_alias(expr).0, conformed.transformed)
                {
                    nested_columns.push(column.name.as_str());
                }
                changed |= conformed.transformed;
                exprs.push(conformed.data);
                continue;
            }
            None => {
                exprs.push(expr.clone());
                continue;
            }
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
    let input = match projection.input.as_ref() {
        LogicalPlan::Values(values) if !nested_columns.is_empty() => {
            match conform_nested_values(values, &nested_columns)? {
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

fn conform_nested_values(values: &Values, columns: &[&str]) -> Result<Option<Values>> {
    let empty = DFSchema::empty();
    let mut rows = values.values.clone();
    let mut changed = false;
    for name in columns {
        let Some(index) = values.schema.index_of_column_by_name(None, name) else {
            continue;
        };
        for row in &mut rows {
            let conformed = conform_nested_casts(&row[index], &empty)?;
            changed |= conformed.transformed;
            row[index] = conformed.data;
        }
    }
    Ok(changed.then(|| Values {
        schema: Arc::clone(&values.schema),
        values: rows,
    }))
}

fn conform_nested_casts(expr: &Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    expr.clone().transform_down(|node| {
        let Expr::Cast(cast) = &node else {
            return Ok(Transformed::no(node));
        };
        let shape = cast.field.data_type();
        if !holds_nested_ns_wall(shape) {
            return Ok(Transformed::no(node));
        }
        if cast
            .expr
            .get_type(schema)
            .is_ok_and(|found| found == *shape)
        {
            return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
        }
        let inner = timestamp_ns_conform_expr(cast.expr.as_ref().clone(), shape)?;
        let outer = Cast::new_from_field(Box::new(inner), Arc::clone(&cast.field));
        Ok(Transformed::new(
            Expr::Cast(outer),
            true,
            TreeNodeRecursion::Jump,
        ))
    })
}

fn conform_nested(expr: &Expr, target: &DataType, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let conformed = conform_nested_casts(expr, schema)?;
    if conformed.transformed || !expr.get_type(schema).is_ok_and(|found| found != *target) {
        return Ok(conformed);
    }
    let (inner, name) = split_alias(expr);
    let name = name.unwrap_or_else(|| expr.schema_name().to_string());
    let whole = timestamp_ns_conform_expr(inner.clone(), target)?;
    Ok(Transformed::yes(realias(whole, Some(name))))
}

fn ns_store_targets(plan: &LogicalPlan) -> Option<Vec<Option<Store>>> {
    let LogicalPlan::Dml(dml) = plan else {
        return None;
    };
    let wall_only = match dml.op {
        WriteOp::Insert(_) => false,
        WriteOp::Update => true,
        _ => return None,
    };
    let schema = dml.target.schema();
    let fields = schema.fields();
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return None;
    };
    if projection.expr.len() != fields.len() {
        return None;
    }
    let targets: Vec<Option<Store>> = fields
        .iter()
        .map(|field| {
            let leaf = timestamp_ns_target(field.data_type())
                .filter(|zoned| !(wall_only && *zoned))
                .map(Store::Leaf);
            let nested =
                (!wall_only && holds_nested_ns_wall(field.data_type())).then_some(Store::Nested);
            leaf.or(nested)
        })
        .collect();
    targets.iter().any(Option::is_some).then_some(targets)
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
