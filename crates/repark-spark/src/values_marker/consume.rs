use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::type_coercion::binary::type_union_resolution;
use datafusion::logical_expr::{
    Cast, DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, Union, Values, WriteOp,
};
use datafusion::optimizer::AnalyzerRule;

use super::{VALUES_CELL_NAME, values_cell_argument};

const UNIT: &str = "DECIMAL-DOUBLE-UNION-COERCION-1";

#[derive(Debug, Default)]
pub struct SparkValuesTyping;

impl AnalyzerRule for SparkValuesTyping {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if !has_values(&plan)? {
            return Ok(plan);
        }
        let mut retyped = false;
        plan.transform_down_up_with_subqueries(type_down, |node| type_up(node, &mut retyped))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_values_typing"
    }
}

pub fn insert_values_typing_first(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules
        .iter()
        .position(|rule| rule.name() == "spark_timestamp_unit_widen")
    else {
        return Err(DataFusionError::Plan(
            "the VALUES typing rule requires the spark_timestamp_unit_widen analyzer rule"
                .to_string(),
        ));
    };
    rules.insert(position, Arc::new(SparkValuesTyping));
    Ok(rules)
}

fn has_values(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = matches!(node, LogicalPlan::Values(_));
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn type_down(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::RecursiveQuery(_) => {
            let stripped = plan.transform_up_with_subqueries(|node| {
                node.map_expressions(|expr| expr.transform_up(strip_marker))
            })?;
            Ok(Transformed::new(
                stripped.data,
                stripped.transformed,
                TreeNodeRecursion::Jump,
            ))
        }
        LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Insert(_)) => type_insert_source(dml),
        other => Ok(Transformed::no(other)),
    }
}

fn type_insert_source(dml: DmlStatement) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    };
    let LogicalPlan::Values(values) = projection.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    };
    if !values_marked(values) {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    }
    let typed = type_values(values.clone(), Placement::Target)?;
    let projection = Projection::try_new_with_schema(
        projection.expr.clone(),
        Arc::new(LogicalPlan::Values(typed)),
        Arc::clone(&projection.schema),
    )?;
    Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement {
        input: Arc::new(LogicalPlan::Projection(projection)),
        ..dml
    })))
}

fn type_up(plan: LogicalPlan, retyped: &mut bool) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Values(values) => {
            if !values_marked(&values) {
                refuse_unmarked_mix(&values)?;
                return Ok(Transformed::no(LogicalPlan::Values(values)));
            }
            let before = Arc::clone(&values.schema);
            let typed = type_values(values, Placement::Standalone)?;
            *retyped |= typed.schema != before;
            Ok(Transformed::yes(LogicalPlan::Values(typed)))
        }
        LogicalPlan::RecursiveQuery(_) => Ok(Transformed::no(plan)),
        other => {
            refuse_stray_marker(&other)?;
            if !*retyped {
                return Ok(Transformed::no(other));
            }
            if let LogicalPlan::Union(union) = other {
                return rebuild_stale_union(union);
            }
            other.recompute_schema().map(Transformed::yes)
        }
    }
}

fn rebuild_stale_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let merged = union.schema.fields();
    let stale = union.inputs.iter().any(|input| {
        let mine = input.schema().fields();
        mine.len() != merged.len()
            || mine
                .iter()
                .zip(merged.iter())
                .any(|(mine, merged)| mine.data_type() != merged.data_type())
    });
    if !stale {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    Ok(Transformed::yes(LogicalPlan::Union(
        Union::try_new_with_loose_types(union.inputs)?,
    )))
}

fn refuse_stray_marker(plan: &LogicalPlan) -> Result<()> {
    let mut stray = false;
    plan.apply_expressions(|expr| {
        stray = expr.exists(|node| Ok(values_cell_argument(node).is_some()))?;
        Ok(if stray {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    if stray {
        return Err(DataFusionError::Plan(format!(
            "{UNIT}: the VALUES cell marker {VALUES_CELL_NAME} appeared outside a VALUES cell; \
             a Spark-door seat marked a statement the VALUES typing rule cannot consume"
        )));
    }
    Ok(())
}

fn values_marked(values: &Values) -> bool {
    values.values.iter().flatten().any(|cell| {
        cell.exists(|node| Ok(values_cell_argument(node).is_some()))
            .unwrap_or(false)
    })
}

fn strip_marker(expr: Expr) -> Result<Transformed<Expr>> {
    match expr {
        Expr::ScalarFunction(function)
            if function.func.name() == VALUES_CELL_NAME && function.args.len() == 1 =>
        {
            let mut args = function.args;
            Ok(Transformed::yes(args.remove(0)))
        }
        other => Ok(Transformed::no(other)),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    Standalone,
    Target,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Class {
    Decimal,
    Float,
    Integer,
    Null,
    Other,
}

fn class_of(data_type: &DataType) -> Class {
    match data_type {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Class::Decimal,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Class::Float,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Class::Integer,
        DataType::Null => Class::Null,
        _ => Class::Other,
    }
}

fn unaliased(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unaliased(&alias.expr),
        other => other,
    }
}

fn marked_argument(cell: &Expr) -> Option<&Expr> {
    let mut found = None;
    let mut count = 0usize;
    unaliased(cell)
        .apply(|node| {
            Ok(match values_cell_argument(node) {
                Some(argument) => {
                    count += 1;
                    found = Some(argument);
                    TreeNodeRecursion::Jump
                }
                None => TreeNodeRecursion::Continue,
            })
        })
        .ok()?;
    if count == 1 { found } else { None }
}

fn column_placement(
    rows: &[Vec<Expr>],
    column: usize,
    planned: &DataType,
    empty: &DFSchema,
) -> Option<Placement> {
    let mut decimal = false;
    let mut float = false;
    let mut common: Option<DataType> = None;
    for row in rows {
        let argument = row.get(column).and_then(marked_argument)?;
        let data_type = argument.get_type(empty).ok()?;
        match class_of(&data_type) {
            Class::Decimal => decimal = true,
            Class::Float => float = true,
            Class::Integer => {}
            Class::Null => continue,
            Class::Other => return None,
        }
        common = Some(match common {
            None => data_type,
            Some(previous) => type_union_resolution(&[previous, data_type])?,
        });
    }
    if !(decimal && float) {
        return None;
    }
    Some(if common.as_ref() == Some(planned) {
        Placement::Standalone
    } else {
        Placement::Target
    })
}

fn realiased(cell: &Expr, typed: Expr) -> Expr {
    match cell {
        Expr::Alias(alias) => Expr::Alias(Alias::new(
            typed,
            alias.relation.clone(),
            alias.name.clone(),
        )),
        _ => typed,
    }
}

fn widened_cell(cell: &Expr, target: &DataType, placement: Placement, empty: &DFSchema) -> Expr {
    realiased(cell, widened_value(cell, target, placement, empty))
}

fn widened_value(cell: &Expr, target: &DataType, placement: Placement, empty: &DFSchema) -> Expr {
    let Some(argument) = marked_argument(cell) else {
        return unaliased(cell).clone();
    };
    let argument = argument.clone();
    let source = argument.get_type(empty).unwrap_or(DataType::Null);
    if source == DataType::Null {
        return Expr::Literal(
            ScalarValue::try_from(match placement {
                Placement::Standalone => &DataType::Float64,
                Placement::Target => target,
            })
            .unwrap_or(ScalarValue::Null),
            None,
        );
    }
    let double = if source == DataType::Float64 {
        argument
    } else {
        Expr::Cast(Cast::new(Box::new(argument), DataType::Float64))
    };
    match placement {
        Placement::Target if *target != DataType::Float64 => {
            Expr::Cast(Cast::new(Box::new(double), target.clone()))
        }
        _ => double,
    }
}

fn stripped_cell(cell: &Expr) -> Result<Expr> {
    let stripped = cell.clone().transform_up(strip_marker).data()?;
    if let Expr::Cast(cast) = unaliased(&stripped)
        && matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Null, _))
    {
        let typed = Expr::Literal(ScalarValue::try_from(cast.field.data_type())?, None);
        return Ok(realiased(&stripped, typed));
    }
    Ok(stripped)
}

fn type_values(values: Values, forced: Placement) -> Result<Values> {
    let empty = DFSchema::empty();
    let Values { schema, values } = values;
    let mut fields: Vec<_> = schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect();
    let mut rows = values;
    for (column, (_, field)) in fields.iter_mut().enumerate() {
        let target = field.data_type().clone();
        if let Some(found) = column_placement(&rows, column, &target, &empty) {
            let placement = if forced == Placement::Target {
                Placement::Target
            } else {
                found
            };
            for row in &mut rows {
                if let Some(cell) = row.get_mut(column) {
                    *cell = widened_cell(cell, &target, placement, &empty);
                }
            }
            if placement == Placement::Standalone {
                *field = Arc::new(field.as_ref().clone().with_data_type(DataType::Float64));
            }
            continue;
        }
        for row in &mut rows {
            if let Some(cell) = row.get_mut(column) {
                *cell = stripped_cell(cell)?;
            }
        }
        keep_user_casts_distinct(&mut rows, column, &empty);
    }
    let schema = DFSchema::new_with_metadata(fields, schema.metadata().clone())?;
    Ok(Values {
        schema: Arc::new(schema),
        values: rows,
    })
}

fn float_to_decimal_cast(cell: &Expr, empty: &DFSchema) -> Option<DataType> {
    let Expr::Cast(cast) = unaliased(cell) else {
        return None;
    };
    (class_of(cast.field.data_type()) == Class::Decimal
        && cast
            .expr
            .get_type(empty)
            .is_ok_and(|source| class_of(&source) == Class::Float))
    .then(|| cast.field.data_type().clone())
}

fn decimal_source(cell: &Expr, empty: &DFSchema) -> bool {
    let cell = unaliased(cell);
    if matches!(cell, Expr::Literal(value, _) if value.is_null()) {
        return false;
    }
    let source = match cell {
        Expr::Cast(cast) => cast.expr.as_ref(),
        other => other,
    };
    source
        .get_type(empty)
        .is_ok_and(|data_type| class_of(&data_type) == Class::Decimal)
}

fn planner_mix(rows: &[Vec<Expr>], column: usize, empty: &DFSchema) -> bool {
    let cells = || rows.iter().filter_map(|row| row.get(column));
    cells().any(|cell| float_to_decimal_cast(cell, empty).is_some())
        && cells().any(|cell| decimal_source(cell, empty))
}

fn keep_user_casts_distinct(rows: &mut [Vec<Expr>], column: usize, empty: &DFSchema) {
    if rows.len() < 2 || !planner_mix(rows, column, empty) {
        return;
    }
    for row in rows.iter_mut() {
        if let Some(cell) = row.get_mut(column)
            && let Some(target) = float_to_decimal_cast(cell, empty)
        {
            let inner = unaliased(cell).clone();
            *cell = realiased(cell, Expr::Cast(Cast::new(Box::new(inner), target)));
        }
    }
}

fn refuse_unmarked_mix(values: &Values) -> Result<()> {
    if values.values.len() < 2 {
        return Ok(());
    }
    let empty = DFSchema::empty();
    for (column, field) in values.schema.fields().iter().enumerate() {
        if class_of(field.data_type()) == Class::Decimal
            && planner_mix(&values.values, column, &empty)
        {
            return Err(DataFusionError::Plan(format!(
                "{UNIT}: a multi-row VALUES mixing a DECIMAL and a FLOAT or DOUBLE in column {} \
                 reached the Spark-door analyzer without its cell markers, so Spark's double \
                 type cannot be restored; the statement is refused instead of answering in the \
                 planner's {}",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}
