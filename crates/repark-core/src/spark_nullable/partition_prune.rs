use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, Volatility};
use datafusion::physical_expr::PhysicalExpr;

use super::partition::partition_pruning_ansi_from_options;

const SPARK_NONNULL_NAME: &str = "__repark_spark_nonnull__";

type Bindings = HashMap<String, (ScalarValue, Expr)>;

enum Folded {
    Value(Option<bool>),
    Unlowered,
}

fn expr_uses_only_columns(col_names: &[&str], expr: &Expr) -> bool {
    let mut applicable = true;
    let _ = expr.apply(|node| match node {
        Expr::Column(Column { name, .. }) => {
            applicable &= col_names.contains(&name.as_str());
            if applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(_, _)
        | Expr::Alias(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::ScalarVariable(_, _)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::BinaryExpr(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::InList(_)
        | Expr::Case(_) => Ok(TreeNodeRecursion::Continue),
        Expr::ScalarFunction(func) => match func.func.signature().volatility {
            Volatility::Immutable => Ok(TreeNodeRecursion::Continue),
            Volatility::Stable | Volatility::Volatile => {
                applicable = false;
                Ok(TreeNodeRecursion::Stop)
            }
        },
        _ => {
            applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });
    applicable
}

fn strip_column_qualifiers(expr: Expr) -> Expr {
    expr.transform(|node| match node {
        Expr::Column(column) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
            column.name,
        )))),
        other => Ok(Transformed::no(other)),
    })
    .map_or_else(
        |_| Expr::Literal(ScalarValue::Boolean(Some(true)), None),
        |t| t.data,
    )
}

fn flatten_conjuncts(filters: &[Expr]) -> Vec<Expr> {
    let mut out = Vec::with_capacity(filters.len());
    let mut stack: Vec<Expr> = filters.iter().rev().cloned().collect();
    while let Some(expr) = stack.pop() {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                stack.push(*right);
                stack.push(*left);
            }
            other => out.push(other),
        }
    }
    out
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn without_nonnull_wrapper(expr: &Expr) -> &Expr {
    match expr {
        Expr::ScalarFunction(func) if func.func.name() == SPARK_NONNULL_NAME => {
            func.args.first().map_or(expr, |inner| inner)
        }
        other => other,
    }
}

fn binding_of(
    expr: &Expr,
    column_types: &HashMap<String, DataType>,
) -> Option<(String, ScalarValue, Expr)> {
    let comparison = without_nonnull_wrapper(expr);
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = comparison else {
        return None;
    };
    if !matches!(op, Operator::Eq | Operator::IsNotDistinctFrom) {
        return None;
    }
    let (column, value) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, _))
        | (Expr::Literal(value, _), Expr::Column(column)) => (column, value),
        _ => return None,
    };
    let data_type = column_types.get(&column.name)?;
    (&value.data_type() == data_type)
        .then(|| (column.name.clone(), value.clone(), comparison.clone()))
}

fn guards_a_binding(left: &Expr, op: Operator, right: &Expr, predicates: &[&Expr]) -> bool {
    let equality = match op {
        Operator::Eq | Operator::NotEq => Operator::Eq,
        Operator::IsNotDistinctFrom | Operator::IsDistinctFrom => Operator::IsNotDistinctFrom,
        _ => return false,
    };
    let candidate = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left.clone()),
        equality,
        Box::new(right.clone()),
    ));
    predicates.contains(&&candidate)
}

fn substitute(expr: Expr, bindings: &Bindings) -> Expr {
    let predicates: Vec<&Expr> = bindings.values().map(|(_, predicate)| predicate).collect();
    expr.clone()
        .transform_down(|node| {
            let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &node else {
                return Ok(Transformed::no(node));
            };
            if !is_comparison(*op) || guards_a_binding(left, *op, right, &predicates) {
                return Ok(Transformed::no(node));
            }
            let replaced = node.transform(|inner| match inner {
                Expr::Column(column) => match bindings.get(&column.name) {
                    Some((value, _)) => Ok(Transformed::yes(Expr::Literal(value.clone(), None))),
                    None => Ok(Transformed::no(Expr::Column(column))),
                },
                other => Ok(Transformed::no(other)),
            })?;
            Ok(Transformed::new(
                replaced.data,
                replaced.transformed,
                TreeNodeRecursion::Jump,
            ))
        })
        .map_or(expr, |t| t.data)
}

fn propagate(
    expr: &Expr,
    replace_children: bool,
    null_is_false: bool,
    column_types: &HashMap<String, DataType>,
) -> (Option<Expr>, Bindings) {
    if null_is_false && let Some((name, value, predicate)) = binding_of(expr, column_types) {
        return (None, HashMap::from([(name, (value, predicate))]));
    }
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let (new_left, mut bindings) = propagate(left, false, null_is_false, column_types);
            let (new_right, right_bindings) = propagate(right, false, null_is_false, column_types);
            bindings.extend(right_bindings);
            let changed = new_left.is_some() || new_right.is_some();
            let left_expr = new_left.unwrap_or_else(|| left.as_ref().clone());
            let right_expr = new_right.unwrap_or_else(|| right.as_ref().clone());
            if replace_children && !bindings.is_empty() {
                let rebuilt =
                    substitute(left_expr, &bindings).and(substitute(right_expr, &bindings));
                (Some(rebuilt), bindings)
            } else if changed {
                (Some(left_expr.and(right_expr)), bindings)
            } else {
                (None, bindings)
            }
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let (new_left, _) = propagate(left, true, null_is_false, column_types);
            let (new_right, _) = propagate(right, true, null_is_false, column_types);
            if new_left.is_none() && new_right.is_none() {
                return (None, Bindings::new());
            }
            let rebuilt = new_left
                .unwrap_or_else(|| left.as_ref().clone())
                .or(new_right.unwrap_or_else(|| right.as_ref().clone()));
            (Some(rebuilt), Bindings::new())
        }
        Expr::Not(child) => {
            let (new_child, _) = propagate(child, true, false, column_types);
            (
                new_child.map(|inner| Expr::Not(Box::new(inner))),
                Bindings::new(),
            )
        }
        _ => (None, Bindings::new()),
    }
}

fn propagate_constants(
    conjuncts: Vec<Expr>,
    column_types: &HashMap<String, DataType>,
) -> Vec<Expr> {
    let mut bindings = Bindings::new();
    let mut rewritten = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        let (new_expr, found) = propagate(&conjunct, false, true, column_types);
        bindings.extend(found);
        rewritten.push(new_expr.unwrap_or(conjunct));
    }
    if rewritten.len() < 2 || bindings.is_empty() {
        return rewritten;
    }
    rewritten
        .into_iter()
        .map(|conjunct| substitute(conjunct, &bindings))
        .collect()
}

fn evaluate_constant(state: &dyn Session, expr: Expr) -> DataFusionResult<Folded> {
    let Ok(physical) = state.create_physical_expr(expr, &DFSchema::empty()) else {
        return Ok(Folded::Unlowered);
    };
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    let array = physical.evaluate(&batch)?.into_array(1)?;
    match array.as_any().downcast_ref::<BooleanArray>() {
        Some(bools) if bools.len() == 1 => {
            Ok(Folded::Value(bools.is_valid(0).then(|| bools.value(0))))
        }
        _ => Ok(Folded::Unlowered),
    }
}

fn fold_constant_conjuncts(
    state: &dyn Session,
    rewritten: Vec<Expr>,
    part_names: &[&str],
    ansi_enabled: bool,
) -> DataFusionResult<Option<Vec<Expr>>> {
    let mut leading = true;
    let mut leading_false = false;
    let mut always_false = false;
    let mut part_filters = Vec::with_capacity(rewritten.len());
    for conjunct in rewritten {
        if conjunct.column_refs().is_empty() && expr_uses_only_columns(&[], &conjunct) {
            if leading && leading_false {
                continue;
            }
            match evaluate_constant(state, conjunct) {
                Ok(Folded::Value(Some(true)) | Folded::Unlowered) => {}
                Ok(Folded::Value(Some(false))) => {
                    always_false = true;
                    leading_false |= leading;
                }
                Ok(Folded::Value(None)) => always_false = true,
                Err(error) => {
                    if ansi_enabled {
                        return Err(error);
                    }
                }
            }
            continue;
        }
        leading = false;
        if expr_uses_only_columns(part_names, &conjunct) {
            part_filters.push(conjunct);
        }
    }
    Ok((!always_false).then_some(part_filters))
}

pub(super) fn prune_partitioned_files(
    state: &dyn Session,
    files: &[PartitionedFile],
    partition_fields: &[FieldRef],
    table_schema: &SchemaRef,
    filters: &[Expr],
) -> DataFusionResult<Vec<PartitionedFile>> {
    if partition_fields.is_empty() || filters.is_empty() {
        return Ok(files.to_vec());
    }
    let part_names: Vec<&str> = partition_fields
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let column_types: HashMap<String, DataType> = table_schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect();
    let conjuncts: Vec<Expr> = flatten_conjuncts(filters)
        .into_iter()
        .map(strip_column_qualifiers)
        .collect();
    let ansi_enabled = partition_pruning_ansi_from_options(state.config_options());
    let rewritten = propagate_constants(conjuncts, &column_types);
    let Some(part_filters) = fold_constant_conjuncts(state, rewritten, &part_names, ansi_enabled)?
    else {
        return Ok(Vec::new());
    };
    if part_filters.is_empty() {
        return Ok(files.to_vec());
    }
    let Ok(part_df_schema) = DFSchema::from_unqualified_fields(
        partition_fields.iter().cloned().collect(),
        HashMap::new(),
    ) else {
        return Ok(files.to_vec());
    };
    let mut physicals: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(part_filters.len());
    for filter in part_filters {
        if let Ok(physical) = state.create_physical_expr(filter, &part_df_schema) {
            physicals.push(physical);
        }
    }
    if physicals.is_empty() {
        return Ok(files.to_vec());
    }
    let part_arrow_schema = Arc::clone(part_df_schema.inner());
    let mut kept = Vec::with_capacity(files.len());
    for file in files {
        let arrays: DataFusionResult<Vec<ArrayRef>> = file
            .partition_values
            .iter()
            .map(|value| value.to_array_of_size(1))
            .collect();
        let Ok(arrays) = arrays else {
            kept.push(file.clone());
            continue;
        };
        let Ok(batch) = RecordBatch::try_new(Arc::clone(&part_arrow_schema), arrays) else {
            kept.push(file.clone());
            continue;
        };
        let mut keep_file = true;
        for physical in &physicals {
            let evaluated = physical
                .evaluate(&batch)
                .and_then(|value| value.into_array(1));
            match evaluated {
                Ok(array) => {
                    if let Some(bools) = array.as_any().downcast_ref::<BooleanArray>()
                        && bools.len() == 1
                        && (!bools.is_valid(0) || !bools.value(0))
                    {
                        keep_file = false;
                        break;
                    }
                }
                Err(error) => {
                    if ansi_enabled {
                        return Err(error);
                    }
                    break;
                }
            }
        }
        if keep_file {
            kept.push(file.clone());
        }
    }
    Ok(kept)
}
