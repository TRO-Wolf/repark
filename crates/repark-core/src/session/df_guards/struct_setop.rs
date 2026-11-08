use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, NullEquality, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, ExprSchemable, Join, JoinType, LogicalPlan, Union};
use datafusion::optimizer::AnalyzerRule;

pub(crate) struct StructNameMismatch {
    pub(crate) path: String,
    pub(crate) left_names: Vec<String>,
    pub(crate) right_names: Vec<String>,
}

pub(crate) fn first_struct_name_mismatch(
    left: &DataType,
    right: &DataType,
) -> Option<StructNameMismatch> {
    mismatch_at(left, right, String::new())
}

fn mismatch_at(left: &DataType, right: &DataType, path: String) -> Option<StructNameMismatch> {
    match (left, right) {
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            let left_names: Vec<&str> = left_fields
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            let right_names: Vec<&str> = right_fields
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            if left_names != right_names {
                return Some(StructNameMismatch {
                    path,
                    left_names: left_names.iter().map(ToString::to_string).collect(),
                    right_names: right_names.iter().map(ToString::to_string).collect(),
                });
            }
            for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
                let mut child = path.clone();
                child.push('.');
                child.push_str(left_field.name());
                if let Some(found) =
                    mismatch_at(left_field.data_type(), right_field.data_type(), child)
                {
                    return Some(found);
                }
            }
            None
        }
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::FixedSizeList(left, _), DataType::FixedSizeList(right, _))
        | (DataType::ListView(left), DataType::ListView(right)) => {
            mismatch_at(left.data_type(), right.data_type(), format!("{path}[]"))
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            let (Some((left_key, left_value)), Some((right_key, right_value))) = (
                map_key_and_value(left_entries.data_type()),
                map_key_and_value(right_entries.data_type()),
            ) else {
                return None;
            };
            mismatch_at(left_key, right_key, format!("{path}{{key}}"))
                .or_else(|| mismatch_at(left_value, right_value, format!("{path}{{value}}")))
        }
        (DataType::Dictionary(_, left_value), DataType::Dictionary(_, right_value)) => {
            mismatch_at(left_value, right_value, path)
        }
        _ => None,
    }
}

fn map_key_and_value(entries: &DataType) -> Option<(&DataType, &DataType)> {
    if let DataType::Struct(fields) = entries
        && let [key, value] = &fields[..]
    {
        return Some((key.data_type(), value.data_type()));
    }
    None
}

#[derive(Debug, Default)]
pub struct StructSetopNames;

impl AnalyzerRule for StructSetopNames {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DataFusionResult<LogicalPlan> {
        plan.apply_with_subqueries(|node| {
            refuse_in_node(node)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "struct_setop_names"
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn insert_struct_setop_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> DataFusionResult<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(DataFusionError::Internal(
            "the struct set-operation rule requires the default type_coercion analyzer rule"
                .to_string(),
        ));
    };
    rules.insert(position, Arc::new(StructSetopNames));
    Ok(rules)
}

fn refuse_in_node(node: &LogicalPlan) -> DataFusionResult<()> {
    match node {
        LogicalPlan::Union(union) => refuse_in_union(union),
        LogicalPlan::Join(join) => refuse_in_join(join),
        _ => Ok(()),
    }
}

fn refuse_in_union(union: &Union) -> DataFusionResult<()> {
    let width = union.schema.fields().len();
    let Some(first) = union.inputs.first() else {
        return Ok(());
    };
    for (input_index, input) in union.inputs.iter().enumerate().skip(1) {
        for position in 0..width {
            let (Some(left), Some(right)) = (
                union_input_type(first, position),
                union_input_type(input, position),
            ) else {
                continue;
            };
            if let Some(mismatch) = first_struct_name_mismatch(&left, &right) {
                let Some(column) = union.schema.fields().get(position) else {
                    continue;
                };
                return Err(refusal(
                    "UNION",
                    position,
                    input_index,
                    column.name(),
                    &left,
                    &right,
                    &mismatch,
                ));
            }
        }
    }
    Ok(())
}

fn union_input_type(input: &LogicalPlan, position: usize) -> Option<DataType> {
    if let LogicalPlan::Projection(projection) = input {
        let expr = projection.expr.get(position)?;
        expr.get_type(projection.input.schema()).ok()
    } else {
        input
            .schema()
            .fields()
            .get(position)
            .map(|field| field.data_type().clone())
    }
}

fn refuse_in_join(join: &Join) -> DataFusionResult<()> {
    let operation = match join.join_type {
        JoinType::LeftSemi => "INTERSECT",
        JoinType::LeftAnti => "EXCEPT",
        _ => return Ok(()),
    };
    if join.null_equality != NullEquality::NullEqualsNull || join.filter.is_some() {
        return Ok(());
    }
    let left_fields = join.left.schema().fields();
    let right_fields = join.right.schema().fields();
    if join.on.len() != left_fields.len() || join.on.len() != right_fields.len() {
        return Ok(());
    }
    for (position, (left_expr, right_expr)) in join.on.iter().enumerate() {
        let (Some(left_field), Some(right_field)) =
            (left_fields.get(position), right_fields.get(position))
        else {
            return Ok(());
        };
        if !is_bare_column(left_expr, left_field.name())
            || !is_bare_column(right_expr, right_field.name())
        {
            return Ok(());
        }
    }
    for (position, (left_expr, right_expr)) in join.on.iter().enumerate() {
        let (Ok(left), Ok(right)) = (
            left_expr.get_type(join.left.schema()),
            right_expr.get_type(join.right.schema()),
        ) else {
            continue;
        };
        if let Some(mismatch) = first_struct_name_mismatch(&left, &right) {
            let Some(column) = left_fields.get(position) else {
                continue;
            };
            return Err(refusal(
                operation,
                position,
                1,
                column.name(),
                &left,
                &right,
                &mismatch,
            ));
        }
    }
    Ok(())
}

fn is_bare_column(expr: &Expr, name: &str) -> bool {
    matches!(expr, Expr::Column(column) if column.name == name)
}

fn ordinal(one_based: usize) -> String {
    match one_based {
        1 => "first",
        2 => "second",
        3 => "third",
        4 => "fourth",
        5 => "fifth",
        6 => "sixth",
        7 => "seventh",
        8 => "eighth",
        9 => "ninth",
        10 => "tenth",
        _ => return format!("{one_based}th"),
    }
    .to_string()
}

#[allow(clippy::too_many_arguments)]
fn refusal(
    operation: &str,
    position: usize,
    input_index: usize,
    column: &str,
    left: &DataType,
    right: &DataType,
    mismatch: &StructNameMismatch,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_COLUMN_TYPE] {operation} can only be performed on tables with \
         compatible column types. The {} column of the {} table is \"{right}\" type which is \
         not compatible with \"{left}\" at the same column of the first table. Struct field \
         names differ at {}{}: [{}] versus [{}]; RePark pairs struct fields by name in set \
         operations, so it refuses field names that differ in order or set.",
        ordinal(position + 1),
        ordinal(input_index + 1),
        column,
        mismatch.path,
        mismatch.right_names.join(", "),
        mismatch.left_names.join(", "),
    ))
}
