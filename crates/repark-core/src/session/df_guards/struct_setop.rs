use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, NullEquality, Result as DataFusionResult};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Join, JoinType, LogicalPlan, RecursiveQuery, Union,
};
use datafusion::optimizer::AnalyzerRule;

use super::struct_walk::{StructNameMismatch, first_struct_name_mismatch};

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
pub fn insert_struct_setop_after_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> DataFusionResult<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(DataFusionError::Internal(
            "the struct set-operation rule requires the default type_coercion analyzer rule"
                .to_string(),
        ));
    };
    rules.insert(position + 1, Arc::new(StructSetopNames));
    Ok(rules)
}

fn refuse_in_node(node: &LogicalPlan) -> DataFusionResult<()> {
    match node {
        LogicalPlan::Union(union) => refuse_in_union(union),
        LogicalPlan::Join(join) => refuse_in_join(join),
        LogicalPlan::RecursiveQuery(recursive) => refuse_in_recursive(recursive),
        _ => Ok(()),
    }
}

fn refuse_in_union(union: &Union) -> DataFusionResult<()> {
    for (input_index, input) in union.inputs.iter().enumerate() {
        for (position, target) in union.schema.fields().iter().enumerate() {
            let Some(source) = source_type_at(input, position) else {
                continue;
            };
            if let Some(mismatch) = first_struct_name_mismatch(target.data_type(), &source) {
                let table = table_ordinal(input_index);
                return Err(refusal(
                    "UNION",
                    position,
                    &table,
                    target.name(),
                    target.data_type(),
                    &source,
                    &mismatch,
                ));
            }
        }
    }
    Ok(())
}

fn source_type_at(input: &LogicalPlan, position: usize) -> Option<DataType> {
    if let LogicalPlan::Projection(projection) = input {
        let expr = projection.expr.get(position)?;
        beneath_casts(expr).get_type(projection.input.schema()).ok()
    } else {
        input
            .schema()
            .fields()
            .get(position)
            .map(|field| field.data_type().clone())
    }
}

fn refuse_in_recursive(recursive: &RecursiveQuery) -> DataFusionResult<()> {
    let operation = if recursive.is_distinct {
        "UNION"
    } else {
        "UNION ALL"
    };
    for (position, static_field) in recursive.static_term.schema().fields().iter().enumerate() {
        let Some(source) = source_type_at(&recursive.recursive_term, position) else {
            continue;
        };
        if let Some(mismatch) = first_struct_name_mismatch(static_field.data_type(), &source) {
            return Err(refusal(
                operation,
                position,
                "recursive term",
                static_field.name(),
                static_field.data_type(),
                &source,
                &mismatch,
            ));
        }
    }
    Ok(())
}

fn refuse_in_join(join: &Join) -> DataFusionResult<()> {
    let distinct = matches!(join.left.as_ref(), LogicalPlan::Distinct(_));
    let operation = match (join.join_type, distinct) {
        (JoinType::LeftSemi, true) => "INTERSECT",
        (JoinType::LeftSemi, false) => "INTERSECT ALL",
        (JoinType::LeftAnti, true) => "EXCEPT",
        (JoinType::LeftAnti, false) => "EXCEPT ALL",
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
        if !is_key_column(left_expr, left_field.name())
            || !is_key_column(right_expr, right_field.name())
        {
            return Ok(());
        }
    }
    for (position, (left_expr, right_expr)) in join.on.iter().enumerate() {
        let (Ok(left), Ok(right)) = (
            beneath_casts(left_expr).get_type(join.left.schema()),
            beneath_casts(right_expr).get_type(join.right.schema()),
        ) else {
            continue;
        };
        if let Some(mismatch) = first_struct_name_mismatch(&left, &right) {
            let Some(column) = left_fields.get(position) else {
                continue;
            };
            let table = table_ordinal(1);
            return Err(refusal(
                operation,
                position,
                &table,
                column.name(),
                &left,
                &right,
                &mismatch,
            ));
        }
    }
    Ok(())
}

fn beneath_casts(expr: &Expr) -> &Expr {
    let mut current = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    loop {
        if let Expr::Cast(cast) = current {
            current = cast.expr.as_ref();
        } else if let Expr::TryCast(cast) = current {
            current = cast.expr.as_ref();
        } else {
            return current;
        }
    }
}

fn is_key_column(expr: &Expr, name: &str) -> bool {
    matches!(beneath_casts(expr), Expr::Column(column) if column.name == name)
}

fn table_ordinal(input_index: usize) -> String {
    format!("{} table", ordinal(input_index + 1))
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
    table: &str,
    column: &str,
    left: &DataType,
    right: &DataType,
    mismatch: &StructNameMismatch,
) -> DataFusionError {
    let detail = match mismatch {
        StructNameMismatch::Names {
            path,
            left_names,
            right_names,
        } => format!(
            "Struct field names differ at {column}{path}: [{}] versus [{}]",
            right_names.join(", "),
            left_names.join(", ")
        ),
        StructNameMismatch::Unpaired {
            path,
            left: left_nested,
            right: right_nested,
        } => format!(
            "Struct field names differ at {column}{path}: [{}] versus [{}]",
            right_nested, left_nested
        ),
    };
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_COLUMN_TYPE] {operation} can only be performed on tables with \
         compatible column types. The {} column of the {} is \"{right}\" type which is \
         not compatible with \"{left}\" at the same column of the first table. {detail}; RePark \
         pairs struct fields by name in set operations, so it refuses field names that differ in \
         order or set.",
        ordinal(position + 1),
        table,
    ))
}
