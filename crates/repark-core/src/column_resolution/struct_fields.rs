use datafusion::arrow::datatypes::DataType;
use datafusion::common::Column;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;

use super::WrittenRefs;

pub(super) fn refuse_ambiguous_struct_fields(
    plan: &LogicalPlan,
    written: &WrittenRefs,
) -> Result<()> {
    if written.qualified.is_empty() && written.outer_qualified.is_empty() {
        return Ok(());
    }
    plan.apply_with_subqueries(|node| {
        check_node(node, written)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn check_node(node: &LogicalPlan, written: &WrittenRefs) -> Result<()> {
    let inputs = node.inputs();
    if inputs.is_empty() {
        return Ok(());
    }
    for expr in node.expressions() {
        expr.apply(|leaf| {
            if let Expr::ScalarFunction(func) = leaf
                && func.func.name() == "get_field"
                && let [base, Expr::Literal(ScalarValue::Utf8(Some(key)), _)] = func.args.as_slice()
            {
                check_struct_access(&inputs, base, key, written)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(())
}

fn check_struct_access(
    inputs: &[&LogicalPlan],
    base: &Expr,
    key: &str,
    written: &WrittenRefs,
) -> Result<()> {
    let Some(DataType::Struct(fields)) = struct_type_of(inputs, base) else {
        return Ok(());
    };
    let matches = fields
        .iter()
        .filter(|field| field.name().eq_ignore_ascii_case(key))
        .count();
    if matches < 2 {
        return Ok(());
    }
    let parent = match base {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::ScalarFunction(func) if func.func.name() == "get_field" => {
            match func.args.as_slice() {
                [_, Expr::Literal(ScalarValue::Utf8(Some(inner)), _)] => Some(inner.as_str()),
                _ => None,
            }
        }
        _ => None,
    };
    let Some(name) = written_field_spelling(written, parent, key) else {
        return Ok(());
    };
    Err(DataFusionError::Plan(format!(
        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field `{name}`. It appears \
         {matches} times in the schema. SQLSTATE: 42000"
    )))
}

fn written_field_spelling(
    written: &WrittenRefs,
    parent: Option<&str>,
    key: &str,
) -> Option<String> {
    let mut spellings: Vec<&String> = Vec::new();
    for (qualifier, leaf) in written
        .qualified
        .iter()
        .chain(written.outer_qualified.iter())
    {
        if !leaf.eq_ignore_ascii_case(key) {
            continue;
        }
        if let Some(parent) = parent
            && !qualifier.eq_ignore_ascii_case(parent)
        {
            continue;
        }
        spellings.push(leaf);
    }
    if spellings.is_empty() {
        return None;
    }
    if let Some(found) = spellings.iter().find(|leaf| leaf.as_str() == key) {
        return Some((*found).clone());
    }
    spellings.into_iter().min().cloned()
}

fn struct_type_of(inputs: &[&LogicalPlan], expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Column(column) => {
            column_type(inputs, column).filter(|data_type| matches!(data_type, DataType::Struct(_)))
        }
        Expr::ScalarFunction(func) if func.func.name() == "get_field" => {
            let [base, Expr::Literal(ScalarValue::Utf8(Some(key)), _)] = func.args.as_slice()
            else {
                return None;
            };
            let DataType::Struct(fields) = struct_type_of(inputs, base)? else {
                return None;
            };
            if let Some(field) = fields.iter().find(|field| field.name() == key) {
                return Some(field.data_type().clone());
            }
            let mut found = None;
            for field in &fields {
                if field.name().eq_ignore_ascii_case(key) {
                    if found.is_some() {
                        return None;
                    }
                    found = Some(field.data_type().clone());
                }
            }
            found
        }
        Expr::Alias(alias) => struct_type_of(inputs, &alias.expr),
        _ => None,
    }
}

fn column_type(inputs: &[&LogicalPlan], column: &Column) -> Option<DataType> {
    let mut found = None;
    for input in inputs {
        for (qualifier, field) in input.schema().iter() {
            if qualifier == column.relation.as_ref() && field.name() == &column.name {
                if found.is_some() {
                    return None;
                }
                found = Some(field.data_type().clone());
            }
        }
    }
    found
}
