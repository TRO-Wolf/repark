use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{Expr, Statement};
use iceberg::{NamespaceIdent, TableIdent};

use super::{
    AllowedDeleteIn, PredicateDmlSpec, delete_target_and_alias, expression_contains_subquery,
    object_name_parts, rewrite_target_refs_in_expr,
};

/// # Errors
/// A plan error when the target namespace is invalid.
pub fn try_allowed_plain_identity(statement: &Statement) -> Result<Option<AllowedDeleteIn>> {
    let Statement::Delete(delete) = statement else {
        return Ok(None);
    };
    if delete.using.is_some()
        || delete.returning.is_some()
        || delete.output.is_some()
        || delete.limit.is_some()
        || !delete.order_by.is_empty()
        || !delete.tables.is_empty()
    {
        return Ok(None);
    }
    let Some(selection) = delete.selection.as_ref() else {
        return Ok(None);
    };
    if !is_scalar_comparison(selection) {
        return Ok(None);
    }
    let Some((object_name, alias)) = delete_target_and_alias(delete) else {
        return Ok(None);
    };
    allowed_from_target(object_name, alias, selection)
}

fn is_scalar_comparison(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            !expression_contains_subquery(left) && !expression_contains_subquery(right)
        }
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => is_scalar_comparison(inner),
        _ => false,
    }
}

fn allowed_from_target(
    object_name: &datafusion::sql::sqlparser::ast::ObjectName,
    alias: Option<String>,
    selection: &Expr,
) -> Result<Option<AllowedDeleteIn>> {
    if object_name.0.len() != 3 {
        return Ok(None);
    }
    let parts = object_name_parts(object_name);
    if parts.len() != 3 {
        return Ok(None);
    }
    let catalog_name = parts[0].clone();
    let table_name = parts[parts.len() - 1].clone();
    let namespace = parts[1..parts.len() - 1].to_vec();
    let namespace = NamespaceIdent::from_vec(namespace).map_err(|error| {
        DataFusionError::Plan(format!(
            "DML target `{object_name}` has an invalid namespace: {error}"
        ))
    })?;
    let target_alias = alias.unwrap_or_else(|| table_name.clone());
    let mut scratch_selection = selection.clone();
    rewrite_target_refs_in_expr(&mut scratch_selection, &parts, &target_alias);
    Ok(Some(AllowedDeleteIn {
        catalog_name,
        spec: PredicateDmlSpec {
            target: TableIdent::new(namespace, table_name),
            target_alias,
            selection_sql: scratch_selection.to_string(),
            assignments: None,
        },
    }))
}
