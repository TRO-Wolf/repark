use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, Statement, Value, ValueWithSpan};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::write::commit_error::{commit_result, operation_id_and_summary};
use crate::write::conflict_filter::{literal_datum, top_level_field};
use crate::write::predicate_dml::{
    delete_target_and_alias, object_name_parts, rewrite_target_refs_in_expr,
};

const MAX_PREDICATE_DEPTH: usize = 64;

#[derive(Debug, Clone)]
pub struct MetaDeleteTarget {
    pub catalog_name: String,
    pub target: TableIdent,
    pub target_alias: String,
    pub selection_sql: Option<String>,
}

pub struct MetaDeletePlan {
    table: Table,
    predicate: Predicate,
    case_sensitive: bool,
}

struct ColumnScope<'a> {
    schema: &'a Schema,
    target_alias: &'a str,
    case_insensitive: bool,
}

#[allow(clippy::missing_errors_doc)]
pub fn try_meta_delete_target(statement: &Statement) -> Result<Option<MetaDeleteTarget>> {
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
    let Some((object_name, alias)) = delete_target_and_alias(delete) else {
        return Ok(None);
    };
    if object_name.0.len() != 3 {
        return Ok(None);
    }
    let parts = object_name_parts(object_name);
    if parts.len() != 3 {
        return Ok(None);
    }
    let catalog_name = parts[0].clone();
    let table_name = parts[2].clone();
    let namespace = NamespaceIdent::from_vec(vec![parts[1].clone()]).map_err(|error| {
        DataFusionError::Plan(format!(
            "DELETE target `{object_name}` has an invalid namespace: {error}"
        ))
    })?;
    let target_alias = alias.unwrap_or_else(|| table_name.clone());
    let selection_sql = delete.selection.as_ref().map(|selection| {
        let mut scoped = selection.clone();
        rewrite_target_refs_in_expr(&mut scoped, &parts, &target_alias);
        scoped.to_string()
    });
    Ok(Some(MetaDeleteTarget {
        catalog_name,
        target: TableIdent::new(namespace, table_name),
        target_alias,
        selection_sql,
    }))
}

#[allow(clippy::missing_errors_doc)]
pub async fn try_metadata_delete(
    catalog: &Arc<dyn Catalog>,
    target: &MetaDeleteTarget,
    case_insensitive: bool,
) -> Result<bool> {
    let Some(plan) = plan_metadata_delete(catalog, target, case_insensitive).await? else {
        return Ok(false);
    };
    commit_metadata_delete(catalog, plan).await?;
    Ok(true)
}

#[allow(clippy::missing_errors_doc)]
pub async fn plan_metadata_delete(
    catalog: &Arc<dyn Catalog>,
    target: &MetaDeleteTarget,
    case_insensitive: bool,
) -> Result<Option<MetaDeletePlan>> {
    let Ok(table) = catalog.load_table(&target.target).await else {
        return Ok(None);
    };
    let Some(predicate) =
        delete_predicate(target, table.metadata().current_schema(), case_insensitive)
    else {
        return Ok(None);
    };
    let case_sensitive = !case_insensitive;
    if !table
        .can_delete_using_metadata(&predicate, None, case_sensitive)
        .await
        .map_err(iceberg_err)?
    {
        return Ok(None);
    }
    Ok(Some(MetaDeletePlan {
        table,
        predicate,
        case_sensitive,
    }))
}

#[must_use]
pub fn delete_predicate(
    target: &MetaDeleteTarget,
    schema: &Schema,
    case_insensitive: bool,
) -> Option<Predicate> {
    let Some(selection_sql) = target.selection_sql.as_deref() else {
        return Some(Predicate::AlwaysTrue);
    };
    let scope = ColumnScope {
        schema,
        target_alias: &target.target_alias,
        case_insensitive,
    };
    exact_predicate(&parse_selection(selection_sql)?, &scope, 0)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_metadata_delete(
    catalog: &Arc<dyn Catalog>,
    plan: MetaDeletePlan,
) -> Result<()> {
    let (operation_id, summary) = operation_id_and_summary();
    let transaction = Transaction::new(&plan.table);
    let action = transaction
        .delete_files()
        .delete_from_row_filter(plan.predicate)
        .case_sensitive(plan.case_sensitive)
        .set_snapshot_properties(summary);
    let transaction = action.apply(transaction).map_err(iceberg_err)?;
    commit_result(transaction.commit(catalog.as_ref()).await, &operation_id).map(|_| ())
}

fn parse_selection(selection_sql: &str) -> Option<Expr> {
    let mut parser = Parser::new(&GenericDialect {})
        .try_with_sql(selection_sql)
        .ok()?;
    let expr = parser.parse_expr().ok()?;
    matches!(parser.peek_token().token, Token::EOF).then_some(expr)
}

fn exact_predicate(expr: &Expr, scope: &ColumnScope<'_>, depth: usize) -> Option<Predicate> {
    if depth >= MAX_PREDICATE_DEPTH {
        return None;
    }
    match expr {
        Expr::Nested(inner) => exact_predicate(inner, scope, depth + 1),
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(true),
            ..
        }) => Some(Predicate::AlwaysTrue),
        Expr::IsNull(inner) => Some(Reference::new(column_name(inner, scope)?).is_null()),
        Expr::IsNotNull(inner) => Some(Reference::new(column_name(inner, scope)?).is_not_null()),
        Expr::InList {
            expr: inner,
            list,
            negated: false,
        } => in_list_predicate(inner, list, scope),
        Expr::Like {
            negated: false,
            any: false,
            expr: inner,
            pattern,
            escape_char: None,
        } => starts_with_predicate(inner, pattern, scope),
        Expr::BinaryOp { left, op, right } => binary_predicate(left, op, right, scope, depth),
        _ => None,
    }
}

fn binary_predicate(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    scope: &ColumnScope<'_>,
    depth: usize,
) -> Option<Predicate> {
    match op {
        BinaryOperator::And => Some(
            exact_predicate(left, scope, depth + 1)?.and(exact_predicate(right, scope, depth + 1)?),
        ),
        BinaryOperator::Or => Some(exact_predicate(left, scope, depth + 1)?.or(exact_predicate(
            right,
            scope,
            depth + 1,
        )?)),
        BinaryOperator::Eq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => comparison_predicate(left, op, right, scope),
        _ => None,
    }
}

fn comparison_predicate(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    scope: &ColumnScope<'_>,
) -> Option<Predicate> {
    if let Some(name) = column_name(left, scope) {
        let field_type = primitive_type(scope.schema, &name)?;
        let datum = ordered_literal(op, right, &field_type)?;
        return apply_comparison(name, op, datum, false);
    }
    let name = column_name(right, scope)?;
    let field_type = primitive_type(scope.schema, &name)?;
    let datum = ordered_literal(op, left, &field_type)?;
    apply_comparison(name, op, datum, true)
}

fn ordered_literal(op: &BinaryOperator, expr: &Expr, field_type: &PrimitiveType) -> Option<Datum> {
    let floating = matches!(field_type, PrimitiveType::Float | PrimitiveType::Double);
    if floating && !matches!(op, BinaryOperator::Eq) {
        return None;
    }
    literal_datum(expr, field_type)
}

fn apply_comparison(
    name: String,
    op: &BinaryOperator,
    datum: Datum,
    swapped: bool,
) -> Option<Predicate> {
    let reference = Reference::new(name);
    match (op, swapped) {
        (BinaryOperator::Eq, _) => Some(reference.equal_to(datum)),
        (BinaryOperator::Lt, false) | (BinaryOperator::Gt, true) => {
            Some(reference.less_than(datum))
        }
        (BinaryOperator::LtEq, false) | (BinaryOperator::GtEq, true) => {
            Some(reference.less_than_or_equal_to(datum))
        }
        (BinaryOperator::Gt, false) | (BinaryOperator::Lt, true) => {
            Some(reference.greater_than(datum))
        }
        (BinaryOperator::GtEq, false) | (BinaryOperator::LtEq, true) => {
            Some(reference.greater_than_or_equal_to(datum))
        }
        _ => None,
    }
}

fn in_list_predicate(inner: &Expr, list: &[Expr], scope: &ColumnScope<'_>) -> Option<Predicate> {
    if list.is_empty() {
        return None;
    }
    let name = column_name(inner, scope)?;
    let field_type = primitive_type(scope.schema, &name)?;
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        values.push(literal_datum(item, &field_type)?);
    }
    Some(Reference::new(name).is_in(values))
}

fn starts_with_predicate(
    inner: &Expr,
    pattern: &Expr,
    scope: &ColumnScope<'_>,
) -> Option<Predicate> {
    let name = column_name(inner, scope)?;
    if !matches!(primitive_type(scope.schema, &name)?, PrimitiveType::String) {
        return None;
    }
    let prefix = literal_prefix(pattern)?;
    Some(Reference::new(name).starts_with(Datum::string(prefix)))
}

fn literal_prefix(pattern: &Expr) -> Option<String> {
    let Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
        ..
    }) = pattern
    else {
        return None;
    };
    let prefix = text.strip_suffix('%')?;
    let plain = !prefix.contains(['%', '_', '\\']);
    (plain && !prefix.is_empty()).then(|| prefix.to_string())
}

fn column_name(expr: &Expr, scope: &ColumnScope<'_>) -> Option<String> {
    let ident = match expr {
        Expr::Identifier(ident) => ident,
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if !parts[0].value.eq_ignore_ascii_case(scope.target_alias) {
                return None;
            }
            &parts[1]
        }
        _ => return None,
    };
    let field = if scope.case_insensitive {
        top_level_field(scope.schema, &ident.value)?
    } else {
        let normalized = if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_lowercase()
        };
        scope.schema.as_struct().field_by_name(&normalized)?
    };
    matches!(field.field_type.as_ref(), Type::Primitive(_)).then(|| field.name.clone())
}

fn primitive_type(schema: &Schema, name: &str) -> Option<PrimitiveType> {
    match top_level_field(schema, name)?.field_type.as_ref() {
        Type::Primitive(primitive) => Some(primitive.clone()),
        _ => None,
    }
}

fn iceberg_err(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests;
