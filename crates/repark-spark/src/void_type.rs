use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CastKind, DataType, Expr, Insert, SetExpr, Statement, TableObject, Value, ValueWithSpan,
    VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::Span;
use iceberg::spec::{PrimitiveType, Type};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::name_parts;
use crate::write_to_branch::qualify_table_parts;

pub(crate) fn rewrite_cast_null_to_void(statement: &mut Statement) {
    let _ = statement.visit(&mut CastNullToVoid);
}

struct CastNullToVoid;

impl VisitorMut for CastNullToVoid {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Cast {
            kind: CastKind::Cast,
            expr: operand,
            data_type,
            array: false,
            format: None,
            ..
        } = expr
            && is_void_spelling(data_type)
            && is_null_valued(operand)
        {
            *expr = Expr::Value(ValueWithSpan {
                value: Value::Null,
                span: Span::empty(),
            });
        }
        ControlFlow::Continue(())
    }
}

fn is_void_spelling(data_type: &DataType) -> bool {
    match data_type {
        DataType::Custom(name, modifiers) => {
            modifiers.is_empty() && name.to_string().eq_ignore_ascii_case("void")
        }
        _ => false,
    }
}

fn is_null_valued(expr: &Expr) -> bool {
    match expr {
        Expr::Value(value) => matches!(value.value, Value::Null),
        Expr::Nested(inner) => is_null_valued(inner),
        Expr::Cast { expr, .. } => is_null_valued(expr),
        _ => false,
    }
}

pub(crate) async fn refuse_insert_void_values(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: &Statement,
) -> Result<()> {
    let Statement::Insert(insert) = statement else {
        return Ok(());
    };
    refuse_non_null_void_values(ctx, catalogs, insert).await
}

async fn refuse_non_null_void_values(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
) -> Result<()> {
    let TableObject::TableName(name) = &insert.table else {
        return Ok(());
    };
    let Some(source) = insert.source.as_ref() else {
        return Ok(());
    };
    let values = match source.body.as_ref() {
        SetExpr::Values(values) => values,
        SetExpr::Query(query) => match query.body.as_ref() {
            SetExpr::Values(values) => values,
            _ => return Ok(()),
        },
        _ => return Ok(()),
    };
    let parts = qualify_table_parts(ctx, name_parts(name));
    if parts.len() < 3 {
        return Ok(());
    }
    let Some(catalog) = catalogs.get(&parts[0]) else {
        return Ok(());
    };
    let Ok(namespace) = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(());
    };
    let fields = table.metadata().current_schema().as_struct().fields();
    if !fields.iter().any(|field| {
        matches!(
            field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Unknown)
        )
    }) {
        return Ok(());
    }
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let display = crate::catalog_ops::quoted_table_display(&parts);
    for row in &values.rows {
        check_void_row(
            ctx,
            fields,
            &display,
            &row.content,
            &insert.columns,
            case_insensitive,
        )
        .await?;
    }
    Ok(())
}

async fn check_void_row(
    ctx: &SessionContext,
    fields: &[std::sync::Arc<iceberg::spec::NestedField>],
    display: &str,
    row: &[Expr],
    columns: &[datafusion::sql::sqlparser::ast::ObjectName],
    case_insensitive: bool,
) -> Result<()> {
    if columns.is_empty() {
        if row.len() != fields.len() {
            return Ok(());
        }
        for (value, field) in row.iter().zip(fields.iter()) {
            refuse_void_value(ctx, display, field, value).await?;
        }
        return Ok(());
    }
    if row.len() != columns.len() {
        return Ok(());
    }
    for (value, column) in row.iter().zip(columns.iter()) {
        let Some(field) = fields.iter().find(|field| {
            if case_insensitive {
                field.name.eq_ignore_ascii_case(&column.to_string())
            } else {
                field.name == column.to_string()
            }
        }) else {
            continue;
        };
        refuse_void_value(ctx, display, field, value).await?;
    }
    Ok(())
}

async fn refuse_void_value(
    ctx: &SessionContext,
    display: &str,
    field: &iceberg::spec::NestedField,
    value: &Expr,
) -> Result<()> {
    if !matches!(
        field.field_type.as_ref(),
        Type::Primitive(PrimitiveType::Unknown)
    ) || is_null_valued(value)
    {
        return Ok(());
    }
    let Some(source) = void_source_type_name(ctx, value).await else {
        return Ok(());
    };
    Err(DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table {display}: Cannot safely cast `{}` \"{source}\" to \"VOID\". SQLSTATE: KD000",
        field.name
    )))
}

async fn void_source_type_name(ctx: &SessionContext, value: &Expr) -> Option<String> {
    if let Some(name) = void_literal_type_name(value) {
        return Some(name.to_string());
    }
    let probe = format!("SELECT {value} AS probe");
    let frame = ctx.sql(&probe).await.ok()?;
    let described = frame.schema().fields().first()?;
    Some(crate::spark_type_names::spark_ddl_type_name(described.data_type()).to_uppercase())
}

fn void_literal_type_name(value: &Expr) -> Option<&'static str> {
    match value {
        Expr::Value(literal) => match &literal.value {
            Value::Number(text, _) => {
                if text.bytes().all(|byte| byte.is_ascii_digit()) {
                    Some("INT")
                } else {
                    Some("DOUBLE")
                }
            }
            Value::SingleQuotedString(_)
            | Value::NationalStringLiteral(_)
            | Value::TripleSingleQuotedString(_) => Some("STRING"),
            Value::Boolean(_) => Some("BOOLEAN"),
            _ => None,
        },
        Expr::Nested(inner) => void_literal_type_name(inner),
        Expr::Cast { expr, .. } => void_literal_type_name(expr),
        _ => None,
    }
}
