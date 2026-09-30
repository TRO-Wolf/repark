use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion::arrow::datatypes::{DataType as ArrowType, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CastKind, DataType, DollarQuotedString, Expr, Insert, ObjectName, SetExpr, Statement,
    TableObject, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::Span;
use iceberg::spec::{NestedField, PrimitiveType, Type};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::insert_defaults::is_default_marker;
use repark_iceberg::write::ntz_store::refuse_ntz_writes;
use repark_iceberg::write::void_store::refuse_void_writes;

use crate::catalog_ops::name_parts;
use crate::write_to_branch::qualify_table_parts;

mod insert_source_types;
mod ltz_values_store;
mod select_values_arms;
mod sibling_types;
mod source_leaves;

pub(crate) use source_leaves::{is_string_type, source_type_is_reliable};

pub(crate) async fn refuse_insert_source_types(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    by_name: bool,
) -> Result<()> {
    Box::pin(insert_source_types::refuse_insert_source_types(
        ctx, catalogs, insert, by_name,
    ))
    .await
}

pub(crate) async fn refuse_partition_overwrite_sources(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
    table_name: &ObjectName,
    filled: &repark_iceberg::write::insert_defaults::OverwriteSource,
    reserved: &[String],
) -> Result<()> {
    Box::pin(insert_source_types::refuse_partition_overwrite_sources(
        ctx, table, table_name, filled, reserved,
    ))
    .await
}

pub(crate) use ltz_values_store::number_text_type;

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

pub(crate) fn is_null_valued(expr: &Expr) -> bool {
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
    ltz_values_store::refuse_unassignable_ltz_values(ctx, catalogs, insert).await?;
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
        SetExpr::Values(values) => Some(values),
        SetExpr::Query(query) => match query.body.as_ref() {
            SetExpr::Values(values) => Some(values),
            _ => None,
        },
        _ => None,
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
            Type::Primitive(PrimitiveType::Unknown | PrimitiveType::Timestamp)
        )
    }) {
        return Ok(());
    }
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let display = crate::catalog_ops::quoted_table_display(&parts);
    let Some(values) = values else {
        return refuse_void_query(
            ctx,
            fields,
            &display,
            source,
            &insert.columns,
            case_insensitive,
        )
        .await;
    };
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
        check_ntz_row(
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

pub(crate) fn parenthesize_stacked_minus(value: &Expr) -> Expr {
    let mut rewritten = value.clone();
    let _ = rewritten.visit(&mut StackedSignParens);
    rewritten
}

struct StackedSignParens;

impl VisitorMut for StackedSignParens {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::UnaryOp { expr: operand, .. } = expr
            && matches!(operand.as_ref(), Expr::UnaryOp { .. })
        {
            let inner = operand.as_ref().clone();
            **operand = Expr::Nested(Box::new(inner));
        }
        ControlFlow::Continue(())
    }
}

pub(crate) fn probe_text(value: &Expr) -> String {
    let mut rewritten = parenthesize_stacked_minus(value);
    let _ = rewritten.visit(&mut DollarQuoteProbeStrings);
    rewritten.to_string()
}

struct DollarQuoteProbeStrings;

impl VisitorMut for DollarQuoteProbeStrings {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(literal) = expr
            && let Value::SingleQuotedString(text) = &literal.value
            && text.contains('\'')
        {
            let value = text.clone();
            let tag = probe_string_tag(&value);
            literal.value = Value::DollarQuotedString(DollarQuotedString {
                value,
                tag: Some(tag),
            });
        }
        ControlFlow::Continue(())
    }
}

fn probe_string_tag(value: &str) -> String {
    let mut tag = String::from("p");
    while format!("{value}${tag}$").find(format!("${tag}$").as_str()) != Some(value.len()) {
        tag.push('p');
    }
    tag
}

fn numeric_ntz_refusal(
    display: &str,
    column: &str,
    source: &ArrowType,
    target: &ArrowType,
) -> String {
    let from = crate::spark_type_names::spark_ddl_type_name(source).to_uppercase();
    let to = crate::spark_type_names::spark_ddl_type_name(target).to_uppercase();
    let column = format!("`{column}`");
    repark_common::spark_error::message(
        repark_common::spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
        &[
            ("tableName", display),
            ("columnName", column.as_str()),
            ("fromType", from.as_str()),
            ("toType", to.as_str()),
        ],
    )
}

fn refuse_unassignable_ntz_positions(
    ctx: &SessionContext,
    display: &str,
    plan: &LogicalPlan,
    positions: &[(&NestedField, &Expr)],
    naive: &ArrowType,
) -> Result<()> {
    let state = ctx.state();
    let Ok(analyzed) =
        state
            .analyzer()
            .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
    else {
        return Ok(());
    };
    let planned = analyzed.schema().fields();
    if planned.len() != positions.len() {
        return Ok(());
    }
    for (field, (target, _)) in planned.iter().zip(positions) {
        if !matches!(
            target.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Timestamp)
        ) {
            continue;
        }
        let rendered = format!("`{}`", target.name);
        if let Some(text) = repark_iceberg::write::update_cast::incompatible_update_message(
            display,
            &rendered,
            field.data_type(),
            naive,
        ) {
            return Err(DataFusionError::Plan(text));
        }
        if field.data_type().is_numeric() {
            return Err(DataFusionError::Plan(numeric_ntz_refusal(
                display,
                &target.name,
                field.data_type(),
                naive,
            )));
        }
    }
    Ok(())
}

fn ntz_probe_cell(index: usize, value: &Expr) -> String {
    if is_default_marker(value) {
        format!("NULL AS p{index}")
    } else {
        format!("{} AS p{index}", probe_text(value))
    }
}

async fn check_ntz_row(
    ctx: &SessionContext,
    fields: &[std::sync::Arc<NestedField>],
    display: &str,
    row: &[Expr],
    columns: &[ObjectName],
    case_insensitive: bool,
) -> Result<()> {
    let positions: Vec<(&NestedField, &Expr)> = if columns.is_empty() {
        if row.len() != fields.len() {
            return Ok(());
        }
        fields.iter().map(AsRef::as_ref).zip(row.iter()).collect()
    } else {
        if row.len() != columns.len() {
            return Ok(());
        }
        row.iter()
            .zip(columns.iter())
            .filter_map(|(value, column)| {
                fields
                    .iter()
                    .find(|field| {
                        if case_insensitive {
                            field.name.eq_ignore_ascii_case(&column_name(column))
                        } else {
                            field.name == column_name(column)
                        }
                    })
                    .map(|field| (field.as_ref(), value))
            })
            .collect()
    };
    if !positions.iter().any(|(field, _)| {
        matches!(
            field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Timestamp)
        )
    }) {
        return Ok(());
    }
    let probe = format!(
        "SELECT {}",
        positions
            .iter()
            .enumerate()
            .map(|(index, (_, value))| ntz_probe_cell(index, value))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let frame = ctx.sql(&probe).await?;
    let naive = ArrowType::Timestamp(TimeUnit::Microsecond, None);
    refuse_unassignable_ntz_positions(ctx, display, frame.logical_plan(), &positions, &naive)?;
    let dummy = ArrowType::Boolean;
    let pairs = positions.iter().map(|(field, _)| {
        let target = match field.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Timestamp) => &naive,
            _ => &dummy,
        };
        (field.name.as_str(), target)
    });
    refuse_ntz_writes(ctx, display, frame.logical_plan(), pairs)
}

async fn refuse_void_query(
    ctx: &SessionContext,
    fields: &[std::sync::Arc<iceberg::spec::NestedField>],
    display: &str,
    source: &datafusion::sql::sqlparser::ast::Query,
    columns: &[datafusion::sql::sqlparser::ast::ObjectName],
    case_insensitive: bool,
) -> Result<()> {
    let targets: Option<Vec<&iceberg::spec::NestedField>> = if columns.is_empty() {
        Some(fields.iter().map(AsRef::as_ref).collect())
    } else {
        columns
            .iter()
            .map(|column| {
                let name = column_name(column);
                fields
                    .iter()
                    .find(|field| {
                        field.name == name
                            || (case_insensitive && field.name.eq_ignore_ascii_case(&name))
                    })
                    .map(AsRef::as_ref)
            })
            .collect()
    };
    let Some(targets) = targets else {
        return Ok(());
    };
    let Ok(frame) = ctx.sql(&source.to_string()).await else {
        return Ok(());
    };
    let types: Vec<ArrowType> = targets
        .iter()
        .map(|field| match field.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Unknown) => ArrowType::Null,
            Type::Primitive(PrimitiveType::Timestamp) => {
                ArrowType::Timestamp(TimeUnit::Microsecond, None)
            }
            _ => ArrowType::Boolean,
        })
        .collect();
    let pairs = targets
        .iter()
        .zip(&types)
        .map(|(field, data_type)| (field.name.as_str(), data_type));
    refuse_void_writes(ctx, display, frame.logical_plan(), pairs)?;
    let pairs = targets
        .iter()
        .zip(&types)
        .map(|(field, data_type)| (field.name.as_str(), data_type));
    refuse_ntz_writes(ctx, display, frame.logical_plan(), pairs)
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
                field.name.eq_ignore_ascii_case(&column_name(column))
            } else {
                field.name == column_name(column)
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
    ) || is_null_or_default_cell(value)
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

pub(crate) fn column_name(column: &datafusion::sql::sqlparser::ast::ObjectName) -> String {
    column
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map_or_else(|| column.to_string(), |ident| ident.value.clone())
}

fn is_null_or_default_cell(expr: &Expr) -> bool {
    is_default_marker(expr) || is_bare_null(expr)
}

fn is_bare_null(expr: &Expr) -> bool {
    match expr {
        Expr::Value(value) => matches!(value.value, Value::Null),
        Expr::Nested(inner) => is_bare_null(inner),
        _ => false,
    }
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
