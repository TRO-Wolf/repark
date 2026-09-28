use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, Insert, ObjectName, SetExpr, TableObject, UnaryOperator, Value,
};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use super::column_name;
use crate::catalog_ops::{name_parts, quoted_table_display};
use crate::write_to_branch::qualify_table_parts;

pub(crate) async fn refuse_unassignable_ltz_values(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
) -> Result<()> {
    if insert.partitioned.is_some() {
        return Ok(());
    }
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
    let Some(values) = values else {
        return Ok(());
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
    let schema = table.metadata().current_schema();
    let Ok(presented) = repark_iceberg::catalog::uuid_presentation::presented_arrow_schema(schema)
    else {
        return Ok(());
    };
    if !presented
        .fields()
        .iter()
        .any(|field| is_ltz(field.data_type()))
    {
        return Ok(());
    }
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let display = quoted_table_display(&parts);
    for row in &values.rows {
        check_row(
            ctx,
            &presented,
            &display,
            &row.content,
            &insert.columns,
            case_insensitive,
        )
        .await?;
    }
    Ok(())
}

async fn check_row(
    ctx: &SessionContext,
    presented: &ArrowSchema,
    display: &str,
    row: &[Expr],
    columns: &[ObjectName],
    case_insensitive: bool,
) -> Result<()> {
    if columns.is_empty() {
        if row.len() != presented.fields().len() {
            return Ok(());
        }
        for (value, field) in row.iter().zip(presented.fields()) {
            refuse_value(ctx, display, field.name(), field.data_type(), value).await?;
        }
        return Ok(());
    }
    if row.len() != columns.len() {
        return Ok(());
    }
    for (value, column) in row.iter().zip(columns.iter()) {
        let wanted = column_name(column);
        let Some(field) = presented.fields().iter().find(|field| {
            if case_insensitive {
                field.name().eq_ignore_ascii_case(&wanted)
            } else {
                field.name() == &wanted
            }
        }) else {
            continue;
        };
        refuse_value(ctx, display, field.name(), field.data_type(), value).await?;
    }
    Ok(())
}

async fn refuse_value(
    ctx: &SessionContext,
    display: &str,
    column: &str,
    target: &DataType,
    value: &Expr,
) -> Result<()> {
    if !is_ltz(target) {
        return Ok(());
    }
    let source = if let Some(data_type) = literal_source_type(value) {
        data_type
    } else {
        let Some(probed) = probe_source_type(ctx, value).await else {
            return Ok(());
        };
        probed
    };
    if let Some(text) = repark_iceberg::write::update_cast::incompatible_update_message(
        display,
        &format!("`{column}`"),
        &source,
        target,
    ) {
        return Err(DataFusionError::Plan(text));
    }
    Ok(())
}

fn is_ltz(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, Some(_)))
}

fn literal_source_type(value: &Expr) -> Option<DataType> {
    match value {
        Expr::Value(literal) => match &literal.value {
            Value::Number(text, _) => Some(number_text_type(text)),
            Value::SingleQuotedString(_)
            | Value::NationalStringLiteral(_)
            | Value::TripleSingleQuotedString(_) => Some(DataType::Utf8),
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Null => Some(DataType::Null),
            _ => None,
        },
        Expr::Nested(inner) => literal_source_type(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => {
            let mut peeled = expr.as_ref();
            while let Expr::Nested(inner) = peeled {
                peeled = inner;
            }
            if let Expr::Value(literal) = peeled
                && let Value::Number(text, _) = &literal.value
            {
                Some(number_text_type(text))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn number_text_type(text: &str) -> DataType {
    if text.bytes().all(|byte| byte.is_ascii_digit()) {
        DataType::Int32
    } else {
        DataType::Float64
    }
}

async fn probe_source_type(ctx: &SessionContext, value: &Expr) -> Option<DataType> {
    let frame = ctx.sql(&format!("SELECT {value} AS probe")).await.ok()?;
    frame
        .schema()
        .fields()
        .first()
        .map(|field| field.data_type().clone())
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn values_row(sql: &str) -> Vec<Expr> {
        let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let SetExpr::Values(values) = source.body.as_ref() else {
            panic!("want a VALUES source");
        };
        values.rows[0].content.clone()
    }

    fn typed(row: &[Expr]) -> Vec<Option<DataType>> {
        row.iter().map(literal_source_type).collect()
    }

    #[test]
    fn bare_and_signed_integers_read_as_int() {
        let row = values_row("INSERT INTO t VALUES (1, -1, +2, (3))");
        assert_eq!(typed(&row), vec![Some(DataType::Int32); 4], "{row:?}");
    }

    #[test]
    fn fractional_numbers_strings_booleans_and_nulls_read_as_their_kind() {
        let row = values_row("INSERT INTO t VALUES (1.5, 's', true, NULL)");
        assert_eq!(
            typed(&row),
            vec![
                Some(DataType::Float64),
                Some(DataType::Utf8),
                Some(DataType::Boolean),
                Some(DataType::Null),
            ],
            "{row:?}"
        );
    }

    #[test]
    fn casts_typed_strings_and_functions_defer_to_the_probe() {
        let row =
            values_row("INSERT INTO t VALUES (CAST(1 AS TIMESTAMP), DATE '2024-01-04', abs(-1))");
        assert_eq!(typed(&row), vec![None, None, None], "{row:?}");
    }
}
