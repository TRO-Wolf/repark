use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, CastKind, Expr, Ident, Insert, ObjectName, Query, SelectItem, SetExpr,
    TableObject, UnaryOperator, Value, ValueWithSpan, Values,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::spec::NestedFieldRef;
use repark_common::spark_error;
use repark_core::CatalogRegistry;

use super::{PreparedInsert, reparse_insert_sql, reparsed};

const SOURCE_ALIAS: &str = "__repark_partition_src";

enum ClauseValue {
    Dynamic,
    Null,
    Text(String),
}

struct ClauseItem {
    name: String,
    value: ClauseValue,
}

struct StaticColumn {
    index: usize,
    name: String,
    literal: Expr,
}

pub(crate) async fn rewrite_partition_clause(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
) -> Result<Option<PreparedInsert>> {
    let Some(clause) = insert.partitioned.as_ref() else {
        return Ok(None);
    };
    let TableObject::TableName(name) = &insert.table else {
        return Ok(None);
    };
    let Some(items) = clause_items(clause)? else {
        if insert.overwrite {
            return Ok(None);
        }
        return Err(clause_refusal(clause));
    };
    if insert.overwrite {
        if let Ok(Some((_, _, table, _))) =
            crate::insert_overwrite::try_resolve_iceberg_overwrite_target(ctx, catalogs, name).await
        {
            static_columns(&table, &items)?;
        }
        return Ok(None);
    }
    if !plain_positional(insert) {
        return Ok(None);
    }
    let rewritten = rewrite_append(ctx, catalogs, insert, name, &items).await?;
    Ok(Some(PreparedInsert {
        owned_append: true,
        ..rewritten
    }))
}

pub(crate) fn sql_has_partition_append(sql: &str) -> bool {
    if !sql.to_ascii_lowercase().contains("partition") {
        return false;
    }
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    let mut significant = tokens
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .peekable();
    let mut keyword = |expected: Keyword| {
        significant
            .next_if(|token| matches!(token, Token::Word(word) if word.keyword == expected && word.quote_style.is_none()))
            .is_some()
    };
    if !(keyword(Keyword::INSERT) && keyword(Keyword::INTO)) {
        return false;
    }
    keyword(Keyword::TABLE);
    let mut expect_part = true;
    for token in significant {
        match (expect_part, token) {
            (true, Token::Word(_)) => expect_part = false,
            (false, Token::Period) => expect_part = true,
            (false, Token::Word(word)) => {
                return word.keyword == Keyword::PARTITION && word.quote_style.is_none();
            }
            _ => return false,
        }
    }
    false
}

async fn rewrite_append(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    name: &ObjectName,
    items: &[ClauseItem],
) -> Result<PreparedInsert> {
    let (catalog_name, table) =
        match crate::insert_overwrite::try_resolve_iceberg_overwrite_target(ctx, catalogs, name)
            .await
        {
            Ok(Some((catalog_name, _, table, _))) => (catalog_name, table),
            Ok(None) => return Err(non_partition_column(&items[0].name)),
            Err(_) => return reparsed(without_partition(insert)),
        };
    let request = repark_iceberg::write::PartitionOverwriteRequest {
        names: items.iter().map(|item| item.name.clone()).collect(),
        ..Default::default()
    };
    repark_iceberg::write::validated_static_equalities(&table, &request)?;
    let statics = static_columns(&table, items)?;
    if statics.is_empty() {
        if let Some(source) = insert.source.as_ref().filter(|_| insert.columns.is_empty()) {
            refuse_positional_arity(ctx, catalogs, &catalog_name, &table, source).await?;
        }
        return reparsed(without_partition(insert));
    }
    rewrite_with_statics(ctx, catalogs, insert, &catalog_name, &table, &statics).await
}

async fn rewrite_with_statics(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    catalog_name: &str,
    table: &iceberg::table::Table,
    statics: &[StaticColumn],
) -> Result<PreparedInsert> {
    let source = insert.source.as_ref().ok_or_else(|| {
        DataFusionError::Plan(
            "INSERT INTO … PARTITION requires a SELECT or VALUES source".to_string(),
        )
    })?;
    let planned = planned_source_names(ctx, catalogs, source).await?;
    if insert.columns.is_empty() {
        let fields = field_names(table);
        let expected = fields.len() - statics.len();
        if planned.len() != expected {
            let display = table_display(catalog_name, table);
            let data = data_columns(source, &planned, statics);
            return Err(arity_error(
                &display,
                &fields,
                &data,
                planned.len() < expected,
            ));
        }
    }
    if let Some(rows) = plain_values(source) {
        let mut rewritten = without_partition(insert);
        let listed = listed_columns(insert, statics)?;
        let mut values = rows.clone();
        for row in &mut values.rows {
            if listed.is_empty() {
                for column in statics {
                    row.content.insert(column.index, column.literal.clone());
                }
            } else {
                row.content
                    .extend(statics.iter().map(|column| column.literal.clone()));
            }
        }
        if !listed.is_empty() {
            rewritten.columns = listed
                .iter()
                .map(|name| ObjectName::from(vec![Ident::with_quote('`', name.clone())]))
                .collect();
        }
        let mut query = source.as_ref().clone();
        query.body = Box::new(SetExpr::Values(values));
        rewritten.source = Some(Box::new(query));
        return reparsed(rewritten);
    }
    let aliases: Vec<String> = (1..=planned.len())
        .map(|index| format!("__repark_p{index}"))
        .collect();
    let (columns, items) = if insert.columns.is_empty() {
        (
            String::new(),
            positional_items(field_names(table).len(), &aliases, statics),
        )
    } else {
        let listed = listed_columns(insert, statics)?;
        let names = listed.iter().map(|name| quote(name)).collect::<Vec<_>>();
        let items = aliases
            .iter()
            .cloned()
            .chain(statics.iter().map(|column| column.literal.to_string()))
            .collect();
        (format!(" ({})", names.join(", ")), items)
    };
    let name = &insert.table;
    reparse_insert_sql(format!(
        "INSERT INTO {name}{columns} SELECT {} FROM ({source}) AS {SOURCE_ALIAS}({})",
        items.join(", "),
        aliases.join(", ")
    ))
}

pub(crate) async fn refuse_positional_arity(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    table: &iceberg::table::Table,
    source: &Query,
) -> Result<()> {
    let planned = planned_source_names(ctx, catalogs, source).await?;
    let fields = field_names(table);
    if planned.len() == fields.len() {
        return Ok(());
    }
    let display = table_display(catalog_name, table);
    let data = data_columns(source, &planned, &[]);
    Err(arity_error(
        &display,
        &fields,
        &data,
        planned.len() < fields.len(),
    ))
}

async fn planned_source_names(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    source: &Query,
) -> Result<Vec<String>> {
    if let Some(values) = plain_values(source) {
        let width = values.rows.first().map_or(0, |row| row.content.len());
        return Ok((1..=width).map(|index| format!("col{index}")).collect());
    }
    let probe = crate::spark_ast::execute_passthrough(
        ctx,
        catalogs,
        &format!("SELECT * FROM ({source}) AS {SOURCE_ALIAS}"),
    )
    .await?;
    Ok(probe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect())
}

fn plain_values(source: &Query) -> Option<&Values> {
    let plain = source.with.is_none()
        && source.order_by.is_none()
        && source.limit_clause.is_none()
        && source.fetch.is_none();
    match source.body.as_ref() {
        SetExpr::Values(values) if plain => Some(values),
        _ => None,
    }
}

fn field_names(table: &iceberg::table::Table) -> Vec<String> {
    table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

fn positional_items(width: usize, aliases: &[String], statics: &[StaticColumn]) -> Vec<String> {
    let mut remaining = aliases.iter();
    (0..width)
        .map(
            |index| match statics.iter().find(|column| column.index == index) {
                Some(column) => column.literal.to_string(),
                None => remaining.next().cloned().unwrap_or_default(),
            },
        )
        .collect()
}

fn listed_columns(insert: &Insert, statics: &[StaticColumn]) -> Result<Vec<String>> {
    if insert.columns.is_empty() {
        return Ok(Vec::new());
    }
    let listed: Vec<String> = insert
        .columns
        .iter()
        .map(crate::insert_overwrite::object_name_last)
        .collect();
    if let Some(column) = statics.iter().find(|column| {
        listed
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column.name))
    }) {
        return Err(static_column_in_list(&column.name));
    }
    Ok(listed
        .into_iter()
        .chain(statics.iter().map(|column| column.name.clone()))
        .collect())
}

fn plain_positional(insert: &Insert) -> bool {
    !insert.replace_into
        && !insert.ignore
        && insert.or.is_none()
        && insert.on.is_none()
        && insert.returning.is_none()
        && insert.output.is_none()
        && insert.assignments.is_empty()
        && insert.after_columns.is_empty()
        && insert.settings.is_none()
        && insert.format_clause.is_none()
        && insert.source.is_some()
}

fn without_partition(insert: &Insert) -> Insert {
    let mut stripped = insert.clone();
    stripped.partitioned = None;
    stripped
}

fn clause_items(clause: &[Expr]) -> Result<Option<Vec<ClauseItem>>> {
    let mut seen = HashSet::new();
    let mut items = Vec::with_capacity(clause.len());
    for expr in clause {
        let Some(item) = clause_item(expr) else {
            return Ok(None);
        };
        if !seen.insert(item.name.to_ascii_lowercase()) {
            return Err(DataFusionError::SQL(
                Box::new(ParserError::ParserError(format!(
                    "[DUPLICATE_KEY] Found duplicate keys {}. SQLSTATE: 23505",
                    quote(&item.name)
                ))),
                None,
            ));
        }
        items.push(item);
    }
    Ok(Some(items))
}

fn clause_item(expr: &Expr) -> Option<ClauseItem> {
    match expr {
        Expr::Identifier(ident) => Some(ClauseItem {
            name: ident.value.clone(),
            value: ClauseValue::Dynamic,
        }),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => match left.as_ref() {
            Expr::Identifier(ident) => Some(ClauseItem {
                name: ident.value.clone(),
                value: constant_value(right)?,
            }),
            _ => None,
        },
        _ => None,
    }
}

fn constant_value(expr: &Expr) -> Option<ClauseValue> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => {
                Some(ClauseValue::Text(text.clone()))
            }
            Value::Number(raw, _) => Some(ClauseValue::Text(raw.clone())),
            Value::Boolean(flag) => Some(ClauseValue::Text(flag.to_string())),
            Value::Null => Some(ClauseValue::Null),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Value(ValueWithSpan {
                value: Value::Number(raw, _),
                ..
            }) => Some(ClauseValue::Text(format!("-{raw}"))),
            _ => None,
        },
        Expr::TypedString(typed) => match &typed.value.value {
            Value::SingleQuotedString(text) => Some(ClauseValue::Text(text.clone())),
            _ => None,
        },
        _ => None,
    }
}

fn static_columns(
    table: &iceberg::table::Table,
    items: &[ClauseItem],
) -> Result<Vec<StaticColumn>> {
    let schema = table.metadata().current_schema();
    let fields = schema.as_struct().fields();
    let mut statics = Vec::new();
    for item in items {
        let text = match &item.value {
            ClauseValue::Dynamic => continue,
            ClauseValue::Null => None,
            ClauseValue::Text(text) => Some(text.as_str()),
        };
        let Some((index, field)) = fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name.eq_ignore_ascii_case(&item.name))
        else {
            return Err(non_partition_column(&item.name));
        };
        statics.push(StaticColumn {
            index,
            name: field.name.clone(),
            literal: static_literal(text, field)?,
        });
    }
    statics.sort_by_key(|column| column.index);
    Ok(statics)
}

fn static_literal(text: Option<&str>, field: &NestedFieldRef) -> Result<Expr> {
    let arrow_type = iceberg::arrow::type_to_arrow_type(&field.field_type)
        .map_err(repark_iceberg::catalog::iceberg_to_datafusion)?;
    let spark_type = crate::spark_type_names::spark_ddl_type_name(&arrow_type);
    let Some(text) = text else {
        return typed_cast(Value::Null, &spark_type);
    };
    if matches!(
        arrow_type,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View
    ) {
        return Ok(Expr::Value(
            Value::SingleQuotedString(text.to_string()).into(),
        ));
    }
    let trimmed = text.trim();
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let probe = Arc::new(StringArray::from(vec![trimmed]));
    if cast_with_options(probe.as_ref(), &arrow_type, &options).is_err() {
        return Err(repark_iceberg::write::illegal_argument_error(
            spark_error::message(
                spark_error::CAST_INVALID_INPUT,
                &[
                    ("value", &format!("'{text}'")),
                    ("fromType", "STRING"),
                    ("toType", &spark_type.to_ascii_uppercase()),
                ],
            ),
        ));
    }
    typed_cast(Value::SingleQuotedString(trimmed.to_string()), &spark_type)
}

fn typed_cast(value: Value, spark_type: &str) -> Result<Expr> {
    let data_type = Parser::new(&DatabricksDialect {})
        .try_with_sql(spark_type)
        .and_then(|mut parser| parser.parse_data_type())
        .map_err(|error| {
            DataFusionError::Internal(format!(
                "INSERT INTO … PARTITION could not spell type {spark_type}: {error}"
            ))
        })?;
    Ok(Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(Expr::Value(value.into())),
        data_type,
        array: false,
        format: None,
    })
}

fn data_columns(source: &Query, planned: &[String], statics: &[StaticColumn]) -> Vec<String> {
    let mut names = spark_source_names(source).unwrap_or_else(|| planned.to_vec());
    for column in statics {
        let at = column.index.min(names.len());
        names.insert(at, column.name.clone());
    }
    names
}

fn spark_source_names(source: &Query) -> Option<Vec<String>> {
    let mut body = source.body.as_ref();
    loop {
        match body {
            SetExpr::Query(inner) => body = inner.body.as_ref(),
            SetExpr::SetOperation { left, .. } => body = left.as_ref(),
            SetExpr::Values(values) => {
                return Some(
                    (1..=values.rows.first()?.content.len())
                        .map(|index| format!("col{index}"))
                        .collect(),
                );
            }
            SetExpr::Select(select) => {
                return select
                    .projection
                    .iter()
                    .map(|item| match item {
                        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                        SelectItem::UnnamedExpr(expr) => {
                            crate::insert_by_name::spark_names::expression_name(expr, &[])
                        }
                        _ => None,
                    })
                    .collect();
            }
            _ => return None,
        }
    }
}

fn arity_error(table: &str, fields: &[String], data: &[String], short: bool) -> DataFusionError {
    let table_columns = fields.iter().map(|name| quote(name)).collect::<Vec<_>>();
    let data_columns = data.iter().map(|name| quote(name)).collect::<Vec<_>>();
    if short {
        return DataFusionError::Plan(spark_error::message(
            spark_error::INSERT_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS,
            &[
                ("tableName", table),
                ("tableColumns", &table_columns.join(", ")),
                ("dataColumns", &data_columns.join(", ")),
            ],
        ));
    }
    DataFusionError::Plan(format!(
        "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to {table}, the reason \
         is too many data columns:\nTable columns: {}.\nData columns: {}. SQLSTATE: 21S01",
        table_columns.join(", "),
        data_columns.join(", ")
    ))
}

fn table_display(catalog_name: &str, table: &iceberg::table::Table) -> String {
    let identifier = table.identifier();
    std::iter::once(catalog_name.to_string())
        .chain(identifier.namespace().iter().cloned())
        .chain(std::iter::once(identifier.name().to_string()))
        .map(|part| quote(&part))
        .collect::<Vec<_>>()
        .join(".")
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn non_partition_column(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[NON_PARTITION_COLUMN] PARTITION clause cannot contain the non-partition column: {}. \
         SQLSTATE: 42000",
        quote(name)
    ))
}

fn static_column_in_list(column: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST] Static partition column {column} is also \
         specified in the column list. SQLSTATE: 42713"
    ))
}

fn clause_refusal(clause: &[Expr]) -> DataFusionError {
    let written = clause
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "INSERT INTO … PARTITION accepts `column` or `column = constant` items, got \
         `PARTITION ({written})`"
    ))
}
