use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch, RecordBatchOptions, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    ObjectName, ShowStatementFilter, ShowStatementFilterPosition, ShowStatementIn,
    ShowStatementOptions, Use,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::describe_show::filter_pattern_matches;
use crate::{catalog_handle, iceberg_err, name_parts};

#[allow(clippy::missing_errors_doc)]
pub(crate) fn session_defaults(ctx: &SessionContext) -> (String, String) {
    let catalog = ctx.copied_config().options().catalog.clone();
    (catalog.default_catalog, catalog.default_schema)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn set_session_defaults(ctx: &SessionContext, catalog: &str, namespace: &str) {
    let state = ctx.state_ref();
    let mut guard = state.write();
    let options = guard.config_mut().options_mut();
    options.catalog.default_catalog = catalog.to_string();
    options.catalog.default_schema = namespace.to_string();
}

#[must_use]
pub(crate) fn default_namespace_for_catalog(catalog: &str) -> &str {
    if catalog == "spark_catalog" {
        "default"
    } else {
        ""
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn table_or_view_not_found(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `{name}` cannot be found. Verify the \
         spelling and correctness of the schema and catalog. If you did not qualify the name \
         with a schema, verify the current_schema() output, or qualify the name with the \
         correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or \
         DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    ))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn schema_not_found(parts: &[&str]) -> DataFusionError {
    let rendered = parts
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".");
    DataFusionError::Plan(format!(
        "[SCHEMA_NOT_FOUND] The schema {rendered} cannot be found. Verify the spelling and \
         correctness of the schema and catalog. If you did not qualify the name with a \
         catalog, verify the current_schema() output, or qualify the name with the correct \
         catalog. To tolerate the error on drop use DROP SCHEMA IF EXISTS. SQLSTATE: 42704"
    ))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn complete_name(ctx: &SessionContext, parts: &[String]) -> Result<Vec<String>> {
    let (default_catalog, default_schema) = session_defaults(ctx);
    match parts {
        [table] => {
            if default_schema.is_empty() {
                return Err(table_or_view_not_found(table));
            }
            Ok(vec![default_catalog, default_schema, table.clone()])
        }
        [namespace, table] => Ok(vec![default_catalog, namespace.clone(), table.clone()]),
        _ => Ok(parts.to_vec()),
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn rename_dest(
    src_catalog: &str,
    src_namespace: &NamespaceIdent,
    dest: &ObjectName,
) -> Result<(String, TableIdent)> {
    let parts = name_parts(dest);
    match parts.as_slice() {
        [table] => Ok((
            src_catalog.to_string(),
            TableIdent::new(src_namespace.clone(), table.clone()),
        )),
        [namespace, table] => Ok((
            src_catalog.to_string(),
            TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone()),
        )),
        [catalog, namespace, table] => Ok((
            catalog.clone(),
            TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone()),
        )),
        _ => Err(DataFusionError::Plan(format!(
            "ALTER TABLE expects a three-part `catalog.namespace.table` name, got `{dest}`"
        ))),
    }
}

#[allow(clippy::missing_errors_doc)]
async fn namespace_exists(
    catalogs: &CatalogRegistry,
    catalog: &str,
    namespace: &str,
) -> Result<bool> {
    let Some(handle) = catalogs.get(catalog) else {
        return Ok(false);
    };
    handle
        .namespace_exists(&NamespaceIdent::new(namespace.to_string()))
        .await
        .map_err(iceberg_err)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn use_empty_frame(ctx: &SessionContext) -> Result<DataFrame> {
    let options = RecordBatchOptions::new().with_row_count(Some(0));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options)?;
    ctx.read_batch(batch)
}

#[allow(clippy::missing_errors_doc)]
fn switch_catalog(
    ctx: &SessionContext,
    catalog: &str,
    current_catalog: &str,
    current_namespace: &str,
) -> Result<DataFrame> {
    let namespace = if catalog == current_catalog {
        current_namespace.to_string()
    } else {
        default_namespace_for_catalog(catalog).to_string()
    };
    set_session_defaults(ctx, catalog, &namespace);
    use_empty_frame(ctx)
}

#[allow(clippy::missing_errors_doc)]
async fn execute_use_object(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    parts: &[String],
    namespace_only: bool,
) -> Result<DataFrame> {
    let (current_catalog, current_namespace) = session_defaults(ctx);
    if !namespace_only {
        if let [one] = parts {
            if catalogs.is_registered(one) {
                return switch_catalog(ctx, one, &current_catalog, &current_namespace);
            }
            if namespace_exists(catalogs, &current_catalog, one).await? {
                set_session_defaults(ctx, &current_catalog, one);
                return use_empty_frame(ctx);
            }
            return Err(schema_not_found(&[&current_catalog, one]));
        }
        if let [first, second] = parts {
            if catalogs.is_registered(first) {
                if namespace_exists(catalogs, first, second).await? {
                    set_session_defaults(ctx, first, second);
                    return use_empty_frame(ctx);
                }
                return Err(schema_not_found(&[first, second]));
            }
            return Err(schema_not_found(&[&current_catalog, first, second]));
        }
        if parts.len() > 2 {
            let mut rendered: Vec<&str> = Vec::with_capacity(parts.len() + 1);
            let first_is_catalog = parts
                .first()
                .is_some_and(|first| catalogs.is_registered(first));
            if !first_is_catalog {
                rendered.push(current_catalog.as_str());
            }
            rendered.extend(parts.iter().map(String::as_str));
            return Err(schema_not_found(&rendered));
        }
        return Err(schema_not_found(&[&current_catalog]));
    }
    if let [one] = parts {
        if namespace_exists(catalogs, &current_catalog, one).await? {
            set_session_defaults(ctx, &current_catalog, one);
            return use_empty_frame(ctx);
        }
        return Err(schema_not_found(&[&current_catalog, one]));
    }
    if parts.is_empty() {
        return Err(schema_not_found(&[&current_catalog]));
    }
    let mut rendered: Vec<&str> = Vec::with_capacity(parts.len() + 1);
    rendered.push(current_catalog.as_str());
    rendered.extend(parts.iter().map(String::as_str));
    Err(schema_not_found(&rendered))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn show_parse_refusal(statement: &str) -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "Syntax error in `{statement}`: this option is not part of the Spark SHOW grammar"
        ))),
        None,
    )
}

#[allow(clippy::missing_errors_doc)]
fn suffix_like_pattern(statement: &str, options: &ShowStatementOptions) -> Result<Option<String>> {
    if options.starts_with.is_some() || options.limit.is_some() || options.limit_from.is_some() {
        return Err(show_parse_refusal(statement));
    }
    match &options.filter_position {
        None => Ok(None),
        Some(ShowStatementFilterPosition::Suffix(ShowStatementFilter::Like(pattern))) => {
            Ok(Some(pattern.clone()))
        }
        Some(_) => Err(show_parse_refusal(statement)),
    }
}

#[allow(clippy::missing_errors_doc)]
fn show_in_parts(show_in: Option<&ShowStatementIn>) -> Result<Option<Vec<String>>> {
    let Some(scope) = show_in else {
        return Ok(None);
    };
    if scope.parent_type.is_some() {
        return Err(show_parse_refusal("SHOW ... IN"));
    }
    let Some(name) = &scope.parent_name else {
        return Err(show_parse_refusal("SHOW ... IN"));
    };
    Ok(Some(name_parts(name)))
}

#[allow(clippy::missing_errors_doc)]
fn show_catalogs_batch(names: Vec<String>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "catalog",
        DataType::Utf8,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(names))],
    )?)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn execute_show_catalogs(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    options: &ShowStatementOptions,
) -> Result<DataFrame> {
    if options.show_in.is_some() {
        return Err(show_parse_refusal("SHOW CATALOGS"));
    }
    let pattern = suffix_like_pattern("SHOW CATALOGS", options)?;
    let mut names = catalogs.catalog_names();
    if !names.iter().any(|name| name == "spark_catalog") {
        names.push("spark_catalog".to_string());
    }
    names.sort();
    let rows: Vec<String> = names
        .into_iter()
        .filter(|name| {
            pattern
                .as_deref()
                .is_none_or(|pattern| filter_pattern_matches(name, pattern))
        })
        .collect();
    ctx.read_batch(show_catalogs_batch(rows)?)
}

#[allow(clippy::missing_errors_doc)]
fn show_tables_batch(rows: Vec<(String, String)>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("tableName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
    ]));
    let (namespaces, tables): (Vec<String>, Vec<String>) = rows.into_iter().unzip();
    let temporary = BooleanArray::from(vec![false; tables.len()]);
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(namespaces)),
            Arc::new(StringArray::from(tables)),
            Arc::new(temporary),
        ],
    )?)
}

#[allow(clippy::missing_errors_doc)]
async fn resolve_show_tables_scope(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    scope: Option<Vec<String>>,
) -> Result<((String, String), bool)> {
    let (current_catalog, current_namespace) = session_defaults(ctx);
    let Some(parts) = scope else {
        return Ok(((current_catalog, current_namespace), true));
    };
    match parts.as_slice() {
        [one] => {
            if catalogs.is_registered(one) {
                return Ok(((one.clone(), current_namespace), true));
            }
            if namespace_exists(catalogs, &current_catalog, one).await? {
                return Ok(((current_catalog, one.clone()), false));
            }
            Err(schema_not_found(&[&current_catalog, one]))
        }
        [first, second] => {
            if catalogs.is_registered(first) {
                if namespace_exists(catalogs, first, second).await? {
                    return Ok(((first.clone(), second.clone()), false));
                }
                return Err(schema_not_found(&[first, second]));
            }
            Err(schema_not_found(&[&current_catalog, first, second]))
        }
        _ => {
            let mut rendered: Vec<&str> = Vec::with_capacity(parts.len() + 1);
            let first_is_catalog = parts
                .first()
                .is_some_and(|first| catalogs.is_registered(first));
            if !first_is_catalog {
                rendered.push(current_catalog.as_str());
            }
            rendered.extend(parts.iter().map(String::as_str));
            Err(schema_not_found(&rendered))
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_show_tables(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    options: &ShowStatementOptions,
) -> Result<DataFrame> {
    let pattern = suffix_like_pattern("SHOW TABLES", options)?;
    let scope = show_in_parts(options.show_in.as_ref())?;
    let ((catalog, namespace), ambient) = resolve_show_tables_scope(ctx, catalogs, scope).await?;
    let empty = show_tables_batch(Vec::new())?;
    let Some(handle) = catalogs.get(&catalog) else {
        return ctx.read_batch(empty);
    };
    if ambient
        && (namespace.is_empty() || !namespace_exists(catalogs, &catalog, &namespace).await?)
    {
        return ctx.read_batch(empty);
    }
    let mut tables = repark_iceberg::catalog::list_table_names(handle.as_ref(), &namespace).await?;
    tables.sort();
    let rows: Vec<(String, String)> = tables
        .into_iter()
        .filter(|table| {
            pattern
                .as_deref()
                .is_none_or(|pattern| filter_pattern_matches(table, pattern))
        })
        .map(|table| (namespace.clone(), table))
        .collect();
    ctx.read_batch(show_tables_batch(rows)?)
}

#[allow(clippy::missing_errors_doc)]
fn show_columns_batch(columns: Vec<String>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col_name",
        DataType::Utf8,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(columns))],
    )?)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_show_columns(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    options: &ShowStatementOptions,
) -> Result<DataFrame> {
    let Some(scope) = &options.show_in else {
        return Err(show_parse_refusal("SHOW COLUMNS"));
    };
    if scope.parent_type.is_some()
        || options.starts_with.is_some()
        || options.limit.is_some()
        || options.limit_from.is_some()
        || options.filter_position.is_some()
    {
        return Err(show_parse_refusal("SHOW COLUMNS"));
    }
    let Some(name) = &scope.parent_name else {
        return Err(show_parse_refusal("SHOW COLUMNS"));
    };
    let completed = complete_name(ctx, &name_parts(name))?;
    let [catalog, namespace, table] = completed.as_slice() else {
        return Err(table_or_view_not_found(&name.to_string()));
    };
    let handle = catalog_handle(catalogs, catalog)?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    if !handle.table_exists(&ident).await.map_err(iceberg_err)? {
        return Err(table_or_view_not_found(&format!(
            "{catalog}.{namespace}.{table}"
        )));
    }
    let table = handle.load_table(&ident).await.map_err(iceberg_err)?;
    let columns: Vec<String> = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    ctx.read_batch(show_columns_batch(columns)?)
}

#[must_use]
pub(crate) fn is_use_default(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let Some(head) = trimmed.get(..3) else {
        return false;
    };
    if !head.eq_ignore_ascii_case("use") {
        return false;
    }
    let rest = trimmed[3..].trim_start();
    if rest.is_empty() || !trimmed[3..].starts_with(|char: char| char.is_whitespace()) {
        return false;
    }
    let Some(word) = rest.get(..7) else {
        return false;
    };
    if !word.eq_ignore_ascii_case("default") {
        return false;
    }
    let tail = rest[7..].trim();
    tail.is_empty() || tail == ";"
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_use(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    target: &Use,
) -> Result<DataFrame> {
    match target {
        Use::Object(name) => execute_use_object(ctx, catalogs, &name_parts(name), false).await,
        Use::Database(name) | Use::Schema(name) => {
            execute_use_object(ctx, catalogs, &name_parts(name), true).await
        }
        Use::Catalog(name) => Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(format!(
                "Syntax error at or near '{name}': extra input. SQLSTATE: 42601"
            ))),
            None,
        )),
        Use::Default => execute_use_object(ctx, catalogs, &["DEFAULT".to_string()], false).await,
        Use::Warehouse(name) | Use::Role(name) => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL statement: USE {name}"
        ))),
        Use::SecondaryRoles(roles) => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL statement: USE SECONDARY ROLES {roles}"
        ))),
    }
}

pub(crate) enum RefreshTarget {
    Table(Vec<String>),
    Path,
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn try_parse_refresh(sql: &str) -> Option<Result<RefreshTarget>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::REFRESH) {
        return None;
    }
    let _ = parser.parse_keyword(Keyword::TABLE);
    if matches!(parser.peek_token().token, Token::SingleQuotedString(_)) {
        if parser.parse_literal_string().is_err() {
            return None;
        }
        return match parser.peek_token().token {
            Token::EOF | Token::SemiColon => Some(Ok(RefreshTarget::Path)),
            _ => None,
        };
    }
    let name = parser.parse_object_name(false).ok()?;
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return None;
    }
    let parts = name_parts(&name);
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    Some(Ok(RefreshTarget::Table(parts)))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_refresh(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    target: RefreshTarget,
) -> Result<DataFrame> {
    let RefreshTarget::Table(parts) = target else {
        return use_empty_frame(ctx);
    };
    if parts.len() == 1
        && ctx
            .table_provider(TableReference::Bare {
                table: parts[0].as_str().into(),
            })
            .await
            .is_ok()
    {
        return use_empty_frame(ctx);
    }
    let completed = complete_name(ctx, &parts)?;
    let [catalog, namespace, table] = completed.as_slice() else {
        return Err(table_or_view_not_found(&parts.join(".")));
    };
    let handle = catalog_handle(catalogs, catalog)?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    if !handle.table_exists(&ident).await.map_err(iceberg_err)? {
        return Err(table_or_view_not_found(&format!(
            "{catalog}.{namespace}.{table}"
        )));
    }
    crate::reregister_catalog_provider(ctx, handle.clone(), catalog).await?;
    use_empty_frame(ctx)
}
