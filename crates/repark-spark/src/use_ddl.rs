use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::RecordBatchOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::Use;
use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::NamespaceIdent;
use repark_core::CatalogRegistry;

use crate::{iceberg_err, name_parts};

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
async fn switch_catalog(
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
                return switch_catalog(ctx, one, &current_catalog, &current_namespace).await;
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
