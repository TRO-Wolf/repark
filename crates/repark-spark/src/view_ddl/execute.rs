use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::ObjectName;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, LocationPolicy};
use repark_iceberg::view::{
    ViewDefinition, ViewTarget, create_or_replace_view, drop_catalog_view, list_catalog_views,
    split_view_properties, view_schema_for_output,
};

use crate::catalog_ops::{catalog_handle, name_parts, table_or_view_not_found};
use crate::describe_show::filter_pattern_matches;
use crate::view_ddl::parse::{CreateViewStatement, ShowViewsStatement};
use crate::view_ddl::read::{plan_prepared_body, prepare_view_body_sql};

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_create_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: CreateViewStatement,
) -> Result<DataFrame> {
    let (catalog, namespace_name, view_name) = complete_view_name(ctx, &statement.name)?;
    let handle = catalog_handle(catalogs, &catalog)?;
    let warehouse = warehouse_for_catalog(catalogs, &catalog)?;
    let (stored_properties, location_override) =
        split_view_properties(statement.comment.as_deref(), &statement.properties)?;
    let namespace = NamespaceIdent::new(namespace_name.clone());
    let (prepared, pins) =
        prepare_view_body_sql(ctx, catalogs, &catalog, &namespace, &statement.body_sql).await?;
    let frame = plan_prepared_body(ctx, catalogs, &prepared, &pins).await?;
    pins.release(ctx);
    repark_core::refuse_iceberg_create_of_tightened_plan(frame.logical_plan())
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    let view_display = format!("{catalog}.{namespace_name}.{view_name}");
    let schema =
        view_schema_for_output(frame.schema().as_arrow(), &statement.aliases, &view_display)?;
    let target = ViewTarget {
        catalog_name: &catalog,
        catalog: handle.as_ref(),
        namespace: &namespace,
        namespace_name: &namespace_name,
        view_name: &view_name,
    };
    let definition = ViewDefinition {
        sql: statement.body_sql,
        schema,
        properties: stored_properties,
        warehouse,
        location_override,
        default_catalog: catalog.clone(),
    };
    create_or_replace_view(
        &target,
        statement.or_replace,
        statement.if_not_exists,
        definition,
    )
    .await?;
    ctx.read_empty()
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_drop_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<DataFrame> {
    for name in names {
        let parts = name_parts(name);
        let (catalog, namespace_name, view_name) = complete_view_name(ctx, &parts)?;
        let handle = catalog_handle(catalogs, &catalog)?;
        let namespace = NamespaceIdent::new(namespace_name.clone());
        let target = ViewTarget {
            catalog_name: &catalog,
            catalog: handle.as_ref(),
            namespace: &namespace,
            namespace_name: &namespace_name,
            view_name: &view_name,
        };
        drop_catalog_view(&target, if_exists).await?;
    }
    ctx.read_empty()
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_show_views(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: ShowViewsStatement,
) -> Result<DataFrame> {
    let (catalog, namespace_name) = match statement.namespace.as_slice() {
        [namespace] => (default_catalog_name(ctx), namespace.clone()),
        [catalog, namespace] => (catalog.clone(), namespace.clone()),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "expected a two-part `IN <catalog.namespace>` name, got `{}`",
                statement.namespace.join(".")
            )));
        }
    };
    let handle = catalog_handle(catalogs, &catalog)?;
    let namespace = NamespaceIdent::new(namespace_name.clone());
    let mut views =
        list_catalog_views(&catalog, handle.as_ref(), &namespace, &namespace_name).await?;
    views.sort();
    if let Some(pattern) = statement.like.as_deref() {
        views.retain(|view| filter_pattern_matches(view, pattern));
    }
    ctx.read_batch(show_views_batch(&namespace_name, &views)?)
}

pub(crate) fn show_views_batch(namespace: &str, views: &[String]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("viewName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![namespace; views.len()])),
            Arc::new(StringArray::from(views.to_vec())),
            Arc::new(BooleanArray::from(vec![false; views.len()])),
        ],
    )?)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn refuse_view_write_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &ObjectName,
) -> Result<()> {
    let parts = name_parts(name);
    if is_metadata_table_target(&parts) {
        return Err(metadata_table_write_refusal(&parts));
    }
    let Ok((catalog, namespace_name, view_name)) = complete_view_name(ctx, &parts) else {
        return Ok(());
    };
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace_name.clone()),
        view_name.clone(),
    );
    if catalogs.is_view(&catalog, &ident).await? {
        return Err(table_or_view_not_found(
            &catalog,
            &namespace_name,
            &view_name,
        ));
    }
    Ok(())
}

fn is_metadata_table_target(parts: &[String]) -> bool {
    parts.len() >= 4
        && parts
            .last()
            .is_some_and(|suffix| crate::is_metadata_table_name(suffix))
}

fn metadata_table_write_refusal(parts: &[String]) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Iceberg metadata table `{}` is read-only — INSERT/UPDATE/DELETE/MERGE/\
         CTAS/TRUNCATE/CREATE VIEW/DROP/ALTER targeting a metadata table is not supported",
        parts.join(".")
    ))
}

fn complete_view_name(ctx: &SessionContext, parts: &[String]) -> Result<(String, String, String)> {
    if is_metadata_table_target(parts) {
        return Err(metadata_table_write_refusal(parts));
    }
    match parts {
        [view] => Ok((
            default_catalog_name(ctx),
            default_namespace_name(ctx),
            view.clone(),
        )),
        [namespace, view] => Ok((default_catalog_name(ctx), namespace.clone(), view.clone())),
        [catalog, namespace, view] => Ok((catalog.clone(), namespace.clone(), view.clone())),
        _ => Err(DataFusionError::Plan(format!(
            "expected a [catalog.[namespace.]]view name, got `{}`",
            parts.join(".")
        ))),
    }
}

fn default_catalog_name(ctx: &SessionContext) -> String {
    ctx.copied_config()
        .options()
        .catalog
        .default_catalog
        .clone()
}

fn default_namespace_name(ctx: &SessionContext) -> String {
    ctx.copied_config().options().catalog.default_schema.clone()
}

fn warehouse_for_catalog(catalogs: &CatalogRegistry, catalog_name: &str) -> Result<String> {
    match catalogs.location_policy(catalog_name) {
        Some(LocationPolicy::TempFallbackAllowed { root }) => {
            root.to_str().map(str::to_string).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "catalog `{catalog_name}` has a non-UTF8 warehouse path"
                ))
            })
        }
        Some(LocationPolicy::RequireExplicitLocation | LocationPolicy::ServiceManagedLocation)
        | None => Ok(String::new()),
    }
}
