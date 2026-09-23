use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::ObjectName;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};
use repark_common::spark_error;
use repark_core::{CatalogRegistry, LocationPolicy};
use repark_iceberg::view::{
    ViewDefinition, ViewTarget, create_or_replace_view, drop_catalog_view, list_catalog_views,
    split_view_properties, view_schema_for_output,
};

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, table_or_view_not_found};
use crate::describe_show::filter_pattern_matches;
use crate::view_ddl::parse::{
    AlterViewAction, AlterViewStatement, CreateViewStatement, ShowViewsStatement,
};
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
pub(crate) async fn execute_alter_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: AlterViewStatement,
) -> Result<DataFrame> {
    let (catalog, namespace_name, view_name) = complete_view_name(ctx, &statement.name)?;
    let handle = catalog_handle(catalogs, &catalog)?;
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace_name.clone()),
        view_name.clone(),
    );
    let view = match handle.load_view(&ident).await {
        Ok(view) => view,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ViewNotFound | ErrorKind::FeatureUnsupported
            ) =>
        {
            return alter_view_missing_target(
                handle.as_ref(),
                &catalog,
                &namespace_name,
                &view_name,
                &ident,
                &statement.action,
            )
            .await;
        }
        Err(error) => return Err(iceberg_err(error)),
    };
    match statement.action {
        AlterViewAction::SetProperties(properties) => {
            let commit = view_properties_commit(&view, properties, &[])?;
            handle.update_view(commit).await.map_err(iceberg_err)?;
        }
        AlterViewAction::UnsetProperties { keys, if_exists } => {
            let removals = select_unset_removals(&keys, if_exists, view.metadata().properties())?;
            if !removals.is_empty() {
                let commit = view_properties_commit(&view, HashMap::new(), &removals)?;
                handle.update_view(commit).await.map_err(iceberg_err)?;
            }
        }
        AlterViewAction::RenameTo(target) => {
            let (target_catalog, target_namespace, target_name) = complete_view_name(ctx, &target)?;
            if target_catalog != catalog {
                return Err(DataFusionError::Plan(format!(
                    "Cannot move view between catalogs: from={catalog} and to={target_catalog}"
                )));
            }
            let destination = TableIdent::new(
                NamespaceIdent::new(target_namespace.clone()),
                target_name.clone(),
            );
            match handle.view_exists(&destination).await {
                Ok(true) => {
                    let relation = format!("{target_namespace}.{target_name}");
                    return Err(DataFusionError::Plan(spark_error::message(
                        spark_error::VIEW_ALREADY_EXISTS,
                        &[("relationName", relation.as_str())],
                    )));
                }
                Ok(false) => {}
                Err(error) => return Err(iceberg_err(error)),
            }
            handle
                .rename_view(&ident, &destination)
                .await
                .map_err(iceberg_err)?;
        }
    }
    ctx.read_empty()
}

async fn alter_view_missing_target(
    handle: &dyn Catalog,
    catalog: &str,
    namespace_name: &str,
    view_name: &str,
    ident: &TableIdent,
    action: &AlterViewAction,
) -> Result<DataFrame> {
    if matches!(action, AlterViewAction::RenameTo(_)) {
        let table_exists = handle.table_exists(ident).await.map_err(iceberg_err)?;
        if table_exists {
            return Err(DataFusionError::Plan(
                "Cannot rename a table with ALTER VIEW. Please use ALTER TABLE instead."
                    .to_string(),
            ));
        }
        return Err(table_or_view_not_found(catalog, namespace_name, view_name));
    }
    Err(DataFusionError::Plan(spark_error::message(
        spark_error::UNSUPPORTED_FEATURE_CATALOG_OPERATION,
        &[("catalogName", catalog)],
    )))
}

fn select_unset_removals(
    keys: &[String],
    if_exists: bool,
    stored: &HashMap<String, String>,
) -> Result<Vec<String>> {
    let mut removals = Vec::new();
    for key in keys {
        if stored.contains_key(key) {
            removals.push(key.clone());
        } else if !if_exists {
            return Err(DataFusionError::Plan(format!(
                "Cannot remove property that is not set: '{key}'"
            )));
        }
    }
    Ok(removals)
}

fn view_properties_commit(
    view: &iceberg::view::View,
    sets: HashMap<String, String>,
    removals: &[String],
) -> Result<iceberg::view::ViewCommit> {
    let mut update = view.update_properties();
    for (key, value) in sets {
        update = update.set(key, value).map_err(iceberg_err)?;
    }
    for key in removals {
        update = update.remove(key.clone()).map_err(iceberg_err)?;
    }
    update.to_commit().map_err(iceberg_err)
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

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, SqlViewRepresentation, Type,
        ViewMetadataBuilder, ViewRepresentation, ViewRepresentations,
    };
    use iceberg::view::View;
    use iceberg::{ViewCreation, ViewUpdate};

    fn stored_view(properties: HashMap<String, String>) -> View {
        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap_or_else(|error| panic!("schema must build: {error}"));
        let creation = ViewCreation::builder()
            .name("v".to_string())
            .location("memory://ns/v".to_string())
            .representations(ViewRepresentations::new(vec![ViewRepresentation::Sql(
                SqlViewRepresentation {
                    sql: "SELECT 1".to_string(),
                    dialect: "spark".to_string(),
                },
            )]))
            .schema(schema)
            .properties(properties)
            .default_namespace(NamespaceIdent::new("ns".to_string()))
            .build();
        let metadata = ViewMetadataBuilder::from_view_creation(creation)
            .unwrap_or_else(|error| panic!("metadata builder: {error}"))
            .build()
            .unwrap_or_else(|error| panic!("metadata must build: {error}"))
            .metadata;
        View::builder()
            .file_io(FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("ns".to_string()),
                "v".to_string(),
            ))
            .build()
            .unwrap_or_else(|error| panic!("view must build: {error}"))
    }

    fn stored(properties: &[(&str, &str)]) -> HashMap<String, String> {
        properties
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn set_commit_carries_the_property_updates() {
        let view = stored_view(HashMap::new());
        let mut commit = view_properties_commit(&view, stored(&[("k", "v"), ("n", "1")]), &[])
            .unwrap_or_else(|error| panic!("commit must build: {error}"));
        let updates = commit.take_updates();
        let [ViewUpdate::SetProperties { updates }] = updates.as_slice() else {
            panic!("expected a single SetProperties update, got {updates:?}");
        };
        assert_eq!(
            updates,
            &stored(&[("k", "v"), ("n", "1")]),
            "pins: ice-views-1/C-017"
        );
    }

    #[test]
    fn unset_commit_carries_the_removals() {
        let view = stored_view(stored(&[("k", "v"), ("k2", "w")]));
        let removals = vec!["k".to_string()];
        let mut commit = view_properties_commit(&view, HashMap::new(), &removals)
            .unwrap_or_else(|error| panic!("commit must build: {error}"));
        let updates = commit.take_updates();
        let [ViewUpdate::RemoveProperties { removals }] = updates.as_slice() else {
            panic!("expected a single RemoveProperties update, got {updates:?}");
        };
        assert_eq!(removals, &vec!["k".to_string()], "pins: ice-views-1/C-017");
    }

    #[test]
    fn unset_selection_keeps_present_keys_in_statement_order() {
        let stored = stored(&[("k", "v"), ("n", "1")]);
        let keys = vec!["n".to_string(), "k".to_string()];
        let removals = select_unset_removals(&keys, false, &stored)
            .unwrap_or_else(|error| panic!("selection must succeed: {error}"));
        assert_eq!(removals, vec!["n".to_string(), "k".to_string()]);
    }

    #[test]
    fn unset_selection_refuses_the_first_missing_key_without_if_exists() {
        let stored = stored(&[("k", "v")]);
        let keys = vec!["k".to_string(), "nope".to_string(), "also_gone".to_string()];
        let Err(error) = select_unset_removals(&keys, false, &stored) else {
            panic!("must refuse")
        };
        assert_eq!(
            error.to_string(),
            "Error during planning: Cannot remove property that is not set: 'nope'",
            "pins: ice-views-1/C-017"
        );
    }

    #[test]
    fn unset_selection_skips_missing_keys_with_if_exists() {
        let stored = stored(&[("k", "v")]);
        let keys = vec!["nope".to_string(), "k".to_string(), "also_gone".to_string()];
        let removals = select_unset_removals(&keys, true, &stored)
            .unwrap_or_else(|error| panic!("selection must succeed: {error}"));
        assert_eq!(removals, vec!["k".to_string()]);
        let keys = vec!["nope".to_string()];
        let removals = select_unset_removals(&keys, true, &stored)
            .unwrap_or_else(|error| panic!("selection must succeed: {error}"));
        assert!(removals.is_empty());
    }
}
