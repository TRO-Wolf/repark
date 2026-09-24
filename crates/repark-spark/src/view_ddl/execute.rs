use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Ident, ObjectName};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};
use repark_common::spark_error;
use repark_core::{CatalogRegistry, LocationPolicy, TempViewSession};
use repark_iceberg::view::{
    ViewDefinition, ViewTarget, create_or_replace_view, drop_catalog_view, list_catalog_views,
    split_view_properties, view_schema_for_output,
};

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, table_or_view_not_found};
use crate::describe_show::filter_pattern_matches;
use crate::view_ddl::parse::{
    AlterViewAction, AlterViewStatement, CreateTempViewStatement, CreateViewStatement,
    ShowTblpropertiesStatement,
    ShowViewsStatement,
};
use crate::view_ddl::read::{plan_prepared_body, prepare_view_body_sql};
use crate::view_ddl::temp_view::{
    TempViewDefinition, refuse_recursive_temp_view, replanning_temp_view,
};

pub(crate) const NO_TEMP_VIEW_HOME: &str = "CREATE TEMPORARY VIEW needs a session: this SQL \
     door runs without a session-local temp-view home, so it cannot register a temporary view. \
     Run the statement through ReparkSession::sql";

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_create_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: CreateViewStatement,
) -> Result<DataFrame> {
    let (catalog, namespace_name, view_name) = complete_view_name(catalogs, &statement.name)?;
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
pub(crate) async fn route_create_temp_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    parsed: Result<CreateTempViewStatement>,
    write_options: &crate::write_options::StatementWriteOptions,
    temp_views: Option<&dyn TempViewSession>,
) -> Result<DataFrame> {
    let statement = parsed?;
    write_options.refuse_if_non_empty("CREATE TEMPORARY VIEW")?;
    execute_create_temp_view(ctx, catalogs, statement, temp_views).await
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_create_temp_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: CreateTempViewStatement,
    temp_views: Option<&dyn TempViewSession>,
) -> Result<DataFrame> {
    let Some(temp_views) = temp_views else {
        return Err(DataFusionError::NotImplemented(
            NO_TEMP_VIEW_HOME.to_string(),
        ));
    };
    let (catalog, namespace_name) = crate::use_ddl::session_defaults(catalogs);
    let mut home = temp_views.temp_view_home().map_err(temp_view_err)?;
    home.push(temp_view_home_segment(&statement.name));
    let definition = TempViewDefinition {
        home,
        display: format!("`{}`", statement.name.value.replace('`', "``")),
        sql: statement.body_sql,
        catalog,
        namespace: NamespaceIdent::new(namespace_name),
        aliases: statement.aliases,
    };
    let view = replanning_temp_view(ctx, catalogs, definition, temp_views).await?;
    let name = temp_view_name_arg(&statement.name);
    if !statement.or_replace
        && temp_views
            .resolve_temp_view_home_ref(&name)
            .map_err(temp_view_err)?
            .is_some()
    {
        return Err(DataFusionError::Plan(spark_error::message(
            spark_error::TEMP_TABLE_OR_VIEW_ALREADY_EXISTS,
            &[("relationName", view.display())],
        )));
    }
    refuse_recursive_temp_view(ctx, &view).await?;
    let frame = ctx.read_table(view)?;
    temp_views
        .create_or_replace_temp_view_from(&name, &frame)
        .map_err(temp_view_err)?;
    ctx.read_empty()
}

pub(crate) fn temp_view_home_segment(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

pub(crate) fn temp_view_name_arg(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        format!("\"{}\"", ident.value.replace('"', "\"\""))
    } else {
        ident.value.clone()
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn temp_view_err(error: repark_common::Error) -> DataFusionError {
    match error {
        repark_common::Error::Analysis(message) => DataFusionError::Plan(message),
        other => DataFusionError::External(Box::new(other)),
    }
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
        let (catalog, namespace_name, view_name) = complete_view_name(catalogs, &parts)?;
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
    let (catalog, namespace_name, view_name) = complete_view_name(catalogs, &statement.name)?;
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
            let (target_catalog, target_namespace, target_name) =
                complete_view_name(catalogs, &target)?;
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
    execute_show_views_with(ctx, catalogs, statement, Vec::new()).await
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_show_views_with(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: ShowViewsStatement,
    mut temp_views: Vec<String>,
) -> Result<DataFrame> {
    let (namespace_name, mut views) = catalog_view_names(catalogs, &statement.namespace).await?;
    views.sort();
    temp_views.sort();
    if let Some(pattern) = statement.like.as_deref() {
        views.retain(|view| filter_pattern_matches(view, pattern));
        temp_views.retain(|view| filter_pattern_matches(view, pattern));
    }
    let rows = views
        .into_iter()
        .map(|view| (namespace_name.clone(), view, false))
        .chain(
            temp_views
                .into_iter()
                .map(|view| (String::new(), view, true)),
        )
        .collect::<Vec<_>>();
    ctx.read_batch(show_view_rows_batch(&rows)?)
}

async fn catalog_view_names(
    catalogs: &CatalogRegistry,
    namespace: &[String],
) -> Result<(String, Vec<String>)> {
    let (catalog, namespace_name) = match namespace {
        [] => {
            let (catalog, namespace_name) = crate::use_ddl::session_defaults(catalogs);
            let Some(handle) = catalogs.get(&catalog) else {
                return Ok((namespace_name, Vec::new()));
            };
            let namespace = NamespaceIdent::new(namespace_name.clone());
            if namespace_name.is_empty()
                || !handle
                    .namespace_exists(&namespace)
                    .await
                    .map_err(iceberg_err)?
            {
                return Ok((namespace_name, Vec::new()));
            }
            (catalog, namespace_name)
        }
        [namespace] => (
            crate::use_ddl::session_defaults(catalogs).0,
            namespace.clone(),
        ),
        [catalog, namespace] => (catalog.clone(), namespace.clone()),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "expected a two-part `IN <catalog.namespace>` name, got `{}`",
                namespace.join(".")
            )));
        }
    };
    let handle = catalog_handle(catalogs, &catalog)?;
    let namespace = NamespaceIdent::new(namespace_name.clone());
    let views = list_catalog_views(&catalog, handle.as_ref(), &namespace, &namespace_name).await?;
    Ok((namespace_name, views))
}

pub(crate) fn show_view_rows_batch(rows: &[(String, String, bool)]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("viewName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(namespace, _, _)| namespace.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, view, _)| view.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|(_, _, temporary)| *temporary)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?)
}

const SHOW_TBLPROPERTIES_RESERVED: [&str; 4] =
    ["location", "provider", "format-version", "comment"];

pub(crate) async fn execute_show_tblproperties(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: ShowTblpropertiesStatement,
) -> Option<Result<DataFrame>> {
    let Ok((catalog, namespace_name, view_name)) = complete_view_name(catalogs, &statement.name)
    else {
        return None;
    };
    let Ok(handle) = catalog_handle(catalogs, &catalog) else {
        return None;
    };
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
            return show_tblproperties_missing_error(
                handle.as_ref(),
                &catalog,
                &namespace_name,
                &view_name,
                &ident,
            )
            .await
            .map(Err);
        }
        Err(error) => return Some(Err(iceberg_err(error))),
    };
    let rows = show_tblproperties_rows(
        &view,
        statement.key.as_deref(),
        &catalog,
        &namespace_name,
        &view_name,
    );
    Some(show_tblproperties_batch(&rows).and_then(|batch| ctx.read_batch(batch)))
}

async fn show_tblproperties_missing_error(
    handle: &dyn Catalog,
    catalog: &str,
    namespace_name: &str,
    view_name: &str,
    ident: &TableIdent,
) -> Option<DataFusionError> {
    match handle.table_exists(ident).await {
        Ok(true) => None,
        Ok(false) => Some(table_or_view_not_found(catalog, namespace_name, view_name)),
        Err(error) => Some(iceberg_err(error)),
    }
}

pub(crate) fn show_tblproperties_rows(
    view: &iceberg::view::View,
    key: Option<&str>,
    catalog: &str,
    namespace_name: &str,
    view_name: &str,
) -> Vec<(String, String)> {
    let metadata = view.metadata();
    let mut rows = vec![
        ("location".to_string(), metadata.location().to_string()),
        ("provider".to_string(), "iceberg".to_string()),
        (
            "format-version".to_string(),
            (metadata.format_version() as u8).to_string(),
        ),
    ];
    let mut stored = metadata
        .properties()
        .iter()
        .filter(|(name, _)| !SHOW_TBLPROPERTIES_RESERVED.contains(&name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    stored.sort();
    rows.extend(stored);
    match key {
        Some(key) => {
            let value = rows.iter().find(|(name, _)| name == key).map_or_else(
                || {
                    format!(
                        "View {catalog}.{namespace_name}.{view_name} does not have \
                         property: {key}"
                    )
                },
                |(_, value)| value.clone(),
            );
            vec![(key.to_string(), value)]
        }
        None => rows,
    }
}

pub(crate) fn show_tblproperties_batch(rows: &[(String, String)]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, value)| value.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn refuse_view_write_target(
    _ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &ObjectName,
) -> Result<()> {
    let parts = name_parts(name);
    if is_metadata_table_target(&parts) {
        return Err(metadata_table_write_refusal(&parts));
    }
    let Ok((catalog, namespace_name, view_name)) = complete_view_name(catalogs, &parts) else {
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

fn complete_view_name(
    catalogs: &CatalogRegistry,
    parts: &[String],
) -> Result<(String, String, String)> {
    if is_metadata_table_target(parts) {
        return Err(metadata_table_write_refusal(parts));
    }
    let (default_catalog, default_namespace) = crate::use_ddl::session_defaults(catalogs);
    match parts {
        [view] if default_namespace.is_empty() => Err(table_or_view_not_found(
            &default_catalog,
            &default_namespace,
            view,
        )),
        [view] => Ok((default_catalog, default_namespace, view.clone())),
        [namespace, view] => Ok((default_catalog, namespace.clone(), view.clone())),
        [catalog, namespace, view] => Ok((catalog.clone(), namespace.clone(), view.clone())),
        _ => Err(DataFusionError::Plan(format!(
            "expected a [catalog.[namespace.]]view name, got `{}`",
            parts.join(".")
        ))),
    }
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
    fn commit_refuses_a_key_that_is_both_set_and_removed() {
        let view = stored_view(stored(&[("k", "v")]));
        let removals = vec!["k".to_string()];
        let Err(error) = view_properties_commit(&view, stored(&[("k", "v2")]), &removals) else {
            panic!("a key both set and removed must refuse");
        };
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External error, got {error:?}");
        };
        let source = inner
            .downcast_ref::<iceberg::Error>()
            .unwrap_or_else(|| panic!("expected an Iceberg error, got {error:?}"));
        assert_eq!(source.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.to_string(),
            "External error: DataInvalid => Cannot remove and update the same key: k"
        );
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
        assert!(matches!(error, DataFusionError::Plan(_)));
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

    #[test]
    fn show_tblproperties_rows_reserve_then_sort_stored() {
        let view = stored_view(stored(&[("k", "v"), ("a", "b"), ("provider", "fake")]));
        let rows = show_tblproperties_rows(&view, None, "sc", "ns", "v");
        assert_eq!(
            rows,
            vec![
                ("location".to_string(), "memory://ns/v".to_string()),
                ("provider".to_string(), "iceberg".to_string()),
                ("format-version".to_string(), "1".to_string()),
                ("a".to_string(), "b".to_string()),
                ("k".to_string(), "v".to_string()),
            ]
        );
    }

    #[test]
    fn show_tblproperties_rows_keyed_hits_and_misses() {
        let view = stored_view(stored(&[("k", "v")]));
        assert_eq!(
            show_tblproperties_rows(&view, Some("provider"), "sc", "ns", "v"),
            vec![("provider".to_string(), "iceberg".to_string())]
        );
        assert_eq!(
            show_tblproperties_rows(&view, Some("K"), "sc", "ns", "v"),
            vec![(
                "K".to_string(),
                "View sc.ns.v does not have property: K".to_string()
            )]
        );
    }

    #[test]
    fn show_tblproperties_batch_carries_non_nullable_key_value_strings() {
        let batch = show_tblproperties_batch(&[
            ("k".to_string(), "v".to_string()),
            ("a".to_string(), "b".to_string()),
        ])
        .unwrap_or_else(|error| panic!("batch must build: {error}"));
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 2);
        for (field, expected) in schema.fields().iter().zip(["key", "value"]) {
            assert_eq!(field.name(), expected);
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert!(!field.is_nullable());
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("key column must be Utf8"));
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("value column must be Utf8"));
        assert_eq!(keys.iter().collect::<Vec<_>>(), vec![Some("k"), Some("a")]);
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some("v"), Some("b")]
        );
    }
}
