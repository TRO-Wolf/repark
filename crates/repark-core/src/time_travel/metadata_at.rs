use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_iceberg::catalog::{
    MetadataAsofMode, SnapshotMetadataTableProvider, metadata_asof_mode,
    snapshot_scope_refusal_text,
};

use super::{TimeTravelSpec, next_temp_view_name, resolve_snapshot_id};
use crate::SessionTimeZone;
use crate::catalog_state::CatalogRegistry;

#[allow(clippy::missing_errors_doc)]
pub fn provider_for_spec(
    table: Table,
    metadata_type: MetadataTableType,
    spec: &TimeTravelSpec,
    zone: &SessionTimeZone,
) -> Result<Arc<dyn TableProvider>> {
    if metadata_asof_mode(&metadata_type) == MetadataAsofMode::RefuseSnapshotScope {
        return Err(DataFusionError::Plan(snapshot_scope_refusal_text(
            &metadata_type,
        )));
    }
    if metadata_asof_mode(&metadata_type) == MetadataAsofMode::ServeCurrent {
        resolve_snapshot_id(table.metadata(), spec, zone)?;
        let provider = SnapshotMetadataTableProvider::try_new_current(table, metadata_type)?;
        return Ok(Arc::new(provider));
    }
    if let TimeTravelSpec::SnapshotId(snapshot_id) = spec
        && table.metadata().snapshot_by_id(*snapshot_id).is_none()
    {
        let provider = SnapshotMetadataTableProvider::try_new_empty(table, metadata_type)?;
        return Ok(Arc::new(provider));
    }
    let resolved = resolve_snapshot_id(table.metadata(), spec, zone)?;
    let provider = SnapshotMetadataTableProvider::try_new_scoped(table, metadata_type, resolved)?;
    Ok(Arc::new(provider))
}

#[must_use]
pub fn read_sql(table_name: &str, parts: &[String]) -> String {
    if split_metadata_path(parts).is_none() {
        return format!("SELECT * FROM {table_name}");
    }
    let quoted = parts
        .iter()
        .map(|part| quoted_ident(part))
        .collect::<Vec<_>>();
    format!("SELECT * FROM {}", quoted.join("."))
}

#[allow(clippy::missing_errors_doc)]
pub async fn read_metadata_path_at(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_parts: &[String],
    spec: &TimeTravelSpec,
    zone: &SessionTimeZone,
) -> Result<Option<DataFrame>> {
    if !matches!(
        spec,
        TimeTravelSpec::SnapshotId(_)
            | TimeTravelSpec::VersionRef(_)
            | TimeTravelSpec::TimestampMs(_)
    ) {
        return Ok(None);
    }
    let Some((base_parts, metadata_type)) = split_metadata_path(table_parts) else {
        return Ok(None);
    };
    if table_exists_parts(catalogs, table_parts).await? {
        return Ok(None);
    }
    if !table_exists_parts(catalogs, &base_parts).await? {
        return Ok(None);
    }
    let table = super::load_iceberg_table(catalogs, &base_parts).await?;
    let provider = provider_for_spec(table, metadata_type, spec, zone)?;
    let temp_name = next_temp_view_name();
    let qualified = format!("datafusion.public.{temp_name}");
    let catalog = ctx.catalog("datafusion").ok_or_else(|| {
        DataFusionError::Plan(format!(
            "no session catalog `datafusion` for time-travel temp view (have {:?})",
            ctx.catalog_names()
        ))
    })?;
    let schema = catalog.schema("public").ok_or_else(|| {
        DataFusionError::Plan("no schema `datafusion.public` for time-travel temp view".to_string())
    })?;
    let _ = schema.deregister_table(&temp_name);
    schema
        .register_table(temp_name.clone(), provider)
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register time-travel temp view {qualified}: {error}"
            ))
        })?;
    let frame = ctx.table(qualified.as_str()).await.map_err(|error| {
        DataFusionError::Plan(format!(
            "time-travel temp view {qualified} unresolved: {error}"
        ))
    })?;
    Ok(Some(frame))
}

fn split_metadata_path(parts: &[String]) -> Option<(Vec<String>, MetadataTableType)> {
    let [catalog, namespace, table, last] = parts else {
        return None;
    };
    let lowered = last.to_ascii_lowercase();
    let metadata_type = MetadataTableType::try_from(lowered.as_str()).ok()?;
    Some((
        vec![catalog.clone(), namespace.clone(), table.clone()],
        metadata_type,
    ))
}

fn quoted_ident(part: &str) -> String {
    format!("\"{}\"", part.replace('"', "\"\""))
}

async fn table_exists_parts(catalogs: &CatalogRegistry, parts: &[String]) -> Result<bool> {
    if parts.len() < 2 {
        return Ok(false);
    }
    let Some(catalog) = catalogs.get(&parts[0]) else {
        return Ok(false);
    };
    let rest = &parts[1..];
    if rest.is_empty() {
        return Ok(false);
    }
    let Some(ident) = table_ident_from_parts(rest) else {
        return Ok(false);
    };
    match catalog.table_exists(&ident).await {
        Ok(exists) => Ok(exists),
        Err(error) => table_exists_probe_from_error(&ident, error),
    }
}

fn table_ident_from_parts(parts: &[String]) -> Option<TableIdent> {
    match parts {
        [] => None,
        [_table] => None,
        [namespace, table] => Some(TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        )),
        multi => {
            let table = multi.last()?.clone();
            let namespace_parts: Vec<String> = multi[..multi.len() - 1].to_vec();
            let namespace = NamespaceIdent::from_vec(namespace_parts).ok()?;
            Some(TableIdent::new(namespace, table))
        }
    }
}

fn table_exists_probe_from_error(ident: &TableIdent, error: iceberg::Error) -> Result<bool> {
    match error.kind() {
        ErrorKind::NamespaceNotFound | ErrorKind::TableNotFound => Ok(false),
        ErrorKind::DataInvalid if ident.namespace().as_ref().len() > 1 => Ok(false),
        _ => Err(super::iceberg_err(error)),
    }
}
