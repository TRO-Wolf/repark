use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::Expr;
use iceberg::maintenance::{DeleteOrphanFiles, PrefixMismatchMode};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{LocationPolicy, memory_warehouse_fallback_root};

use super::orphan_file_list::{FileListRequest, listed_orphans, normalize_location_path};
use super::{CallArgs, resolve_table_ident, rewrite_options};
use crate::iceberg_err;

const ORPHAN_OLDER_THAN_FLOOR_MS: i64 = 24 * 60 * 60 * 1000;

#[allow(clippy::missing_errors_doc)]
pub(crate) fn refuse_shared_temp_fallback_location(
    policy: Option<&LocationPolicy>,
    scan_location: &str,
    table_arg: &str,
) -> Result<()> {
    let Some(LocationPolicy::TempFallbackAllowed { root }) = policy else {
        return Ok(());
    };
    let scan = normalize_orphan_scan_path(scan_location);
    for segment in ["repark_ctas", "repark_ansi_ctas"] {
        let fallback_root = normalize_lexically(&root.join(segment));
        if fallback_root.starts_with(&scan) {
            return Err(DataFusionError::Plan(format!(
                "CALL remove_orphan_files refuses to sweep `{table_arg}`: path `{scan_location}` \
                 sits in or contains the shared CTAS fallback root `{}`. That path is derived \
                 from the catalog, namespace and table NAME alone, so any other process using \
                 the same names writes to the same directory — and this procedure deletes \
                 whatever the table's own metadata does not reference, which would include \
                 another session's live files. Re-create the namespace with an explicit location \
                 (`CREATE NAMESPACE <catalog>.<namespace> LOCATION '<path>'`) so the table owns \
                 its directory, then sweep it.",
                fallback_root.display()
            )));
        }
    }
    Ok(())
}

async fn refuse_scan_over_other_tables(
    policy: Option<&LocationPolicy>,
    catalog: &dyn Catalog,
    catalog_name: &str,
    swept: &TableIdent,
    scan_location: &str,
    table_arg: &str,
) -> Result<()> {
    if !matches!(policy, Some(LocationPolicy::TempFallbackAllowed { .. })) {
        return Ok(());
    }
    let scan = normalize_orphan_scan_path(scan_location);
    let mut pending = catalog.list_namespaces(None).await.map_err(iceberg_err)?;
    let mut seen: HashSet<NamespaceIdent> = HashSet::new();
    while let Some(namespace) = pending.pop() {
        if !seen.insert(namespace.clone()) {
            continue;
        }
        pending.extend(
            catalog
                .list_namespaces(Some(&namespace))
                .await
                .map_err(iceberg_err)?,
        );
        for ident in catalog.list_tables(&namespace).await.map_err(iceberg_err)? {
            if &ident == swept {
                continue;
            }
            let other = catalog.load_table(&ident).await.map_err(iceberg_err)?;
            let other_location = other.metadata().location();
            if normalize_orphan_scan_path(other_location).starts_with(&scan) {
                return Err(DataFusionError::Plan(format!(
                    "CALL remove_orphan_files refuses to sweep `{table_arg}`: path \
                     `{scan_location}` holds table `{catalog_name}.{}.{}` at `{other_location}`. \
                     This procedure deletes every file the swept table's own metadata does not \
                     reference, which would include that table's live files. Sweep a path that \
                     holds only this table's files.",
                    ident.namespace().as_ref().join("."),
                    ident.name()
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn refuse_service_managed_orphan_sweep(
    policy: Option<&LocationPolicy>,
    catalog_name: &str,
    table_arg: &str,
) -> Result<()> {
    if matches!(policy, Some(LocationPolicy::ServiceManagedLocation)) {
        return Err(DataFusionError::Plan(s3_tables_orphan_sweep_reason(
            catalog_name,
            table_arg,
        )));
    }
    Ok(())
}

pub(super) fn s3_tables_orphan_sweep_reason(catalog_name: &str, table_arg: &str) -> String {
    format!(
        "CALL remove_orphan_files refuses to sweep `{table_arg}`: catalog `{catalog_name}` \
         is an S3 Tables catalog and table buckets do not support listing — the bucket \
         answers ListObjectsV2 with 405 MethodNotAllowed — so no listing-based orphan sweep \
         can run there. S3 Tables removes unreferenced files itself through the table \
         bucket's maintenance configuration: enable `unreferencedFileRemoval` in \
         PutTableBucketMaintenanceConfiguration instead of running this procedure."
    )
}

pub(super) fn normalize_orphan_scan_path(location: &str) -> PathBuf {
    normalize_lexically(&memory_warehouse_fallback_root(location))
}

pub(super) fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

pub(super) fn now_millis() -> Result<i64> {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| {
            DataFusionError::Execution(
                "system clock is before the Unix epoch, so the remove_orphan_files floor cannot \
                 be evaluated — refusing rather than deleting against an unknown cutoff"
                    .to_string(),
            )
        })?;
    i64::try_from(since_epoch.as_millis()).map_err(|_| {
        DataFusionError::Execution(
            "system clock is beyond the representable millisecond range — refusing rather than \
             deleting against an unknown cutoff"
                .to_string(),
        )
    })
}

fn orphan_result_dataframe(ctx: &SessionContext, locations: &[String]) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "orphan_file_location",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(locations.to_vec()))],
    )?;
    ctx.read_batches(vec![batch])
}

fn refuse_partial_delete(total: usize, failures: &[(String, String)]) -> Result<()> {
    let Some((path, error)) = failures.first() else {
        return Ok(());
    };
    Err(DataFusionError::Execution(format!(
        "CALL remove_orphan_files deleted {deleted} of {total} orphan files; {failed} could \
         not be removed. First failure: `{path}` — {error}. Re-run to retry the remainder.",
        deleted = total - failures.len(),
        failed = failures.len(),
    )))
}

pub(super) async fn execute_remove_orphan_files(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    policy: Option<LocationPolicy>,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&[
        "table",
        "older_than",
        "location",
        "dry_run",
        "max_concurrent_deletes",
        "file_list_view",
        "equal_schemes",
        "equal_authorities",
        "prefix_mismatch_mode",
        "prefix_listing",
        "stream_results",
    ])?;
    args.reject_excess_positional(4)?;

    let table_arg = args.require_string("table", 0)?;
    refuse_service_managed_orphan_sweep(policy.as_ref(), catalog_name, &table_arg)?;

    let older_than_ms = match args.optional_timestamp_ms("older_than", Some(1))? {
        Some(cutoff) => cutoff,
        None => now_millis()? - 3 * 86_400_000,
    };
    let floor_ms = now_millis()? - ORPHAN_OLDER_THAN_FLOOR_MS;
    if older_than_ms > floor_ms {
        return Err(DataFusionError::Plan(
            "CALL remove_orphan_files refuses an `older_than` less than 24 hours in the past. A \
             short interval can delete files an in-flight commit has written but not yet \
             referenced, which corrupts the table. This matches Apache Spark's own floor."
                .to_string(),
        ));
    }

    let location = args.optional_string_at("location", Some(2))?;
    let dry_run = args.optional_bool("dry_run", Some(3))?.unwrap_or(false);
    args.optional_i64("max_concurrent_deletes", None)?;
    args.optional_bool("stream_results", None)?;
    args.optional_bool("prefix_listing", None)?;
    let file_list_view = args.optional_string("file_list_view")?;
    let prefix_mismatch_mode = match args.optional_string("prefix_mismatch_mode")? {
        Some(raw) => Some(
            PrefixMismatchMode::from_string(&raw)
                .map_err(|error| DataFusionError::Plan(error.message().to_string()))?,
        ),
        None => None,
    };
    let equal_schemes = orphan_string_map(args, "equal_schemes")?;
    let equal_authorities = orphan_string_map(args, "equal_authorities")?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let scan_location = location
        .clone()
        .unwrap_or_else(|| table.metadata().location().to_string());
    refuse_shared_temp_fallback_location(policy.as_ref(), &scan_location, &table_arg)?;
    refuse_scan_over_other_tables(
        policy.as_ref(),
        catalog.as_ref(),
        catalog_name,
        table.identifier(),
        &scan_location,
        &table_arg,
    )
    .await?;

    if let Some(view) = file_list_view {
        let request = FileListRequest {
            view,
            scope: scan_location,
            older_than_ms,
            prefix_mismatch_mode: prefix_mismatch_mode.unwrap_or(PrefixMismatchMode::Error),
            equal_schemes: equal_schemes.unwrap_or_default(),
            equal_authorities: equal_authorities.unwrap_or_default(),
        };
        let orphans = listed_orphans(ctx, &table, &request).await?;
        if !dry_run {
            delete_listed_orphans(&table, &orphans).await?;
        }
        return orphan_result_dataframe(ctx, &orphans);
    }

    let mut action = DeleteOrphanFiles::new(table).older_than(older_than_ms);
    if let Some(location) = location {
        action = action.location(normalize_location_path(&location));
    }
    if let Some(mode) = prefix_mismatch_mode {
        action = action.prefix_mismatch_mode(mode);
    }
    if let Some(schemes) = equal_schemes {
        action = action.equal_schemes(schemes);
    }
    if let Some(authorities) = equal_authorities {
        action = action.equal_authorities(authorities);
    }
    if dry_run {
        action = action.delete_with(|_path| Box::pin(async { Ok(()) }));
    }
    let result = action.execute().await.map_err(iceberg_err)?;
    let failures: Vec<(String, String)> = result
        .delete_failures
        .iter()
        .map(|failure| (failure.path.clone(), failure.error.to_string()))
        .collect();
    refuse_partial_delete(result.orphan_file_locations.len(), &failures)?;
    orphan_result_dataframe(ctx, &result.orphan_file_locations)
}

async fn delete_listed_orphans(table: &Table, orphans: &[String]) -> Result<()> {
    let file_io = table.file_io();
    let mut failures = Vec::new();
    for path in orphans {
        if let Err(error) = file_io.delete(path).await {
            failures.push((path.clone(), error.to_string()));
        }
    }
    refuse_partial_delete(orphans.len(), &failures)
}

fn orphan_string_map(args: &CallArgs, name: &str) -> Result<Option<HashMap<String, String>>> {
    let Some(expr) = args.named.get(name) else {
        return Ok(None);
    };
    let Expr::Function(function) = expr else {
        return Err(DataFusionError::Plan(format!(
            "CALL remove_orphan_files argument `{name}` must be map(k, v, …), got {expr}"
        )));
    };
    if !function.name.to_string().eq_ignore_ascii_case("map") {
        return Err(DataFusionError::Plan(format!(
            "CALL remove_orphan_files argument `{name}` must be map(k, v, …), got {expr}"
        )));
    }
    let mut out = HashMap::new();
    for (key, value) in
        rewrite_options::pairs_from_map_args(&function.args, "remove_orphan_files", name)?
    {
        let Some(value) = value else {
            return Err(DataFusionError::Plan(format!(
                "CALL remove_orphan_files argument `{name}` value for `{key}` must be a string \
                 literal, got NULL"
            )));
        };
        out.insert(key, value);
    }
    Ok(Some(out))
}
