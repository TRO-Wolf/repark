//! Spark Iceberg `CALL catalog.system.<proc>(…)` maintenance procedure router.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, Function};
use iceberg::maintenance::{DeleteOrphanFiles, PrefixMismatchMode, RewritePositionDeleteFiles};
use iceberg::transaction::{
    ApplyTransactionAction, CleanupReport, ExpireSnapshotsCleanup, Transaction,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use repark_core::{CatalogRegistry, LocationPolicy, memory_warehouse_fallback_root};

use crate::call_args::{CallArgs, bind, expr_as_i64_array};
use crate::{catalog_handle, iceberg_err, name_parts, reject_path_escape_ident, reregister};

mod add_files;
mod ancestors_of;
mod apply_partitioning;
mod branch_ops;
mod changelog;
mod compute_partition_stats;
mod compute_table_stats;
mod create_changelog_view;
mod params;
mod plan_partitioning;
mod plan_partitioning_bytes;
mod plan_partitioning_score;
mod rewrite_data_files;
mod rewrite_manifests;
pub(crate) mod rewrite_options;
mod rewrite_table_path;
mod rewrite_where;
mod run_maintenance;
mod run_maintenance_apply;

const SUPPORTED_PROCEDURES: &[&str] = &[
    "add_files",
    "ancestors_of",
    "apply_partitioning",
    "cherrypick_snapshot",
    "compute_partition_stats",
    "compute_table_stats",
    "create_changelog_view",
    "expire_snapshots",
    "fast_forward",
    "plan_partitioning",
    "publish_changes",
    "register_table",
    "rewrite_data_files",
    "rewrite_manifests",
    "remove_orphan_files",
    "rewrite_position_delete_files",
    "rewrite_table_path",
    "rollback_to_snapshot",
    "rollback_to_timestamp",
    "run_maintenance",
    "set_current_snapshot",
];

/// Execute one `CALL catalog.system.<proc>(…)` statement.
/// # Errors
/// Plan / `NotImplemented` / iceberg commit failures as [`DataFusionError`].
pub async fn execute_call(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    function: &Function,
) -> Result<DataFrame> {
    let (catalog_name, procedure) = resolve_call_target(catalogs, &function.name)?;
    let catalog = Arc::clone(catalog_handle(catalogs, &catalog_name)?);
    let args = CallArgs::parse(&function.args)?;

    match procedure.as_str() {
        "add_files" => add_files::execute_add_files(ctx, catalog, &catalog_name, &args).await,
        "ancestors_of" => {
            ancestors_of::execute_ancestors_of(ctx, catalog, &catalog_name, &args).await
        }
        "compute_table_stats" => {
            compute_table_stats::execute_compute_table_stats(ctx, catalog, &catalog_name, &args)
                .await
        }
        "compute_partition_stats" => {
            compute_partition_stats::execute_compute_partition_stats(
                ctx,
                catalog,
                &catalog_name,
                &args,
            )
            .await
        }
        "create_changelog_view" => {
            create_changelog_view::execute_create_changelog_view(ctx, catalog, &catalog_name, &args)
                .await
        }
        "rewrite_table_path" => {
            rewrite_table_path::execute_rewrite_table_path(ctx, catalog, &catalog_name, &args).await
        }
        "expire_snapshots" => execute_expire_snapshots(ctx, catalog, &catalog_name, &args).await,
        "rewrite_data_files" => {
            Box::pin(rewrite_data_files::execute_rewrite_data_files(
                ctx,
                catalog,
                &catalog_name,
                &args,
            ))
            .await
        }
        "rewrite_position_delete_files" => {
            Box::pin(execute_rewrite_position_delete_files(
                ctx,
                catalog,
                &catalog_name,
                &args,
            ))
            .await
        }
        "rewrite_manifests" => {
            rewrite_manifests::execute_rewrite_manifests(ctx, catalog, &catalog_name, &args).await
        }
        "rollback_to_snapshot" => {
            execute_rollback_to_snapshot(ctx, catalog, &catalog_name, &args).await
        }
        "fast_forward" => {
            branch_ops::execute_fast_forward(ctx, catalog, &catalog_name, &args).await
        }
        "cherrypick_snapshot" => {
            branch_ops::execute_cherrypick_snapshot(ctx, catalog, &catalog_name, &args).await
        }
        "publish_changes" => {
            branch_ops::execute_publish_changes(ctx, catalog, &catalog_name, &args).await
        }
        "set_current_snapshot" => {
            branch_ops::execute_set_current_snapshot(ctx, catalog, &catalog_name, &args).await
        }
        "rollback_to_timestamp" => {
            branch_ops::execute_rollback_to_timestamp(ctx, catalog, &catalog_name, &args).await
        }
        "remove_orphan_files" => {
            execute_remove_orphan_files(
                ctx,
                catalog,
                &catalog_name,
                catalogs.location_policy(&catalog_name),
                &args,
            )
            .await
        }
        "register_table" => execute_register_table(ctx, catalog, &catalog_name, &args).await,
        "run_maintenance" => {
            run_maintenance::execute_run_maintenance(ctx, &catalog_name, &args, catalogs).await
        }
        "plan_partitioning" => {
            plan_partitioning::execute_plan_partitioning(ctx, &catalog_name, &args, catalogs).await
        }
        "apply_partitioning" => {
            apply_partitioning::execute_apply_partitioning(ctx, &catalog_name, &args, catalogs)
                .await
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "CALL system.{other} is not supported. Supported procedures: {}.",
            SUPPORTED_PROCEDURES.join(", ")
        ))),
    }
}

fn resolve_call_target(
    catalogs: &CatalogRegistry,
    name: &datafusion::sql::sqlparser::ast::ObjectName,
) -> Result<(String, String)> {
    let parts = name_parts(name);
    match parts.as_slice() {
        [catalog, system, procedure]
            if system.eq_ignore_ascii_case("system") && !procedure.is_empty() =>
        {
            Ok((catalog.clone(), procedure.to_ascii_lowercase()))
        }
        [system, procedure] if system.eq_ignore_ascii_case("system") && !procedure.is_empty() => {
            let (default_catalog, _) = crate::use_ddl::session_defaults(catalogs);
            Ok((default_catalog, procedure.to_ascii_lowercase()))
        }
        _ => Err(DataFusionError::Plan(format!(
            "CALL expects `catalog.system.<procedure>(…)` or `system.<procedure>(…)` against \
             the current catalog, got `{name}`"
        ))),
    }
}

// === Table identity resolution ===

/// Resolve the Spark `table` string against the CALL catalog.
fn resolve_table_ident(catalog_name: &str, table_arg: &str) -> Result<TableIdent> {
    let raw_parts: Vec<&str> = table_arg.split('.').map(str::trim).collect();
    if raw_parts.iter().any(|part| part.is_empty()) {
        return Err(DataFusionError::Plan(format!(
            "CALL table `{table_arg}` must not contain empty path segments"
        )));
    }
    let parts = raw_parts;
    match parts.as_slice() {
        [namespace, table] => {
            reject_path_escape_ident(namespace, "CALL namespace")?;
            reject_path_escape_ident(table, "CALL table")?;
            Ok(TableIdent::new(
                NamespaceIdent::new((*namespace).to_string()),
                (*table).to_string(),
            ))
        }
        [catalog, namespace, table] if *catalog == catalog_name => {
            reject_path_escape_ident(namespace, "CALL namespace")?;
            reject_path_escape_ident(table, "CALL table")?;
            Ok(TableIdent::new(
                NamespaceIdent::new((*namespace).to_string()),
                (*table).to_string(),
            ))
        }
        [catalog, _, _] => Err(DataFusionError::Plan(format!(
            "CALL table `{table_arg}` catalogs as `{catalog}` but procedure is on catalog \
             `{catalog_name}` — use `namespace.table` or `{catalog_name}.namespace.table`"
        ))),
        _ => Err(DataFusionError::Plan(format!(
            "CALL table `{table_arg}` must be `namespace.table` or `catalog.namespace.table`"
        ))),
    }
}

pub(super) fn illegal_argument(message: String) -> DataFusionError {
    DataFusionError::Configuration(message)
}

fn count_as_i64(count: usize) -> Result<i64> {
    i64::try_from(count).map_err(|_| {
        DataFusionError::Plan(format!(
            "CALL result count {count} does not fit i64 (refusing to fabricate MAX)"
        ))
    })
}

fn count_as_i32(count: usize) -> Result<i32> {
    i32::try_from(count).map_err(|_| {
        DataFusionError::Plan(format!(
            "CALL result count {count} does not fit i32 (refusing to fabricate MAX)"
        ))
    })
}

/// Byte totals arrive from the fork as `u64`; Spark's column is a signed `bigint`.
fn bytes_as_i64(bytes: u64) -> Result<i64> {
    i64::try_from(bytes).map_err(|_| {
        DataFusionError::Plan(format!(
            "CALL result byte count {bytes} does not fit i64 (refusing to fabricate MAX)"
        ))
    })
}

// === expire_snapshots ===

/// Spark's six-column output, all of it (MW-1).
async fn execute_expire_snapshots(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    let bound = bind(args, params::params_for("expire_snapshots"), &[])?;
    let table_arg = bound.require_string("table")?;
    let older_than_ms = bound.optional_timestamp_ms("older_than")?;
    let retain_last = bound.optional_i32("retain_last")?;
    let snapshot_ids = match bound.get("snapshot_ids") {
        Some(expr) => expr_as_i64_array(expr, "snapshot_ids")?,
        None => Vec::new(),
    };
    bound.optional_i64("max_concurrent_deletes")?;
    bound.optional_bool("stream_results")?;
    bound.optional_bool("clean_expired_metadata")?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    let mut action = Transaction::new(&table).expire_snapshots();
    if let Some(timestamp_ms) = older_than_ms {
        action = action.expire_older_than(timestamp_ms);
    }
    if let Some(retain) = retain_last {
        if retain < 1 {
            return Err(DataFusionError::Plan(format!(
                "CALL expire_snapshots retain_last must be >= 1, got {retain}"
            )));
        }
        action = action.retain_last(retain);
    }
    for snapshot_id in snapshot_ids {
        action = action.expire_snapshot_id(snapshot_id);
    }

    let tx = Transaction::new(&table);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let cleanup = ExpireSnapshotsCleanup::new(table.file_io().clone());
    let (_committed, report) = cleanup
        .commit_and_clean(tx, catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;
    expire_result_dataframe(ctx, &report)
}

/// pins: rp-1-fork-repin/C-009
fn expire_result_dataframe(ctx: &SessionContext, report: &CleanupReport) -> Result<DataFrame> {
    // Spark declares every one of these NULLABLE, unlike its two rewrite procedures.
    let schema = Arc::new(Schema::new(vec![
        Field::new("deleted_data_files_count", DataType::Int64, true),
        Field::new("deleted_position_delete_files_count", DataType::Int64, true),
        Field::new("deleted_equality_delete_files_count", DataType::Int64, true),
        Field::new("deleted_manifest_files_count", DataType::Int64, true),
        Field::new("deleted_manifest_lists_count", DataType::Int64, true),
        Field::new("deleted_statistics_files_count", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![count_as_i64(
                report.deleted_data_files().len(),
            )?])),
            Arc::new(Int64Array::from(vec![count_as_i64(
                report.deleted_position_delete_files().len(),
            )?])),
            Arc::new(Int64Array::from(vec![count_as_i64(
                report.deleted_equality_delete_files().len(),
            )?])),
            Arc::new(Int64Array::from(vec![count_as_i64(
                report.deleted_manifests.len(),
            )?])),
            Arc::new(Int64Array::from(vec![count_as_i64(
                report.deleted_manifest_lists.len(),
            )?])),
            Arc::new(Int64Array::from(vec![count_as_i64(
                report.deleted_statistics_files.len(),
            )?])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

// === rewrite_position_delete_files ===

async fn execute_rewrite_position_delete_files(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    let bound = bind(
        args,
        params::params_for("rewrite_position_delete_files"),
        &[],
    )?;
    let table_arg = bound.require_string("table")?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let pairs = rewrite_options::extract_option_pairs(&bound, "rewrite_position_delete_files")?;
    let options = rewrite_options::parse_rpd_options(&pairs, &table)?;
    let where_predicate = match bound.optional_string("where")? {
        Some(where_sql) => Some(rewrite_where::parse_rewrite_where(
            where_sql.as_str(),
            table.metadata().current_schema(),
        )?),
        None => None,
    };

    let mut action = RewritePositionDeleteFiles::new(table);
    if let Some(size) = options.target_file_size_bytes {
        action = action.target_file_size_bytes(size);
    }
    if let Some(size) = options.min_file_size_bytes {
        action = action.min_file_size_bytes(u64::try_from(size).unwrap_or(0));
    }
    if let Some(size) = options.max_file_size_bytes {
        action = action.max_file_size_bytes(u64::try_from(size).unwrap_or(0));
    }
    if let Some(count) = options.min_input_files {
        action = action.min_input_files(count);
    }
    if let Some(size) = options.max_file_group_size_bytes {
        action = action.max_file_group_size_bytes(size);
    }
    if options.rewrite_all {
        action = action.rewrite_all(true);
    }
    if let Some(predicate) = where_predicate {
        action = action.filter(predicate);
    }
    let result = action
        .execute(catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("rewritten_delete_files_count", DataType::Int32, false),
        Field::new("added_delete_files_count", DataType::Int32, false),
        Field::new("rewritten_bytes_count", DataType::Int64, false),
        Field::new("added_bytes_count", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![count_as_i32(
                result.rewritten_delete_files_count,
            )?])),
            Arc::new(Int32Array::from(vec![count_as_i32(
                result.added_delete_files_count,
            )?])),
            Arc::new(Int64Array::from(vec![bytes_as_i64(
                result.rewritten_bytes_count,
            )?])),
            Arc::new(Int64Array::from(vec![bytes_as_i64(
                result.added_bytes_count,
            )?])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

// === remove_orphan_files ===

/// Java enforces a 24-hour orphan sweep floor at the procedure layer; the fork action does not.
const ORPHAN_OLDER_THAN_FLOOR_MS: i64 = 24 * 60 * 60 * 1000;

/// Refuse to sweep a table sitting in the shared CTAS temp-fallback root.
pub(crate) fn refuse_shared_temp_fallback_location(
    policy: Option<&LocationPolicy>,
    table_location: &str,
    table_arg: &str,
) -> Result<()> {
    let Some(LocationPolicy::TempFallbackAllowed { root }) = policy else {
        return Ok(());
    };
    let scan = normalize_orphan_scan_path(table_location);
    for segment in ["repark_ctas", "repark_ansi_ctas"] {
        let mut fallback_root = root.clone();
        fallback_root.push(segment);
        let fallback_root = normalize_lexically(&fallback_root);
        if scan_hits_fallback(&scan, &fallback_root) {
            return Err(DataFusionError::Plan(format!(
                "CALL remove_orphan_files refuses to sweep `{table_arg}`: path `{table_location}` \
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

fn s3_tables_orphan_sweep_reason(catalog_name: &str, table_arg: &str) -> String {
    format!(
        "CALL remove_orphan_files refuses to sweep `{table_arg}`: catalog `{catalog_name}` \
         is an S3 Tables catalog and table buckets do not support listing — the bucket \
         answers ListObjectsV2 with 405 MethodNotAllowed — so no listing-based orphan sweep \
         can run there. S3 Tables removes unreferenced files itself through the table \
         bucket's maintenance configuration: enable `unreferencedFileRemoval` in \
         PutTableBucketMaintenanceConfiguration instead of running this procedure."
    )
}

fn normalize_orphan_scan_path(location: &str) -> PathBuf {
    normalize_lexically(&memory_warehouse_fallback_root(location))
}

fn normalize_lexically(path: &Path) -> PathBuf {
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

fn scan_hits_fallback(scan: &Path, fallback_root: &Path) -> bool {
    scan.starts_with(fallback_root) || fallback_root.starts_with(scan)
}

/// Wall-clock millis since the epoch, for the `older_than` floor.
fn now_millis() -> Result<i64> {
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

/// Spark's one-column output: one ROW PER ORPHAN, not a summary count.
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

async fn execute_remove_orphan_files(
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
    // Spark positional order: table, older_than, location, dry_run.
    args.reject_excess_positional(4)?;
    if args.has_named("file_list_view") {
        return Err(DataFusionError::NotImplemented(
            "CALL remove_orphan_files argument `file_list_view` is not supported in v1 \
             (supported: table, older_than, location, dry_run, max_concurrent_deletes, \
             equal_schemes, equal_authorities, prefix_mismatch_mode, prefix_listing, \
             stream_results)"
                .to_string(),
        ));
    }

    let table_arg = args.require_string("table", 0)?;
    refuse_service_managed_orphan_sweep(policy.as_ref(), catalog_name, &table_arg)?;

    let older_than_ms = match args.optional_timestamp_ms("older_than", Some(1))? {
        Some(cutoff) => cutoff,
        None => now_millis()? - 3 * 86_400_000,
    };

    // Java's floor, same threshold, same reason (see ORPHAN_OLDER_THAN_FLOOR_MS).
    let floor_ms = now_millis()? - ORPHAN_OLDER_THAN_FLOOR_MS;
    if older_than_ms > floor_ms {
        return Err(DataFusionError::Plan(
            "CALL remove_orphan_files refuses an `older_than` less than 24 hours in the past. A \
             short interval can delete files an in-flight commit has written but not yet \
             referenced, which corrupts the table. This matches Apache Spark's own floor."
                .to_string(),
        ));
    }

    let location = args.optional_string("location")?;
    let dry_run = args.optional_bool("dry_run", Some(3))?.unwrap_or(false);
    args.optional_i64("max_concurrent_deletes", None)?;
    args.optional_bool("stream_results", None)?;
    args.optional_bool("prefix_listing", None)?;
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
    refuse_shared_temp_fallback_location(policy.as_ref(), table.metadata().location(), &table_arg)?;
    if let Some(scan_location) = location.as_deref() {
        refuse_shared_temp_fallback_location(policy.as_ref(), scan_location, &table_arg)?;
    }

    let mut action = DeleteOrphanFiles::new(table).older_than(older_than_ms);
    if let Some(location) = location {
        action = action.location(location);
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
        // The fork returns the full orphan list regardless of the deleter.
        action = action.delete_with(|_path| Box::pin(async { Ok(()) }));
    }
    let result = action.execute().await.map_err(iceberg_err)?;

    // Never report a partial delete as a success.
    if let Some(first) = result.delete_failures.first() {
        return Err(DataFusionError::Execution(format!(
            "CALL remove_orphan_files deleted {deleted} of {total} orphan files; {failed} could \
             not be removed. First failure: `{path}` — {error}. Re-run to retry the remainder.",
            deleted = result.orphan_file_locations.len() - result.delete_failures.len(),
            total = result.orphan_file_locations.len(),
            failed = result.delete_failures.len(),
            path = first.path,
            error = first.error,
        )));
    }

    orphan_result_dataframe(ctx, &result.orphan_file_locations)
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

// === rollback_to_snapshot ===

async fn execute_rollback_to_snapshot(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "snapshot_id"])?;
    // Spark arity: table, snapshot_id
    args.reject_excess_positional(2)?;
    let table_arg = args.require_string("table", 0)?;
    let snapshot_id = args.optional_i64("snapshot_id", Some(1))?.ok_or_else(|| {
        DataFusionError::Plan(
            "CALL rollback_to_snapshot requires `snapshot_id` (named or positional #1)".to_string(),
        )
    })?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let previous_snapshot_id = table.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "table `{table_arg}` has no current snapshot to roll back from"
        ))
    })?;

    // Fork R98: ManageSnapshotsAction::rollback_to.
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().rollback_to(snapshot_id);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let committed = tx.commit(catalog.as_ref()).await.map_err(iceberg_err)?;
    let current_snapshot_id = committed.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan("rollback committed but table has no current snapshot".to_string())
    })?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("previous_snapshot_id", DataType::Int64, false),
        Field::new("current_snapshot_id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![previous_snapshot_id])),
            Arc::new(Int64Array::from(vec![current_snapshot_id])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

// === register_table ===

/// Spark Iceberg `CALL system.register_table(table, metadata_file)`.
/// # Errors
/// # Errors Plan errors for missing/empty arguments; catalog errors via [`crate::iceberg_err`].
async fn execute_register_table(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "metadata_file"])?;
    args.reject_excess_positional(2)?;
    let table_arg = args.require_string("table", 0)?;
    let metadata_file = args.require_string("metadata_file", 1)?;
    if metadata_file.trim().is_empty() {
        return Err(DataFusionError::Plan(
            "CALL register_table requires a non-empty `metadata_file`".to_string(),
        ));
    }

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog
        .register_table(&ident, metadata_file)
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;

    let snapshot = table.metadata().current_snapshot();
    let current_snapshot_id = snapshot.map(|snap| snap.snapshot_id());
    let total_records_count = snapshot.and_then(|snap| summary_i64(snap, "total-records"));
    let total_data_files_count = snapshot.and_then(|snap| summary_i64(snap, "total-data-files"));

    let schema = Arc::new(Schema::new(vec![
        Field::new("current_snapshot_id", DataType::Int64, true),
        Field::new("total_records_count", DataType::Int64, true),
        Field::new("total_data_files_count", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![current_snapshot_id])),
            Arc::new(Int64Array::from(vec![total_records_count])),
            Arc::new(Int64Array::from(vec![total_data_files_count])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

/// Snapshot-summary integer, or none if the key is absent or not an i64.
fn summary_i64(snapshot: &iceberg::spec::Snapshot, key: &str) -> Option<i64> {
    snapshot
        .summary()
        .additional_properties
        .get(key)
        .and_then(|raw| raw.parse::<i64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    fn object_name(parts: &[&str]) -> ObjectName {
        ObjectName(
            parts
                .iter()
                .map(|part| ObjectNamePart::Identifier(Ident::new(*part)))
                .collect(),
        )
    }

    #[test]
    fn resolve_call_target_requires_system_middle() {
        let catalogs = CatalogRegistry::new();
        let ok = resolve_call_target(
            &catalogs,
            &object_name(&["ice", "system", "expire_snapshots"]),
        )
        .unwrap();
        assert_eq!(ok.0, "ice");
        assert_eq!(ok.1, "expire_snapshots");
        assert!(
            resolve_call_target(&catalogs, &object_name(&["ice", "expire_snapshots"])).is_err()
        );
        assert!(
            resolve_call_target(&catalogs, &object_name(&["ice", "sys", "expire_snapshots"]))
                .is_err()
        );
    }

    #[test]
    fn resolve_call_target_two_part_uses_current_catalog() {
        let ctx = SessionContext::new();
        let catalogs = CatalogRegistry::new();
        crate::use_ddl::set_session_defaults(&ctx, &catalogs, "ice", "sales");
        let ok =
            resolve_call_target(&catalogs, &object_name(&["system", "expire_snapshots"])).unwrap();
        assert_eq!(ok.0, "ice");
        assert_eq!(ok.1, "expire_snapshots");
        let ok =
            resolve_call_target(&catalogs, &object_name(&["SYSTEM", "Expire_Snapshots"])).unwrap();
        assert_eq!(ok.0, "ice");
        assert_eq!(ok.1, "expire_snapshots");
    }

    #[test]
    fn resolve_table_ident_two_and_three_part() {
        let ident = resolve_table_ident("ice", "sales.t").unwrap();
        assert_eq!(ident.namespace().as_ref(), &vec!["sales".to_string()]);
        assert_eq!(ident.name(), "t");
        let ident = resolve_table_ident("ice", "ice.sales.t").unwrap();
        assert_eq!(ident.name(), "t");
        assert!(resolve_table_ident("ice", "other.sales.t").is_err());
        assert!(resolve_table_ident("ice", "t").is_err());
        // C1-SEC-001: path-escape + empty-segment refuse (CTAS parity; no silent collapse).
        assert!(resolve_table_ident("ice", "sales/evil.t").is_err());
        assert!(resolve_table_ident("ice", "sales...t").is_err());
        assert!(resolve_table_ident("ice", "..sales.t").is_err());
        assert!(resolve_table_ident("ice", "../sales.t").is_err());
        assert!(resolve_table_ident("ice", "sales.t/evil").is_err());
    }
}
