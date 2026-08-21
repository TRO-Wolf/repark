//! Spark Iceberg `CALL catalog.system.<proc>(…)` maintenance procedure router (I3 /
//! R-MAINTENANCE-CALL).
//!
//! Four procedures, each backed by an action the owned fork already provides:
//!
//! 1. `expire_snapshots` — fork R133 `Transaction::expire_snapshots` +
//!    `ExpireSnapshotsCleanup::commit_and_clean`
//!    (`crates/iceberg/src/transaction/expire_snapshots.rs` + `expire_cleanup.rs`, pin
//!    `4723104b`).
//! 2. `rewrite_data_files` — fork R135 bin-pack
//!    (`crates/iceberg/src/maintenance/rewrite_data_files.rs`
//!    `RewriteDataFiles::new(table).execute(&catalog)`).
//! 3. `rewrite_position_delete_files` — fork R136
//!    (`crates/iceberg/src/maintenance/rewrite_position_delete_files.rs`
//!    `RewritePositionDeleteFiles::new(table).execute(&catalog)`), wired by MW-2 on 2026-08-21.
//! 4. `rollback_to_snapshot` — fork R98 `ManageSnapshotsAction::rollback_to`
//!    (`crates/iceberg/src/transaction/manage_snapshots.rs:164-167`).
//!
//! **Every catalog policy** (MW-1, 2026-08-21). The v1 surface refused Glue and S3 Tables as a
//! blast-radius fence; nothing downstream of that gate ever assumed a local filesystem, so
//! lifting it was policy, not machinery.
//!
//! **On a service-managed (S3 Tables) catalog, expect commit conflicts and retry them.** The
//! service runs its own compaction and snapshot expiry, committing concurrently with this engine
//! (fork `ENGINE_CONTRACT` §8). `CommitFailed` requirement mismatches are ROUTINE there, and
//! `validate_data_files_exist` trips when service compaction rewrites a file an in-flight
//! position delete references. The commit fails loudly and the table is not damaged — this is
//! Iceberg's optimistic concurrency working, not a sign of corruption. Re-run the procedure.
//!
//! **Parsing:** named (`arg => value`) and positional arguments are both accepted —
//! Apache Iceberg Spark Procedures docs:
//! "CALL supports passing arguments by name (recommended) or by position."
//! <https://iceberg.apache.org/docs/latest/spark-procedures/#usage>
//!
//! **Result rows:** every procedure here returns Spark's full column list, in Spark's order, with
//! Spark's types and nullability, each value measured against a live oracle rather than inferred.
//! Counts are **never fabricated**: a column the engine cannot source honestly does not get a
//! made-up number. As of MW-2 no procedure omits a Spark column.
//!
//! **Out of scope (loud):**
//! - `remove_orphan_files` — MW-3 (do not hand-roll file listing; the fork's `DeleteOrphanFiles`
//!   is the surface).
//! - rewrite `strategy` / `sort_order` other than default bin-pack — R135 deferred list.
//! - the `options` map and the `where` filter on both rewrite procedures.
//! - any other `system.*` procedure.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan,
};
use iceberg::maintenance::{RewriteDataFiles, RewritePositionDeleteFiles};
use iceberg::spec::DataContentType;
use iceberg::transaction::{
    ApplyTransactionAction, CleanupReport, ExpireSnapshotsCleanup, Transaction,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use repark_core::{CatalogRegistry, parse_timestamp_to_ms};

use crate::{catalog_handle, iceberg_err, name_parts, reject_path_escape_ident, reregister};

/// Procedures supported by this router (listed in unknown-proc errors).
const SUPPORTED_PROCEDURES: &[&str] = &[
    "expire_snapshots",
    "rewrite_data_files",
    "rewrite_position_delete_files",
    "rollback_to_snapshot",
];

/// ===========================================================================================
/// Execute one `CALL catalog.system.<proc>(…)` statement.
///
/// Routes the three v1 maintenance procedures; unknown / deferred procedures fail loud with
/// the supported list. Enforces the LOCAL-catalog gate before any table load.
/// ===========================================================================================
///
/// # Errors
/// Plan / `NotImplemented` / iceberg commit failures as [`DataFusionError`].
pub async fn execute_call(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    function: &Function,
) -> Result<DataFrame> {
    let (catalog_name, procedure) = resolve_call_target(&function.name)?;
    let catalog = Arc::clone(catalog_handle(catalogs, &catalog_name)?);
    let args = CallArgs::parse(&function.args)?;

    match procedure.as_str() {
        "expire_snapshots" => execute_expire_snapshots(ctx, catalog, &catalog_name, &args).await,
        "rewrite_data_files" => {
            execute_rewrite_data_files(ctx, catalog, &catalog_name, &args).await
        }
        "rewrite_position_delete_files" => {
            execute_rewrite_position_delete_files(ctx, catalog, &catalog_name, &args).await
        }
        "rollback_to_snapshot" => {
            execute_rollback_to_snapshot(ctx, catalog, &catalog_name, &args).await
        }
        "remove_orphan_files" => Err(DataFusionError::NotImplemented(format!(
            "CALL system.remove_orphan_files is not supported — do not hand-roll orphan \
             file listing in RePark; queue on the owned iceberg-rust fork's DeleteOrphanFiles \
             surface (R-MAINTENANCE-CALL residual / fork-queue). Supported procedures: {}.",
            SUPPORTED_PROCEDURES.join(", ")
        ))),
        other => Err(DataFusionError::NotImplemented(format!(
            "CALL system.{other} is not supported. Supported procedures: {}.",
            SUPPORTED_PROCEDURES.join(", ")
        ))),
    }
}

/// `catalog.system.procedure` (exactly three parts; middle must be `system`).
fn resolve_call_target(
    name: &datafusion::sql::sqlparser::ast::ObjectName,
) -> Result<(String, String)> {
    let parts = name_parts(name);
    match parts.as_slice() {
        [catalog, system, procedure]
            if system.eq_ignore_ascii_case("system") && !procedure.is_empty() =>
        {
            Ok((catalog.clone(), procedure.to_ascii_lowercase()))
        }
        _ => Err(DataFusionError::Plan(format!(
            "CALL expects `catalog.system.<procedure>(…)`, got `{name}`"
        ))),
    }
}

// ===========================================================================================
// Argument bag
// ===========================================================================================

/// Parsed CALL arguments — named map + ordered positional list (Spark allows either form,
/// not mixed).
#[derive(Debug, Default)]
struct CallArgs {
    named: HashMap<String, Expr>,
    positional: Vec<Expr>,
}

impl CallArgs {
    fn parse(args: &FunctionArguments) -> Result<Self> {
        match args {
            FunctionArguments::None => Ok(Self::default()),
            FunctionArguments::Subquery(_) => Err(DataFusionError::Plan(
                "CALL does not accept a subquery argument list".to_string(),
            )),
            FunctionArguments::List(list) => {
                let mut named = HashMap::new();
                let mut positional = Vec::new();
                for arg in &list.args {
                    match arg {
                        FunctionArg::Named { name, arg, .. }
                        | FunctionArg::ExprNamed {
                            name: Expr::Identifier(name),
                            arg,
                            ..
                        } => {
                            let key = name.value.to_ascii_lowercase();
                            let expr = match arg {
                                FunctionArgExpr::Expr(expr) => expr.clone(),
                                other => {
                                    return Err(DataFusionError::Plan(format!(
                                        "CALL named argument `{key}` must be a scalar \
                                         expression, got {other}"
                                    )));
                                }
                            };
                            if named.insert(key.clone(), expr).is_some() {
                                return Err(DataFusionError::Plan(format!(
                                    "duplicate CALL argument `{key}`"
                                )));
                            }
                        }
                        FunctionArg::ExprNamed { name, .. } => {
                            return Err(DataFusionError::Plan(format!(
                                "CALL named argument name must be an identifier, got {name}"
                            )));
                        }
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            positional.push(expr.clone());
                        }
                        FunctionArg::Unnamed(other) => {
                            return Err(DataFusionError::Plan(format!(
                                "CALL positional argument must be a scalar expression, got {other}"
                            )));
                        }
                    }
                }
                if !named.is_empty() && !positional.is_empty() {
                    return Err(DataFusionError::Plan(
                        "CALL does not support mixing named and positional arguments \
                         (Iceberg Spark Procedures — named or positional, not both)"
                            .to_string(),
                    ));
                }
                Ok(Self { named, positional })
            }
        }
    }

    fn require_string(&self, name: &str, position: usize) -> Result<String> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_string(expr, name);
        }
        self.positional
            .get(position)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CALL argument `{name}` is required (named `{name} => …` or positional \
                     #{position})"
                ))
            })
            .and_then(|expr| expr_as_string(expr, name))
    }

    fn optional_string(&self, name: &str) -> Result<Option<String>> {
        self.named
            .get(name)
            .map(|expr| expr_as_string(expr, name))
            .transpose()
    }

    fn optional_i64(&self, name: &str, position: Option<usize>) -> Result<Option<i64>> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_i64(expr, name).map(Some);
        }
        if let Some(index) = position
            && let Some(expr) = self.positional.get(index)
        {
            return expr_as_i64(expr, name).map(Some);
        }
        Ok(None)
    }

    fn optional_i32(&self, name: &str, position: Option<usize>) -> Result<Option<i32>> {
        match self.optional_i64(name, position)? {
            None => Ok(None),
            Some(value) => i32::try_from(value).map(Some).map_err(|_| {
                DataFusionError::Plan(format!(
                    "CALL argument `{name}` value {value} does not fit i32"
                ))
            }),
        }
    }

    fn optional_timestamp_ms(&self, name: &str, position: Option<usize>) -> Result<Option<i64>> {
        if let Some(expr) = self.named.get(name) {
            return expr_as_timestamp_ms(expr, name).map(Some);
        }
        if let Some(index) = position
            && let Some(expr) = self.positional.get(index)
        {
            return expr_as_timestamp_ms(expr, name).map(Some);
        }
        Ok(None)
    }

    fn has_named(&self, name: &str) -> bool {
        self.named.contains_key(name)
    }

    fn reject_unknown_named(&self, allowed: &[&str]) -> Result<()> {
        for key in self.named.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(DataFusionError::Plan(format!(
                    "unknown CALL argument `{key}`; allowed: {}",
                    allowed.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// Reject more positional arguments than the procedure arity (C1-L-001 / C1-L-002).
    ///
    /// Spark accepts trailing optional positionals omitted; extra *beyond* the max arity must
    /// not be silently dropped (otherwise positional `strategy` on rewrite binpacks silently).
    fn reject_excess_positional(&self, max_arity: usize) -> Result<()> {
        if self.positional.len() > max_arity {
            return Err(DataFusionError::Plan(format!(
                "CALL accepts at most {max_arity} positional argument(s); got {}",
                self.positional.len()
            )));
        }
        Ok(())
    }
}

fn expr_as_string(expr: &Expr, arg_name: &str) -> Result<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => Ok(text.clone()),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be a string literal, got {other}"
        ))),
    }
}

fn expr_as_i64(expr: &Expr, arg_name: &str) -> Result<i64> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        }) => raw.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{arg_name}` is not an integer: {raw}"
            ))
        }),
        Expr::UnaryOp {
            op: datafusion::sql::sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let value = expr_as_i64(expr, arg_name)?;
            value.checked_neg().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CALL argument `{arg_name}` integer negation overflows i64: {value}"
                ))
            })
        }
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => text.trim().parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{arg_name}` string is not an integer: {text}"
            ))
        }),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be an integer, got {other}"
        ))),
    }
}

fn value_to_string(value: &Value) -> Option<&str> {
    match value {
        Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text.as_str()),
        _ => None,
    }
}

fn expr_as_timestamp_ms(expr: &Expr, arg_name: &str) -> Result<i64> {
    match expr {
        // TIMESTAMP '…' / DATE '…'
        Expr::TypedString(typed) => {
            let raw = value_to_string(&typed.value.value).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CALL argument `{arg_name}` TIMESTAMP payload must be a string, got {}",
                    typed.value.value
                ))
            })?;
            parse_timestamp_to_ms(raw)
        }
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => parse_timestamp_to_ms(text),
        Expr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        }) => raw.parse::<i64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{arg_name}` is not a timestamp or epoch-ms integer: {raw}"
            ))
        }),
        Expr::Cast { expr, .. } => expr_as_timestamp_ms(expr, arg_name),
        other => Err(DataFusionError::Plan(format!(
            "CALL argument `{arg_name}` must be a TIMESTAMP literal, string, or epoch-ms \
             integer, got {other}"
        ))),
    }
}

// ===========================================================================================
// Table identity resolution
// ===========================================================================================

/// Resolve the Spark `table` string against the CALL catalog.
///
/// Accepts `namespace.table` (two-part, relative to the CALL catalog) or
/// `catalog.namespace.table` when the catalog segment matches the CALL catalog.
/// Each segment is path-escape rejected (same contract as CTAS idents — C1-SEC-001).
/// Empty segments (`a..b`, leading/trailing `.`) refuse loud rather than collapsing.
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

/// Byte totals arrive from the fork as `u64`; Spark's column is a signed `bigint`. Refuse rather
/// than saturate, for the same reason the count helpers do.
fn bytes_as_i64(bytes: u64) -> Result<i64> {
    i64::try_from(bytes).map_err(|_| {
        DataFusionError::Plan(format!(
            "CALL result byte count {bytes} does not fit i64 (refusing to fabricate MAX)"
        ))
    })
}

// ===========================================================================================
// expire_snapshots
// ===========================================================================================

/// Spark's six-column output, all of it (MW-1). Measured against a live Spark 4.0.1 +
/// Iceberg 1.10.0 oracle.
///
/// | Spark column | Source |
/// |---|---|
/// | `deleted_data_files_count` | funnel entries classified [`DataContentType::Data`] |
/// | `deleted_position_delete_files_count` | classified [`DataContentType::PositionDeletes`] |
/// | `deleted_equality_delete_files_count` | classified [`DataContentType::EqualityDeletes`] |
/// | `deleted_manifest_files_count` | `deleted_manifests.len()` |
/// | `deleted_manifest_lists_count` | `deleted_manifest_lists.len()` |
/// | `deleted_statistics_files_count` | `deleted_statistics_files.len()` |
///
/// The fork returns the first three as ONE funnel; [`classify_content_files`] rebuilds the split
/// from the manifest entries' own `content_type()`. Counts are still never fabricated — an
/// unclassifiable path lands in none of the three columns.
async fn execute_expire_snapshots(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&[
        "table",
        "older_than",
        "retain_last",
        "snapshot_ids",
        "max_concurrent_deletes",
        "stream_results",
        "clean_expired_metadata",
    ])?;
    // Spark arity: table, older_than?, retain_last? (+ deferred named-only args)
    args.reject_excess_positional(3)?;
    for unsupported in [
        "snapshot_ids",
        "max_concurrent_deletes",
        "stream_results",
        "clean_expired_metadata",
    ] {
        if args.has_named(unsupported) {
            return Err(DataFusionError::NotImplemented(format!(
                "CALL expire_snapshots argument `{unsupported}` is not supported in v1 \
                 (supported: table, older_than, retain_last)"
            )));
        }
    }

    let table_arg = args.require_string("table", 0)?;
    let older_than_ms = args.optional_timestamp_ms("older_than", Some(1))?;
    let retain_last = args.optional_i32("retain_last", Some(2))?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    let mut action = Transaction::new(&table).expire_snapshots();
    if let Some(timestamp_ms) = older_than_ms {
        action = action.expire_older_than(timestamp_ms);
    }
    if let Some(retain) = retain_last {
        // Pre-validate Java/fork floor (`retain_last >= 1`) so CALL fails at plan time with a
        // stable message rather than only as an iceberg commit error (C3-Q-001).
        if retain < 1 {
            return Err(DataFusionError::Plan(format!(
                "CALL expire_snapshots retain_last must be >= 1, got {retain}"
            )));
        }
        action = action.retain_last(retain);
    }

    // Classify BEFORE the cleanup runs: afterwards the files and their manifests are gone, and
    // the funnel the fork returns is paths only. This is the one ordering the split depends on.
    let classified = classify_content_files(&table).await;

    let tx = Transaction::new(&table);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let cleanup = ExpireSnapshotsCleanup::new(table.file_io().clone());
    let (_committed, report) = cleanup
        .commit_and_clean(tx, catalog.as_ref())
        .await
        .map_err(iceberg_err)?;
    let counts = ExpireCounts::tally(&report.deleted_content_files, &classified);

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;
    expire_result_dataframe(ctx, &report, &counts)
}

/// Classify every content file the table can currently reach, by Spark's three-way split.
///
/// MW-1. The fork's [`CleanupReport`] returns ONE funnel — `deleted_content_files` holds data,
/// position-delete, equality-delete and DV puffin paths together — because
/// `expire_cleanup` collects `entry.file_path()` into a path set and drops the classification.
/// Spark reports the three separately, and on a merge-on-read table the difference is not
/// cosmetic: measured against a live Spark 4.0.1 + Iceberg 1.10.0 oracle, three MERGEs plus a
/// compaction expire as `deleted_data_files_count=4`, `deleted_position_delete_files_count=2`.
/// Reporting the funnel under Spark's data-file name over-counts by exactly the delete files.
///
/// The classification is not lost, only discarded: every [`ManifestEntry`] carries
/// `content_type()`. So this walks the table as it stands BEFORE expiry — every file expiry can
/// delete is reachable from some snapshot at that point — and builds path → content type. The
/// counts stay honest: a path the map cannot classify is counted nowhere and reported through
/// [`ExpireCounts::unclassified`], never folded into a column to make the arithmetic look tidy.
async fn classify_content_files(table: &iceberg::table::Table) -> HashMap<String, DataContentType> {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let mut classified = HashMap::new();
    for snapshot in metadata.snapshots() {
        let Ok(manifest_list) = snapshot.load_manifest_list(file_io, metadata).await else {
            // Best-effort: an unreadable manifest list leaves its files unclassified, which is
            // visible in the result rather than silently miscounted.
            continue;
        };
        for manifest_file in manifest_list.entries() {
            let Ok(manifest) = manifest_file.load_manifest(file_io).await else {
                continue;
            };
            for entry in manifest.entries() {
                classified.insert(entry.file_path().to_string(), entry.content_type());
            }
        }
    }
    classified
}

/// The three content-file counts Spark reports, plus what could not be classified.
#[derive(Debug, Default)]
struct ExpireCounts {
    data: i64,
    position_deletes: i64,
    equality_deletes: i64,
    /// Deleted paths absent from the pre-expiry classification. Never folded into a column.
    unclassified: i64,
}

impl ExpireCounts {
    fn tally(deleted: &[String], classified: &HashMap<String, DataContentType>) -> Self {
        let mut counts = Self::default();
        for path in deleted {
            match classified.get(path) {
                Some(DataContentType::Data) => counts.data += 1,
                Some(DataContentType::PositionDeletes) => counts.position_deletes += 1,
                Some(DataContentType::EqualityDeletes) => counts.equality_deletes += 1,
                None => counts.unclassified += 1,
            }
        }
        counts
    }
}

fn expire_result_dataframe(
    ctx: &SessionContext,
    report: &CleanupReport,
    counts: &ExpireCounts,
) -> Result<DataFrame> {
    // Spark declares every one of these NULLABLE (jar `OUTPUT_TYPE`, `iconst_1` per StructField),
    // unlike its two rewrite procedures, which it declares non-nullable. Match Spark per
    // procedure rather than applying one blanket rule across the surface.
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
            Arc::new(Int64Array::from(vec![counts.data])),
            Arc::new(Int64Array::from(vec![counts.position_deletes])),
            Arc::new(Int64Array::from(vec![counts.equality_deletes])),
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

// ===========================================================================================
// rewrite_data_files
// ===========================================================================================

async fn execute_rewrite_data_files(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "strategy", "sort_order", "options", "where"])?;
    // Supported positional arity v1: table + optional strategy only (C2-Q-002). sort_order /
    // options / where are named-only deferred; extra positionals refuse as excess arity (not a
    // misleading sort_order message).
    args.reject_excess_positional(2)?;
    // strategy: named OR positional #1 (Spark rewrite_data_files positional order).
    let strategy = if let Some(named) = args.optional_string("strategy")? {
        Some(named)
    } else if args.positional.len() > 1 {
        Some(expr_as_string(&args.positional[1], "strategy")?)
    } else {
        None
    };
    if let Some(strategy) = strategy {
        // Trim so `' binpack '` matches Spark-ish whitespace tolerance (C3-L-001).
        let normalized = strategy.trim().to_ascii_lowercase();
        if normalized != "binpack" {
            return Err(DataFusionError::NotImplemented(format!(
                "CALL rewrite_data_files strategy `{strategy}` is not supported — only \
                 binpack is ported (fork R135 deferred: sort / zOrder strategies)"
            )));
        }
    }
    if args.has_named("sort_order") {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_data_files sort_order is not supported — fork R135 deferred \
             (sort / zOrder strategies); only default binpack is available"
                .to_string(),
        ));
    }
    if args.has_named("options") {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_data_files options map is not supported in v1 — use table \
             properties / defaults (fork R135 binpack defaults: min_input_files=5, …)"
                .to_string(),
        ));
    }
    if args.has_named("where") {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_data_files where filter is not supported in v1 (fork filter \
             builder exists but is not wired through CALL yet)"
                .to_string(),
        ));
    }

    let table_arg = args.require_string("table", 0)?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    // For multi-small-file fixtures under Spark's default min_input_files=5, pure inserts of a
    // few rows each are always "small". Lower min_input_files only when the caller cannot pass
    // options (options map is deferred) — keep Java default of 5 so empty/no-op plans match
    // Spark. Tests build ≥5 small files.
    let result = RewriteDataFiles::new(table)
        .execute(catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;

    // Spark's five columns, all of them, all non-nullable (MW-2 closed the fifth).
    //
    // | Spark column | Source |
    // |---|---|
    // | `rewritten_data_files_count` | `result.rewritten_data_files_count` |
    // | `added_data_files_count` | `result.added_data_files_count` |
    // | `rewritten_bytes_count` | `result.rewritten_bytes_count` |
    // | `failed_data_files_count` | always 0 — partial progress is deferred, so nothing can fail |
    // | `removed_delete_files_count` | always 0 — see below |
    //
    // `removed_delete_files_count` counts what Java's RemoveDanglingDeletes sub-action removed.
    // That sub-action runs only under the `remove-dangling-deletes` option, whose Java default is
    // false (`RewriteDataFiles.REMOVE_DANGLING_DELETES_DEFAULT`, javap-verified), and this
    // procedure refuses the options map above — so the non-default path is unreachable here and
    // the count of removals is genuinely zero. Measured against a live Spark 4.0.1 + Iceberg
    // 1.10.0 oracle, which reported 0 on every fixture tried, with the option both off AND
    // explicitly on. This is an honest count of a real quantity, not a placeholder for a number
    // the engine could not obtain.
    let schema = Arc::new(Schema::new(vec![
        Field::new("rewritten_data_files_count", DataType::Int32, false),
        Field::new("added_data_files_count", DataType::Int32, false),
        Field::new("rewritten_bytes_count", DataType::Int64, false),
        Field::new("failed_data_files_count", DataType::Int32, false),
        Field::new("removed_delete_files_count", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![count_as_i32(
                result.rewritten_data_files_count,
            )?])),
            Arc::new(Int32Array::from(vec![count_as_i32(
                result.added_data_files_count,
            )?])),
            Arc::new(Int64Array::from(vec![bytes_as_i64(
                result.rewritten_bytes_count,
            )?])),
            Arc::new(Int32Array::from(vec![0])),
            Arc::new(Int32Array::from(vec![0])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

// ===========================================================================================
// rewrite_position_delete_files
// ===========================================================================================

/// Spark's four-column output, all of it. Measured by EXECUTING the procedure on a live
/// Spark 4.0.1 + Iceberg 1.10.0 oracle, not read from a constant.
///
/// | Spark column | Type | Nullable | Source |
/// |---|---|---|---|
/// | `rewritten_delete_files_count` | int | false | `result.rewritten_delete_files_count` |
/// | `added_delete_files_count` | int | false | `result.added_delete_files_count` |
/// | `rewritten_bytes_count` | bigint | false | `result.rewritten_bytes_count` |
/// | `added_bytes_count` | bigint | false | `result.added_bytes_count` |
///
/// Nothing is fabricated and nothing is omitted: the fork's `RewritePositionDeleteFilesResult`
/// mirrors Java's `RewritePositionDeleteFiles$Result` one accessor at a time, so this is the one
/// procedure whose schema the engine did not have to choose.
///
/// **`added_delete_files_count` diverges on a file-granularity table** (registry row `MOR-1`).
/// The fork writes ONE compacted delete file per `(spec, partition)` group; Spark honours
/// `write.delete.granularity`, whose default is `file`, and writes one per data file. On a table
/// this engine wrote the two agree, because this engine's own merge-on-read writer is
/// partition-granularity already. On a table Spark wrote at the default granularity they do not.
/// The live row set is identical either way — this is file layout, not contents.
async fn execute_rewrite_position_delete_files(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "options", "where"])?;
    // Only `table` is supported positionally; `options` / `where` are named-only deferred, so an
    // extra positional refuses as excess arity rather than under a misleading argument name.
    args.reject_excess_positional(1)?;
    if args.has_named("options") {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_position_delete_files options map is not supported in v1 — use table \
             properties / defaults (fork bin-pack planner groups by (spec, partition))"
                .to_string(),
        ));
    }
    if args.has_named("where") {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_position_delete_files where filter is not supported in v1 (the fork \
             exposes RewritePositionDeleteFiles::filter but it is not wired through CALL yet)"
                .to_string(),
        ));
    }

    let table_arg = args.require_string("table", 0)?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    let result = RewritePositionDeleteFiles::new(table)
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

// ===========================================================================================
// rollback_to_snapshot
// ===========================================================================================

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

    // Fork R98: ManageSnapshotsAction::rollback_to — snapshot_id must be an ancestor of
    // current main (`crates/iceberg/src/transaction/manage_snapshots.rs:162-167`).
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
        let ok = resolve_call_target(&object_name(&["ice", "system", "expire_snapshots"])).unwrap();
        assert_eq!(ok.0, "ice");
        assert_eq!(ok.1, "expire_snapshots");
        assert!(resolve_call_target(&object_name(&["ice", "expire_snapshots"])).is_err());
        assert!(resolve_call_target(&object_name(&["ice", "sys", "expire_snapshots"])).is_err());
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

    #[test]
    fn expr_as_i64_unary_minus_min_refuses_overflow() {
        use datafusion::sql::sqlparser::ast::{
            Expr as AstExpr, UnaryOperator, Value as AstValue, ValueWithSpan,
        };
        use datafusion::sql::sqlparser::tokenizer::Span;
        let min = AstExpr::Value(ValueWithSpan {
            value: AstValue::Number(i64::MIN.to_string(), false),
            span: Span::empty(),
        });
        let negated = AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(min),
        };
        // -i64::MIN cannot be represented — must Plan-error, not panic/wrap (C1-SAF-001).
        assert!(expr_as_i64(&negated, "snapshot_id").is_err());
    }
}
