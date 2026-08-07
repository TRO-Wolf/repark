//! Spark Iceberg `CALL catalog.system.<proc>(…)` maintenance procedure router (I3 /
//! R-MAINTENANCE-CALL).
//!
//! v1 ships **exactly three** procedures that the owned fork already provides:
//!
//! 1. `expire_snapshots` — fork R133 `Transaction::expire_snapshots` +
//!    `ExpireSnapshotsCleanup::commit_and_clean`
//!    (`crates/iceberg/src/transaction/expire_snapshots.rs` + `expire_cleanup.rs`, pin
//!    `4723104b`).
//! 2. `rewrite_data_files` — fork R135 bin-pack
//!    (`crates/iceberg/src/maintenance/rewrite_data_files.rs`
//!    `RewriteDataFiles::new(table).execute(&catalog)`).
//! 3. `rollback_to_snapshot` — fork R98 `ManageSnapshotsAction::rollback_to`
//!    (`crates/iceberg/src/transaction/manage_snapshots.rs:164-167`).
//!
//! **LOCAL catalogs only** ([`LocationPolicy::TempFallbackAllowed`]). Glue / S3 Tables
//! (`RequireExplicitLocation` / `ServiceManagedLocation`) refuse every procedure loud —
//! expire/cleanup never touch remote object storage through this surface.
//!
//! **Parsing:** named (`arg => value`) and positional arguments are both accepted —
//! Apache Iceberg Spark Procedures docs:
//! "CALL supports passing arguments by name (recommended) or by position."
//! <https://iceberg.apache.org/docs/latest/spark-procedures/#usage>
//!
//! **Result rows:** column names/types pin to Spark's documented output schema where the
//! fork exposes an honest metric; divergence-pin (name differ / absent-with-disclosure)
//! where it does not. Counts are **never fabricated**.
//!
//! **Out of scope (loud):**
//! - `remove_orphan_files` — fork-queue residual (do not hand-roll file listing).
//! - rewrite `strategy` / `sort_order` other than default bin-pack — R135 deferred list.
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
use iceberg::maintenance::RewriteDataFiles;
use iceberg::transaction::{
    ApplyTransactionAction, CleanupReport, ExpireSnapshotsCleanup, Transaction,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use repark_core::{CatalogRegistry, LocationPolicy, parse_timestamp_to_ms};

use crate::{catalog_handle, iceberg_err, name_parts, reject_path_escape_ident, reregister};

/// Procedures supported by this router (listed in unknown-proc errors).
const SUPPORTED_PROCEDURES: &[&str] = &[
    "expire_snapshots",
    "rewrite_data_files",
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
    refuse_non_local_catalog(catalogs, &catalog_name)?;
    let args = CallArgs::parse(&function.args)?;

    match procedure.as_str() {
        "expire_snapshots" => execute_expire_snapshots(ctx, catalog, &catalog_name, &args).await,
        "rewrite_data_files" => {
            execute_rewrite_data_files(ctx, catalog, &catalog_name, &args).await
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

/// LOCAL-only hard line: maintenance CALL never runs against Glue / S3 Tables catalogs.
fn refuse_non_local_catalog(catalogs: &CatalogRegistry, catalog_name: &str) -> Result<()> {
    match catalogs.location_policy(catalog_name) {
        // E-4 (phase-1 forced edit): the variant carries the resolved fallback root now.
        Some(LocationPolicy::TempFallbackAllowed { .. }) => Ok(()),
        Some(LocationPolicy::RequireExplicitLocation) => Err(DataFusionError::Plan(format!(
            "CALL system.* maintenance procedures are LOCAL-only in v1 — catalog \
             `{catalog_name}` is a Glue/remote catalog (RequireExplicitLocation). \
             Refuse expire/rewrite/rollback against Glue/S3; use a memory/local catalog."
        ))),
        Some(LocationPolicy::ServiceManagedLocation) => Err(DataFusionError::Plan(format!(
            "CALL system.* maintenance procedures are LOCAL-only in v1 — catalog \
             `{catalog_name}` is S3 Tables (ServiceManagedLocation). Refuse expire/rewrite/\
             rollback against remote object storage."
        ))),
        None => Err(DataFusionError::Plan(format!(
            "unknown catalog `{catalog_name}`"
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

// ===========================================================================================
// expire_snapshots
// ===========================================================================================

/// Spark output (docs) + fork [`CleanupReport`] divergence:
///
/// | Spark column | Fork source |
/// |---|---|
/// | `deleted_data_files_count` | **divergence:** fork `deleted_content_files` is ALL content \
///   (data + pos/eq delete + DV puffins) in one funnel — not split. Reported under Spark's \
///   `deleted_data_files_count` name; pos/eq Spark columns are **omitted** (absent-with-\
///   disclosure) rather than zero-filled. |
/// | `deleted_manifest_files_count` | `deleted_manifests.len()` |
/// | `deleted_manifest_lists_count` | `deleted_manifest_lists.len()` |
/// | `deleted_statistics_files_count` | `deleted_statistics_files.len()` |
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

fn expire_result_dataframe(ctx: &SessionContext, report: &CleanupReport) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("deleted_data_files_count", DataType::Int64, false),
        Field::new("deleted_manifest_files_count", DataType::Int64, false),
        Field::new("deleted_manifest_lists_count", DataType::Int64, false),
        Field::new("deleted_statistics_files_count", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![count_as_i64(
                report.deleted_content_files.len(),
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

    // Spark: rewritten_data_files_count (int), added_data_files_count (int),
    // rewritten_bytes_count (long), failed_data_files_count (int), removed_delete_files_count (int).
    // Fork exposes the first three. failed is always 0 (partial-progress deferred — honest zero).
    // removed_delete_files_count is ABSENT (fork does not expose dangling-delete removal here).
    let rewritten_bytes = i64::try_from(result.rewritten_bytes_count).map_err(|_| {
        DataFusionError::Plan(format!(
            "CALL rewrite rewritten_bytes_count {} does not fit i64 (refusing to fabricate MAX)",
            result.rewritten_bytes_count
        ))
    })?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("rewritten_data_files_count", DataType::Int32, false),
        Field::new("added_data_files_count", DataType::Int32, false),
        Field::new("rewritten_bytes_count", DataType::Int64, false),
        Field::new("failed_data_files_count", DataType::Int32, false),
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
            Arc::new(Int64Array::from(vec![rewritten_bytes])),
            Arc::new(Int32Array::from(vec![0])),
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
