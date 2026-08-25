//! The Spark-SQL statement router: `execute` / `execute_with_read_only` / `execute_inner`.
//!
//! Ported from the v1 SQL crate's `lib.rs` (declared-rename unit; the crate root here is a
//! manifest per `scripts/check_lib_rs.py`, so the router body lives in this module). `execute`
//! parses the statement with DataFusion's `sqlparser`, intercepts the forms DataFusion cannot
//! execute against an Iceberg catalog, and passes everything else straight through.
//!
//! **PR-3b completes the router** — the full v1 execute family is live (MERGE INTO, INSERT
//! OVERWRITE, CALL, branch/tag ref DDL, and the r25 T2 write-to-branch sniff restored).
//!
//! Intercepted (live in this build):
//! - **CTAS** (`CREATE TABLE … AS SELECT`) — lowered onto the fork's `StagedTableTransaction`
//!   (create or replace): one catalog publish, no drop-then-insert hole (`ENGINE_CONTRACT`
//!   §8a).
//! - **Column-def `CREATE TABLE … (cols) USING iceberg`** — I5 schema-only staged create.
//! - **`DROP TABLE`** and **`CREATE` / `DROP NAMESPACE | DATABASE`** — catalog ops on the
//!   iceberg handle, with `IF [NOT] EXISTS` idempotency.
//! - **`ALTER TABLE`** — `SET` / `UNSET TBLPROPERTIES`, `RENAME TO`, schema evolution, and the
//!   I7 partition-field DDL via the fork's `UpdateSchema` (see [`crate::alter`]; I6/I7).
//! - **`DESCRIBE {NAMESPACE|DATABASE|SCHEMA} [EXTENDED]`** — read back a namespace's properties
//!   as Spark's `info_name`/`info_value` frame (Group Z; pinned to a live pyspark 4.0.0
//!   `DataSourceV2` oracle).
//! - **`SHOW {NAMESPACES|SCHEMAS|DATABASES}`** — list a catalog's namespaces as Spark's
//!   one-column `namespace` frame (Group AB; same oracle, incl. `LIKE`-pattern semantics).
//! - **`MERGE INTO`** — lowered in [`crate::merge`] and executed by
//!   `repark_iceberg::write::merge` (COW **and** merge-on-read; fork `ENGINE_CONTRACT` §6).
//! - **`INSERT OVERWRITE`** — empty: probe → plan/type-validate → provider self-scan wipe
//!   (C1-Q-001); non-empty (r23 OV1): stream → staged files →
//!   `commit_overwrite_replace_all` (stage-then-swap; see [`crate::insert_overwrite`]).
//! - **`CALL`** — Iceberg maintenance procedures via [`crate::call`] (I3; LOCAL catalogs only).
//! - **Snapshot-ref DDL** (I5) — `CREATE|DROP|REPLACE BRANCH|TAG` via [`crate::ref_ddl`], plus
//!   the r25 T2 write-to-branch STOP (`ref_ddl::sniff_write_to_branch`).
//! - **Metadata tables** (I2) — Spark `cat.ns.tbl.snapshots` → fork `cat.ns.tbl$snapshots`.
//! - **Time travel** (I1) — `VERSION AS OF` / `TIMESTAMP AS OF` / `FOR SYSTEM_*` rewritten to
//!   snapshot-pinned static providers before normal routing.
//! - **`TRUNCATE TABLE`** — targeted loud refuse (C4-L-001), verbatim from v1.
//!
//! Passthrough: `DELETE` / `UPDATE` / non-overwrite `INSERT INTO` ride DataFusion onto the fork
//! provider's DML (ADR-0003) behind the P11 read-only-catalog refuse, the **G3-E8
//! subquery-predicate valve** (a `WHERE` subquery is lost at DataFusion's DML planning boundary
//! and degenerates into match-all — see [`crate::normalize::refuse_dml_subquery_predicate`]), and
//! the r22 A2 BUG-001 merge-on-read multi-spec valve. Multi-statement SQL refuses first
//! (BUG-010); the SEC-02 local-filesystem DDL gate runs inside the passthrough.
//!
//! **This module's parse is NOT the executing parse.** [`parse_single_normalized`] uses
//! `DatabricksDialect`; [`spark_ast::execute_passthrough`] re-parses under the session dialect
//! and plans THAT. Any statement the first parse rejects reaches the second one through
//! [`execute_unparsable_fallthrough`] — Spark's FROM-less `DELETE <table> WHERE …` is the live
//! example. A DML data-loss guard must therefore attach at the executing parse (G3-E8 does; the
//! arms below keep an early call only for valve ORDER), or it is fail-open by construction.

use std::collections::HashSet;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{ObjectType, Statement, TableObject};
use repark_core::CatalogRegistry;

use crate::{
    DmlSubqueryVerb, MorDmlKind, alter, build_ctas, call, create_table, delete_target_object_name,
    describe_show, execute_create_namespace, execute_ctas, execute_drop_namespace,
    execute_drop_table, execute_insert_overwrite, merge, metadata_tables,
    object_name_from_table_with_joins, parse_single_normalized, passthrough_after_p11, ref_ddl,
    refuse_dml_subquery_predicate, refuse_mor_unpartitioned_multi_spec_dml,
    refuse_multi_statement_sql, refuse_read_only_dml_from_delete, refuse_read_only_dml_table_sql,
    refuse_v3_cow_dml, spark_ast, starts_with_branch_or_tag_ddl, starts_with_merge, time_travel,
    try_parse_create_namespace,
};

/// ===========================================================================================
/// Execute one Spark-SQL statement against `ctx`, routing the Iceberg DDL/write forms to their
/// handlers and passing everything else (reads, `INSERT INTO`, …) to DataFusion.
///
/// Intercepted today: CTAS (`CREATE TABLE … AS SELECT`, decomposed), **column-def**
/// `CREATE TABLE … (cols) USING iceberg` (I5 schema-only staged create), `DROP TABLE`,
/// `CREATE` / `DROP NAMESPACE | DATABASE`, `ALTER TABLE` (SET/UNSET TBLPROPERTIES, RENAME TO,
/// ADD/DROP/RENAME COLUMN + stretch ALTER COLUMN — I6, + the I7 partition-field DDL),
/// **`CREATE|DROP BRANCH|TAG`** (I5 → fork `ManageSnapshots`; REPLACE still loud),
/// `MERGE INTO` (COW + merge-on-read), **empty** `INSERT OVERWRITE` (probe + plan/type-validate +
/// provider wipe — C1-Q-001; fork empty-overwrite short-circuit is fixed on the pin),
/// `CALL` (I3: three maintenance procs; unknown/deferred refuse listing supported),
/// `TRUNCATE TABLE` (loud `NotImplemented` — C4-L-001),
/// `DESCRIBE {NAMESPACE|DATABASE|SCHEMA}` (Group Z) and
/// `SHOW {NAMESPACES|SCHEMAS|DATABASES}` (Group AB).
/// Non-empty `INSERT [OVERWRITE]` / `DELETE` / `UPDATE` pass through to the fork provider's DML.
/// ===========================================================================================
///
/// # Errors
/// Propagates parse, planning, iceberg, and execution errors as [`DataFusionError`].
pub async fn execute(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    execute_with_read_only(ctx, catalogs, sql, &HashSet::new()).await
}

/// Execute with a set of read-only (postgres) catalog names for P11 DML routing.
///
/// # Errors
/// Any planning/execution error from the underlying statement, plus the P11 refusal when a
/// DML statement targets a read-only (postgres) catalog.
pub async fn execute_with_read_only<S: std::hash::BuildHasher>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    read_only_catalogs: &HashSet<String, S>,
) -> Result<DataFrame> {
    // Clone + attach on the registry snapshot so P11 survives `.await` thread hops
    // (thread_local would race under multi-thread Tokio — C1-Q-001).
    let mut catalogs = catalogs.clone();
    catalogs.set_read_only_catalogs(read_only_catalogs.iter().cloned().collect());
    // r25 T2: write-to-branch STOP — refuse before metadata rewrite / main-branch fallthrough.
    // Fork FastAppend always SetSnapshotRef MAIN_BRANCH (no to_branch commit target).
    // Two-part `a.branch_x` is ambiguous with a REAL `schema.branch_x` table (morning critic):
    // refuse only when the full name does not resolve but the prefix does (Spark's
    // `t.branch_<name>` spelling); neither resolving falls through to planning's own
    // "table not found", which is the more informative error.
    if let Some(sniff) = ref_ddl::sniff_write_to_branch(sql) {
        let refuse = match &sniff {
            ref_ddl::WriteToBranchSniff::MultiPart => true,
            ref_ddl::WriteToBranchSniff::TwoPart { parts } => {
                let full =
                    datafusion::sql::TableReference::partial(parts[0].as_str(), parts[1].as_str());
                let prefix = datafusion::sql::TableReference::bare(parts[0].as_str());
                !ctx.table_exist(full).unwrap_or(false) && ctx.table_exist(prefix).unwrap_or(false)
            }
        };
        if refuse {
            return Err(ref_ddl::refuse_write_to_branch());
        }
    }
    // I2 / R-METADATA-TABLES — Spark `cat.ns.tbl.snapshots` → fork `cat.ns.tbl$snapshots`
    // (iceberg-datafusion schema provider). Real tables named e.g. `files` win; DML + AS OF
    // composition refuse loud. Kept out of `execute_inner` (clippy `too_many_lines`).
    let sql_after_meta: std::borrow::Cow<'_, str> =
        if metadata_tables::sql_may_have_metadata_table_path(sql) {
            match metadata_tables::prepare_metadata_table_sql(&catalogs, sql).await? {
                Some(rewritten) => std::borrow::Cow::Owned(rewritten),
                None => std::borrow::Cow::Borrowed(sql),
            }
        } else {
            std::borrow::Cow::Borrowed(sql)
        };
    // The time-travel rewrite registers ephemeral pinned relations on the session; they are
    // released again as soon as the statement has been PLANNED, so a long-lived session neither
    // accumulates them nor shows them in the introspection surface (`SHOW TABLES` /
    // `information_schema.tables`). The plan owns its provider, so the returned `DataFrame` still
    // collects after the name is gone. The release runs on every `?` / `return` path of the split
    // below — but NOT on unwind or future-drop: `PinnedViews` carries no `Drop` impl by design (it
    // would have to own a `SessionContext` clone), and neither source exists today (panics banned
    // in prod, PyO3 drives this via `block_on`).
    let mut pinned = time_travel::PinnedViews::default();
    let result = execute_time_travelled(ctx, &catalogs, sql_after_meta.as_ref(), &mut pinned).await;
    pinned.release(ctx);
    result
}

/// The rest of the router, from the time-travel rewrite onward. Split out purely so
/// [`execute_with_read_only`] can release `pinned` on every `?` / `return` path of the rewrite —
/// the ones an inline `?` would have skipped. (Unwind / future-drop bypass it; see the call site.)
async fn execute_time_travelled(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    pinned: &mut time_travel::PinnedViews,
) -> Result<DataFrame> {
    // Iceberg time travel (`VERSION AS OF` / `TIMESTAMP AS OF` / `FOR SYSTEM_* AS OF`) is not
    // modelled by Databricks-dialect sqlparser. Rewrite to snapshot-pinned static providers
    // (fork `IcebergStaticTableProvider::try_new_from_table_snapshot`) before normal routing.
    // I1 / R-TIME-TRAVEL — kept out of `execute_inner` so the router stays under clippy
    // `too_many_lines`.
    let sql_storage: std::borrow::Cow<'_, str> = if time_travel::sql_has_time_travel(sql) {
        match time_travel::prepare_time_travel_sql(ctx, catalogs, sql, pinned).await? {
            Some(rewritten) => std::borrow::Cow::Owned(rewritten),
            None => std::borrow::Cow::Borrowed(sql),
        }
    } else {
        std::borrow::Cow::Borrowed(sql)
    };
    execute_inner(ctx, catalogs, sql_storage.as_ref()).await
}

async fn execute_inner(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    // r22 A2 / BUG-010: refuse genuine multi-statement scripts before any intercept/passthrough.
    // Trailing `;` / whitespace / comments after a single statement remain allowed (Spark oracle).
    refuse_multi_statement_sql(sql)?;
    // Pre-parse recognizers for forms stock sqlparser cannot model (or would drop clauses from).
    if let Some(frame) = try_preparse_intercepts(ctx, catalogs, sql).await {
        return frame;
    }
    // If we can't parse it to a single statement we recognise, let DataFusion have it (passthrough)
    // — except MERGE / residual BRANCH|TAG, which get targeted errors instead of parse fails.
    let Some((statement, partitioning)) = parse_single_normalized(sql)? else {
        return execute_unparsable_fallthrough(ctx, catalogs, sql).await;
    };
    // G15 — intercepted CREATE / ALTER never reach execute_passthrough; refuse
    // collation on this parse too so column-def COLLATE cannot be a generic
    // "column option not supported" or a silent Iceberg string.
    crate::refuse_collation_in_statement(&statement)?;
    match &statement {
        Statement::CreateTable(create) if create.query.is_some() => {
            execute_ctas(ctx, catalogs, build_ctas(create, &partitioning)?).await
        }
        // Column-def CREATE TABLE (schema-only staged create — I5). Postgres targets still refuse
        // via catalog_handle/P11 inside execute_create_table.
        Statement::CreateTable(create) => {
            if let Some(message) =
                refuse_read_only_dml_table_sql(catalogs, &create.name.to_string())
            {
                return Err(DataFusionError::Plan(message));
            }
            create_table::execute_create_table(ctx, catalogs, create, &partitioning).await
        }
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            if_exists,
            ..
        } => execute_drop_table(ctx, catalogs, names, *if_exists).await,
        Statement::Drop {
            object_type: ObjectType::Schema | ObjectType::Database,
            names,
            if_exists,
            ..
        } => execute_drop_namespace(ctx, catalogs, names, *if_exists).await,
        Statement::AlterTable(alter_table) => {
            alter::execute_alter_table(ctx, catalogs, &alter_table.name, &alter_table.operations)
                .await
        }
        Statement::Merge(merge) => {
            if merge.output.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "MERGE OUTPUT/RETURNING clauses are not supported".to_string(),
                ));
            }
            merge::execute_merge(
                ctx,
                catalogs,
                &merge.table,
                &merge.source,
                &merge.on,
                &merge.clauses,
            )
            .await
        }
        // INSERT OVERWRITE: empty → probe/validate/self-scan provider wipe (C1-Q-001; not DELETE
        // — BUG-003). Non-empty → OV1 stage-then-swap (stream + commit_overwrite_replace_all).
        Statement::Insert(insert) if insert.overwrite => {
            execute_insert_overwrite(ctx, catalogs, sql, insert).await
        }
        // Non-overwrite INSERT would otherwise passthrough to DF and miss P11 for pg targets.
        Statement::Insert(insert) => {
            let refusal = match &insert.table {
                TableObject::TableName(name) => {
                    refuse_read_only_dml_table_sql(catalogs, &name.to_string())
                }
                TableObject::TableFunction(_) | TableObject::TableQuery(_) => None,
            };
            passthrough_after_p11(ctx, catalogs, sql, refusal).await
        }
        // DELETE/UPDATE — the guarded passthrough; the valve chain lives in `execute_delete` /
        // `execute_update` (kept out of this fn for clippy `too_many_lines`).
        Statement::Delete(delete) => execute_delete(ctx, catalogs, sql, delete).await,
        Statement::Update(update) => execute_update(ctx, catalogs, sql, update).await,
        // Iceberg `CALL catalog.system.<proc>(…)` — I3 / R-MAINTENANCE-CALL.
        // Seven procedures (six maintenance + `register_table`); unknown names refuse listing
        // the supported set.
        Statement::Call(function) => call::execute_call(ctx, catalogs, function).await,
        // `TRUNCATE TABLE` is planned (parity §2.3) but not wired — fail loud with a targeted
        // message rather than DF's opaque Unsupported (C4-L-001). Prefer empty INSERT OVERWRITE
        // (provider wipe) until a dedicated truncate action lands.
        Statement::Truncate { .. } => Err(DataFusionError::NotImplemented(
            "TRUNCATE TABLE is not supported yet — use INSERT OVERWRITE … SELECT … WHERE false \
             (empty overwrite wipe) or DELETE FROM <table> without a predicate \
             (docs/spark-sql-iceberg-parity.md §2.3 / P3)"
                .to_string(),
        )),
        _ => spark_ast::execute_passthrough(ctx, catalogs, sql).await,
    }
}

/// ===========================================================================================
/// `DELETE FROM …` — the three valves, then the passthrough. Order is load-bearing:
///
/// 1. **P11** read-only-catalog refuse (C2-L-001): the most fundamental "you cannot write here at
///    all"; a DELETE/UPDATE passthrough would otherwise miss it for postgres targets.
/// 2. **G3-E8** subquery-predicate refuse: a subquery in the `WHERE` clause is lost at
///    DataFusion's DML planning boundary and degenerates into match-all — silent whole-table
///    deletion (see [`crate::normalize::refuse_dml_subquery_predicate`]).
/// 3. **BUG-001** (r22 A2) merge-on-read + multi-spec history + currently-unpartitioned refuse
///    (fork DF position-delete unpartitioned fast path silently under-deletes). MERGE untouched.
///
/// (2) precedes (3) because both are data-loss valves and (2) is a pure sync AST walk while (3)
/// loads the target's Iceberg metadata (a network round-trip on Glue / S3 Tables) — cheap before
/// expensive. Pinned by `tests::dml::g3e8_subquery_valve_precedes_the_mor_multi_spec_valve`, and
/// mirrored on the ANSI door by `repark_sql`'s `mor_valve_runs_after_the_g3e8_valve`.
///
/// **(2) here is the EARLY call, not the load-bearing one.** The valve's authoritative attachment
/// is inside [`spark_ast::execute_passthrough`], on the executing parse — the only parse that
/// sees every DML route into DataFusion (F-A / panel L1 M-1). This call is kept solely so the
/// cheap sync refusal wins the ORDER above; deleting it would not open the hole, it would only
/// spend an Iceberg metadata load before refusing.
/// ===========================================================================================
async fn execute_delete(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    delete: &datafusion::sql::sqlparser::ast::Delete,
) -> Result<DataFrame> {
    if let Some(message) = refuse_read_only_dml_from_delete(catalogs, delete) {
        return Err(DataFusionError::Plan(message));
    }
    // ObjectName only — never TableWithJoins Display (aliases would under-refuse BUG-001).
    let object_name = delete_target_object_name(delete);
    {
        let as_statement = datafusion::sql::sqlparser::ast::Statement::Delete(delete.clone());
        if repark_iceberg::write::predicate_dml::try_allowed_delete_in(&as_statement)?.is_none() {
            refuse_dml_subquery_predicate(
                DmlSubqueryVerb::Delete,
                delete.selection.as_ref(),
                &object_name.map_or_else(|| "<table>".to_string(), ToString::to_string),
            )?;
        }
    }
    refuse_mor_unpartitioned_multi_spec_dml(catalogs, object_name, MorDmlKind::Delete).await?;
    // pins: v3r-1-rulings/C-001 — the passthrough seat of the V3-COW-1 guard.
    refuse_v3_cow_dml(catalogs, object_name, MorDmlKind::Delete).await?;
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

/// `UPDATE … SET …` — the same three valves in the same order as [`execute_delete`], reading the
/// target from the `TableWithJoins` primary relation and the predicate from `Update::selection`.
/// A subquery in a `SET` assignment is deliberately NOT gated (correct today, or a loud plan
/// error — never silently wrong; see the G3-E8 valve's doc and `task/g3e8-guard-ledger.md`).
async fn execute_update(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    update: &datafusion::sql::sqlparser::ast::Update,
) -> Result<DataFrame> {
    let object_name = object_name_from_table_with_joins(&update.table);
    let table_sql = object_name.map_or_else(|| update.table.to_string(), ToString::to_string);
    if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
        return Err(DataFusionError::Plan(message));
    }
    {
        let as_statement = datafusion::sql::sqlparser::ast::Statement::Update(update.clone());
        if repark_iceberg::write::predicate_dml::try_allowed_update_in(&as_statement)?.is_none() {
            refuse_dml_subquery_predicate(
                DmlSubqueryVerb::Update,
                update.selection.as_ref(),
                &table_sql,
            )?;
        }
    }
    refuse_mor_unpartitioned_multi_spec_dml(catalogs, object_name, MorDmlKind::Update).await?;
    // pins: v3r-1-rulings/C-002 — the passthrough seat of the V3-COW-1 guard.
    refuse_v3_cow_dml(catalogs, object_name, MorDmlKind::Update).await?;
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

/// Pre-`parse_single_normalized` intercepts: ALTER (I6 residual + I7), CREATE/DESCRIBE/SHOW
/// namespace, and snapshot-ref DDL (I5).
async fn try_preparse_intercepts(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Option<Result<DataFrame>> {
    // I7 — ADD/DROP/REPLACE PARTITION FIELD + REPLACE COLUMNS (stock sqlparser cannot model).
    if let Some(parsed) = alter::try_parse_iceberg_alter_ddl(sql) {
        return Some(match parsed {
            Ok(ddl) => alter::execute_iceberg_alter_ddl(ctx, catalogs, ddl).await,
            Err(error) => Err(error),
        });
    }
    // I6 residual — forms stock sqlparser still cannot model (WRITE ORDERED BY, ALTER COLUMN
    // COMMENT / MOVE). Loud NotImplemented rather than opaque parse fallthrough.
    if let Some(refused) = alter::refuse_unsupported_alter_sql(sql) {
        return Some(refused);
    }
    // `CREATE {NAMESPACE|SCHEMA|DATABASE}` with Spark's `LOCATION` / `COMMENT` /
    // `WITH DBPROPERTIES` / `WITH PROPERTIES` clauses — which sqlparser cannot model on
    // `CREATE SCHEMA`.
    if let Some(parsed) = try_parse_create_namespace(sql) {
        return Some(match parsed {
            Ok(create_namespace) => execute_create_namespace(ctx, catalogs, create_namespace).await,
            Err(error) => Err(error),
        });
    }
    // `DESCRIBE {NAMESPACE|DATABASE|SCHEMA} [EXTENDED]` (Group Z).
    if let Some(parsed) = describe_show::try_parse_describe_namespace(sql) {
        return Some(match parsed {
            Ok(describe_namespace) => {
                describe_show::execute_describe_namespace(ctx, catalogs, describe_namespace).await
            }
            Err(error) => Err(error),
        });
    }
    // `SHOW {NAMESPACES|SCHEMAS|DATABASES}` (Group AB).
    if let Some(parsed) = describe_show::try_parse_show_namespaces(sql) {
        return Some(match parsed {
            Ok(show_namespaces) => {
                describe_show::execute_show_namespaces(ctx, catalogs, show_namespaces).await
            }
            Err(error) => Err(error),
        });
    }
    // Snapshot-ref DDL (I5) — not modelled by stock sqlparser.
    if let Some(parsed) = ref_ddl::try_parse_ref_ddl(sql) {
        return Some(match parsed {
            Ok(ddl) => ref_ddl::execute_ref_ddl(ctx, catalogs, ddl).await,
            Err(error) => Err(error),
        });
    }
    None
}

/// Fall-through when `parse_single_normalized` returns `None` (MERGE / residual BRANCH|TAG / DF).
async fn execute_unparsable_fallthrough(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    if starts_with_merge(sql) {
        return Err(DataFusionError::Plan(
            "could not parse this MERGE INTO form (the supported surface + v1 limits are \
             tracked in docs/spark-sql-iceberg-parity.md §2.3 / task/todo.md)"
                .to_string(),
        ));
    }
    // Residual BRANCH|TAG shapes the dedicated parser missed — still fail loud (not ParserError).
    if starts_with_branch_or_tag_ddl(sql) {
        return Err(DataFusionError::NotImplemented(
            "this CREATE/DROP/REPLACE BRANCH|TAG form is not supported yet — supported: \
             ALTER TABLE t CREATE|DROP BRANCH|TAG [AS OF VERSION n] and CREATE|DROP BRANCH|TAG \
             name IN t (docs/spark-sql-iceberg-parity.md §2.2 / I5)"
                .to_string(),
        ));
    }
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

#[cfg(test)]
mod tests;
