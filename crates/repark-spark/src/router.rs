//! The Spark-SQL statement router: `execute` / `execute_with_read_only` / `execute_inner`.
//!
//! Ported from the v1 SQL crate's `lib.rs` (declared-rename unit; the crate root here is a
//! manifest per `scripts/check_lib_rs.py`, so the router body lives in this module). `execute`
//! parses the statement with DataFusion's `sqlparser`, intercepts the forms DataFusion cannot
//! execute against an Iceberg catalog, and passes everything else straight through.
//!
//! **PR-3a restores the DDL half** — CTAS, column-def CREATE TABLE, DROP TABLE, namespace DDL,
//! and ALTER run their v1 handlers. The remaining handler modules — MERGE INTO, INSERT
//! OVERWRITE, CALL, and branch/tag ref DDL — land in phase-2 PR-3b; until then their router
//! arms refuse loudly with [`DataFusionError::NotImplemented`] naming the construct (see
//! [`refuse_pending`]). The v1 write-to-branch sniff (`ref_ddl::sniff_write_to_branch`) is a
//! declared TEMPORARY omission restored with `ref_ddl` in PR-3b.
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
//! - **Metadata tables** (I2) — Spark `cat.ns.tbl.snapshots` → fork `cat.ns.tbl$snapshots`.
//! - **Time travel** (I1) — `VERSION AS OF` / `TIMESTAMP AS OF` / `FOR SYSTEM_*` rewritten to
//!   snapshot-pinned static providers before normal routing.
//! - **`TRUNCATE TABLE`** — targeted loud refuse (C4-L-001), verbatim from v1.
//!
//! Passthrough: `DELETE` / `UPDATE` / non-overwrite `INSERT INTO` ride DataFusion onto the fork
//! provider's DML (ADR-0003) behind the P11 read-only-catalog refuse and the r22 A2 BUG-001
//! merge-on-read multi-spec valve. Multi-statement SQL refuses first (BUG-010); the SEC-02
//! local-filesystem DDL gate runs inside the passthrough.

use std::collections::HashSet;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{ObjectType, Statement, TableObject};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{
    passthrough_after_p11, refuse_read_only_dml_from_delete, refuse_read_only_dml_table_sql,
};
use crate::normalize::{
    MorDmlKind, delete_target_object_name, object_name_from_table_with_joins,
    parse_single_normalized, refuse_mor_unpartitioned_multi_spec_dml, refuse_multi_statement_sql,
    starts_with_branch_or_tag_ddl, starts_with_merge,
};
use crate::{
    alter, build_ctas, create_table, describe_show, execute_create_namespace, execute_ctas,
    execute_drop_namespace, execute_drop_table, metadata_tables, spark_ast, time_travel,
    try_parse_create_namespace,
};

/// ===========================================================================================
/// Execute one Spark-SQL statement against `ctx`, routing the Iceberg DDL/write forms to their
/// handlers and passing everything else (reads, `INSERT INTO`, …) to DataFusion.
///
/// PR-3a: interception is live for CTAS (decomposed), column-def CREATE TABLE, DROP TABLE,
/// CREATE/DROP NAMESPACE|DATABASE, ALTER TABLE (I6/I7), DESCRIBE/SHOW namespace forms, metadata
/// tables, time travel, TRUNCATE (targeted refuse), and the DML passthrough guards; the MERGE /
/// INSERT OVERWRITE / CALL / ref-DDL handlers refuse loudly until phase-2 PR-3b restores them
/// (see the module docs).
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
    // TEMPORARY (declared PR-2, still open at PR-3a): v1 runs the r25 T2 write-to-branch STOP
    // here via
    // `ref_ddl::sniff_write_to_branch`. The `ref_ddl` module lands in phase-2 PR-3b; the sniff
    // is restored verbatim with it (ledger-declared omission — branch-suffixed write targets
    // fall through to planning's own "table not found" until then).
    //
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
    // Iceberg time travel (`VERSION AS OF` / `TIMESTAMP AS OF` / `FOR SYSTEM_* AS OF`) is not
    // modelled by Databricks-dialect sqlparser. Rewrite to snapshot-pinned static providers
    // (fork `IcebergStaticTableProvider::try_new_from_table_snapshot`) before normal routing.
    // I1 / R-TIME-TRAVEL — kept out of `execute_inner` so the router stays under clippy
    // `too_many_lines`.
    let sql_storage: std::borrow::Cow<'_, str> =
        if time_travel::sql_has_time_travel(sql_after_meta.as_ref()) {
            match time_travel::prepare_time_travel_sql(ctx, &catalogs, sql_after_meta.as_ref())
                .await?
            {
                Some(rewritten) => std::borrow::Cow::Owned(rewritten),
                None => sql_after_meta,
            }
        } else {
            sql_after_meta
        };
    execute_inner(ctx, &catalogs, sql_storage.as_ref()).await
}

/// TEMPORARY (PR-2 refuse-arm class, PR-3a residue): a loud `NotImplemented` for a router arm
/// whose handler module arrives in phase-2 PR-3b (MERGE, INSERT OVERWRITE, CALL, ref DDL).
/// Every call site names the construct and the restoring PR; each arm carries a refuse test in
/// `router/tests.rs`.
fn refuse_pending(construct: &str, restoring_pr: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{construct} is not available in this build yet — the handler lands in phase-2 \
         {restoring_pr}"
    ))
}

async fn execute_inner(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    // r22 A2 / BUG-010: refuse genuine multi-statement scripts before any intercept/passthrough.
    // Trailing `;` / whitespace / comments after a single statement remain allowed (Spark oracle).
    refuse_multi_statement_sql(sql)?;
    // Pre-parse recognizers (alter I7/I6-residual, create-namespace, describe/show) + the PR-3b
    // pre-parse refuse set (see `try_preparse_intercepts`).
    if let Some(frame) = try_preparse_intercepts(ctx, catalogs, sql).await {
        return frame;
    }
    // If we can't parse it to a single statement we recognise, let DataFusion have it (passthrough)
    // — except MERGE / residual BRANCH|TAG, which get targeted errors instead of parse fails.
    let Some((statement, partitioning)) = parse_single_normalized(sql)? else {
        return execute_unparsable_fallthrough(ctx, catalogs, sql).await;
    };
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
        // TEMPORARY refuse arms (PR-3a residue): the DML/ref handlers land in PR-3b.
        Statement::Merge(_) => Err(refuse_pending("MERGE INTO", "PR-3b (merge)")),
        Statement::Insert(insert) if insert.overwrite => Err(refuse_pending(
            "INSERT OVERWRITE",
            "PR-3b (insert_overwrite)",
        )),
        Statement::Call(_) => Err(refuse_pending(
            "CALL (Iceberg maintenance procedures)",
            "PR-3b (call)",
        )),
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
        // DELETE/UPDATE passthrough would also miss P11 for pg targets (C2-L-001).
        // r22 A2 / BUG-001: MoR + multi-spec history + current unpartitioned → refuse loud
        // (fork DF position-delete unpartitioned fast path silent under-delete). MERGE untouched.
        Statement::Delete(delete) => {
            if let Some(message) = refuse_read_only_dml_from_delete(catalogs, delete) {
                return Err(DataFusionError::Plan(message));
            }
            // ObjectName only — never TableWithJoins Display (aliases would under-refuse BUG-001).
            refuse_mor_unpartitioned_multi_spec_dml(
                catalogs,
                delete_target_object_name(delete),
                MorDmlKind::Delete,
            )
            .await?;
            spark_ast::execute_passthrough(ctx, catalogs, sql).await
        }
        Statement::Update(update) => {
            let object_name = object_name_from_table_with_joins(&update.table);
            let table_sql =
                object_name.map_or_else(|| update.table.to_string(), ToString::to_string);
            if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
                return Err(DataFusionError::Plan(message));
            }
            refuse_mor_unpartitioned_multi_spec_dml(catalogs, object_name, MorDmlKind::Update)
                .await?;
            spark_ast::execute_passthrough(ctx, catalogs, sql).await
        }
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

/// Pre-`parse_single_normalized` intercepts: ALTER (I6 residual + I7), CREATE/DESCRIBE/SHOW
/// namespace, plus the PR-3a-residue refuse for snapshot-ref DDL (I5 — the `ref_ddl` module
/// lands in PR-3b, so its statement shapes refuse loudly here; TEMPORARY refuse-arm class).
async fn try_preparse_intercepts(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Option<Result<DataFrame>> {
    // Snapshot-ref DDL (I5) — `CREATE|DROP|REPLACE BRANCH|TAG` forms, both top-level and
    // ALTER-scoped (PR-3b). Checked FIRST so the ref-DDL shapes name their construct instead of
    // reaching the live ALTER handlers. The bare `REPLACE BRANCH|TAG` form (v1:
    // `ref_ddl::try_parse_ref_ddl`) is sniffed explicitly — the normalize sniff covers only the
    // CREATE/DROP and ALTER-scoped shapes.
    if starts_with_branch_or_tag_ddl(sql)
        || starts_with_keywords(sql, &["REPLACE", "BRANCH"])
        || starts_with_keywords(sql, &["REPLACE", "TAG"])
    {
        return Some(Err(refuse_pending(
            "CREATE/DROP/REPLACE BRANCH|TAG (snapshot-ref DDL)",
            "PR-3b (ref_ddl)",
        )));
    }
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
    None
}

/// Token-level "statement starts with these keyword words" sniff (case-insensitive, tolerant of
/// leading whitespace/comments — the `starts_with_merge` pattern). PR-3b refuse-arm recognizer (ref DDL).
fn starts_with_keywords(sql: &str, keywords: &[&str]) -> bool {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    let mut words = tokens.iter().filter_map(|token| match token {
        Token::Word(word) => Some(word.value.as_str()),
        _ => None,
    });
    keywords.iter().all(|keyword| {
        words
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case(keyword))
    })
}

/// Fall-through when `parse_single_normalized` returns `None` (MERGE / residual BRANCH|TAG / DF).
async fn execute_unparsable_fallthrough(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    if starts_with_merge(sql) {
        return Err(refuse_pending("MERGE INTO", "PR-3b (merge)"));
    }
    // Residual BRANCH|TAG shapes reaching here — v1's defense-in-depth arm, kept so an
    // unparsable ref-DDL form never falls through to an opaque DataFusion parse error.
    if starts_with_branch_or_tag_ddl(sql) {
        return Err(refuse_pending(
            "CREATE/DROP/REPLACE BRANCH|TAG (snapshot-ref DDL)",
            "PR-3b (ref_ddl)",
        ));
    }
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

#[cfg(test)]
mod tests;
