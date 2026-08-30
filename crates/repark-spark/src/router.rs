//! The Spark SQL statement router.

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

/// Execute one Spark-SQL statement, routing Iceberg DDL and writes and passing reads to DataFusion.
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
/// # Errors
/// # Errors Any planning/execution error from the underlying statement, plus the P11 refusal.
pub async fn execute_with_read_only<S: std::hash::BuildHasher>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    read_only_catalogs: &HashSet<String, S>,
) -> Result<DataFrame> {
    // Canonicalize once at the Spark SQL front door so later tokenizers cannot re-process escapes.
    // Translate downstream parser locations back to the caller's SQL before returning an error.
    let canonical = crate::spark_literals::canonicalize(sql)?;
    let canonical_sql = canonical.as_ref();
    // Clone the registry snapshot so P11 survives `.await` thread hops.
    let mut catalogs = catalogs.clone();
    catalogs.set_read_only_catalogs(read_only_catalogs.iter().cloned().collect());
    // Refuse write-to-branch forms before metadata rewrite.
    if let Some(sniff) = ref_ddl::sniff_write_to_branch(canonical_sql) {
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
    // I2 / R-METADATA-TABLES — Spark `cat.ns.tbl.snapshots` → fork `cat.ns.tbl$snapshots`.
    let sql_after_meta: std::borrow::Cow<'_, str> =
        if metadata_tables::sql_may_have_metadata_table_path(canonical_sql) {
            match metadata_tables::prepare_metadata_table_sql(&catalogs, canonical_sql).await? {
                Some(rewritten) => std::borrow::Cow::Owned(rewritten),
                None => std::borrow::Cow::Borrowed(canonical_sql),
            }
        } else {
            std::borrow::Cow::Borrowed(canonical_sql)
        };
    // Release pinned relations after planning.
    let mut pinned = time_travel::PinnedViews::default();
    let original_for_locations =
        original_sql_for_locations(sql, canonical_sql, sql_after_meta.as_ref());
    let result = execute_time_travelled(
        ctx,
        &catalogs,
        sql_after_meta.as_ref(),
        original_for_locations,
        &mut pinned,
    )
    .await;
    pinned.release(ctx);
    result
}

/// Continue routing after the time-travel rewrite while preserving pinned-view cleanup.
async fn execute_time_travelled(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    original_for_locations: Option<&str>,
    pinned: &mut time_travel::PinnedViews,
) -> Result<DataFrame> {
    // Iceberg time travel is not modelled by Databricks-dialect sqlparser.
    let sql_storage: std::borrow::Cow<'_, str> = if time_travel::sql_has_time_travel(sql) {
        match time_travel::prepare_time_travel_sql(ctx, catalogs, sql, pinned).await? {
            Some(rewritten) => std::borrow::Cow::Owned(rewritten),
            None => std::borrow::Cow::Borrowed(sql),
        }
    } else {
        std::borrow::Cow::Borrowed(sql)
    };
    let result = execute_inner(ctx, catalogs, sql_storage.as_ref()).await;
    if let Some(original) = original_for_locations
        .and_then(|original| original_sql_for_locations(original, sql, sql_storage.as_ref()))
    {
        result.map_err(|error| {
            crate::spark_literals::translate_downstream_error(original, sql_storage.as_ref(), error)
        })
    } else {
        result
    }
}

fn original_sql_for_locations<'a>(original: &'a str, before: &str, after: &str) -> Option<&'a str> {
    (before == after).then_some(original)
}

async fn execute_inner(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    // Refuse genuine multi-statement scripts before any intercept or passthrough.
    refuse_multi_statement_sql(sql)?;
    // Pre-parse recognizers for forms stock sqlparser cannot model (or would drop clauses from).
    if let Some(frame) = try_preparse_intercepts(ctx, catalogs, sql).await {
        return frame;
    }
    // If we can't parse it to a single statement we recognise, let DataFusion have it.
    let Some((statement, partitioning)) = parse_single_normalized(sql)? else {
        return execute_unparsable_fallthrough(ctx, catalogs, sql).await;
    };
    // G15.
    crate::refuse_collation_in_statement(&statement)?;
    match &statement {
        Statement::CreateTable(create) if create.query.is_some() => {
            execute_ctas(ctx, catalogs, build_ctas(create, &partitioning)?).await
        }
        // Column-def CREATE TABLE (schema-only staged create — I5).
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
        // INSERT OVERWRITE: probe and validate before an empty-source wipe.
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
        // DELETE/UPDATE.
        Statement::Delete(delete) => execute_delete(ctx, catalogs, sql, delete).await,
        Statement::Update(update) => execute_update(ctx, catalogs, sql, update).await,
        // Iceberg `CALL catalog.system.<proc>(…)` — I3 / R-MAINTENANCE-CALL.
        Statement::Call(function) => call::execute_call(ctx, catalogs, function).await,
        // `TRUNCATE TABLE` is planned but not wired.
        Statement::Truncate { .. } => Err(DataFusionError::NotImplemented(
            "TRUNCATE TABLE is not supported yet — use INSERT OVERWRITE … SELECT … WHERE false \
             (empty overwrite wipe) or DELETE FROM <table> without a predicate \
             (docs/spark-sql-iceberg-parity.md §2.3 / P3)"
                .to_string(),
        )),
        _ => spark_ast::execute_passthrough(ctx, catalogs, sql).await,
    }
}

/// `DELETE FROM …` applies the write-safety valves before provider execution.
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
    refuse_mor_unpartitioned_multi_spec_dml(ctx, catalogs, object_name, MorDmlKind::Delete).await?;
    // pins: v3r-1-rulings/C-001 — the passthrough seat of the V3-COW-1 guard.
    refuse_v3_cow_dml(ctx, catalogs, object_name, MorDmlKind::Delete).await?;
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

/// `UPDATE … SET …`.
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
    refuse_mor_unpartitioned_multi_spec_dml(ctx, catalogs, object_name, MorDmlKind::Update).await?;
    // pins: v3r-1-rulings/C-002 — the passthrough seat of the V3-COW-1 guard.
    refuse_v3_cow_dml(ctx, catalogs, object_name, MorDmlKind::Update).await?;
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

/// Pre-`parse_single_normalized` intercepts: ALTER, CREATE/DESCRIBE/SHOW namespace.
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
    // I6 residual — forms stock sqlparser still cannot model.
    if let Some(refused) = alter::refuse_unsupported_alter_sql(sql) {
        return Some(refused);
    }
    // CREATE NAMESPACE LOCATION/COMMENT/WITH properties: sqlparser cannot model those clauses.
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
