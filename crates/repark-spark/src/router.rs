//! The Spark SQL statement router.

use std::collections::HashSet;

use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Insert, ObjectType, Statement, TableObject};
use repark_core::{CatalogRegistry, TempViewSession};

use crate::{
    DmlSubqueryVerb, MorDmlKind, alter, alter_write_order, build_ctas, call, column_move,
    create_table, delete_target_object_name, describe_show, execute_alter_namespace,
    execute_append_with_options, execute_create_namespace, execute_ctas, execute_drop_namespace,
    execute_drop_table, execute_insert_overwrite, execute_truncate, insert_arity, merge,
    metadata_tables, nested_column_ddl, object_name_from_table_with_joins, parse_single_normalized,
    passthrough_after_p11, ref_ddl, refuse_dml_subquery_predicate,
    refuse_mor_unpartitioned_multi_spec_dml, refuse_multi_statement_sql,
    refuse_read_only_dml_from_delete, refuse_read_only_dml_table_sql, spark_ast,
    starts_with_branch_or_tag_ddl, starts_with_merge, time_travel, try_parse_alter_namespace,
    try_parse_create_namespace, wap, write_to_branch,
};

mod comment_on_table;
mod hive_change_column;
pub(crate) mod insert_positional;
mod table_props_ddl;

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

#[allow(clippy::missing_errors_doc)]
pub async fn execute_static_overwrite<S: std::hash::BuildHasher>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    read_only_catalogs: &HashSet<String, S>,
) -> Result<DataFrame> {
    let write_options = crate::write_options::StatementWriteOptions {
        overwrite_intent: repark_iceberg::write::OverwriteIntent::Static,
        ..crate::write_options::StatementWriteOptions::empty()
    };
    execute_with_statement_options(ctx, catalogs, sql, read_only_catalogs, &write_options).await
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
    execute_with_statement_options(
        ctx,
        catalogs,
        sql,
        read_only_catalogs,
        &crate::write_options::StatementWriteOptions::empty(),
    )
    .await
}

#[allow(clippy::missing_errors_doc)]
pub async fn execute_with_statement_options<S: std::hash::BuildHasher>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    read_only_catalogs: &HashSet<String, S>,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<DataFrame> {
    execute_in_session(ctx, catalogs, sql, read_only_catalogs, write_options, None).await
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_in_session<S: std::hash::BuildHasher>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    read_only_catalogs: &HashSet<String, S>,
    write_options: &crate::write_options::StatementWriteOptions,
    temp_views: Option<&dyn TempViewSession>,
) -> Result<DataFrame> {
    crate::normalize::refuse_unclosed_bracketed_comment(sql)?;
    if crate::show_create::starts_with_show_create_table(sql)
        && let Some(Err(error)) = crate::show_create::try_parse_show_create(sql)
    {
        return Err(error);
    }
    if let Some(Err(error)) = describe_show::try_parse_describe_table(sql) {
        return Err(error);
    }
    // Canonicalize once at the Spark SQL front door so later tokenizers cannot process escapes again.
    // Translate downstream parser locations back to the caller's SQL before returning an error.
    let verbatim =
        crate::spark_literals::escaped_verbatim_from_options(ctx.state().config().options());
    let canonical = crate::alter_write_order::verbatim_write_order_sql(sql)
        .map_or_else(
            || crate::spark_literals::canonicalize_verbatim(sql, verbatim),
            Ok,
        )
        .map_err(|error| crate::show_table_extended::refusal_or(sql, error))?;
    let canonical_sql = canonical.as_ref();
    // Clone the registry snapshot so P11 survives `.await` thread hops.
    let mut catalogs = catalogs.clone();
    catalogs.set_read_only_catalogs(read_only_catalogs.iter().cloned().collect());
    let temp = crate::view_ddl::temp_ddl::route_temp_view_statement;
    if let Some(outcome) = Box::pin(temp(
        ctx,
        &catalogs,
        canonical_sql,
        write_options,
        temp_views,
    ))
    .await
    {
        return outcome;
    }
    execute_calibrated(ctx, &catalogs, canonical_sql, Some(sql), write_options).await
}

async fn execute_calibrated(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    canonical_sql: &str,
    original_sql: Option<&str>,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<DataFrame> {
    if crate::view_ddl::parse::is_create_view_statement(canonical_sql) {
        return Box::pin(execute_inner(ctx, catalogs, canonical_sql, write_options)).await;
    }
    // I2 / R-METADATA-TABLES — Spark `cat.ns.tbl.snapshots` → fork `cat.ns.tbl$snapshots`.
    let sql_after_meta: std::borrow::Cow<'_, str> =
        if crate::describe_show::metadata_table::rewrites_metadata_path(canonical_sql) {
            match metadata_tables::prepare_metadata_table_sql(catalogs, canonical_sql).await? {
                Some(rewritten) => std::borrow::Cow::Owned(rewritten),
                None => std::borrow::Cow::Borrowed(canonical_sql),
            }
        } else {
            std::borrow::Cow::Borrowed(canonical_sql)
        };
    // Release pinned relations after planning.
    let mut pinned = time_travel::PinnedViews::default();
    let sql_after_changes: std::borrow::Cow<'_, str> =
        if time_travel::changes::sql_may_have_changes_relation(sql_after_meta.as_ref()) {
            match time_travel::changes::prepare_changes_sql(
                ctx,
                catalogs,
                sql_after_meta.as_ref(),
                &mut pinned,
            )
            .await?
            {
                Some(rewritten) => std::borrow::Cow::Owned(rewritten),
                None => std::borrow::Cow::Borrowed(sql_after_meta.as_ref()),
            }
        } else {
            std::borrow::Cow::Borrowed(sql_after_meta.as_ref())
        };
    let sql_after_branch = write_to_branch::apply_write_to_branch(
        ctx,
        catalogs,
        sql_after_changes.as_ref(),
        &mut pinned,
        !write_options.is_empty(),
    )
    .await?;
    let sql_after_wap_read =
        wap::apply_wap_read_redirect(ctx, catalogs, sql_after_branch.as_ref(), &mut pinned).await?;
    let routed_sql = sql_after_wap_read
        .as_deref()
        .unwrap_or_else(|| sql_after_branch.as_ref());
    let mut lineage_pins = repark_core::LineagePins::default();
    let mut metadata_column_pins = repark_core::MetadataColumnPins::default();
    let original_for_locations =
        original_sql.and_then(|sql| original_sql_for_locations(sql, canonical_sql, routed_sql));
    let result = Box::pin(execute_time_travelled(
        ctx,
        catalogs,
        routed_sql,
        original_for_locations,
        &mut pinned,
        &mut lineage_pins,
        &mut metadata_column_pins,
        write_options,
    ))
    .await;
    metadata_column_pins.release(ctx);
    lineage_pins.release(ctx);
    pinned.release(ctx);
    result
}

/// Continue routing after the time-travel rewrite while preserving pinned-view cleanup.
#[allow(clippy::too_many_arguments)]
async fn execute_time_travelled(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    original_for_locations: Option<&str>,
    pinned: &mut time_travel::PinnedViews,
    lineage_pins: &mut repark_core::LineagePins,
    metadata_column_pins: &mut repark_core::MetadataColumnPins,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<DataFrame> {
    // Iceberg time travel is not modelled by Databricks-dialect sqlparser.
    let sql_after_tt: std::borrow::Cow<'_, str> = if time_travel::sql_has_time_travel(sql) {
        match time_travel::prepare_time_travel_sql(ctx, catalogs, sql, pinned).await? {
            Some(rewritten) => std::borrow::Cow::Owned(rewritten),
            None => std::borrow::Cow::Borrowed(sql),
        }
    } else {
        std::borrow::Cow::Borrowed(sql)
    };
    let dialect = datafusion::sql::sqlparser::dialect::DatabricksDialect {};
    let sql_after_mc: std::borrow::Cow<'_, str> = match repark_core::prepare_metadata_column_sql(
        ctx,
        catalogs,
        sql_after_tt.as_ref(),
        &dialect,
        metadata_column_pins,
    )
    .await?
    {
        Some(rewritten) => std::borrow::Cow::Owned(rewritten),
        None => sql_after_tt,
    };
    let sql_storage: std::borrow::Cow<'_, str> = match repark_core::prepare_lineage_sql(
        ctx,
        catalogs,
        sql_after_mc.as_ref(),
        &dialect,
        lineage_pins,
    )
    .await?
    {
        Some(rewritten) => std::borrow::Cow::Owned(rewritten),
        None => sql_after_mc,
    };
    let result = execute_inner(ctx, catalogs, sql_storage.as_ref(), write_options).await;
    if let Some(original) = original_for_locations
        .and_then(|original| original_sql_for_locations(original, sql, sql_storage.as_ref()))
    {
        let verbatim =
            crate::spark_literals::escaped_verbatim_from_options(ctx.state().config().options());
        result.map_err(|error| {
            crate::spark_literals::translate_downstream_error_verbatim(
                original,
                sql_storage.as_ref(),
                error,
                verbatim,
            )
        })
    } else {
        result
    }
}

fn original_sql_for_locations<'a>(original: &'a str, before: &str, after: &str) -> Option<&'a str> {
    (before == after).then_some(original)
}

pub(crate) fn rewrite_sql_for_execute(sql: &str, catalogs: &CatalogRegistry) -> String {
    let rewritten = repark_functions::cast_map::rewrite_map_casts(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);
    let system_rewritten = crate::describe_show::rewrite_system_function_calls(sql, |name| {
        catalogs.get(name).is_some()
    });
    system_rewritten.unwrap_or_else(|| sql.to_owned())
}

#[allow(clippy::too_many_lines)]
async fn execute_inner(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<DataFrame> {
    crate::view_ddl::read::ensure_view_wrappers(ctx, catalogs)?;
    let rewritten_sql = rewrite_sql_for_execute(sql, catalogs);
    let sql = rewritten_sql.as_str();
    let evolving = merge::schema_evolution::strip_schema_evolution(sql);
    let schema_evolution = evolving.is_some();
    let sql = evolving.as_deref().unwrap_or(sql);
    // Refuse genuine multi-statement scripts before any intercept or passthrough.
    refuse_multi_statement_sql(sql)?;
    if let Some(replace) = insert_positional::replace_where::parse_replace_where(sql)? {
        let execute = insert_positional::replace_where::execute_replace_where;
        return Box::pin(execute(ctx, catalogs, replace, write_options)).await;
    }
    if let Some(stripped) = crate::insert_by_name::strip_insert_by_name(sql)? {
        return Box::pin(crate::insert_by_name::execute_insert_by_name(
            ctx,
            catalogs,
            &stripped,
            write_options,
        ))
        .await;
    }
    // Pre-parse recognizers for forms stock sqlparser cannot model (or would drop clauses from).
    if let Some(frame) = try_preparse_intercepts(ctx, catalogs, sql, write_options).await {
        return frame;
    }
    // If we can't parse it to a single statement we recognise, let DataFusion have it.
    let Some((statement, partitioning, clauses)) = parse_single_normalized(sql)? else {
        write_options.refuse_if_non_empty("this INSERT form")?;
        return execute_unparsable_fallthrough(ctx, catalogs, sql).await;
    };
    // G15.
    crate::refuse_collation_in_statement(&statement)?;
    crate::refuse_declared_function_in_statement(&statement)?;
    refuse_options_on_non_write(&statement, write_options)?;
    if let Statement::CreateTable(create) = &statement {
        crate::normalize::replace_table::refuse_missing_replace_target(catalogs, sql, &create.name)
            .await?;
    }
    match &statement {
        Statement::CreateTable(create) if create.query.is_some() => {
            execute_ctas(
                ctx,
                catalogs,
                build_ctas(catalogs, create, &partitioning, &clauses)?,
                write_options,
            )
            .await
        }
        // Column-def CREATE TABLE (schema-only staged create — I5).
        Statement::CreateTable(create) => {
            if let Some(message) =
                refuse_read_only_dml_table_sql(catalogs, &create.name.to_string())
            {
                return Err(DataFusionError::Plan(message));
            }
            write_options.refuse_if_non_empty("CREATE TABLE without AS SELECT")?;
            create_table::execute_create_table(ctx, catalogs, create, &partitioning, &clauses).await
        }
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            if_exists,
            purge,
            ..
        } => execute_drop_table(ctx, catalogs, names, *if_exists, *purge).await,
        Statement::Drop {
            object_type: ObjectType::View,
            names,
            if_exists,
            temporary: false,
            ..
        } => crate::view_ddl::execute::execute_drop_view(ctx, catalogs, names, *if_exists).await,
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
            merge::execute_merge_statement(ctx, catalogs, merge, schema_evolution).await
        }
        // Non-overwrite INSERT would otherwise passthrough to DF and miss P11 for pg targets.
        Statement::Insert(insert) => {
            crate::view_dispatch::refuse_insert_into_view(ctx, catalogs, insert).await?;
            execute_insert_routed(ctx, catalogs, sql, insert, write_options).await
        }
        // DELETE/UPDATE.
        Statement::Delete(delete) => execute_delete(ctx, catalogs, sql, delete).await,
        Statement::Update(update) => execute_update(ctx, catalogs, sql, update).await,
        // Iceberg `CALL catalog.system.<proc>(…)` — I3 / R-MAINTENANCE-CALL.
        Statement::Call(function) => call::execute_call(ctx, catalogs, function).await,
        Statement::Truncate(truncate) => execute_truncate(ctx, catalogs, truncate).await,
        Statement::Use(target) => crate::use_ddl::execute_use(ctx, catalogs, target).await,
        Statement::ShowCatalogs { terse: true, .. }
        | Statement::ShowTables { terse: true, .. }
        | Statement::ShowColumns { extended: true, .. }
        | Statement::ShowColumns { full: true, .. } => {
            Err(crate::use_ddl::show_parse_refusal("SHOW ..."))
        }
        Statement::ShowCatalogs { show_options, .. } => {
            crate::use_ddl::execute_show_catalogs(ctx, catalogs, show_options)
        }
        Statement::ShowTables { show_options, .. } => {
            crate::use_ddl::execute_show_tables(ctx, catalogs, show_options).await
        }
        Statement::ShowColumns { show_options, .. } => {
            crate::use_ddl::execute_show_columns(ctx, catalogs, show_options).await
        }
        _ => spark_ast::execute_passthrough(ctx, catalogs, sql).await,
    }
}

async fn execute_insert_routed(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    insert: &Insert,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<DataFrame> {
    if crate::insert_by_name::evolution::routes_positional_by_name(
        ctx,
        catalogs,
        insert,
        write_options,
    )
    .await?
    {
        return Box::pin(crate::insert_by_name::execute_insert_by_name(
            ctx,
            catalogs,
            sql,
            write_options,
        ))
        .await;
    }
    let prepared = insert_positional::prepare_positional_insert(ctx, catalogs, insert).await?;
    let (sql, insert) = prepared
        .as_ref()
        .map_or((sql, insert), |p| (p.sql.as_str(), &p.insert));
    if insert.overwrite {
        return execute_insert_overwrite(ctx, catalogs, sql, insert, write_options).await;
    }
    insert_arity::refuse_if_short_values(ctx, catalogs, insert).await?;
    if !write_options.is_empty() || prepared.as_ref().is_some_and(|p| p.owned_append) {
        return execute_append_with_options(ctx, catalogs, sql, insert, write_options).await;
    }
    if repark_iceberg::write::session_write_conf_is_set(ctx)
        && let TableObject::TableName(name) = &insert.table
        && crate::insert_overwrite::try_resolve_iceberg_overwrite_target(ctx, catalogs, name)
            .await?
            .is_some()
    {
        return execute_append_with_options(ctx, catalogs, sql, insert, write_options).await;
    }
    let refusal = match &insert.table {
        TableObject::TableName(name) => refuse_read_only_dml_table_sql(catalogs, &name.to_string()),
        TableObject::TableFunction(_) | TableObject::TableQuery(_) => None,
    };
    passthrough_after_p11(ctx, catalogs, sql, refusal).await
}

fn refuse_options_on_non_write(
    statement: &Statement,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<()> {
    let context = match statement {
        Statement::CreateTable(_) | Statement::Insert(_) => return Ok(()),
        Statement::Merge(_) => "MERGE INTO",
        Statement::Delete(_) => "DELETE FROM",
        Statement::Update(_) => "UPDATE",
        Statement::Truncate(_) => "TRUNCATE TABLE",
        Statement::Call(_) => "CALL",
        Statement::Drop { .. } => "DROP",
        Statement::AlterTable(_) => "ALTER TABLE",
        _ => "this statement",
    };
    write_options.refuse_if_non_empty(context)
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
    if let Some(name) = delete_target_object_name(delete) {
        crate::view_ddl::execute::refuse_view_write_target(ctx, catalogs, name).await?;
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
    if try_metadata_delete_door(ctx, catalogs, delete).await? {
        return ctx.read_empty();
    }
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

async fn try_metadata_delete_door(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    delete: &datafusion::sql::sqlparser::ast::Delete,
) -> Result<bool> {
    let statement = datafusion::sql::sqlparser::ast::Statement::Delete(delete.clone());
    let Some(target) = repark_iceberg::write::meta_delete::try_meta_delete_target(&statement)?
    else {
        return Ok(false);
    };
    if catalogs.get(&target.catalog_name).is_none() {
        return Ok(false);
    }
    let handle = crate::catalog_handle(catalogs, &target.catalog_name)?;
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    repark_iceberg::write::meta_delete::try_metadata_delete(handle, &target, case_insensitive).await
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
    if let Some(name) = object_name {
        crate::view_ddl::execute::refuse_view_write_target(ctx, catalogs, name).await?;
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
    let folded = crate::update_cast::refuse_cast_then_fold_nested(ctx, catalogs, update).await?;
    spark_ast::execute_passthrough(ctx, catalogs, folded.as_deref().unwrap_or(sql)).await
}

fn show_partitions_preparse(
    sql: &str,
    refuse_options: impl FnOnce(&str) -> Result<()>,
) -> Option<Result<DataFrame>> {
    let show = describe_show::try_parse_show_partitions(sql)?;
    match refuse_options("SHOW PARTITIONS") {
        Ok(()) => Some(Err(describe_show::show_partitions_refusal(&show))),
        Err(error) => Some(Err(error)),
    }
}

async fn describe_namespace_preparse(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    refuse_options: impl FnOnce(&str) -> Result<()>,
) -> Option<Result<DataFrame>> {
    let parsed = describe_show::try_parse_describe_namespace(sql)?;
    Some(
        match parsed.and_then(|ddl| refuse_options("DESCRIBE NAMESPACE").map(|()| ddl)) {
            Ok(describe_namespace) => {
                describe_show::execute_describe_namespace(ctx, catalogs, describe_namespace).await
            }
            Err(error) => Err(error),
        },
    )
}

async fn describe_table_resolves_in_session(ctx: &SessionContext, table: &str) -> bool {
    ctx.table_provider(TableReference::Bare {
        table: table.into(),
    })
    .await
    .is_ok()
}

async fn try_describe_table_intercept(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let parsed = describe_show::try_parse_describe_table(sql).or_else(|| {
        crate::describe_show::metadata_table::try_parse_describe_metadata_table(sql).map(Ok)
    })?;
    let mut describe_table = match parsed.and_then(|ddl| {
        write_options
            .refuse_if_non_empty("DESCRIBE TABLE")
            .map(|()| ddl)
    }) {
        Ok(describe_table) => describe_table,
        Err(error) => return Some(Err(error)),
    };
    let shadowed = describe_table.catalog.is_empty()
        && describe_table.namespace.is_empty()
        && describe_table_resolves_in_session(ctx, &describe_table.table).await;
    describe_table.complete_from_session(ctx);
    if shadowed || catalogs.get(&describe_table.catalog).is_none() {
        return None;
    }
    Some(describe_show::execute_describe_table(ctx, catalogs, describe_table).await)
}

async fn try_refresh_intercept(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let parsed = crate::use_ddl::try_parse_refresh(sql)?;
    let target = match parsed.and_then(|target| {
        write_options
            .refuse_if_non_empty("REFRESH TABLE")
            .map(|()| target)
    }) {
        Ok(target) => target,
        Err(error) => return Some(Err(error)),
    };
    Some(crate::use_ddl::execute_refresh(ctx, catalogs, target).await)
}

#[derive(Clone, Copy)]
pub(crate) enum PlannerDefaultSide {
    Catalog,
    Namespace,
}

pub(crate) fn planner_default_set_side(sql: &str) -> Option<PlannerDefaultSide> {
    let keyword_start = crate::show_create::skip_sql_whitespace_and_comments(sql, 0)?;
    let keyword_end = keyword_start + 3;
    let head = sql.get(keyword_start..keyword_end)?;
    if !head.eq_ignore_ascii_case("set") {
        return None;
    }
    if sql
        .as_bytes()
        .get(keyword_end)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        return None;
    }
    let key_start = crate::show_create::skip_sql_whitespace_and_comments(sql, keyword_end)?;
    for (key, side) in [
        (
            "datafusion.catalog.default_catalog",
            PlannerDefaultSide::Catalog,
        ),
        (
            "datafusion.catalog.default_schema",
            PlannerDefaultSide::Namespace,
        ),
    ] {
        let key_end = key_start + key.len();
        if sql
            .get(key_start..key_end)
            .is_some_and(|head| head.eq_ignore_ascii_case(key))
            && crate::show_create::skip_sql_whitespace_and_comments(sql, key_end)
                .and_then(|equals| sql.get(equals..))
                .is_some_and(|tail| tail.starts_with('='))
        {
            return Some(side);
        }
    }
    None
}

async fn try_use_default_intercept(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Option<Result<DataFrame>> {
    if !crate::use_ddl::is_use_default(sql) {
        return None;
    }
    Some(
        crate::use_ddl::execute_use(
            ctx,
            catalogs,
            &datafusion::sql::sqlparser::ast::Use::Default,
        )
        .await,
    )
}

async fn try_planner_default_set_mirror(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let side = planner_default_set_side(sql)?;
    if let Err(error) = write_options.refuse_if_non_empty("SET") {
        return Some(Err(error));
    }
    let frame = match spark_ast::execute_passthrough(ctx, catalogs, sql).await {
        Ok(frame) => frame,
        Err(error) => return Some(Err(error)),
    };
    let planner = ctx.copied_config().options().catalog.clone();
    let (current_catalog, current_namespace) = crate::use_ddl::session_defaults(catalogs);
    match side {
        PlannerDefaultSide::Catalog => crate::use_ddl::set_session_defaults(
            ctx,
            catalogs,
            &planner.default_catalog,
            &current_namespace,
        ),
        PlannerDefaultSide::Namespace => crate::use_ddl::set_session_defaults(
            ctx,
            catalogs,
            &current_catalog,
            &planner.default_schema,
        ),
    }
    Some(Ok(frame))
}

/// Pre-`parse_single_normalized` intercepts: ALTER, CREATE/DESCRIBE/SHOW namespace.
async fn try_alter_intercepts(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let parsed_ddl = |context: &str| write_options.refuse_if_non_empty(context);
    if let Some(parsed) = alter::try_parse_iceberg_alter_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => alter::execute_iceberg_alter_ddl(ctx, catalogs, ddl).await,
                Err(error) => Err(error),
            },
        );
    }
    if let Some(parsed) = alter_write_order::try_parse_write_order_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => alter_write_order::execute_write_order_ddl(ctx, catalogs, ddl).await,
                Err(error) => Err(error),
            },
        );
    }
    if let Some(parsed) = nested_column_ddl::try_parse_nested_column_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => nested_column_ddl::execute_nested_column_ddl(ctx, catalogs, ddl).await,
                Err(error) => Err(error),
            },
        );
    }
    if let Some(parsed) = column_move::try_parse_column_move_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => column_move::execute_column_move_ddl(ctx, catalogs, ddl).await,
                Err(error) => Err(error),
            },
        );
    }
    if let Some(parsed) = crate::table_props_ddl::try_parse_set_identifier_fields_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => {
                    crate::table_props_ddl::execute_identifier_fields_ddl(ctx, catalogs, ddl).await
                }
                Err(error) => Err(error),
            },
        );
    }
    if let Some(parsed) = crate::table_props_ddl::try_parse_drop_identifier_fields_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => {
                    crate::table_props_ddl::execute_identifier_fields_ddl(ctx, catalogs, ddl).await
                }
                Err(error) => Err(error),
            },
        );
    }
    if let Some(parsed) = table_props_ddl::try_parse_set_location_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => table_props_ddl::execute_set_location_ddl(ctx, catalogs, ddl).await,
                Err(error) => Err(error),
            },
        );
    }
    None
}

#[allow(clippy::too_many_lines)]
async fn try_preparse_intercepts(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let parsed_ddl = |context: &str| write_options.refuse_if_non_empty(context);
    if let Some(parsed) = crate::view_ddl::parse::try_parse_create_view(sql) {
        let statement = match parsed.and_then(|create| parsed_ddl("CREATE VIEW").map(|()| create)) {
            Ok(statement) => statement,
            Err(error) => return Some(Err(error)),
        };
        return Some(crate::view_ddl::execute::execute_create_view(ctx, catalogs, statement).await);
    }
    if let Some(error) = crate::view_ddl::parse::try_parse_alter_view_as(sql) {
        return Some(Err(error));
    }
    if let Some(parsed) = crate::view_ddl::parse::try_parse_alter_view(sql) {
        let statement = match parsed.and_then(|alter| parsed_ddl("ALTER VIEW").map(|()| alter)) {
            Ok(statement) => statement,
            Err(error) => return Some(Err(error)),
        };
        return Some(crate::view_ddl::execute::execute_alter_view(ctx, catalogs, statement).await);
    }
    if let Some(parsed) = crate::view_ddl::parse::try_parse_show_tblproperties(sql) {
        match parsed {
            Err(error) => return Some(Err(error)),
            Ok(statement) => {
                if let Some(result) =
                    crate::view_ddl::execute::execute_show_tblproperties(ctx, catalogs, statement)
                        .await
                {
                    return Some(result);
                }
            }
        }
    }
    if let Some(parsed) = crate::view_ddl::parse::try_parse_show_views(sql) {
        let statement = match parsed {
            Ok(statement) => statement,
            Err(error) => return Some(Err(error)),
        };
        return Some(crate::view_ddl::execute::execute_show_views(ctx, catalogs, statement).await);
    }
    if let Some(outcome) = try_alter_intercepts(ctx, catalogs, sql, write_options).await {
        return Some(outcome);
    }
    if let Some(frame) = try_preparse_comment_ddl(ctx, catalogs, sql, write_options).await {
        return Some(frame);
    }
    if let Some(refused) = nested_column_ddl::residual_column_comment_refusal(sql) {
        return Some(Err(refused));
    }
    if let Some(refused) = crate::table_props_ddl::unset_tblproperties_if_refusal(sql) {
        return Some(Err(refused));
    }
    if let Some(parsed) = try_parse_alter_namespace(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER NAMESPACE").map(|()| ddl)) {
                Ok(alter_namespace) => {
                    execute_alter_namespace(ctx, catalogs, alter_namespace).await
                }
                Err(error) => Err(error),
            },
        );
    }
    // CREATE NAMESPACE LOCATION/COMMENT/WITH properties: sqlparser cannot model those clauses.
    if let Some(parsed) = try_parse_create_namespace(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("CREATE NAMESPACE").map(|()| ddl)) {
                Ok(create_namespace) => {
                    execute_create_namespace(ctx, catalogs, create_namespace).await
                }
                Err(error) => Err(error),
            },
        );
    }
    // `DESCRIBE {NAMESPACE|DATABASE|SCHEMA} [EXTENDED]` (Group Z).
    if let Some(outcome) = describe_namespace_preparse(ctx, catalogs, sql, parsed_ddl).await {
        return Some(outcome);
    }
    if let Some(outcome) = crate::catalog_ops::v2_json_preparse(sql, parsed_ddl) {
        return Some(outcome);
    }
    if let Some(result) = try_describe_table_intercept(ctx, catalogs, sql, write_options).await {
        return Some(result);
    }
    if let Some(result) = crate::show_table_extended::try_show_table_extended_intercept(
        ctx,
        catalogs,
        sql,
        write_options,
    )
    .await
    {
        return Some(result);
    }
    if let Some(result) =
        crate::show_create::try_show_create_intercept(ctx, catalogs, sql, write_options).await
    {
        return Some(result);
    }
    if let Some(result) = try_refresh_intercept(ctx, catalogs, sql, write_options).await {
        return Some(result);
    }
    // `SHOW {NAMESPACES|SCHEMAS|DATABASES}` (Group AB).
    if let Some(parsed) = describe_show::try_parse_show_namespaces(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("SHOW NAMESPACES").map(|()| ddl)) {
                Ok(show_namespaces) => {
                    describe_show::execute_show_namespaces(ctx, catalogs, show_namespaces).await
                }
                Err(error) => Err(error),
            },
        );
    }
    if let Some(show) = describe_show::try_parse_show_system_functions(sql)
        && parsed_ddl("SHOW FUNCTIONS").is_ok()
        && catalogs.get(&show.catalog).is_some()
    {
        return Some(describe_show::execute_show_system_functions(ctx, &show));
    }
    if let Some(outcome) = show_partitions_preparse(sql, parsed_ddl) {
        return Some(outcome);
    }
    if let Some(outcome) = crate::catalog_ops::v2_tail_preparse(sql, parsed_ddl) {
        return Some(outcome);
    }
    if let Some(result) = try_use_default_intercept(ctx, catalogs, sql).await {
        return Some(result);
    }
    if let Some(result) = try_planner_default_set_mirror(ctx, catalogs, sql, write_options).await {
        return Some(result);
    }
    // Snapshot-ref DDL (I5) — not modelled by stock sqlparser.
    if let Some(parsed) = ref_ddl::try_parse_ref_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("BRANCH/TAG DDL").map(|()| ddl)) {
                Ok(ddl) => ref_ddl::execute_ref_ddl(ctx, catalogs, ddl).await,
                Err(error) => Err(error),
            },
        );
    }
    None
}

async fn try_preparse_comment_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let parsed_ddl = |context: &str| write_options.refuse_if_non_empty(context);
    if let Some(parsed) = comment_on_table::try_parse_comment_on_table_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("COMMENT ON TABLE").map(|()| ddl)) {
                Ok(ddl) => comment_on_table::execute_comment_on_table_ddl(ctx, catalogs, ddl).await,
                Err(error) => Err(error),
            },
        );
    }
    if let Some(parsed) = hive_change_column::try_parse_hive_change_column_ddl(sql) {
        return Some(
            match parsed.and_then(|ddl| parsed_ddl("ALTER TABLE").map(|()| ddl)) {
                Ok(ddl) => {
                    hive_change_column::execute_hive_change_column_ddl(ctx, catalogs, ddl).await
                }
                Err(error) => Err(error),
            },
        );
    }
    None
}

/// Fall-through when `parse_single_normalized` returns `None` (MERGE / residual BRANCH|TAG / DF).
pub(crate) async fn execute_unparsable_fallthrough(
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
