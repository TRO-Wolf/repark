//! The ANSI statement router.

use std::borrow::Cow;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{ObjectType, Statement};
use repark_core::EngineContext;

use crate::{
    alter, create_table, guards, merge, ref_ddl, refusals, schema_ddl, sniff, time_travel, truncate,
};

/// The dialect handed to DataFusion's parser.
pub(crate) const PARSER_DIALECT: datafusion::config::Dialect = datafusion::config::Dialect::Generic;

/// Execute one ANSI SQL statement against an [`EngineContext`].
/// # Errors
/// A guard refusal, a parse or plan failure, or an iceberg or execution error from a handler.
pub async fn execute(cx: EngineContext<'_>, sql: &str) -> Result<DataFrame> {
    guards::run_text_guards(&cx, sql)?;

    // --- Pre-parse stage: productions stock sqlparser cannot reach.
    if let Some(refusal) = refusals::recognize_alter_table_execute(sql) {
        return Err(refusal);
    }
    if let Some(ddl) = ref_ddl::try_parse_ref_ddl(sql) {
        return ref_ddl::execute_ref_ddl(&cx, ddl?).await;
    }
    let sql = match alter::rewrite_set_properties(sql) {
        Some(rewritten) => Cow::Owned(rewritten),
        None => Cow::Borrowed(sql),
    };
    // Release every relation registered by the rewrite after planning.
    let mut pinned = time_travel::PinnedViews::default();
    let mut lineage_pins = repark_core::LineagePins::default();
    let result = execute_time_travelled(&cx, &sql, &mut pinned, &mut lineage_pins).await;
    lineage_pins.release(cx.ctx);
    pinned.release(cx.ctx);
    result
}

/// Pin v3 lineage columns onto a temp provider when the statement names them.
async fn rewrite_lineage_sql<'a>(
    cx: &EngineContext<'_>,
    sql: Cow<'a, str>,
    lineage_pins: &mut repark_core::LineagePins,
) -> Result<Cow<'a, str>> {
    let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
    match repark_core::prepare_lineage_sql(
        cx.ctx,
        cx.catalogs,
        sql.as_ref(),
        &dialect,
        lineage_pins,
    )
    .await?
    {
        Some(rewritten) => Ok(Cow::Owned(rewritten)),
        None => Ok(sql),
    }
}

/// Run the pipeline after the `FOR … AS OF` rewrite.
async fn execute_time_travelled(
    cx: &EngineContext<'_>,
    sql: &str,
    pinned: &mut time_travel::PinnedViews,
    lineage_pins: &mut repark_core::LineagePins,
) -> Result<DataFrame> {
    let sql = match time_travel::prepare_time_travel_sql(cx, sql, pinned).await? {
        Some(rewritten) => Cow::Owned(rewritten),
        None => Cow::Borrowed(sql),
    };
    let sql = rewrite_lineage_sql(cx, sql, lineage_pins).await?;
    let sql: &str = &sql;

    let statement = match cx.ctx.state().sql_to_statement(sql, &PARSER_DIALECT) {
        Ok(statement) => statement,
        Err(err) => {
            // G15: refuse type-position COLLATE before returning the generic parse error.
            guards::refuse_type_position_collation_in_sql(sql)?;
            return Err(sniff::upgrade_error(sql, err));
        }
    };
    if let DFStatement::Reset(datafusion::sql::parser::ResetStatement::Variable(name)) = &statement
    {
        guards::refuse_collation_reset_variable(&name.to_string())?;
        return delegate(cx, sql).await;
    }
    let DFStatement::Statement(statement) = statement else {
        // DataFusion's parser extensions use the delegated plan path.
        return delegate(cx, sql).await;
    };

    // G15 runs at parse altitude before statement-specific handling.
    guards::refuse_collation_in_statement(statement.as_ref())?;
    crate::declared_refuse::refuse_in_statement(statement.as_ref())?;

    match statement.as_ref() {
        Statement::CreateTable(create) => create_table::execute_create_table(cx, create).await,
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            ..
        } => {
            schema_ddl::execute_create_schema(
                cx,
                schema_object_name(schema_name)?,
                *if_not_exists,
                with.as_ref(),
            )
            .await
        }
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            if_exists,
            ..
        } => schema_ddl::execute_drop_table(cx, names, *if_exists).await,
        Statement::Drop {
            object_type: ObjectType::Schema | ObjectType::Database,
            names,
            if_exists,
            cascade,
            ..
        } => schema_ddl::execute_drop_schema(cx, names, *if_exists, *cascade).await,
        Statement::AlterTable(alter) => alter::execute_alter_table(cx, alter).await,
        Statement::Merge(merge) => merge::execute_merge(cx, merge).await,
        // --- INSERT OVERWRITE: PARTITION forms execute; whole-table stays Q9.
        Statement::Insert(insert) if insert.overwrite => {
            crate::insert_overwrite::execute_insert_overwrite(cx, insert).await
        }
        Statement::Call(function) => Err(refusals::maintenance_call(&function.name.to_string())),
        Statement::Truncate(truncate) => truncate::execute_truncate(cx, truncate).await,
        // --- Delegated DML: allow-list first, then G3-E8 and async MoR/V3 valves.
        Statement::Delete(_) | Statement::Update(_) => {
            if let Some(allowed) =
                repark_iceberg::write::predicate_dml::try_allowed_delete_in(statement.as_ref())?
            {
                guards::refuse_mor_multi_spec_dml(cx, statement.as_ref()).await?;
                let handle = schema_ddl::catalog_handle(cx.catalogs, &allowed.catalog_name)?;
                repark_iceberg::write::predicate_dml::execute_predicate_dml(
                    cx.ctx,
                    handle,
                    &allowed.spec,
                )
                .await?;
                return cx.ctx.read_empty();
            }
            if let Some(allowed) =
                repark_iceberg::write::predicate_dml::try_allowed_update_in(statement.as_ref())?
            {
                guards::refuse_mor_multi_spec_dml(cx, statement.as_ref()).await?;
                let handle = schema_ddl::catalog_handle(cx.catalogs, &allowed.catalog_name)?;
                repark_iceberg::write::predicate_dml::execute_predicate_dml(
                    cx.ctx,
                    handle,
                    &allowed.spec,
                )
                .await?;
                return cx.ctx.read_empty();
            }
            guards::refuse_dml_subquery_predicate(statement.as_ref())?;
            guards::refuse_mor_multi_spec_dml(cx, statement.as_ref()).await?;
            // pins: v3r-1-rulings/C-001, C-002 — the passthrough seat of the V3-COW-1 guard.
            guards::refuse_v3_cow_dml(cx, statement.as_ref()).await?;
            delegate(cx, sql).await
        }
        _ => delegate(cx, sql).await,
    }
}

/// Plan with DataFusion, run the SEC-02 guard on the resulting plan, then execute.
async fn delegate(cx: &EngineContext<'_>, sql: &str) -> Result<DataFrame> {
    // Plan, apply SEC-02, then execute through the shared pre-execute belt.
    let belt = repark_core::PreExecute::from_engine_context(cx);
    let plan = match belt.plan(sql).await {
        Ok(plan) => plan,
        Err(err) => return Err(sniff::upgrade_error(sql, err)),
    };
    // Door-specific (SEC-02): the belt deliberately does not own the local-filesystem gate.
    guards::refuse_local_filesystem_plan(cx.ctx, cx.catalogs, &plan)?;
    belt.guard(&plan)?;
    belt.execute(plan).await
}

/// Return the schema name, rejecting authorization forms that this engine cannot model.
fn schema_object_name(
    schema_name: &datafusion::sql::sqlparser::ast::SchemaName,
) -> Result<&datafusion::sql::sqlparser::ast::ObjectName> {
    use datafusion::sql::sqlparser::ast::SchemaName;
    match schema_name {
        SchemaName::Simple(name) => Ok(name),
        SchemaName::UnnamedAuthorization(_) | SchemaName::NamedAuthorization(_, _) => {
            Err(DataFusionError::NotImplemented(
                "CREATE SCHEMA … AUTHORIZATION is not supported — this engine has no schema \
                 ownership model"
                    .to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests;
