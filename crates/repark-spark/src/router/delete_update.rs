use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{ObjectName, Statement};
use repark_core::CatalogRegistry;
use repark_iceberg::write::data_format::{
    MorDeleteFormat, inherited_delete_format_refusal, mor_delete_format,
    refuse_any_unreadable_delete_format,
};
use repark_iceberg::write::predicate_dml::plain::{
    plain_identity_needs_fork, try_allowed_plain_identity_or_update,
};
use repark_iceberg::write::predicate_dml::{try_allowed_delete_in, try_allowed_update_in};

use crate::dml_target::{DmlTarget, resolve_dml_target};
use crate::{
    DmlSubqueryVerb, MorDmlKind, delete_target_object_name, object_name_from_table_with_joins,
    refuse_dml_subquery_predicate, refuse_mor_unpartitioned_multi_spec_dml,
    refuse_read_only_dml_from_delete, refuse_read_only_dml_table_sql, spark_ast,
};

pub(crate) async fn execute_delete(
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
    crate::catalog_ops::refuse_encrypted_write_target(ctx, catalogs, object_name).await?;
    if try_metadata_delete_door(ctx, catalogs, delete).await? {
        return ctx.read_empty();
    }
    let statement = Statement::Delete(delete.clone());
    refuse_unreadable_delete_format(ctx, catalogs, &statement, object_name, MorDmlKind::Delete)
        .await?;
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

pub(crate) async fn execute_update(
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
    if let Some(error) = super::pg_insert::refuse_postgres_update(catalogs, &update.table) {
        return Err(error);
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
    crate::catalog_ops::refuse_encrypted_write_target(ctx, catalogs, object_name).await?;
    let statement = Statement::Update(update.clone());
    refuse_unreadable_delete_format(ctx, catalogs, &statement, object_name, MorDmlKind::Update)
        .await?;
    let folded = crate::update_cast::refuse_cast_then_fold_nested(ctx, catalogs, update).await?;
    spark_ast::execute_passthrough(ctx, catalogs, folded.as_deref().unwrap_or(sql)).await
}

async fn repark_writer_takes(
    catalogs: &CatalogRegistry,
    statement: &Statement,
    target: &DmlTarget,
) -> Result<bool> {
    if try_allowed_delete_in(statement)?.is_some() || try_allowed_update_in(statement)?.is_some() {
        return Ok(true);
    }
    let Some(allowed) = try_allowed_plain_identity_or_update(statement)? else {
        return Ok(false);
    };
    if allowed.catalog_name != target.catalog_name || allowed.spec.target != target.ident {
        return Ok(false);
    }
    let Some(catalog) = catalogs.get(&allowed.catalog_name) else {
        return Ok(false);
    };
    Ok(!plain_identity_needs_fork(catalog, &allowed.spec).await)
}

async fn refuse_unreadable_delete_format(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: &Statement,
    table_name: Option<&ObjectName>,
    kind: MorDmlKind,
) -> Result<()> {
    let Some(table_name) = table_name else {
        return Ok(());
    };
    let Some(target) = resolve_dml_target(ctx, catalogs, table_name).await? else {
        return Ok(());
    };
    if target.on_branch {
        return refuse_any_unreadable_delete_format(&target.table, kind.mode_property());
    }
    match mor_delete_format(&target.table, kind.mode_property())? {
        MorDeleteFormat::Readable => Ok(()),
        MorDeleteFormat::Inherited(_)
            if repark_writer_takes(catalogs, statement, &target).await? =>
        {
            Ok(())
        }
        MorDeleteFormat::Inherited(format) => Err(inherited_delete_format_refusal(
            &target.table.identifier().to_string(),
            format,
        )),
    }
}
