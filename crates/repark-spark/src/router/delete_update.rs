use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use repark_core::CatalogRegistry;

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
    let folded = crate::update_cast::refuse_cast_then_fold_nested(ctx, catalogs, update).await?;
    spark_ast::execute_passthrough(ctx, catalogs, folded.as_deref().unwrap_or(sql)).await
}
