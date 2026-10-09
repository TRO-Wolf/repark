//! `UPDATE … SET …` dispatch, moved out of the router at its line ceiling.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use repark_core::CatalogRegistry;

use crate::{
    DmlSubqueryVerb, MorDmlKind, object_name_from_table_with_joins, refuse_dml_subquery_predicate,
    refuse_mor_unpartitioned_multi_spec_dml, refuse_read_only_dml_table_sql, spark_ast,
};

/// `UPDATE … SET …`.
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
    let folded = crate::update_cast::refuse_cast_then_fold_nested(ctx, catalogs, update).await?;
    spark_ast::execute_passthrough(ctx, catalogs, folded.as_deref().unwrap_or(sql)).await
}
