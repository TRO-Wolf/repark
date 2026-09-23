use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use repark_core::CatalogRegistry;

use crate::router::rewrite_sql_for_execute;
use crate::{
    metadata_tables, refuse_multi_statement_sql, spark_ast, time_travel, wap, write_to_branch,
};

pub(crate) async fn execute_view_body_query(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<DataFrame> {
    crate::view_ddl::read::ensure_view_wrappers(ctx, catalogs)?;
    let rewritten_sql = rewrite_sql_for_execute(sql, catalogs);
    let sql = rewritten_sql.as_str();
    refuse_multi_statement_sql(sql)?;
    let sql_after_meta: std::borrow::Cow<'_, str> =
        if metadata_tables::sql_may_have_metadata_table_path(sql) {
            match metadata_tables::prepare_metadata_table_sql(catalogs, sql).await? {
                Some(rewritten) => std::borrow::Cow::Owned(rewritten),
                None => std::borrow::Cow::Borrowed(sql),
            }
        } else {
            std::borrow::Cow::Borrowed(sql)
        };
    let mut pinned = time_travel::PinnedViews::default();
    let sql_after_branch = write_to_branch::apply_write_to_branch(
        ctx,
        catalogs,
        sql_after_meta.as_ref(),
        &mut pinned,
        false,
    )
    .await?;
    let sql_after_wap =
        wap::apply_wap_read_redirect(ctx, catalogs, sql_after_branch.as_ref(), &mut pinned).await?;
    let routed_sql = sql_after_wap
        .as_deref()
        .unwrap_or_else(|| sql_after_branch.as_ref());
    let dialect = datafusion::sql::sqlparser::dialect::DatabricksDialect {};
    let mut lineage_pins = repark_core::LineagePins::default();
    let sql_storage: std::borrow::Cow<'_, str> = match repark_core::prepare_lineage_sql(
        ctx,
        catalogs,
        routed_sql,
        &dialect,
        &mut lineage_pins,
    )
    .await?
    {
        Some(rewritten) => std::borrow::Cow::Owned(rewritten),
        None => std::borrow::Cow::Borrowed(routed_sql),
    };
    let result = spark_ast::execute_passthrough(ctx, catalogs, sql_storage.as_ref()).await;
    lineage_pins.release(ctx);
    pinned.release(ctx);
    result
}

pub(crate) async fn refuse_insert_into_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &datafusion::sql::sqlparser::ast::Insert,
) -> Result<()> {
    if let datafusion::sql::sqlparser::ast::TableObject::TableName(name) = &insert.table {
        crate::view_ddl::execute::refuse_view_write_target(catalogs, name).await?;
    }
    Ok(())
}
