use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Insert, TableObject};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{namespace_schema_name, refuse_read_only_dml_table_sql, reregister};
use crate::insert_overwrite::try_resolve_iceberg_overwrite_target;
use crate::spark_ast;

pub(crate) async fn execute_append_with_options(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    options: &crate::write_options::StatementWriteOptions,
) -> Result<DataFrame> {
    let table_name = match &insert.table {
        TableObject::TableName(name) => name,
        TableObject::TableFunction(_) | TableObject::TableQuery(_) => {
            return Err(DataFusionError::Plan(
                "INSERT into a table function does not support write options \
                 (ICE-WRITE-OPTIONS-1)"
                    .to_string(),
            ));
        }
    };
    if insert.replace_into {
        return Err(DataFusionError::Plan(
            "REPLACE INTO does not support write options (ICE-WRITE-OPTIONS-1)".to_string(),
        ));
    }
    if !insert.columns.is_empty() {
        return Err(DataFusionError::Plan(
            "INSERT with an explicit column list does not support write options \
             (ICE-WRITE-OPTIONS-1)"
                .to_string(),
        ));
    }
    let table_sql = table_name.to_string();
    if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
        return Err(DataFusionError::Plan(message));
    }
    let Some((catalog_name, catalog, table, branch)) =
        try_resolve_iceberg_overwrite_target(ctx, catalogs, table_name).await?
    else {
        return Err(DataFusionError::Plan(format!(
            "INSERT with write options requires a 3-part Iceberg table name, got \
             `{table_sql}` (ICE-WRITE-OPTIONS-1)"
        )));
    };
    let source = insert.source.as_ref().ok_or_else(|| {
        DataFusionError::Plan(
            "INSERT with write options requires a SELECT or VALUES source".to_string(),
        )
    })?;
    let materialize_sql = format!("SELECT * FROM ({source}) AS _repark_app_src");
    let source_df = spark_ast::execute_passthrough(ctx, catalogs, &materialize_sql).await?;
    let stream = source_df.execute_stream().await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    repark_iceberg::write::append_with_statement_options(
        &catalog,
        &table,
        stream,
        &options.snapshot_extra,
        &options.staging_overrides(),
        concurrency,
        branch.as_deref(),
    )
    .await?;
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, catalog, &catalog_name, &namespace).await?;
    ctx.read_empty()
}
