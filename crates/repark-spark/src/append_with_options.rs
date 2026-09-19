use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Insert, TableObject};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{namespace_schema_name, refuse_read_only_dml_table_sql, reregister};
use crate::insert_overwrite::{
    object_name_last, overwrite_source_with_default_fills, try_resolve_iceberg_overwrite_target,
};
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
    let listed: Vec<String> = insert.columns.iter().map(object_name_last).collect();
    let (column_names, materialize_sql) =
        overwrite_source_with_default_fills(&table, &listed, source)?;
    let source_df = spark_ast::execute_passthrough(ctx, catalogs, &materialize_sql).await?;
    let stream = source_df.execute_stream().await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    let (snapshot_extra, staging) = options.resolve_with_session(ctx)?;
    if column_names.is_empty() {
        repark_iceberg::write::append_with_statement_options(
            &catalog,
            &table,
            stream,
            &snapshot_extra,
            &staging,
            concurrency,
            branch.as_deref(),
        )
        .await?;
    } else {
        let files = repark_iceberg::write::stage_overwrite_files_with(
            &table,
            stream,
            column_names,
            concurrency,
            &staging,
        )
        .await?;
        repark_iceberg::write::commit_append_with_summary(
            &catalog,
            &table,
            files,
            &snapshot_extra,
            branch.as_deref(),
        )
        .await?;
    }
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, catalog, &catalog_name, &namespace).await?;
    ctx.read_empty()
}
