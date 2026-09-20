use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Insert, Statement, TableObject};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{namespace_schema_name, refuse_read_only_dml_table_sql, reregister};
use crate::insert_overwrite::try_resolve_iceberg_overwrite_target;
use crate::spark_ast;

pub(crate) async fn execute_append_with_options(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
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
    if insert.source.is_none() {
        return Err(DataFusionError::Plan(
            "INSERT with write options requires a SELECT or VALUES source".to_string(),
        ));
    }
    let planning_sql = match branch.as_deref() {
        None => None,
        Some(_) => insert_sql_without_write_ref(insert, table_name),
    };
    let source_df =
        spark_ast::execute_insert_source(ctx, catalogs, planning_sql.as_deref().unwrap_or(sql))
            .await?;
    let stream = source_df.execute_stream().await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    let (snapshot_extra, mut staging) = options.resolve_with_session(ctx)?;
    if options.is_empty() {
        staging.fork_insert_dictionary_rule = true;
        let files = repark_iceberg::write::stage_overwrite_files_with(
            &table,
            stream,
            Vec::new(),
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
    } else {
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
    }
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, catalog, &catalog_name, &namespace).await?;
    ctx.read_empty()
}

fn insert_sql_without_write_ref(
    insert: &Insert,
    name: &datafusion::sql::sqlparser::ast::ObjectName,
) -> Option<String> {
    let mut base = name.clone();
    base.0.pop()?;
    let mut planning = insert.clone();
    planning.table = TableObject::TableName(base);
    Some(Statement::Insert(planning).to_string())
}
