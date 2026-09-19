use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use iceberg::Catalog;
use iceberg::table::Table;
use uuid::Uuid;

use super::lineage::{rewrite_column_names, survivor_sql};
use super::{register_affected_rewrite_target, register_identity_table};
use crate::write::concurrency::concurrency_from_ctx;
use crate::write::merge::row_lineage::table_carries_merge_lineage;
use crate::write::merge::session_staging::write_new_data_files_from_stream_with;
use crate::write::merge::{
    CommitScope, commit_overwrite_on_ref, deregister_merge_scratch, quote_ident,
    resolve_affected_data_files,
};
use crate::write::position_delete::PositionDeletePair;
use crate::write::session_write_conf::resolve_empty_session_write;

#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_identity_update_cow(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    snapshot_id: Option<i64>,
    rewrite: (Vec<PositionDeletePair>, Vec<RecordBatch>),
    scope: &CommitScope,
    branch: Option<&str>,
) -> Result<()> {
    let (pairs, data_batches) = rewrite;
    let mut affected: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (path, _) in &pairs {
        if seen.insert(path.clone()) {
            affected.push(path.to_string());
        }
    }
    let ident_table = register_identity_table(ctx, &pairs)?;
    let rewrite_name =
        register_affected_rewrite_target(ctx, table, snapshot_id, write_schema, &affected)?;
    let new_table = register_update_values_table(ctx, data_batches)?;
    let carry_lineage = table_carries_merge_lineage(table);
    let columns = rewrite_column_names(write_schema, carry_lineage)
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let rewrite_sql = format!(
        "{survivors} UNION ALL SELECT {columns} FROM {newvals}",
        survivors = survivor_sql(write_schema, &rewrite_name, &ident_table, carry_lineage),
        newvals = quote_ident(&new_table),
    );
    let rewrite_result = async {
        let stream = ctx.sql(&rewrite_sql).await?.execute_stream().await?;
        let concurrency = concurrency_from_ctx(ctx);
        let (_, staging) = resolve_empty_session_write(ctx)?;
        write_new_data_files_from_stream_with(table, write_schema, stream, concurrency, &staging)
            .await
    }
    .await;
    let _ = ctx.deregister_table(ident_table.as_str());
    let _ = ctx.deregister_table(new_table.as_str());
    let _ = deregister_merge_scratch(ctx, &rewrite_name);
    let new_files = rewrite_result?;
    let affected_entries = resolve_affected_data_files(table, snapshot_id, &affected).await?;
    let (snapshot_extra, _) = resolve_empty_session_write(ctx)?;
    commit_overwrite_on_ref(
        catalog,
        table,
        snapshot_id,
        affected_entries,
        new_files,
        scope,
        branch,
        &snapshot_extra,
    )
    .await
}

fn register_update_values_table(ctx: &SessionContext, batches: Vec<RecordBatch>) -> Result<String> {
    let name = format!("__repark_pred_upd_{}", Uuid::new_v4().simple());
    if batches.is_empty() {
        return Err(DataFusionError::Internal(
            "identity UPDATE COW rewrite has no new-value batches".to_string(),
        ));
    }
    let schema = batches[0].schema();
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(name.as_str(), Arc::new(provider))?;
    Ok(name)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_identity_cow(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    snapshot_id: Option<i64>,
    pairs: &[PositionDeletePair],
    scope: &CommitScope,
    branch: Option<&str>,
) -> Result<()> {
    let mut affected: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (path, _) in pairs {
        if seen.insert(path.clone()) {
            affected.push(path.to_string());
        }
    }
    let ident_table = register_identity_table(ctx, pairs)?;
    let rewrite_name =
        register_affected_rewrite_target(ctx, table, snapshot_id, write_schema, &affected)?;
    let rewrite_sql = survivor_sql(
        write_schema,
        &rewrite_name,
        &ident_table,
        table_carries_merge_lineage(table),
    );
    let rewrite_result = async {
        let stream = ctx.sql(&rewrite_sql).await?.execute_stream().await?;
        let concurrency = concurrency_from_ctx(ctx);
        let (_, staging) = resolve_empty_session_write(ctx)?;
        write_new_data_files_from_stream_with(table, write_schema, stream, concurrency, &staging)
            .await
    }
    .await;
    let _ = ctx.deregister_table(ident_table.as_str());
    let _ = deregister_merge_scratch(ctx, &rewrite_name);
    let new_files = rewrite_result?;
    let affected_entries = resolve_affected_data_files(table, snapshot_id, &affected).await?;
    let (snapshot_extra, _) = resolve_empty_session_write(ctx)?;
    commit_overwrite_on_ref(
        catalog,
        table,
        snapshot_id,
        affected_entries,
        new_files,
        scope,
        branch,
        &snapshot_extra,
    )
    .await
}
