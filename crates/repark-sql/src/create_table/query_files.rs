use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::DataFile;
use iceberg::table::Table;
use repark_iceberg::write::WriterStagingOverrides;

pub(super) async fn published(
    ctx: &SessionContext,
    table: &Table,
    query: DataFrame,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>> {
    let files = staged(ctx, table, query, staging).await?;
    Ok(repark_iceberg::write::forget_staged_files(files))
}

pub(super) async fn staged(
    ctx: &SessionContext,
    table: &Table,
    query: DataFrame,
    staging: &WriterStagingOverrides,
) -> Result<Vec<DataFile>> {
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    if staging.codec.is_none()
        && staging.level.is_none()
        && staging.target_file_size_bytes.is_none()
    {
        let task_ctx = Arc::new(query.task_ctx());
        let plan = query.create_physical_plan().await?;
        return repark_iceberg::write::write_data_files_from_plan(
            table,
            plan,
            task_ctx,
            concurrency,
            true,
        )
        .await;
    }
    let stream = query.execute_stream().await?;
    if table.metadata().default_partition_spec().is_unpartitioned() {
        return repark_iceberg::write::stage_unpartitioned_stream_with_overrides(
            table,
            stream,
            concurrency,
            staging,
        )
        .await;
    }
    repark_iceberg::write::stage_partitioned_stream_with_overrides(
        table,
        stream,
        staging,
        concurrency,
    )
    .await
}
