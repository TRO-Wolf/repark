use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;
use iceberg::table::Table;

use crate::write::concurrency::concurrency_from_ctx;
use crate::write::merge::session_staging::write_new_data_files_from_stream_with;
use crate::write::merge::{
    CommitScope, KnownPartitions, RowDeltaKind, commit_row_delta_kind_on_ref,
};
use crate::write::position_delete::PositionDeletePair;
use crate::write::session_write_conf::resolve_empty_session_write;

#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_identity_delete_mor(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<PositionDeletePair>,
    scope: &CommitScope,
    known_partitions: KnownPartitions,
    branch: Option<&str>,
) -> Result<()> {
    let (snapshot_extra, staging) = resolve_empty_session_write(ctx)?;
    commit_row_delta_kind_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        Vec::new(),
        concurrency_from_ctx(ctx),
        &scope.row_delta(RowDeltaKind::Delete),
        branch,
        known_partitions,
        &snapshot_extra,
        &staging,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_identity_update_mor(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    write_schema: &datafusion::arrow::datatypes::SchemaRef,
    snapshot_id: Option<i64>,
    rewrite: (Vec<PositionDeletePair>, Vec<RecordBatch>),
    scope: &CommitScope,
    known_partitions: KnownPartitions,
    branch: Option<&str>,
) -> Result<()> {
    let (pairs, data_batches) = rewrite;
    let stream = futures::stream::iter(data_batches.into_iter().map(Ok));
    let (snapshot_extra, staging) = resolve_empty_session_write(ctx)?;
    let data_files = write_new_data_files_from_stream_with(
        table,
        write_schema,
        stream,
        concurrency_from_ctx(ctx),
        &staging,
    )
    .await?;
    commit_row_delta_kind_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency_from_ctx(ctx),
        &scope.row_delta(RowDeltaKind::Merge),
        branch,
        known_partitions,
        &snapshot_extra,
        &staging,
    )
    .await
}
