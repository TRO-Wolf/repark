//! COW rewrite scratch tables: file-scoped target, affected-path `MemTable`, drop guard.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use iceberg::table::Table;
use uuid::Uuid;

use super::{
    TargetScanStream, deregister_merge_scratch, register_streaming_target, scratch_schema,
};
use crate::write::file_scoped_rewrite::allowlist_from_paths;
use crate::write::scan_concurrency::scan_concurrency_from_ctx;
use crate::write::scan_prune::file_scoped_rewrite_from_ctx;

/// Column name for the affected-path `MemTable` (semi-join key).
pub(super) const AFFECTED_PATHS_COL: &str = "path";

/// RAII guard for MERGE scratch tables registered during COW rewrite.
pub(super) struct MergeScratchGuard<'a> {
    ctx: &'a SessionContext,
    names: Vec<String>,
}

impl<'a> MergeScratchGuard<'a> {
    pub(super) fn new(ctx: &'a SessionContext) -> Self {
        Self {
            ctx,
            names: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, name: String) {
        self.names.push(name);
    }
}

impl Drop for MergeScratchGuard<'_> {
    fn drop(&mut self) {
        for name in &self.names {
            let _ = deregister_merge_scratch(self.ctx, name);
        }
    }
}

/// Register a one-column `MemTable` of affected `_file` paths for the COW rewrite semi-join.
pub(super) fn register_affected_paths_table(
    ctx: &SessionContext,
    affected: &[String],
) -> Result<String> {
    let name = format!("__repark_merge_aff_paths_{}", Uuid::new_v4().simple());
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        AFFECTED_PATHS_COL,
        DataType::Utf8,
        false,
    )]));
    let path_array = StringArray::from(
        affected
            .iter()
            .map(std::string::String::as_str)
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(path_array)])?;
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(|error| {
        DataFusionError::Internal(format!("affected-path MemTable build failed: {error}"))
    })?;
    ctx.register_table(name.as_str(), Arc::new(table))?;
    Ok(name)
}

/// Register a file-scoped streaming COW target when `affected` is a non-empty proper subset.
pub(super) fn maybe_register_file_scoped_rewrite_target(
    ctx: &SessionContext,
    table: &Table,
    snapshot_id: Option<i64>,
    write_schema: &SchemaRef,
    affected: &[String],
) -> Result<Option<String>> {
    if !file_scoped_rewrite_from_ctx(ctx) || affected.is_empty() {
        return Ok(None);
    }
    let allowlist = allowlist_from_paths(affected);
    let scratch = scratch_schema(write_schema);
    let scan_concurrency = scan_concurrency_from_ctx(ctx);
    let source: Arc<dyn PartitionStream> = Arc::new(TargetScanStream::new(
        table.clone(),
        snapshot_id,
        Arc::clone(&scratch),
        write_schema,
        None,
        scan_concurrency.concurrency_limit,
        Some(allowlist),
    ));
    let name = register_streaming_target(ctx, scratch, source)?;
    Ok(Some(name))
}
