//! COW rewrite scratch tables: file-scoped target, affected-path `MemTable`, drop guard.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use iceberg::table::Table;
use uuid::Uuid;

use super::{TargetScanStream, deregister_merge_scratch, register_streaming_target};
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

const SCRATCH_CATALOG: &str = "datafusion";
const SCRATCH_SCHEMA: &str = "public";

pub(super) fn quote_scratch_name(name: &str) -> String {
    name.split('.')
        .map(crate::write::idents::quote_ident_spark)
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn register_scratch_provider(
    ctx: &SessionContext,
    provider: Arc<dyn TableProvider>,
    kind: &str,
) -> Result<String> {
    let leaf = format!("__repark_{kind}_{}", Uuid::new_v4().simple());
    let catalog = ctx.catalog(SCRATCH_CATALOG).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "no session catalog `{SCRATCH_CATALOG}` for MERGE scratch (have {:?})",
            ctx.catalog_names()
        ))
    })?;
    let schema = catalog.schema(SCRATCH_SCHEMA).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "no schema `{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}` for MERGE scratch"
        ))
    })?;
    let _ = schema.deregister_table(&leaf);
    schema
        .register_table(leaf.clone(), provider)
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register MERGE scratch {SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{leaf}: {error}"
            ))
        })?;
    let state = ctx.state();
    let defaults = &state.config().options().catalog;
    if defaults.default_catalog == SCRATCH_CATALOG && defaults.default_schema == SCRATCH_SCHEMA {
        Ok(leaf)
    } else {
        Ok(format!("{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{leaf}"))
    }
}

/// Register a one-column `MemTable` of affected `_file` paths for the COW rewrite semi-join.
pub(super) fn register_affected_paths_table(
    ctx: &SessionContext,
    affected: &[String],
) -> Result<String> {
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
    register_scratch_provider(ctx, Arc::new(table), "merge_aff_paths")
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
    let scratch = super::row_lineage::scratch_schema_for_table(write_schema, table);
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
