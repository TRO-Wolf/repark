//! Whole-table `TRUNCATE TABLE`: delete-only overwrite with an empty add set.

use std::sync::Arc;

use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::table::Table;

use super::overwrite::commit_overwrite_replace_all;

/// Commit a Spark-equal Iceberg truncate: `overwrite_files` with `AlwaysTrue` and no added files.
///
/// The fork classifies delete-only overwrite as `Operation::Delete`. That matches live PySpark
/// 4.1.2 + Iceberg 1.11.0 `TRUNCATE TABLE` on a v2 table.
///
/// # Errors
/// Isolation parse, apply, or commit failures as [`datafusion::error::DataFusionError`].
pub async fn commit_truncate(catalog: &Arc<dyn Catalog>, table: &Table) -> Result<Table> {
    commit_overwrite_replace_all(catalog, table, Vec::new()).await
}
