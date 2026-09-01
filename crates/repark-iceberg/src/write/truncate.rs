//! Whole-table `TRUNCATE TABLE`: delete-only overwrite with an empty add set.

use std::sync::Arc;

use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::table::Table;

/// Commit a Spark-equal Iceberg truncate: `overwrite_files` with `AlwaysTrue` and no added files.
///
/// The fork classifies delete-only overwrite as `Operation::Delete`. That matches live PySpark
/// 4.1.2 + Iceberg 1.11.0 `TRUNCATE TABLE` on a v2 table.
///
/// # Errors
/// Isolation parse, apply, or commit failures as [`datafusion::error::DataFusionError`].
pub async fn commit_truncate(catalog: &Arc<dyn Catalog>, table: &Table) -> Result<Table> {
    commit_truncate_to(catalog, table, None).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_truncate_to(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    branch: Option<&str>,
) -> Result<Table> {
    super::overwrite_commit::commit_overwrite_replace_all_to(catalog, table, Vec::new(), branch)
        .await
}
