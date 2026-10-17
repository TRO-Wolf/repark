use std::sync::Arc;

use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::Transaction;

use super::append::iceberg_err;
use crate::write::commit_error::{commit_result, operation_id_and_summary};
use crate::write::staged_commit::StagedFiles;

#[allow(clippy::missing_errors_doc)]
pub async fn commit_append(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    new_files: Vec<DataFile>,
) -> Result<Table> {
    let (operation_id, summary) = operation_id_and_summary();
    let tx = Transaction::new(table);
    let action = tx.merge_append().set_snapshot_properties(summary);
    let staged = StagedFiles::claimed(new_files)
        .stage(action, tx)
        .map_err(iceberg_err)?;
    commit_result(
        staged.commit(table, catalog.as_ref(), &operation_id).await,
        &operation_id,
    )
}
