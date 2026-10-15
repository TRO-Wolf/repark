use std::sync::Arc;

use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use super::append::iceberg_err;
use crate::write::commit_error::{commit_with_keyed_sweep, operation_id_and_summary};
use crate::write::encryption::written_file_paths;

#[allow(clippy::missing_errors_doc)]
pub async fn commit_append(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    new_files: Vec<DataFile>,
) -> Result<Table> {
    let (operation_id, summary) = operation_id_and_summary();
    let staged_paths = written_file_paths(&new_files);
    let tx = Transaction::new(table);
    let action = tx
        .merge_append()
        .add_data_files(new_files)
        .set_snapshot_properties(summary);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    commit_with_keyed_sweep(
        table,
        &staged_paths,
        tx.commit(catalog.as_ref()).await,
        &operation_id,
    )
    .await
}
