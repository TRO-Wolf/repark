use std::sync::Arc;

use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::write::commit_error::{commit_result, operation_id_and_summary};

#[must_use]
pub fn snapshot_id_for_commit(table: &Table, branch: Option<&str>) -> Option<i64> {
    match branch {
        Some(name) => table
            .metadata()
            .snapshot_for_ref(name)
            .map(|snapshot| snapshot.snapshot_id()),
        None => table.metadata().current_snapshot_id(),
    }
}

#[must_use]
pub fn maybe_to_branch<A>(
    action: A,
    branch: Option<&str>,
    to_branch: impl FnOnce(A, &str) -> A,
) -> A {
    match branch {
        Some(name) => to_branch(action, name),
        None => action,
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_append_to(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    new_files: Vec<DataFile>,
    branch: Option<&str>,
) -> Result<Table> {
    let (operation_id, summary) = operation_id_and_summary();
    let tx = Transaction::new(table);
    let action = tx
        .fast_append()
        .add_data_files(new_files)
        .set_snapshot_properties(summary);
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action
        .apply(tx)
        .map_err(crate::catalog::iceberg_to_datafusion)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}
