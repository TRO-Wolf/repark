use std::sync::Arc;

use datafusion::error::Result;
use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::write::commit_error::{commit_result, operation_id_and_summary};
use crate::write::illegal_argument::number_format_error;
use crate::write::overwrite::OverwriteIsolation;
use crate::write::write_options::isolation_with_override;

#[derive(Debug, Clone, Copy, Default)]
pub struct FilterValidation<'a> {
    pub isolation: Option<&'a str>,
    pub validate_from_snapshot_id: Option<&'a str>,
}

impl FilterValidation<'_> {
    #[allow(clippy::missing_errors_doc)]
    pub fn isolation(&self, table: &Table) -> Result<Option<OverwriteIsolation>> {
        isolation_with_override(table, self.isolation)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn start(&self, table: &Table, branch: Option<&str>) -> Result<Option<i64>> {
        let current = snapshot_id_for_commit(table, branch);
        let requested = self
            .validate_from_snapshot_id
            .map(|raw| raw.parse::<i64>().map_err(|_| number_format_error(raw)))
            .transpose()?;
        let explicit = self
            .isolation
            .is_some_and(|raw| !raw.eq_ignore_ascii_case("none"));
        match requested {
            Some(start) if explicit => {
                check_start_is_ancestor(table, current, start)?;
                Ok(Some(start))
            }
            _ => Ok(current),
        }
    }
}

fn check_start_is_ancestor(table: &Table, current: Option<i64>, start: i64) -> Result<()> {
    let metadata = table.metadata();
    let mut cursor = current;
    let mut oldest = None;
    for _ in 0..=metadata.snapshots().len() {
        let Some(snapshot_id) = cursor else { break };
        if snapshot_id == start {
            return Ok(());
        }
        oldest = Some(snapshot_id);
        cursor = metadata
            .snapshot_by_id(snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    match oldest {
        None => Ok(()),
        Some(ancestor) => Err(crate::catalog::iceberg_to_datafusion(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!(
                "Cannot determine history between starting snapshot {start} and the last \
                 known ancestor {ancestor}"
            ),
        ))),
    }
}

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
        .merge_append()
        .add_data_files(new_files)
        .set_snapshot_properties(summary);
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action
        .apply(tx)
        .map_err(crate::catalog::iceberg_to_datafusion)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}
