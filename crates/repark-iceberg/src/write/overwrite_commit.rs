use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::Catalog;
use iceberg::expr::Predicate;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use uuid::Uuid;

use super::commit_target::{maybe_to_branch, snapshot_id_for_commit};
use super::merge::OPERATION_ID_PROP;
use super::overwrite::{OverwriteIsolation, parse_overwrite_isolation};

#[allow(clippy::missing_errors_doc)]
pub async fn commit_overwrite_replace_all_to(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    branch: Option<&str>,
) -> Result<Table> {
    let isolation = parse_overwrite_isolation(table)?;
    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
    let tx = Transaction::new(table);
    let mut action = tx
        .overwrite_files()
        .overwrite_by_row_filter(Predicate::AlwaysTrue)
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = snapshot_id_for_commit(table, branch) {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action.apply(tx).map_err(iceberg_err)?;
    tx.commit(catalog.as_ref()).await.map_err(iceberg_err)
}

fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
