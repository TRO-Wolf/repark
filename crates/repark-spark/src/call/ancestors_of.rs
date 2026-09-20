use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::Catalog;

use super::{CallArgs, illegal_argument, resolve_table_ident};
use crate::iceberg_err;

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_ancestors_of(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "snapshot_id"])?;
    args.reject_excess_positional(2)?;
    let table_arg = args.require_string("table", 0)?;
    let snapshot_id = args.optional_i64("snapshot_id", Some(1))?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let metadata = table.metadata();

    let head = match snapshot_id {
        Some(id) => id,
        None => metadata
            .current_snapshot_id()
            .ok_or_else(|| illegal_argument("Cannot find snapshot: -1".to_string()))?,
    };
    if metadata.snapshot_by_id(head).is_none() {
        return Err(illegal_argument(format!("Cannot find snapshot: {head}")));
    }

    let mut ids = Vec::new();
    let mut stamps = Vec::new();
    let mut current = Some(head);
    while let Some(id) = current {
        let snapshot = metadata.snapshot_by_id(id).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ancestors_of reached snapshot {id} which the table metadata no longer \
                 contains — refusing rather than returning a truncated chain"
            ))
        })?;
        ids.push(snapshot.snapshot_id());
        stamps.push(snapshot.timestamp_ms());
        current = snapshot.parent_snapshot_id();
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(stamps)),
        ],
    )?;
    ctx.read_batches(vec![batch])
}
