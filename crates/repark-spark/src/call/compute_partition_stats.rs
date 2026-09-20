use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::Catalog;
use iceberg::maintenance::ComputePartitionStats;

use super::{CallArgs, illegal_argument, resolve_table_ident};
use crate::{iceberg_err, reregister};

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_compute_partition_stats(
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
    if table
        .metadata()
        .default_partition_spec()
        .fields()
        .is_empty()
    {
        return Err(illegal_argument("Table must be partitioned".to_string()));
    }

    let mut action = ComputePartitionStats::new(table);
    if let Some(id) = snapshot_id {
        action = action.snapshot_id(id);
    }
    let result = action
        .execute(catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "partition_statistics_file",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            result.statistics_file.statistics_path,
        ]))],
    )?;
    ctx.read_batches(vec![batch])
}
