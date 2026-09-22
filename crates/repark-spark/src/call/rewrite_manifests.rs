//! `CALL <catalog>.system.rewrite_manifests(…)` over the fork's `RewriteManifestsAction`.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{ManifestContentType, ManifestFile, Snapshot, TableProperties};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, table::Table};
use repark_core::illegal_argument_error;

use super::{CallArgs, resolve_table_ident};
use crate::{iceberg_err, reregister};

struct MatchingManifests {
    data_count: usize,
    data_bytes: u64,
    delete_count: usize,
    delete_bytes: u64,
}

/// Execute `CALL <catalog>.system.rewrite_manifests(table => …)`.
pub(super) async fn execute_rewrite_manifests(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "use_caching", "spec_id", "sort_by"])?;
    // Spark positional order: table, use_caching, spec_id, sort_by (jar `PARAMETERS`).
    args.reject_excess_positional(4)?;
    // Parse and drop.
    args.optional_bool("use_caching", Some(1))?;
    let requested_spec = args.optional_i32("spec_id", Some(2))?;
    let sort_by = args.optional_string_array("sort_by", Some(3))?;

    let table_arg = args.require_string("table", 0)?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    let spec_id = match requested_spec {
        Some(id) => table
            .metadata()
            .partition_spec_by_id(id)
            .map(|_| id)
            .ok_or_else(|| illegal_argument_error(format!("Invalid spec id {id}")))?,
        None => table.metadata().default_partition_spec_id(),
    };

    let Some(snapshot) = table.metadata().current_snapshot() else {
        // Spark finds no manifests on a table with no snapshot and answers zeros.
        return zero_result(ctx);
    };
    let matching = match_manifests(&table, snapshot, spec_id).await?;
    let target_bytes = target_manifest_size_bytes(&table);
    let data_work = is_leg_work(matching.data_count, matching.data_bytes, target_bytes);
    let delete_work = is_leg_work(matching.delete_count, matching.delete_bytes, target_bytes);
    if !data_work && !delete_work {
        return zero_result(ctx);
    }

    let tx = Transaction::new(&table);
    let unclustered = tx.rewrite_manifests().rewrite_delete_manifests(true);
    let grouped = match sort_by {
        Some(columns) => unclustered
            .sort_by_columns(columns)
            .map_err(|error| illegal_argument_error(error.message().to_string()))?,
        None => {
            // One cluster key, so every matching entry lands in one manifest per spec.
            unclustered.cluster_by(|_| String::new())
        }
    };
    let action = grouped.rewrite_if(move |manifest| {
        manifest.partition_spec_id == spec_id
            && match manifest.content {
                ManifestContentType::Data => data_work,
                ManifestContentType::Deletes => delete_work,
            }
    });
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let committed = match tx.commit(catalog.as_ref()).await {
        Err(error) if is_sort_column_refusal(&error) => {
            return Err(illegal_argument_error(error.message().to_string()));
        }
        result => result.map_err(iceberg_err)?,
    };

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;

    let new_snapshot = committed.metadata().current_snapshot().ok_or_else(|| {
        DataFusionError::Execution(
            "rewrite_manifests committed but the table has no current snapshot".to_string(),
        )
    })?;
    let rewritten = summary_count(new_snapshot, "manifests-replaced")?;
    let added = summary_count(new_snapshot, "manifests-created")?;
    count_result(ctx, rewritten, added)
}

async fn match_manifests(
    table: &Table,
    snapshot: &Snapshot,
    spec_id: i32,
) -> Result<MatchingManifests> {
    let entries = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .map_err(iceberg_err)?;
    let mut matching = MatchingManifests {
        data_count: 0,
        data_bytes: 0,
        delete_count: 0,
        delete_bytes: 0,
    };
    for manifest in entries.entries() {
        if manifest.partition_spec_id != spec_id {
            continue;
        }
        match manifest.content {
            ManifestContentType::Data => {
                matching.data_count += 1;
                matching.data_bytes = matching.data_bytes.saturating_add(manifest_bytes(manifest));
            }
            ManifestContentType::Deletes => {
                matching.delete_count += 1;
                matching.delete_bytes = matching
                    .delete_bytes
                    .saturating_add(manifest_bytes(manifest));
            }
        }
    }
    Ok(matching)
}

/// A manifest length is a signed field in the spec.
fn manifest_bytes(manifest: &ManifestFile) -> u64 {
    u64::try_from(manifest.manifest_length).unwrap_or(0)
}

/// Java's `RewriteManifests.targetNumManifests` divisor (`commit.manifest.target-size-bytes`).
fn target_manifest_size_bytes(table: &Table) -> u64 {
    table
        .metadata()
        .properties()
        .get(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT)
}

fn is_leg_work(count: usize, bytes: u64, target_bytes: u64) -> bool {
    count > 1 || bytes > target_bytes
}

fn is_sort_column_refusal(error: &Error) -> bool {
    const NEEDLES: [&str; 2] = ["Cannot sort by column", "Cannot sort by columns"];
    NEEDLES
        .iter()
        .any(|needle| error.message().contains(needle))
}

/// A summary count Spark reads as one of its two columns.
fn summary_count(snapshot: &Snapshot, key: &str) -> Result<i32> {
    let raw = snapshot
        .summary()
        .additional_properties
        .get(key)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "rewrite_manifests committed but its snapshot summary has no `{key}` — refusing \
                 to report a count the engine cannot source"
            ))
        })?;
    raw.parse::<i32>().map_err(|_| {
        DataFusionError::Execution(format!(
            "rewrite_manifests snapshot summary `{key}` is `{raw}`, which is not an i32 count"
        ))
    })
}

/// Spark's two non-nullable `int` columns (jar `OUTPUT_TYPE`, `iconst_0` per `StructField`).
fn result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("rewritten_manifests_count", DataType::Int32, false),
        Field::new("added_manifests_count", DataType::Int32, false),
    ]))
}

fn count_result(ctx: &SessionContext, rewritten: i32, added: i32) -> Result<DataFrame> {
    let batch = RecordBatch::try_new(
        result_schema(),
        vec![
            Arc::new(Int32Array::from(vec![rewritten])),
            Arc::new(Int32Array::from(vec![added])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

fn zero_result(ctx: &SessionContext) -> Result<DataFrame> {
    count_result(ctx, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_small_manifest_is_quiet_on_either_leg() {
        assert!(!is_leg_work(1, 100, 8192));
        assert!(!is_leg_work(0, 0, 8192));
    }

    #[test]
    fn two_manifests_or_an_over_target_one_is_work() {
        assert!(is_leg_work(2, 100, 8192));
        assert!(is_leg_work(1, 8193, 8192));
    }
}
