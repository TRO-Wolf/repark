//! `CALL <catalog>.system.rewrite_manifests(…)` over the fork's `RewriteManifestsAction`.
//! The live file set stays identical; only manifest grouping changes.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{ManifestContentType, ManifestFile, Snapshot, TableProperties};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, table::Table};

use super::{CallArgs, resolve_table_ident};
use crate::{iceberg_err, reregister};

/// The current snapshot's manifests, split the way Spark's action splits them.
struct MatchingManifests {
    /// Data manifests at the table's current spec — the leg this engine rewrites.
    data_count: usize,
    /// The byte size of those data manifests, for Spark's `targetNumManifests` rule.
    data_bytes: u64,
    /// Delete manifests at the table's current spec — the leg the fork cannot rewrite.
    delete_count: usize,
}

/// ===========================================================================================
/// Execute `CALL <catalog>.system.rewrite_manifests(table => …)`.
///
/// Return `rewritten_manifests_count` and `added_manifests_count` from the new snapshot summary
/// because the fork action returns no counts. Delete manifests remain unchanged; refusing a
/// zero-result case avoids a false clean signal when Spark would rewrite them.
///
/// # Errors
/// Plan / `NotImplemented` / iceberg commit failures as [`DataFusionError`].
/// ===========================================================================================
pub(super) async fn execute_rewrite_manifests(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "use_caching", "spec_id"])?;
    // Spark positional order: table, use_caching, spec_id (jar `PARAMETERS`).
    args.reject_excess_positional(3)?;
    // Parse and drop. Spark's `use-caching` option caches its manifest DataFrame between the two
    // legs of its own action. This engine reads manifests directly, so the value changes nothing.
    // The type check is STRICTER than Spark, which casts a string literal and runs (registry
    // `MANIFEST-2`): a quoted argument on a procedure this small is more likely a typo than intent.
    args.optional_bool("use_caching", Some(1))?;
    if args.optional_i32("spec_id", Some(2))?.is_some() {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_manifests spec_id is not supported — this procedure always rewrites \
             the manifests of the table's CURRENT partition spec, which is Spark's default. \
             Spark can target an older spec; this engine cannot select one"
                .to_string(),
        ));
    }

    let table_arg = args.require_string("table", 0)?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    let Some(snapshot) = table.metadata().current_snapshot() else {
        // Spark finds no manifests on a table with no snapshot and answers zeros. The fork action
        // fails `DataInvalid` instead, so this returns before the action runs.
        return zero_result(ctx);
    };
    let spec_id = table.metadata().default_partition_spec_id();
    let matching = match_manifests(&table, snapshot, spec_id).await?;

    if is_data_leg_noop(&matching, target_manifest_size_bytes(&table)) {
        refuse_uncompactable_delete_manifests(&matching, &table_arg)?;
        return zero_result(ctx);
    }

    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        // One cluster key, so every matching entry lands in one manifest per spec. Both engines
        // read `commit.manifest.target-size-bytes`, but they size differently above it: Java
        // repartitions into `ceil(total / target)` groups, and the fork rolls on a running
        // estimate. Below the target — the 8 MB default — both write one manifest and the counts
        // agree; above it `added_manifests_count` diverges (registry `MANIFEST-3`).
        .cluster_by(|_| String::new())
        // Java's default filter — `RewriteManifestsSparkAction` rewrites the current spec only.
        // Without it a table whose spec evolved rewrites manifests Spark keeps.
        .rewrite_if(move |manifest| manifest.partition_spec_id == spec_id);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    let committed = tx.commit(catalog.as_ref()).await.map_err(iceberg_err)?;

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

/// Split the current snapshot's manifest list into Spark's two legs at the current spec.
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
            ManifestContentType::Deletes => matching.delete_count += 1,
        }
    }
    Ok(matching)
}

/// A manifest length is a signed field in the spec. A negative one is corrupt metadata, and it
/// must not shrink the total that decides whether the rewrite runs.
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

/// Spark's no-op rule: one matching manifest that already fits one target manifest is left alone
/// (`targetNumManifests == 1 && matching.size() == 1`), and no snapshot is committed. Measured on
/// the live oracle 2026-08-23: a second call on a freshly rewritten table answers `0, 0` and adds
/// no snapshot. Without this the fork would rewrite that manifest into itself and answer `1, 1`.
fn is_data_leg_noop(matching: &MatchingManifests, target_bytes: u64) -> bool {
    matching.data_count <= 1 && matching.data_bytes <= target_bytes
}

/// Refuse rather than answer zeros a caller reads as "nothing to compact".
///
/// Spark rewrites delete manifests in a second leg of the same procedure; the fork's action keeps
/// every delete manifest byte-identical. Two or more of them is work Spark would do, so zeros
/// here would be false. Below two, Spark's own delete leg is a no-op and the zeros are honest.
fn refuse_uncompactable_delete_manifests(
    matching: &MatchingManifests,
    table_arg: &str,
) -> Result<()> {
    if matching.delete_count < 2 {
        return Ok(());
    }
    Err(DataFusionError::NotImplemented(format!(
        "CALL rewrite_manifests found nothing to do on the data manifests of `{table_arg}`, and \
         it will not report zeros while {} delete manifest(s) stay uncompacted. Apache Spark \
         rewrites delete manifests in a second leg of this procedure; the owned fork's action \
         carries every delete manifest forward unchanged, so this engine cannot. Compact the \
         delete FILES first with `CALL rewrite_position_delete_files`, which reduces how many \
         delete manifests later commits produce",
        matching.delete_count
    )))
}

/// A summary count Spark reads as one of its two columns.
///
/// The fork writes both keys on every rewrite commit. A missing or unparsable one means the
/// action changed shape, and this refuses rather than reporting a fabricated zero.
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

    fn matching(data_count: usize, data_bytes: u64, delete_count: usize) -> MatchingManifests {
        MatchingManifests {
            data_count,
            data_bytes,
            delete_count,
        }
    }

    /// pins: mw-6-rewrite-manifests/C-004
    #[test]
    fn the_no_op_rule_is_sparks_target_num_manifests_rule() {
        // One manifest inside the target: Spark leaves it alone.
        assert!(is_data_leg_noop(&matching(1, 100, 8192), 8192));
        // One manifest over the target: Spark splits it, so this engine runs too.
        assert!(!is_data_leg_noop(&matching(1, 8193, 0), 8192));
        // Two manifests: Spark merges them however small they are.
        assert!(!is_data_leg_noop(&matching(2, 100, 0), 8192));
        // No data manifests at all: nothing matches, so zeros.
        assert!(is_data_leg_noop(&matching(0, 0, 3), 8192));
    }

    /// pins: mw-6-rewrite-manifests/C-005
    #[test]
    fn zeros_refuse_only_when_spark_would_compact_delete_manifests() {
        assert!(refuse_uncompactable_delete_manifests(&matching(1, 10, 0), "sales.t").is_ok());
        assert!(refuse_uncompactable_delete_manifests(&matching(1, 10, 1), "sales.t").is_ok());
        let error = refuse_uncompactable_delete_manifests(&matching(1, 10, 2), "sales.t")
            .expect_err("two delete manifests is work Spark would do");
        assert!(error.to_string().contains("delete manifest"));
    }
}
