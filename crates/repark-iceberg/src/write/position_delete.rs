//! Position-delete file writing — the merge-on-read WRITE primitive.
//!
//! A merge-on-read row-level operation does not rewrite the data file a mutated row lives in. It
//! records the row's `(data file path, ordinal)` identity in a **position-delete file** and commits
//! that file alongside any new data files in ONE `RowDelta` snapshot; the next scan applies the
//! deletes (fork `GAP_MATRIX` row R117, `arrow/delete_filter.rs`). This module owns the "turn
//! `(_file, _pos)` pairs into committable position-delete [`DataFile`]s" half of that recipe — the
//! commit half lives with the operation that produced the pairs ([`crate::write::merge`]).
//!
//! The writer itself is the fork's production `PositionDeleteFileWriter` (row R113) — `RePark` never
//! hand-rolls a delete-file encoder. What `RePark` owns is the two things the fork's writer
//! deliberately leaves to its caller:
//!
//!   * **Sort order.** The Iceberg spec requires every position-delete file's rows to be ascending
//!     by `(file_path, pos)`; the fork's writer is write-as-given (R113: "sorting is the caller's
//!     job, matching Java"). A concurrent Iceberg scan interleaves data files in arbitrary order, so
//!     the pairs a MERGE collects are NOT sorted at collection time — [`sort_position_delete_pairs`]
//!     restores spec order before anything is written.
//!   * **Partition stamping.** A position-delete file is associated with the `(spec_id, partition)`
//!     of the DATA file it deletes from — the commit validates the delete file's partition against
//!     the registered spec. So the pairs are grouped by their target data file's OWN
//!     `(spec_id, partition)` (read off the live manifests) and one delete file is written per group,
//!     stamped with that group's [`PartitionKey`]. This mirrors Java `PositionDeleteWriter` (always
//!     carries a per-data-file `PartitionKey`) and the fork's own merge-on-read DELETE/UPDATE arm in
//!     `iceberg-datafusion`'s `physical_plan/delete.rs`, which `RePark`'s MERGE arm is built to match
//!     file-for-file.
//!
//! Reading the partition off the DATA FILE (not off the table's current default spec) is what makes
//! the stamp correct: the row being deleted physically lives in that file, under the spec that file
//! was written with.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::error::{DataFusionError, Result};
use futures::stream::{self, StreamExt};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, MetricsConfig, PartitionKey,
    Struct,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use uuid::Uuid;

use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::iceberg_err;
use crate::write::writer_props::writer_properties_for;

// === r20 P2a: merge ===
/// One deleted row's identity: the data file it lives in, and its 0-based ordinal within that file.
/// Exactly the `(_file, _pos)` pair the pinned core scan surfaces (fork `metadata_columns.rs`).
///
/// Path is `Arc<str>` so MERGE Stage B path interning (P1b scout #2) can share one allocation
/// across the seen-pair set, the pos-delete list, and this writer without re-cloning `String`
/// per mutated row (P2a residual).
pub(crate) type PositionDeletePair = (Arc<str>, i64);

/// ===========================================================================================
/// Sort `(file_path, pos)` pairs into the ascending order the Iceberg spec requires of every
/// position-delete file.
///
/// The fork's `PositionDeleteFileWriter` writes rows AS GIVEN (row R113 — "sorting is the caller's
/// job, matching Java"), and the pairs reach us in scan order, which a concurrent scan interleaves
/// across data files arbitrarily. Tuple ordering on `(String, i64)` is lexicographic-then-numeric —
/// exactly the spec's `(file_path, pos)` ordering. A named seam so the ordering guarantee can be
/// pinned by a deterministic unit test, independent of any scan's interleaving.
/// ===========================================================================================
pub(crate) fn sort_position_delete_pairs(pairs: &mut [PositionDeletePair]) {
    pairs.sort();
}

/// ===========================================================================================
/// Write real Parquet position-delete file(s) for `pairs`, each stamped with the
/// `(spec_id, partition)` of the DATA file it deletes from.
///
/// Returns EVERY file the (rolling) writer produced: a large delete set may roll into more than one
/// file and ALL of them must be committed, or the deletes in a dropped file would be silently lost
/// (rows resurrected on the next scan).
///
/// **Every** pair is grouped by the `(spec_id, partition)` of the DATA FILE IT DELETES FROM —
/// there is deliberately NO "the table is unpartitioned, so skip the lookup" fast path. Keying that
/// shortcut off the table's DEFAULT spec is the wrong predicate: under partition-spec evolution a
/// table whose *current* default is unpartitioned can still hold live data files written under an
/// earlier PARTITIONED spec, and those files' deletes must carry that spec's partition, not an empty
/// one. (`RePark`'s own DDL cannot produce that state today, and the fork currently fails such a
/// commit loud and atomically — `Partition value is not compatible` — so the bug is masked rather
/// than live. It is still the wrong predicate, and a Glue table evolved by Spark/Java reaches the
/// state.) A group whose resolved spec IS unpartitioned still gets a `None` partition key, so the
/// unpartitioned case is a *result* of the lookup rather than an assumption ahead of it — the
/// manifest walk is the same one the copy-on-write arm already does per MERGE.
///
/// The caller need not pre-sort: every group is sorted here immediately before it is written.
///
/// # Errors
/// Returns a DataFusion error if the writer config/stack cannot be built, a manifest cannot be
/// loaded, a pair references a data file that is not live in the pinned snapshot or whose partition
/// spec is not registered on the table, or a non-empty group somehow produced no delete file (an
/// internal invariant — silently losing deletes is never acceptable).
/// ===========================================================================================
pub(crate) async fn write_position_deletes(
    table: &Table,
    pairs: &[PositionDeletePair],
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>> {
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    let config = PositionDeleteWriterConfig::new().map_err(iceberg_err)?;
    let metadata = table.metadata();

    // Stamp each delete file with the partition of the data file it deletes FROM, never the table's
    // current default — the deleted row physically lives in that file, under the spec that file was
    // written with, and the commit validates the delete file's partition against it.
    let partitions = data_file_partitions(table).await?;
    let mut groups: HashMap<(i32, Struct), Vec<PositionDeletePair>> = HashMap::new();
    for pair in pairs {
        let key = partitions.get(pair.0.as_ref()).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "position-delete: data file `{}` is not live in the current snapshot's manifests",
                pair.0
            ))
        })?;
        // Arc clone — not a path-string allocation (P2a).
        groups
            .entry(key)
            .or_default()
            .push((Arc::clone(&pair.0), pair.1));
    }

    // Sort each group before any write so per-file `(file_path, pos)` order is preserved even when
    // groups are written concurrently (spec invariant is within-file, not across files).
    let mut prepared = Vec::with_capacity(groups.len());
    for ((spec_id, partition), mut group) in groups {
        sort_position_delete_pairs(&mut group);
        let spec = metadata
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "position-delete: data file references unknown partition spec {spec_id}"
                ))
            })?
            .as_ref()
            .clone();
        let partition_key = if spec.is_unpartitioned() {
            None
        } else {
            Some(PartitionKey::new(
                spec,
                metadata.current_schema().clone(),
                partition,
            ))
        };
        prepared.push((group, partition_key));
    }

    let max_concurrent = concurrency.max_concurrent_files.max(1);
    if max_concurrent == 1 || prepared.len() <= 1 {
        let mut delete_files = Vec::with_capacity(prepared.len());
        for (group, partition_key) in prepared {
            delete_files.extend(
                write_position_deletes_for_partition(table, &config, &group, partition_key).await?,
            );
        }
        return Ok(delete_files);
    }

    // Parallelize ACROSS delete-file groups only (each group is already sorted). Bound by K.
    let results: Vec<Result<Vec<DataFile>>> = stream::iter(prepared)
        .map(|(group, partition_key)| {
            let config = config.clone();
            async move {
                write_position_deletes_for_partition(table, &config, &group, partition_key).await
            }
        })
        .buffer_unordered(max_concurrent)
        .collect()
        .await;

    let mut delete_files = Vec::new();
    for result in results {
        delete_files.extend(result?);
    }
    Ok(delete_files)
}

/// ===========================================================================================
/// Map every LIVE data file path in the current snapshot to the `(spec_id, partition)` it was
/// written under, by walking that snapshot's DATA manifests.
///
/// The same manifest walk [`crate::write::merge::resolve_affected_data_files`] does for the copy-on-write
/// arm, projected to just what the position-delete stamp needs. A table with no snapshot yields an
/// empty map — there is nothing to delete from.
/// ===========================================================================================
async fn data_file_partitions(table: &Table) -> Result<HashMap<String, (i32, Struct)>> {
    let metadata = table.metadata();
    let mut partitions = HashMap::new();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(partitions);
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map_err(iceberg_err)?;
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(iceberg_err)?;
        for entry in manifest.entries() {
            if entry.is_alive() && entry.data_file().content_type() == DataContentType::Data {
                let file = entry.data_file();
                partitions
                    .entry(file.file_path().to_string())
                    .or_insert_with(|| (file.partition_spec_id(), file.partition().clone()));
            }
        }
    }
    Ok(partitions)
}

/// ===========================================================================================
/// Write ONE `(spec_id, partition)` group's pairs through the fork's `PositionDeleteFileWriter`.
///
/// `pairs` must already be in spec `(file_path, pos)` order (the callers sort each group). The
/// Parquet metrics config is `for_position_delete` — Java's choice, which keeps the `file_path` /
/// `pos` bounds FULL so delete-file path pruning stays precise (the default `truncate(16)` would
/// widen the path range and defeat it). A non-empty group that produced no file is an internal
/// error, never a silent success: the deletes would be lost and the rows resurrected.
/// ===========================================================================================
async fn write_position_deletes_for_partition(
    table: &Table,
    config: &PositionDeleteWriterConfig,
    pairs: &[PositionDeletePair],
    partition_key: Option<PartitionKey>,
) -> Result<Vec<DataFile>> {
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(iceberg_err)?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "pos-del".to_string(),
        Some(Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );
    let parquet_builder =
        ParquetWriterBuilder::new(writer_properties_for(table)?, config.schema().clone())
            .with_metrics_config(MetricsConfig::for_position_delete());
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut writer = PositionDeleteFileWriterBuilder::new(rolling_builder, config.clone())
        .build(partition_key)
        .await
        .map_err(iceberg_err)?;

    let paths: Vec<&str> = pairs.iter().map(|(path, _)| path.as_ref()).collect();
    let positions: Vec<i64> = pairs.iter().map(|(_, pos)| *pos).collect();
    let batch = RecordBatch::try_new(
        config.arrow_schema().clone(),
        vec![
            std::sync::Arc::new(StringArray::from(paths)) as ArrayRef,
            std::sync::Arc::new(Int64Array::from(positions)) as ArrayRef,
        ],
    )?;
    writer.write(batch).await.map_err(iceberg_err)?;
    let files = writer.close().await.map_err(iceberg_err)?;
    if files.is_empty() {
        return Err(DataFusionError::Internal(
            "position-delete writer produced no file for a non-empty pair group (the deletes \
             would be silently lost and the rows resurrected on the next scan)"
                .to_string(),
        ));
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PIN T-SORT — [`sort_position_delete_pairs`] restores the Iceberg spec's ascending
    /// `(file_path, pos)` order from scan-interleaved input. The fork's writer is write-as-given
    /// (R113), so THIS is the only thing standing between an interleaved scan and a spec-violating
    /// delete file. Deterministic: fixed input, no scan involved. Mutation: making the sort a no-op
    /// leaves the interleaved order and turns this RED.
    #[test]
    fn sorts_pairs_by_file_then_position() {
        // Scan-interleaved: files out of order, and positions out of order within a file.
        let mut pairs: Vec<PositionDeletePair> = vec![
            (Arc::from("f2"), 1),
            (Arc::from("f1"), 5),
            (Arc::from("f2"), 0),
            (Arc::from("f1"), 0),
            (Arc::from("f10"), 3),
        ];
        sort_position_delete_pairs(&mut pairs);
        let paths: Vec<(&str, i64)> = pairs
            .iter()
            .map(|(path, pos)| (path.as_ref(), *pos))
            .collect();
        assert_eq!(
            paths,
            vec![
                ("f1", 0),
                ("f1", 5),
                // Lexicographic on the PATH — "f10" < "f2" as strings, which is what the spec's
                // byte-ordering means (paths are compared as strings, never as numbers).
                ("f10", 3),
                ("f2", 0),
                ("f2", 1),
            ]
        );
    }

    /// PIN P2a — `PositionDeletePair` path is `Arc<str>` so sort/group clone does not re-allocate
    /// the path bytes (strong count rises; pointer identity shared).
    #[test]
    fn position_delete_pair_path_is_arc_shared() {
        let path: Arc<str> = Arc::from("s3://bucket/data/file-0001.parquet");
        let pair_a: PositionDeletePair = (Arc::clone(&path), 0);
        let pair_b: PositionDeletePair = (Arc::clone(&path), 1);
        assert!(Arc::ptr_eq(&pair_a.0, &pair_b.0));
        assert_eq!(Arc::strong_count(&path), 3);
        let mut group = vec![pair_b, pair_a];
        sort_position_delete_pairs(&mut group);
        assert!(Arc::ptr_eq(&group[0].0, &path));
        assert_eq!(group[0].1, 0);
        assert_eq!(group[1].1, 1);
    }
}
