//! Position-delete file writing for merge-on-read operations.
//!
//! The module converts streamed `(_file, _pos)` pairs into fork
//! `PositionDeleteFileWriter` data files. It sorts pairs by `(file_path, pos)` and groups them by
//! the configured granularity. Every group uses the owning data file's `(spec_id, partition)`;
//! unpartitioned non-zero specs pass the resolved spec to avoid the fork's spec-0 default.
//! Commit coordination remains in [`crate::write::merge`].

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::error::{DataFusionError, Result};
use futures::stream::{self, StreamExt};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, MetricsConfig, PartitionKey,
    PartitionSpec, Struct,
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
use iceberg::{Catalog, TableIdent};
use uuid::Uuid;

use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::iceberg_err;
use crate::write::writer_props::writer_properties_for;

/// Iceberg default partition-spec id used by the fork when no spec is supplied.
const DEFAULT_PARTITION_SPEC_ID: i32 = 0;

/// Iceberg table property for position-delete grouping. The Spark default is `file`.
pub const DELETE_GRANULARITY_PROP: &str = "write.delete.granularity";

/// How position-delete pairs are grouped into files. The fork writer has no knob — grouping
/// is the engine's (`ENGINE_CONTRACT` §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeleteGranularity {
    /// One delete file per referenced data file (Spark default).
    File,
    /// One delete file per `(spec_id, partition)` group (Iceberg-core default).
    Partition,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PositionDeleteGroupKey {
    Partition { spec_id: i32, partition: Struct },
    File(Arc<str>),
}

/// One deleted row's identity: the data file it lives in, and its 0-based ordinal within that file.
/// Exactly the `(_file, _pos)` pair the pinned core scan surfaces (fork `metadata_columns.rs`).
///
/// `Arc<str>` lets the path be shared across the seen-pair set, delete list, and writer.
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
/// Parse `write.delete.granularity`. Absent → [`DeleteGranularity::File`] (Spark). `'file'` /
/// `'partition'` accepted case-insensitively. Anything else refuses loud.
///
/// Model: Grok 4.6 xHigh
/// pins: mw-9-delete-granularity/C-001, C-004
/// ===========================================================================================
///
/// # Errors
/// Present-but-unrecognised value.
pub(crate) fn parse_delete_granularity(raw: Option<&str>) -> Result<DeleteGranularity> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DeleteGranularity::File);
    };
    if value.eq_ignore_ascii_case("file") {
        return Ok(DeleteGranularity::File);
    }
    if value.eq_ignore_ascii_case("partition") {
        return Ok(DeleteGranularity::Partition);
    }
    Err(DataFusionError::Plan(format!(
        "table property `{DELETE_GRANULARITY_PROP}` = '{value}' is not supported \
         (accepted: 'file', 'partition'; Spark default is 'file')"
    )))
}

/// ===========================================================================================
/// Write real Parquet position-delete file(s) for `pairs`, each stamped with the
/// `(spec_id, partition)` of the DATA file it deletes from.
///
/// Returns EVERY file the (rolling) writer produced: a large delete set may roll into more than one
/// file and ALL of them must be committed, or the deletes in a dropped file would be silently lost
/// (rows resurrected on the next scan).
///
/// **Every** pair is grouped by [`parse_delete_granularity`]: `file` (Spark default) keys on the
/// data-file path; `partition` keys on the data file's `(spec_id, partition)`. There is
/// deliberately NO "the table is unpartitioned, so skip the lookup" fast path. Keying that
/// shortcut off the table's DEFAULT spec is the wrong predicate: under partition-spec evolution a
/// table whose *current* default is unpartitioned can still hold live data files written under an
/// earlier PARTITIONED spec, and those files' deletes must carry that spec's partition, not an empty
/// one. (`RePark`'s own DDL can produce that state via [`crate::write::alter::apply_partition_spec_changes`]
/// — drop the last partition field — and a Glue table evolved by Spark/Java reaches it too.) A
/// group whose resolved spec IS unpartitioned still gets a `None` partition key (empty-tuple
/// semantics). That `None` is a *result* of the lookup, not an assumption keyed off the table's
/// current default. It is not enough on its own: the fork's writer, given no key and no
/// `.with_partition_spec`, stamps `DEFAULT_PARTITION_SPEC_ID` (0). When spec 0 is still the
/// original *partitioned* spec, the commit then fails loud (`Partition value is not compatible`);
/// when spec 0 happens to be unpartitioned too, the delete commits and never applies. So an
/// unpartitioned group whose resolved spec id is not 0 also chains `.with_partition_spec(spec)`.
/// The spec-0 unpartitioned path stays `None` + no configured spec — the fork default is correct
/// only there. The manifest walk is the same one the copy-on-write arm already does per MERGE.
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
    let prepared = prepare_position_delete_groups(table, pairs).await?;

    let max_concurrent = concurrency.max_concurrent_files.max(1);
    if max_concurrent == 1 || prepared.len() <= 1 {
        let mut delete_files = Vec::with_capacity(prepared.len());
        for (group, partition_key, builder_spec) in prepared {
            delete_files.extend(
                write_position_deletes_for_partition(
                    table,
                    &config,
                    &group,
                    partition_key,
                    builder_spec,
                )
                .await?,
            );
        }
        return Ok(delete_files);
    }

    // Parallelize ACROSS delete-file groups only (each group is already sorted). Bound by K.
    let results: Vec<Result<Vec<DataFile>>> = stream::iter(prepared)
        .map(|(group, partition_key, builder_spec)| {
            let config = config.clone();
            async move {
                write_position_deletes_for_partition(
                    table,
                    &config,
                    &group,
                    partition_key,
                    builder_spec,
                )
                .await
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
/// Group pairs by [`parse_delete_granularity`], sort each group, and stamp the data-file partition.
///
/// Model: Grok 4.6 xHigh
/// ===========================================================================================
async fn prepare_position_delete_groups(
    table: &Table,
    pairs: &[PositionDeletePair],
) -> Result<
    Vec<(
        Vec<PositionDeletePair>,
        Option<PartitionKey>,
        Option<PartitionSpec>,
    )>,
> {
    let metadata = table.metadata();
    let granularity = parse_delete_granularity(
        metadata
            .properties()
            .get(DELETE_GRANULARITY_PROP)
            .map(String::as_str),
    )?;
    let partitions = data_file_partitions(table).await?;
    let mut groups: HashMap<PositionDeleteGroupKey, Vec<PositionDeletePair>> = HashMap::new();
    for pair in pairs {
        let (spec_id, partition) = partitions.get(pair.0.as_ref()).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "position-delete: data file `{}` is not live in the current snapshot's manifests",
                pair.0
            ))
        })?;
        let key = match granularity {
            DeleteGranularity::Partition => {
                PositionDeleteGroupKey::Partition { spec_id, partition }
            }
            DeleteGranularity::File => PositionDeleteGroupKey::File(Arc::clone(&pair.0)),
        };
        groups
            .entry(key)
            .or_default()
            .push((Arc::clone(&pair.0), pair.1));
    }

    let mut prepared = Vec::with_capacity(groups.len());
    for (key, mut group) in groups {
        sort_position_delete_pairs(&mut group);
        let (spec_id, partition) = match &key {
            PositionDeleteGroupKey::Partition { spec_id, partition } => {
                (*spec_id, partition.clone())
            }
            PositionDeleteGroupKey::File(path) => {
                partitions.get(path.as_ref()).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete: grouped data file `{path}` missing from live manifests"
                    ))
                })?
            }
        };
        let spec = metadata
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "position-delete: data file references unknown partition spec {spec_id}"
                ))
            })?
            .as_ref()
            .clone();
        let builder_spec = if spec.is_unpartitioned() && spec.spec_id() != DEFAULT_PARTITION_SPEC_ID
        {
            Some(spec.clone())
        } else {
            None
        };
        let partition_key = if spec.is_unpartitioned() {
            None
        } else {
            Some(
                PartitionKey::new(spec, metadata.current_schema().clone(), partition)
                    .map_err(iceberg_err)?,
            )
        };
        prepared.push((group, partition_key, builder_spec));
    }
    Ok(prepared)
}

/// ===========================================================================================
/// Write ONE `(spec_id, partition)` group's pairs through the fork's `PositionDeleteFileWriter`.
///
/// `pairs` must already be in spec `(file_path, pos)` order (the callers sort each group).
/// `builder_spec` is the resolved unpartitioned-but-not-spec-0 spec, if any: the fork's writer
/// stamps `DEFAULT_PARTITION_SPEC_ID` (0) when `partition_key` is `None` unless
/// `.with_partition_spec` is chained (M16). The spec-0 unpartitioned path passes `None` here so
/// that construction stays byte-identical. The Parquet metrics config is `for_position_delete` —
/// Java's choice, which keeps the `file_path` / `pos` bounds FULL so delete-file path pruning
/// stays precise (the default `truncate(16)` would widen the path range and defeat it). A
/// non-empty group that produced no file is an internal error, never a silent success: the
/// deletes would be lost and the rows resurrected.
/// ===========================================================================================
async fn write_position_deletes_for_partition(
    table: &Table,
    config: &PositionDeleteWriterConfig,
    pairs: &[PositionDeletePair],
    partition_key: Option<PartitionKey>,
    builder_spec: Option<PartitionSpec>,
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
    let mut writer_builder = PositionDeleteFileWriterBuilder::new(rolling_builder, config.clone());
    if let Some(spec) = builder_spec {
        writer_builder = writer_builder.with_partition_spec(spec);
    }
    let mut writer = writer_builder
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

/// Which SQL DML verb is being gated for the BUG-001 merge-on-read multi-spec hazard.
#[derive(Debug, Clone, Copy)]
pub enum MorDmlKind {
    /// SQL `DELETE` (gated via `write.delete.mode`).
    Delete,
    /// SQL `UPDATE` (gated via `write.update.mode`).
    Update,
}

impl MorDmlKind {
    /// The table property naming the write mode this verb consults.
    #[must_use]
    pub const fn mode_property(self) -> &'static str {
        match self {
            Self::Delete => "write.delete.mode",
            Self::Update => "write.update.mode",
        }
    }

    /// The SQL verb, for refuse messages.
    #[must_use]
    pub const fn verb(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
        }
    }
}

/// ===========================================================================================
/// BUG-001 P0 valve: SQL `DELETE`/`UPDATE` via iceberg-datafusion merge-on-read stamps
/// every position delete with `partition_key = None` when the **current default** partition
/// spec is unpartitioned — after partition-spec evolution that leaves older data files under
/// prior specs, deletes can commit while rows remain visible (fork
/// `physical_plan/delete.rs` unpartitioned fast path; `ENGINE_CONTRACT` §7a).
///
/// Refuse when **all** of: (1) `write.{delete,update}.mode = merge-on-read`, (2) current default
/// spec is unpartitioned, (3) table metadata carries more than one partition spec in history.
/// Over-refuse is OK; under-refuse is not. **`MERGE` is never gated here** (repark-owned
/// merge-on-read writer is fixed).
///
/// Hoisted from the v1 SQL crate's `normalize` module (phase-2 PR-3b declared rename): this is
/// the catalog-handle half of the valve — it lives beside the position-delete path whose fork
/// hazard it gates. The SQL door keeps the [`ObjectName`]-resolution wrapper and calls here;
/// `table_sql` is the caller's display form of the target for the refuse message.
///
/// # Errors
/// [`DataFusionError::Plan`] naming the fork hazard and copy-on-write / `MERGE` workarounds.
/// ===========================================================================================
pub async fn refuse_mor_unpartitioned_multi_spec_dml(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    table_sql: &str,
    kind: MorDmlKind,
) -> Result<()> {
    let Ok(table) = catalog.load_table(ident).await else {
        // Missing table → DF/path will raise; not our refuse.
        return Ok(());
    };
    let metadata = table.metadata();
    // Case/trim tolerant: under-refuse on `Merge-on-Read` / padded values would re-open the
    // silent under-delete path. Over-refuse of odd spellings is OK.
    let is_merge_on_read = metadata
        .properties()
        .get(kind.mode_property())
        .is_some_and(|mode| mode.trim().eq_ignore_ascii_case("merge-on-read"));
    if !is_merge_on_read {
        return Ok(());
    }
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Ok(());
    }
    let spec_count = metadata.partition_specs_iter().len();
    if spec_count <= 1 {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "refusing SQL {} on Iceberg table `{table_sql}`: write mode is merge-on-read, the \
         current partition spec is unpartitioned, and the table has {spec_count} partition \
         specs in history — the iceberg-datafusion MoR position-delete path stamps deletes \
         with partition_key=None for unpartitioned current specs without looking up each data \
         file's real (spec_id, partition), which can silently under-delete after partition-spec \
         evolution (owned fork issue: integrations/datafusion physical_plan/delete.rs \
         write_position_deletes unpartitioned fast path; ENGINE_CONTRACT §7a). Workarounds: \
         set {}='copy-on-write' (or unset for COW default), or use MERGE INTO (RePark-owned MoR \
         writer stamps per-file partitions correctly)",
        kind.verb(),
        kind.mode_property(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// pins: mw-9-delete-granularity/C-001, C-004
    #[test]
    fn parse_delete_granularity_spark_default_and_refuse() {
        assert_eq!(
            parse_delete_granularity(None).expect("absent"),
            DeleteGranularity::File
        );
        assert_eq!(
            parse_delete_granularity(Some("  ")).expect("whitespace-only is absent"),
            DeleteGranularity::File
        );
        assert_eq!(
            parse_delete_granularity(Some("file")).expect("file"),
            DeleteGranularity::File
        );
        assert_eq!(
            parse_delete_granularity(Some("FILE")).expect("FILE"),
            DeleteGranularity::File
        );
        assert_eq!(
            parse_delete_granularity(Some("partition")).expect("partition"),
            DeleteGranularity::Partition
        );
        let err = parse_delete_granularity(Some("banana"))
            .expect_err("unknown value")
            .to_string();
        assert!(
            err.contains(DELETE_GRANULARITY_PROP)
                && err.contains("'file'")
                && err.contains("'partition'")
                && err.contains("banana"),
            "refuse must name the property, both quoted legal values, and the illegal \
             value (unquoted `file` is already a substring of the property name): {err}"
        );
    }

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

    /// PIN M16 — a spec-evolved unpartitioned table (spec 0 partitioned → spec 1
    /// unpartitioned) writes data under spec 1. The position-delete writer must stamp
    /// that resolved spec id, not the fork's `DEFAULT_PARTITION_SPEC_ID` (0) fallback
    /// that applies when `partition_key = None` and `.with_partition_spec` is omitted.
    ///
    /// Spec 0 is still the original *partitioned* spec, so a spec-0 empty-tuple delete
    /// is a loud `Partition value is not compatible` `RowDelta` commit (fork
    /// `ENGINE_CONTRACT` §7a), not a silently inapplicable delete. Mutation: drop
    /// `.with_partition_spec` on the unpartitioned-but-not-spec-0 branch → this
    /// asserts `partition_spec_id() == evolved` and turns RED.
    #[tokio::test]
    async fn evolved_unpartitioned_spec_position_delete_claims_resolved_spec_id() {
        let warehouse = tempfile::TempDir::new().expect("temp warehouse");
        let catalog = sales_memory_catalog(&warehouse).await;
        let ident = create_identity_then_drop_to_unpartitioned(&catalog, "m16").await;
        let evolved_spec_id = default_spec_id(&catalog, &ident).await;
        assert_ne!(evolved_spec_id, DEFAULT_PARTITION_SPEC_ID);
        assert!(default_spec_is_unpartitioned(&catalog, &ident).await);

        append_id_v(&catalog, &ident, &[1, 2, 3], &["a", "b", "c"]).await;
        let data_files = live_content_files(&catalog, &ident, DataContentType::Data).await;
        assert_eq!(data_files.len(), 1, "one post-evolve data file");
        assert_eq!(
            data_files[0].partition_spec_id(),
            evolved_spec_id,
            "fixture: the data file must be written under the evolved spec"
        );
        let data_path: Arc<str> = Arc::from(data_files[0].file_path());

        let table = catalog.load_table(&ident).await.expect("reload for writer");
        let written = write_position_deletes(
            &table,
            &[(Arc::clone(&data_path), 1)],
            WriteConcurrency::new(1).expect("K=1"),
        )
        .await
        .expect("write position deletes for the spec-1 file");
        assert_eq!(
            written.len(),
            1,
            "one unpartitioned group ⇒ one delete file"
        );
        let merge_result = merge_delete_id(&catalog, &ident, 2).await;
        assert_eq!(
            written[0].partition_spec_id(),
            evolved_spec_id,
            "M16: the emitted delete must claim the resolved unpartitioned spec, not 0 \
             (MERGE outcome: {merge_result:?})"
        );
        merge_result.expect("MoR MERGE delete on a spec-1 file must commit");

        let committed =
            live_content_files(&catalog, &ident, DataContentType::PositionDeletes).await;
        assert_eq!(
            committed.len(),
            1,
            "exactly one committed position-delete file"
        );
        assert_eq!(
            committed[0].partition_spec_id(),
            evolved_spec_id,
            "the committed delete must carry the evolved spec (otherwise the row resurrects)"
        );
        assert_eq!(
            scanned_ids(&catalog, &ident).await,
            vec![1, 3],
            "the spec-stamped delete must apply: id=2 is gone"
        );
    }

    /// PIN M16 control — an unpartitioned-from-birth table is spec 0. The writer must keep
    /// stamping 0 there (the fork default is correct only for this path). Guards the
    /// unpartitioned-spec-0 branch against a blanket `.with_partition_spec` that would still
    /// happen to emit 0 today but would couple this path to the evolved-spec fix.
    #[tokio::test]
    async fn unpartitioned_spec_zero_position_delete_still_claims_spec_zero() {
        let warehouse = tempfile::TempDir::new().expect("temp warehouse");
        let catalog = sales_memory_catalog(&warehouse).await;
        let ident = create_unpartitioned_target(&catalog, "m16_spec0").await;
        assert_eq!(
            default_spec_id(&catalog, &ident).await,
            DEFAULT_PARTITION_SPEC_ID
        );
        assert!(default_spec_is_unpartitioned(&catalog, &ident).await);

        append_id_v(&catalog, &ident, &[1, 2], &["a", "b"]).await;
        let data_files = live_content_files(&catalog, &ident, DataContentType::Data).await;
        let data_path: Arc<str> = Arc::from(data_files[0].file_path());
        let table = catalog.load_table(&ident).await.expect("reload");
        let written = write_position_deletes(
            &table,
            &[(data_path, 0)],
            WriteConcurrency::new(1).expect("K=1"),
        )
        .await
        .expect("write spec-0 position deletes");
        assert_eq!(written.len(), 1);
        assert_eq!(
            written[0].partition_spec_id(),
            DEFAULT_PARTITION_SPEC_ID,
            "unpartitioned spec 0 must keep stamping 0"
        );
    }

    /// pins: mw-9-delete-granularity/C-001
    #[tokio::test]
    async fn default_file_granularity_writes_one_delete_file_per_data_file() {
        let written = write_two_file_deletes(&HashMap::new()).await;
        assert_eq!(
            written.len(),
            2,
            "Spark default file granularity: one MERGE-shaped write across two data files \
             must emit two delete files"
        );
    }

    /// pins: mw-9-delete-granularity/C-002
    #[tokio::test]
    async fn explicit_file_granularity_writes_one_delete_file_per_data_file() {
        let written = write_two_file_deletes(&HashMap::from([(
            DELETE_GRANULARITY_PROP.to_string(),
            "file".to_string(),
        )]))
        .await;
        assert_eq!(written.len(), 2);
    }

    /// pins: mw-9-delete-granularity/C-003
    #[tokio::test]
    async fn partition_granularity_writes_one_delete_file_for_an_unpartitioned_table() {
        let written = write_two_file_deletes(&HashMap::from([(
            DELETE_GRANULARITY_PROP.to_string(),
            "partition".to_string(),
        )]))
        .await;
        assert_eq!(
            written.len(),
            1,
            "partition granularity: the whole unpartitioned table is one group"
        );
    }

    async fn write_two_file_deletes(properties: &HashMap<String, String>) -> Vec<DataFile> {
        let warehouse = tempfile::TempDir::new().expect("temp warehouse");
        let catalog = sales_memory_catalog(&warehouse).await;
        let ident = create_unpartitioned_target_with(&catalog, "gran", properties).await;
        append_id_v(&catalog, &ident, &[1], &["a"]).await;
        append_id_v(&catalog, &ident, &[2], &["b"]).await;
        let data_files = live_content_files(&catalog, &ident, DataContentType::Data).await;
        assert_eq!(data_files.len(), 2);
        let pairs: Vec<PositionDeletePair> = data_files
            .iter()
            .map(|file| (Arc::from(file.file_path()), 0))
            .collect();
        let table = catalog.load_table(&ident).await.expect("reload");
        write_position_deletes(&table, &pairs, WriteConcurrency::new(1).expect("K=1"))
            .await
            .expect("write grouped deletes")
    }

    async fn sales_memory_catalog(warehouse: &tempfile::TempDir) -> Arc<dyn Catalog> {
        use iceberg::NamespaceIdent;
        let warehouse_path = warehouse
            .path()
            .to_str()
            .expect("utf-8 warehouse path")
            .to_string();
        let catalog = crate::catalog::memory_catalog(&warehouse_path)
            .await
            .expect("memory catalog");
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("create namespace");
        catalog
    }

    fn id_v_schema() -> iceberg::spec::Schema {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("id/v schema")
    }

    fn id_v_batch(ids: &[i32], values: &[&str]) -> RecordBatch {
        use datafusion::arrow::array::{Int32Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("v", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    values.iter().map(|value| Some(*value)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("id/v batch")
    }

    async fn create_unpartitioned_target(catalog: &Arc<dyn Catalog>, name: &str) -> TableIdent {
        create_unpartitioned_target_with(catalog, name, &HashMap::new()).await
    }

    async fn create_unpartitioned_target_with(
        catalog: &Arc<dyn Catalog>,
        name: &str,
        properties: &HashMap<String, String>,
    ) -> TableIdent {
        use iceberg::{NamespaceIdent, TableCreation};
        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string());
        catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name(name.to_string())
                    .schema(id_v_schema())
                    .properties(properties.clone())
                    .build(),
            )
            .await
            .expect("create unpartitioned table");
        ident
    }

    async fn create_identity_then_drop_to_unpartitioned(
        catalog: &Arc<dyn Catalog>,
        name: &str,
    ) -> TableIdent {
        use iceberg::spec::{Transform, UnboundPartitionSpec};
        use iceberg::{NamespaceIdent, TableCreation};

        use crate::write::alter::{PartitionSpecChange, apply_partition_spec_changes};

        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string());
        let initial_spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id", Transform::Identity)
            .expect("add identity partition field")
            .build();
        catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name(name.to_string())
                    .schema(id_v_schema())
                    .partition_spec(initial_spec)
                    .properties(HashMap::from([(
                        "write.merge.mode".to_string(),
                        "merge-on-read".to_string(),
                    )]))
                    .build(),
            )
            .await
            .expect("create identity-partitioned table");
        let created = catalog.load_table(&ident).await.expect("load created");
        assert_eq!(
            created.metadata().default_partition_spec_id(),
            DEFAULT_PARTITION_SPEC_ID,
            "fixture: create-time spec is spec 0"
        );
        assert!(
            !created
                .metadata()
                .default_partition_spec()
                .is_unpartitioned(),
            "fixture: spec 0 is partitioned"
        );
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::RemoveFieldByTransform {
                source_name: "id".to_string(),
                transform: Transform::Identity,
            }],
        )
        .await
        .expect("drop the only partition field");
        ident
    }

    async fn default_spec_id(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> i32 {
        catalog
            .load_table(ident)
            .await
            .expect("load")
            .metadata()
            .default_partition_spec_id()
    }

    async fn default_spec_is_unpartitioned(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> bool {
        catalog
            .load_table(ident)
            .await
            .expect("load")
            .metadata()
            .default_partition_spec()
            .is_unpartitioned()
    }

    async fn append_id_v(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
        ids: &[i32],
        values: &[&str],
    ) {
        crate::write::append::append(catalog, ident, vec![id_v_batch(ids, values)])
            .await
            .expect("append");
    }

    async fn merge_delete_id(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
        id: i32,
    ) -> datafusion::error::Result<()> {
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        use crate::write::merge::{MatchedAction, MatchedClause, MergeSpec, execute_merge};

        let ctx = SessionContext::new();
        let source_batch = id_v_batch(&[id], &["ignored"]);
        ctx.register_table(
            "src",
            Arc::new(
                MemTable::try_new(source_batch.schema(), vec![vec![source_batch]])
                    .expect("source memtable"),
            ),
        )
        .expect("register src");
        execute_merge(
            &ctx,
            catalog,
            &MergeSpec {
                target: ident.clone(),
                target_alias: "t".to_string(),
                source_from_sql: "src".to_string(),
                source_alias: "s".to_string(),
                on_sql: "t.id = s.id".to_string(),
                matched: vec![MatchedClause {
                    predicate_sql: None,
                    action: MatchedAction::Delete,
                }],
                not_matched: vec![],
            },
        )
        .await
    }

    async fn scanned_ids(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<i32> {
        use datafusion::arrow::array::Int32Array;
        let table = catalog.load_table(ident).await.expect("load for scan");
        let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(
            table
                .scan()
                .select(["id"])
                .build()
                .expect("build scan")
                .to_arrow()
                .await
                .expect("scan to_arrow"),
        )
        .await
        .expect("collect scan");
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id is Int32")
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Live DATA or DELETE files in the current snapshot (Added/Existing).
    async fn live_content_files(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
        content: DataContentType,
    ) -> Vec<DataFile> {
        use iceberg::spec::ManifestContentType;
        let table = catalog.load_table(ident).await.expect("load table");
        let metadata = table.metadata();
        let snapshot = metadata.current_snapshot().expect("current snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .expect("load manifest list");
        let want = match content {
            DataContentType::Data => ManifestContentType::Data,
            DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                ManifestContentType::Deletes
            }
        };
        let mut files = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != want {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("load manifest");
            for entry in manifest.entries() {
                if entry.is_alive() && entry.data_file().content_type() == content {
                    files.push(entry.data_file().clone());
                }
            }
        }
        files
    }

    /// PIN — fork #182 made `PartitionKey::new` fallible (`validate_partition_data`).
    /// The write path maps `iceberg::Error` through [`iceberg_err`]. Unpartitioned +
    /// empty struct is the merge/mod.rs unpartitioned-writer shape; an identity spec
    /// with an empty struct must refuse (the adapter must not swallow that Err).
    #[test]
    fn partition_key_new_is_fallible_and_maps_through_iceberg_err() {
        use iceberg::spec::{
            NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
            UnboundPartitionSpec,
        };

        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let schema_ref = std::sync::Arc::new(schema);
        let ok = PartitionKey::new(
            PartitionSpec::unpartition_spec(),
            std::sync::Arc::clone(&schema_ref),
            Struct::empty(),
        );
        assert!(
            ok.is_ok(),
            "unpartitioned + empty struct must construct: {ok:?}"
        );

        let partitioned = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id", Transform::Identity)
            .expect("add identity partition field")
            .build()
            .bind(std::sync::Arc::clone(&schema_ref))
            .expect("bind identity spec");
        let err = PartitionKey::new(partitioned, schema_ref, Struct::empty());
        let err = err.expect_err("identity spec + empty struct must fail validation");
        assert!(
            matches!(iceberg_err(err), DataFusionError::External(_)),
            "adapter must wrap the constructor Err as DataFusionError::External"
        );
    }
}
