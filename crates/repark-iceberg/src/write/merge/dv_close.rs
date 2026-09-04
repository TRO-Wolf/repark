use std::collections::{HashMap, HashSet};

use datafusion::error::{DataFusionError, Result};
use iceberg::delete_vector_container::{
    DvContainerClose, close_touched_dv_containers_with_partitions,
};
use iceberg::spec::{DataFile, FormatVersion, ManifestList};
use iceberg::table::Table;
use iceberg::transaction::RowDeltaAction;

use super::KnownPartitions;
use super::abort;
use super::iceberg_err;
use crate::write::concurrency::WriteConcurrency;
use crate::write::position_delete::PositionDeletePair;

pub(super) struct PreparedDeletes {
    pub referenced: HashSet<String>,
    pub abort_paths: Vec<String>,
    pub arm_validate_deleted_files_on_delete: bool,
    kind: PreparedKind,
}

enum PreparedKind {
    PositionDeletes(Vec<DataFile>),
    DeletionVectors(DvContainerClose),
}

pub(super) async fn prepare_row_delta_deletes(
    table: &Table,
    pairs: &[PositionDeletePair],
    concurrency: WriteConcurrency,
    known_partitions: KnownPartitions,
    snapshot_id: Option<i64>,
) -> Result<PreparedDeletes> {
    match table.metadata().format_version() {
        FormatVersion::V2 => {
            let mut referenced: HashSet<String> = HashSet::new();
            for (path, _) in pairs {
                if !referenced.contains(path.as_ref()) {
                    referenced.insert(path.as_ref().to_string());
                }
            }
            let delete_files =
                crate::write::position_delete::write_position_deletes(table, pairs, concurrency)
                    .await?;
            let abort_paths = abort::written_file_paths(&delete_files);
            Ok(PreparedDeletes {
                referenced,
                abort_paths,
                arm_validate_deleted_files_on_delete: false,
                kind: PreparedKind::PositionDeletes(delete_files),
            })
        }
        FormatVersion::V3 => {
            if pairs.is_empty() {
                return Ok(PreparedDeletes {
                    referenced: HashSet::new(),
                    abort_paths: Vec::new(),
                    arm_validate_deleted_files_on_delete: true,
                    kind: PreparedKind::PositionDeletes(Vec::new()),
                });
            }
            let plan = plan_deletion_vectors(table, pairs, known_partitions, snapshot_id).await?;
            let abort_paths = plan
                .close
                .added
                .iter()
                .map(|file| file.file_path().to_string())
                .collect();
            Ok(PreparedDeletes {
                referenced: plan.referenced,
                abort_paths,
                arm_validate_deleted_files_on_delete: true,
                kind: PreparedKind::DeletionVectors(plan.close),
            })
        }
        FormatVersion::V1 => Err(DataFusionError::NotImplemented(
            "merge-on-read RowDelta for format version V1 is not implemented".to_string(),
        )),
    }
}

impl PreparedDeletes {
    pub(super) fn apply(self, action: RowDeltaAction) -> RowDeltaAction {
        match self.kind {
            PreparedKind::PositionDeletes(files) => action.add_deletes(files),
            PreparedKind::DeletionVectors(close) => apply_close(action, close),
        }
    }
}

struct DvCommitPlan {
    referenced: HashSet<String>,
    close: DvContainerClose,
}

async fn plan_deletion_vectors(
    table: &Table,
    pairs: &[PositionDeletePair],
    mut known_partitions: KnownPartitions,
    snapshot_id: Option<i64>,
) -> Result<DvCommitPlan> {
    let mut new_positions: HashMap<String, Vec<u64>> = HashMap::new();
    for (path, position) in pairs {
        let position = u64::try_from(*position).map_err(|_| {
            DataFusionError::Internal(format!(
                "deletion-vector: negative row position {position} for data file `{path}`"
            ))
        })?;
        match new_positions.get_mut(path.as_ref()) {
            Some(slot) => slot.push(position),
            None => {
                new_positions.insert(path.as_ref().to_string(), vec![position]);
            }
        }
    }
    known_partitions.retain(|path, _| new_positions.contains_key(path));
    let manifest_list = scanned_manifest_list(table, snapshot_id).await?;
    let close = close_touched_dv_containers_with_partitions(
        table,
        &new_positions,
        snapshot_id,
        &known_partitions,
        manifest_list.as_ref(),
    )
    .await
    .map_err(iceberg_err)?;
    Ok(DvCommitPlan {
        referenced: close.referenced_data_files(),
        close,
    })
}

async fn scanned_manifest_list(
    table: &Table,
    snapshot_id: Option<i64>,
) -> Result<Option<ManifestList>> {
    let metadata = table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => metadata.snapshot_by_id(id),
        None => metadata.current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map(Some)
        .map_err(iceberg_err)
}

fn apply_close(mut action: RowDeltaAction, close: DvContainerClose) -> RowDeltaAction {
    if !close.added.is_empty() {
        action = action.add_deletes(close.added);
    }
    if !close.removed.is_empty() {
        action = action.remove_deletes_many(close.removed);
    }
    action
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::io::ErrorKind;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;
    use futures::{StreamExt, TryStreamExt};
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileFormat, ManifestContentType, NestedField, PrimitiveType, Schema,
        Transform, Type, UnboundPartitionSpec,
    };
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use datafusion::physical_plan::streaming::PartitionStream;
    use datafusion::prelude::SessionContext;

    use super::super::{
        IsolationLevel, KnownPartitions, RowDeltaKind, RowDeltaPolicy, TargetScanStream,
        commit_row_delta_kind, drain_partition_sink, new_partition_sink, register_streaming_target,
        scratch_schema,
    };
    use super::*;
    use crate::write::concurrency::WriteConcurrency;

    const PART_DV_TABLE: &str = "/tmp/repark-v3e3-partdv/ns/v3part";
    static PART_DV_LOCK: Mutex<()> = Mutex::new(());

    struct DirLock {
        path: PathBuf,
    }

    impl DirLock {
        fn acquire(path: &str) -> Self {
            let path = PathBuf::from(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("fixture lock parent");
            }
            let started = Instant::now();
            loop {
                match fs::create_dir(&path) {
                    Ok(()) => return Self { path },
                    Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                        assert!(
                            started.elapsed() <= Duration::from_mins(2),
                            "fixture lock {}: held for 2 minutes (no steal)",
                            path.display()
                        );
                        std::thread::sleep(Duration::from_millis(25));
                    }
                    Err(err) => panic!("fixture lock {}: {err}", path.display()),
                }
            }
        }
    }

    impl Drop for DirLock {
        fn drop(&mut self) {
            let _ = fs::remove_dir(&self.path);
        }
    }

    struct SparkFixture {
        _thread: MutexGuard<'static, ()>,
        _cross_process: DirLock,
        metadata_file: String,
    }

    fn copy_dir_all(from: &Path, to: &Path) {
        fs::create_dir_all(to).expect("create dest");
        for entry in fs::read_dir(from).expect("read src") {
            let entry = entry.expect("dirent");
            let dest = to.join(entry.file_name());
            if entry.file_type().expect("ft").is_dir() {
                copy_dir_all(&entry.path(), &dest);
            } else {
                fs::copy(entry.path(), dest).expect("copy file");
            }
        }
    }

    fn materialize_part_dv() -> SparkFixture {
        let held = PART_DV_LOCK.lock().expect("part-dv fixture lock");
        let cross_process = DirLock::acquire(&format!("{PART_DV_TABLE}.lock"));
        let dest_path = PathBuf::from(PART_DV_TABLE);
        if dest_path.exists() {
            fs::remove_dir_all(&dest_path).expect("clear previous fixture");
        }
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../repark-spark/src/tests/fixtures/v3-spark-part-dv");
        copy_dir_all(&src, &dest_path);
        let metadata_file = dest_path.join("metadata").join("v3.metadata.json");
        assert!(
            metadata_file.is_file(),
            "Spark fixture must include Hadoop-named v3.metadata.json"
        );
        SparkFixture {
            _thread: held,
            _cross_process: cross_process,
            metadata_file: metadata_file.to_string_lossy().into_owned(),
        }
    }

    async fn memory_catalog(warehouse: &TempDir) -> std::sync::Arc<dyn Catalog> {
        let path = warehouse
            .path()
            .to_str()
            .expect("utf-8 warehouse path")
            .to_string();
        std::sync::Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(std::sync::Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
                )
                .await
                .expect("build memory catalog"),
        )
    }

    async fn live_ids(table: &Table) -> Vec<i32> {
        let scan = table.scan().select(["id"]).build().expect("scan ids");
        let batches: Vec<_> = scan
            .to_arrow()
            .await
            .expect("to_arrow")
            .try_collect()
            .await
            .expect("collect ids");
        let mut ids = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id is Int32");
            for index in 0..batch.num_rows() {
                if !column.is_null(index) {
                    ids.push(column.value(index));
                }
            }
        }
        ids.sort_unstable();
        ids
    }

    async fn pair_for_id(table: &Table, target_id: i32) -> PositionDeletePair {
        let scan = table
            .scan()
            .select(["id", "_file", "_pos"])
            .build()
            .expect("scan identity");
        let batches: Vec<_> = scan
            .to_arrow()
            .await
            .expect("to_arrow identity")
            .try_collect()
            .await
            .expect("collect identity");
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .expect("id")
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("id is Int32, got {:?}", batch.column_by_name("id")));
            let files_col = cast(
                batch.column_by_name("_file").expect("_file"),
                &DataType::Utf8,
            )
            .expect("cast _file to Utf8");
            let files = files_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("_file Utf8");
            let positions_col = cast(
                batch.column_by_name("_pos").expect("_pos"),
                &DataType::Int64,
            )
            .expect("cast _pos to Int64");
            let positions = positions_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("_pos Int64");
            for index in 0..batch.num_rows() {
                if !ids.is_null(index) && ids.value(index) == target_id {
                    return (
                        std::sync::Arc::from(files.value(index)),
                        positions.value(index),
                    );
                }
            }
        }
        panic!("id {target_id} is not a live row");
    }

    struct LiveDv {
        container: String,
        format: DataFileFormat,
        offset: Option<i64>,
        sequence: Option<i64>,
    }

    async fn live_dv_by_referenced(table: &Table) -> HashMap<String, LiveDv> {
        let mut out = HashMap::new();
        let metadata = table.metadata();
        let snapshot = metadata.current_snapshot().expect("current snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .expect("manifest list");
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("load delete manifest");
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::PositionDeletes {
                    continue;
                }
                let Some(referenced) = data_file.referenced_data_file() else {
                    continue;
                };
                out.insert(
                    referenced,
                    LiveDv {
                        container: data_file.file_path().to_string(),
                        format: data_file.file_format(),
                        offset: data_file.content_offset(),
                        sequence: entry.sequence_number(),
                    },
                );
            }
        }
        out
    }

    #[tokio::test]
    async fn shared_puffin_row_delta_keeps_the_untouched_sibling() {
        let fixture = materialize_part_dv();
        let warehouse = TempDir::new().expect("catalog warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace");
        let ident = TableIdent::new(namespace, "v3part".to_string());
        catalog
            .register_table(&ident, fixture.metadata_file.clone())
            .await
            .expect("register shared-puffin fixture");
        let table = catalog.load_table(&ident).await.expect("load");
        let before = live_dv_by_referenced(&table).await;
        assert_eq!(before.len(), 2, "fixture must start with two live DVs");
        let pair = pair_for_id(&table, 1).await;
        let sibling_referenced = before
            .keys()
            .find(|path| path.as_str() != pair.0.as_ref())
            .expect("sibling blob")
            .clone();
        let sibling_before = before.get(&sibling_referenced).expect("sibling entry");
        let sibling_seq = sibling_before.sequence;
        let sibling_container = sibling_before.container.clone();
        let sibling_offset = sibling_before.offset;
        let touched_container = before
            .get(pair.0.as_ref())
            .expect("touched entry")
            .container
            .clone();
        let pair_path = std::sync::Arc::clone(&pair.0);
        let pin = table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        commit_row_delta_kind(
            &catalog,
            &table,
            pin,
            vec![pair],
            Vec::new(),
            WriteConcurrency::new(1).expect("K=1"),
            RowDeltaPolicy {
                kind: RowDeltaKind::Delete,
                isolation: IsolationLevel::Serializable,
            },
        )
        .await
        .expect("shared-puffin seam commit");
        let table = catalog.load_table(&ident).await.expect("reload");
        assert_eq!(live_ids(&table).await, vec![3, 4, 6]);
        let after = live_dv_by_referenced(&table).await;
        assert_eq!(after.len(), 2, "untouched sibling must stay live");
        for live in after.values() {
            assert_eq!(live.format, DataFileFormat::Puffin);
        }
        let after_sibling = after.get(&sibling_referenced).expect("sibling still live");
        assert_eq!(after_sibling.sequence, sibling_seq);
        assert_eq!(
            (after_sibling.container.as_str(), after_sibling.offset),
            (sibling_container.as_str(), sibling_offset),
            "the untouched sibling entry keeps its container and content_offset"
        );
        let after_touched = after.get(pair_path.as_ref()).expect("touched still live");
        assert_ne!(
            after_touched.container, touched_container,
            "the touched blob moves into a newly written container"
        );
        assert_eq!(
            after
                .values()
                .map(|live| live.container.as_str())
                .collect::<HashSet<_>>()
                .len(),
            2,
            "Spark's layout after the second DELETE is two containers"
        );
    }

    async fn data_manifest_paths(table: &Table) -> Vec<String> {
        let metadata = table.metadata();
        let snapshot = metadata.current_snapshot().expect("current snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .expect("manifest list");
        manifest_list
            .entries()
            .iter()
            .filter(|manifest_file| manifest_file.content == ManifestContentType::Data)
            .map(|manifest_file| manifest_file.manifest_path.clone())
            .collect()
    }

    struct HiddenManifests {
        moved: Vec<(PathBuf, PathBuf)>,
    }

    impl HiddenManifests {
        fn hide(paths: &[String]) -> Self {
            assert!(!paths.is_empty(), "the fixture must have a data manifest");
            let mut moved = Vec::new();
            for path in paths {
                let from = PathBuf::from(path);
                let to = from.with_extension("avro.hidden");
                fs::rename(&from, &to).expect("hide data manifest");
                moved.push((from, to));
            }
            Self { moved }
        }
    }

    impl Drop for HiddenManifests {
        fn drop(&mut self) {
            for (from, to) in &self.moved {
                let _ = fs::rename(to, from);
            }
        }
    }

    #[tokio::test]
    async fn closing_a_covered_v3_delete_reads_the_data_manifest_for_sequence_numbers() {
        let fixture = materialize_part_dv();
        let warehouse = TempDir::new().expect("catalog warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace");
        let ident = TableIdent::new(namespace, "v3part".to_string());
        catalog
            .register_table(&ident, fixture.metadata_file.clone())
            .await
            .expect("register shared-puffin fixture");
        let table = catalog.load_table(&ident).await.expect("load");
        let pair = pair_for_id(&table, 1).await;
        let touched = pair.0.as_ref().to_string();
        let hidden = HiddenManifests::hide(&data_manifest_paths(&table).await);
        let refused = prepare_row_delta_deletes(
            &table,
            std::slice::from_ref(&pair),
            WriteConcurrency::new(1).expect("K=1"),
            KnownPartitions::new(),
            None,
        )
        .await;
        assert!(
            refused.is_err(),
            "the close walks the data manifests for sequence numbers, so hiding them must fail"
        );
        drop(hidden);
        let prepared = prepare_row_delta_deletes(
            &table,
            &[pair],
            WriteConcurrency::new(1).expect("K=1"),
            KnownPartitions::new(),
            None,
        )
        .await
        .expect("close with the data manifests present");
        assert_eq!(prepared.referenced.len(), 1);
        match prepared.kind {
            PreparedKind::DeletionVectors(close) => {
                assert_eq!(close.added.len(), 1);
                assert_eq!(close.removed.len(), 1);
                assert!(
                    close.data_sequence_numbers.contains_key(&touched),
                    "every touched path carries a data sequence number"
                );
            }
            PreparedKind::PositionDeletes(_) => panic!("v3 must plan deletion vectors"),
        }
    }

    async fn partitioned_v3_table(
        catalog: &std::sync::Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
    ) -> (Table, TableIdent) {
        partitioned_table(catalog, namespace, Some(FormatVersion::V3)).await
    }

    async fn partitioned_v2_table(
        catalog: &std::sync::Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
    ) -> (Table, TableIdent) {
        partitioned_table(catalog, namespace, None).await
    }

    async fn partitioned_table(
        catalog: &std::sync::Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        format_version: Option<FormatVersion>,
    ) -> (Table, TableIdent) {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("build schema");
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(2, "part", Transform::Identity)
            .expect("add identity partition field")
            .build();
        let creation = TableCreation::builder()
            .name("parts".to_string())
            .schema(schema)
            .partition_spec(spec)
            .properties(HashMap::new())
            .build();
        catalog
            .create_table(namespace, creation)
            .await
            .expect("create partitioned table");
        let ident = TableIdent::new(namespace.clone(), "parts".to_string());
        if let Some(format_version) = format_version {
            crate::write::format_version::set_properties_and_format_version(
                catalog.as_ref(),
                &ident,
                None,
                HashMap::new(),
                &[],
                Some(format_version),
            )
            .await
            .expect("set format version");
        }
        let table = catalog.load_table(&ident).await.expect("load fresh");
        if let Some(format_version) = format_version {
            assert_eq!(table.metadata().format_version(), format_version);
        }
        let arrow_schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", DataType::Int32, false),
            datafusion::arrow::datatypes::Field::new("part", DataType::Int32, false),
        ]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            arrow_schema,
            vec![
                std::sync::Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                std::sync::Arc::new(Int32Array::from(vec![0, 0, 1, 1])),
            ],
        )
        .expect("seed batch");
        let files = crate::write::append::write_partitioned_data_files(&table, vec![batch])
            .await
            .expect("write the seed data files");
        assert_eq!(files.len(), 2, "one data file per identity partition");
        super::super::commit(catalog, &table, None, Vec::new(), files)
            .await
            .expect("append the seed");
        let table = catalog.load_table(&ident).await.expect("reload seeded");
        (table, ident)
    }

    async fn scanned_partitions(table: &Table) -> KnownPartitions {
        let metadata = table.metadata();
        let snapshot = metadata.current_snapshot().expect("current snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .expect("manifest list");
        let mut known = KnownPartitions::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("data manifest");
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let file = entry.data_file();
                known.insert(
                    file.file_path().to_string(),
                    (file.partition_spec_id(), file.partition().clone()),
                );
            }
        }
        known
    }

    #[tokio::test]
    async fn a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest() {
        let warehouse = TempDir::new().expect("catalog warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace");
        let (table, ident) = partitioned_v3_table(&catalog, &namespace).await;
        let pair = pair_for_id(&table, 1).await;
        let known = scanned_partitions(&table).await;
        assert_eq!(known.len(), 2, "the seed writes one file per partition");
        assert!(
            known
                .values()
                .any(|(_, partition)| !partition.fields().is_empty()),
            "the fixture must be partitioned, or the pin proves nothing"
        );
        let hidden = HiddenManifests::hide(&data_manifest_paths(&table).await);
        let prepared = prepare_row_delta_deletes(
            &table,
            std::slice::from_ref(&pair),
            WriteConcurrency::new(1).expect("K=1"),
            known,
            None,
        )
        .await
        .expect("a complete partition map skips the data-manifest walk");
        match prepared.kind {
            PreparedKind::DeletionVectors(close) => {
                assert_eq!(close.added.len(), 1);
                assert!(
                    close.data_sequence_numbers.is_empty(),
                    "pure-DV complete known_partitions leaves data_sequence_numbers empty"
                );
            }
            PreparedKind::PositionDeletes(_) => panic!("v3 must plan deletion vectors"),
        }
        drop(hidden);
        let table = catalog.load_table(&ident).await.expect("reload");
        assert_eq!(live_ids(&table).await, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn a_legacy_delete_fills_data_sequence_numbers_even_with_a_complete_partition_map() {
        let warehouse = TempDir::new().expect("catalog warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace");
        let (table, ident) = partitioned_v2_table(&catalog, &namespace).await;
        let pair = pair_for_id(&table, 1).await;
        commit_row_delta_kind(
            &catalog,
            &table,
            table
                .metadata()
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            vec![pair],
            Vec::new(),
            WriteConcurrency::new(1).expect("K=1"),
            RowDeltaPolicy {
                kind: RowDeltaKind::Delete,
                isolation: IsolationLevel::Serializable,
            },
        )
        .await
        .expect("v2 parquet position delete");
        crate::write::format_version::set_properties_and_format_version(
            catalog.as_ref(),
            &ident,
            None,
            HashMap::new(),
            &[],
            Some(FormatVersion::V3),
        )
        .await
        .expect("upgrade to v3");
        let table = catalog.load_table(&ident).await.expect("reload upgraded");
        assert_eq!(table.metadata().format_version(), FormatVersion::V3);
        let pair = pair_for_id(&table, 2).await;
        let known = scanned_partitions(&table).await;
        let touched = pair.0.as_ref().to_string();
        let hidden = HiddenManifests::hide(&data_manifest_paths(&table).await);
        let refused = prepare_row_delta_deletes(
            &table,
            std::slice::from_ref(&pair),
            WriteConcurrency::new(1).expect("K=1"),
            known.clone(),
            None,
        )
        .await;
        assert!(
            refused.is_err(),
            "a live legacy delete still walks the data manifests"
        );
        drop(hidden);
        let prepared = prepare_row_delta_deletes(
            &table,
            &[pair],
            WriteConcurrency::new(1).expect("K=1"),
            known,
            None,
        )
        .await
        .expect("close with the data manifests present");
        match prepared.kind {
            PreparedKind::DeletionVectors(close) => {
                assert_eq!(close.added.len(), 1);
                assert!(
                    !close.legacy_deletes.is_empty(),
                    "the upgraded parquet delete is live"
                );
                assert!(
                    close.data_sequence_numbers.contains_key(&touched),
                    "legacy deletes force a total sequence-number map"
                );
            }
            PreparedKind::PositionDeletes(_) => panic!("v3 must plan deletion vectors"),
        }
    }

    async fn eight_manifest_puredv_table(catalog: &std::sync::Arc<dyn Catalog>) -> Table {
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace");
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("schema");
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(2, "part", Transform::Identity)
            .expect("identity partition field")
            .build();
        let creation = TableCreation::builder()
            .name("puredv".to_string())
            .schema(schema)
            .partition_spec(spec)
            .properties(HashMap::from([(
                "commit.manifest-merge.enabled".to_string(),
                "false".to_string(),
            )]))
            .build();
        catalog
            .create_table(&namespace, creation)
            .await
            .expect("create");
        let ident = TableIdent::new(namespace, "puredv".to_string());
        crate::write::format_version::set_properties_and_format_version(
            catalog.as_ref(),
            &ident,
            None,
            HashMap::new(),
            &[],
            Some(FormatVersion::V3),
        )
        .await
        .expect("upgrade to v3");
        let arrow_schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", DataType::Int32, false),
            datafusion::arrow::datatypes::Field::new("part", DataType::Int32, false),
        ]));
        for part in 0..8_i32 {
            let table = catalog.load_table(&ident).await.expect("load");
            let batch = datafusion::arrow::array::RecordBatch::try_new(
                std::sync::Arc::clone(&arrow_schema),
                vec![
                    std::sync::Arc::new(Int32Array::from(vec![part])),
                    std::sync::Arc::new(Int32Array::from(vec![part])),
                ],
            )
            .expect("seed batch");
            let files = crate::write::append::write_partitioned_data_files(&table, vec![batch])
                .await
                .expect("write");
            super::super::commit(catalog, &table, None, Vec::new(), files)
                .await
                .expect("append");
        }
        catalog.load_table(&ident).await.expect("reload")
    }

    async fn identity_pairs_for_id_zero(
        table: &Table,
    ) -> (Vec<PositionDeletePair>, KnownPartitions, Option<i64>) {
        let write_schema = std::sync::Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .expect("write schema"),
        );
        let scratch = scratch_schema(&write_schema);
        let snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        let partitions = new_partition_sink();
        let source: std::sync::Arc<dyn PartitionStream> = std::sync::Arc::new(
            TargetScanStream::new(
                table.clone(),
                snapshot_id,
                std::sync::Arc::clone(&scratch),
                &write_schema,
                None,
                Some(1),
                None,
            )
            .with_partition_sink(std::sync::Arc::clone(&partitions)),
        );
        let ctx = SessionContext::new();
        let target_name = register_streaming_target(&ctx, std::sync::Arc::clone(&scratch), source)
            .expect("register streaming target");
        let sql = format!("SELECT \"_file\", \"_pos\" FROM {target_name} AS t WHERE id = 0");
        let mut stream = ctx
            .sql(&sql)
            .await
            .expect("plan identity sql")
            .execute_stream()
            .await
            .expect("execute identity sql");
        let mut pairs: Vec<PositionDeletePair> = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("identity batch");
            let files_col = cast(
                batch.column_by_name("_file").expect("_file"),
                &DataType::Utf8,
            )
            .expect("cast _file");
            let files = files_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8");
            let positions_col = cast(
                batch.column_by_name("_pos").expect("_pos"),
                &DataType::Int64,
            )
            .expect("cast _pos");
            let positions = positions_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64");
            for index in 0..batch.num_rows() {
                pairs.push((
                    std::sync::Arc::from(files.value(index)),
                    positions.value(index),
                ));
            }
        }
        (pairs, drain_partition_sink(&partitions), snapshot_id)
    }

    #[tokio::test]
    async fn a_plain_identity_delete_closes_with_no_data_manifest() {
        let warehouse = TempDir::new().expect("catalog warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let table = eight_manifest_puredv_table(&catalog).await;
        let (pairs, mut known, snapshot_id) = identity_pairs_for_id_zero(&table).await;
        assert_eq!(pairs.len(), 1);
        let touched = pairs[0].0.as_ref().to_string();
        known.retain(|path, _| path == &touched);
        assert!(
            known.contains_key(&touched),
            "the production identity scan must record the touched path"
        );
        let hidden = HiddenManifests::hide(&data_manifest_paths(&table).await);
        let prepared = prepare_row_delta_deletes(
            &table,
            &pairs,
            WriteConcurrency::new(1).expect("K=1"),
            known,
            snapshot_id,
        )
        .await
        .expect("plain identity close skips the data-manifest walk");
        match prepared.kind {
            PreparedKind::DeletionVectors(close) => {
                assert!(
                    close.data_sequence_numbers.is_empty(),
                    "complete production map leaves data_sequence_numbers empty"
                );
            }
            PreparedKind::PositionDeletes(_) => panic!("v3 must plan deletion vectors"),
        }
        drop(hidden);
    }
}
