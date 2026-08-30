use std::collections::{HashMap, HashSet};

use datafusion::error::{DataFusionError, Result};
use iceberg::delete_vector_container::{DvContainerClose, close_touched_dv_containers};
use iceberg::spec::{DataFile, FormatVersion};
use iceberg::table::Table;
use iceberg::transaction::RowDeltaAction;

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
) -> Result<PreparedDeletes> {
    match table.metadata().format_version() {
        FormatVersion::V2 => {
            let referenced = pairs
                .iter()
                .map(|(path, _)| path.as_ref().to_string())
                .collect();
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
            let plan = plan_deletion_vectors(table, pairs).await?;
            let abort_paths = plan
                .close
                .added
                .iter()
                .map(|(file, _)| file.file_path().to_string())
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
) -> Result<DvCommitPlan> {
    let mut new_positions: HashMap<String, Vec<u64>> = HashMap::new();
    for (path, position) in pairs {
        let position = u64::try_from(*position).map_err(|_| {
            DataFusionError::Internal(format!(
                "deletion-vector: negative row position {position} for data file `{path}`"
            ))
        })?;
        new_positions
            .entry(path.as_ref().to_string())
            .or_default()
            .push(position);
    }
    let close = close_touched_dv_containers(table, &new_positions)
        .await
        .map_err(iceberg_err)?;
    Ok(DvCommitPlan {
        referenced: close.referenced_data_files(),
        close,
    })
}

fn apply_close(mut action: RowDeltaAction, close: DvContainerClose) -> RowDeltaAction {
    for (file, sequence) in close.added {
        action = match sequence {
            Some(sequence) => action.add_delete_file_with_sequence_number(file, sequence),
            None => action.add_deletes([file]),
        };
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
    use futures::TryStreamExt;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
    use tempfile::TempDir;

    use super::super::{IsolationLevel, RowDeltaKind, RowDeltaPolicy, commit_row_delta_kind};
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

    async fn live_dv_by_referenced(
        table: &Table,
    ) -> HashMap<String, (String, DataFileFormat, Option<i64>)> {
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
                    (
                        data_file.file_path().to_string(),
                        data_file.file_format(),
                        entry.sequence_number(),
                    ),
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
        let sibling_seq = before.get(&sibling_referenced).expect("sibling entry").2;
        let old_paths: HashSet<String> = before.values().map(|(path, _, _)| path.clone()).collect();
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
        for (_, format, _) in after.values() {
            assert_eq!(*format, DataFileFormat::Puffin);
        }
        let after_sibling = after.get(&sibling_referenced).expect("sibling still live");
        assert_eq!(after_sibling.2, sibling_seq);
        for path in &old_paths {
            assert!(
                after.values().all(|(live, _, _)| live != path),
                "old container {path} must not stay live"
            );
        }
    }
}
