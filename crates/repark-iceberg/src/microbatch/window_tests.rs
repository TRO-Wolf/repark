use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema,
    Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::*;

fn id_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema")
}

async fn fixture_table(name: &str) -> (TempDir, Arc<dyn Catalog>, TableIdent) {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string());
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name(name.to_string())
                .schema(id_schema())
                .build(),
        )
        .await
        .expect("create table");
    (warehouse, catalog, ident)
}

fn synthetic_file(path: &str, rows: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(rows)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("synthetic file")
}

async fn commit_append(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    files: Vec<DataFile>,
) -> i64 {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(files);
    let tx = action.apply(tx).expect("apply append");
    let committed = tx.commit(catalog.as_ref()).await.expect("commit append");
    committed
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id()
}

async fn commit_overwrite(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    adds: Vec<DataFile>,
    removes: Vec<DataFile>,
) -> i64 {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx
        .overwrite_files()
        .add_files(adds)
        .delete_data_files(removes);
    let tx = action.apply(tx).expect("apply overwrite");
    let committed = tx.commit(catalog.as_ref()).await.expect("commit overwrite");
    committed
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id()
}

async fn commit_delete(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    removes: Vec<DataFile>,
) -> i64 {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.overwrite_files().delete_data_files(removes);
    let tx = action.apply(tx).expect("apply delete");
    let committed = tx.commit(catalog.as_ref()).await.expect("commit delete");
    committed
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id()
}

async fn commit_replace(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    delete: DataFile,
    add: DataFile,
) -> i64 {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.rewrite_files(vec![delete], vec![add]);
    let tx = action.apply(tx).expect("apply replace");
    let committed = tx.commit(catalog.as_ref()).await.expect("commit replace");
    committed
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id()
}

async fn load(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Table {
    catalog.load_table(ident).await.expect("load table")
}

fn uncapped() -> ReadCaps {
    ReadCaps {
        max_files: None,
        max_rows: None,
    }
}

fn capped(files: Option<usize>, rows: Option<u64>) -> ReadCaps {
    ReadCaps {
        max_files: files.map(|value| NonZeroUsize::new(value).expect("nonzero files")),
        max_rows: rows.map(|value| NonZeroU64::new(value).expect("nonzero rows")),
    }
}

fn input_offset(table: &Table, snapshot: i64, position: u64) -> InputOffset {
    InputOffset {
        table: TableUuid::of(table),
        table_name: table.identifier().to_string(),
        snapshot: SnapshotId::new(snapshot),
        position: FilePosition::new(position),
    }
}

fn operation_of(table: &Table, snapshot: i64) -> Operation {
    table
        .metadata()
        .snapshot_by_id(snapshot)
        .expect("snapshot")
        .summary()
        .operation
        .clone()
}

fn snapshot_timestamp(table: &Table, snapshot: i64) -> i64 {
    table
        .metadata()
        .snapshot_by_id(snapshot)
        .expect("snapshot")
        .timestamp_ms()
}

#[tokio::test]
async fn earliest_offset_points_at_oldest_ancestor() {
    let (_warehouse, catalog, ident) = fixture_table("earliest").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table, uncapped());
    let offset = planner
        .initial_offset(&StartPosition::Earliest)
        .await
        .expect("initial")
        .expect("some");
    assert_eq!(offset.snapshot, SnapshotId::new(first));
    assert_eq!(offset.position, FilePosition::new(0));
}

#[tokio::test]
async fn initial_offset_on_empty_table_reads_none_on_every_start() {
    let (_warehouse, catalog, ident) = fixture_table("empty").await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table, uncapped());
    for start in [
        StartPosition::Earliest,
        StartPosition::FromTimestamp { millis: 0 },
        StartPosition::AfterSnapshot(SnapshotId::new(1)),
    ] {
        let offset = planner.initial_offset(&start).await.expect("initial");
        assert!(offset.is_none());
    }
}

#[tokio::test]
async fn after_snapshot_marks_named_snapshot_consumed() {
    let (_warehouse, catalog, ident) = fixture_table("after").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    let second = commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let offset = planner
        .initial_offset(&StartPosition::AfterSnapshot(SnapshotId::new(first)))
        .await
        .expect("initial")
        .expect("some");
    assert_eq!(offset.snapshot, SnapshotId::new(first));
    assert_eq!(offset.position, FilePosition::new(1));
    let plan = planner
        .next_window(&offset, WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some");
    assert_eq!(plan.end.snapshot, SnapshotId::new(second));
    assert_eq!(plan.files.len(), 1);
}

#[tokio::test]
async fn after_snapshot_with_unknown_id_refuses_naming_oldest() {
    let (_warehouse, catalog, ident) = fixture_table("after-unknown").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table, uncapped());
    let error = planner
        .initial_offset(&StartPosition::AfterSnapshot(SnapshotId::new(999_999_999)))
        .await
        .expect_err("must refuse");
    match error {
        MicroBatchError::SourceSnapshotExpired {
            snapshot, oldest, ..
        } => {
            assert_eq!(snapshot, SnapshotId::new(999_999_999));
            assert_eq!(oldest, SnapshotId::new(first));
        }
        other => panic!("expected SourceSnapshotExpired, got {other:?}"),
    }
}

#[tokio::test]
async fn from_timestamp_matches_second_commit_inclusively() {
    let (_warehouse, catalog, ident) = fixture_table("stamps").await;
    commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    std::thread::sleep(std::time::Duration::from_millis(10));
    let second = commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let stamp = snapshot_timestamp(&table, second);
    let planner = WindowPlanner::new(table, uncapped());
    let offset = planner
        .initial_offset(&StartPosition::FromTimestamp { millis: stamp })
        .await
        .expect("initial")
        .expect("some");
    assert_eq!(offset.snapshot, SnapshotId::new(second));
    assert_eq!(offset.position, FilePosition::new(0));
}

#[tokio::test]
async fn from_timestamp_zero_starts_at_oldest() {
    let (_warehouse, catalog, ident) = fixture_table("stamp-zero").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table, uncapped());
    let offset = planner
        .initial_offset(&StartPosition::FromTimestamp { millis: 0 })
        .await
        .expect("initial")
        .expect("some");
    assert_eq!(offset.snapshot, SnapshotId::new(first));
    assert_eq!(offset.position, FilePosition::new(0));
}

#[tokio::test]
async fn from_timestamp_past_head_reads_none() {
    let (_warehouse, catalog, ident) = fixture_table("stamp-past").await;
    commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let offset = planner
        .initial_offset(&StartPosition::FromTimestamp { millis: i64::MAX })
        .await
        .expect("initial");
    assert!(offset.is_none());
}

#[tokio::test]
async fn window_streams_appends_oldest_first() {
    let (_warehouse, catalog, ident) = fixture_table("order").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    let second = commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let from = input_offset(&table, first, 0);
    let plan = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some");
    assert_eq!(plan.start, from);
    assert_eq!(plan.end.snapshot, SnapshotId::new(second));
    assert_eq!(plan.end.position, FilePosition::new(1));
    assert_eq!(plan.num_input_rows, 5);
    let snapshots: Vec<i64> = plan.files.iter().map(|file| file.snapshot.get()).collect();
    assert_eq!(snapshots, vec![first, second]);
    let positions: Vec<u64> = plan.files.iter().map(|file| file.position.get()).collect();
    assert_eq!(positions, vec![0, 0]);
}

#[tokio::test]
async fn window_returns_none_when_from_is_consumed_head() {
    let (_warehouse, catalog, ident) = fixture_table("drained").await;
    commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    let second = commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let from = input_offset(&table, second, 1);
    let plan = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("window");
    assert!(plan.is_none());
}

#[tokio::test]
async fn position_resumes_mid_snapshot() {
    let (_warehouse, catalog, ident) = fixture_table("resume").await;
    let first = commit_append(
        &catalog,
        &ident,
        vec![
            synthetic_file("s1-a.parquet", 1),
            synthetic_file("s1-b.parquet", 1),
            synthetic_file("s1-c.parquet", 1),
        ],
    )
    .await;
    let second = commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 4)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let from = input_offset(&table, first, 1);
    let plan = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some");
    let snapshots: Vec<i64> = plan.files.iter().map(|file| file.snapshot.get()).collect();
    assert_eq!(snapshots, vec![first, first, second]);
    let positions: Vec<u64> = plan.files.iter().map(|file| file.position.get()).collect();
    assert_eq!(positions, vec![1, 2, 0]);
    assert_eq!(plan.end.snapshot, SnapshotId::new(second));
    assert_eq!(plan.end.position, FilePosition::new(1));
    assert_eq!(plan.num_input_rows, 6);
}

#[tokio::test]
async fn max_files_splits_snapshot_into_single_file_windows() {
    let (_warehouse, catalog, ident) = fixture_table("max-files").await;
    let first = commit_append(
        &catalog,
        &ident,
        vec![
            synthetic_file("s1-c.parquet", 1),
            synthetic_file("s1-a.parquet", 1),
            synthetic_file("s1-b.parquet", 1),
        ],
    )
    .await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), capped(Some(1), None));
    let mut from = input_offset(&table, first, 0);
    let mut paths = Vec::new();
    for position in 1..=3u64 {
        let plan = planner
            .next_window(&from, WindowLimit::Capped)
            .await
            .expect("window")
            .expect("some");
        assert_eq!(plan.files.len(), 1);
        assert_eq!(plan.end.snapshot, SnapshotId::new(first));
        assert_eq!(plan.end.position, FilePosition::new(position));
        paths.push(plan.files[0].task.data_file_path().to_string());
        from = plan.end;
    }
    let mut sorted = paths.clone();
    sorted.sort();
    assert_eq!(paths, sorted);
    let drained = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("window");
    assert!(drained.is_none());
}

#[tokio::test]
async fn max_rows_keeps_first_file_whole() {
    let (_warehouse, catalog, ident) = fixture_table("max-rows").await;
    let first = commit_append(
        &catalog,
        &ident,
        vec![
            synthetic_file("s1-a.parquet", 3),
            synthetic_file("s1-b.parquet", 3),
        ],
    )
    .await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), capped(None, Some(1)));
    let mut from = input_offset(&table, first, 0);
    for position in 1..=2u64 {
        let plan = planner
            .next_window(&from, WindowLimit::Capped)
            .await
            .expect("window")
            .expect("some");
        assert_eq!(plan.files.len(), 1);
        assert_eq!(plan.num_input_rows, 3);
        assert_eq!(plan.end.position, FilePosition::new(position));
        from = plan.end;
    }
    let drained = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("window");
    assert!(drained.is_none());
}

#[tokio::test]
async fn unbounded_limit_ignores_caps() {
    let (_warehouse, catalog, ident) = fixture_table("unbounded").await;
    let first = commit_append(
        &catalog,
        &ident,
        vec![
            synthetic_file("s1-a.parquet", 3),
            synthetic_file("s1-b.parquet", 3),
        ],
    )
    .await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), capped(Some(1), Some(1)));
    let from = input_offset(&table, first, 0);
    let plan = planner
        .next_window(&from, WindowLimit::Unbounded)
        .await
        .expect("window")
        .expect("some");
    assert_eq!(plan.files.len(), 2);
    assert_eq!(plan.num_input_rows, 6);
    assert_eq!(plan.end.position, FilePosition::new(2));
}

#[tokio::test]
async fn overwrite_inside_window_refuses() {
    let (_warehouse, catalog, ident) = fixture_table("overwrite").await;
    let first_file = synthetic_file("s1-a.parquet", 2);
    let first = commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    let second = commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 2)]).await;
    let third = commit_overwrite(
        &catalog,
        &ident,
        vec![synthetic_file("s3-a.parquet", 2)],
        vec![first_file],
    )
    .await;
    let fourth = commit_append(&catalog, &ident, vec![synthetic_file("s4-a.parquet", 2)]).await;
    let table = load(&catalog, &ident).await;
    assert_eq!(operation_of(&table, third), Operation::Overwrite);
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let from = input_offset(&table, first, 0);
    let delivered = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("appends before the overwrite")
        .expect("some");
    let snapshots: Vec<i64> = delivered
        .files
        .iter()
        .map(|file| file.snapshot.get())
        .collect();
    assert_eq!(snapshots, vec![first, second]);
    assert_eq!(delivered.end, input_offset(&table, second, 1));
    let error = planner
        .next_window(&delivered.end, WindowLimit::Capped)
        .await
        .expect_err("must refuse");
    match &error {
        MicroBatchError::NonAppendSnapshot {
            snapshot,
            operation,
            from: error_from,
            to,
            ..
        } => {
            assert_eq!(*snapshot, SnapshotId::new(third));
            assert_eq!(*operation, Operation::Overwrite);
            assert_eq!(*error_from, Some(SnapshotId::new(second)));
            assert_eq!(*to, SnapshotId::new(fourth));
        }
        other => panic!("expected NonAppendSnapshot, got {other:?}"),
    }
    assert!(
        error
            .to_string()
            .starts_with(&format!("Cannot process overwrite snapshot: {third}"))
    );
    let past = planner
        .initial_offset(&StartPosition::AfterSnapshot(SnapshotId::new(third)))
        .await
        .expect("after the overwrite")
        .expect("some");
    assert_eq!(past, input_offset(&table, third, 1));
    let plan = planner
        .next_window(&past, WindowLimit::Capped)
        .await
        .expect("past")
        .expect("some");
    assert_eq!(plan.end.snapshot, SnapshotId::new(fourth));
    assert_eq!(plan.files.len(), 1);
}

#[tokio::test]
async fn delete_inside_window_refuses() {
    let (_warehouse, catalog, ident) = fixture_table("delete").await;
    let first_file = synthetic_file("s1-a.parquet", 2);
    let first = commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    let second = commit_delete(&catalog, &ident, vec![first_file]).await;
    let table = load(&catalog, &ident).await;
    assert_eq!(operation_of(&table, second), Operation::Delete);
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let from = input_offset(&table, first, 0);
    let delivered = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("the append before the delete")
        .expect("some");
    assert_eq!(delivered.end, input_offset(&table, first, 1));
    let error = planner
        .next_window(&delivered.end, WindowLimit::Capped)
        .await
        .expect_err("must refuse");
    match &error {
        MicroBatchError::NonAppendSnapshot {
            snapshot,
            operation,
            from: error_from,
            ..
        } => {
            assert_eq!(*snapshot, SnapshotId::new(second));
            assert_eq!(*operation, Operation::Delete);
            assert_eq!(*error_from, Some(SnapshotId::new(first)));
        }
        other => panic!("expected NonAppendSnapshot, got {other:?}"),
    }
    assert!(
        error
            .to_string()
            .starts_with(&format!("Cannot process delete snapshot: {second}"))
    );
}

#[tokio::test]
async fn replace_inside_window_skipped_silently() {
    let (_warehouse, catalog, ident) = fixture_table("replace").await;
    let first_file = synthetic_file("s1-a.parquet", 2);
    let first = commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    let second = commit_replace(
        &catalog,
        &ident,
        first_file,
        synthetic_file("s2-b.parquet", 2),
    )
    .await;
    let third = commit_append(&catalog, &ident, vec![synthetic_file("s3-c.parquet", 4)]).await;
    let table = load(&catalog, &ident).await;
    assert_eq!(operation_of(&table, second), Operation::Replace);
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let from = input_offset(&table, first, 0);
    let plan = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some");
    let snapshots: Vec<i64> = plan.files.iter().map(|file| file.snapshot.get()).collect();
    assert_eq!(snapshots, vec![first, third]);
    assert_eq!(plan.end.snapshot, SnapshotId::new(third));
    assert_eq!(plan.num_input_rows, 6);
}

#[tokio::test]
async fn unknown_from_snapshot_refuses_naming_oldest() {
    let (_warehouse, catalog, ident) = fixture_table("expired").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    commit_append(&catalog, &ident, vec![synthetic_file("s2-a.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let from = input_offset(&table, 999_999_999, 0);
    let error = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect_err("must refuse");
    match error {
        MicroBatchError::SourceSnapshotExpired {
            snapshot, oldest, ..
        } => {
            assert_eq!(snapshot, SnapshotId::new(999_999_999));
            assert_eq!(oldest, SnapshotId::new(first));
        }
        other => panic!("expected SourceSnapshotExpired, got {other:?}"),
    }
}

#[tokio::test]
async fn replaced_table_refuses_before_any_scan() {
    let (_warehouse, catalog, ident) = fixture_table("alpha").await;
    commit_append(&catalog, &ident, vec![synthetic_file("s1-a.parquet", 2)]).await;
    let table = load(&catalog, &ident).await;
    let (_other_warehouse, other_catalog, other_ident) = fixture_table("beta").await;
    commit_append(
        &other_catalog,
        &other_ident,
        vec![synthetic_file("s1-a.parquet", 2)],
    )
    .await;
    let other_table = load(&other_catalog, &other_ident).await;
    let planner = WindowPlanner::new(other_table.clone(), uncapped());
    let from = input_offset(&table, 1, 0);
    let error = planner
        .next_window(&from, WindowLimit::Capped)
        .await
        .expect_err("must refuse");
    match error {
        MicroBatchError::SourceReplaced {
            recorded, current, ..
        } => {
            assert_eq!(recorded, TableUuid::of(&table));
            assert_eq!(current, TableUuid::of(&other_table));
        }
        other => panic!("expected SourceReplaced, got {other:?}"),
    }
}

mod window_fold_pins;
