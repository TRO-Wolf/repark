use super::window_fold_pins::{expect_non_append, pause};
use super::*;

async fn appends_then(
    name: &str,
    non_append: Operation,
) -> (TempDir, Arc<dyn Catalog>, TableIdent, [i64; 3]) {
    let (warehouse, catalog, ident) = fixture_table(name).await;
    let first_file = synthetic_file("a.parquet", 1);
    let first = commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    let second = commit_append(&catalog, &ident, vec![synthetic_file("b.parquet", 1)]).await;
    let third = match non_append {
        Operation::Delete => commit_delete(&catalog, &ident, vec![first_file]).await,
        _ => {
            commit_overwrite(
                &catalog,
                &ident,
                vec![synthetic_file("x.parquet", 2)],
                vec![first_file],
            )
            .await
        }
    };
    (warehouse, catalog, ident, [first, second, third])
}

#[tokio::test]
async fn unbounded_walk_refuses_the_first_non_append_before_any_file() {
    for operation in [Operation::Overwrite, Operation::Delete] {
        let (_warehouse, catalog, ident, [first, _, third]) =
            appends_then("r17-unbounded", operation.clone()).await;
        let table = load(&catalog, &ident).await;
        assert_eq!(operation_of(&table, third), operation);
        for caps in [uncapped(), capped(Some(1), None)] {
            let planner = WindowPlanner::new(table.clone(), caps);
            let error = planner
                .next_window(&input_offset(&table, first, 0), WindowLimit::Unbounded)
                .await
                .expect_err("an AvailableNow target walk refuses before batch 0");
            expect_non_append(&error, third, &operation, Some(first), third);
        }
    }
}

#[tokio::test]
async fn capped_walk_delivers_both_appends_then_refuses_the_non_append() {
    for operation in [Operation::Overwrite, Operation::Delete] {
        let (_warehouse, catalog, ident, [first, second, third]) =
            appends_then("r17-capped", operation.clone()).await;
        let table = load(&catalog, &ident).await;
        let planner = WindowPlanner::new(table.clone(), capped(Some(1), None));
        let mut cursor = input_offset(&table, first, 0);
        let mut ends = Vec::new();
        let error = loop {
            match planner.next_window(&cursor, WindowLimit::Capped).await {
                Ok(Some(plan)) => {
                    cursor = plan.end.clone();
                    ends.push(plan.end);
                }
                Ok(None) => panic!("the walk must reach the {operation:?}"),
                Err(error) => break error,
            }
        };
        assert_eq!(
            ends,
            vec![
                input_offset(&table, first, 1),
                input_offset(&table, second, 1)
            ]
        );
        expect_non_append(&error, third, &operation, Some(second), third);
    }
}

fn expect_out_of_range(error: &MicroBatchError, snapshot: i64, position: u64, files: u64) {
    match error {
        MicroBatchError::OffsetPositionOutOfRange {
            snapshot: refused,
            position: refused_position,
            files: refused_files,
            ..
        } => {
            assert_eq!(*refused, SnapshotId::new(snapshot));
            assert_eq!(*refused_position, FilePosition::new(position));
            assert_eq!(*refused_files, files);
        }
        other => panic!("expected OffsetPositionOutOfRange, got {other:?}"),
    }
}

#[tokio::test]
async fn a_replace_start_past_its_added_files_refuses() {
    let (_warehouse, catalog, ident) = fixture_table("replace-range").await;
    let first_file = synthetic_file("a.parquet", 1);
    commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    let replace = commit_replace(
        &catalog,
        &ident,
        first_file,
        synthetic_file("a2.parquet", 1),
    )
    .await;
    let head = commit_append(&catalog, &ident, vec![synthetic_file("c.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    assert_eq!(operation_of(&table, replace), Operation::Replace);
    let planner = WindowPlanner::new(table.clone(), uncapped());
    for limit in [WindowLimit::Capped, WindowLimit::Unbounded] {
        for position in [2, 99] {
            let error = planner
                .next_window(&input_offset(&table, replace, position), limit)
                .await
                .expect_err("a position past the replace's one added file must refuse");
            expect_out_of_range(&error, replace, position, 1);
        }
        let plan = planner
            .next_window(&input_offset(&table, replace, 1), limit)
            .await
            .expect("a position equal to the count resumes")
            .expect("some");
        assert_eq!(plan.end, input_offset(&table, head, 1));
    }
}

#[tokio::test]
async fn an_overwrite_start_past_its_added_files_refuses() {
    let (_warehouse, catalog, ident) = fixture_table("overwrite-range").await;
    let first_file = synthetic_file("a.parquet", 1);
    commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    let overwrite = commit_overwrite(
        &catalog,
        &ident,
        vec![
            synthetic_file("x.parquet", 1),
            synthetic_file("y.parquet", 1),
        ],
        vec![first_file],
    )
    .await;
    let head = commit_append(&catalog, &ident, vec![synthetic_file("d.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    for limit in [WindowLimit::Capped, WindowLimit::Unbounded] {
        for position in [3, 99] {
            let error = planner
                .next_window(&input_offset(&table, overwrite, position), limit)
                .await
                .expect_err("a position past the overwrite's two added files must refuse");
            expect_out_of_range(&error, overwrite, position, 2);
        }
        let plan = planner
            .next_window(&input_offset(&table, overwrite, 2), limit)
            .await
            .expect("a position equal to the count resumes")
            .expect("some");
        assert_eq!(plan.end, input_offset(&table, head, 1));
    }
}

#[tokio::test]
async fn a_delete_as_head_refuses_at_the_initial_offset() {
    let (_warehouse, catalog, ident) = fixture_table("delete-head").await;
    let first_file = synthetic_file("a.parquet", 2);
    commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    pause();
    let delete = commit_delete(&catalog, &ident, vec![first_file]).await;
    let table = load(&catalog, &ident).await;
    assert_eq!(operation_of(&table, delete), Operation::Delete);
    assert_eq!(
        table
            .metadata()
            .current_snapshot()
            .expect("head")
            .snapshot_id(),
        delete
    );
    let start = StartPosition::FromTimestamp {
        millis: snapshot_timestamp(&table, delete),
    };
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let error = planner
        .initial_offset(&start)
        .await
        .expect_err("a delete as the head with nothing after it must refuse");
    expect_non_append(&error, delete, &Operation::Delete, None, delete);
    assert!(
        error
            .to_string()
            .starts_with(&format!("Cannot process delete snapshot: {delete}"))
    );
    let after = planner
        .initial_offset(&StartPosition::AfterSnapshot(SnapshotId::new(delete)))
        .await
        .expect("after")
        .expect("some");
    assert_eq!(after, input_offset(&table, delete, 0));
    assert!(
        planner
            .next_window(&after, WindowLimit::Capped)
            .await
            .expect("nothing follows the delete")
            .is_none()
    );
    let head = commit_append(&catalog, &ident, vec![synthetic_file("b.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let error = planner
        .initial_offset(&start)
        .await
        .expect_err("data after the delete keeps the refusal");
    expect_non_append(&error, delete, &Operation::Delete, None, head);
    let plan = planner
        .next_window(&after, WindowLimit::Capped)
        .await
        .expect("resume past the delete")
        .expect("some");
    assert_eq!(plan.end, input_offset(&table, head, 1));
}

fn expect_not_in_lineage(error: &MicroBatchError, snapshot: i64, head: i64) {
    match error {
        MicroBatchError::SnapshotNotInLineage {
            snapshot: refused,
            head: refused_head,
            ..
        } => {
            assert_eq!(*refused, SnapshotId::new(snapshot));
            assert_eq!(*refused_head, SnapshotId::new(head));
        }
        other => panic!("expected SnapshotNotInLineage, got {other:?}"),
    }
    let text = error.to_string();
    assert!(
        text.starts_with(&format!(
            "Cannot find snapshot after {snapshot}: not an ancestor of table's current snapshot {head}"
        )),
        "{text}"
    );
    assert!(!text.contains("expired"), "{text}");
}

#[tokio::test]
async fn a_snapshot_rolled_out_of_the_lineage_refuses_as_not_an_ancestor() {
    let (_warehouse, catalog, ident) = fixture_table("rolled-back").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("a.parquet", 1)]).await;
    let orphan = commit_append(&catalog, &ident, vec![synthetic_file("b.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().rollback_to(first);
    let tx = action.apply(tx).expect("apply rollback");
    tx.commit(catalog.as_ref()).await.expect("commit rollback");
    let head = commit_append(&catalog, &ident, vec![synthetic_file("c.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    assert!(table.metadata().snapshot_by_id(orphan).is_some());
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let error = planner
        .initial_offset(&StartPosition::AfterSnapshot(SnapshotId::new(orphan)))
        .await
        .expect_err("a start after a snapshot outside the lineage must refuse");
    expect_not_in_lineage(&error, orphan, head);
    for position in [0, 1] {
        for limit in [WindowLimit::Capped, WindowLimit::Unbounded] {
            let error = planner
                .next_window(&input_offset(&table, orphan, position), limit)
                .await
                .expect_err("a resume from a snapshot outside the lineage must refuse");
            expect_not_in_lineage(&error, orphan, head);
        }
    }
    let plan = planner
        .next_window(&input_offset(&table, first, 1), WindowLimit::Capped)
        .await
        .expect("an ancestor still resumes")
        .expect("some");
    assert_eq!(plan.end, input_offset(&table, head, 1));
}

fn with_skewed_snapshots(table: &Table, offsets: &[(i64, i64)]) -> Table {
    let base = table
        .metadata()
        .current_snapshot()
        .expect("a first snapshot");
    let start = base.timestamp_ms();
    let mut parent = base.snapshot_id();
    let mut sequence = base.sequence_number();
    let mut builder =
        iceberg::spec::TableMetadataBuilder::new_from_metadata(table.metadata().clone(), None);
    for &(snapshot, offset) in offsets {
        sequence += 1;
        let added = iceberg::spec::Snapshot::builder()
            .with_snapshot_id(snapshot)
            .with_parent_snapshot_id(Some(parent))
            .with_sequence_number(sequence)
            .with_timestamp_ms(start + offset)
            .with_manifest_list(format!(
                "{}/skew-{snapshot}.avro",
                table.metadata().location()
            ))
            .with_summary(iceberg::spec::Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build();
        builder = builder
            .add_snapshot(added)
            .expect("add a skewed snapshot")
            .set_ref(
                iceberg::spec::MAIN_BRANCH,
                iceberg::spec::SnapshotReference::new(
                    snapshot,
                    iceberg::spec::SnapshotRetention::branch(None, None, None),
                ),
            )
            .expect("move main");
        parent = snapshot;
    }
    let metadata = builder.build().expect("skewed metadata").metadata;
    Table::builder()
        .metadata(metadata)
        .identifier(table.identifier().clone())
        .file_io(table.file_io().clone())
        .metadata_location(table.metadata_location().expect("location").to_string())
        .build()
        .expect("skewed table")
}

#[tokio::test]
async fn from_timestamp_walks_back_from_the_head_as_oldest_ancestor_after() {
    let (_warehouse, catalog, ident) = fixture_table("skewed").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("a.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let start = snapshot_timestamp(&table, first);
    let skewed = with_skewed_snapshots(&table, &[(902, 50_000), (903, 20_000), (904, 60_000)]);
    let stamps: Vec<i64> = [first, 902, 903, 904]
        .iter()
        .map(|snapshot| snapshot_timestamp(&skewed, *snapshot) - start)
        .collect();
    assert_eq!(stamps, vec![0, 50_000, 20_000, 60_000]);
    let planner = WindowPlanner::new(skewed.clone(), uncapped());
    for (target, expected) in [
        (30_000, 904),
        (60_000, 904),
        (20_000, 903),
        (10_000, 902),
        (0, first),
        (-1, first),
    ] {
        let offset = planner
            .initial_offset(&StartPosition::FromTimestamp {
                millis: start + target,
            })
            .await
            .expect("initial")
            .expect("the head is at or after T");
        assert_eq!(
            offset,
            input_offset(&skewed, expected, 0),
            "T = start + {target}"
        );
    }
    assert!(
        planner
            .initial_offset(&StartPosition::FromTimestamp {
                millis: start + 60_001,
            })
            .await
            .expect("idle")
            .is_none()
    );
    let below_head = with_skewed_snapshots(&table, &[(902, 50_000), (903, 20_000)]);
    let planner = WindowPlanner::new(below_head, uncapped());
    assert!(
        planner
            .initial_offset(&StartPosition::FromTimestamp {
                millis: start + 30_000,
            })
            .await
            .expect("a head below T reads no start")
            .is_none()
    );
}
