use super::*;

pub(super) fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(20));
}

pub(super) fn expect_non_append(
    error: &MicroBatchError,
    snapshot: i64,
    operation: &Operation,
    from: Option<i64>,
    to: i64,
) {
    match error {
        MicroBatchError::NonAppendSnapshot {
            snapshot: refused,
            operation: refused_operation,
            from: refused_from,
            to: refused_to,
            ..
        } => {
            assert_eq!(*refused, SnapshotId::new(snapshot));
            assert_eq!(refused_operation, operation);
            assert_eq!(*refused_from, from.map(SnapshotId::new));
            assert_eq!(*refused_to, SnapshotId::new(to));
        }
        other => panic!("expected NonAppendSnapshot, got {other:?}"),
    }
}

pub(super) async fn drain(planner: &WindowPlanner, from: InputOffset) -> Vec<WindowPlan> {
    let mut plans = Vec::new();
    let mut cursor = from;
    while let Some(plan) = planner
        .next_window(&cursor, WindowLimit::Capped)
        .await
        .expect("window")
    {
        cursor = plan.end.clone();
        plans.push(plan);
    }
    plans
}

pub(super) fn shape(plans: &[WindowPlan]) -> Vec<(u64, i64, u64)> {
    plans
        .iter()
        .map(|plan| {
            (
                plan.num_input_rows,
                plan.end.snapshot.get(),
                plan.end.position.get(),
            )
        })
        .collect()
}

#[tokio::test]
async fn earliest_on_first_snapshot_overwrite_refuses_then_after_snapshot_resumes() {
    let (_warehouse, catalog, ident) = fixture_table("first-overwrite").await;
    let table = load(&catalog, &ident).await;
    let tx = Transaction::new(&table);
    let action = tx
        .replace_partitions()
        .add_files(vec![synthetic_file("first.parquet", 4)]);
    let tx = action.apply(tx).expect("apply replace partitions");
    let first = tx
        .commit(catalog.as_ref())
        .await
        .expect("commit replace partitions")
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id();
    let second = commit_append(&catalog, &ident, vec![synthetic_file("second.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    assert_eq!(operation_of(&table, first), Operation::Overwrite);
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let start = planner
        .initial_offset(&StartPosition::Earliest)
        .await
        .expect("initial")
        .expect("some");
    assert_eq!(start, input_offset(&table, first, 0));
    let error = planner
        .next_window(&start, WindowLimit::Capped)
        .await
        .expect_err("must refuse");
    expect_non_append(&error, first, &Operation::Overwrite, Some(first), second);
    assert!(
        error
            .to_string()
            .starts_with(&format!("Cannot process overwrite snapshot: {first}"))
    );
    let after = planner
        .initial_offset(&StartPosition::AfterSnapshot(SnapshotId::new(first)))
        .await
        .expect("after")
        .expect("some");
    assert_eq!(after, input_offset(&table, first, 1));
    let plan = planner
        .next_window(&after, WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some");
    let paths: Vec<&str> = plan
        .files
        .iter()
        .map(|file| file.task.data_file_path())
        .collect();
    assert_eq!(paths, vec!["second.parquet"]);
}

#[tokio::test]
async fn from_timestamp_landing_on_overwrite_refuses_in_the_window() {
    let (_warehouse, catalog, ident) = fixture_table("stamp-overwrite").await;
    let first_file = synthetic_file("a.parquet", 2);
    commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    pause();
    let overwrite = commit_overwrite(
        &catalog,
        &ident,
        vec![
            synthetic_file("c.parquet", 5),
            synthetic_file("e.parquet", 1),
        ],
        vec![first_file],
    )
    .await;
    pause();
    let head = commit_append(&catalog, &ident, vec![synthetic_file("d.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let start = planner
        .initial_offset(&StartPosition::FromTimestamp {
            millis: snapshot_timestamp(&table, overwrite),
        })
        .await
        .expect("initial")
        .expect("some");
    assert_eq!(start, input_offset(&table, overwrite, 0));
    let error = planner
        .next_window(&start, WindowLimit::Capped)
        .await
        .expect_err("must refuse");
    expect_non_append(
        &error,
        overwrite,
        &Operation::Overwrite,
        Some(overwrite),
        head,
    );
    let midway = planner
        .next_window(&input_offset(&table, overwrite, 1), WindowLimit::Capped)
        .await
        .expect_err("one of two files still unread");
    expect_non_append(
        &midway,
        overwrite,
        &Operation::Overwrite,
        Some(overwrite),
        head,
    );
    let after = planner
        .initial_offset(&StartPosition::AfterSnapshot(SnapshotId::new(overwrite)))
        .await
        .expect("after")
        .expect("some");
    assert_eq!(after, input_offset(&table, overwrite, 2));
    let plan = planner
        .next_window(&after, WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some");
    assert_eq!(plan.end, input_offset(&table, head, 1));
    assert_eq!(plan.num_input_rows, 1);
}

#[tokio::test]
async fn from_timestamp_landing_on_delete_refuses_at_the_initial_offset() {
    let (_warehouse, catalog, ident) = fixture_table("stamp-delete").await;
    let first_file = synthetic_file("a.parquet", 2);
    commit_append(&catalog, &ident, vec![first_file.clone()]).await;
    pause();
    let delete = commit_delete(&catalog, &ident, vec![first_file]).await;
    pause();
    let head = commit_append(&catalog, &ident, vec![synthetic_file("d.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    assert_eq!(operation_of(&table, delete), Operation::Delete);
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let error = planner
        .initial_offset(&StartPosition::FromTimestamp {
            millis: snapshot_timestamp(&table, delete),
        })
        .await
        .expect_err("must refuse");
    expect_non_append(&error, delete, &Operation::Delete, None, head);
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
    let plan = planner
        .next_window(&after, WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some");
    assert_eq!(plan.end, input_offset(&table, head, 1));
    assert_eq!(plan.num_input_rows, 1);
}

#[tokio::test]
async fn from_timestamp_past_head_waits_for_a_snapshot_at_or_after_it() {
    let (_warehouse, catalog, ident) = fixture_table("stamp-landing").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("a.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let target = snapshot_timestamp(&table, first) + 1_500;
    let start = StartPosition::FromTimestamp { millis: target };
    let planner = WindowPlanner::new(table, uncapped());
    assert!(
        planner
            .initial_offset(&start)
            .await
            .expect("idle")
            .is_none()
    );
    let below = commit_append(&catalog, &ident, vec![synthetic_file("b.parquet", 7)]).await;
    let table = load(&catalog, &ident).await;
    assert!(snapshot_timestamp(&table, below) < target);
    let planner = WindowPlanner::new(table, uncapped());
    assert!(
        planner
            .initial_offset(&start)
            .await
            .expect("still idle")
            .is_none()
    );
    while std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis()
        <= u128::try_from(target).expect("positive target")
    {
        pause();
    }
    let landed = commit_append(&catalog, &ident, vec![synthetic_file("c.parquet", 3)]).await;
    let table = load(&catalog, &ident).await;
    assert!(snapshot_timestamp(&table, landed) >= target);
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let offset = planner
        .initial_offset(&start)
        .await
        .expect("landed")
        .expect("some");
    assert_eq!(offset, input_offset(&table, landed, 0));
    let plans = drain(&planner, offset).await;
    assert_eq!(shape(&plans), vec![(3, landed, 1)]);
}

#[tokio::test]
async fn capped_window_delivers_appends_before_refusing_the_overwrite() {
    let (_warehouse, catalog, ident) = fixture_table("lazy-refusal").await;
    let first_file = synthetic_file("a.parquet", 1);
    let first = commit_append(
        &catalog,
        &ident,
        vec![
            first_file.clone(),
            synthetic_file("b.parquet", 1),
            synthetic_file("c.parquet", 1),
        ],
    )
    .await;
    let overwrite = commit_overwrite(
        &catalog,
        &ident,
        vec![synthetic_file("x.parquet", 1)],
        vec![first_file],
    )
    .await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), capped(Some(1), None));
    let mut cursor = input_offset(&table, first, 0);
    let mut paths = Vec::new();
    for position in 1..=3u64 {
        let plan = planner
            .next_window(&cursor, WindowLimit::Capped)
            .await
            .expect("an append before the overwrite")
            .expect("some");
        assert_eq!(plan.end, input_offset(&table, first, position));
        paths.extend(
            plan.files
                .iter()
                .map(|file| file.task.data_file_path().to_string()),
        );
        cursor = plan.end;
    }
    assert_eq!(paths, vec!["a.parquet", "b.parquet", "c.parquet"]);
    let error = planner
        .next_window(&cursor, WindowLimit::Capped)
        .await
        .expect_err("the window now enters the overwrite");
    expect_non_append(
        &error,
        overwrite,
        &Operation::Overwrite,
        Some(first),
        overwrite,
    );
}

#[tokio::test]
async fn capped_windows_plan_a_bounded_number_of_snapshots() {
    let (_warehouse, catalog, ident) = fixture_table("lazy-planning").await;
    let mut snapshots = Vec::new();
    for index in 0..8 {
        snapshots.push(
            commit_append(
                &catalog,
                &ident,
                vec![synthetic_file(&format!("f{index}.parquet"), 1)],
            )
            .await,
        );
    }
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), capped(Some(1), None));
    let mut cursor = input_offset(&table, snapshots[0], 0);
    let mut listings = Vec::new();
    loop {
        let before = planner.planned_listings();
        let plan = planner
            .next_window(&cursor, WindowLimit::Capped)
            .await
            .expect("window");
        listings.push(planner.planned_listings() - before);
        match plan {
            Some(plan) => cursor = plan.end,
            None => break,
        }
    }
    assert_eq!(listings, vec![1, 2, 2, 2, 2, 2, 2, 2, 1]);
    assert_eq!(cursor, input_offset(&table, snapshots[7], 1));
}

#[tokio::test]
async fn position_past_the_added_files_refuses() {
    let (_warehouse, catalog, ident) = fixture_table("position-range").await;
    let first = commit_append(
        &catalog,
        &ident,
        vec![
            synthetic_file("a.parquet", 1),
            synthetic_file("b.parquet", 1),
        ],
    )
    .await;
    let head = commit_append(&catalog, &ident, vec![synthetic_file("c.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    for (snapshot, position, files) in [(first, 99, 2), (first, 3, 2), (head, 2, 1)] {
        let error = planner
            .next_window(
                &input_offset(&table, snapshot, position),
                WindowLimit::Capped,
            )
            .await
            .expect_err("must refuse");
        match error {
            MicroBatchError::OffsetPositionOutOfRange {
                snapshot: refused,
                position: refused_position,
                files: refused_files,
                ..
            } => {
                assert_eq!(refused, SnapshotId::new(snapshot));
                assert_eq!(refused_position, FilePosition::new(position));
                assert_eq!(refused_files, files);
            }
            other => panic!("expected OffsetPositionOutOfRange, got {other:?}"),
        }
    }
    let at_count = planner
        .next_window(&input_offset(&table, first, 2), WindowLimit::Capped)
        .await
        .expect("position equal to the count")
        .expect("some");
    assert_eq!(at_count.end, input_offset(&table, head, 1));
    assert!(
        planner
            .next_window(&input_offset(&table, head, 1), WindowLimit::Capped)
            .await
            .expect("drained head")
            .is_none()
    );
}

#[tokio::test]
async fn earliest_refuses_truncated_history_after_expiry() {
    let (_warehouse, catalog, ident) = fixture_table("truncated").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("a.parquet", 1)]).await;
    pause();
    let second = commit_append(&catalog, &ident, vec![synthetic_file("b.parquet", 1)]).await;
    pause();
    commit_append(&catalog, &ident, vec![synthetic_file("c.parquet", 1)]).await;
    let table = load(&catalog, &ident).await;
    let tx = Transaction::new(&table);
    let action = tx.expire_snapshots().expire_snapshot_id(first);
    let tx = action.apply(tx).expect("apply expire");
    tx.commit(catalog.as_ref()).await.expect("commit expire");
    let table = load(&catalog, &ident).await;
    assert!(table.metadata().snapshot_by_id(first).is_none());
    let planner = WindowPlanner::new(table, uncapped());
    let error = planner
        .initial_offset(&StartPosition::Earliest)
        .await
        .expect_err("must refuse");
    match error {
        MicroBatchError::TruncatedHistory {
            oldest,
            missing_parent,
            ..
        } => {
            assert_eq!(oldest, SnapshotId::new(second));
            assert_eq!(missing_parent, SnapshotId::new(first));
        }
        other => panic!("expected TruncatedHistory, got {other:?}"),
    }
}

#[tokio::test]
async fn max_rows_adds_the_crossing_file_then_stops_at_the_cap() {
    let (_warehouse, catalog, ident) = fixture_table("max-rows-spark").await;
    let mut snapshots = Vec::new();
    for name in ["s1.parquet", "s2.parquet", "s3.parquet"] {
        snapshots.push(commit_append(&catalog, &ident, vec![synthetic_file(name, 2)]).await);
    }
    let table = load(&catalog, &ident).await;
    let from = input_offset(&table, snapshots[0], 0);
    for (rows, expected) in [
        (3, vec![(4, snapshots[1], 1), (2, snapshots[2], 1)]),
        (4, vec![(4, snapshots[1], 1), (2, snapshots[2], 1)]),
        (5, vec![(6, snapshots[2], 1)]),
    ] {
        let planner = WindowPlanner::new(table.clone(), capped(None, Some(rows)));
        assert_eq!(
            shape(&drain(&planner, from.clone()).await),
            expected,
            "max rows {rows}"
        );
    }
}

#[tokio::test]
async fn max_rows_rule_holds_inside_one_snapshot_and_on_a_zero_row_tail() {
    let (_warehouse, catalog, ident) = fixture_table("max-rows-one").await;
    let single = commit_append(
        &catalog,
        &ident,
        vec![
            synthetic_file("f0.parquet", 2),
            synthetic_file("f1.parquet", 2),
            synthetic_file("f2.parquet", 2),
        ],
    )
    .await;
    let table = load(&catalog, &ident).await;
    let from = input_offset(&table, single, 0);
    for (rows, expected) in [
        (3, vec![(4, single, 2), (2, single, 3)]),
        (4, vec![(4, single, 2), (2, single, 3)]),
        (5, vec![(6, single, 3)]),
    ] {
        let planner = WindowPlanner::new(table.clone(), capped(None, Some(rows)));
        assert_eq!(
            shape(&drain(&planner, from.clone()).await),
            expected,
            "max rows {rows}"
        );
    }
    let (_tail_warehouse, tail_catalog, tail_ident) = fixture_table("zero-row-tail").await;
    let tail = commit_append(
        &tail_catalog,
        &tail_ident,
        vec![
            synthetic_file("g0.parquet", 2),
            synthetic_file("g1.parquet", 0),
        ],
    )
    .await;
    let table = load(&tail_catalog, &tail_ident).await;
    let planner = WindowPlanner::new(table.clone(), capped(None, Some(2)));
    let plans = drain(&planner, input_offset(&table, tail, 0)).await;
    assert_eq!(shape(&plans), vec![(2, tail, 1), (0, tail, 2)]);
}

#[tokio::test]
async fn a_task_without_a_record_count_refuses() {
    let (_warehouse, catalog, ident) = fixture_table("no-count").await;
    let first = commit_append(&catalog, &ident, vec![synthetic_file("a.parquet", 2)]).await;
    let table = load(&catalog, &ident).await;
    let planner = WindowPlanner::new(table.clone(), uncapped());
    let mut task = planner
        .next_window(&input_offset(&table, first, 0), WindowLimit::Capped)
        .await
        .expect("window")
        .expect("some")
        .files
        .remove(0)
        .task;
    task.record_count = None;
    match planner.planned_file(first, 0, task) {
        Err(MicroBatchError::Catalog(message)) => {
            assert!(message.contains("a.parquet"), "{message}");
            assert!(message.contains("no record count"), "{message}");
        }
        other => panic!("expected Catalog, got {other:?}"),
    }
}
