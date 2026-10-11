use std::time::{Duration, UNIX_EPOCH};

use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::{SPARK_EPOCH_ID_KEY, SinkRecord};

use crate::microbatch::driver::{QueryState, ShutdownOutcome, StreamingQueryManager, Trigger};
use crate::microbatch::testing::{
    Fixture, SINK, SOURCE, options, stamped_epochs, started, table_spec, wait_for_epoch,
};

#[test]
fn the_next_trigger_is_the_next_multiple_of_the_interval() {
    let at = |millis: u64| UNIX_EPOCH + Duration::from_millis(millis);
    let wait = |millis: u64, interval: u64| {
        super::until_next_trigger(at(millis), Duration::from_millis(interval))
    };
    assert_eq!(wait(10_250, 2_000), Duration::from_millis(1_750));
    assert_eq!(wait(11_999, 2_000), Duration::from_millis(1));
    assert_eq!(wait(12_000, 2_000), Duration::from_secs(2));
    assert_eq!(wait(0, 2_000), Duration::from_secs(2));
    assert_eq!(wait(3_599_000, 3_600_000), Duration::from_secs(1));
    assert_eq!(wait(5, 10_000), Duration::from_millis(9_995));
}

fn durable_epoch(outcome: &ShutdownOutcome) -> Option<u64> {
    outcome.durable().map(|record| record.epoch.get())
}

async fn bronze(fixture: &Fixture) {
    fixture.insert(SOURCE, "(1), (2), (3)").await;
    fixture.insert(SOURCE, "(4), (5)").await;
}

#[tokio::test]
async fn available_now_drains_and_stops() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(&fixture, table_spec(Trigger::AvailableNow, &options(&[]))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(handle.state(), QueryState::Stopped);
    assert!(!handle.is_active());
    let outcome = handle.stop().await;
    assert!(
        matches!(outcome, ShutdownOutcome::Drained { .. }),
        "{outcome:?}"
    );
    assert_eq!(durable_epoch(&outcome), Some(0));
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0]);
    let head = sink.metadata().current_snapshot().expect("a head");
    assert_eq!(
        head.summary().additional_properties.get(SPARK_EPOCH_ID_KEY),
        Some(&"0".to_string())
    );
    assert!(
        StreamingQueryManager::of(&fixture.session)
            .active()
            .is_empty()
    );
}

#[tokio::test]
async fn available_now_honours_the_caps_one_batch_per_file() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let spec = table_spec(
        Trigger::AvailableNow,
        &options(&[("streaming-max-files-per-micro-batch", "1")]),
    );
    let handle = started(&fixture, spec).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0, 1]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
    let batches: Vec<(u64, u64)> = handle
        .recent_progress()
        .iter()
        .map(|progress| (progress.batch_id.get(), progress.num_input_rows))
        .collect();
    assert_eq!(
        batches,
        [(0, 3), (1, 2)],
        "no trailing idle progress (MB0b-R15)"
    );
}

#[tokio::test]
async fn a_restart_resumes_from_the_sink_and_numbers_epochs_on() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let spec = table_spec(Trigger::AvailableNow, &options(&[]));
    let first = started(&fixture, spec.clone()).await;
    assert_eq!(first.await_termination(None).await, Ok(true));
    let idle = started(&fixture, spec.clone()).await;
    assert_eq!(idle.await_termination(None).await, Ok(true));
    let outcome = idle.stop().await;
    assert_eq!(durable_epoch(&outcome), Some(0));
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    fixture.insert(SOURCE, "(6)").await;
    let second = started(&fixture, spec).await;
    assert_eq!(second.await_termination(None).await, Ok(true));
    assert_ne!(second.run_id(), first.run_id());
    assert_eq!(second.id(), first.id());
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn available_now_on_an_empty_source_drains_without_a_commit() {
    let fixture = Fixture::new().await;
    let handle = started(&fixture, table_spec(Trigger::AvailableNow, &options(&[]))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    let outcome = handle.stop().await;
    assert_eq!(outcome, ShutdownOutcome::Drained { durable: None });
    let batches: Vec<(u64, u64)> = handle
        .recent_progress()
        .iter()
        .map(|progress| (progress.batch_id.get(), progress.num_input_rows))
        .collect();
    assert_eq!(batches, [(0, 0)], "one idle progress (MB0b-R14)");
    let sink = fixture.table("silver").await;
    assert!(sink.metadata().current_snapshot().is_none());
}

#[tokio::test]
async fn once_runs_one_uncapped_batch_and_stops() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let spec = table_spec(
        Trigger::Once,
        &options(&[("streaming-max-files-per-micro-batch", "1")]),
    );
    let handle = started(&fixture, spec).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn processing_time_ticks_and_stop_reports_the_durable_offset() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(
        &fixture,
        table_spec(Trigger::ProcessingTime(Duration::ZERO), &options(&[])),
    )
    .await;
    wait_for_epoch(&handle, 0).await;
    assert!(handle.is_active());
    fixture.insert(SOURCE, "(6), (7)").await;
    wait_for_epoch(&handle, 1).await;
    assert_eq!(
        handle
            .await_termination(Some(Duration::from_millis(20)))
            .await,
        Ok(false)
    );
    let outcome = handle.stop().await;
    assert!(
        matches!(outcome, ShutdownOutcome::Stopped { .. }),
        "{outcome:?}"
    );
    let durable: &SinkRecord = outcome.durable().expect("a durable record");
    assert_eq!(durable.epoch.get(), 1);
    assert_eq!(durable.run, handle.run_id());
    assert_eq!(
        durable.offsets.inputs()[0].position.get(),
        1,
        "the end offset is past the one file of the third append"
    );
    assert_eq!(handle.state(), QueryState::Stopped);
    assert!(!handle.is_active());
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(handle.stop().await, outcome);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
}

#[tokio::test]
async fn a_long_trigger_interval_still_stops_at_once() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let mut spec = table_spec(
        Trigger::ProcessingTime(Duration::from_hours(1)),
        &options(&[]),
    );
    spec.stop_timeout = Some(Duration::from_secs(30));
    let handle = started(&fixture, spec).await;
    wait_for_epoch(&handle, 0).await;
    let outcome = tokio::time::timeout(Duration::from_secs(10), handle.stop())
        .await
        .expect("stop wakes the trigger wait");
    assert!(
        matches!(outcome, ShutdownOutcome::Stopped { .. }),
        "{outcome:?}"
    );
    assert_eq!(durable_epoch(&outcome), Some(0));
}

#[tokio::test]
async fn dropping_the_handle_does_not_stop_the_query() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(
        &fixture,
        table_spec(Trigger::ProcessingTime(Duration::ZERO), &options(&[])),
    )
    .await;
    let id = handle.id();
    wait_for_epoch(&handle, 0).await;
    drop(handle);
    fixture.insert(SOURCE, "(6)").await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let found = manager.get(id).expect("the query is still active");
    wait_for_epoch(&found, 1).await;
    drop(found);
    let outcomes = manager.stop_all().await;
    assert_eq!(outcomes.len(), 1);
    assert!(matches!(outcomes[0], ShutdownOutcome::Stopped { .. }));
    assert_eq!(durable_epoch(&outcomes[0]), Some(1));
    assert!(manager.active().is_empty());
}

#[tokio::test]
async fn start_refuses_a_local_catalog_and_a_second_start() {
    let fixture = Fixture::new().await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let handle = manager
        .register(
            &fixture.session,
            table_spec(Trigger::AvailableNow, &options(&[])),
        )
        .await
        .expect("register");
    let error = handle.start().await.expect_err("a memory catalog refuses");
    assert_eq!(
        error,
        MicroBatchError::LocalCatalogRefused {
            catalog: "ice".to_string()
        }
    );
    assert_eq!(
        error.to_string(),
        "streaming needs a shared catalog; ice is a local filesystem catalog. Use Glue or S3 Tables"
    );
    assert_eq!(handle.state(), QueryState::Registered);
    handle
        .start_below_catalog_check()
        .expect("the test seam starts");
    let again = handle
        .start_below_catalog_check()
        .expect_err("a second start refuses");
    assert!(
        matches!(&again, MicroBatchError::Catalog(message) if message.contains("already started")),
        "{again:?}"
    );
    assert_eq!(handle.await_termination(None).await, Ok(true));
}

#[tokio::test]
async fn a_second_query_with_the_same_id_refuses_while_the_first_is_active() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let spec = table_spec(Trigger::ProcessingTime(Duration::ZERO), &options(&[]));
    let first = started(&fixture, spec.clone()).await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let second = manager
        .register(&fixture.session, spec)
        .await
        .expect("register");
    let error = second
        .start_below_catalog_check()
        .expect_err("the same id is active");
    assert!(
        matches!(&error, MicroBatchError::Catalog(message) if message.contains("same id is already active")),
        "{error:?}"
    );
    assert_eq!(second.state(), QueryState::Registered);
    first.stop().await;
    second
        .start_below_catalog_check()
        .expect("the id is free after the first stops");
    second.stop().await;
}

#[tokio::test]
async fn stopping_a_registered_query_never_runs_it() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let handle = manager
        .register(
            &fixture.session,
            table_spec(Trigger::AvailableNow, &options(&[])),
        )
        .await
        .expect("register");
    let outcome = handle.stop().await;
    assert_eq!(outcome, ShutdownOutcome::Stopped { durable: None });
    assert_eq!(handle.state(), QueryState::Stopped);
    let error = handle
        .start_below_catalog_check()
        .expect_err("a stopped query does not start");
    assert!(matches!(error, MicroBatchError::Catalog(_)));
    assert!(
        fixture
            .table("silver")
            .await
            .metadata()
            .current_snapshot()
            .is_none()
    );
}

#[tokio::test]
async fn a_changed_source_refuses_inputs_changed() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let first = started(&fixture, table_spec(Trigger::AvailableNow, &options(&[]))).await;
    assert_eq!(first.await_termination(None).await, Ok(true));
    fixture.insert("ice.sales.other", "(9)").await;
    let mut spec = table_spec(Trigger::AvailableNow, &options(&[]));
    spec.source = "ice.sales.other".to_string();
    let second = started(&fixture, spec).await;
    let error = second
        .await_termination(None)
        .await
        .expect_err("the input set changed");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::InputsChanged { recorded, current, .. }
                if recorded == &["sales.orders".to_string()] && current == &["sales.other".to_string()]
        ),
        "{error:?}"
    );
    assert_eq!(second.state(), QueryState::Failed);
    let outcome = second.stop().await;
    assert!(matches!(outcome, ShutdownOutcome::Failed { .. }));
    assert_eq!(durable_epoch(&outcome), Some(0));
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn processing_time_delivers_every_append_before_a_non_append() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    fixture.insert(SOURCE, "(2)").await;
    fixture
        .session
        .sql(&format!("DELETE FROM {SOURCE} WHERE id = 1"))
        .await
        .expect("the delete plans")
        .collect()
        .await
        .expect("the delete commits");
    let spec = table_spec(
        Trigger::ProcessingTime(Duration::ZERO),
        &options(&[("streaming-max-files-per-micro-batch", "1")]),
    );
    let handle = started(&fixture, spec).await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the delete refuses");
    assert!(
        matches!(error.as_ref(), MicroBatchError::NonAppendSnapshot { .. }),
        "{error:?}"
    );
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
    assert_eq!(fixture.ids(SINK).await, [1, 2]);
}

#[tokio::test]
async fn available_now_refuses_a_non_append_before_any_batch() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    fixture.insert(SOURCE, "(2)").await;
    fixture
        .session
        .sql(&format!("DELETE FROM {SOURCE} WHERE id = 1"))
        .await
        .expect("the delete plans")
        .collect()
        .await
        .expect("the delete commits");
    let spec = table_spec(
        Trigger::AvailableNow,
        &options(&[("streaming-max-files-per-micro-batch", "1")]),
    );
    let handle = started(&fixture, spec).await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the delete refuses");
    assert!(
        matches!(error.as_ref(), MicroBatchError::NonAppendSnapshot { .. }),
        "{error:?}"
    );
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
}

#[tokio::test]
async fn a_window_planned_past_the_available_now_end_is_cut_at_the_end() {
    use crate::time_travel::microbatch_source::MicroBatchSource;
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let source = MicroBatchSource::open(&fixture.session, SOURCE, options(&[]))
        .await
        .expect("open");
    let from = source
        .initial_offset()
        .await
        .expect("initial")
        .expect("a start");
    let target = source
        .available_now_target(&from)
        .await
        .expect("target")
        .expect("an end");
    fixture.insert(SOURCE, "(6), (7)").await;
    let batch = source
        .next_batch_until(&from, &target)
        .await
        .expect("plan")
        .expect("a batch");
    assert_eq!(batch.num_input_rows, 5);
    assert_eq!(batch.end, target);
    let rows = batch.frame.collect().await.expect("read");
    assert_eq!(crate::microbatch::testing::ids_of(&rows), [1, 2, 3, 4, 5]);
    assert!(
        source
            .next_batch_until(&batch.end, &target)
            .await
            .expect("plan")
            .is_none()
    );
    let past = source
        .next_batch(
            &batch.end,
            repark_iceberg::microbatch::window::WindowLimit::Capped,
        )
        .await
        .expect("plan")
        .expect("the later append");
    assert_eq!(past.num_input_rows, 2);
}

#[tokio::test]
async fn progress_and_status_follow_t3() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(
        &fixture,
        table_spec(
            Trigger::ProcessingTime(Duration::from_hours(1)),
            &options(&[]),
        ),
    )
    .await;
    assert_eq!(
        handle.status().message,
        crate::microbatch::progress::StatusMessage::Initializing
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while handle.status().message
        != crate::microbatch::progress::StatusMessage::WaitingForNextTrigger
    {
        assert!(
            std::time::Instant::now() < deadline,
            "{:?}",
            handle.status()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        handle.status().json(),
        serde_json::json!({
            "message": "Waiting for next trigger",
            "isDataAvailable": true,
            "isTriggerActive": false
        })
    );
    let recent = handle.recent_progress();
    assert_eq!(recent.len(), 1);
    let json = handle.last_progress().expect("a progress").json();
    assert_eq!(json, recent[0].json());
    assert_eq!(json["id"], handle.id().to_string());
    assert_eq!(json["runId"], handle.run_id().to_string());
    assert_eq!(json["batchId"], 0);
    assert_eq!(json["numInputRows"], 5);
    assert_eq!(json["sink"]["description"], SINK);
    assert_eq!(json["sink"]["numOutputRows"], 5);
    let source = &json["sources"][0];
    assert_eq!(
        source["description"],
        "IcebergMicroBatchStream[sales.orders]"
    );
    assert_eq!(source["startOffset"], serde_json::Value::Null);
    let head = fixture
        .table("orders")
        .await
        .metadata()
        .current_snapshot_id()
        .expect("a head");
    assert_eq!(
        source["endOffset"],
        serde_json::json!({"version": 1, "snapshot_id": head, "position": 1, "scan_all_files": false})
    );
    handle.stop().await;
    assert_eq!(
        handle.status().json(),
        serde_json::json!({
            "message": "Stopped",
            "isDataAvailable": false,
            "isTriggerActive": false
        })
    );
}

#[tokio::test]
async fn an_idle_restart_reports_the_next_batch_id_and_commits_nothing() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let spec = table_spec(Trigger::AvailableNow, &options(&[]));
    let first = started(&fixture, spec.clone()).await;
    assert_eq!(first.await_termination(None).await, Ok(true));
    let snapshots = fixture.table("silver").await.metadata().snapshots().count();
    let idle = started(&fixture, spec).await;
    assert_eq!(idle.await_termination(None).await, Ok(true));
    let recent = idle.recent_progress();
    assert_eq!(recent.len(), 1);
    let json = recent[0].json();
    assert_eq!(json["batchId"], 1);
    assert_eq!(json["numInputRows"], 0);
    let source = &json["sources"][0];
    assert_ne!(source["startOffset"], serde_json::Value::Null);
    assert_eq!(source["startOffset"], source["endOffset"]);
    assert_eq!(
        fixture.table("silver").await.metadata().snapshots().count(),
        snapshots
    );
    let next = first
        .last_progress()
        .expect("the first run's progress")
        .json();
    assert_eq!(source["endOffset"], next["sources"][0]["endOffset"]);
}

fn files_under(root: &str) -> std::collections::BTreeSet<String> {
    let mut pending = vec![std::path::PathBuf::from(root)];
    let mut found = std::collections::BTreeSet::new();
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(path.display().to_string());
            }
        }
    }
    found
}

#[tokio::test]
async fn a_keyed_sink_refuses_the_batch_and_stages_no_file() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let sink = fixture.table("silver").await;
    let catalog = fixture
        .session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("catalog");
    let tx = Transaction::new(&sink);
    let tx = tx
        .update_table_properties()
        .set(
            "encryption.key-id".to_string(),
            "SEKRETKEYVAL9f3a7".to_string(),
        )
        .apply(tx)
        .expect("apply");
    tx.commit(catalog.as_ref()).await.expect("key set");
    let before = files_under(&fixture.root());
    let handle = started(&fixture, table_spec(Trigger::AvailableNow, &options(&[]))).await;
    let ended = handle.await_termination(None).await;
    let refusal = MicroBatchError::EncryptedSinkRefused {
        sink: String::from("sales.silver"),
    };
    assert_eq!(
        ended.map_err(|error| error.as_ref().clone()),
        Err(refusal.clone())
    );
    assert_eq!(handle.exception().as_deref(), Some(&refusal));
    let _ = handle.stop().await;
    let after = files_under(&fixture.root());
    let added: Vec<&String> = after.difference(&before).collect();
    assert!(added.is_empty(), "{added:#?}");
    assert_eq!(
        fixture.table("silver").await.metadata().snapshots().count(),
        0
    );
}
