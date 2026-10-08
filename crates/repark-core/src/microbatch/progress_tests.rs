use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

use repark_iceberg::microbatch::offset::{
    Epoch, FilePosition, InputOffset, QueryId, RunId, SnapshotId, TableUuid,
};
use tokio::time::Instant;
use uuid::Uuid;

use super::*;

fn identity() -> Identity<'static> {
    Identity {
        id: QueryId::new(Uuid::from_u128(1)),
        run_id: RunId::new(Uuid::from_u128(2)),
        name: None,
        source: "sales.orders",
        sink: "ice.sales.silver",
    }
}

fn offset(snapshot: i64, position: u64) -> InputOffset {
    InputOffset {
        table: TableUuid::new(Uuid::from_u128(3)),
        table_name: "sales.orders".to_string(),
        snapshot: SnapshotId::new(snapshot),
        position: FilePosition::new(position),
    }
}

fn report(executed: bool, epoch: u64, started: Instant, rows: u64) -> TriggerReport {
    TriggerReport {
        executed,
        epoch: Epoch::new(epoch),
        started_at: SystemTime::UNIX_EPOCH + Duration::from_millis(1_791_307_474_878),
        started,
        planned: started + Duration::from_millis(2),
        finished: started + Duration::from_millis(184),
        add_batch: Duration::from_millis(97),
        num_input_rows: rows,
        start_offset: None,
        end_offset: Some(offset(7_596_292_182_655_365_750, 1)),
        num_output_rows: executed.then_some(rows),
    }
}

fn keys(value: &serde_json::Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

#[test]
fn a_batch_progress_renders_t3s_fields() {
    let mut log = ProgressLog::new(DEFAULT_RECENT_PROGRESS);
    log.record(&identity(), &report(true, 0, Instant::now(), 3));
    let json = log.last().expect("a progress").json();
    assert_eq!(
        keys(&json),
        set(&[
            "id",
            "runId",
            "name",
            "timestamp",
            "batchId",
            "batchDuration",
            "numInputRows",
            "inputRowsPerSecond",
            "processedRowsPerSecond",
            "durationMs",
            "stateOperators",
            "sources",
            "sink",
        ])
    );
    assert_eq!(
        keys(&json["durationMs"]),
        set(&[
            "addBatch",
            "commitOffsets",
            "getBatch",
            "latestOffset",
            "queryPlanning",
            "triggerExecution",
            "walCommit",
        ])
    );
    assert_eq!(
        keys(&json["sources"][0]),
        set(&[
            "description",
            "startOffset",
            "endOffset",
            "latestOffset",
            "numInputRows",
            "inputRowsPerSecond",
            "processedRowsPerSecond",
        ])
    );
    assert_eq!(keys(&json["sink"]), set(&["description", "numOutputRows"]));
    assert_eq!(json["id"], "00000000-0000-0000-0000-000000000001");
    assert_eq!(json["runId"], "00000000-0000-0000-0000-000000000002");
    assert_eq!(json["name"], serde_json::Value::Null);
    assert_eq!(json["timestamp"], "2026-10-06T17:24:34.878Z");
    assert_eq!(json["batchId"], 0);
    assert_eq!(json["batchDuration"], 184);
    assert_eq!(json["numInputRows"], 3);
    assert_eq!(json["inputRowsPerSecond"], 0.0);
    assert_eq!(json["processedRowsPerSecond"], 16.3);
    assert_eq!(json["durationMs"]["triggerExecution"], 184);
    assert_eq!(json["durationMs"]["addBatch"], 97);
    assert_eq!(json["durationMs"]["latestOffset"], 2);
    assert_eq!(json["durationMs"]["walCommit"], 0);
    assert_eq!(json["durationMs"]["commitOffsets"], 0);
    assert_eq!(json["stateOperators"], serde_json::json!([]));
    let source = &json["sources"][0];
    assert_eq!(
        source["description"],
        "IcebergMicroBatchStream[sales.orders]"
    );
    assert_eq!(source["startOffset"], serde_json::Value::Null);
    assert_eq!(source["latestOffset"], serde_json::Value::Null);
    assert_eq!(
        source["endOffset"],
        serde_json::json!({
            "version": 1,
            "snapshot_id": 7_596_292_182_655_365_750_i64,
            "position": 1,
            "scan_all_files": false
        })
    );
    assert_eq!(json["sink"]["description"], "ice.sales.silver");
    assert_eq!(json["sink"]["numOutputRows"], 3);
}

#[test]
fn input_rate_follows_the_previous_trigger_and_output_rows_default_to_minus_one() {
    let mut log = ProgressLog::new(DEFAULT_RECENT_PROGRESS);
    let first = Instant::now();
    log.record(&identity(), &report(true, 0, first, 3));
    let mut second = report(true, 1, first + Duration::from_secs(2), 4);
    second.num_output_rows = None;
    log.record(&identity(), &second);
    let json = log.last().expect("a progress").json();
    assert_eq!(json["inputRowsPerSecond"], 2.0);
    assert_eq!(json["sources"][0]["inputRowsPerSecond"], 2.0);
    assert_eq!(json["sink"]["numOutputRows"], -1);
}

#[test]
fn the_ring_keeps_the_newest_limit_entries() {
    let mut log = ProgressLog::new(2);
    let start = Instant::now();
    for epoch in 0..3 {
        log.record(
            &identity(),
            &report(true, epoch, start + Duration::from_secs(epoch), 1),
        );
    }
    let ids: Vec<u64> = log
        .recent()
        .iter()
        .map(|progress| progress.batch_id.get())
        .collect();
    assert_eq!(ids, [1, 2]);
    assert_eq!(ProgressLog::new(0).limit, 1);
}

#[test]
fn an_idle_trigger_reports_at_most_once_per_interval() {
    let mut log = ProgressLog::new(DEFAULT_RECENT_PROGRESS);
    let start = Instant::now();
    log.record(&identity(), &report(false, 4, start, 0));
    log.record(
        &identity(),
        &report(false, 4, start + Duration::from_secs(1), 0),
    );
    log.record(
        &identity(),
        &report(true, 4, start + Duration::from_secs(2), 1),
    );
    log.record(
        &identity(),
        &report(false, 5, start + Duration::from_secs(3), 0),
    );
    log.record(
        &identity(),
        &report(false, 5, start + NO_DATA_PROGRESS_INTERVAL, 0),
    );
    let ids: Vec<(u64, u64)> = log
        .recent()
        .iter()
        .map(|progress| (progress.batch_id.get(), progress.num_input_rows))
        .collect();
    assert_eq!(ids, [(4, 0), (4, 1), (5, 0)]);
}

#[test]
fn status_renders_t3s_messages() {
    let mut log = ProgressLog::new(DEFAULT_RECENT_PROGRESS);
    assert_eq!(log.status().message, StatusMessage::Initializing);
    log.trigger_started();
    log.data_found();
    assert_eq!(
        log.status().json(),
        serde_json::json!({
            "message": "Processing new data",
            "isDataAvailable": true,
            "isTriggerActive": true
        })
    );
    log.trigger_finished(true);
    assert_eq!(
        log.status().json(),
        serde_json::json!({
            "message": "Waiting for next trigger",
            "isDataAvailable": true,
            "isTriggerActive": false
        })
    );
    log.stopped();
    assert_eq!(
        log.status().json(),
        serde_json::json!({
            "message": "Stopped",
            "isDataAvailable": false,
            "isTriggerActive": false
        })
    );
}
