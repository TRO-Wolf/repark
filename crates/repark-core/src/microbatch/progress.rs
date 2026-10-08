use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use repark_iceberg::microbatch::offset::{
    Epoch, InputOffset, QueryId, RunId, spark_source_offset_json,
};
use serde::{Serialize, Serializer};
use tokio::time::Instant;

pub const DEFAULT_RECENT_PROGRESS: usize = 100;
pub const NO_DATA_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_NUM_OUTPUT_ROWS: i64 = -1;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamingQueryProgress {
    #[serde(serialize_with = "display_text")]
    pub id: QueryId,
    #[serde(serialize_with = "display_text")]
    pub run_id: RunId,
    pub name: Option<String>,
    pub timestamp: String,
    #[serde(serialize_with = "epoch_number")]
    pub batch_id: Epoch,
    pub batch_duration: u64,
    pub num_input_rows: u64,
    #[serde(serialize_with = "rate")]
    pub input_rows_per_second: f64,
    #[serde(serialize_with = "rate")]
    pub processed_rows_per_second: f64,
    pub duration_ms: DurationMs,
    pub state_operators: Vec<()>,
    pub sources: Vec<SourceProgress>,
    pub sink: SinkProgress,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DurationMs {
    pub add_batch: u64,
    pub commit_offsets: u64,
    pub get_batch: u64,
    pub latest_offset: u64,
    pub query_planning: u64,
    pub trigger_execution: u64,
    pub wal_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceProgress {
    pub description: String,
    pub start_offset: Option<serde_json::Value>,
    pub end_offset: Option<serde_json::Value>,
    pub latest_offset: Option<serde_json::Value>,
    pub num_input_rows: u64,
    #[serde(serialize_with = "rate")]
    pub input_rows_per_second: f64,
    #[serde(serialize_with = "rate")]
    pub processed_rows_per_second: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkProgress {
    pub description: String,
    #[serde(serialize_with = "output_rows")]
    pub num_output_rows: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStatus {
    pub message: StatusMessage,
    pub is_data_available: bool,
    pub is_trigger_active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusMessage {
    WaitingForNextTrigger,
    Stopped,
    Initializing,
    ProcessingNewData,
}

impl StatusMessage {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            StatusMessage::WaitingForNextTrigger => "Waiting for next trigger",
            StatusMessage::Stopped => "Stopped",
            StatusMessage::Initializing => "Initializing sources",
            StatusMessage::ProcessingNewData => "Processing new data",
        }
    }
}

impl fmt::Display for StatusMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for StatusMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl StreamingQueryProgress {
    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

impl QueryStatus {
    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

fn display_text<T: fmt::Display, S: Serializer>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::ref_option,
    reason = "serde's serialize_with passes the field by reference"
)]
fn epoch_number<S: Serializer>(epoch: &Epoch, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(epoch.get())
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::ref_option,
    reason = "serde's serialize_with passes the field by reference"
)]
fn rate<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64((value * 10.0).round() / 10.0)
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::ref_option,
    reason = "serde's serialize_with passes the field by reference"
)]
fn output_rows<S: Serializer>(rows: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error> {
    match rows.and_then(|rows| i64::try_from(rows).ok()) {
        Some(rows) => serializer.serialize_i64(rows),
        None => serializer.serialize_i64(DEFAULT_NUM_OUTPUT_ROWS),
    }
}

#[must_use]
pub fn offset_json(offset: &InputOffset) -> Option<serde_json::Value> {
    serde_json::from_str(&spark_source_offset_json(offset)).ok()
}

#[must_use]
pub fn source_description(table: &str) -> String {
    format!("IcebergMicroBatchStream[{table}]")
}

pub(crate) struct TriggerReport {
    pub(crate) executed: bool,
    pub(crate) epoch: Epoch,
    pub(crate) started_at: SystemTime,
    pub(crate) started: Instant,
    pub(crate) planned: Instant,
    pub(crate) finished: Instant,
    pub(crate) add_batch: Duration,
    pub(crate) num_input_rows: u64,
    pub(crate) start_offset: Option<InputOffset>,
    pub(crate) end_offset: Option<InputOffset>,
    pub(crate) num_output_rows: Option<u64>,
}

pub(crate) struct Identity<'query> {
    pub(crate) id: QueryId,
    pub(crate) run_id: RunId,
    pub(crate) name: Option<&'query str>,
    pub(crate) source: &'query str,
    pub(crate) sink: &'query str,
}

pub(crate) struct ProgressLog {
    limit: usize,
    recent: VecDeque<StreamingQueryProgress>,
    status: QueryStatus,
    last_trigger: Option<Instant>,
    last_no_data: Option<Instant>,
}

impl ProgressLog {
    pub(crate) fn new(limit: usize) -> ProgressLog {
        ProgressLog {
            limit: limit.max(1),
            recent: VecDeque::new(),
            status: QueryStatus {
                message: StatusMessage::Initializing,
                is_data_available: false,
                is_trigger_active: false,
            },
            last_trigger: None,
            last_no_data: None,
        }
    }

    pub(crate) fn status(&self) -> QueryStatus {
        self.status
    }

    pub(crate) fn last(&self) -> Option<StreamingQueryProgress> {
        self.recent.back().cloned()
    }

    pub(crate) fn recent(&self) -> Vec<StreamingQueryProgress> {
        self.recent.iter().cloned().collect()
    }

    pub(crate) fn trigger_started(&mut self) {
        self.status.is_trigger_active = true;
    }

    pub(crate) fn data_found(&mut self) {
        self.status.is_data_available = true;
        self.status.message = StatusMessage::ProcessingNewData;
    }

    pub(crate) fn trigger_finished(&mut self, found: bool) {
        self.status.is_trigger_active = false;
        self.status.is_data_available = found;
        self.status.message = StatusMessage::WaitingForNextTrigger;
    }

    pub(crate) fn stopped(&mut self) {
        self.status = QueryStatus {
            message: StatusMessage::Stopped,
            is_data_available: false,
            is_trigger_active: false,
        };
    }

    pub(crate) fn record(&mut self, identity: &Identity<'_>, report: &TriggerReport) {
        if !report.executed {
            if self
                .last_no_data
                .is_some_and(|last| report.started.duration_since(last) < NO_DATA_PROGRESS_INTERVAL)
            {
                self.last_trigger = Some(report.started);
                return;
            }
            self.last_no_data = Some(report.started);
        }
        let progress = build(identity, report, self.last_trigger);
        self.last_trigger = Some(report.started);
        if self.recent.len() >= self.limit {
            self.recent.pop_front();
        }
        self.recent.push_back(progress);
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Spark's progress rates are doubles over row counts"
)]
fn per_second(rows: u64, seconds: f64) -> f64 {
    if seconds.is_finite() && seconds > 0.0 {
        rows as f64 / seconds
    } else {
        0.0
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "a millisecond count rendered as seconds"
)]
fn seconds(millis: u64) -> f64 {
    millis as f64 / 1000.0
}

fn build(
    identity: &Identity<'_>,
    report: &TriggerReport,
    last_trigger: Option<Instant>,
) -> StreamingQueryProgress {
    let trigger_execution = millis(report.finished.duration_since(report.started));
    let latest_offset = millis(report.planned.duration_since(report.started));
    let input_seconds = last_trigger.map_or(f64::INFINITY, |last| {
        seconds(millis(report.started.duration_since(last)))
    });
    let processing_seconds = seconds(trigger_execution.max(1));
    let input_rate = per_second(report.num_input_rows, input_seconds);
    let processed_rate = per_second(report.num_input_rows, processing_seconds);
    let started_at: DateTime<Utc> = report.started_at.into();
    StreamingQueryProgress {
        id: identity.id,
        run_id: identity.run_id,
        name: identity.name.map(str::to_string),
        timestamp: started_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        batch_id: report.epoch,
        batch_duration: trigger_execution,
        num_input_rows: report.num_input_rows,
        input_rows_per_second: input_rate,
        processed_rows_per_second: processed_rate,
        duration_ms: DurationMs {
            add_batch: millis(report.add_batch),
            commit_offsets: 0,
            get_batch: 0,
            latest_offset,
            query_planning: 0,
            trigger_execution,
            wal_commit: 0,
        },
        state_operators: Vec::new(),
        sources: vec![SourceProgress {
            description: source_description(identity.source),
            start_offset: report.start_offset.as_ref().and_then(offset_json),
            end_offset: report.end_offset.as_ref().and_then(offset_json),
            latest_offset: None,
            num_input_rows: report.num_input_rows,
            input_rows_per_second: input_rate,
            processed_rows_per_second: processed_rate,
        }],
        sink: SinkProgress {
            description: identity.sink.to_string(),
            num_output_rows: report.num_output_rows,
        },
    }
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
