use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::wrap_pyfunction;
use repark_core::microbatch::driver::{
    DEFAULT_CATALOG_TIMEOUT, DEFAULT_POLLING_DELAY, RecordedLocation, SinkSpec, StreamSpec,
    StreamingQueryManager, Trigger,
};
use repark_core::microbatch::progress::DEFAULT_RECENT_PROGRESS;
use repark_core::microbatch::{MicroBatchError, relation};
use repark_core::time_travel::microbatch_source::SourceOptions;

use crate::dataframe::PyDataFrame;
use crate::exceptions::{IllegalArgumentException, mask_user_visible, masked_message_params};
use crate::fence::fenced_span;
use crate::session::PyReparkSession;
use crate::streaming_errors::microbatch_py_err;
use crate::streaming_query::{
    BatchBodyAdapter, PyStreamingQuery, note_started, session_allows_local_catalog_for_tests,
};
use crate::trigger_interval::check_trigger_interval;

const CHECKPOINT_KEY: &str = "checkpointLocation";
const SINK_KEY: &str = "repark.cdc.sink";
const FANOUT_KEY: &str = "fanout-enabled";
const CATALOG_TIMEOUT_KEY: &str = "repark.cdc.catalog-timeout";
const PATH_KEY: &str = "path";
const STREAMING_PREFIX: &str = "streaming-";
const STREAM_PREFIX: &str = "stream-";
const REPARK_CDC_PREFIX: &str = "repark.cdc.";
const CONF_CHECKPOINT: &str = "spark.sql.streaming.checkpointLocation";
const CONF_STOP_TIMEOUT: &str = "spark.sql.streaming.stopTimeout";
const CONF_POLLING_DELAY: &str = "spark.sql.streaming.pollingDelay";
const CONF_RECENT_LIMIT: &str = "spark.sql.streaming.numRecentProgressUpdates";
const OUTPUT_MODE_CONDITION: &str = "STREAMING_OUTPUT_MODE.INVALID";
const INVALID_CONF_VALUE: &str = "INVALID_CONF_VALUE.TYPE_MISMATCH";
const EMPTY_CHECKPOINT_TEXT: &str = "Can not create a Path from an empty string";
const NO_PATH_TEXT: &str = "Cannot open table: path is not set";

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(load_stream, module)?)?;
    module.add_function(wrap_pyfunction!(is_streaming_frame, module)?)?;
    module.add_function(wrap_pyfunction!(check_output_mode, module)?)?;
    module.add_function(wrap_pyfunction!(start_stream, module)?)?;
    module.add_function(wrap_pyfunction!(to_table_stream, module)?)?;
    module.add_function(wrap_pyfunction!(
        _streaming_tests_allow_local_catalog,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(check_stream_format, module)?)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoorKind {
    Table,
    ForeachBatch,
}

pub(crate) fn attached(
    py: Python<'_>,
    raised: PyErr,
    condition: &str,
    params: &[(&str, &str)],
) -> PyErr {
    let value = raised.value(py);
    let held = masked_message_params(py, params);
    if let Err(failure) = value.setattr("_spark_error_class", condition) {
        tracing::warn!(error = %failure, "streaming refusal condition setattr failed");
    }
    if let Err(failure) = value.setattr("_spark_message_parameters", held) {
        tracing::warn!(error = %failure, "streaming refusal params setattr failed");
    }
    raised
}

fn has_interpreted_prefix(folded: &str) -> bool {
    folded.starts_with(STREAMING_PREFIX)
        || folded.starts_with(STREAM_PREFIX)
        || folded.starts_with(REPARK_CDC_PREFIX)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn validate_writer_options(
    py: Python<'_>,
    options: &BTreeMap<String, String>,
) -> Result<(), PyErr> {
    for key in options.keys() {
        let known = key.eq_ignore_ascii_case(CHECKPOINT_KEY)
            || key.eq_ignore_ascii_case(SINK_KEY)
            || key.eq_ignore_ascii_case(FANOUT_KEY)
            || key.eq_ignore_ascii_case(CATALOG_TIMEOUT_KEY);
        if !known && has_interpreted_prefix(&key.to_ascii_lowercase()) {
            return Err(microbatch_py_err(
                py,
                &MicroBatchError::UnknownOption { key: key.clone() },
                None,
            ));
        }
    }
    Ok(())
}

fn writer_option<'a>(options: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    options
        .iter()
        .find_map(|(known, value)| known.eq_ignore_ascii_case(key).then_some(value.as_str()))
}

fn catalog_refusal(py: Python<'_>, text: String) -> PyErr {
    microbatch_py_err(py, &MicroBatchError::Catalog(text), None)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_checkpoint(
    py: Python<'_>,
    options: &BTreeMap<String, String>,
    streaming_confs: &BTreeMap<String, String>,
    door: DoorKind,
) -> Result<Option<String>, PyErr> {
    let value = writer_option(options, CHECKPOINT_KEY)
        .or_else(|| writer_option(streaming_confs, CONF_CHECKPOINT))
        .map(str::to_string);
    match value {
        Some(location) if location.is_empty() => {
            Err(IllegalArgumentException::new_err(EMPTY_CHECKPOINT_TEXT))
        }
        Some(location) => Ok(Some(location)),
        None => match door {
            DoorKind::Table => Err(microbatch_py_err(
                py,
                &MicroBatchError::CheckpointLocationMissing,
                None,
            )),
            #[allow(clippy::match_same_arms)]
            DoorKind::ForeachBatch => Err(microbatch_py_err(
                py,
                &MicroBatchError::CheckpointLocationMissing,
                None,
            )),
        },
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_output_mode_value(py: Python<'_>, mode: &str) -> Result<(), PyErr> {
    if matches!(
        mode.to_ascii_lowercase().as_str(),
        "append" | "complete" | "update"
    ) {
        return Ok(());
    }
    Err(attached(
        py,
        IllegalArgumentException::new_err(mask_user_visible(format!(
            "[{OUTPUT_MODE_CONDITION}] Invalid streaming output mode: {mode}. Accepted output \
             modes are 'Append', 'Complete', 'Update'. SQLSTATE: 42KDE"
        ))),
        OUTPUT_MODE_CONDITION,
        &[("outputMode", mode)],
    ))
}

#[pyfunction]
#[allow(clippy::missing_errors_doc)]
pub fn check_output_mode(py: Python<'_>, mode: &str) -> PyResult<()> {
    check_output_mode_value(py, mode)
}

fn stream_format_feature(door: &str, format: Option<&str>) -> Option<String> {
    match format {
        None => Some(format!("{door}.format(parquet)")),
        Some(source) if source.to_lowercase() != "iceberg" => {
            let shown: String = source.chars().take(64).collect();
            Some(format!("{door}.format({shown})"))
        }
        Some(_) => None,
    }
}

#[pyfunction]
#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
fn check_stream_format(py: Python<'_>, door: &str, format: Option<String>) -> PyResult<()> {
    if let Some(feature) = stream_format_feature(door, format.as_deref()) {
        return Err(microbatch_py_err(
            py,
            &MicroBatchError::FeatureRefused { feature },
            None,
        ));
    }
    Ok(())
}

fn time_conf_micros(text: &str) -> Option<u128> {
    let body = text.trim().to_ascii_lowercase();
    let digits = body.strip_prefix('-').unwrap_or(&body);
    let split = digits
        .find(|cell: char| cell.is_ascii_alphabetic())
        .unwrap_or(digits.len());
    let (count_text, suffix) = digits.split_at(split);
    if count_text.is_empty() || !count_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let factor: u128 = match suffix {
        "" | "ms" => 1_000,
        "us" => 1,
        "s" => 1_000_000,
        "m" | "min" => 60_000_000,
        "h" => 3_600_000_000,
        "d" => 86_400_000_000,
        _ => return None,
    };
    count_text.parse::<u128>().ok()?.checked_mul(factor)
}

pub(crate) fn parse_time_conf(text: &str) -> Option<Duration> {
    let negative = text.trim().starts_with('-');
    let micros = time_conf_micros(text)?;
    if negative {
        return Some(Duration::ZERO);
    }
    u64::try_from(micros).ok().map(Duration::from_micros)
}

fn invalid_conf_value(py: Python<'_>, key: &str, value: &str, conf_type: &str) -> PyErr {
    attached(
        py,
        IllegalArgumentException::new_err(mask_user_visible(format!(
            "[{INVALID_CONF_VALUE}] The value '{value}' in the config \"{key}\" is invalid. It \
             should be a/an '{conf_type}' value. SQLSTATE: 22022"
        ))),
        INVALID_CONF_VALUE,
        &[
            ("confName", key),
            ("confValue", value),
            ("confType", conf_type),
        ],
    )
}

#[derive(Debug)]
struct StreamConfs {
    stop_timeout: Option<Duration>,
    polling_delay: Duration,
    recent_limit: usize,
}

#[allow(clippy::missing_errors_doc)]
fn parse_streaming_confs(
    py: Python<'_>,
    streaming_confs: &BTreeMap<String, String>,
) -> Result<StreamConfs, PyErr> {
    let stop_timeout = match writer_option(streaming_confs, CONF_STOP_TIMEOUT) {
        None => None,
        Some(text) => Some(parse_time_conf(text).ok_or_else(|| {
            invalid_conf_value(py, CONF_STOP_TIMEOUT, text, "time in MILLISECONDS")
        })?),
    };
    let polling_delay = match writer_option(streaming_confs, CONF_POLLING_DELAY) {
        None => DEFAULT_POLLING_DELAY,
        Some(text) => parse_time_conf(text).ok_or_else(|| {
            invalid_conf_value(py, CONF_POLLING_DELAY, text, "time in MILLISECONDS")
        })?,
    };
    let recent_limit = match writer_option(streaming_confs, CONF_RECENT_LIMIT) {
        None => DEFAULT_RECENT_PROGRESS,
        Some(text) => match text.parse::<i32>() {
            Ok(count) if count > 0 => usize::try_from(count).unwrap_or(usize::MAX),
            Ok(_) => 1,
            Err(_) => {
                return Err(invalid_conf_value(py, CONF_RECENT_LIMIT, text, "int"));
            }
        },
    };
    Ok(StreamConfs {
        stop_timeout,
        polling_delay,
        recent_limit,
    })
}

#[allow(clippy::missing_errors_doc)]
fn build_trigger(py: Python<'_>, kind: &str, interval: Option<&str>) -> Result<Trigger, PyErr> {
    match kind {
        "default" => Ok(Trigger::default()),
        "processingTime" => {
            let text = interval.ok_or_else(|| {
                catalog_refusal(
                    py,
                    String::from("a processingTime trigger needs an interval string"),
                )
            })?;
            check_trigger_interval(text)
                .map(|millis| Trigger::ProcessingTime(Duration::from_millis(millis)))
        }
        "once" => Ok(Trigger::Once),
        "availableNow" => Ok(Trigger::AvailableNow),
        "continuous" => Err(microbatch_py_err(
            py,
            &MicroBatchError::FeatureRefused {
                feature: String::from("trigger(continuous)"),
            },
            None,
        )),
        _ => Err(catalog_refusal(py, format!("unknown trigger kind {kind}"))),
    }
}

#[allow(clippy::missing_errors_doc)]
fn check_start_mode(py: Python<'_>, output_mode: Option<&str>) -> Result<(), PyErr> {
    if let Some(mode) = output_mode {
        relation::check_output_mode(mode).map_err(|error| microbatch_py_err(py, &error, None))?;
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
fn build_stream_spec(
    py: Python<'_>,
    frame: &PyDataFrame,
    sink: SinkSpec,
    trigger_kind: &str,
    trigger_interval: Option<&str>,
    options: &BTreeMap<String, String>,
    streaming_confs: &BTreeMap<String, String>,
    query_name: Option<String>,
    checkpoint: Option<String>,
) -> Result<StreamSpec, PyErr> {
    let trigger = build_trigger(py, trigger_kind, trigger_interval)?;
    let template = relation::PlanTemplate::from_frame(frame.inner())
        .map_err(|error| microbatch_py_err(py, &error, None))?;
    let source_options = SourceOptions::from_options(template.source_options())
        .map_err(|error| microbatch_py_err(py, &error, None))?;
    let confs = parse_streaming_confs(py, streaming_confs)?;
    let catalog_timeout = match writer_option(options, CATALOG_TIMEOUT_KEY) {
        None => DEFAULT_CATALOG_TIMEOUT,
        Some(text) => check_trigger_interval(text).map(Duration::from_millis)?,
    };
    let mut spec = StreamSpec::new(template.source(), source_options, sink);
    spec.plan = Some(template);
    spec.trigger = trigger;
    spec.query_name = query_name;
    spec.checkpoint_location = checkpoint.map(RecordedLocation::new);
    spec.stop_timeout = confs.stop_timeout;
    spec.polling_delay = confs.polling_delay;
    spec.catalog_timeout = catalog_timeout;
    spec.recent_progress_limit = confs.recent_limit;
    Ok(spec)
}

#[allow(clippy::missing_errors_doc)]
fn start_spec(
    py: Python<'_>,
    session: &PyReparkSession,
    spec: StreamSpec,
) -> Result<PyStreamingQuery, PyErr> {
    let manager = StreamingQueryManager::of(&session.session);
    if let Some(name) = &spec.query_name
        && manager
            .active()
            .iter()
            .any(|query| query.name().as_deref() == Some(name.as_str()))
    {
        return Err(IllegalArgumentException::new_err(format!(
            "Cannot start query with name {name} as a query with that name is already active \
             in this SparkSession"
        )));
    }
    let runtime = Arc::clone(&session.runtime);
    let handle = py.detach(|| runtime.block_on(manager.register(&session.session, spec)));
    let handle = handle.map_err(|error| microbatch_py_err(py, &error, None))?;
    let below = session_allows_local_catalog_for_tests(session);
    let started = py.detach(|| {
        runtime.block_on(async {
            if below {
                handle.start_below_catalog_check()
            } else {
                handle.start().await
            }
        })
    });
    started.map_err(|error| microbatch_py_err(py, &error, None))?;
    note_started(session, &handle);
    Ok(PyStreamingQuery::new(handle, runtime))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_sink_declared<'a>(
    py: Python<'_>,
    options: &'a BTreeMap<String, String>,
) -> Result<&'a str, PyErr> {
    match writer_option(options, SINK_KEY) {
        Some(sink) => Ok(sink),
        None => Err(microbatch_py_err(
            py,
            &MicroBatchError::SinkUndeclared,
            None,
        )),
    }
}

#[pyfunction]
#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
pub fn load_stream(
    py: Python<'_>,
    session: &PyReparkSession,
    source: &str,
    options: BTreeMap<String, String>,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.read", "load_stream", {
        let opened = py.detach(|| {
            session.runtime.block_on(relation::streaming_frame(
                &session.session,
                source,
                &options,
            ))
        });
        match opened {
            Ok(frame) => Ok(PyDataFrame::new(frame, Arc::clone(&session.runtime))),
            Err(error) => Err(microbatch_py_err(py, &error, None)),
        }
    })
}

#[pyfunction]
pub fn is_streaming_frame(frame: &PyDataFrame) -> bool {
    relation::is_streaming_frame(frame.inner())
}

#[pyfunction]
fn _streaming_tests_allow_local_catalog(session: &PyReparkSession) {
    crate::streaming_query::mark_session_allowing_local_catalog_for_tests(session);
}

#[pyfunction]
#[allow(
    clippy::missing_errors_doc,
    clippy::too_many_arguments,
    clippy::needless_pass_by_value
)]
pub fn start_stream(
    py: Python<'_>,
    session: Py<PyReparkSession>,
    frame: &PyDataFrame,
    trigger_kind: &str,
    trigger_interval: Option<String>,
    options: BTreeMap<String, String>,
    streaming_confs: BTreeMap<String, String>,
    query_name: Option<String>,
    foreach: Option<Py<PyAny>>,
    path: Option<String>,
    partition_by: Vec<String>,
    output_mode: Option<String>,
    alive_token: Py<PyAny>,
) -> PyResult<PyStreamingQuery> {
    let _ = partition_by;
    check_start_mode(py, output_mode.as_deref())?;
    let bound = session.bind(py);
    let inner = bound.borrow();
    if let Some(body) = foreach {
        let checkpoint = check_checkpoint(py, &options, &streaming_confs, DoorKind::ForeachBatch)?;
        let sink = check_sink_declared(py, &options)?.to_string();
        validate_writer_options(py, &options)?;
        let dataframe_class = py
            .import("repark.spark.dataframe")?
            .getattr("DataFrame")?
            .unbind();
        let adapter = BatchBodyAdapter::new(
            body,
            session.clone_ref(py),
            alive_token,
            dataframe_class,
            Arc::clone(&inner.runtime),
        );
        let spec = build_stream_spec(
            py,
            frame,
            SinkSpec::ForeachBatch {
                sink,
                body: Arc::new(adapter),
            },
            trigger_kind,
            trigger_interval.as_deref(),
            &options,
            &streaming_confs,
            query_name,
            checkpoint,
        )?;
        return start_spec(py, &inner, spec);
    }
    let sink = match path {
        Some(given) => given,
        None => match writer_option(&options, PATH_KEY) {
            Some(found) if !found.is_empty() => found.to_string(),
            _ => return Err(IllegalArgumentException::new_err(NO_PATH_TEXT)),
        },
    };
    let checkpoint = check_checkpoint(py, &options, &streaming_confs, DoorKind::Table)?;
    validate_writer_options(py, &options)?;
    let spec = build_stream_spec(
        py,
        frame,
        SinkSpec::Table { sink },
        trigger_kind,
        trigger_interval.as_deref(),
        &options,
        &streaming_confs,
        query_name,
        checkpoint,
    )?;
    start_spec(py, &inner, spec)
}

#[pyfunction]
#[allow(
    clippy::missing_errors_doc,
    clippy::too_many_arguments,
    clippy::needless_pass_by_value
)]
pub fn to_table_stream(
    py: Python<'_>,
    session: &PyReparkSession,
    frame: &PyDataFrame,
    table: &str,
    trigger_kind: &str,
    trigger_interval: Option<String>,
    options: BTreeMap<String, String>,
    streaming_confs: BTreeMap<String, String>,
    query_name: Option<String>,
    partition_by: Vec<String>,
    output_mode: Option<String>,
) -> PyResult<PyStreamingQuery> {
    let _ = partition_by;
    check_start_mode(py, output_mode.as_deref())?;
    let checkpoint = check_checkpoint(py, &options, &streaming_confs, DoorKind::Table)?;
    if let Some(declared) = writer_option(&options, SINK_KEY)
        && declared != table
    {
        return Err(IllegalArgumentException::new_err(mask_user_visible(
            format!(
                "option \"repark.cdc.sink\" names \"{declared}\" but toTable names \"{table}\"; pass \
             one sink"
            ),
        )));
    }
    validate_writer_options(py, &options)?;
    let spec = build_stream_spec(
        py,
        frame,
        SinkSpec::Table {
            sink: table.to_string(),
        },
        trigger_kind,
        trigger_interval.as_deref(),
        &options,
        &streaming_confs,
        query_name,
        checkpoint,
    )?;
    start_spec(py, session, spec)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule};

    use super::*;
    use crate::exceptions::{
        AnalysisException, IllegalArgumentException, PySparkException, RecoveryRequiredException,
        StreamingQueryException,
    };

    fn options(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn message(error: &PyErr, py: Python<'_>) -> String {
        error.value(py).to_string()
    }

    fn condition(error: &PyErr, py: Python<'_>) -> String {
        error
            .value(py)
            .getattr("_spark_error_class")
            .expect("condition attached")
            .extract::<String>()
            .expect("condition is str")
    }

    fn params(error: &PyErr, py: Python<'_>) -> BTreeMap<String, String> {
        error
            .value(py)
            .getattr("_spark_message_parameters")
            .expect("params attached")
            .extract::<Bound<'_, PyDict>>()
            .expect("params are a dict")
            .iter()
            .map(|(key, value)| {
                (
                    key.extract::<String>().expect("param key"),
                    value.extract::<String>().expect("param value"),
                )
            })
            .collect()
    }

    #[test]
    fn stream_format_names_the_refused_feature() {
        assert_eq!(
            stream_format_feature("readStream", None),
            Some(String::from("readStream.format(parquet)"))
        );
        assert_eq!(
            stream_format_feature("writeStream", None),
            Some(String::from("writeStream.format(parquet)"))
        );
        for folded in ["iceberg", "Iceberg", "ICEBERG"] {
            assert_eq!(stream_format_feature("readStream", Some(folded)), None);
            assert_eq!(stream_format_feature("writeStream", Some(folded)), None);
        }
        assert_eq!(
            stream_format_feature("readStream", Some("parquet")),
            Some(String::from("readStream.format(parquet)"))
        );
        let long = "x".repeat(80);
        let shown = "x".repeat(64);
        assert_eq!(
            stream_format_feature("writeStream", Some(&long)),
            Some(format!("writeStream.format({shown})"))
        );
    }

    #[test]
    fn known_writer_options_pass() {
        Python::attach(|py| {
            validate_writer_options(
                py,
                &options(&[
                    ("CHECKPOINTLOCATION", "/tmp/x"),
                    (SINK_KEY, "ice.sales.silver"),
                    (FANOUT_KEY, "true"),
                    (CATALOG_TIMEOUT_KEY, "60s"),
                    ("unrelated", "anything"),
                ]),
            )
            .expect("known and plain writer keys pass");
        });
    }

    #[test]
    fn unknown_prefixed_writer_options_refuse_with_unknown_option() {
        Python::attach(|py| {
            let refused = validate_writer_options(py, &options(&[("streaming-zzz", "1")]))
                .expect_err("a prefixed writer unknown refuses");
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(condition(&refused, py), "REPARK_MICROBATCH.UNKNOWN_OPTION");
            assert_eq!(
                params(&refused, py),
                BTreeMap::from([("key".to_string(), "streaming-zzz".to_string())])
            );
        });
    }

    #[test]
    fn missing_checkpoint_refuses_with_the_w8_text() {
        Python::attach(|py| {
            for door in [DoorKind::Table, DoorKind::ForeachBatch] {
                let refused = check_checkpoint(py, &options(&[]), &options(&[]), door)
                    .expect_err("a missing checkpoint refuses");
                assert!(refused.is_instance_of::<AnalysisException>(py));
                assert!(refused.is_instance_of::<PySparkException>(py));
                assert_eq!(
                    message(&refused, py),
                    "checkpointLocation must be specified either through \
                     option(\"checkpointLocation\", ...) or SparkSession.conf.set(\
                     \"spark.sql.streaming.checkpointLocation\", ...)."
                );
                assert_eq!(condition(&refused, py), "_LEGACY_ERROR_TEMP_1298");
                assert!(params(&refused, py).is_empty());
                assert!(!message(&refused, py).contains("SQLSTATE"));
            }
        });
    }

    #[test]
    fn checkpoint_option_or_conf_passes() {
        Python::attach(|py| {
            assert_eq!(
                check_checkpoint(
                    py,
                    &options(&[("CheckpointLocation", "/tmp/x")]),
                    &options(&[]),
                    DoorKind::Table,
                )
                .expect("the option passes"),
                Some(String::from("/tmp/x"))
            );
            assert_eq!(
                check_checkpoint(
                    py,
                    &options(&[]),
                    &options(&[(CONF_CHECKPOINT, "/tmp/conf")]),
                    DoorKind::Table,
                )
                .expect("the session conf passes"),
                Some(String::from("/tmp/conf"))
            );
            assert_eq!(
                check_checkpoint(
                    py,
                    &options(&[("checkpointLocation", "/tmp/opt")]),
                    &options(&[(CONF_CHECKPOINT, "/tmp/conf")]),
                    DoorKind::Table,
                )
                .expect("the option wins over the conf"),
                Some(String::from("/tmp/opt"))
            );
        });
    }

    #[test]
    fn empty_checkpoint_refuses_with_sparks_path_text() {
        Python::attach(|py| {
            for (options, confs) in [
                (options(&[("checkpointLocation", "")]), options(&[])),
                (
                    options(&[("checkpointLocation", "")]),
                    options(&[(CONF_CHECKPOINT, "/tmp/conf")]),
                ),
                (options(&[]), options(&[(CONF_CHECKPOINT, "")])),
            ] {
                let refused = check_checkpoint(py, &options, &confs, DoorKind::Table)
                    .expect_err("empty refuses");
                assert!(refused.is_instance_of::<IllegalArgumentException>(py));
                assert_eq!(message(&refused, py), EMPTY_CHECKPOINT_TEXT);
            }
            assert_eq!(
                check_checkpoint(
                    py,
                    &options(&[("checkpointLocation", "/tmp/opt")]),
                    &options(&[(CONF_CHECKPOINT, "")]),
                    DoorKind::Table,
                )
                .expect("a set option wins over an empty conf"),
                Some(String::from("/tmp/opt"))
            );
        });
    }

    #[test]
    fn undeclared_foreach_sink_refuses_with_sink_undeclared() {
        Python::attach(|py| {
            let refused =
                check_sink_declared(py, &options(&[])).expect_err("a missing sink refuses");
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(
                message(&refused, py),
                "[REPARK_MICROBATCH.SINK_UNDECLARED] no sink declared for this streaming query; \
                 pass .option(\"repark.cdc.sink\", \"<table>\")"
            );
            assert_eq!(condition(&refused, py), "REPARK_MICROBATCH.SINK_UNDECLARED");
            assert!(params(&refused, py).is_empty());
            assert!(!message(&refused, py).contains("SQLSTATE"));
            check_sink_declared(py, &options(&[("Repark.CDC.Sink", "ice.sales.silver")]))
                .expect("a declared sink passes");
        });
    }

    #[test]
    fn streaming_exceptions_shape_the_hierarchy_and_carry_recovery_fields() {
        Python::attach(|py| {
            let failed = StreamingQueryException::new_err("[STREAM_FAILED] boom");
            assert!(failed.is_instance_of::<PySparkException>(py));
            assert!(failed.is_instance_of::<PyRuntimeError>(py));
            let recovery = RecoveryRequiredException::new_err("recover me");
            assert!(recovery.is_instance_of::<PySparkException>(py));
            assert!(recovery.is_instance_of::<PyRuntimeError>(py));
            assert!(!recovery.is_instance_of::<StreamingQueryException>(py));
            assert!(!failed.is_instance_of::<RecoveryRequiredException>(py));
            let value = recovery.value(py);
            value.setattr("query_id", "q-1").expect("query_id attaches");
            value.setattr("epoch", 3).expect("epoch attaches");
            value
                .setattr("durable_offset", "offsets-3")
                .expect("durable_offset attaches");
            assert_eq!(
                value
                    .getattr("query_id")
                    .expect("query_id reads")
                    .extract::<String>()
                    .expect("query_id is str"),
                "q-1"
            );
        });
    }

    fn door_session(py: Python<'_>) -> PyReparkSession {
        PyReparkSession::new(py, None, None, None, None, None).expect("a session")
    }

    #[test]
    fn load_stream_validates_then_opens_through_the_mapper() {
        Python::attach(|py| {
            let session = door_session(py);
            let Err(refused) = load_stream(
                py,
                &session,
                "ice.sales.orders",
                options(&[("streaming-skip-delete-snapshots", "true")]),
            ) else {
                panic!("the door validates");
            };
            assert_eq!(
                condition(&refused, py),
                "REPARK_MICROBATCH.SKIP_OPTION_REFUSED"
            );
            let Err(missing) = load_stream(py, &session, "ice.sales.orders", options(&[])) else {
                panic!("an unregistered catalog refuses");
            };
            assert!(missing.is_instance_of::<AnalysisException>(py));
            assert!(message(&missing, py).contains("catalog \"ice\" is not registered"));
        });
    }

    fn stub_dataframe_module(py: Python<'_>) {
        let grandparent = PyModule::new(py, "repark").expect("a stub grandparent package");
        let parent = PyModule::new(py, "repark.spark").expect("a stub parent package");
        let module = PyModule::new(py, "repark.spark.dataframe").expect("a stub module");
        module
            .dict()
            .set_item("DataFrame", py.None())
            .expect("the stub class installs");
        let modules = py
            .import("sys")
            .expect("sys imports")
            .getattr("modules")
            .expect("sys.modules reads");
        modules
            .set_item("repark", grandparent)
            .expect("the grandparent installs");
        modules
            .set_item("repark.spark", parent)
            .expect("the parent installs");
        modules
            .set_item("repark.spark.dataframe", module)
            .expect("the stub installs");
    }

    #[test]
    fn start_stream_orders_checkpoint_before_sink_before_unknowns() {
        Python::attach(|py| {
            let session = Py::new(py, door_session(py)).expect("a session object");
            let frame = session
                .borrow(py)
                .sql(py, "SELECT 1 AS id")
                .expect("a frame");
            let start = |options: BTreeMap<String, String>, foreach: bool| {
                start_stream(
                    py,
                    session.clone_ref(py),
                    &frame,
                    "availableNow",
                    None,
                    options,
                    BTreeMap::new(),
                    None,
                    foreach.then(|| py.None().into_any()),
                    None,
                    Vec::new(),
                    None,
                    py.None().into_any(),
                )
            };
            let refused = start(options(&[]), true).expect_err("refuses");
            assert_eq!(condition(&refused, py), "_LEGACY_ERROR_TEMP_1298");
            let refused =
                start(options(&[("checkpointLocation", "/tmp/x")]), true).expect_err("refuses");
            assert_eq!(condition(&refused, py), "REPARK_MICROBATCH.SINK_UNDECLARED");
            let refused = start(
                options(&[
                    ("checkpointLocation", "/tmp/x"),
                    (SINK_KEY, "ice.sales.silver"),
                    ("streaming-zzz", "1"),
                ]),
                true,
            )
            .expect_err("refuses");
            assert_eq!(condition(&refused, py), "REPARK_MICROBATCH.UNKNOWN_OPTION");
            let refused = start(
                options(&[("checkpointLocation", "/tmp/x"), ("streaming-zzz", "1")]),
                true,
            )
            .expect_err("refuses");
            assert_eq!(condition(&refused, py), "REPARK_MICROBATCH.SINK_UNDECLARED");
            stub_dataframe_module(py);
            let refused = start(
                options(&[
                    ("checkpointLocation", "/tmp/x"),
                    (SINK_KEY, "ice.sales.silver"),
                ]),
                true,
            )
            .expect_err("a valid foreach start reaches the frame");
            assert!(refused.is_instance_of::<AnalysisException>(py));
            assert!(message(&refused, py).contains("needs a streaming DataFrame"));
            let refused = start(options(&[("checkpointLocation", "/tmp/x")]), false)
                .expect_err("a pathless start refuses");
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(message(&refused, py), NO_PATH_TEXT);
        });
    }

    #[test]
    fn to_table_stream_validates_then_reaches_the_frame() {
        Python::attach(|py| {
            let session = door_session(py);
            let frame = session.sql(py, "SELECT 1 AS id").expect("a frame");
            let start = |table: &str, options: BTreeMap<String, String>| {
                to_table_stream(
                    py,
                    &session,
                    &frame,
                    table,
                    "availableNow",
                    None,
                    options,
                    BTreeMap::new(),
                    None,
                    Vec::new(),
                    None,
                )
            };
            let refused = start("ice.sales.silver", options(&[])).expect_err("the door validates");
            assert_eq!(condition(&refused, py), "_LEGACY_ERROR_TEMP_1298");
            let refused = start(
                "ice.sales.silver",
                options(&[("checkpointLocation", "/tmp/x"), ("streaming-zzz", "1")]),
            )
            .expect_err("unknown writer options refuse");
            assert_eq!(condition(&refused, py), "REPARK_MICROBATCH.UNKNOWN_OPTION");
            let refused = start(
                "ice.sales.silver",
                options(&[
                    ("checkpointLocation", "/tmp/x"),
                    (SINK_KEY, "ice.sales.other"),
                ]),
            )
            .expect_err("a differing sink option refuses");
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(
                message(&refused, py),
                "option \"repark.cdc.sink\" names \"ice.sales.other\" but toTable names \
                 \"ice.sales.silver\"; pass one sink"
            );
            let refused = start(
                "ice.sales.silver",
                options(&[("checkpointLocation", "/tmp/x")]),
            )
            .expect_err("a batch frame refuses");
            assert!(refused.is_instance_of::<AnalysisException>(py));
            assert!(message(&refused, py).contains("needs a streaming DataFrame"));
        });
    }
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod spec_tests;
