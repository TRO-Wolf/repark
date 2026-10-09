use std::collections::BTreeMap;
use std::sync::Arc;

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::wrap_pyfunction;
use repark_core::microbatch::{MicroBatchError, relation};

use crate::dataframe::PyDataFrame;
use crate::exceptions::masked_message_params;
use crate::fence::fenced_span;
use crate::session::PyReparkSession;
use crate::streaming_errors::microbatch_py_err;

const CHECKPOINT_KEY: &str = "checkpointLocation";
const SINK_KEY: &str = "repark.cdc.sink";
const FANOUT_KEY: &str = "fanout-enabled";
const CATALOG_TIMEOUT_KEY: &str = "repark.cdc.catalog-timeout";
const STREAMING_PREFIX: &str = "streaming-";
const STREAM_PREFIX: &str = "stream-";
const REPARK_CDC_PREFIX: &str = "repark.cdc.";

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(load_stream, module)?)?;
    module.add_function(wrap_pyfunction!(is_streaming_frame, module)?)?;
    module.add_function(wrap_pyfunction!(start_stream, module)?)?;
    module.add_function(wrap_pyfunction!(to_table_stream, module)?)?;
    Ok(())
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

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_checkpoint(
    py: Python<'_>,
    options: &BTreeMap<String, String>,
    checkpoint_conf: Option<&str>,
) -> Result<(), PyErr> {
    let optioned = options
        .keys()
        .any(|key| key.eq_ignore_ascii_case(CHECKPOINT_KEY));
    if optioned || checkpoint_conf.is_some() {
        Ok(())
    } else {
        Err(microbatch_py_err(
            py,
            &MicroBatchError::CheckpointLocationMissing,
            None,
        ))
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_sink_declared(
    py: Python<'_>,
    options: &BTreeMap<String, String>,
) -> Result<(), PyErr> {
    let declared = options.keys().any(|key| key.eq_ignore_ascii_case(SINK_KEY));
    if declared {
        Ok(())
    } else {
        Err(microbatch_py_err(
            py,
            &MicroBatchError::SinkUndeclared,
            None,
        ))
    }
}

fn stub_terminal() -> PyErr {
    PyNotImplementedError::new_err(
        "MB-4 stub: the query validated and the streaming driver is not wired in this round",
    )
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
#[allow(
    clippy::missing_errors_doc,
    clippy::too_many_arguments,
    clippy::needless_pass_by_value
)]
pub fn start_stream(
    py: Python<'_>,
    session: &PyReparkSession,
    frame: &PyDataFrame,
    trigger_kind: &str,
    trigger_interval: Option<String>,
    options: BTreeMap<String, String>,
    checkpoint_conf: Option<String>,
    query_name: Option<String>,
    foreach: Option<Py<PyAny>>,
    path: Option<String>,
    partition_by: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let _ = (
        session,
        frame,
        trigger_kind,
        trigger_interval,
        query_name,
        path,
        partition_by,
    );
    check_checkpoint(py, &options, checkpoint_conf.as_deref())?;
    if foreach.is_some() {
        check_sink_declared(py, &options)?;
    }
    validate_writer_options(py, &options)?;
    Err(stub_terminal())
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
    checkpoint_conf: Option<String>,
    query_name: Option<String>,
    partition_by: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let _ = (
        session,
        frame,
        table,
        trigger_kind,
        trigger_interval,
        query_name,
        partition_by,
    );
    check_checkpoint(py, &options, checkpoint_conf.as_deref())?;
    validate_writer_options(py, &options)?;
    Err(stub_terminal())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

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
            let refused = check_checkpoint(py, &options(&[]), None)
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
        });
    }

    #[test]
    fn checkpoint_option_or_conf_passes() {
        Python::attach(|py| {
            check_checkpoint(py, &options(&[("CheckpointLocation", "/tmp/x")]), None)
                .expect("the option passes");
            check_checkpoint(py, &options(&[("checkpointLocation", "")]), None)
                .expect("an empty option counts as specified");
            check_checkpoint(py, &options(&[]), Some("/tmp/x")).expect("the session conf passes");
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

    #[test]
    fn start_stream_orders_checkpoint_before_sink_before_unknowns() {
        Python::attach(|py| {
            let session = door_session(py);
            let frame = session.sql(py, "SELECT 1 AS id").expect("a frame");
            let start = |options: BTreeMap<String, String>, foreach: bool| {
                start_stream(
                    py,
                    &session,
                    &frame,
                    "availableNow",
                    None,
                    options,
                    None,
                    None,
                    foreach.then(|| py.None().into_any()),
                    None,
                    Vec::new(),
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
            let terminal = start(
                options(&[
                    ("checkpointLocation", "/tmp/x"),
                    (SINK_KEY, "ice.sales.silver"),
                ]),
                true,
            )
            .expect_err("a valid start stops at the stub");
            assert!(terminal.is_instance_of::<PyNotImplementedError>(py));
            let terminal = start(options(&[("checkpointLocation", "/tmp/x")]), false)
                .expect_err("a start without a body needs no sink");
            assert!(terminal.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    #[test]
    fn to_table_stream_validates_then_stops_at_the_stub() {
        Python::attach(|py| {
            let session = door_session(py);
            let frame = session.sql(py, "SELECT 1 AS id").expect("a frame");
            let refused = to_table_stream(
                py,
                &session,
                &frame,
                "ice.sales.silver",
                "availableNow",
                None,
                options(&[]),
                None,
                None,
                Vec::new(),
            )
            .expect_err("the door validates");
            assert_eq!(condition(&refused, py), "_LEGACY_ERROR_TEMP_1298");
            let refused = to_table_stream(
                py,
                &session,
                &frame,
                "ice.sales.silver",
                "availableNow",
                None,
                options(&[("checkpointLocation", "/tmp/x"), ("streaming-zzz", "1")]),
                None,
                None,
                Vec::new(),
            )
            .expect_err("unknown writer options refuse");
            assert_eq!(condition(&refused, py), "REPARK_MICROBATCH.UNKNOWN_OPTION");
            let terminal = to_table_stream(
                py,
                &session,
                &frame,
                "ice.sales.silver",
                "availableNow",
                None,
                options(&[("checkpointLocation", "/tmp/x")]),
                None,
                None,
                Vec::new(),
            )
            .expect_err("a valid start stops at the stub");
            assert!(terminal.is_instance_of::<PyNotImplementedError>(py));
        });
    }
}
