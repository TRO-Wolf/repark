use datafusion::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use repark_core::microbatch::MicroBatchError;

use crate::exceptions::{
    AnalysisException, IllegalArgumentException, RecoveryRequiredException,
    StreamingQueryException, mask_user_visible,
};
use crate::streaming::attached;

const SKIP_OPTION_REFUSED: &str = "REPARK_MICROBATCH.SKIP_OPTION_REFUSED";
const UNKNOWN_OPTION: &str = "REPARK_MICROBATCH.UNKNOWN_OPTION";
const CHECKPOINT_MISSING: &str = "_LEGACY_ERROR_TEMP_1298";
const LOCAL_CATALOG_REFUSED: &str = "REPARK_MICROBATCH.LOCAL_CATALOG_REFUSED";
const KEYLESS_SOURCE: &str = "REPARK_MICROBATCH.KEYLESS_SOURCE";
const SINK_UNDECLARED: &str = "REPARK_MICROBATCH.SINK_UNDECLARED";
const MERGE_ISOLATION_REFUSED: &str = "REPARK_MICROBATCH.MERGE_ISOLATION_REFUSED";
const RECOVERY_REQUIRED: &str = "REPARK_MICROBATCH.RECOVERY_REQUIRED";
const STREAM_FAILED: &str = "STREAM_FAILED";
const NOT_IMPLEMENTED: &str = "NOT_IMPLEMENTED";

const FENCED: &str = "REPARK_MICROBATCH.FENCED";
const GENERATION_MISMATCH: &str = "REPARK_MICROBATCH.GENERATION_MISMATCH";
const INPUTS_CHANGED: &str = "REPARK_MICROBATCH.INPUTS_CHANGED";
const SOURCE_REPLACED: &str = "REPARK_MICROBATCH.SOURCE_REPLACED";
const SOURCE_SNAPSHOT_EXPIRED: &str = "REPARK_MICROBATCH.SOURCE_SNAPSHOT_EXPIRED";
const TRUNCATED_HISTORY: &str = "REPARK_MICROBATCH.TRUNCATED_HISTORY";
const UNSUPPORTED_OFFSET_FORMAT: &str = "REPARK_MICROBATCH.UNSUPPORTED_OFFSET_FORMAT";
const SINK_COMMITTED_TWICE: &str = "REPARK_MICROBATCH.SINK_COMMITTED_TWICE";
const SINK_BUSY: &str = "REPARK_MICROBATCH.SINK_BUSY";
const UNSTAMPED_SINK_WRITE: &str = "REPARK_MICROBATCH.UNSTAMPED_SINK_WRITE";
const STREAMING_ACTION_REFUSED: &str = "_LEGACY_ERROR_TEMP_3102";

const STREAMING_ACTION_REFUSED_TEXT: &str = "Queries with streaming sources must be executed with writeStream.start(), or from a streaming table or flow definition within a Spark Declarative Pipeline.;\niceberg";

#[derive(Debug, Clone, Copy)]
pub(crate) struct QueryHead<'a> {
    pub(crate) query_id: &'a str,
    pub(crate) run_id: &'a str,
}

fn stream_failed(py: Python<'_>, cause: &str, head: Option<QueryHead<'_>>) -> PyErr {
    let text = match head {
        Some(head) => format!(
            "[{STREAM_FAILED}] Query [id = {query}, runId = {run}] terminated with exception: {cause} SQLSTATE: XXKST",
            query = head.query_id,
            run = head.run_id,
        ),
        None => format!("[{STREAM_FAILED}] {cause} SQLSTATE: XXKST"),
    };
    attached(
        py,
        StreamingQueryException::new_err(mask_user_visible(text)),
        STREAM_FAILED,
        &[],
    )
}

fn not_implemented(py: Python<'_>, feature: &str) -> PyErr {
    let masked = mask_user_visible(feature);
    let message = format!("[{NOT_IMPLEMENTED}] {masked} is not implemented.");
    let build = || -> PyResult<PyErr> {
        let class = py
            .import("repark.errors")?
            .getattr("PySparkNotImplementedError")?;
        let params = PyDict::new(py);
        params.set_item("feature", masked)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("errorClass", NOT_IMPLEMENTED)?;
        kwargs.set_item("messageParameters", params)?;
        Ok(PyErr::from_value(class.call((message,), Some(&kwargs))?))
    };
    match build() {
        Ok(raised) => raised,
        Err(failure) => failure,
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn microbatch_py_err(
    py: Python<'_>,
    error: &MicroBatchError,
    head: Option<QueryHead<'_>>,
) -> PyErr {
    match error {
        MicroBatchError::SkipOptionRefused { option } => attached(
            py,
            IllegalArgumentException::new_err(mask_user_visible(format!(
                "[{SKIP_OPTION_REFUSED}] {error}"
            ))),
            SKIP_OPTION_REFUSED,
            &[("option", option)],
        ),
        MicroBatchError::UnknownOption { key } => attached(
            py,
            IllegalArgumentException::new_err(mask_user_visible(format!(
                "[{UNKNOWN_OPTION}] {error}"
            ))),
            UNKNOWN_OPTION,
            &[("key", key.as_str())],
        ),
        MicroBatchError::CheckpointLocationMissing => attached(
            py,
            AnalysisException::new_err(error.to_string()),
            CHECKPOINT_MISSING,
            &[],
        ),
        MicroBatchError::OutputModeRefused { mode } => {
            not_implemented(py, &format!("outputMode({mode})"))
        }
        MicroBatchError::StatefulOperatorRefused { operator } => {
            not_implemented(py, &format!("{operator} on a streaming DataFrame"))
        }
        MicroBatchError::FeatureRefused { feature } => not_implemented(py, feature),
        MicroBatchError::LocalCatalogRefused { .. } => attached(
            py,
            AnalysisException::new_err(mask_user_visible(format!(
                "[{LOCAL_CATALOG_REFUSED}] {error}"
            ))),
            LOCAL_CATALOG_REFUSED,
            &[],
        ),
        MicroBatchError::KeylessSource { .. } => attached(
            py,
            AnalysisException::new_err(mask_user_visible(format!("[{KEYLESS_SOURCE}] {error}"))),
            KEYLESS_SOURCE,
            &[],
        ),
        MicroBatchError::SinkUndeclared => attached(
            py,
            IllegalArgumentException::new_err(mask_user_visible(format!(
                "[{SINK_UNDECLARED}] {error}"
            ))),
            SINK_UNDECLARED,
            &[],
        ),
        MicroBatchError::MergeIsolationRefused { .. } => attached(
            py,
            AnalysisException::new_err(mask_user_visible(format!(
                "[{MERGE_ISOLATION_REFUSED}] {error}"
            ))),
            MERGE_ISOLATION_REFUSED,
            &[],
        ),
        MicroBatchError::NonAppendSnapshot { .. } | MicroBatchError::BatchFailed { .. } => {
            stream_failed(py, &error.to_string(), head)
        }
        MicroBatchError::Fenced { .. } => stream_failed(py, &format!("[{FENCED}] {error}"), head),
        MicroBatchError::GenerationMismatch { .. } => {
            stream_failed(py, &format!("[{GENERATION_MISMATCH}] {error}"), head)
        }
        MicroBatchError::InputsChanged { .. } => {
            stream_failed(py, &format!("[{INPUTS_CHANGED}] {error}"), head)
        }
        MicroBatchError::SourceReplaced { .. } => {
            stream_failed(py, &format!("[{SOURCE_REPLACED}] {error}"), head)
        }
        MicroBatchError::SourceSnapshotExpired { .. } => {
            stream_failed(py, &format!("[{SOURCE_SNAPSHOT_EXPIRED}] {error}"), head)
        }
        MicroBatchError::TruncatedHistory { .. } => {
            stream_failed(py, &format!("[{TRUNCATED_HISTORY}] {error}"), head)
        }
        MicroBatchError::UnsupportedOffsetFormat { .. } => {
            stream_failed(py, &format!("[{UNSUPPORTED_OFFSET_FORMAT}] {error}"), head)
        }
        MicroBatchError::SinkCommittedTwice { .. } => {
            stream_failed(py, &format!("[{SINK_COMMITTED_TWICE}] {error}"), head)
        }
        MicroBatchError::SinkBusy { .. } => {
            stream_failed(py, &format!("[{SINK_BUSY}] {error}"), head)
        }
        MicroBatchError::UnstampedSinkWrite { .. } => {
            stream_failed(py, &format!("[{UNSTAMPED_SINK_WRITE}] {error}"), head)
        }
        MicroBatchError::AlreadyCommitted { .. }
        | MicroBatchError::AwaitFromDriver { .. }
        | MicroBatchError::DriverPanicked { .. } => stream_failed(py, &error.to_string(), head),
        #[allow(clippy::match_same_arms)]
        MicroBatchError::Catalog(_)
        | MicroBatchError::CatalogTimeout { .. }
        | MicroBatchError::SnapshotNotInLineage { .. }
        | MicroBatchError::OffsetPositionOutOfRange { .. } => match head {
            Some(_) => stream_failed(py, &error.to_string(), head),
            None => AnalysisException::new_err(mask_user_visible(error.to_string())),
        },
        MicroBatchError::RecoveryRequired {
            query,
            epoch,
            durable,
            ..
        } => {
            let raised = attached(
                py,
                RecoveryRequiredException::new_err(mask_user_visible(format!(
                    "[{RECOVERY_REQUIRED}] {error}"
                ))),
                RECOVERY_REQUIRED,
                &[],
            );
            let value = raised.value(py);
            if let Err(failure) = value.setattr("query_id", query.to_string()) {
                tracing::warn!(error = %failure, "recovery query_id setattr failed");
            }
            if let Err(failure) = value.setattr("epoch", epoch.get()) {
                tracing::warn!(error = %failure, "recovery epoch setattr failed");
            }
            let durable_offset = durable.as_ref().map(|record| format!("{record:?}"));
            if let Err(failure) = value.setattr("durable_offset", durable_offset) {
                tracing::warn!(error = %failure, "recovery durable_offset setattr failed");
            }
            raised
        }
        _ => match head {
            Some(_) => stream_failed(py, &error.to_string(), head),
            None => AnalysisException::new_err(mask_user_visible(error.to_string())),
        },
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn refuse_streaming_action(py: Python<'_>, frame: &DataFrame) -> PyResult<()> {
    if repark_core::microbatch::relation::is_streaming_frame(frame) {
        return Err(attached(
            py,
            AnalysisException::new_err(STREAMING_ACTION_REFUSED_TEXT),
            STREAMING_ACTION_REFUSED,
            &[],
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule};
    use repark_core::microbatch::{
        Epoch, FilePosition, Generation, MicroBatchError, Operation, QueryId, RecoveryReason,
        RunId, SnapshotId, TableUuid,
    };

    use super::*;
    use crate::exceptions::PySparkException;

    const HEAD: QueryHead<'static> = QueryHead {
        query_id: "q-1",
        run_id: "r-1",
    };

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

    fn condition_opt(error: &PyErr, py: Python<'_>) -> Option<String> {
        error
            .value(py)
            .getattr("_spark_error_class")
            .ok()?
            .extract::<String>()
            .ok()
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

    fn query_id() -> QueryId {
        QueryId::new(RunId::fresh().get())
    }

    fn table_uuid() -> TableUuid {
        TableUuid::new(RunId::fresh().get())
    }

    fn generation(value: u64) -> Generation {
        Generation::new(value).expect("positive generation")
    }

    fn stub_errors_module(py: Python<'_>) {
        let parent = PyModule::new(py, "repark").expect("a stub parent package");
        let module = PyModule::new(py, "repark.errors").expect("a stub errors module");
        let globals = module.dict();
        py.run(
            c"class PySparkNotImplementedError(Exception):
    def __init__(self, message=None, *, errorClass=None, messageParameters=None, contexts=None):
        super().__init__(message)
        self._error_class = errorClass
        self._message_parameters = messageParameters
    def getErrorClass(self):
        return self._error_class
    def getMessageParameters(self):
        return self._message_parameters
    def getSqlState(self):
        return None
",
            Some(&globals),
            None,
        )
        .expect("the stub class defines");
        let modules = py
            .import("sys")
            .expect("sys imports")
            .getattr("modules")
            .expect("sys.modules reads");
        modules
            .set_item("repark", parent)
            .expect("the parent installs");
        modules
            .set_item("repark.errors", module)
            .expect("the stub installs");
    }

    fn not_implemented_shape(error: &PyErr, py: Python<'_>, feature: &str) {
        let value = error.value(py);
        assert_eq!(
            value
                .get_type()
                .getattr("__name__")
                .expect("a type name")
                .extract::<String>()
                .expect("the name is str"),
            "PySparkNotImplementedError"
        );
        assert_eq!(
            message(error, py),
            format!("[NOT_IMPLEMENTED] {feature} is not implemented.")
        );
        assert_eq!(
            value
                .call_method0("getErrorClass")
                .expect("a condition")
                .extract::<String>()
                .expect("the condition is str"),
            NOT_IMPLEMENTED
        );
        let reported: BTreeMap<String, String> = value
            .call_method0("getMessageParameters")
            .expect("params")
            .extract::<Bound<'_, PyDict>>()
            .expect("params are a dict")
            .iter()
            .map(|(key, item)| {
                (
                    key.extract::<String>().expect("param key"),
                    item.extract::<String>().expect("param value"),
                )
            })
            .collect();
        assert_eq!(
            reported,
            BTreeMap::from([("feature".to_string(), feature.to_string())])
        );
        assert!(
            value
                .call_method0("getSqlState")
                .expect("a sqlstate")
                .is_none()
        );
    }

    fn failed_shape(error: &PyErr, py: Python<'_>, cause: &str) {
        assert!(error.is_instance_of::<StreamingQueryException>(py));
        assert!(error.is_instance_of::<PySparkException>(py));
        assert_eq!(condition(error, py), STREAM_FAILED);
        assert!(params(error, py).is_empty());
        assert_eq!(
            message(error, py),
            format!(
                "[{STREAM_FAILED}] Query [id = q-1, runId = r-1] terminated with exception: {cause} SQLSTATE: XXKST"
            )
        );
    }

    #[test]
    fn non_append_snapshot_overwrite_renders_stream_failed_like_mb0_r2() {
        Python::attach(|py| {
            let error = MicroBatchError::NonAppendSnapshot {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(123),
                operation: Operation::Overwrite,
                from: None,
                to: SnapshotId::new(200),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
            assert!(
                message(&microbatch_py_err(py, &error, Some(HEAD)), py)
                    .contains("Cannot process overwrite snapshot: 123")
            );
        });
    }

    #[test]
    fn non_append_snapshot_delete_renders_stream_failed_like_mb0_r5() {
        Python::attach(|py| {
            let error = MicroBatchError::NonAppendSnapshot {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(456),
                operation: Operation::Delete,
                from: None,
                to: SnapshotId::new(456),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
            assert!(
                message(&microbatch_py_err(py, &error, Some(HEAD)), py)
                    .contains("Cannot process delete snapshot: 456")
            );
        });
    }

    #[test]
    fn skip_option_refused_matches_stub_shape() {
        Python::attach(|py| {
            let refused = microbatch_py_err(
                py,
                &MicroBatchError::SkipOptionRefused {
                    option: "streaming-skip-delete-snapshots",
                },
                None,
            );
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(condition(&refused, py), SKIP_OPTION_REFUSED);
            assert_eq!(
                params(&refused, py),
                BTreeMap::from([(
                    "option".to_string(),
                    "streaming-skip-delete-snapshots".to_string()
                )])
            );
            assert_eq!(
                message(&refused, py),
                "[REPARK_MICROBATCH.SKIP_OPTION_REFUSED] streaming-skip-delete-snapshots is not \
                 accepted: Bronze is append-only (O-5)"
            );
            assert!(!message(&refused, py).contains("SQLSTATE"));
        });
    }

    #[test]
    fn unknown_option_matches_stub_shape() {
        Python::attach(|py| {
            let refused = microbatch_py_err(
                py,
                &MicroBatchError::UnknownOption {
                    key: String::from("streaming-zzz"),
                },
                None,
            );
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(condition(&refused, py), UNKNOWN_OPTION);
            assert_eq!(
                params(&refused, py),
                BTreeMap::from([("key".to_string(), "streaming-zzz".to_string())])
            );
            assert_eq!(
                message(&refused, py),
                "[REPARK_MICROBATCH.UNKNOWN_OPTION] unknown streaming option streaming-zzz; \
                 remove it or fix the spelling"
            );
            assert!(!message(&refused, py).contains("SQLSTATE"));
        });
    }

    #[test]
    fn checkpoint_location_missing_matches_mb0_w8_verbatim() {
        Python::attach(|py| {
            let refused = microbatch_py_err(py, &MicroBatchError::CheckpointLocationMissing, None);
            assert!(refused.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition(&refused, py), CHECKPOINT_MISSING);
            assert!(params(&refused, py).is_empty());
            assert_eq!(
                message(&refused, py),
                "checkpointLocation must be specified either through \
                 option(\"checkpointLocation\", ...) or SparkSession.conf.set(\
                 \"spark.sql.streaming.checkpointLocation\", ...)."
            );
            assert!(!message(&refused, py).contains("SQLSTATE"));
        });
    }

    #[test]
    fn output_mode_refused_raises_not_implemented() {
        Python::attach(|py| {
            stub_errors_module(py);
            let refused = microbatch_py_err(
                py,
                &MicroBatchError::OutputModeRefused {
                    mode: String::from("complete"),
                },
                None,
            );
            not_implemented_shape(&refused, py, "outputMode(complete)");
        });
    }

    #[test]
    fn stateful_operator_refused_raises_not_implemented() {
        Python::attach(|py| {
            stub_errors_module(py);
            let refused = microbatch_py_err(
                py,
                &MicroBatchError::StatefulOperatorRefused {
                    operator: String::from("aggregation"),
                },
                None,
            );
            not_implemented_shape(&refused, py, "aggregation on a streaming DataFrame");
        });
    }

    #[test]
    fn feature_refused_raises_not_implemented() {
        Python::attach(|py| {
            stub_errors_module(py);
            let refused = microbatch_py_err(
                py,
                &MicroBatchError::FeatureRefused {
                    feature: String::from("trigger(continuous)"),
                },
                None,
            );
            not_implemented_shape(&refused, py, "trigger(continuous)");
        });
    }

    #[test]
    fn local_catalog_refused_renders_mbe8() {
        Python::attach(|py| {
            let error = MicroBatchError::LocalCatalogRefused {
                catalog: String::from("hadoop"),
            };
            let refused = microbatch_py_err(py, &error, None);
            assert!(refused.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition(&refused, py), LOCAL_CATALOG_REFUSED);
            assert!(params(&refused, py).is_empty());
            assert_eq!(
                message(&refused, py),
                format!("[{LOCAL_CATALOG_REFUSED}] {error}")
            );
            assert!(!message(&refused, py).contains("SQLSTATE"));
        });
    }

    #[test]
    fn keyless_source_renders_mbe9() {
        Python::attach(|py| {
            let error = MicroBatchError::KeylessSource {
                table: String::from("bronze.events"),
            };
            let refused = microbatch_py_err(py, &error, None);
            assert!(refused.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition(&refused, py), KEYLESS_SOURCE);
            assert!(params(&refused, py).is_empty());
            assert_eq!(message(&refused, py), format!("[{KEYLESS_SOURCE}] {error}"));
            assert!(!message(&refused, py).contains("SQLSTATE"));
        });
    }

    #[test]
    fn sink_undeclared_matches_stub_shape() {
        Python::attach(|py| {
            let refused = microbatch_py_err(py, &MicroBatchError::SinkUndeclared, None);
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(condition(&refused, py), SINK_UNDECLARED);
            assert!(params(&refused, py).is_empty());
            assert_eq!(
                message(&refused, py),
                "[REPARK_MICROBATCH.SINK_UNDECLARED] no sink declared for this streaming query; \
                 pass .option(\"repark.cdc.sink\", \"<table>\")"
            );
            assert!(!message(&refused, py).contains("SQLSTATE"));
        });
    }

    #[test]
    fn sink_busy_renders_stream_failed_at_start_and_run() {
        Python::attach(|py| {
            let error = MicroBatchError::SinkBusy {
                sink: String::from("silver.events"),
            };
            let cause = format!("[{SINK_BUSY}] {error}");
            failed_shape(&microbatch_py_err(py, &error, Some(HEAD)), py, &cause);
            let started = microbatch_py_err(py, &error, None);
            assert!(started.is_instance_of::<StreamingQueryException>(py));
            assert_eq!(condition(&started, py), STREAM_FAILED);
            assert_eq!(
                message(&started, py),
                format!("[{STREAM_FAILED}] {cause} SQLSTATE: XXKST")
            );
        });
    }

    #[test]
    fn sink_committed_twice_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::SinkCommittedTwice {
                epoch: Epoch::FIRST,
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{SINK_COMMITTED_TWICE}] {error}"),
            );
            let error = MicroBatchError::UnstampedSinkWrite {
                sink: String::from("silver.events"),
                epoch: Epoch::FIRST,
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{UNSTAMPED_SINK_WRITE}] {error}"),
            );
        });
    }

    #[test]
    fn already_committed_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::AlreadyCommitted {
                query: query_id(),
                epoch: Epoch::FIRST,
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
        });
    }

    #[test]
    fn fenced_renders_stream_failed_with_fenced_cause() {
        Python::attach(|py| {
            let error = MicroBatchError::Fenced {
                query: query_id(),
                epoch: Epoch::FIRST,
                winner: RunId::fresh(),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{FENCED}] {error}"),
            );
        });
    }

    #[test]
    fn generation_mismatch_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::GenerationMismatch {
                query: query_id(),
                resumed: generation(1),
                stamped: generation(2),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{GENERATION_MISMATCH}] {error}"),
            );
        });
    }

    #[test]
    fn inputs_changed_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::InputsChanged {
                query: query_id(),
                recorded: vec![String::from("bronze.a")],
                current: vec![String::from("bronze.b")],
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{INPUTS_CHANGED}] {error}"),
            );
        });
    }

    #[test]
    fn source_replaced_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::SourceReplaced {
                table: String::from("bronze.events"),
                recorded: table_uuid(),
                current: table_uuid(),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{SOURCE_REPLACED}] {error}"),
            );
        });
    }

    #[test]
    fn source_snapshot_expired_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::SourceSnapshotExpired {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(7),
                oldest: SnapshotId::new(9),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{SOURCE_SNAPSHOT_EXPIRED}] {error}"),
            );
        });
    }

    #[test]
    fn snapshot_not_in_lineage_splits_start_and_run() {
        Python::attach(|py| {
            let error = MicroBatchError::SnapshotNotInLineage {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(7),
                head: SnapshotId::new(9),
            };
            let started = microbatch_py_err(py, &error, None);
            assert!(started.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition_opt(&started, py), None);
            assert_eq!(message(&started, py), error.to_string());
            assert!(!message(&started, py).contains("SQLSTATE"));
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
        });
    }

    #[test]
    fn offset_position_out_of_range_splits_start_and_run() {
        Python::attach(|py| {
            let error = MicroBatchError::OffsetPositionOutOfRange {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(7),
                position: FilePosition::new(99),
                files: 2,
            };
            let started = microbatch_py_err(py, &error, None);
            assert!(started.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition_opt(&started, py), None);
            assert_eq!(message(&started, py), error.to_string());
            assert!(!message(&started, py).contains("SQLSTATE"));
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
        });
    }

    #[test]
    fn truncated_history_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::TruncatedHistory {
                table: String::from("bronze.events"),
                oldest: SnapshotId::new(9),
                missing_parent: SnapshotId::new(8),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{TRUNCATED_HISTORY}] {error}"),
            );
        });
    }

    #[test]
    fn unsupported_offset_format_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::UnsupportedOffsetFormat {
                found: String::from("2"),
                supported: 1,
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &format!("[{UNSUPPORTED_OFFSET_FORMAT}] {error}"),
            );
        });
    }

    #[test]
    fn merge_isolation_refused_renders_mbe15() {
        Python::attach(|py| {
            let error = MicroBatchError::MergeIsolationRefused {
                sink: String::from("silver.events"),
                property: String::from("write.merge.isolation-level"),
            };
            let refused = microbatch_py_err(py, &error, None);
            assert!(refused.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition(&refused, py), MERGE_ISOLATION_REFUSED);
            assert!(params(&refused, py).is_empty());
            assert_eq!(
                message(&refused, py),
                format!("[{MERGE_ISOLATION_REFUSED}] {error}")
            );
            assert!(!message(&refused, py).contains("SQLSTATE"));
        });
    }

    #[test]
    fn batch_failed_renders_stream_failed_like_mb0_w6() {
        Python::attach(|py| {
            let error = MicroBatchError::BatchFailed {
                epoch: Epoch::FIRST,
                cause: String::from("boom"),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
            assert!(
                message(&microbatch_py_err(py, &error, Some(HEAD)), py)
                    .contains("batch 0 failed: boom")
            );
        });
    }

    #[test]
    fn await_from_driver_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::AwaitFromDriver { query: query_id() };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
        });
    }

    #[test]
    fn driver_panicked_renders_stream_failed() {
        Python::attach(|py| {
            let error = MicroBatchError::DriverPanicked {
                epoch: Epoch::FIRST,
                message: String::from("boom"),
            };
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
        });
    }

    #[test]
    fn catalog_timeout_splits_start_and_run() {
        Python::attach(|py| {
            let error = MicroBatchError::CatalogTimeout {
                call: "load the sink",
                waited: Duration::from_mins(1),
            };
            let started = microbatch_py_err(py, &error, None);
            assert!(started.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition_opt(&started, py), None);
            assert_eq!(
                message(&started, py),
                "catalog call (load the sink) timed out after 60s \
                 (repark.cdc.catalog-timeout); the offset did not advance, restart the query to \
                 retry"
            );
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
        });
    }

    #[test]
    fn catalog_passthrough_splits_start_and_run() {
        Python::attach(|py| {
            let error = MicroBatchError::Catalog(String::from(
                "streaming-max-files-per-micro-batch needs a positive integer file count no \
                 larger than 2147483647, got \"abc\"",
            ));
            let started = microbatch_py_err(py, &error, None);
            assert!(started.is_instance_of::<AnalysisException>(py));
            assert_eq!(condition_opt(&started, py), None);
            assert_eq!(message(&started, py), error.to_string());
            assert!(!message(&started, py).contains("SQLSTATE"));
            failed_shape(
                &microbatch_py_err(py, &error, Some(HEAD)),
                py,
                &error.to_string(),
            );
        });
    }

    #[test]
    fn recovery_required_carries_query_epoch_and_durable() {
        Python::attach(|py| {
            let reasons = [
                RecoveryReason::CommitOutcomeUnknown {
                    operation_id: Some(String::from("op-1")),
                    resume_refusal: None,
                },
                RecoveryReason::StopTimeout {
                    waited: Duration::from_secs(30),
                },
                RecoveryReason::OffsetMismatch {
                    summary_epoch: Some(Epoch::FIRST),
                    property_epoch: None,
                },
                RecoveryReason::StampedSnapshotExpired,
                RecoveryReason::StampNotInLineage {
                    snapshot: SnapshotId::new(12),
                },
                RecoveryReason::UnstampedSinkCommit {
                    snapshot: SnapshotId::new(11),
                },
            ];
            for reason in reasons {
                let query = query_id();
                let error = MicroBatchError::RecoveryRequired {
                    query,
                    epoch: Epoch::new(3),
                    durable: None,
                    reason,
                };
                let required = microbatch_py_err(py, &error, None);
                assert!(required.is_instance_of::<RecoveryRequiredException>(py));
                assert!(required.is_instance_of::<PySparkException>(py));
                assert!(!required.is_instance_of::<StreamingQueryException>(py));
                assert_eq!(condition(&required, py), RECOVERY_REQUIRED);
                assert!(params(&required, py).is_empty());
                assert_eq!(
                    message(&required, py),
                    format!("[{RECOVERY_REQUIRED}] {error}")
                );
                assert!(!message(&required, py).contains("SQLSTATE"));
                let value = required.value(py);
                assert_eq!(
                    value
                        .getattr("query_id")
                        .expect("query_id carried")
                        .extract::<String>()
                        .expect("query_id is str"),
                    query.to_string()
                );
                assert_eq!(
                    value
                        .getattr("epoch")
                        .expect("epoch carried")
                        .extract::<u64>()
                        .expect("epoch is int"),
                    3
                );
                assert!(
                    value
                        .getattr("durable_offset")
                        .expect("durable_offset carried")
                        .is_none()
                );
            }
        });
    }
}
