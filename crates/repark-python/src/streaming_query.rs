use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyAny;
use repark_core::microbatch::MicroBatchError;
use repark_core::microbatch::driver::{QueryHandle, ShutdownOutcome};
use tokio::runtime::Runtime;

use crate::streaming_errors::{QueryHead, microbatch_py_err};

const MAX_AWAIT_SECS: f64 = 1_000_000_000_000.0;

#[derive(Debug)]
#[pyclass(name = "PyStreamingQuery", module = "repark._native")]
pub struct PyStreamingQuery {
    handle: QueryHandle,
    runtime: Arc<Runtime>,
}

impl PyStreamingQuery {
    pub(crate) fn new(handle: QueryHandle, runtime: Arc<Runtime>) -> Self {
        PyStreamingQuery { handle, runtime }
    }

    fn run_error(&self, py: Python<'_>, error: &MicroBatchError) -> PyErr {
        let query = self.handle.id().to_string();
        let run = self.handle.run_id().to_string();
        microbatch_py_err(
            py,
            error,
            Some(QueryHead {
                query_id: &query,
                run_id: &run,
            }),
        )
    }
}

#[pymethods]
impl PyStreamingQuery {
    pub fn id(&self) -> String {
        self.handle.id().to_string()
    }

    pub fn run_id(&self) -> String {
        self.handle.run_id().to_string()
    }

    pub fn name(&self) -> Option<String> {
        self.handle.name()
    }

    pub fn is_active(&self) -> bool {
        self.handle.is_active()
    }

    pub fn status(&self) -> String {
        self.handle.status().json().to_string()
    }

    pub fn last_progress(&self) -> Option<String> {
        self.handle
            .last_progress()
            .map(|progress| progress.json().to_string())
    }

    pub fn recent_progress(&self) -> Vec<String> {
        self.handle
            .recent_progress()
            .iter()
            .map(|progress| progress.json().to_string())
            .collect()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn await_termination(
        &self,
        py: Python<'_>,
        timeout_secs: Option<f64>,
    ) -> PyResult<Option<bool>> {
        let timeout = match timeout_secs {
            None => None,
            Some(secs) if (0.0..MAX_AWAIT_SECS).contains(&secs) => {
                Some(Duration::from_secs_f64(secs))
            }
            Some(secs) => {
                return Err(microbatch_py_err(
                    py,
                    &MicroBatchError::Catalog(format!(
                        "awaitTermination timeout must be a non-negative number of seconds, got {secs}"
                    )),
                    None,
                ));
            }
        };
        let done = py.detach(|| {
            self.runtime
                .block_on(self.handle.await_termination(timeout))
        });
        match done {
            Ok(true) if timeout.is_some() => Ok(Some(true)),
            Ok(true) => Ok(None),
            Ok(false) => Ok(Some(false)),
            Err(error) => Err(self.run_error(py, &error)),
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn stop(&self, py: Python<'_>) -> PyResult<()> {
        let outcome = py.detach(|| self.runtime.block_on(self.handle.stop()));
        if matches!(outcome, ShutdownOutcome::RecoveryRequired { .. }) {
            let error = self.handle.exception().ok_or_else(|| {
                microbatch_py_err(
                    py,
                    &MicroBatchError::Catalog(format!(
                        "query {id} ended RecoveryRequired without a recorded cause",
                        id = self.handle.id()
                    )),
                    None,
                )
            })?;
            return Err(self.run_error(py, &error));
        }
        Ok(())
    }

    pub fn exception(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.handle.exception().map(|error| {
            self.run_error(py, &error)
                .value(py)
                .clone()
                .into_any()
                .unbind()
        })
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStreamingQuery>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use pyo3::prelude::*;
    use repark_core::microbatch::MicroBatchError;
    use repark_core::microbatch::driver::{SinkSpec, StreamSpec, StreamingQueryManager, Trigger};
    use repark_core::time_travel::microbatch_source::SourceOptions;

    use super::*;
    use crate::exceptions::AnalysisException;
    use crate::session::PyReparkSession;

    fn condition(error: &PyErr, py: Python<'_>) -> String {
        error
            .value(py)
            .getattr("_spark_error_class")
            .expect("condition attached")
            .extract::<String>()
            .expect("condition is str")
    }

    fn session_with_tables(py: Python<'_>, tag: &str) -> (Py<PyReparkSession>, PathBuf) {
        let warehouse = std::env::temp_dir().join(format!(
            "repark-py-streaming-query-{tag}-{id}",
            id = std::process::id()
        ));
        let session = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, None, None).expect("a session"),
        )
        .expect("a session object");
        session
            .borrow(py)
            .register_memory_catalog(py, "sc", &warehouse.to_string_lossy())
            .expect("a memory catalog");
        for statement in [
            "CREATE NAMESPACE sc.mb5",
            "CREATE TABLE sc.mb5.source (id BIGINT, k STRING)",
            "CREATE TABLE sc.mb5.sink_a (id BIGINT, k STRING)",
            "CREATE TABLE sc.mb5.sink_b (id BIGINT, k STRING)",
            "INSERT INTO sc.mb5.source VALUES (1, 'k1'), (2, 'k0'), (3, 'k1')",
        ] {
            session
                .borrow(py)
                .sql(py, statement)
                .expect("a seed statement");
        }
        (session, warehouse)
    }

    fn table_spec(sink: &str, trigger: Trigger) -> StreamSpec {
        let options =
            SourceOptions::from_options(&BTreeMap::new()).expect("default source options");
        let mut spec = StreamSpec::new(
            "sc.mb5.source",
            options,
            SinkSpec::Table {
                sink: sink.to_string(),
            },
        );
        spec.trigger = trigger;
        spec.query_name = Some(String::from("item5"));
        spec
    }

    #[test]
    fn the_seam_starts_on_a_memory_catalog_and_plain_start_refuses_mbe8() {
        Python::attach(|py| {
            let (session, warehouse) = session_with_tables(py, "seam");
            let owned = session.borrow(py);
            let runtime = Arc::clone(&owned.runtime);
            let manager = StreamingQueryManager::of(&owned.session);
            let plain = runtime
                .block_on(manager.register(
                    &owned.session,
                    table_spec("sc.mb5.sink_a", Trigger::default()),
                ))
                .expect("a registered query");
            let refused = runtime
                .block_on(plain.start())
                .expect_err("a local sink refuses");
            assert!(matches!(
                refused,
                MicroBatchError::LocalCatalogRefused { .. }
            ));
            let rendered = microbatch_py_err(py, &refused, None);
            assert!(rendered.is_instance_of::<AnalysisException>(py));
            assert_eq!(
                condition(&rendered, py),
                "REPARK_MICROBATCH.LOCAL_CATALOG_REFUSED"
            );
            let running = runtime
                .block_on(manager.register(
                    &owned.session,
                    table_spec("sc.mb5.sink_a", Trigger::default()),
                ))
                .expect("a second handle");
            runtime
                .block_on(async { running.start_below_catalog_check() })
                .expect("the seam starts");
            let query = PyStreamingQuery::new(running, Arc::clone(&runtime));
            assert!(query.is_active());
            assert_eq!(query.name(), Some(String::from("item5")));
            assert!(!query.id().is_empty());
            assert!(!query.run_id().is_empty());
            assert_ne!(query.id(), query.run_id());
            assert!(query.status().contains("isTriggerActive"));
            assert!(query.exception(py).is_none());
            query.stop(py).expect("a stop drains");
            assert!(!query.is_active());
            assert!(
                query
                    .await_termination(py, None)
                    .expect("a stopped wait")
                    .is_none()
            );
            drop(owned);
            let _ = std::fs::remove_dir_all(&warehouse);
        });
    }

    #[test]
    fn an_available_now_query_reports_progress_through_the_class() {
        Python::attach(|py| {
            let (session, warehouse) = session_with_tables(py, "progress");
            let owned = session.borrow(py);
            let runtime = Arc::clone(&owned.runtime);
            let manager = StreamingQueryManager::of(&owned.session);
            let handle = runtime
                .block_on(manager.register(
                    &owned.session,
                    table_spec("sc.mb5.sink_b", Trigger::AvailableNow),
                ))
                .expect("a registered query");
            runtime
                .block_on(async { handle.start_below_catalog_check() })
                .expect("the seam starts");
            let query = PyStreamingQuery::new(handle, Arc::clone(&runtime));
            assert_eq!(
                query
                    .await_termination(py, Some(60.0))
                    .expect("an available-now drain"),
                Some(true)
            );
            let last = query.last_progress().expect("a last progress");
            assert!(last.contains("\"batchId\":0"), "{last}");
            assert!(last.contains("\"numInputRows\":3"), "{last}");
            assert_eq!(query.recent_progress().len(), 1);
            let sink = owned
                .sql(py, "SELECT * FROM sc.mb5.sink_b")
                .expect("a sink read");
            assert_eq!(sink.count(py).expect("a count"), 3);
            query.stop(py).expect("a drained stop");
            assert!(query.exception(py).is_none());
            drop(owned);
            let _ = std::fs::remove_dir_all(&warehouse);
        });
    }
}
