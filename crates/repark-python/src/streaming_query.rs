use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::wrap_pyfunction;
use repark_core::microbatch::Epoch;
use repark_core::microbatch::MicroBatchError;
use repark_core::microbatch::driver::{
    BatchBody, QueryHandle, ShutdownOutcome, StreamingQueryManager,
};
use tokio::runtime::Runtime;

use crate::dataframe::{PyDataFrame, with_stream_poll_no_detach};
use crate::exceptions::IllegalArgumentException;
use crate::session::PyReparkSession;
use crate::streaming_errors::{QueryHead, microbatch_py_err};

const MAX_AWAIT_SECS: f64 = 1_000_000_000_000.0;
const AWAIT_ANY_POLL: Duration = Duration::from_millis(10);

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

#[derive(Debug, Default)]
struct BindingManagerState {
    known: Mutex<Vec<QueryHandle>>,
    terminated: Mutex<Vec<QueryHandle>>,
}

impl BindingManagerState {
    fn of(session: &PyReparkSession) -> Arc<Self> {
        let state = session.session.context().state_ref();
        let mut state = state.write();
        if let Some(known) = state.config().get_extension::<Self>() {
            return known;
        }
        let fresh = Arc::new(Self::default());
        state.config_mut().set_extension(Arc::clone(&fresh));
        fresh
    }

    fn reap(&self) {
        let known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        let mut terminated = self
            .terminated
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for handle in known.iter() {
            if !handle.is_active() && !terminated.iter().any(|done| done.id() == handle.id()) {
                terminated.push(handle.clone());
            }
        }
    }

    fn first_terminated(&self) -> Option<QueryHandle> {
        self.terminated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first()
            .cloned()
    }
}

pub(crate) fn note_started(session: &PyReparkSession, handle: &QueryHandle) {
    let state = BindingManagerState::of(session);
    state
        .known
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(handle.clone());
}

fn parse_query_uuid(text: &str) -> Option<u128> {
    let mut parts = text.split('-');
    let mut words = [0u64; 5];
    for slot in &mut words {
        let part = parts.next()?;
        if part.is_empty() || part.len() > 16 || !part.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return None;
        }
        *slot = u64::from_str_radix(part, 16).ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    let high = words[0].wrapping_shl(32) | words[1].wrapping_shl(16) | words[2];
    let low = words[3].wrapping_shl(48) | words[4];
    Some((u128::from(high) << 64) | u128::from(low))
}

fn canonical_uuid_text(value: u128) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (value >> 96) & 0xffff_ffff,
        (value >> 80) & 0xffff,
        (value >> 64) & 0xffff,
        (value >> 48) & 0xffff,
        value & 0xffff_ffff_ffff,
    )
}

fn wrap_query(
    py: Python<'_>,
    session: &PyReparkSession,
    handle: QueryHandle,
) -> PyResult<Py<PyStreamingQuery>> {
    Py::new(
        py,
        PyStreamingQuery::new(handle, Arc::clone(&session.runtime)),
    )
}

#[pyfunction]
fn streams_active(
    py: Python<'_>,
    session: &PyReparkSession,
) -> PyResult<Vec<Py<PyStreamingQuery>>> {
    let manager = StreamingQueryManager::of(&session.session);
    manager
        .active()
        .into_iter()
        .map(|handle| wrap_query(py, session, handle))
        .collect()
}

#[pyfunction]
#[allow(clippy::missing_errors_doc)]
fn streams_get(
    py: Python<'_>,
    session: &PyReparkSession,
    id: &str,
) -> PyResult<Option<Py<PyStreamingQuery>>> {
    let Some(parsed) = parse_query_uuid(id) else {
        return Err(IllegalArgumentException::new_err(format!(
            "Invalid UUID string: {id}"
        )));
    };
    let want = canonical_uuid_text(parsed);
    let manager = StreamingQueryManager::of(&session.session);
    manager
        .active()
        .into_iter()
        .find(|query| query.id().to_string() == want)
        .map(|handle| wrap_query(py, session, handle))
        .transpose()
}

#[allow(clippy::missing_errors_doc)]
fn report_terminated(py: Python<'_>, handle: &QueryHandle, timed: bool) -> PyResult<Option<bool>> {
    if let Some(error) = handle.exception() {
        let query = handle.id().to_string();
        let run = handle.run_id().to_string();
        return Err(microbatch_py_err(
            py,
            &error,
            Some(QueryHead {
                query_id: &query,
                run_id: &run,
            }),
        ));
    }
    Ok(timed.then_some(true))
}

#[pyfunction]
#[allow(clippy::missing_errors_doc)]
fn streams_await_any_termination(
    py: Python<'_>,
    session: &PyReparkSession,
    timeout_secs: Option<f64>,
) -> PyResult<Option<bool>> {
    let timeout = match timeout_secs {
        None => None,
        Some(secs) if (0.0..MAX_AWAIT_SECS).contains(&secs) => Some(Duration::from_secs_f64(secs)),
        Some(secs) => {
            return Err(microbatch_py_err(
                py,
                &MicroBatchError::Catalog(format!(
                    "awaitAnyTermination timeout must be a non-negative number of seconds, got {secs}"
                )),
                None,
            ));
        }
    };
    let state = BindingManagerState::of(session);
    state.reap();
    if let Some(done) = state.first_terminated() {
        return report_terminated(py, &done, timeout.is_some());
    }
    let runtime = Arc::clone(&session.runtime);
    let ended: Option<QueryHandle> = py.detach(|| {
        runtime.block_on(async {
            let start = Instant::now();
            loop {
                state.reap();
                if let Some(done) = state.first_terminated() {
                    return Some(done);
                }
                if timeout.is_some_and(|limit| start.elapsed() >= limit) {
                    return None;
                }
                tokio::time::sleep(AWAIT_ANY_POLL).await;
            }
        })
    });
    match ended {
        None => Ok(Some(false)),
        Some(done) => report_terminated(py, &done, timeout.is_some()),
    }
}

#[pyfunction]
fn streams_reset_terminated(session: &PyReparkSession) {
    let state = BindingManagerState::of(session);
    state
        .terminated
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clear();
    state
        .known
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(QueryHandle::is_active);
}

pub(crate) struct BatchBodyAdapter {
    body: Py<PyAny>,
    session: Py<PyReparkSession>,
    alive_token: Py<PyAny>,
    dataframe_class: Py<PyAny>,
    runtime: Arc<Runtime>,
}

impl BatchBodyAdapter {
    pub(crate) fn new(
        body: Py<PyAny>,
        session: Py<PyReparkSession>,
        alive_token: Py<PyAny>,
        dataframe_class: Py<PyAny>,
        runtime: Arc<Runtime>,
    ) -> Self {
        BatchBodyAdapter {
            body,
            session,
            alive_token,
            dataframe_class,
            runtime,
        }
    }

    fn call_body(&self, frame: DataFrame, epoch: Epoch) -> Result<(), MicroBatchError> {
        with_stream_poll_no_detach(|| {
            Python::attach(|py| {
                let native = Bound::new(py, PyDataFrame::new(frame, Arc::clone(&self.runtime)))
                    .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
                let facade = self
                    .dataframe_class
                    .bind(py)
                    .call1((native, self.session.bind(py), self.alive_token.bind(py)))
                    .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
                self.body
                    .bind(py)
                    .call1((facade, epoch.get()))
                    .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
                Ok(())
            })
        })
    }
}

impl BatchBody for BatchBodyAdapter {
    fn run(&self, frame: DataFrame, epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        Box::pin(async move {
            if tokio::runtime::Handle::try_current().is_ok() {
                tokio::task::block_in_place(|| self.call_body(frame, epoch))
            } else {
                self.call_body(frame, epoch)
            }
        })
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStreamingQuery>()?;
    module.add_function(wrap_pyfunction!(streams_active, module)?)?;
    module.add_function(wrap_pyfunction!(streams_get, module)?)?;
    module.add_function(wrap_pyfunction!(streams_await_any_termination, module)?)?;
    module.add_function(wrap_pyfunction!(streams_reset_terminated, module)?)?;
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
    use crate::exceptions::{AnalysisException, IllegalArgumentException};
    use crate::session::PyReparkSession;

    fn condition(error: &PyErr, py: Python<'_>) -> String {
        error
            .value(py)
            .getattr("_spark_error_class")
            .expect("condition attached")
            .extract::<String>()
            .expect("condition is str")
    }

    fn message(error: &PyErr, py: Python<'_>) -> String {
        error
            .value(py)
            .str()
            .expect("a message")
            .to_str()
            .expect("utf8")
            .to_string()
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

    fn started_pair(
        py: Python<'_>,
        tag: &str,
    ) -> (
        Py<PyReparkSession>,
        PathBuf,
        repark_core::microbatch::driver::QueryHandle,
        repark_core::microbatch::driver::QueryHandle,
    ) {
        let (session, warehouse) = session_with_tables(py, tag);
        let owned = session.borrow(py);
        let manager = StreamingQueryManager::of(&owned.session);
        let runtime = Arc::clone(&owned.runtime);
        let first = runtime
            .block_on(manager.register(
                &owned.session,
                table_spec("sc.mb5.sink_a", Trigger::default()),
            ))
            .expect("a first handle");
        let second = runtime
            .block_on(manager.register(
                &owned.session,
                table_spec("sc.mb5.sink_b", Trigger::default()),
            ))
            .expect("a second handle");
        runtime
            .block_on(async { first.start_below_catalog_check() })
            .expect("the first starts");
        runtime
            .block_on(async { second.start_below_catalog_check() })
            .expect("the second starts");
        note_started(&owned, &first);
        note_started(&owned, &second);
        drop(owned);
        (session, warehouse, first, second)
    }

    fn active_ids(py: Python<'_>, queries: &[Py<PyStreamingQuery>]) -> Vec<String> {
        queries
            .iter()
            .map(|query| query.bind(py).borrow().id())
            .collect()
    }

    #[test]
    fn manager_lists_active_gets_by_id_and_drops_stopped() {
        Python::attach(|py| {
            let (session, warehouse, first, second) = started_pair(py, "manager");
            let owned = session.borrow(py);
            let inner: &PyReparkSession = &owned;
            let runtime = Arc::clone(&owned.runtime);
            let ids = active_ids(py, &streams_active(py, inner).expect("active"));
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&first.id().to_string()));
            assert!(ids.contains(&second.id().to_string()));
            let same = streams_get(py, inner, &first.id().to_string())
                .expect("a lookup")
                .expect("found");
            assert_eq!(same.bind(py).borrow().id(), first.id().to_string());
            let folded = streams_get(py, inner, &first.id().to_string().to_ascii_uppercase())
                .expect("a folded lookup")
                .expect("found");
            assert_eq!(folded.bind(py).borrow().id(), first.id().to_string());
            let short = first
                .id()
                .to_string()
                .split('-')
                .map(|part| part.trim_start_matches('0'))
                .map(|part| if part.is_empty() { "0" } else { part })
                .collect::<Vec<_>>()
                .join("-");
            let valued = streams_get(py, inner, &short)
                .expect("a short lookup")
                .expect("found");
            assert_eq!(valued.bind(py).borrow().id(), first.id().to_string());
            assert!(
                streams_get(py, inner, "1-2-3-4-5")
                    .expect("a short unknown")
                    .is_none()
            );
            assert!(
                streams_get(py, inner, "00000000-0000-4000-8000-000000000000")
                    .expect("an unknown")
                    .is_none()
            );
            for bad in ["bogus", "1-2-3-4", "{00000000-0000-4000-8000-000000000000}"] {
                let refused = streams_get(py, inner, bad).expect_err("a refusal");
                assert!(refused.is_instance_of::<IllegalArgumentException>(py));
                assert_eq!(message(&refused, py), format!("Invalid UUID string: {bad}"));
            }
            let _ = runtime.block_on(first.stop());
            let ids = active_ids(py, &streams_active(py, inner).expect("active"));
            assert_eq!(ids, vec![second.id().to_string()]);
            assert!(
                streams_get(py, inner, &first.id().to_string())
                    .expect("a stopped lookup")
                    .is_none()
            );
            let _ = runtime.block_on(second.stop());
            drop(owned);
            let _ = std::fs::remove_dir_all(&warehouse);
        });
    }

    #[test]
    fn await_any_termination_reports_a_stop_and_reset_clears_it() {
        Python::attach(|py| {
            let (session, warehouse, first, second) = started_pair(py, "awaitany");
            let owned = session.borrow(py);
            let inner: &PyReparkSession = &owned;
            let runtime = Arc::clone(&owned.runtime);
            assert_eq!(
                streams_await_any_termination(py, inner, Some(0.0)).expect("an idle wait"),
                Some(false)
            );
            let _ = runtime.block_on(first.stop());
            assert_eq!(
                streams_await_any_termination(py, inner, None).expect("a stop"),
                None
            );
            assert_eq!(
                streams_await_any_termination(py, inner, Some(30.0)).expect("a repeat"),
                Some(true)
            );
            streams_reset_terminated(inner);
            assert_eq!(
                streams_await_any_termination(py, inner, Some(0.0)).expect("a cleared wait"),
                Some(false)
            );
            let bad =
                streams_await_any_termination(py, inner, Some(-1.0)).expect_err("a bad timeout");
            assert!(message(&bad, py).contains("awaitAnyTermination timeout"));
            let _ = runtime.block_on(second.stop());
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
