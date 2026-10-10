use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use repark_core::write_postgres::{
    PostgresWrite, PostgresWritePath, PostgresWriteReport, PostgresWriteTarget,
    parse_write_path_option, record_postgres_write_report, take_postgres_write_report,
};

use crate::dataframe::PyDataFrame;
use crate::deep_stack::{block_on_grown_sized, frame_drive_segment_cached, grown_clone_frame};
use crate::fence::fenced_span;
use crate::session::PyReparkSession;

type WriteReportTuple = (String, u64, Option<String>);

fn report_tuple(report: &PostgresWriteReport) -> WriteReportTuple {
    let path = match report.path {
        PostgresWritePath::Bulk => "bulk",
        PostgresWritePath::Row => "row",
    };
    (path.to_string(), report.rows, report.fallback.clone())
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (session, frame, url, dbtable, properties, write_path, columns))]
pub fn session_write_postgres(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    frame: &PyDataFrame,
    url: &str,
    dbtable: &str,
    properties: HashMap<String, String>,
    write_path: Option<&str>,
    columns: Vec<String>,
) -> PyResult<WriteReportTuple> {
    fenced_span!("py.write", "session_write_postgres", {
        let path = parse_write_path_option(write_path).map_err(crate::datafusion_to_py_err)?;
        let write = PostgresWrite {
            target: PostgresWriteTarget::Url {
                url: url.to_string(),
                dbtable: dbtable.to_string(),
                properties: properties.into_iter().collect(),
            },
            columns: Some(columns),
            case_insensitive: false,
            path,
        };
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let segment = frame_drive_segment_cached(&frame.depths())?;
        let frame = grown_clone_frame(frame.inner(), &frame.depths());
        let report = py
            .detach(|| block_on_grown_sized(&runtime, inner.write_postgres(frame, write), segment))
            .map_err(crate::datafusion_to_py_err)?;
        record_postgres_write_report(session.session.context(), report.clone());
        Ok(report_tuple(&report))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
pub fn session_take_postgres_write_report(
    session: PyRef<'_, PyReparkSession>,
) -> PyResult<Option<WriteReportTuple>> {
    fenced_span!("py.write", "session_take_postgres_write_report", {
        Ok(take_postgres_write_report(session.session.context())
            .as_ref()
            .map(report_tuple))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_write_postgres, module)?)?;
    module.add_function(wrap_pyfunction!(
        session_take_postgres_write_report,
        module
    )?)?;
    Ok(())
}
