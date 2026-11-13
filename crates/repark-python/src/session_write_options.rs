use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::datasource::default_table_source::source_as_provider;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{LogicalPlan, TableSource, WriteOp};
use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::deep_stack::{
    block_on_grown_if, block_on_grown_sized, frame_drive_segment_cached, sql_drive_grown,
};
use crate::fence::fenced_span;
use crate::session::PyReparkSession;

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, query, options, force_static_overwrite = false, force_dynamic_overwrite = false, source_by_name = false))]
pub fn session_sql_with_write_options(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    query: &str,
    options: HashMap<String, String>,
    force_static_overwrite: bool,
    force_dynamic_overwrite: bool,
    source_by_name: bool,
) -> PyResult<PyDataFrame> {
    let overwrite_intent = match (force_static_overwrite, force_dynamic_overwrite) {
        (false, false) => repark_core::OverwriteIntent::Session,
        (true, false) => repark_core::OverwriteIntent::Static,
        (false, true) => repark_core::OverwriteIntent::Dynamic,
        (true, true) => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                crate::exceptions::mask_user_visible(
                    "force_static_overwrite and force_dynamic_overwrite are mutually exclusive",
                ),
            ));
        }
    };
    fenced_span!("py.sql", "session_sql_with_write_options", {
        let grown = sql_drive_grown(query)
            || session.deep_view_levels() > crate::deep_stack::DEEP_NESTING_DEPTH;
        if grown {
            crate::deep_stack::grown_sync(crate::deep_stack::GROWN_STACK_SEGMENT_BYTES, || {
                repark_spark::refuse_declared_function_in_sql(query)
            })
            .map_err(crate::datafusion_to_py_err)?;
        } else {
            repark_spark::refuse_declared_function_in_sql(query)
                .map_err(crate::datafusion_to_py_err)?;
        }
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let df = py
            .detach(|| {
                block_on_grown_if(
                    &runtime,
                    inner.sql_built_with_write_options(
                        query,
                        &options,
                        overwrite_intent,
                        source_by_name,
                    ),
                    grown,
                )
            })
            .map_err(crate::to_py_err)?;
        if let Some(message) = pending_write_refusal(df.logical_plan(), force_dynamic_overwrite) {
            return Err(crate::to_py_err(repark_core::Error::NotImplemented(
                message,
            )));
        }
        Ok(PyDataFrame::new(df, runtime))
    })
}

const PENDING_WRITE_CARD: &str = "NATIVE-DATAFRAME-WRITE-SILENT-1";

const STAGING_PREFIX: &str = "repark-staging-";

const STAGING_HEX_LEN: usize = 32;

pub(crate) fn pending_write_refusal(plan: &LogicalPlan, dynamic_overwrite: bool) -> Option<String> {
    match plan {
        LogicalPlan::Dml(dml) => match dml.op {
            WriteOp::Insert(op) => Some(table_refusal(
                &dml.table_name.to_quoted_string(),
                op,
                dynamic_overwrite,
                in_memory(&dml.target),
            )),
            _ => None,
        },
        LogicalPlan::Copy(copy) => Some(path_refusal(&copy_destination(&copy.output_url))),
        _ => None,
    }
}

fn in_memory(target: &Arc<dyn TableSource>) -> bool {
    source_as_provider(target).is_ok_and(|provider| provider.downcast_ref::<MemTable>().is_some())
}

fn table_refusal(target: &str, op: InsertOp, dynamic_overwrite: bool, in_memory: bool) -> String {
    let workaround = match op {
        InsertOp::Append => format!(
            "run INSERT INTO {target} SELECT ... through repark.sql(...) and collect the result"
        ),
        InsertOp::Overwrite | InsertOp::Replace if in_memory => format!(
            "{target} is an in-memory table, where INSERT OVERWRITE is not implemented: run \
             CREATE OR REPLACE TABLE {target} AS SELECT ... through repark.sql(...) and collect \
             the result, which replaces the table with the query's columns and rows"
        ),
        InsertOp::Overwrite | InsertOp::Replace if dynamic_overwrite => format!(
            "on an unpartitioned table, which is every table repark.sql(...) can create, \
             overwritePartitions replaces every row: run INSERT OVERWRITE {target} SELECT ... \
             through repark.sql(...) and collect the result"
        ),
        InsertOp::Overwrite | InsertOp::Replace => format!(
            "run INSERT OVERWRITE {target} SELECT ... through repark.sql(...) and collect the \
             result"
        ),
    };
    format!(
        "DataFrame.write and writeTo cannot write a repark.sql() frame into the table {target}: \
         the write would return without storing any rows, so nothing was written; {workaround} \
         ({PENDING_WRITE_CARD})"
    )
}

fn path_refusal(path: &str) -> String {
    format!(
        "DataFrame.write cannot write a repark.sql() frame to the path {path}: the write would \
         return without storing any rows, so nothing was written; write the frame from a \
         ReparkSession ({PENDING_WRITE_CARD})"
    )
}

pub(crate) fn copy_destination(url: &str) -> String {
    let trimmed = url.strip_suffix('/').unwrap_or(url);
    let (parent, segment) = match trimmed.rsplit_once('/') {
        Some((parent, segment)) => (Some(parent), segment),
        None => (None, trimmed),
    };
    match (staged_name(segment), parent) {
        (Some(""), Some("")) => "/".to_string(),
        (Some(""), Some(parent)) => parent.to_string(),
        (Some(""), None) => ".".to_string(),
        (Some(name), Some(parent)) => format!("{parent}/{name}"),
        (Some(name), None) => name.to_string(),
        (None, _) => url.to_string(),
    }
}

fn staged_name(segment: &str) -> Option<&str> {
    let rest = segment.strip_prefix(STAGING_PREFIX)?;
    let hex = rest.get(..STAGING_HEX_LEN)?;
    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let tail = rest.get(STAGING_HEX_LEN..)?;
    if tail.is_empty() {
        return Some("");
    }
    tail.strip_prefix('-').filter(|name| !name.is_empty())
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (session, frame, url, format, mode, options, partition_by))]
pub fn session_write_path(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    frame: &PyDataFrame,
    url: &str,
    format: &str,
    mode: &str,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
) -> PyResult<usize> {
    fenced_span!("py.write", "session_write_path", {
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let segment = frame_drive_segment_cached(&frame.depths())?;
        let frame = crate::deep_stack::grown_clone_frame(frame.inner(), &frame.depths());
        py.detach(|| {
            block_on_grown_sized(
                &runtime,
                inner.write_path(&frame, url, format, mode, &options, &partition_by),
                segment,
            )
        })
        .map_err(crate::to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (sql, keep_verbatim))]
pub fn built_sql_user_fragment(sql: &str, keep_verbatim: bool) -> PyResult<String> {
    fenced_span!("py.sql", "built_sql_user_fragment", {
        repark_spark::spark_literals::built_fragment::canonicalize_fragment_for_default_parse(
            sql,
            keep_verbatim,
        )
        .map(std::borrow::Cow::into_owned)
        .map_err(crate::datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, frame, view, options, partition_by, stored_as))]
pub fn session_text_write_copy_parts(
    session: PyRef<'_, PyReparkSession>,
    frame: &PyDataFrame,
    view: &str,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
    stored_as: &str,
) -> PyResult<(String, String, String)> {
    fenced_span!("py.write", "session_text_write_copy_parts", {
        session
            .session
            .text_write_copy_parts(frame.inner(), view, &options, &partition_by, stored_as)
            .map(|parts| (parts.select_sql, parts.stored_as, parts.spec_options_sql))
            .map_err(crate::to_py_err)
    })
}

#[pyfunction]
fn write_doors() -> Vec<(String, bool)> {
    repark_spark::write_door::WriteDoor::ALL
        .iter()
        .map(|door| (door.id().to_string(), door.is_spark_door()))
        .collect()
}

#[pyfunction]
#[pyo3(signature = (sql, options = None))]
fn write_door_of(sql: &str, options: Option<HashMap<String, String>>) -> PyResult<Option<String>> {
    let pairs = options.unwrap_or_default().into_iter().collect();
    repark_spark::write_door::write_door_of_sql(sql, pairs)
        .map(|door| door.map(|door| door.id().to_string()))
        .map_err(crate::datafusion_to_py_err)
}

#[pyfunction]
fn last_write_doors(session: &PyReparkSession) -> Vec<String> {
    repark_spark::write_door::last_write_doors(session.session.context())
        .into_iter()
        .map(|door| door.id().to_string())
        .collect()
}

#[cfg(test)]
#[path = "session_write_options_tests.rs"]
mod tests;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_sql_with_write_options, module)?)?;
    module.add_function(wrap_pyfunction!(write_doors, module)?)?;
    module.add_function(wrap_pyfunction!(write_door_of, module)?)?;
    module.add_function(wrap_pyfunction!(last_write_doors, module)?)?;
    module.add_function(wrap_pyfunction!(session_write_path, module)?)?;
    module.add_function(wrap_pyfunction!(built_sql_user_fragment, module)?)?;
    module.add_function(wrap_pyfunction!(session_text_write_copy_parts, module)?)?;
    Ok(())
}
