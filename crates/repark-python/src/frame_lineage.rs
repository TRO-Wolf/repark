use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;
use repark_core::frame_names::{
    AttrId, FrameNode, JoinSide, Prepared, Refusal, SELF_JOIN_CONDITION, SelfJoinRules, check_refs,
    missing_condition, missing_message, plan_is_relation, quoted_names, self_join_message,
    shared_ids,
};
use repark_functions::case_sensitive::SPARK_SQL_FAIL_AMBIGUOUS_SELF_JOIN_KEY;

use crate::AnalysisException;
use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::fence::fenced;
use crate::session::PyReparkSession;
use crate::session_runtime::session_self_join;
use crate::temp_view_names::session_rule;

#[pyclass(frozen, name = "FrameNode", module = "repark._native")]
pub struct PyFrameNode {
    pub(crate) node: Arc<FrameNode>,
}

#[pymethods]
impl PyFrameNode {
    #[getter]
    fn id(&self) -> u64 {
        self.node.id().get()
    }

    #[getter]
    fn renews(&self) -> bool {
        self.node.renews()
    }

    #[getter]
    fn output_count(&self) -> usize {
        self.node.outputs().len()
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyFrameNode>()?;
    module.add_function(wrap_pyfunction!(frame_root, module)?)?;
    module.add_function(wrap_pyfunction!(frame_derived, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_self_join_refs, module)?)?;
    module.add_function(wrap_pyfunction!(prepare_join_condition, module)?)?;
    module.add_function(wrap_pyfunction!(join_shared_remint, module)?)?;
    module.add_function(wrap_pyfunction!(join_plan_lineage, module)?)?;
    Ok(())
}

fn session_rules(session: &PyReparkSession) -> SelfJoinRules {
    let config = session_self_join(&session.session);
    SelfJoinRules {
        fail_ambiguous: config.fail_ambiguous,
        auto_resolve: config.auto_resolve,
    }
}

fn attached(py: Python<'_>, raised: PyErr, condition: &str, params: &[(&str, String)]) -> PyErr {
    let value = raised.value(py);
    let held = PyDict::new(py);
    for (key, item) in params {
        if let Err(failure) = held.set_item(key, item) {
            tracing::warn!(error = %failure, "self-join refusal param set failed");
        }
    }
    if let Err(failure) = value.setattr("_spark_error_class", condition) {
        tracing::warn!(error = %failure, "self-join refusal condition setattr failed");
    }
    if let Err(failure) = value.setattr("_spark_message_parameters", held) {
        tracing::warn!(error = %failure, "self-join refusal params setattr failed");
    }
    raised
}

pub(crate) fn self_join_error(py: Python<'_>, names: &[String]) -> PyErr {
    let config = SPARK_SQL_FAIL_AMBIGUOUS_SELF_JOIN_KEY;
    attached(
        py,
        AnalysisException::new_err(self_join_message(names, config)),
        SELF_JOIN_CONDITION,
        &[
            ("ambiguousAttrs", names.join(", ")),
            ("config", config.to_string()),
        ],
    )
}

pub(crate) fn missing_attributes_error(
    py: Python<'_>,
    names: &[String],
    input: &[String],
    operation: &[String],
) -> PyErr {
    let mut params = vec![
        ("missingAttributes", quoted_names(names)),
        ("input", quoted_names(input)),
        ("operator", "!Join".to_string()),
    ];
    if !operation.is_empty() {
        params.push(("operation", quoted_names(operation)));
    }
    attached(
        py,
        AnalysisException::new_err(missing_message(names, input, operation)),
        missing_condition(operation),
        &params,
    )
}

pub(crate) fn refusal_error(py: Python<'_>, refusal: &Refusal) -> PyErr {
    match refusal {
        Refusal::SelfJoin { names } => self_join_error(py, names),
        Refusal::Missing {
            names,
            input,
            operation,
        } => missing_attributes_error(py, names, input, operation),
    }
}

pub(crate) fn ambiguous_reference_error(
    py: Python<'_>,
    qualifier: &[String],
    written: &str,
    hits: &[usize],
    displays: &[String],
) -> PyErr {
    let quoted = |part: &str| format!("`{}`", part.replace('`', "``"));
    let reference = qualifier
        .iter()
        .map(|part| quoted(part))
        .chain(std::iter::once(quoted(written)))
        .collect::<Vec<_>>()
        .join(".");
    let options = hits
        .iter()
        .filter_map(|position| displays.get(*position))
        .map(|display| {
            qualifier
                .iter()
                .map(|part| quoted(part))
                .chain(std::iter::once(quoted(display)))
                .collect::<Vec<_>>()
                .join(".")
        })
        .collect::<Vec<_>>()
        .join(", ");
    attached(
        py,
        AnalysisException::new_err(format!(
            "[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: [{options}]."
        )),
        "AMBIGUOUS_REFERENCE",
        &[
            ("name", reference),
            ("referenceNames", format!("[{options}]")),
        ],
    )
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
pub fn frame_root(frame: &PyDataFrame) -> PyResult<PyFrameNode> {
    fenced!("frame_lineage.frame_root", {
        let inner = frame.inner();
        let empty = DFSchema::empty();
        let schema = if plan_is_relation(inner.logical_plan()) {
            inner.schema()
        } else {
            &empty
        };
        FrameNode::root(schema)
            .map(|node| PyFrameNode { node })
            .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
pub fn frame_derived(
    frame: &PyDataFrame,
    parent: &PyFrameNode,
    others: Vec<PyRef<'_, PyFrameNode>>,
) -> PyResult<PyFrameNode> {
    fenced!("frame_lineage.frame_derived", {
        let inner = frame.inner();
        let relation = plan_is_relation(inner.logical_plan());
        let empty = DFSchema::empty();
        let schema = if relation { inner.schema() } else { &empty };
        let parent = Arc::clone(&parent.node);
        let built = if !relation {
            FrameNode::root(schema)
        } else if others.is_empty() {
            FrameNode::derived(schema, parent)
        } else {
            let others = others.iter().map(|other| Arc::clone(&other.node)).collect();
            FrameNode::set_op(schema, parent, others)
        };
        built
            .map(|node| PyFrameNode { node })
            .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
pub fn refuse_self_join_refs(
    py: Python<'_>,
    session: &PyReparkSession,
    target: &PyFrameNode,
    sql_parts: Vec<String>,
    displays: Vec<String>,
) -> PyResult<()> {
    fenced!("frame_lineage.refuse_self_join_refs", {
        if !target.node.renews() {
            return Ok(());
        }
        let parts = sql_parts.iter().map(String::as_str).collect::<Vec<_>>();
        match check_refs(
            &target.node,
            &displays,
            &parts,
            session_rules(session),
            session_rule(session),
        ) {
            Ok(None) => Ok(()),
            Ok(Some(refusal)) => Err(refusal_error(py, &refusal)),
            Err(error) => Err(datafusion_to_py_err(error)),
        }
    })
}

#[allow(
    clippy::missing_errors_doc,
    clippy::needless_pass_by_value,
    clippy::too_many_arguments
)]
#[allow(clippy::implicit_hasher)]
#[pyfunction]
#[pyo3(signature = (
    session, cond_sql, left_node, right_node, left_frame, right_frame, left_alias, right_alias,
    left_displays, right_displays, names, left_qualifiers, right_qualifiers
))]
pub fn prepare_join_condition(
    py: Python<'_>,
    session: &PyReparkSession,
    cond_sql: &str,
    left_node: &PyFrameNode,
    right_node: &PyFrameNode,
    left_frame: &PyDataFrame,
    right_frame: &PyDataFrame,
    left_alias: &str,
    right_alias: &str,
    left_displays: Vec<String>,
    right_displays: Vec<String>,
    names: HashMap<String, String>,
    left_qualifiers: Option<BTreeMap<String, Vec<String>>>,
    right_qualifiers: Option<BTreeMap<String, Vec<String>>>,
) -> PyResult<(String, HashMap<String, String>)> {
    fenced!("frame_lineage.prepare_join_condition", {
        let left = JoinSide {
            node: &left_node.node,
            schema: left_frame.inner().schema(),
            displays: &left_displays,
            alias: left_alias,
            qualifiers: left_qualifiers.as_ref(),
        };
        let right = JoinSide {
            node: &right_node.node,
            schema: right_frame.inner().schema(),
            displays: &right_displays,
            alias: right_alias,
            qualifiers: right_qualifiers.as_ref(),
        };
        let names = names
            .iter()
            .map(|(attr, name)| (AttrId::from_token(attr), name.clone()))
            .collect::<HashMap<_, _>>();
        let prepared = repark_core::frame_names::prepare_join_condition(
            cond_sql,
            &left,
            &right,
            &names,
            session_rule(session),
            session_rules(session),
        )
        .map_err(datafusion_to_py_err)?;
        match prepared {
            Prepared::Condition(condition) => Ok((
                condition.sql,
                condition
                    .remint
                    .into_iter()
                    .map(|(old, new)| (old.as_str().to_string(), new.as_str().to_string()))
                    .collect(),
            )),
            Prepared::Refused(refusal) => Err(refusal_error(py, &refusal)),
        }
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (left_node, right_node))]
pub fn join_shared_remint(
    left_node: &PyFrameNode,
    right_node: &PyFrameNode,
) -> PyResult<HashMap<String, String>> {
    fenced!("frame_lineage.join_shared_remint", {
        let remint: HashMap<String, String> =
            shared_ids(&left_node.node, right_node.node.outputs())
                .into_iter()
                .map(|id| (id.as_str().to_string(), AttrId::mint().as_str().to_string()))
                .collect();
        Ok(remint)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (joined_frame, left_node, right_node, left_width, emits_right))]
pub fn join_plan_lineage(
    joined_frame: &PyDataFrame,
    left_node: &PyFrameNode,
    right_node: &PyFrameNode,
    left_width: usize,
    emits_right: bool,
) -> PyResult<(PyDataFrame, PyFrameNode)> {
    fenced!("frame_lineage.join_plan_lineage", {
        let shared = shared_ids(&left_node.node, right_node.node.outputs());
        let (state, plan) = joined_frame.inner().clone().into_parts();
        let (plan, remint) = repark_core::frame_names::remint_shared(plan, left_width, &shared)
            .map_err(datafusion_to_py_err)?;
        let schema = plan.schema().clone();
        let node = FrameNode::join(
            &schema,
            Arc::clone(&left_node.node),
            Arc::clone(&right_node.node),
            remint,
            emits_right,
        )
        .map_err(datafusion_to_py_err)?;
        Ok((
            PyDataFrame::new(DataFrame::new(state, plan), joined_frame.runtime_handle()),
            PyFrameNode { node },
        ))
    })
}
