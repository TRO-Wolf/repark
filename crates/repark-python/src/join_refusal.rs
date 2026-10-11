use pyo3::prelude::*;

const NONDET_CONDITION: &str = "INVALID_NON_DETERMINISTIC_EXPRESSIONS";
const NONDET_PREFIX: &str = "[INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a \
     deterministic expression, but the actual expression is \"";
const NONDET_SUFFIX: &str = "\". SQLSTATE: 42K0E";
const NOT_BOOLEAN_CONDITION: &str = "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE";
const NOT_BOOLEAN_PREFIX: &str = "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE] The join condition \"";
const NOT_BOOLEAN_MIDDLE: &str = "\" has the invalid type \"";
const NOT_BOOLEAN_SUFFIX: &str = "\", expected \"BOOLEAN\". SQLSTATE: 42K0E";

#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub(crate) fn analysis_py_err(message: String) -> PyErr {
    if let Some((condition, params)) = refusal_params(&message) {
        let raised = crate::AnalysisException::new_err(message);
        return Python::attach(|py| {
            let value = raised.value(py);
            let pairs: Vec<(&str, &str)> = params
                .iter()
                .map(|(key, held)| (*key, held.as_str()))
                .collect();
            let dict = crate::exceptions::masked_message_params(py, &pairs);
            if let Err(failure) = value.setattr("_spark_error_class", condition) {
                tracing::warn!(error = %failure, "join refusal condition setattr failed");
            }
            if let Err(failure) = value.setattr("_spark_message_parameters", dict) {
                tracing::warn!(error = %failure, "join refusal params setattr failed");
            }
            raised
        });
    }
    crate::AnalysisException::new_err(message)
}

fn refusal_params(message: &str) -> Option<(&'static str, Vec<(&'static str, String)>)> {
    if let Some(rest) = message.strip_prefix(NONDET_PREFIX)
        && let Some(expr) = rest.strip_suffix(NONDET_SUFFIX)
    {
        return Some((NONDET_CONDITION, vec![("sqlExprs", format!("\"{expr}\""))]));
    }
    if let Some(rest) = message.strip_prefix(NOT_BOOLEAN_PREFIX)
        && let Some(middle) = rest.strip_suffix(NOT_BOOLEAN_SUFFIX)
        && let Some((condition, data_type)) = middle.rsplit_once(NOT_BOOLEAN_MIDDLE)
    {
        return Some((
            NOT_BOOLEAN_CONDITION,
            vec![
                ("joinCondition", format!("\"{condition}\"")),
                ("conditionType", format!("\"{data_type}\"")),
            ],
        ));
    }
    None
}
