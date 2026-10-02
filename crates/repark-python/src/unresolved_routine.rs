pub(crate) fn unresolved_routine(message: &str) -> Option<&str> {
    let (_, tail) = message.split_once("Invalid function '")?;
    let (name, _) = tail.split_once('\'')?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return None;
    }
    Some(name)
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn unknown_routine_to_py_err(
    sql: &str,
    err: datafusion::error::DataFusionError,
) -> pyo3::PyErr {
    match repark_core::map_unknown_routine_message(sql, &err.to_string()) {
        Some(message) => crate::to_py_err(repark_core::Error::Analysis(message)),
        None => crate::datafusion_to_py_err(err),
    }
}
