use datafusion::functions_nested::map::map as nested_map;
use datafusion::logical_expr::Expr;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn call_scalar_expr(name: &str, exprs: Vec<Expr>) -> PyResult<Expr> {
    let need = |n: usize| -> PyResult<()> {
        if exprs.len() != n {
            return Err(PyValueError::new_err(format!(
                "call_scalar({name}) expects {n} args, got {}",
                exprs.len()
            )));
        }
        Ok(())
    };
    let need_at_least = |n: usize| -> PyResult<()> {
        if exprs.len() < n {
            return Err(PyValueError::new_err(format!(
                "call_scalar({name}) expects at least {n} args, got {}",
                exprs.len()
            )));
        }
        Ok(())
    };
    let expr = match name {
        "get_json_object" => {
            need(2)?;
            repark_functions::expr_fn::get_json_object(exprs[0].clone(), exprs[1].clone())
        }
        "json_array_length" => {
            need(1)?;
            repark_functions::expr_fn::json_array_length(exprs[0].clone())
        }
        "json_object_keys" => {
            need(1)?;
            repark_functions::expr_fn::json_object_keys(exprs[0].clone())
        }
        "schema_of_json" => {
            need_at_least(1)?;
            repark_functions::expr_fn::schema_of_json(exprs.clone())
        }
        "to_json" => {
            need_at_least(1)?;
            repark_functions::expr_fn::to_json(exprs.clone())
        }
        "from_json" => {
            need_at_least(2)?;
            repark_functions::expr_fn::from_json(exprs.clone())
        }
        "array_insert" => {
            need(3)?;
            repark_functions::expr_fn::array_insert(
                exprs[0].clone(),
                exprs[1].clone(),
                exprs[2].clone(),
            )
        }
        "arrays_zip" => repark_functions::expr_fn::arrays_zip(exprs.clone()),
        "map_concat" => repark_functions::expr_fn::map_concat(exprs.clone()),
        "create_map" => {
            if !exprs.len().is_multiple_of(2) {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects an even number of key/value args, got {}",
                    exprs.len()
                )));
            }
            let keys = exprs.iter().step_by(2).cloned().collect::<Vec<Expr>>();
            let values = exprs
                .iter()
                .skip(1)
                .step_by(2)
                .cloned()
                .collect::<Vec<Expr>>();
            nested_map(keys, values)
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "call_scalar: unsupported function {other:?}"
            )));
        }
    };
    Ok(expr)
}
