use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::ScalarFunction;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[allow(clippy::too_many_lines)]
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
        "posexplode" | "posexplode_outer" | "inline" | "inline_outer" | "__repark_gen_alias" => {
            need_at_least(1)?;
            let udf_name: &'static str = match name {
                "posexplode" => "posexplode",
                "posexplode_outer" => "posexplode_outer",
                "inline" => "inline",
                "inline_outer" => "inline_outer",
                _ => "__repark_gen_alias",
            };
            Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::generator::generator_udf(udf_name),
                exprs.clone(),
            ))
        }
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
        "array_append" => {
            need(2)?;
            Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::collection::spark_array_append_udf(),
                vec![exprs[0].clone(), exprs[1].clone()],
            ))
        }
        "array_prepend" => {
            need(2)?;
            Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::collection::spark_array_prepend_udf(),
                vec![exprs[0].clone(), exprs[1].clone()],
            ))
        }
        "window_time" => {
            need(1)?;
            Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::spark_window_time::window_time_udf(),
                vec![exprs[0].clone()],
            ))
        }
        "arrays_zip" => repark_functions::expr_fn::arrays_zip(exprs.clone()),
        "map_concat" => repark_functions::expr_fn::map_concat(exprs.clone()),
        "create_map" => repark_functions::expr_fn::create_map(exprs.clone()),
        "make_timestamp" | "try_make_timestamp" => {
            if !matches!(exprs.len(), 2 | 3 | 6 | 7) {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2, 3, 6 or 7 args, got {}",
                    exprs.len()
                )));
            }
            match name {
                "make_timestamp" => repark_functions::expr_fn::make_timestamp(exprs.clone()),
                _ => repark_functions::expr_fn::try_make_timestamp(exprs.clone()),
            }
        }
        "make_timestamp_ltz" | "try_make_timestamp_ltz" => {
            if !matches!(exprs.len(), 2 | 3 | 6 | 7) {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2, 3, 6 or 7 args, got {}",
                    exprs.len()
                )));
            }
            match name {
                "make_timestamp_ltz" => {
                    repark_functions::expr_fn::make_timestamp_ltz(exprs.clone())
                }
                _ => repark_functions::expr_fn::try_make_timestamp_ltz(exprs.clone()),
            }
        }
        "make_timestamp_ntz" | "try_make_timestamp_ntz" => {
            if !matches!(exprs.len(), 2 | 6) {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2 or 6 args, got {}",
                    exprs.len()
                )));
            }
            match name {
                "make_timestamp_ntz" => {
                    repark_functions::expr_fn::make_timestamp_ntz(exprs.clone())
                }
                _ => repark_functions::expr_fn::try_make_timestamp_ntz(exprs.clone()),
            }
        }
        "make_ym_interval" => {
            if exprs.len() > 2 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects at most 2 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::make_ym_interval(exprs.clone())
        }
        "try_make_interval" => {
            if exprs.len() > 7 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects at most 7 args, got {}",
                    exprs.len()
                )));
            }
            repark_functions::expr_fn::try_make_interval(exprs.clone())
        }
        "months_between" | "convert_timezone" => {
            if !matches!(exprs.len(), 2 | 3) {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects 2 or 3 args, got {}",
                    exprs.len()
                )));
            }
            match name {
                "months_between" => repark_functions::expr_fn::months_between(exprs.clone()),
                _ => repark_functions::expr_fn::convert_timezone(exprs.clone()),
            }
        }
        "localtimestamp" => {
            need(0)?;
            repark_functions::expr_fn::localtimestamp()
        }
        "timestampadd" => {
            need(3)?;
            repark_functions::expr_fn::timestampadd(exprs.clone())
        }
        "timestampdiff" => {
            need(3)?;
            repark_functions::expr_fn::timestampdiff(exprs.clone())
        }
        "datediff" => {
            need(2)?;
            repark_functions::expr_fn::datediff(exprs[0].clone(), exprs[1].clone())
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "call_scalar: unsupported function {other:?}"
            )));
        }
    };
    Ok(expr)
}
