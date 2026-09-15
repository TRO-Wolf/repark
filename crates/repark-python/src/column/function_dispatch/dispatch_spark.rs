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
    let expr = match name {
        "abs" => {
            need(1)?;
            repark_functions::expr_fn::abs(exprs[0].clone())
        }
        "hypot" => {
            need(2)?;
            repark_functions::expr_fn::hypot(exprs[0].clone(), exprs[1].clone())
        }
        "bin" => {
            need(1)?;
            repark_functions::expr_fn::bin(exprs[0].clone())
        }
        "rint" => {
            need(1)?;
            repark_functions::expr_fn::rint(exprs[0].clone())
        }
        "base64" => {
            need(1)?;
            repark_functions::expr_fn::base64(exprs[0].clone())
        }
        "unbase64" => {
            need(1)?;
            repark_functions::expr_fn::unbase64(exprs[0].clone())
        }
        "size" => {
            need(1)?;
            repark_functions::expr_fn::size(exprs[0].clone())
        }
        "cardinality" => {
            need(1)?;
            repark_functions::expr_fn::cardinality(exprs[0].clone())
        }
        "array_contains" | "array_has" => {
            need(2)?;
            repark_functions::expr_fn::array_contains(exprs[0].clone(), exprs[1].clone())
        }
        "ascii" => {
            need(1)?;
            repark_functions::expr_fn::ascii(exprs[0].clone())
        }
        "length" | "character_length" | "char_length" => {
            need(1)?;
            repark_functions::expr_fn::length(exprs[0].clone())
        }
        "reverse" => {
            need(1)?;
            repark_functions::expr_fn::reverse(exprs[0].clone())
        }
        "sequence" | "generate_series" | "gen_series" => {
            if exprs.len() < 2 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects at least 2 args, got {}",
                    exprs.len()
                )));
            }
            if exprs.len() > 3 {
                return Err(PyValueError::new_err(format!(
                    "call_scalar({name}) expects at most 3 args, got {}",
                    exprs.len()
                )));
            }
            let step = if exprs.len() >= 3 {
                exprs[2].clone()
            } else {
                datafusion::logical_expr::lit(1i64)
            };
            let check_args = vec![exprs[0].clone(), exprs[1].clone(), step];
            repark_functions::cardinality::refuse_facade_literal_expansion("sequence", &check_args)
                .map_err(crate::datafusion_to_py_err)?;
            let mut forwarded = vec![exprs[0].clone(), exprs[1].clone()];
            if exprs.len() >= 3 {
                forwarded.push(exprs[2].clone());
            }
            repark_functions::expr_fn::sequence(forwarded)
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "call_scalar({other}) has no door-converged kernel"
            )));
        }
    };
    Ok(expr)
}
