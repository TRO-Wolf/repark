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
        other => {
            return Err(PyValueError::new_err(format!(
                "call_scalar({other}) has no door-converged kernel"
            )));
        }
    };
    Ok(expr)
}
