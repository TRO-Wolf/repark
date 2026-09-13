use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Case, Cast, Expr, TryCast, lit};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::PyColumn;
use super::expr_build::parse_data_type;
use super::function_dispatch::call_scalar_expr;
use crate::AnalysisException;
use crate::fence::fenced;

fn wrap_binary(left: &str, spark_op: &str, right: &str) -> String {
    format!("({left} {spark_op} {right})")
}

fn wrap_not_equal(left: &str, right: &str) -> String {
    format!("(NOT ({left} = {right}))")
}

fn wrap_negative_display(child: &str) -> String {
    format!("negative({child})")
}

fn wrap_negative_sql(child: &str) -> String {
    format!("(-({child}))")
}

fn wrap_null_safe_display(left: &str, right: &str) -> String {
    format!("({left} <=> {right})")
}

fn wrap_null_safe_sql(left: &str, right: &str) -> String {
    format!("({left} IS NOT DISTINCT FROM {right})")
}

fn wrap_substr(child: &str, start: &str, length: &str) -> String {
    format!("substr({child}, {start}, {length})")
}

fn wrap_string_predicate_display(left: &str, shown: &str, right: &str) -> String {
    format!("{left}.{shown}({right})")
}

fn wrap_string_predicate_sql(call_name: &str, left: &str, right: &str) -> String {
    format!("{call_name}({left}, {right})")
}

fn wrap_invert(child: &str) -> String {
    format!("(NOT {child})")
}

fn wrap_is_null(child: &str) -> String {
    format!("({child} IS NULL)")
}

fn wrap_is_not_null(child: &str) -> String {
    format!("({child} IS NOT NULL)")
}

fn wrap_alias(child: &str, name: &str) -> String {
    format!("{child} AS {name}")
}

fn wrap_index_display(child: &str, key: &str) -> String {
    format!("{child}[{key}]")
}

fn wrap_index_sql(child: &str, key: &str) -> String {
    format!("({child})[{key}]")
}

fn wrap_field_sql(child: &str, quoted_field: &str) -> String {
    format!("({child}).{quoted_field}")
}

fn wrap_cast(keyword: &str, child: &str, spark_type: &str) -> String {
    format!("{keyword}({child} AS {spark_type})")
}

fn format_case_body(arms: &[(String, String)], else_part: Option<&str>) -> String {
    let when_then = arms
        .iter()
        .map(|(condition, value)| format!("WHEN {condition} THEN {value}"))
        .collect::<Vec<_>>()
        .join(" ");
    match else_part {
        None => format!("CASE {when_then} END"),
        Some(else_part) => format!("CASE {when_then} ELSE {else_part} END"),
    }
}

fn apply_binary_op(left: &PyColumn, right: &PyColumn, op_method: &str) -> PyResult<Expr> {
    let left_expr = left.expr();
    let right_expr = right.expr();
    match op_method {
        "add" => Ok(left_expr + right_expr),
        "sub" => Ok(left_expr - right_expr),
        "mul" => Ok(left_expr * right_expr),
        "div" => {
            let numerator = Expr::Cast(Cast::new(Box::new(left_expr), DataType::Float64));
            let denominator = Expr::Cast(Cast::new(Box::new(right_expr), DataType::Float64));
            Ok(numerator / denominator)
        }
        "modulo" => Ok(left_expr % right_expr),
        "eq" => Ok(left_expr.eq(right_expr)),
        "lt" => Ok(left_expr.lt(right_expr)),
        "gt" => Ok(left_expr.gt(right_expr)),
        "le" => Ok(left_expr.lt_eq(right_expr)),
        "ge" => Ok(left_expr.gt_eq(right_expr)),
        "and_" => Ok(left_expr.and(right_expr)),
        "or_" => Ok(left_expr.or(right_expr)),
        other => Err(PyValueError::new_err(format!("unknown binary op {other}"))),
    }
}

fn call_two(name: &str, left: &PyColumn, right: &PyColumn) -> PyResult<PyColumn> {
    Ok(PyColumn::from_expr(call_scalar_expr(
        name,
        vec![left.expr(), right.expr()],
    )?))
}

fn engine_cast(inner: &PyColumn, engine_type: &str, keyword: &str) -> PyResult<PyColumn> {
    let data_type = parse_data_type(engine_type).map_err(AnalysisException::new_err)?;
    let expr = match keyword {
        "CAST" => Expr::Cast(Cast::new(Box::new(inner.expr()), data_type)),
        "TRY_CAST" => Expr::TryCast(TryCast::new(Box::new(inner.expr()), data_type)),
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown cast keyword {other}"
            )));
        }
    };
    Ok(PyColumn::from_expr(expr))
}

type RenderedParts = (PyColumn, String, String, Option<String>);

#[pyclass(name = "PyColumnParts", module = "repark._native")]
pub struct PyColumnParts;

#[allow(
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::missing_errors_doc
)]
#[pymethods]
impl PyColumnParts {
    #[staticmethod]
    fn binary(
        left: &PyColumn,
        right: &PyColumn,
        op_method: &str,
        spark_op: &str,
        left_parts: (String, String, String),
        right_parts: (String, String, String),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.binary", {
            let inner = PyColumn::from_expr(apply_binary_op(left, right, op_method)?);
            let (left_display, left_sql, left_join) = left_parts;
            let (right_display, right_sql, right_join) = right_parts;
            Ok((
                inner,
                wrap_binary(&left_display, spark_op, &right_display),
                wrap_binary(&left_sql, spark_op, &right_sql),
                Some(wrap_binary(&left_join, spark_op, &right_join)),
            ))
        })
    }

    #[staticmethod]
    fn not_equal(
        left: &PyColumn,
        right: &PyColumn,
        left_parts: (String, String, String),
        right_parts: (String, String, String),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.not_equal", {
            let inner = PyColumn::from_expr(left.expr().not_eq(right.expr()));
            let (left_display, left_sql, left_join) = left_parts;
            let (right_display, right_sql, right_join) = right_parts;
            Ok((
                inner,
                wrap_not_equal(&left_display, &right_display),
                wrap_not_equal(&left_sql, &right_sql),
                Some(wrap_not_equal(&left_join, &right_join)),
            ))
        })
    }

    #[staticmethod]
    fn unary_neg(
        inner: &PyColumn,
        child_display: &str,
        child_sql: &str,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.unary_neg", {
            let display = wrap_negative_display(child_display);
            let aliased = PyColumn::from_expr((lit(0_i32) - inner.expr()).alias(&display));
            Ok((aliased, display, wrap_negative_sql(child_sql), None))
        })
    }

    #[staticmethod]
    fn eq_null_safe(
        left: &PyColumn,
        right: &PyColumn,
        left_parts: (String, String, String),
        right_parts: (String, String, String),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.eq_null_safe", {
            let inner = call_two("eq_null_safe", left, right)?;
            let (left_display, left_sql, left_join) = left_parts;
            let (right_display, right_sql, right_join) = right_parts;
            Ok((
                inner,
                wrap_null_safe_display(&left_display, &right_display),
                wrap_null_safe_sql(&left_sql, &right_sql),
                Some(wrap_null_safe_sql(&left_join, &right_join)),
            ))
        })
    }

    #[staticmethod]
    fn substr(
        inner: &PyColumn,
        start: &PyColumn,
        length: &PyColumn,
        display_parts: (String, String, String),
        sql_parts: (String, String, String),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.substr", {
            let native = PyColumn::from_expr(call_scalar_expr(
                "substr",
                vec![inner.expr(), start.expr(), length.expr()],
            )?);
            let (child_display, start_display, length_display) = display_parts;
            let (child_sql, start_sql, length_sql) = sql_parts;
            Ok((
                native,
                wrap_substr(&child_display, &start_display, &length_display),
                wrap_substr(&child_sql, &start_sql, &length_sql),
                None,
            ))
        })
    }

    #[staticmethod]
    fn string_predicate(
        inner: &PyColumn,
        other: &PyColumn,
        call_name: &str,
        shown: &str,
        display_parts: (String, String),
        sql_parts: (String, String),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.string_predicate", {
            let native = call_two(call_name, inner, other)?;
            let (left_display, right_display) = display_parts;
            let (left_sql, right_sql) = sql_parts;
            Ok((
                native,
                wrap_string_predicate_display(&left_display, shown, &right_display),
                wrap_string_predicate_sql(call_name, &left_sql, &right_sql),
                None,
            ))
        })
    }

    #[staticmethod]
    fn bitwise(
        inner: &PyColumn,
        other: &PyColumn,
        call_name: &str,
        spark_op: &str,
        display_parts: (String, String),
        sql_parts: (String, String),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.bitwise", {
            let native = call_two(call_name, inner, other)?;
            let (left_display, right_display) = display_parts;
            let (left_sql, right_sql) = sql_parts;
            Ok((
                native,
                wrap_binary(&left_display, spark_op, &right_display),
                wrap_binary(&left_sql, spark_op, &right_sql),
                None,
            ))
        })
    }

    #[staticmethod]
    fn invert(
        inner: &PyColumn,
        child_display: &str,
        child_sql: &str,
        child_join: &str,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.invert", {
            Ok((
                PyColumn::from_expr(!inner.expr()),
                wrap_invert(child_display),
                wrap_invert(child_sql),
                Some(wrap_invert(child_join)),
            ))
        })
    }

    #[staticmethod]
    fn is_null(
        inner: &PyColumn,
        child_display: &str,
        child_sql: &str,
        child_join: &str,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.is_null", {
            Ok((
                PyColumn::from_expr(inner.expr().is_null()),
                wrap_is_null(child_display),
                wrap_is_null(child_sql),
                Some(wrap_is_null(child_join)),
            ))
        })
    }

    #[staticmethod]
    fn is_not_null(
        inner: &PyColumn,
        child_display: &str,
        child_sql: &str,
        child_join: &str,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.is_not_null", {
            Ok((
                PyColumn::from_expr(inner.expr().is_not_null()),
                wrap_is_not_null(child_display),
                wrap_is_not_null(child_sql),
                Some(wrap_is_not_null(child_join)),
            ))
        })
    }

    #[staticmethod]
    fn case_when(
        when_thens: Vec<(PyColumn, PyColumn)>,
        otherwise: Option<PyColumn>,
        display_arms: Vec<(String, String)>,
        sql_arms: Vec<(String, String)>,
        join_arms: Vec<(String, String)>,
        else_parts: Option<(String, String, String)>,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.case_when", {
            if when_thens.len() != display_arms.len()
                || display_arms.len() != sql_arms.len()
                || sql_arms.len() != join_arms.len()
            {
                return Err(PyValueError::new_err(
                    "case_when arm lists must be the same length",
                ));
            }
            if otherwise.is_some() != else_parts.is_some() {
                return Err(PyValueError::new_err(
                    "case_when otherwise and else_parts must both be set or both be omitted",
                ));
            }
            let when_then_expr = when_thens
                .into_iter()
                .map(|(condition, value)| (Box::new(condition.expr()), Box::new(value.expr())))
                .collect();
            let else_expr = otherwise.map(|column| Box::new(column.expr()));
            let inner = PyColumn::from_expr(Expr::Case(Case {
                expr: None,
                when_then_expr,
                else_expr,
            }));
            let else_display = else_parts.as_ref().map(|parts| parts.0.as_str());
            let else_sql = else_parts.as_ref().map(|parts| parts.1.as_str());
            let else_join = else_parts.as_ref().map(|parts| parts.2.as_str());
            Ok((
                inner,
                format_case_body(&display_arms, else_display),
                format_case_body(&sql_arms, else_sql),
                Some(format_case_body(&join_arms, else_join)),
            ))
        })
    }

    #[staticmethod]
    fn alias(
        inner: &PyColumn,
        child_display: &str,
        child_sql: &str,
        name: &str,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.alias", {
            Ok((
                PyColumn::from_expr(inner.expr().alias(name)),
                wrap_alias(child_display, name),
                child_sql.to_string(),
                None,
            ))
        })
    }

    #[staticmethod]
    fn getitem(
        inner: &PyColumn,
        key: &PyColumn,
        kind: &str,
        display_parts: (String, String),
        sql_parts: (String, String),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.getitem", {
            let (child_display, key_display) = display_parts;
            let (child_sql, key_sql) = sql_parts;
            let (call_name, spark_display, sql_expr) = match kind {
                "index" => (
                    "array_element",
                    wrap_index_display(&child_display, &key_display),
                    wrap_index_sql(&child_sql, &key_sql),
                ),
                "field" => (
                    "get_field",
                    wrap_index_display(&child_display, &key_display),
                    wrap_field_sql(&child_sql, &key_sql),
                ),
                "key" => (
                    "getitem",
                    wrap_index_display(&child_display, &key_display),
                    wrap_index_sql(&child_sql, &key_sql),
                ),
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown getitem kind {other}"
                    )));
                }
            };
            let native = call_two(call_name, inner, key)?;
            Ok((native, spark_display, sql_expr, None))
        })
    }

    #[staticmethod]
    fn cast(
        inner: &PyColumn,
        child_parts: (String, String, String),
        engine_type: &str,
        spark_type: &str,
        keyword: &str,
        flags: (bool, bool),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.cast", {
            let (apply_engine, keep_child_sql) = flags;
            let native = if apply_engine {
                engine_cast(inner, engine_type, keyword)?
            } else {
                inner.clone()
            };
            let (child_display, child_sql, child_join) = child_parts;
            let spark_display = wrap_cast(keyword, &child_display, spark_type);
            if keep_child_sql {
                Ok((native, spark_display, child_sql, None))
            } else {
                Ok((
                    native,
                    spark_display,
                    wrap_cast(keyword, &child_sql, spark_type),
                    Some(wrap_cast(keyword, &child_join, spark_type)),
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_paren_spaces_match_python() {
        assert_eq!(wrap_binary("x", "+", "1"), "(x + 1)");
        assert_eq!(wrap_binary("x", "AND", "flag"), "(x AND flag)");
    }

    #[test]
    fn not_equal_uses_not_equals_form() {
        assert_eq!(wrap_not_equal("x", "1"), "(NOT (x = 1))");
    }

    #[test]
    fn unary_neg_display_and_sql_diverge() {
        assert_eq!(wrap_negative_display("x"), "negative(x)");
        assert_eq!(wrap_negative_sql("x"), "(-(x))");
    }

    #[test]
    fn null_safe_sql_is_distinct_from() {
        assert_eq!(wrap_null_safe_display("x", "NULL"), "(x <=> NULL)");
        assert_eq!(
            wrap_null_safe_sql("x", "NULL"),
            "(x IS NOT DISTINCT FROM NULL)"
        );
    }

    #[test]
    fn case_open_and_closed_match_python() {
        let arms = vec![
            ("(x > 0)".to_string(), "1".to_string()),
            ("(x < 0)".to_string(), "-1".to_string()),
        ];
        assert_eq!(
            format_case_body(&arms, None),
            "CASE WHEN (x > 0) THEN 1 WHEN (x < 0) THEN -1 END"
        );
        assert_eq!(
            format_case_body(&arms[..1], Some("0")),
            "CASE WHEN (x > 0) THEN 1 ELSE 0 END"
        );
    }

    #[test]
    fn cast_and_try_cast_keywords() {
        assert_eq!(wrap_cast("CAST", "x", "DOUBLE"), "CAST(x AS DOUBLE)");
        assert_eq!(wrap_cast("TRY_CAST", "x", "INT"), "TRY_CAST(x AS INT)");
    }

    #[test]
    fn getitem_index_field_and_key_shapes() {
        assert_eq!(wrap_index_display("arr", "0"), "arr[0]");
        assert_eq!(wrap_index_sql("arr", "0"), "(arr)[0]");
        assert_eq!(wrap_field_sql("st", "\"a\""), "(st).\"a\"");
        assert_eq!(wrap_index_display("m", "'k'"), "m['k']");
        assert_eq!(wrap_alias("x", "z"), "x AS z");
        assert_eq!(wrap_invert("flag"), "(NOT flag)");
        assert_eq!(wrap_is_null("x"), "(x IS NULL)");
        assert_eq!(
            wrap_string_predicate_display("s", "startswith", "a"),
            "s.startswith(a)"
        );
        assert_eq!(
            wrap_string_predicate_sql("starts_with", "s", "'a'"),
            "starts_with(s, 'a')"
        );
        assert_eq!(wrap_substr("s", "1", "2"), "substr(s, 1, 2)");
    }
}
