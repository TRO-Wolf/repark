use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Case, Cast, Expr, TryCast, lit};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyList;

use super::PyColumn;
use super::expr_build::parse_data_type;
use super::function_dispatch::call_scalar_expr;
use crate::AnalysisException;
use crate::fence::fenced;

mod construct;

fn wrap_binary(left: &str, spark_op: &str, right: &str) -> String {
    let mut out = String::with_capacity(left.len() + spark_op.len() + right.len() + 4);
    out.push('(');
    out.push_str(left);
    out.push(' ');
    out.push_str(spark_op);
    out.push(' ');
    out.push_str(right);
    out.push(')');
    out
}

fn wrap_not_equal(left: &str, right: &str) -> String {
    let mut out = String::with_capacity(left.len() + right.len() + 11);
    out.push_str("(NOT (");
    out.push_str(left);
    out.push_str(" = ");
    out.push_str(right);
    out.push_str("))");
    out
}

fn wrap_negative_display(child: &str) -> String {
    let mut out = String::with_capacity(child.len() + 10);
    out.push_str("negative(");
    out.push_str(child);
    out.push(')');
    out
}

fn wrap_negative_sql(child: &str) -> String {
    let mut out = String::with_capacity(child.len() + 5);
    out.push_str("(-(");
    out.push_str(child);
    out.push_str("))");
    out
}

fn wrap_null_safe_display(left: &str, right: &str) -> String {
    let mut out = String::with_capacity(left.len() + right.len() + 7);
    out.push('(');
    out.push_str(left);
    out.push_str(" <=> ");
    out.push_str(right);
    out.push(')');
    out
}

fn wrap_null_safe_sql(left: &str, right: &str) -> String {
    let mut out = String::with_capacity(left.len() + right.len() + 24);
    out.push('(');
    out.push_str(left);
    out.push_str(" IS NOT DISTINCT FROM ");
    out.push_str(right);
    out.push(')');
    out
}

fn wrap_substr(child: &str, start: &str, length: &str) -> String {
    let mut out = String::with_capacity(child.len() + start.len() + length.len() + 12);
    out.push_str("substr(");
    out.push_str(child);
    out.push_str(", ");
    out.push_str(start);
    out.push_str(", ");
    out.push_str(length);
    out.push(')');
    out
}

fn wrap_string_predicate_display(left: &str, shown: &str, right: &str) -> String {
    let mut out = String::with_capacity(left.len() + shown.len() + right.len() + 3);
    out.push_str(left);
    out.push('.');
    out.push_str(shown);
    out.push('(');
    out.push_str(right);
    out.push(')');
    out
}

fn wrap_string_predicate_sql(call_name: &str, left: &str, right: &str) -> String {
    let mut out = String::with_capacity(call_name.len() + left.len() + right.len() + 4);
    out.push_str(call_name);
    out.push('(');
    out.push_str(left);
    out.push_str(", ");
    out.push_str(right);
    out.push(')');
    out
}

fn wrap_invert(child: &str) -> String {
    let mut out = String::with_capacity(child.len() + 6);
    out.push_str("(NOT ");
    out.push_str(child);
    out.push(')');
    out
}

fn wrap_is_null(child: &str) -> String {
    let mut out = String::with_capacity(child.len() + 10);
    out.push('(');
    out.push_str(child);
    out.push_str(" IS NULL)");
    out
}

fn wrap_is_not_null(child: &str) -> String {
    let mut out = String::with_capacity(child.len() + 14);
    out.push('(');
    out.push_str(child);
    out.push_str(" IS NOT NULL)");
    out
}

fn wrap_alias(child: &str, name: &str) -> String {
    let mut out = String::with_capacity(child.len() + name.len() + 4);
    out.push_str(child);
    out.push_str(" AS ");
    out.push_str(name);
    out
}

fn wrap_index_display(child: &str, key: &str) -> String {
    let mut out = String::with_capacity(child.len() + key.len() + 2);
    out.push_str(child);
    out.push('[');
    out.push_str(key);
    out.push(']');
    out
}

fn wrap_index_sql(child: &str, key: &str) -> String {
    let mut out = String::with_capacity(child.len() + key.len() + 4);
    out.push('(');
    out.push_str(child);
    out.push_str(")[");
    out.push_str(key);
    out.push(']');
    out
}

fn wrap_field_sql(child: &str, quoted_field: &str) -> String {
    let mut out = String::with_capacity(child.len() + quoted_field.len() + 4);
    out.push('(');
    out.push_str(child);
    out.push_str(").");
    out.push_str(quoted_field);
    out
}

fn wrap_cast(keyword: &str, child: &str, spark_type: &str) -> String {
    let mut out = String::with_capacity(keyword.len() + child.len() + spark_type.len() + 6);
    out.push_str(keyword);
    out.push('(');
    out.push_str(child);
    out.push_str(" AS ");
    out.push_str(spark_type);
    out.push(')');
    out
}

fn wrap_call<S: AsRef<str>>(name: &str, parts: &[S]) -> String {
    let size = name.len()
        + 2
        + parts
            .iter()
            .map(|part| part.as_ref().len() + 2)
            .sum::<usize>();
    let mut out = String::with_capacity(size);
    out.push_str(name);
    out.push('(');
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(part.as_ref());
    }
    out.push(')');
    out
}

fn str_list(list: &Bound<'_, PyList>) -> PyResult<Vec<PyBackedStr>> {
    list.iter()
        .map(|item| item.extract::<PyBackedStr>())
        .collect()
}

fn format_case_body(arms: Vec<(String, String)>, else_part: Option<&str>) -> String {
    let size = arms
        .iter()
        .map(|(condition, value)| condition.len() + value.len() + 12)
        .sum::<usize>()
        + else_part.map_or(0, |part| part.len() + 6)
        + 8;
    let mut out = String::with_capacity(size);
    out.push_str("CASE");
    for (condition, value) in arms {
        out.push_str(" WHEN ");
        out.push_str(&condition);
        out.push_str(" THEN ");
        out.push_str(&value);
    }
    if let Some(part) = else_part {
        out.push_str(" ELSE ");
        out.push_str(part);
    }
    out.push_str(" END");
    out
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

#[allow(clippy::must_use_candidate, clippy::missing_errors_doc)]
#[pymethods]
impl PyColumnParts {
    #[staticmethod]
    fn binary(
        left: &PyColumn,
        right: &PyColumn,
        op_method: &str,
        spark_op: &str,
        left_parts: (&str, &str, &str),
        right_parts: (&str, &str, &str),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.binary", {
            let inner = PyColumn::from_expr(apply_binary_op(left, right, op_method)?);
            let (left_display, left_sql, left_join) = left_parts;
            let (right_display, right_sql, right_join) = right_parts;
            Ok((
                inner,
                wrap_binary(left_display, spark_op, right_display),
                wrap_binary(left_sql, spark_op, right_sql),
                Some(wrap_binary(left_join, spark_op, right_join)),
            ))
        })
    }

    #[staticmethod]
    fn not_equal(
        left: &PyColumn,
        right: &PyColumn,
        left_parts: (&str, &str, &str),
        right_parts: (&str, &str, &str),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.not_equal", {
            let inner = PyColumn::from_expr(left.expr().not_eq(right.expr()));
            let (left_display, left_sql, left_join) = left_parts;
            let (right_display, right_sql, right_join) = right_parts;
            Ok((
                inner,
                wrap_not_equal(left_display, right_display),
                wrap_not_equal(left_sql, right_sql),
                Some(wrap_not_equal(left_join, right_join)),
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
        left_parts: (&str, &str, &str),
        right_parts: (&str, &str, &str),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.eq_null_safe", {
            let inner = call_two("eq_null_safe", left, right)?;
            let (left_display, left_sql, left_join) = left_parts;
            let (right_display, right_sql, right_join) = right_parts;
            Ok((
                inner,
                wrap_null_safe_display(left_display, right_display),
                wrap_null_safe_sql(left_sql, right_sql),
                Some(wrap_null_safe_sql(left_join, right_join)),
            ))
        })
    }

    #[staticmethod]
    fn update_fields(
        inner: &PyColumn,
        ops: Vec<String>,
        paths: Vec<String>,
        values: Vec<PyColumn>,
        struct_parts: (&str, &str, &str),
        value_parts: Vec<(String, String, String)>,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.update_fields", {
            if ops.len() != paths.len() {
                return Err(PyValueError::new_err(
                    "update_fields ops and paths must match",
                ));
            }
            let mut args = Vec::with_capacity(1 + ops.len() * 2 + values.len());
            args.push(inner.expr());
            let mut values = values.into_iter();
            let mut value_parts = value_parts.iter();
            let (struct_display, struct_sql, struct_join) = struct_parts;
            let mut display = format!("update_fields({struct_display}");
            let mut sql = format!("update_fields({struct_sql}");
            let mut join = format!("update_fields({struct_join}");
            for (op, path) in ops.iter().zip(&paths) {
                args.push(lit(op.clone()));
                args.push(lit(path.clone()));
                let quoted = path.replace('\'', "''");
                match op.as_str() {
                    "with" => {
                        let value = values.next().ok_or_else(|| {
                            PyValueError::new_err("update_fields 'with' op needs a value")
                        })?;
                        let (value_display, value_sql, value_join) =
                            value_parts.next().ok_or_else(|| {
                                PyValueError::new_err("update_fields 'with' op needs value parts")
                            })?;
                        args.push(value.expr());
                        let _ = std::fmt::Write::write_fmt(
                            &mut display,
                            format_args!(", WithField({value_display})"),
                        );
                        let _ = std::fmt::Write::write_fmt(
                            &mut sql,
                            format_args!(", 'with', '{quoted}', {value_sql}"),
                        );
                        let _ = std::fmt::Write::write_fmt(
                            &mut join,
                            format_args!(", 'with', '{quoted}', {value_join}"),
                        );
                    }
                    "drop" => {
                        display.push_str(", dropfield()");
                        let _ = std::fmt::Write::write_fmt(
                            &mut sql,
                            format_args!(", 'drop', '{quoted}'"),
                        );
                        let _ = std::fmt::Write::write_fmt(
                            &mut join,
                            format_args!(", 'drop', '{quoted}'"),
                        );
                    }
                    other => {
                        return Err(PyValueError::new_err(format!(
                            "update_fields op must be 'with' or 'drop', got {other}"
                        )));
                    }
                }
            }
            display.push(')');
            sql.push(')');
            join.push(')');
            if values.next().is_some() || value_parts.next().is_some() {
                return Err(PyValueError::new_err(
                    "update_fields value lists must match the 'with' op count",
                ));
            }
            let native = PyColumn::from_expr(repark_core::update_fields_call(args));
            Ok((native, display, sql, Some(join)))
        })
    }

    #[staticmethod]
    fn repark_isnan(inner: &PyColumn, child_parts: (&str, &str, &str)) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.repark_isnan", {
            let (child_display, child_sql, child_join) = child_parts;
            Ok((
                PyColumn::from_expr(repark_core::repark_isnan_call(inner.expr())),
                format!("isnan({child_display})"),
                format!("repark_isnan({child_sql})"),
                Some(format!("repark_isnan({child_join})")),
            ))
        })
    }

    #[staticmethod]
    fn in_list(
        inner: &PyColumn,
        values: Vec<PyColumn>,
        left_parts: (&str, &str, &str),
        right_parts: Vec<(String, String, String)>,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.in_list", {
            if values.len() != right_parts.len() {
                return Err(PyValueError::new_err("in_list values and parts must match"));
            }
            let exprs = values.iter().map(PyColumn::expr).collect();
            let native = PyColumn::from_expr(inner.expr().in_list(exprs, false));
            let wrap = |left: &str, rights: &[&str]| -> String {
                format!("({left} IN ({}))", rights.join(", "))
            };
            let displays: Vec<&str> = right_parts.iter().map(|part| part.0.as_str()).collect();
            let sqls: Vec<&str> = right_parts.iter().map(|part| part.1.as_str()).collect();
            let joins: Vec<&str> = right_parts.iter().map(|part| part.2.as_str()).collect();
            let (left_display, left_sql, left_join) = left_parts;
            Ok((
                native,
                wrap(left_display, &displays),
                wrap(left_sql, &sqls),
                Some(wrap(left_join, &joins)),
            ))
        })
    }

    #[staticmethod]
    fn substr(
        inner: &PyColumn,
        start: &PyColumn,
        length: &PyColumn,
        display_parts: (&str, &str, &str),
        sql_parts: (&str, &str, &str),
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
                wrap_substr(child_display, start_display, length_display),
                wrap_substr(child_sql, start_sql, length_sql),
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
        display_parts: (&str, &str),
        sql_parts: (&str, &str),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.string_predicate", {
            let native = call_two(call_name, inner, other)?;
            let (left_display, right_display) = display_parts;
            let (left_sql, right_sql) = sql_parts;
            Ok((
                native,
                wrap_string_predicate_display(left_display, shown, right_display),
                wrap_string_predicate_sql(call_name, left_sql, right_sql),
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
        display_parts: (&str, &str),
        sql_parts: (&str, &str),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.bitwise", {
            let native = call_two(call_name, inner, other)?;
            let (left_display, right_display) = display_parts;
            let (left_sql, right_sql) = sql_parts;
            Ok((
                native,
                wrap_binary(left_display, spark_op, right_display),
                wrap_binary(left_sql, spark_op, right_sql),
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
                .map(|(condition, value)| (Box::new(condition.expr), Box::new(value.expr)))
                .collect();
            let else_expr = otherwise.map(|column| Box::new(column.expr));
            let inner = PyColumn::from_expr(Expr::Case(Case {
                expr: None,
                when_then_expr,
                else_expr,
            }));
            let (else_display, else_sql, else_join) = match else_parts {
                Some((display, sql, join)) => (Some(display), Some(sql), Some(join)),
                None => (None, None, None),
            };
            Ok((
                inner,
                format_case_body(display_arms, else_display.as_deref()),
                format_case_body(sql_arms, else_sql.as_deref()),
                Some(format_case_body(join_arms, else_join.as_deref())),
            ))
        })
    }

    #[staticmethod]
    fn alias(inner: &PyColumn, child_display: &str, name: &str) -> PyResult<(PyColumn, String)> {
        fenced!("ColumnParts.alias", {
            Ok((
                PyColumn::from_expr(inner.expr().alias(name)),
                wrap_alias(child_display, name),
            ))
        })
    }

    #[staticmethod]
    fn getitem(
        inner: &PyColumn,
        key: &PyColumn,
        kind: &str,
        display_parts: (&str, &str),
        sql_parts: (&str, &str),
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.getitem", {
            let (child_display, key_display) = display_parts;
            let (child_sql, key_sql) = sql_parts;
            let (call_name, spark_display, sql_expr) = match kind {
                "index" => (
                    "array_element",
                    wrap_index_display(child_display, key_display),
                    wrap_index_sql(child_sql, key_sql),
                ),
                "field" => (
                    "get_field",
                    wrap_index_display(child_display, key_display),
                    wrap_field_sql(child_sql, key_sql),
                ),
                "key" => (
                    "getitem",
                    wrap_index_display(child_display, key_display),
                    wrap_index_sql(child_sql, key_sql),
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
    fn field_join_sql(child_sql: &str, key_literal: &str) -> PyResult<String> {
        fenced!("ColumnParts.field_join_sql", {
            let mut out = String::with_capacity(child_sql.len() + key_literal.len() + 4);
            out.push('(');
            out.push_str(child_sql);
            out.push_str(")[");
            out.push_str(key_literal);
            out.push(']');
            Ok(out)
        })
    }

    #[staticmethod]
    fn call_scalar(
        name: &str,
        inners: Vec<PyColumn>,
        display_list: &Bound<'_, PyList>,
        sql_list: &Bound<'_, PyList>,
        join_list: &Bound<'_, PyList>,
        display: Option<&str>,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.call_scalar", {
            let display_parts = str_list(display_list)?;
            let sql_parts = str_list(sql_list)?;
            let join_parts = str_list(join_list)?;
            if display_parts.len() != inners.len()
                || sql_parts.len() != inners.len()
                || join_parts.len() != inners.len()
            {
                return Err(PyValueError::new_err(
                    "call_scalar part lists must match the argument count",
                ));
            }
            let exprs = inners.into_iter().map(|column| column.expr).collect();
            let inner = PyColumn::from_expr(call_scalar_expr(name, exprs)?);
            let shown = display.map_or_else(|| wrap_call(name, &display_parts), str::to_string);
            Ok((
                inner,
                shown,
                wrap_call(name, &sql_parts),
                Some(wrap_call(name, &join_parts)),
            ))
        })
    }

    #[staticmethod]
    fn lit_timestamp(text: &str) -> PyResult<(PyColumn, String)> {
        fenced!("ColumnParts.lit_timestamp", {
            construct::lit_timestamp(text)
        })
    }

    #[staticmethod]
    fn lit_date(text: &str) -> PyResult<(PyColumn, String)> {
        fenced!("ColumnParts.lit_date", { Ok(construct::lit_date(text)) })
    }

    #[staticmethod]
    fn lit_time(text: &str) -> PyResult<(PyColumn, String)> {
        fenced!("ColumnParts.lit_time", { Ok(construct::lit_time(text)) })
    }

    #[staticmethod]
    fn lit_array_cast(
        inner: &PyColumn,
        child_sql: &str,
        element_type: &str,
        cast_type: &str,
    ) -> PyResult<(PyColumn, String, String)> {
        fenced!("ColumnParts.lit_array_cast", {
            construct::lit_array_cast(inner, child_sql, element_type, cast_type)
        })
    }

    #[staticmethod]
    fn pi() -> PyResult<(PyColumn, String)> {
        fenced!("ColumnParts.pi", { Ok(construct::pi()) })
    }

    #[staticmethod]
    fn uuid() -> PyResult<(PyColumn, String)> {
        fenced!("ColumnParts.uuid", { Ok(construct::uuid()) })
    }

    #[staticmethod]
    fn cast(
        py: Python<'_>,
        inner: Bound<'_, PyColumn>,
        child_parts: (&str, &str, &str),
        engine_type: &str,
        spark_type: &str,
        keyword: &str,
        flags: (bool, bool),
    ) -> PyResult<(Py<PyColumn>, String, String, Option<String>)> {
        fenced!("ColumnParts.cast", {
            let (apply_engine, keep_child_sql) = flags;
            let native = if apply_engine {
                Py::new(py, engine_cast(&inner.borrow(), engine_type, keyword)?)?
            } else {
                inner.unbind()
            };
            let (child_display, child_sql, child_join) = child_parts;
            let spark_display = wrap_cast(keyword, child_display, spark_type);
            if keep_child_sql {
                Ok((native, spark_display, child_sql.to_string(), None))
            } else {
                Ok((
                    native,
                    spark_display,
                    wrap_cast(keyword, child_sql, spark_type),
                    Some(wrap_cast(keyword, child_join, spark_type)),
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
            format_case_body(arms.clone(), None),
            "CASE WHEN (x > 0) THEN 1 WHEN (x < 0) THEN -1 END"
        );
        assert_eq!(
            format_case_body(arms[..1].to_vec(), Some("0")),
            "CASE WHEN (x > 0) THEN 1 ELSE 0 END"
        );
    }

    #[test]
    fn cast_and_try_cast_keywords() {
        assert_eq!(wrap_cast("CAST", "x", "DOUBLE"), "CAST(x AS DOUBLE)");
        assert_eq!(wrap_cast("TRY_CAST", "x", "INT"), "TRY_CAST(x AS INT)");
    }

    #[test]
    fn call_renders_name_comma_parts() {
        assert_eq!(wrap_call("sqrt", &["x"]), "sqrt(x)");
        assert_eq!(wrap_call("lpad", &["s", "10", "'x'"]), "lpad(s, 10, 'x')");
        assert_eq!(wrap_call("rand", &[] as &[&str]), "rand()");
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
