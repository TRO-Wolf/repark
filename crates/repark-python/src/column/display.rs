use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Case, Cast, Expr, lit};
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyList;

use super::PyColumn;
use super::expr_build::cast_to;
use super::function_dispatch::call_scalar_expr;
use crate::AnalysisException;
use crate::fence::fenced;

mod construct;
#[cfg(test)]
mod tests;

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

fn wrap_is_duplicated(child: &str) -> String {
    let mut out = String::with_capacity(child.len() + 15);
    out.push_str("is_duplicated(");
    out.push_str(child);
    out.push(')');
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

pub(crate) fn wrap_cast(keyword: &str, child: &str, spark_type: &str) -> String {
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

fn apply_binary_op(left: &PyColumn, right: &PyColumn, op_method: &str) -> PyResult<(Expr, usize)> {
    let left_expr = left.expr();
    let right_expr = right.expr();
    match op_method {
        "add" => Ok((left_expr + right_expr, 1)),
        "sub" => Ok((left_expr - right_expr, 1)),
        "mul" => Ok((left_expr * right_expr, 1)),
        "div" => {
            let numerator = Expr::Cast(Cast::new(Box::new(left_expr), DataType::Float64));
            let denominator = Expr::Cast(Cast::new(Box::new(right_expr), DataType::Float64));
            Ok((numerator / denominator, 2))
        }
        "modulo" => Ok((left_expr % right_expr, 1)),
        "eq" => Ok((left_expr.eq(right_expr), 1)),
        "lt" => Ok((left_expr.lt(right_expr), 1)),
        "gt" => Ok((left_expr.gt(right_expr), 1)),
        "le" => Ok((left_expr.lt_eq(right_expr), 1)),
        "ge" => Ok((left_expr.gt_eq(right_expr), 1)),
        "and_" => Ok((left_expr.and(right_expr), 1)),
        "or_" => Ok((left_expr.or(right_expr), 1)),
        other => Err(PyValueError::new_err(format!("unknown binary op {other}"))),
    }
}

fn call_two(name: &str, left: &PyColumn, right: &PyColumn) -> PyResult<PyColumn> {
    Ok(PyColumn::combine_surveyed(
        call_scalar_expr(name, vec![left.expr(), right.expr()])?,
        [left, right],
    ))
}

fn engine_cast(inner: &PyColumn, engine_type: &str, keyword: &str) -> PyResult<PyColumn> {
    let try_cast = match keyword {
        "CAST" => false,
        "TRY_CAST" => true,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown cast keyword {other}"
            )));
        }
    };
    let expr = cast_to(inner.expr(), engine_type, try_cast).map_err(AnalysisException::new_err)?;
    Ok(PyColumn::combine_surveyed(expr, [inner]))
}

type RenderedParts = (PyColumn, String, String, Option<String>);

impl Drop for PyColumn {
    fn drop(&mut self) {
        let need = crate::deep_stack::clone_need_bytes(self.plan_levels, self.expr_levels);
        crate::deep_stack::grown_sync(need, || {
            let _ = std::mem::replace(&mut self.expr, lit(ScalarValue::Null));
        });
    }
}

impl Clone for PyColumn {
    fn clone(&self) -> Self {
        Self {
            expr: self.expr(),
            expr_levels: self.expr_levels,
            df_levels: self.df_levels,
            plan_levels: self.plan_levels,
        }
    }
}

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
            let (combined, added) = apply_binary_op(left, right, op_method)?;
            let inner = PyColumn::combine(combined, [left, right], added);
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
            let inner = PyColumn::combine(left.expr().not_eq(right.expr()), [left, right], 1);
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
            let negated = Expr::Negative(Box::new(inner.expr()));
            let aliased = PyColumn::combine(negated.alias(&display), [inner], 2);
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
            let mut pending = values.iter();
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
                        let value = pending.next().ok_or_else(|| {
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
            if pending.next().is_some() || value_parts.next().is_some() {
                return Err(PyValueError::new_err(
                    "update_fields value lists must match the 'with' op count",
                ));
            }
            let native = PyColumn::combine_surveyed(
                repark_core::update_fields_call(args),
                std::iter::once(inner).chain(values.iter()),
            );
            Ok((native, display, sql, Some(join)))
        })
    }

    #[staticmethod]
    fn repark_isnan(inner: &PyColumn, child_parts: (&str, &str, &str)) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.repark_isnan", {
            let (child_display, child_sql, child_join) = child_parts;
            Ok((
                PyColumn::combine_surveyed(repark_core::repark_isnan_call(inner.expr()), [inner]),
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
            let native = PyColumn::combine(
                inner.expr().in_list(exprs, false),
                std::iter::once(inner).chain(values.iter()),
                1,
            );
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
            let native = PyColumn::combine_surveyed(
                call_scalar_expr("substr", vec![inner.expr(), start.expr(), length.expr()])?,
                [inner, start, length],
            );
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
                PyColumn::combine(!inner.expr(), [inner], 1),
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
                PyColumn::combine(inner.expr().is_null(), [inner], 1),
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
                PyColumn::combine(inner.expr().is_not_null(), [inner], 1),
                wrap_is_not_null(child_display),
                wrap_is_not_null(child_sql),
                Some(wrap_is_not_null(child_join)),
            ))
        })
    }

    #[staticmethod]
    fn is_duplicated(
        inner: &PyColumn,
        child_display: &str,
        child_sql: &str,
        child_join: &str,
    ) -> PyResult<RenderedParts> {
        fenced!("ColumnParts.is_duplicated", {
            Ok((
                PyColumn::from_expr(super::expr_build::is_duplicated_expr(inner.expr())?),
                wrap_is_duplicated(child_display),
                wrap_is_duplicated(child_sql),
                Some(wrap_is_duplicated(child_join)),
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
                .iter()
                .map(|(condition, value)| (Box::new(condition.expr()), Box::new(value.expr())))
                .collect();
            let else_expr = otherwise.as_ref().map(|column| Box::new(column.expr()));
            let inner = PyColumn::combine(
                Expr::Case(Case {
                    expr: None,
                    when_then_expr,
                    else_expr,
                }),
                when_thens
                    .iter()
                    .flat_map(|(condition, value)| [condition, value])
                    .chain(otherwise.iter()),
                1,
            );
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
                PyColumn::combine(inner.expr().alias(name), [inner], 1),
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
            let exprs = inners.iter().map(PyColumn::expr).collect();
            let inner = PyColumn::combine_surveyed(call_scalar_expr(name, exprs)?, &inners);
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
    #[pyo3(signature = (time, window, slide = None, start = None))]
    fn time_window(
        time: &PyColumn,
        window: &str,
        slide: Option<&str>,
        start: Option<&str>,
    ) -> PyResult<PyColumn> {
        fenced!("ColumnParts.time_window", {
            let slide = match (slide, start) {
                (None, Some(_)) => Some(window),
                (slide, _) => slide,
            };
            let mut args = vec![time.expr(), lit(window)];
            if let Some(slide) = slide {
                args.push(lit(slide));
            }
            if let Some(start) = start {
                args.push(lit(start));
            }
            let call = Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::spark_time_window::window_udf(),
                args,
            ));
            Ok(PyColumn::combine(
                call.alias(repark_functions::spark_time_window::WINDOW_OUTPUT_NAME),
                [time],
                2,
            ))
        })
    }

    #[staticmethod]
    #[pyo3(signature = (time, gap))]
    fn session_window(time: &PyColumn, gap: &PyColumn) -> PyResult<PyColumn> {
        fenced!("ColumnParts.session_window", {
            let call = Expr::ScalarFunction(ScalarFunction::new_udf(
                repark_functions::spark_session_window::session_window_udf(),
                vec![time.expr(), gap.expr()],
            ));
            Ok(PyColumn::combine(
                call.alias(repark_functions::spark_session_window::SESSION_OUTPUT_NAME),
                [time, gap],
                2,
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
    fn cast_type_token(engine_type: &str) -> PyResult<String> {
        fenced!("ColumnParts.cast_type_token", {
            repark_functions::cast_map::map_cast_token(engine_type).ok_or_else(|| {
                crate::ParseException::new_err(format!("unknown cast type '{engine_type}'"))
            })
        })
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
