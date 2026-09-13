use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringArray};
use datafusion::arrow::compute::cast as arrow_cast;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit, TimestampNanosecondType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Cast, Expr, lit};
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::column::PyColumn;

pub(super) fn wrap_keyword_literal(keyword: &str, text: &str) -> String {
    let mut out = String::with_capacity(keyword.len() + text.len() + 4);
    out.push_str(keyword);
    out.push_str(" '");
    out.push_str(text);
    out.push('\'');
    out
}

fn to_timestamp_fallback(text: &str) -> Expr {
    let operand =
        repark_functions::decimal_cast::spark_decimal_cast_nullable_udf().call(vec![lit(text)]);
    repark_functions::expr_fn::to_timestamp(vec![operand])
}

fn timestamp_expr(text: &str) -> PyResult<Expr> {
    let parsed = arrow_cast(
        &StringArray::from(vec![text]),
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
    )
    .map_err(|error| crate::datafusion_to_py_err(DataFusionError::from(error)))?;
    if parsed.is_null(0) {
        return Ok(to_timestamp_fallback(text));
    }
    let nanos = parsed.as_primitive::<TimestampNanosecondType>().value(0);
    Ok(lit(ScalarValue::TimestampMicrosecond(
        Some(nanos.div_euclid(1_000)),
        Some(Arc::<str>::from("UTC")),
    )))
}

fn numpy_element_type(cast_type: &str) -> PyResult<DataType> {
    Ok(match cast_type {
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INT" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "BOOLEAN" => DataType::Boolean,
        "VARCHAR" => DataType::Utf8View,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown array cast type {other}"
            )));
        }
    })
}

fn wrap_angle_call(name: &str, inner: &str) -> String {
    let mut out = String::with_capacity(name.len() + inner.len() + 3);
    out.push_str(name);
    out.push('<');
    out.push_str(inner);
    out.push('>');
    out
}

fn wrap_array_cast_sql(child_sql: &str, cast_type: &str) -> String {
    let mut out = String::with_capacity(child_sql.len() + cast_type.len() + 20);
    out.push_str("CAST(");
    out.push_str(child_sql);
    out.push_str(" AS ARRAY<");
    out.push_str(cast_type);
    out.push_str(">)");
    out
}

pub(super) fn lit_timestamp(text: &str) -> PyResult<(PyColumn, String)> {
    Ok((
        PyColumn::from_expr(timestamp_expr(text)?),
        wrap_keyword_literal("TIMESTAMP", text),
    ))
}

pub(super) fn lit_date(text: &str) -> (PyColumn, String) {
    let inner = PyColumn::from_expr(Expr::Cast(Cast::new(Box::new(lit(text)), DataType::Date32)));
    (inner, wrap_keyword_literal("DATE", text))
}

pub(super) fn lit_time(text: &str) -> (PyColumn, String) {
    let inner = PyColumn::from_expr(Expr::Cast(Cast::new(
        Box::new(lit(text)),
        DataType::Time64(TimeUnit::Nanosecond),
    )));
    (inner, wrap_keyword_literal("TIME", text))
}

pub(super) fn lit_array_cast(
    inner: &PyColumn,
    child_sql: &str,
    element_type: &str,
    cast_type: &str,
) -> PyResult<(PyColumn, String, String)> {
    let element = numpy_element_type(cast_type)?;
    let list = DataType::List(Arc::new(Field::new("item", element, true)));
    let native = PyColumn::from_expr(Expr::Cast(Cast::new(Box::new(inner.expr()), list)));
    Ok((
        native,
        wrap_angle_call("array", element_type),
        wrap_array_cast_sql(child_sql, cast_type),
    ))
}

pub(super) fn pi() -> (PyColumn, String) {
    (
        PyColumn::from_expr(datafusion::functions::expr_fn::pi()),
        "pi()".to_string(),
    )
}

pub(super) fn uuid() -> (PyColumn, String) {
    (
        PyColumn::from_expr(datafusion::functions::expr_fn::uuid()),
        "uuid()".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_literal_wraps_keyword_quote_text_quote() {
        assert_eq!(
            wrap_keyword_literal("TIMESTAMP", "2024-01-02"),
            "TIMESTAMP '2024-01-02'"
        );
        assert_eq!(
            wrap_keyword_literal("DATE", "2024-02-29"),
            "DATE '2024-02-29'"
        );
        assert_eq!(wrap_keyword_literal("TIME", "03:04:05"), "TIME '03:04:05'");
    }

    #[test]
    fn angle_call_renders_array_element_type() {
        assert_eq!(wrap_angle_call("array", "int"), "array<int>");
    }

    #[test]
    fn array_cast_sql_matches_python_shape() {
        assert_eq!(
            wrap_array_cast_sql("array(1, 2)", "INT"),
            "CAST(array(1, 2) AS ARRAY<INT>)"
        );
    }

    #[test]
    fn numpy_element_type_maps_spark_cast_vocabulary() {
        assert_eq!(numpy_element_type("TINYINT").unwrap(), DataType::Int8);
        assert_eq!(numpy_element_type("SMALLINT").unwrap(), DataType::Int16);
        assert_eq!(numpy_element_type("INT").unwrap(), DataType::Int32);
        assert_eq!(numpy_element_type("BIGINT").unwrap(), DataType::Int64);
        assert_eq!(numpy_element_type("FLOAT").unwrap(), DataType::Float32);
        assert_eq!(numpy_element_type("DOUBLE").unwrap(), DataType::Float64);
        assert_eq!(numpy_element_type("BOOLEAN").unwrap(), DataType::Boolean);
        assert_eq!(numpy_element_type("VARCHAR").unwrap(), DataType::Utf8View);
        assert!(numpy_element_type("NOTATYPE").is_err());
    }

    #[test]
    fn timestamp_expr_parses_wall_micros_as_utc() {
        let Expr::Literal(ScalarValue::TimestampMicrosecond(micros, zone), _) =
            timestamp_expr("2024-01-02 03:04:05").unwrap()
        else {
            panic!("expected a microsecond timestamp literal");
        };
        assert_eq!(micros, Some(1_704_164_645_000_000));
        assert_eq!(zone.as_deref(), Some("UTC"));
    }

    #[test]
    fn timestamp_expr_divides_nanoseconds() {
        let Expr::Literal(ScalarValue::TimestampMicrosecond(micros, _), _) =
            timestamp_expr("1969-12-31 23:59:59").unwrap()
        else {
            panic!("expected a microsecond timestamp literal");
        };
        assert_eq!(micros, Some(-1_000_000));
    }

    #[test]
    fn timestamp_expr_falls_back_to_to_timestamp_when_unfoldable() {
        for text in [
            "9999-12-31 23:59:59.999999",
            "9999-12-31 23:59:59.9999999",
            "0001-01-01 00:00:00",
            "1-01-01 00:00:00",
            "garbage",
            "2262-04-12 00:00:00",
        ] {
            let Expr::ScalarFunction(function) = timestamp_expr(text).unwrap() else {
                panic!("expected a to_timestamp call for {text:?}");
            };
            assert_eq!(function.func.name(), "to_timestamp");
            let Expr::ScalarFunction(marker) = &function.args[0] else {
                panic!("expected the decimal-cast marker operand for {text:?}");
            };
            assert_eq!(marker.func.name(), "__repark_decimal_cast_nullable__");
            assert_eq!(marker.args[0], lit(text));
        }
    }
}
