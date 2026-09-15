use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub(crate) fn is_blank_text(text: &str) -> bool {
    text.trim().is_empty()
}

pub(crate) fn parse_java_float_text<F: FromStr>(text: &str) -> Option<F> {
    let trimmed = text.trim();
    trimmed.parse::<F>().ok().or_else(|| {
        trimmed
            .strip_suffix(|cell| matches!(cell, 'd' | 'D' | 'f' | 'F'))
            .and_then(|stem| stem.parse::<F>().ok())
    })
}

pub(crate) fn cast_invalid_input(text: &str, target: &DataType) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{text}' of the type \"STRING\" cannot be cast to \"{}\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018",
        super::spark_float_type_name(target)
    ))
}

fn each_string(array: &dyn Array) -> Result<Vec<Option<&str>>> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().iter().collect()),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().iter().collect()),
        DataType::Utf8View => Ok(array.as_string_view().iter().collect()),
        other => exec_err!("'__repark_parse_java' got a non-string argument of type {other}"),
    }
}

fn scalar_string(scalar: &ScalarValue) -> Result<Option<String>> {
    match scalar {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => Ok(value.clone()),
        ScalarValue::Utf8View(value) => Ok(value.as_deref().map(str::to_owned)),
        ScalarValue::Null => Ok(None),
        other => exec_err!("'__repark_parse_java' got a non-string scalar {other}"),
    }
}

fn parse_rows<F: FromStr>(
    input: &ColumnarValue,
    ansi: bool,
    target: &DataType,
    arrow_fallback: &dyn Fn(&str) -> Option<F>,
) -> Result<Vec<Option<F>>> {
    let held: Option<String>;
    let strings: Vec<Option<&str>> = match input {
        ColumnarValue::Array(array) => each_string(array.as_ref())?,
        ColumnarValue::Scalar(scalar) => {
            held = scalar_string(scalar)?;
            vec![held.as_deref()]
        }
    };
    let mut out = Vec::with_capacity(strings.len());
    for cell in strings {
        let Some(text) = cell else {
            out.push(None);
            continue;
        };
        if let Ok(value) = text.parse::<F>() {
            out.push(Some(value));
            continue;
        }
        if is_blank_text(text) {
            out.push(None);
            continue;
        }
        if let Some(value) = parse_java_float_text::<F>(text) {
            out.push(Some(value));
            continue;
        }
        if let Some(value) = arrow_fallback(text) {
            out.push(Some(value));
            continue;
        }
        if ansi {
            return Err(cast_invalid_input(text, target));
        }
        out.push(None);
    }
    Ok(out)
}

macro_rules! parse_udf {
    ($name:ident, $udf_name:literal, $float:ty, $array:ty, $scalar:ident, $target:expr) => {
        #[derive(Debug)]
        struct $name {
            signature: Signature,
        }

        impl $name {
            fn new() -> Self {
                Self {
                    signature: Signature::any(1, Volatility::Immutable),
                }
            }
        }

        impl PartialEq for $name {
            fn eq(&self, _other: &Self) -> bool {
                true
            }
        }

        impl Eq for $name {}

        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.name().hash(state);
            }
        }

        impl ScalarUDFImpl for $name {
            crate::shim_udf_boilerplate!($udf_name);

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                match arg_types {
                    [format] if super::is_string_type(format) => Ok($target),
                    _ => Err(DataFusionError::Plan(format!(
                        "'{}' expects a STRING argument",
                        self.name()
                    ))),
                }
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
                let nullable = args
                    .arg_fields
                    .first()
                    .is_none_or(|field| field.is_nullable());
                Ok(Arc::new(Field::new(self.name(), $target, nullable)))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let Some(input) = args.args.first() else {
                    return exec_err!("'{}' expects 1 argument", self.name());
                };
                let ansi = crate::ansi::spark_ansi_enabled_from_options(&args.config_options);
                let target = $target;
                let rows = parse_rows::<$float>(input, ansi, &target, &|text| {
                    ScalarValue::Utf8(Some(text.to_owned()))
                        .cast_to(&target)
                        .ok()
                        .and_then(|scalar| match scalar {
                            ScalarValue::$scalar(value) => value,
                            _ => None,
                        })
                })?;
                Ok(ColumnarValue::Array(Arc::new(<$array>::from(rows))))
            }
        }
    };
}

parse_udf!(
    ParseJavaDouble,
    "__repark_parse_java_double__",
    f64,
    Float64Array,
    Float64,
    DataType::Float64
);
parse_udf!(
    ParseJavaFloat,
    "__repark_parse_java_float__",
    f32,
    Float32Array,
    Float32,
    DataType::Float32
);

pub(crate) fn parse_java_double_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ParseJavaDouble::new()))
}

pub(crate) fn parse_java_float_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ParseJavaFloat::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_path_covers_suffix_and_surrounding_space() {
        assert_eq!(parse_java_float_text::<f64>("1d"), Some(1.0));
        assert_eq!(parse_java_float_text::<f64>("1.5D"), Some(1.5));
        assert_eq!(parse_java_float_text::<f64>("-2.5F"), Some(-2.5));
        assert_eq!(parse_java_float_text::<f64>(" 1.0 "), Some(1.0));
        assert_eq!(parse_java_float_text::<f64>("Inf"), Some(f64::INFINITY));
        assert_eq!(parse_java_float_text::<f64>("1dd"), None);
        assert_eq!(parse_java_float_text::<f64>("0x10"), None);
        assert_eq!(parse_java_float_text::<f64>(""), None);
        assert_eq!(parse_java_float_text::<f32>("1f"), Some(1.0));
        assert!(is_blank_text("   "));
        assert!(!is_blank_text("1d"));
    }

    #[test]
    fn invalid_input_error_names_value_and_target() {
        let error = cast_invalid_input("1dd", &DataType::Float64).to_string();
        assert!(error.contains("[CAST_INVALID_INPUT]"));
        assert!(error.contains("'1dd'"));
        assert!(error.contains("\"DOUBLE\""));
        let error = cast_invalid_input("1dd", &DataType::Float32).to_string();
        assert!(error.contains("\"FLOAT\""));
    }
}
