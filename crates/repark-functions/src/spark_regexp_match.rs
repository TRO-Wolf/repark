use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use regex::Regex;

use crate::spark_regexp::compile_spark_regex;

#[must_use]
pub fn regexp_like_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpLike::regexp_like()))
}

#[must_use]
pub fn rlike_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpLike::rlike()))
}

#[must_use]
pub fn regexp_replace_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpReplace::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![regexp_like_udf(), rlike_udf(), regexp_replace_udf()]
}

fn any_arg_nullable(fields: &[FieldRef]) -> bool {
    fields.iter().any(|field| field.is_nullable())
}

fn is_utf8_family(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => true,
        DataType::Dictionary(_, value_type) => is_utf8_family(value_type),
        _ => false,
    }
}

fn coerce_two_or_three_strings(
    arg_types: &[DataType],
    name: &str,
    min: usize,
    max: usize,
) -> Result<Vec<DataType>> {
    if arg_types.len() < min || arg_types.len() > max {
        return Err(DataFusionError::Plan(format!(
            "'{name}' expects {min} to {max} arguments, got {}",
            arg_types.len()
        )));
    }
    for (index, data_type) in arg_types.iter().enumerate() {
        if index < 3 && !is_utf8_family(data_type) && *data_type != DataType::Null {
            return Err(DataFusionError::Plan(format!(
                "'{name}' argument {} must be a string, got {data_type}",
                index + 1
            )));
        }
    }
    Ok(vec![DataType::Utf8; arg_types.len()])
}

fn utf8_array(array: &ArrayRef) -> Result<ArrayRef> {
    cast(array.as_ref(), &DataType::Utf8).map_err(|error| {
        DataFusionError::Execution(format!("failed to cast regexp argument to Utf8: {error}"))
    })
}

fn compile_cached<'cache>(
    cache: &'cache mut HashMap<String, Regex>,
    pattern: &str,
) -> Result<&'cache Regex> {
    if !cache.contains_key(pattern) {
        cache.insert(pattern.to_owned(), compile_spark_regex(pattern)?);
    }
    cache
        .get(pattern)
        .ok_or_else(|| DataFusionError::Internal("regexp match cache insert vanished".to_owned()))
}

#[derive(Debug)]
struct SparkRegexpLike {
    name: &'static str,
    signature: Signature,
}

impl SparkRegexpLike {
    fn regexp_like() -> Self {
        Self {
            name: "regexp_like",
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }

    fn rlike() -> Self {
        Self {
            name: "rlike",
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRegexpLike {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SparkRegexpLike {}

impl Hash for SparkRegexpLike {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for SparkRegexpLike {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            any_arg_nullable(args.arg_fields),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_two_or_three_strings(arg_types, self.name(), 2, 3)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() < 2 {
            return exec_err!(
                "'{}' requires 2 arguments, got {}",
                self.name(),
                arrays.len()
            );
        }
        let strings = utf8_array(&arrays[0])?;
        let strings = strings.as_string::<i32>();
        let patterns = utf8_array(&arrays[1])?;
        let patterns = patterns.as_string::<i32>();
        let mut cache: HashMap<String, Regex> = HashMap::new();
        let mut builder = BooleanBuilder::with_capacity(strings.len());
        for row in 0..strings.len() {
            if strings.is_null(row) || patterns.is_null(row) {
                builder.append_null();
                continue;
            }
            let regex = compile_cached(&mut cache, patterns.value(row))?;
            builder.append_value(regex.is_match(strings.value(row)));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[derive(Debug)]
struct SparkRegexpReplace {
    signature: Signature,
}

impl SparkRegexpReplace {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRegexpReplace {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRegexpReplace {}

impl Hash for SparkRegexpReplace {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRegexpReplace {
    crate::shim_udf_boilerplate!("regexp_replace");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "regexp_replace",
            DataType::Utf8,
            any_arg_nullable(args.arg_fields),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_two_or_three_strings(arg_types, "regexp_replace", 3, 4)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() < 3 {
            return exec_err!(
                "'regexp_replace' requires 3 arguments, got {}",
                arrays.len()
            );
        }
        let strings = utf8_array(&arrays[0])?;
        let strings = strings.as_string::<i32>();
        let patterns = utf8_array(&arrays[1])?;
        let patterns = patterns.as_string::<i32>();
        let replacements = utf8_array(&arrays[2])?;
        let replacements = replacements.as_string::<i32>();
        let mut cache: HashMap<String, Regex> = HashMap::new();
        let mut values: Vec<Option<String>> = Vec::with_capacity(strings.len());
        for row in 0..strings.len() {
            if strings.is_null(row) || patterns.is_null(row) || replacements.is_null(row) {
                values.push(None);
                continue;
            }
            let regex = compile_cached(&mut cache, patterns.value(row))?;
            values.push(Some(
                regex
                    .replace_all(strings.value(row), replacements.value(row))
                    .into_owned(),
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
    }
}
