//! Spark `regexp_count` / `regexp_instr` — NULL-in NULL-out, INT, ignore-idx (G6 / P1).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::spark_regex_engine::{SparkRegex, compile_spark_regex};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// Spark `regexp_count` UDF (overwrites DataFusion's NULL→0 / int64 kernel).
#[must_use]
pub fn regexp_count_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpCount::new()))
}

/// Spark `regexp_instr` UDF (overwrites DataFusion's start-position 3rd arg).
#[must_use]
pub fn regexp_instr_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpInstr::new()))
}

/// Spark `regexp_extract_all(str, regexp[, idx])` UDF.
#[must_use]
pub fn regexp_extract_all_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpExtractAll::new()))
}

/// Spark `regexp_substr(str, regexp)` UDF.
#[must_use]
pub fn regexp_substr_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpSubstr::new()))
}

#[must_use]
pub fn regexp_extract_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpExtract::new()))
}

#[derive(Clone, Copy)]
enum RegexpKind {
    Count,
    Instr,
}

#[derive(Debug)]
struct SparkRegexpCount {
    signature: Signature,
}

impl SparkRegexpCount {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRegexpCount {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRegexpCount {}

impl Hash for SparkRegexpCount {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRegexpCount {
    crate::shim_udf_boilerplate!("regexp_count");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "regexp_count",
            DataType::Int32,
            any_arg_nullable(args.arg_fields),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_args(arg_types, "regexp_count", false)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_regexp(&args, RegexpKind::Count)
    }
}

#[derive(Debug)]
struct SparkRegexpInstr {
    signature: Signature,
}

impl SparkRegexpInstr {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRegexpInstr {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRegexpInstr {}

impl Hash for SparkRegexpInstr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRegexpInstr {
    crate::shim_udf_boilerplate!("regexp_instr");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "regexp_instr",
            DataType::Int32,
            any_arg_nullable(args.arg_fields),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_args(arg_types, "regexp_instr", true)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_regexp(&args, RegexpKind::Instr)
    }
}

#[derive(Debug)]
struct SparkRegexpExtractAll {
    signature: Signature,
}

impl SparkRegexpExtractAll {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRegexpExtractAll {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRegexpExtractAll {}

impl Hash for SparkRegexpExtractAll {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRegexpExtractAll {
    crate::shim_udf_boilerplate!("regexp_extract_all");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(list_of_utf8())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "regexp_extract_all",
            list_of_utf8(),
            any_arg_nullable(args.arg_fields),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_args(arg_types, "regexp_extract_all", true)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_extract_all(&args)
    }
}

#[derive(Debug)]
struct SparkRegexpSubstr {
    signature: Signature,
}

impl SparkRegexpSubstr {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRegexpSubstr {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRegexpSubstr {}

impl Hash for SparkRegexpSubstr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

#[derive(Debug)]
struct SparkRegexpExtract {
    signature: Signature,
}

impl SparkRegexpExtract {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRegexpExtract {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRegexpExtract {}

impl Hash for SparkRegexpExtract {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRegexpExtract {
    crate::shim_udf_boilerplate!("regexp_extract");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "regexp_extract",
            DataType::Utf8,
            any_arg_nullable(args.arg_fields),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_args(arg_types, "regexp_extract", true)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_extract(&args)
    }
}

impl ScalarUDFImpl for SparkRegexpSubstr {
    crate::shim_udf_boilerplate!("regexp_substr");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "regexp_substr",
            DataType::Utf8,
            any_arg_nullable(args.arg_fields),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_args(arg_types, "regexp_substr", false)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_substr(&args)
    }
}

fn list_of_utf8() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
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

fn coerce_regexp_args(
    arg_types: &[DataType],
    name: &str,
    allow_index: bool,
) -> Result<Vec<DataType>> {
    let max_args = if allow_index { 3 } else { 2 };
    if arg_types.len() < 2 || arg_types.len() > max_args {
        return Err(DataFusionError::Plan(format!(
            "'{name}' expects 2{} arguments, got {}",
            if allow_index { " or 3" } else { "" },
            arg_types.len()
        )));
    }
    if !is_utf8_family(&arg_types[0]) {
        return Err(DataFusionError::Plan(format!(
            "'{name}' expects a string first argument, got {}",
            arg_types[0]
        )));
    }
    if !is_utf8_family(&arg_types[1]) {
        return Err(DataFusionError::Plan(format!(
            "'{name}' expects a string regexp argument, got {}",
            arg_types[1]
        )));
    }
    let mut coerced = vec![DataType::Utf8, DataType::Utf8];
    if arg_types.len() == 3 {
        let index_type = &arg_types[2];
        let ok =
            index_type.is_integer() || is_utf8_family(index_type) || *index_type == DataType::Null;
        if !ok {
            return Err(DataFusionError::Plan(format!(
                "'{name}' idx must be an integer (Spark casts STRING), got {index_type}"
            )));
        }
        coerced.push(DataType::Int32);
    }
    Ok(coerced)
}

fn invoke_regexp(args: &ScalarFunctionArgs, kind: RegexpKind) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() < 2 {
        return exec_err!("regexp_count/regexp_instr expects at least 2 arguments");
    }
    let strings = cast(arrays[0].as_ref(), &DataType::Utf8)?;
    let strings = strings.as_string::<i32>();
    let patterns = cast(arrays[1].as_ref(), &DataType::Utf8)?;
    let patterns = patterns.as_string::<i32>();
    let group_index = match arrays.get(2) {
        Some(array) => Some(cast(array.as_ref(), &DataType::Int32)?),
        None => None,
    };

    let fn_name = match kind {
        RegexpKind::Count => "regexp_count",
        RegexpKind::Instr => "regexp_instr",
    };
    let mut cache: HashMap<String, SparkRegex> = HashMap::new();
    let mut values: Vec<Option<i32>> = Vec::with_capacity(strings.len());
    for row in 0..strings.len() {
        let index_is_null = group_index.as_ref().is_some_and(|index| index.is_null(row));
        if strings.is_null(row) || patterns.is_null(row) || index_is_null {
            values.push(None);
            continue;
        }
        let pattern_text = patterns.value(row);
        if !cache.contains_key(pattern_text) {
            cache.insert(
                pattern_text.to_owned(),
                compile_spark_regex(pattern_text, fn_name)?,
            );
        }
        let regex = cache
            .get(pattern_text)
            .ok_or_else(|| DataFusionError::Internal("regexp cache insert vanished".to_owned()))?;
        let text = strings.value(row);
        let result = match kind {
            RegexpKind::Count => regex.count_non_overlapping(text)?,
            RegexpKind::Instr => regex.instr_start(text)?,
        };
        values.push(Some(result));
    }
    Ok(ColumnarValue::Array(Arc::new(Int32Array::from(values))))
}

/// Shared row walk: NULL-in NULL-out, one compiled regex per pattern, group index passed RAW.
fn extract_rows<T>(
    args: &ScalarFunctionArgs,
    name: &str,
    mut per_row: impl FnMut(Option<(&str, &SparkRegex, i32)>) -> Result<T>,
) -> Result<Vec<T>> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() < 2 {
        return exec_err!("{name} expects at least 2 arguments");
    }
    let strings = cast(arrays[0].as_ref(), &DataType::Utf8)?;
    let strings = strings.as_string::<i32>();
    let patterns = cast(arrays[1].as_ref(), &DataType::Utf8)?;
    let patterns = patterns.as_string::<i32>();
    let group_index = match arrays.get(2) {
        Some(array) => Some(cast(array.as_ref(), &DataType::Int32)?),
        None => None,
    };

    let mut cache: HashMap<String, SparkRegex> = HashMap::new();
    let mut out = Vec::with_capacity(strings.len());
    for row in 0..strings.len() {
        let index_is_null = group_index.as_ref().is_some_and(|index| index.is_null(row));
        if strings.is_null(row) || patterns.is_null(row) || index_is_null {
            out.push(per_row(None)?);
            continue;
        }
        let group = match group_index.as_ref() {
            Some(index) => index
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .value(row),
            None => 1,
        };
        let pattern_text = patterns.value(row);
        if !cache.contains_key(pattern_text) {
            cache.insert(
                pattern_text.to_owned(),
                compile_spark_regex(pattern_text, name)?,
            );
        }
        let regex = cache
            .get(pattern_text)
            .ok_or_else(|| DataFusionError::Internal("regexp cache insert vanished".to_owned()))?;
        out.push(per_row(Some((strings.value(row), regex, group)))?);
    }
    Ok(out)
}

fn invoke_extract(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let values = extract_rows(args, "regexp_extract", |row| {
        Ok(match row {
            None => None,
            Some((text, regex, raw_group)) => {
                let captured = regex
                    .find_first(text)?
                    .map(|found| {
                        let group = validate_group_index(
                            raw_group,
                            regex.captures_len(),
                            "regexp_extract",
                        )?;
                        Ok::<_, DataFusionError>(
                            regex.capture_at(text, found.0, group)?.unwrap_or_default(),
                        )
                    })
                    .transpose()?
                    .unwrap_or_default();
                Some(captured)
            }
        })
    })?;
    let array: ArrayRef = Arc::new(StringArray::from(values));
    Ok(ColumnarValue::Array(array))
}

fn invoke_extract_all(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    extract_rows(args, "regexp_extract_all", |row| {
        match row {
            None => builder.append(false),
            Some((text, regex, raw_group)) => {
                let group =
                    validate_group_index(raw_group, regex.captures_len(), "regexp_extract_all")?;
                for (start, _) in regex.collect_matches(text, usize::MAX)? {
                    let captured = regex.capture_at(text, start, group)?;
                    match captured {
                        Some(value) => builder.values().append_value(value),
                        None => builder.values().append_value(""),
                    }
                }
                builder.append(true);
            }
        }
        Ok(())
    })?;
    let array: ArrayRef = Arc::new(builder.finish());
    Ok(ColumnarValue::Array(array))
}

fn invoke_substr(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let values = extract_rows(args, "regexp_substr", |row| {
        Ok(match row {
            None => None,
            Some((text, regex, _group)) => match regex.find_first(text)? {
                Some((start, end)) => (start != end).then(|| text[start..end].to_owned()),
                None => None,
            },
        })
    })?;
    let array: ArrayRef = Arc::new(StringArray::from(values));
    Ok(ColumnarValue::Array(array))
}

/// Negative or over-large groups use Spark's single `REGEX_GROUP_INDEX` error contract.
fn validate_group_index(raw_group: i32, captures_len: usize, name: &str) -> Result<usize> {
    let bound = captures_len.saturating_sub(1);
    let group = usize::try_from(raw_group)
        .ok()
        .filter(|index| *index <= bound);
    group.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in \
             `{name}` is invalid: Expects group index between 0 and {bound}, but got \
             {raw_group}. SQLSTATE: 22023"
        ))
    })
}

#[cfg(test)]
mod tests;
