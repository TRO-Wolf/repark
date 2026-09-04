//! Spark `regexp_count` / `regexp_instr` — NULL-in NULL-out, INT, ignore-idx (G6 / P1).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
use regex::Regex;

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

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("regexp_substr", DataType::Utf8, true)))
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

    let mut cache: HashMap<String, Regex> = HashMap::new();
    let mut values: Vec<Option<i32>> = Vec::with_capacity(strings.len());
    for row in 0..strings.len() {
        let index_is_null = group_index.as_ref().is_some_and(|index| index.is_null(row));
        if strings.is_null(row) || patterns.is_null(row) || index_is_null {
            values.push(None);
            continue;
        }
        let pattern_text = patterns.value(row);
        if !cache.contains_key(pattern_text) {
            cache.insert(pattern_text.to_owned(), compile_spark_regex(pattern_text)?);
        }
        let regex = cache
            .get(pattern_text)
            .ok_or_else(|| DataFusionError::Internal("regexp cache insert vanished".to_owned()))?;
        let text = strings.value(row);
        let result = match kind {
            RegexpKind::Count => count_non_overlapping(text, regex)?,
            RegexpKind::Instr => first_match_utf16_start(text, regex)?,
        };
        values.push(Some(result));
    }
    Ok(ColumnarValue::Array(Arc::new(Int32Array::from(values))))
}

/// Shared row walk: NULL-in NULL-out, one compiled regex per pattern, group index passed RAW.
fn extract_rows<T>(
    args: &ScalarFunctionArgs,
    name: &str,
    mut per_row: impl FnMut(Option<(&str, &Regex, i32)>) -> Result<T>,
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

    let mut cache: HashMap<String, Regex> = HashMap::new();
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
            cache.insert(pattern_text.to_owned(), compile_spark_regex(pattern_text)?);
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
                    .find(text)
                    .map(|found| {
                        let group = validate_group_index(raw_group, regex, "regexp_extract")?;
                        Ok::<_, DataFusionError>(
                            regex
                                .captures_at(text, found.start())
                                .and_then(|caps| caps.get(group).map(|m| m.as_str().to_owned()))
                                .unwrap_or_default(),
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
                let group = validate_group_index(raw_group, regex, "regexp_extract_all")?;
                for found in collect_matches(text, regex)? {
                    let captured = regex
                        .captures_at(text, found.start())
                        .and_then(|caps| caps.get(group).map(|m| m.as_str().to_owned()));
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
            Some((text, regex, _group)) => regex
                .find(text)
                .map(|found| found.as_str())
                .filter(|matched| !matched.is_empty())
                .map(str::to_owned),
        })
    })?;
    let array: ArrayRef = Arc::new(StringArray::from(values));
    Ok(ColumnarValue::Array(array))
}

/// Negative or over-large groups use Spark's single `REGEX_GROUP_INDEX` error contract.
fn validate_group_index(raw_group: i32, regex: &Regex, name: &str) -> Result<usize> {
    let bound = regex.captures_len().saturating_sub(1);
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

pub(crate) fn compile_spark_regex(pattern: &str) -> Result<Regex> {
    let translated = crate::java_regex::translate_java_char_classes(pattern);
    let bound = crate::collection::bind_ascii_perl_classes(&translated);
    Regex::new(&bound).map_err(|error| {
        DataFusionError::Execution(format!("invalid regular expression '{pattern}': {error}"))
    })
}

fn count_overflow() -> DataFusionError {
    DataFusionError::Execution("regexp_count exceeds Spark INT".to_owned())
}

fn bump_count(count: i32) -> Result<i32> {
    count.checked_add(1).ok_or_else(count_overflow)
}

/// Two U+FFFD (3 UTF-8 bytes each).
const MID_SURROGATE_PROBE: &str = "\u{FFFD}\u{FFFD}";
const MID_SURROGATE_PROBE_OFFSET: usize = 3;

/// Detect a match starting at a mid-surrogate UTF-16 index; matching requires `start == offset`.
fn matches_at_mid_surrogate_index(pattern: &Regex) -> bool {
    pattern
        .find_at(MID_SURROGATE_PROBE, MID_SURROGATE_PROBE_OFFSET)
        .is_some_and(|found| found.start() == MID_SURROGATE_PROBE_OFFSET)
}

/// Collect matches with Java's empty-after-non-empty stepping.
fn collect_matches<'text>(text: &'text str, pattern: &Regex) -> Result<Vec<regex::Match<'text>>> {
    let mut found_all = Vec::new();
    if pattern.as_str().is_empty() {
        let boundaries = text
            .char_indices()
            .map(|(offset, _)| offset)
            .chain([text.len()]);
        found_all.extend(boundaries.filter_map(|offset| pattern.find_at(text, offset)));
        return Ok(found_all);
    }
    let mut byte = 0usize;
    loop {
        if byte > text.len() {
            break;
        }
        let Some(found) = pattern.find_at(text, byte) else {
            break;
        };
        found_all.push(found);
        if found_all.len() > usize::try_from(i32::MAX).unwrap_or(usize::MAX) {
            return Err(count_overflow());
        }
        if found.start() == found.end() {
            if found.start() == text.len() {
                break;
            }
            let Some(ch) = text[found.start()..].chars().next() else {
                break;
            };
            byte = found.start() + ch.len_utf8();
        } else {
            byte = found.end();
        }
    }
    Ok(found_all)
}

/// Count matches with Java's empty-after-non-empty stepping and UTF-16 mid-surrogate probe.
fn count_non_overlapping(text: &str, pattern: &Regex) -> Result<i32> {
    if pattern.as_str().is_empty() {
        let count = text.encode_utf16().count().saturating_add(1);
        return i32::try_from(count).map_err(|_| count_overflow());
    }

    let mut count: i32 = 0;
    let mut byte = 0usize;
    let mut mid_surrogate = false;
    loop {
        if mid_surrogate {
            if matches_at_mid_surrogate_index(pattern) {
                count = bump_count(count)?;
            }
            let Some(ch) = text.get(byte..).and_then(|rest| rest.chars().next()) else {
                break;
            };
            byte += ch.len_utf8();
            mid_surrogate = false;
            continue;
        }
        if byte > text.len() {
            break;
        }
        let Some(found) = pattern.find_at(text, byte) else {
            break;
        };
        count = bump_count(count)?;
        if found.start() == found.end() {
            if found.start() == text.len() {
                break;
            }
            let Some(ch) = text[found.start()..].chars().next() else {
                break;
            };
            if ch.len_utf16() == 2 {
                mid_surrogate = true;
                byte = found.start();
            } else {
                byte = found.start() + ch.len_utf8();
            }
        } else {
            byte = found.end();
        }
    }
    Ok(count)
}

fn first_match_utf16_start(text: &str, pattern: &Regex) -> Result<i32> {
    let Some(found) = pattern.find(text) else {
        return Ok(0);
    };
    // Spark / Java `Matcher.start()` is a UTF-16 code-unit index, not a Unicode scalar count.
    let units_before = text[..found.start()].encode_utf16().count();
    let start = units_before.saturating_add(1);
    i32::try_from(start)
        .map_err(|_| DataFusionError::Execution("regexp_instr exceeds Spark INT".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(regexp_count_udf().as_ref().clone());
        ctx.register_udf(regexp_instr_udf().as_ref().clone());
        ctx
    }

    fn ctx_register_all() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    async fn one_i32(ctx: &SessionContext, sql: &str) -> Option<i32> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        let array = batches[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        if array.is_null(0) {
            None
        } else {
            Some(array.value(0))
        }
    }

    async fn one_str(ctx: &SessionContext, sql: &str) -> Option<String> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        let array = batches[0].column(0).as_string::<i32>();
        if array.is_null(0) {
            None
        } else {
            Some(array.value(0).to_owned())
        }
    }

    #[tokio::test]
    async fn regexp_count_null_in_null_out() {
        let ctx = ctx();
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count(CAST(NULL AS VARCHAR), 'ab')").await,
            None
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count('ababab', CAST(NULL AS VARCHAR))").await,
            None
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count('ababab', 'ab')").await,
            Some(3)
        );
    }

    #[tokio::test]
    async fn regexp_instr_ignores_idx_value() {
        let ctx = ctx();
        // Discriminator: group-index would be 3; Spark (and we) return match start 2.
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 1)").await,
            Some(2)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 0)").await,
            Some(2)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 3)").await,
            Some(2)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 99)").await,
            Some(2)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d')").await,
            Some(2)
        );
        assert_eq!(
            one_i32(
                &ctx,
                "SELECT regexp_instr('abcde', 'b(c)d', CAST(NULL AS INT))"
            )
            .await,
            None
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('abcde', 'zzz')").await,
            Some(0)
        );
    }

    #[tokio::test]
    async fn regexp_instr_is_character_not_byte() {
        let ctx = ctx();
        // 🐈 is one scalar / two UTF-16 units; Spark Matcher.start()+1 is 3.
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('🐈ab', 'ab')").await,
            Some(3)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('caféx', 'x')").await,
            Some(5)
        );
        // Empty pattern: UTF-16 boundaries (`🐈` is 2 units → count 3).
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count('🐈', '')").await,
            Some(3)
        );
        // Java `\d` is ASCII; ARABIC-INDIC DIGIT THREE must not count.
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count('٣', '\\d')").await,
            Some(0)
        );
    }

    #[tokio::test]
    async fn empty_pattern_matches_spark() {
        let ctx = ctx();
        // Spark: regexp_count('aaa','') = 4 (zero-width at each boundary).
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count('aaa', '')").await,
            Some(4)
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('aaa', '')").await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn overlapping_count_is_non_overlapping() {
        let ctx = ctx();
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count('aaa', 'aa')").await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn register_all_overwrites_datafusion() {
        let ctx = ctx_register_all();
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_count(CAST(NULL AS VARCHAR), 'ab')").await,
            None
        );
        assert_eq!(
            one_i32(&ctx, "SELECT regexp_instr('abcde', 'b(c)d', 99)").await,
            Some(2)
        );
    }

    #[test]
    fn java_find_loop_matches_spark_zero_width() {
        let digits = compile_spark_regex("[0-9]*").expect("digits");
        assert_eq!(count_non_overlapping("2026-08-19", &digits).expect("c"), 6);
        let stars = compile_spark_regex("b*").expect("b*");
        assert_eq!(count_non_overlapping("abc", &stars).expect("c"), 4);
        let a_star = compile_spark_regex("a*").expect("a*");
        assert_eq!(count_non_overlapping("🐈", &a_star).expect("c"), 3);
        // R4-1: empty `is_match` overcounts start-anchored patterns at a mid-surrogate index.
        let caret = compile_spark_regex("^").expect("caret");
        assert!(!matches_at_mid_surrogate_index(&caret));
        assert_eq!(count_non_overlapping("🐈", &caret).expect("c"), 1);
        let caret_digits = compile_spark_regex(r"^\d*").expect("caret digits");
        assert_eq!(
            count_non_overlapping("🐈2026", &caret_digits).expect("c"),
            1
        );
        let multiline_caret = compile_spark_regex("(?m)^").expect("multiline caret");
        assert!(!matches_at_mid_surrogate_index(&multiline_caret));
        assert_eq!(
            count_non_overlapping("🐈\n🐈", &multiline_caret).expect("c"),
            2
        );
        assert!(matches_at_mid_surrogate_index(&a_star));
    }

    #[tokio::test]
    async fn dictionary_utf8_column_is_accepted() {
        use datafusion::arrow::array::{DictionaryArray, Int8Array, StringArray};
        use datafusion::arrow::datatypes::{Field, Int8Type, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        let ctx = ctx();
        let values = StringArray::from(vec!["ababab", "xy"]);
        let keys = Int8Array::from(vec![0_i8, 1, 0]);
        let dict =
            DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).expect("dictionary");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            dict.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).expect("batch");
        ctx.register_batch("dict_strings", batch).expect("register");
        let batches = ctx
            .sql("SELECT regexp_count(s, 'ab') AS c FROM dict_strings")
            .await
            .expect("plan dict")
            .collect()
            .await
            .expect("exec dict");
        let array = batches[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        assert_eq!(array.value(0), 3);
        assert_eq!(array.value(1), 0);
        assert_eq!(array.value(2), 3);
    }

    #[tokio::test]
    async fn regexp_extract_group_default_and_whole_match() {
        let ctx = ctx_register_all();
        assert_eq!(
            one_str(
                &ctx,
                "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)', 1)"
            )
            .await,
            Some("100".to_owned())
        );
        assert_eq!(
            one_str(
                &ctx,
                "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)', 2)"
            )
            .await,
            Some("200".to_owned())
        );
        assert_eq!(
            one_str(
                &ctx,
                "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)', 0)"
            )
            .await,
            Some("100-200".to_owned())
        );
        assert_eq!(
            one_str(
                &ctx,
                "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)')"
            )
            .await,
            Some("100".to_owned())
        );
        assert_eq!(
            one_str(&ctx, "SELECT regexp_extract('ac', '(a)(b)?', 2)").await,
            Some(String::new())
        );
    }

    #[tokio::test]
    async fn regexp_extract_no_match_is_empty_null_in_null_out() {
        let ctx = ctx_register_all();
        assert_eq!(
            one_str(&ctx, "SELECT regexp_extract('abc', '([0-9]+)', 1)").await,
            Some(String::new())
        );
        assert_eq!(
            one_str(
                &ctx,
                "SELECT regexp_extract(CAST(NULL AS VARCHAR), '([0-9]+)', 1)"
            )
            .await,
            None
        );
        assert_eq!(
            one_str(
                &ctx,
                "SELECT regexp_extract('abc', CAST(NULL AS VARCHAR), 1)"
            )
            .await,
            None
        );
        assert_eq!(
            one_str(
                &ctx,
                "SELECT regexp_extract('abc', '([0-9]+)', CAST(NULL AS INT))"
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn regexp_extract_bad_group_names_extract() {
        let ctx = ctx_register_all();
        for idx in [3, -1] {
            let query = format!("SELECT regexp_extract('a-b', '(a)-(b)', {idx})");
            let result = ctx.sql(&query).await.expect("plan").collect().await;
            let message = format!("{result:?}");
            assert!(result.is_err(), "idx {idx} must raise; got {message}");
            assert!(
                message.contains("`regexp_extract` is invalid")
                    && message.contains("between 0 and 2"),
                "Spark REGEX_GROUP_INDEX shape; got {message}"
            );
        }
    }

    #[tokio::test]
    async fn regexp_extract_java_union_and_unicode_class() {
        let ctx = ctx_register_all();
        assert_eq!(
            one_str(&ctx, "SELECT regexp_extract('alpha', '([[:alpha:]]+)', 1)").await,
            Some("alpha".to_owned())
        );
        assert_eq!(
            one_str(&ctx, "SELECT regexp_extract('fox', '([[:alpha:]]+)', 1)").await,
            Some(String::new())
        );
        assert_eq!(
            one_str(&ctx, "SELECT regexp_extract('alpha', '(\\p{L}+)', 1)").await,
            Some("alpha".to_owned())
        );
    }

    #[tokio::test]
    async fn malformed_string_idx_is_fail_loud() {
        let ctx = ctx();
        let planned = ctx
            .sql("SELECT regexp_instr('abcde', 'b(c)d', 'i')")
            .await
            .expect("plan");
        let result = planned.collect().await;
        assert!(
            result.is_err(),
            "Spark CAST('i' AS INT) is fail-loud; got {result:?}"
        );
    }
}
