//! Spark `regexp_count` / `regexp_instr` — NULL-in NULL-out, INT, ignore-idx (G6 / P1).
//!
//! DataFusion's kernels return `0` for NULL inputs (int64) and treat a 3rd
//! `regexp_instr` argument as START POSITION. Spark 4.1.2 (live oracle):
//! - `regexp_count(NULL, 'ab')` / `regexp_count('ababab', NULL)` → NULL, INT
//! - `regexp_instr` 3rd arg is accepted, **ignored as a value**, NULL-propagates,
//!   and the result is the 1-based **UTF-16** start of the first match
//!   (`'abcde'` / `'b(c)d'` / idx 0, 1, 3, 99 all → `2`; NULL idx → NULL;
//!   `'🐈ab'` / `'ab'` → `3` because 🐈 is two UTF-16 units).
//!
//! This module overwrites both names from [`crate::string::functions`] so the
//! Spark SQL door and the facade `expr_fn` builders share one kernel.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int32Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use regex::Regex;

/// ===========================================================================================
/// Spark `regexp_count` UDF (overwrites DataFusion's NULL→0 / int64 kernel).
/// ===========================================================================================
#[must_use]
pub fn regexp_count_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpCount::new()))
}

/// ===========================================================================================
/// Spark `regexp_instr` UDF (overwrites DataFusion's start-position 3rd arg).
/// ===========================================================================================
#[must_use]
pub fn regexp_instr_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRegexpInstr::new()))
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
        coerce_regexp_args(arg_types, false)
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
        coerce_regexp_args(arg_types, true)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_regexp(&args, RegexpKind::Instr)
    }
}

fn any_arg_nullable(fields: &[FieldRef]) -> bool {
    fields.iter().any(|field| field.is_nullable())
}

fn is_utf8_family(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => true,
        // Dictionary-encoded strings (Parquet) must not plan-refuse; coerced
        // output is Utf8 and the planner inserts the cast (R3-1).
        DataType::Dictionary(_, value_type) => is_utf8_family(value_type),
        _ => false,
    }
}

fn coerce_regexp_args(arg_types: &[DataType], allow_index: bool) -> Result<Vec<DataType>> {
    let name = if allow_index {
        "regexp_instr"
    } else {
        "regexp_count"
    };
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
                "'regexp_instr' idx must be an integer (Spark casts STRING), got {index_type}"
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

fn compile_spark_regex(pattern: &str) -> Result<Regex> {
    // Java/Spark `\d` `\w` `\s` are ASCII; the regex crate's are Unicode.
    let bound = crate::collection::bind_ascii_perl_classes(pattern);
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

/// Java `Matcher.find()`: report an empty match where a previous non-empty
/// match ended, and advance empty matches by one UTF-16 unit (including a
/// mid-surrogate step on supplementary-plane chars). The `regex` crate's
/// `find_iter` suppresses the empty-after-non-empty case (`[0-9]*` on
/// `2026-08-19` is 3 there, 6 in Spark).
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
            if pattern.is_match("") {
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
    // Spark / Java `Matcher.start()` is a UTF-16 code-unit index (🐈 is 2), not
    // a Unicode scalar count (🐈 is 1). `caféx`/`x` is 5 either way.
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
