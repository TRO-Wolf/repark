//! Spark string shims for `substring`/`substr` and `concat`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int64Array, StringBuilder};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// The string shims to register (after `datafusion-spark`, so they win the name clash).
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        substring_udf(),
        concat_udf(),
        crate::spark_length::bit_length_udf(),
        crate::spark_length::octet_length_udf(),
        crate::spark_regexp::regexp_count_udf(),
        crate::spark_regexp::regexp_instr_udf(),
        crate::spark_regexp::regexp_extract_all_udf(),
        crate::spark_regexp::regexp_extract_udf(),
        crate::spark_regexp::regexp_substr_udf(),
        crate::spark_regexp_match::regexp_like_udf(),
        crate::spark_regexp_match::rlike_udf(),
        crate::spark_regexp_match::regexp_replace_udf(),
        crate::spark_split_part::split_part_udf(),
    ]
}

/// Return the Spark `substring` UDF; the analyzer also replaces planner-embedded built-ins with it.
#[must_use]
pub fn substring_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSubstring::new()))
}

/// The Spark `concat` UDF instance (overwrites `datafusion-spark`'s `SparkConcat`).
#[must_use]
pub fn concat_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkConcat::new()))
}

/// `SparkConcat` — zero arguments return `''`, NULL propagates, and output is always `Utf8`.
#[derive(Debug)]
struct SparkConcat {
    signature: Signature,
}

impl SparkConcat {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::UserDefined, TypeSignature::Nullary],
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for SparkConcat {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkConcat {}

impl Hash for SparkConcat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkConcat {
    crate::shim_udf_boilerplate!("concat");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new("concat", DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![DataType::Utf8; arg_types.len()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_concat_utf8(args)
    }
}

/// Null-mask resolution for Spark any-NULL → NULL concat semantics.
enum NullMaskResolution {
    ReturnNull,
    NoMask,
    Apply(NullBuffer),
}

/// Spark `concat`: any-NULL → NULL, always `Utf8` (never `Utf8View`).
fn spark_concat_utf8(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ScalarFunctionArgs {
        args: arg_values,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    } = args;

    if arg_values.is_empty() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            Some(String::new()),
        )));
    }

    let null_mask = compute_null_mask(&arg_values, number_rows)?;
    if matches!(null_mask, NullMaskResolution::ReturnNull) {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
    }

    let utf8_args: Result<Vec<ColumnarValue>> =
        arg_values.iter().map(cast_columnar_value_to_utf8).collect();
    let utf8_args = utf8_args?;
    let utf8_fields: Vec<FieldRef> = arg_fields
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            ))
        })
        .collect();
    let utf8_return = Arc::new(Field::new(
        return_field.name(),
        DataType::Utf8,
        return_field.is_nullable(),
    ));

    let concat_func = datafusion::functions::string::concat::ConcatFunc::new();
    let kernel_args = ScalarFunctionArgs {
        args: utf8_args,
        arg_fields: utf8_fields,
        number_rows,
        return_field: utf8_return,
        config_options,
    };
    let result = concat_func.invoke_with_args(kernel_args)?;
    let result = cast_columnar_value_to_utf8(&result)?;
    apply_null_mask(result, null_mask)
}

/// Cast a [`ColumnarValue`] to `Utf8` (arrays via compute cast; scalars via `ScalarValue` cast).
fn cast_columnar_value_to_utf8(value: &ColumnarValue) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            if array.data_type() == &DataType::Utf8 {
                return Ok(ColumnarValue::Array(Arc::clone(array)));
            }
            let casted = cast(array.as_ref(), &DataType::Utf8)?;
            Ok(ColumnarValue::Array(casted))
        }
        ColumnarValue::Scalar(scalar) => {
            if matches!(scalar, ScalarValue::Utf8(_)) {
                return Ok(ColumnarValue::Scalar(scalar.clone()));
            }
            let casted = scalar.cast_to(&DataType::Utf8)?;
            Ok(ColumnarValue::Scalar(casted))
        }
    }
}

/// Compute the any-NULL mask across concat arguments (Spark semantics).
fn compute_null_mask(args: &[ColumnarValue], number_rows: usize) -> Result<NullMaskResolution> {
    let all_scalars = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    if all_scalars {
        for arg in args {
            if let ColumnarValue::Scalar(scalar) = arg
                && scalar.is_null()
            {
                return Ok(NullMaskResolution::ReturnNull);
            }
        }
        return Ok(NullMaskResolution::NoMask);
    }

    let array_len = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(number_rows);

    let arrays: Result<Vec<_>> = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => Ok(Arc::clone(array)),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(array_len),
        })
        .collect();
    let arrays = arrays?;
    let combined_nulls = arrays
        .iter()
        .map(|array| array.nulls())
        .fold(None, |acc, nulls| NullBuffer::union(acc.as_ref(), nulls));
    match combined_nulls {
        Some(nulls) => Ok(NullMaskResolution::Apply(nulls)),
        None => Ok(NullMaskResolution::NoMask),
    }
}

/// Apply the Spark any-NULL mask onto a concat result that is already `Utf8`.
fn apply_null_mask(result: ColumnarValue, null_mask: NullMaskResolution) -> Result<ColumnarValue> {
    match (result, null_mask) {
        (_, NullMaskResolution::ReturnNull) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        (scalar @ ColumnarValue::Scalar(_), NullMaskResolution::NoMask) => Ok(scalar),
        (ColumnarValue::Array(array), NullMaskResolution::Apply(null_mask)) => {
            let combined_nulls = NullBuffer::union(array.nulls(), Some(&null_mask));
            let new_array = array
                .into_data()
                .into_builder()
                .nulls(combined_nulls)
                .build()?;
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::make_array(new_array),
            )))
        }
        (array @ ColumnarValue::Array(_), NullMaskResolution::NoMask) => Ok(array),
        (scalar, NullMaskResolution::Apply(_)) => {
            internal_err!("spark concat: scalar result with array null mask: {scalar:?}")
        }
    }
}

/// `SparkSubstring` — Spark `substring(str, pos[, len]) -> STRING` (alias `substr`).
#[derive(Debug)]
struct SparkSubstring {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkSubstring {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["substr".to_string()],
        }
    }
}

impl PartialEq for SparkSubstring {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSubstring {}

impl Hash for SparkSubstring {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkSubstring {
    crate::shim_udf_boilerplate!("substring");

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "'substring' expects (str, pos[, len]), got {} argument(s)",
                arg_types.len()
            )));
        }
        let string_ok = matches!(
            arg_types[0],
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        );
        if !string_ok {
            return Err(DataFusionError::Plan(format!(
                "'substring' expects a string first argument, got {}",
                arg_types[0]
            )));
        }
        for position in &arg_types[1..] {
            if !(position.is_integer() || *position == DataType::Null) {
                return Err(DataFusionError::Plan(format!(
                    "'substring' pos/len must be integers, got {position}"
                )));
            }
        }
        let mut coerced = vec![DataType::Utf8, DataType::Int64];
        if arg_types.len() == 3 {
            coerced.push(DataType::Int64);
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let strings = cast(arrays[0].as_ref(), &DataType::Utf8)?;
        let strings = strings.as_string::<i32>();
        let positions = cast(arrays[1].as_ref(), &DataType::Int64)?;
        let positions = positions.as_primitive::<Int64Type>();
        let lengths: Option<Int64Array> = match arrays.get(2) {
            Some(array) => Some(
                cast(array.as_ref(), &DataType::Int64)?
                    .as_primitive::<Int64Type>()
                    .clone(),
            ),
            None => None,
        };
        let lengths = lengths.as_ref();
        let mut byte_capacity = 0usize;
        for row in 0..strings.len() {
            if !strings.is_null(row) {
                byte_capacity = byte_capacity.saturating_add(strings.value(row).len());
            }
        }
        let mut builder = StringBuilder::with_capacity(strings.len(), byte_capacity);
        for row in 0..strings.len() {
            let length_is_null = lengths.is_some_and(|lengths| lengths.is_null(row));
            if strings.is_null(row) || positions.is_null(row) || length_is_null {
                builder.append_null();
                continue;
            }
            let length = lengths.map(|lengths| lengths.value(row));
            builder.append_value(spark_substring(
                strings.value(row),
                positions.value(row),
                length,
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Spark `substringSQL` clips a character window resolved from `pos`; it never errors.
fn spark_substring(value: &str, position: i64, length: Option<i64>) -> String {
    let total = i64::try_from(value.chars().count()).unwrap_or(i64::MAX);
    let start = match position.cmp(&0) {
        std::cmp::Ordering::Greater => position - 1,
        std::cmp::Ordering::Less => total + position,
        std::cmp::Ordering::Equal => 0,
    };
    let end = match length {
        Some(length) if length < 0 => start,
        Some(length) => start.saturating_add(length),
        None => total,
    };
    let lower = usize::try_from(start.clamp(0, total)).unwrap_or(0);
    let upper = usize::try_from(end.clamp(0, total)).unwrap_or(0);
    if lower >= upper {
        return String::new();
    }
    let mut start_byte = value.len();
    let mut end_byte = value.len();
    let mut char_index = 0usize;
    for (byte_index, _) in value.char_indices() {
        if char_index == lower {
            start_byte = byte_index;
        }
        if char_index == upper {
            end_byte = byte_index;
            break;
        }
        char_index += 1;
    }
    // When `upper == total`, the loop ends without setting `end_byte` from an index — keep len().
    if char_index < upper {
        // `lower` past the end should have returned already (lower >= upper after clamp).
    }
    let mut output = String::with_capacity(end_byte.saturating_sub(start_byte));
    output.push_str(&value[start_byte..end_byte]);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::StringArray;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        for udf in functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    /// Full registry path: `datafusion-spark` first, then repark shims (name overwrite wins).
    fn ctx_register_all() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> Option<String> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute `{sql}`: {error}"));
        let column = batches[0].column(0);
        let strings = column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("expected Utf8 for {sql}, got {:?}", column.data_type()));
        strings.is_valid(0).then(|| strings.value(0).to_string())
    }

    /// Optional release measurement, enabled with `REPARK_PERF_MEASURE=1`.
    #[test]
    #[allow(clippy::cast_precision_loss)] // ns/row report only
    fn perf_measure_substring_char_indices() {
        if std::env::var_os("REPARK_PERF_MEASURE").as_deref() != Some(std::ffi::OsStr::new("1")) {
            eprintln!("PERF-03 skipped (set REPARK_PERF_MEASURE=1 to run 1M-row measurement)");
            return;
        }
        let rows = 1_000_000usize;
        let sample = "αβγδεζηθικλμνξοπρστυφχψωhello世界";
        let start = std::time::Instant::now();
        let mut sink = 0usize;
        for index in 0..rows {
            let out = spark_substring(sample, 2, Some(8));
            sink ^= out.len().wrapping_add(index);
        }
        let elapsed = start.elapsed();
        let ns_new = elapsed.as_nanos() as f64 / rows as f64;
        eprintln!(
            "PERF-03 substring_char_indices rows={rows} total_ms={:.3} ns_per_row={ns_new:.3} sink={sink}",
            elapsed.as_secs_f64() * 1000.0
        );
        let start_baseline = std::time::Instant::now();
        let mut sink_baseline = 0usize;
        for index in 0..rows {
            let chars: Vec<char> = sample.chars().collect();
            let total = i64::try_from(chars.len()).unwrap_or(i64::MAX);
            let start_index = 2_i64 - 1;
            let end = start_index.saturating_add(8);
            let lower = usize::try_from(start_index.clamp(0, total)).unwrap_or(0);
            let upper = usize::try_from(end.clamp(0, total)).unwrap_or(0);
            let out: String = if lower >= upper {
                String::new()
            } else {
                chars[lower..upper].iter().collect()
            };
            sink_baseline ^= out.len().wrapping_add(index);
        }
        let elapsed_baseline = start_baseline.elapsed();
        let ns_old = elapsed_baseline.as_nanos() as f64 / rows as f64;
        eprintln!(
            "PERF-03 substring_vec_char_baseline rows={rows} total_ms={:.3} ns_per_row={ns_old:.3} sink={sink_baseline}",
            elapsed_baseline.as_secs_f64() * 1000.0
        );
        let _ = (sink, sink_baseline, ns_new, ns_old);
    }

    #[tokio::test]
    async fn substring_spark_edge_positions() {
        let ctx = ctx();
        let cases: &[(&str, &str)] = &[
            ("substr('hello', 0, 3)", "hel"),
            ("substring('hello', -3, 2)", "ll"),
            ("substring('hello', -7, 3)", "h"),
            ("substr('hello', 1, 3)", "hel"),
            ("substring('hello', 2, 3)", "ell"),
            ("substr('hello', 2)", "ello"),
            ("substring('hello', -2)", "lo"),
            ("substr('hello', 9, 3)", ""),
            ("substring('hello', 1, 0)", ""),
            ("substr('hello', 1, -1)", ""),
            ("substring('', 1, 2)", ""),
        ];
        for (call, expected) in cases {
            assert_eq!(
                one(&ctx, &format!("SELECT {call}")).await.as_deref(),
                Some(*expected),
                "{call}"
            );
        }
    }

    #[tokio::test]
    async fn substring_nulls_and_multibyte() {
        let ctx = ctx();
        assert_eq!(
            one(&ctx, "SELECT substr(CAST(NULL AS STRING), 1, 2)").await,
            None
        );
        assert_eq!(one(&ctx, "SELECT substr('ab', NULL, 2)").await, None);
        assert_eq!(
            one(&ctx, "SELECT substring('héllo', 2, 3)")
                .await
                .as_deref(),
            Some("éll")
        );
    }

    #[tokio::test]
    async fn concat_coalesce_null_empty_returns_utf8() {
        let ctx = ctx();
        assert_eq!(
            one(
                &ctx,
                "SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'x')"
            )
            .await
            .as_deref(),
            Some("x")
        );
        assert_eq!(
            one(
                &ctx,
                "SELECT concat(concat(coalesce(CAST(NULL AS VARCHAR), ''), ', '), 'Ann')"
            )
            .await
            .as_deref(),
            Some(", Ann")
        );
    }

    #[tokio::test]
    async fn concat_any_null_propagates() {
        let ctx = ctx();
        assert_eq!(
            one(&ctx, "SELECT concat('a', CAST(NULL AS VARCHAR), 'b')").await,
            None
        );
    }

    #[tokio::test]
    async fn concat_basic_and_zero_arg() {
        let ctx = ctx();
        assert_eq!(
            one(&ctx, "SELECT concat('store', 'A')").await.as_deref(),
            Some("storeA")
        );
        assert_eq!(one(&ctx, "SELECT concat()").await.as_deref(), Some(""));
    }

    #[tokio::test]
    async fn concat_result_physical_type_is_utf8() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'id') AS id")
            .await
            .expect("plan concat coalesce")
            .collect()
            .await
            .expect("execute concat coalesce");
        assert_eq!(batches[0].column(0).data_type(), &DataType::Utf8);
    }

    #[tokio::test]
    async fn concat_array_any_null_propagates_per_row() {
        let ctx = ctx_register_all();
        let batches = ctx
            .sql(
                "SELECT concat(a, b) AS j FROM (VALUES
                    ('x', CAST(NULL AS VARCHAR)),
                    ('y', 'z'),
                    (CAST(NULL AS VARCHAR), 'w')
                ) AS t(a, b)",
            )
            .await
            .expect("plan array concat")
            .collect()
            .await
            .expect("execute array concat");
        assert_eq!(batches[0].num_rows(), 3);
        let column = batches[0].column(0);
        assert_eq!(column.data_type(), &DataType::Utf8);
        let strings = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 StringArray");
        assert!(!strings.is_valid(0), "row0 any-NULL must be NULL");
        assert_eq!(strings.value(1), "yz");
        assert!(!strings.is_valid(2), "row2 any-NULL must be NULL");
    }

    #[tokio::test]
    async fn concat_register_all_overwrites_datafusion_spark() {
        assert!(
            datafusion_spark::all_default_scalar_functions()
                .iter()
                .any(|udf| udf.name() == "concat"),
            "datafusion-spark must still ship concat for this overwrite pin to mean anything"
        );
        let ctx = ctx_register_all();
        let batches = ctx
            .sql("SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'x') AS id")
            .await
            .expect("plan under register_all")
            .collect()
            .await
            .expect("execute under register_all");
        assert_eq!(batches[0].column(0).data_type(), &DataType::Utf8);
        assert_eq!(
            one(
                &ctx,
                "SELECT concat(coalesce(CAST(NULL AS VARCHAR), ''), 'x')"
            )
            .await
            .as_deref(),
            Some("x")
        );
        assert_eq!(
            one(&ctx, "SELECT concat(1, 2)").await.as_deref(),
            Some("12")
        );
    }
}
