//! Spark string shims for `substring`/`substr` and `concat`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, GenericListArray, Int64Array, OffsetSizeTrait,
    StringArray, StringBuilder, as_fixed_size_list_array, as_large_list_array, as_list_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::{can_cast_types, cast};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Float32Type, Float64Type, Int64Type,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

mod format_number;
pub use format_number::{call_format_number, format_number_udf};

/// The string shims to register (after `datafusion-spark`, so they win the name clash).
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        substring_udf(),
        concat_udf(),
        crate::spark_reverse::reverse_udf(),
        crate::spark_split::split_udf(),
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
        format_number_udf(),
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

pub(crate) fn spark_array_join_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayJoin::new()))
}

#[derive(Debug)]
struct SparkArrayJoin {
    signature: Signature,
}

impl SparkArrayJoin {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArrayJoin {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArrayJoin {}

impl Hash for SparkArrayJoin {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkArrayJoin {
    crate::shim_udf_boilerplate!("__repark_array_join__");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() < 2 || arrays.len() > 3 {
            return Err(DataFusionError::Plan(format!(
                "'{}' expects 2 or 3 arguments",
                self.name()
            )));
        }
        let delimiters = string_option_column(&arrays[1], self.name())?;
        let null_strings = if arrays.len() == 3 {
            string_option_column(&arrays[2], self.name())?
        } else {
            vec![None; arrays[0].len()]
        };
        let joined = match arrays[0].data_type() {
            DataType::List(_) => join_list_rows(
                as_list_array(arrays[0].as_ref()),
                &delimiters,
                &null_strings,
            )?,
            DataType::LargeList(_) => join_list_rows(
                as_large_list_array(arrays[0].as_ref()),
                &delimiters,
                &null_strings,
            )?,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "'{}' expects a list array, got {other}",
                    self.name()
                )));
            }
        };
        Ok(ColumnarValue::Array(Arc::new(joined)))
    }
}

fn string_option_column(values: &ArrayRef, name: &str) -> Result<Vec<Option<String>>> {
    match values.data_type() {
        DataType::Utf8 => Ok(values
            .as_string::<i32>()
            .iter()
            .map(|item| item.map(str::to_owned))
            .collect()),
        DataType::Utf8View => Ok(values
            .as_string_view()
            .iter()
            .map(|item| item.map(str::to_owned))
            .collect()),
        DataType::LargeUtf8 => Ok(values
            .as_string::<i64>()
            .iter()
            .map(|item| item.map(str::to_owned))
            .collect()),
        DataType::Null => Ok(vec![None; values.len()]),
        other => Err(DataFusionError::Plan(format!(
            "'{name}' expects a string delimiter, got {other}"
        ))),
    }
}

fn join_list_rows<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    delimiters: &[Option<String>],
    null_strings: &[Option<String>],
) -> Result<StringArray> {
    let mut builder = StringBuilder::with_capacity(list.len(), list.len() * 8);
    let mut row = String::new();
    for index in 0..list.len() {
        let delimiter = delimiters.get(index).and_then(|item| item.as_deref());
        let (Some(delimiter), false) = (delimiter, list.is_null(index)) else {
            builder.append_null();
            continue;
        };
        row.clear();
        let mut first = true;
        join_array_value(
            &mut row,
            &list.value(index),
            delimiter,
            null_strings.get(index).and_then(|item| item.as_deref()),
            &mut first,
        )?;
        builder.append_value(&row);
    }
    Ok(builder.finish())
}

fn join_array_value(
    row: &mut String,
    values: &ArrayRef,
    delimiter: &str,
    null_string: Option<&str>,
    first: &mut bool,
) -> Result<()> {
    match values.data_type() {
        DataType::List(_) => {
            let nested = as_list_array(values.as_ref());
            for index in 0..nested.len() {
                if nested.is_null(index) {
                    write_null_element(row, delimiter, null_string, first);
                } else {
                    join_array_value(row, &nested.value(index), delimiter, null_string, first)?;
                }
            }
            Ok(())
        }
        DataType::LargeList(_) => {
            let nested = as_large_list_array(values.as_ref());
            for index in 0..nested.len() {
                if nested.is_null(index) {
                    write_null_element(row, delimiter, null_string, first);
                } else {
                    join_array_value(row, &nested.value(index), delimiter, null_string, first)?;
                }
            }
            Ok(())
        }
        DataType::FixedSizeList(_, _) => {
            let nested = as_fixed_size_list_array(values.as_ref());
            for index in 0..nested.len() {
                if nested.is_null(index) {
                    write_null_element(row, delimiter, null_string, first);
                } else {
                    join_array_value(row, &nested.value(index), delimiter, null_string, first)?;
                }
            }
            Ok(())
        }
        DataType::Dictionary(_, value_type) => {
            let unkeyed = cast(values.as_ref(), value_type)?;
            join_array_value(row, &unkeyed, delimiter, null_string, first)
        }
        DataType::Null => Ok(()),
        DataType::Float64 => {
            let primitive = values.as_primitive::<Float64Type>();
            for index in 0..primitive.len() {
                if primitive.is_null(index) {
                    write_null_element(row, delimiter, null_string, first);
                } else {
                    write_element(row, delimiter, first, |row| {
                        crate::java_double::with_java_double_text(primitive.value(index), |text| {
                            row.push_str(text);
                        });
                    });
                }
            }
            Ok(())
        }
        DataType::Float32 => {
            let primitive = values.as_primitive::<Float32Type>();
            for index in 0..primitive.len() {
                if primitive.is_null(index) {
                    write_null_element(row, delimiter, null_string, first);
                } else {
                    write_element(row, delimiter, first, |row| {
                        crate::java_double::with_java_float_text(primitive.value(index), |text| {
                            row.push_str(text);
                        });
                    });
                }
            }
            Ok(())
        }
        DataType::Utf8 => {
            for item in values.as_string::<i32>() {
                write_option_element(row, delimiter, null_string, first, item);
            }
            Ok(())
        }
        DataType::LargeUtf8 => {
            for item in values.as_string::<i64>() {
                write_option_element(row, delimiter, null_string, first, item);
            }
            Ok(())
        }
        DataType::Utf8View => {
            for item in values.as_string_view() {
                write_option_element(row, delimiter, null_string, first, item);
            }
            Ok(())
        }
        other => {
            if can_cast_types(other, &DataType::Utf8) {
                let rendered = cast(values.as_ref(), &DataType::Utf8)?;
                join_array_value(row, &rendered, delimiter, null_string, first)
            } else {
                Err(DataFusionError::Plan(format!(
                    "unsupported data type in array_join: {other}"
                )))
            }
        }
    }
}

fn write_element(
    row: &mut String,
    delimiter: &str,
    first: &mut bool,
    text: impl FnOnce(&mut String),
) {
    if *first {
        *first = false;
    } else {
        row.push_str(delimiter);
    }
    text(row);
}

fn write_null_element(
    row: &mut String,
    delimiter: &str,
    null_string: Option<&str>,
    first: &mut bool,
) {
    if let Some(replacement) = null_string {
        write_element(row, delimiter, first, |row| row.push_str(replacement));
    }
}

fn write_option_element(
    row: &mut String,
    delimiter: &str,
    null_string: Option<&str>,
    first: &mut bool,
    item: Option<&str>,
) {
    match item {
        Some(text) => write_element(row, delimiter, first, |row| row.push_str(text)),
        None => write_null_element(row, delimiter, null_string, first),
    }
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        spark_concat_return(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            "concat",
            spark_concat_return(&declared)?,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.iter().any(crate::collection::is_list_family) {
            crate::collection::plan_array_concat(arg_types)?;
            return Ok(arg_types.to_vec());
        }
        if !arg_types.is_empty() && arg_types.iter().all(crate::collection::is_binary_family) {
            return Ok(vec![DataType::Binary; arg_types.len()]);
        }
        Ok(arg_types
            .iter()
            .map(|data_type| match data_type {
                DataType::Float32 | DataType::Float64 => data_type.clone(),
                _ => DataType::Utf8,
            })
            .collect())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.return_field.data_type() {
            DataType::List(_) => crate::collection::invoke_array_concat(args),
            DataType::Binary => spark_concat_binary(args),
            _ => spark_concat_utf8(args),
        }
    }
}

fn spark_concat_return(arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.iter().any(crate::collection::is_list_family) {
        return Ok(crate::collection::plan_array_concat(arg_types)?.result);
    }
    if !arg_types.is_empty() && arg_types.iter().all(crate::collection::is_binary_family) {
        return Ok(DataType::Binary);
    }
    Ok(DataType::Utf8)
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
    apply_null_mask(result, null_mask, ScalarValue::Utf8(None))
}

fn spark_concat_binary(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ScalarFunctionArgs {
        args: arg_values,
        number_rows,
        ..
    } = args;

    if arg_values.is_empty() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(Vec::new()))));
    }

    let null_mask = compute_null_mask(&arg_values, number_rows)?;
    if matches!(null_mask, NullMaskResolution::ReturnNull) {
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
    }

    let arrays = ColumnarValue::values_to_arrays(&arg_values)?;
    let mut binaries = Vec::with_capacity(arrays.len());
    for array in &arrays {
        let shaped = if array.data_type() == &DataType::Binary {
            Arc::clone(array)
        } else {
            cast(array.as_ref(), &DataType::Binary)?
        };
        binaries.push(shaped);
    }
    let row_count = binaries.first().map_or(0, Array::len);
    let mut builder = BinaryBuilder::with_capacity(row_count, 0);
    for row in 0..row_count {
        let mut piece: Vec<u8> = Vec::new();
        let mut row_null = false;
        for binary in &binaries {
            if binary.is_null(row) {
                row_null = true;
                break;
            }
            piece.extend_from_slice(binary.as_binary::<i32>().value(row));
        }
        if row_null {
            builder.append_null();
        } else {
            builder.append_value(&piece);
        }
    }
    apply_null_mask(
        ColumnarValue::Array(Arc::new(builder.finish())),
        null_mask,
        ScalarValue::Binary(None),
    )
}

/// Cast a [`ColumnarValue`] to `Utf8` (arrays via compute cast; scalars via `ScalarValue` cast).
fn cast_columnar_value_to_utf8(value: &ColumnarValue) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            if array.data_type() == &DataType::Utf8 {
                return Ok(ColumnarValue::Array(Arc::clone(array)));
            }
            match array.data_type() {
                DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(
                    crate::java_double::java_double_strings(array.as_primitive::<Float64Type>()),
                ))),
                DataType::Float32 => Ok(ColumnarValue::Array(Arc::new(
                    crate::java_double::java_float_strings(array.as_primitive::<Float32Type>()),
                ))),
                _ => {
                    let casted = cast(array.as_ref(), &DataType::Utf8)?;
                    Ok(ColumnarValue::Array(casted))
                }
            }
        }
        ColumnarValue::Scalar(scalar) => {
            if matches!(scalar, ScalarValue::Utf8(_)) {
                return Ok(ColumnarValue::Scalar(scalar.clone()));
            }
            match scalar {
                ScalarValue::Float64(value) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value
                        .as_ref()
                        .map(|item| crate::java_double::java_double_text(*item)),
                ))),
                ScalarValue::Float32(value) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value
                        .as_ref()
                        .map(|item| crate::java_double::java_float_text(*item)),
                ))),
                _ => {
                    let casted = scalar.cast_to(&DataType::Utf8)?;
                    Ok(ColumnarValue::Scalar(casted))
                }
            }
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
fn apply_null_mask(
    result: ColumnarValue,
    null_mask: NullMaskResolution,
    null_scalar: ScalarValue,
) -> Result<ColumnarValue> {
    match (result, null_mask) {
        (_, NullMaskResolution::ReturnNull) => Ok(ColumnarValue::Scalar(null_scalar)),
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
mod tests;
