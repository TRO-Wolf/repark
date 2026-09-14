use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayData, ArrayRef, make_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::functions_nested::concat::{array_append_udf, array_prepend_udf};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn spark_array_append_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayAppend::new()))
}

#[must_use]
pub fn spark_array_prepend_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayPrepend::new()))
}

#[derive(Debug)]
struct SparkArrayAppend {
    signature: Signature,
}

impl SparkArrayAppend {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArrayAppend {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArrayAppend {}

impl Hash for SparkArrayAppend {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

#[derive(Debug)]
struct SparkArrayPrepend {
    signature: Signature,
}

impl SparkArrayPrepend {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArrayPrepend {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArrayPrepend {}

impl Hash for SparkArrayPrepend {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

const INT_LADDER: [DataType; 4] = [
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
];

fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        DataType::UInt32 => "INT UNSIGNED".to_string(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(..) => "TIMESTAMP".to_string(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        other => other.to_string().to_uppercase(),
    }
}

fn diff_types_error(name: &str, array_type: &DataType, element_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES] Cannot resolve \"{name}\" due to data \
         type mismatch: Input to `{name}` should have been \"ARRAY\" followed by a value with \
         same element type, but it's [\"{}\", \"{}\"].",
        spark_type_name(array_type),
        spark_type_name(element_type)
    ))
}

fn spark_common_element(array_element: &DataType, element: &DataType) -> Option<DataType> {
    if array_element == element {
        return Some(element.clone());
    }
    if matches!(array_element, DataType::Null) {
        return Some(element.clone());
    }
    if matches!(element, DataType::Null) {
        return Some(array_element.clone());
    }
    let array_int = INT_LADDER.iter().position(|t| t == array_element);
    let element_int = INT_LADDER.iter().position(|t| t == element);
    if let (Some(a), Some(e)) = (array_int, element_int) {
        return Some(INT_LADDER[a.max(e)].clone());
    }
    let array_float = matches!(
        array_element,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    );
    let element_float = matches!(
        element,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    );
    if (array_int.is_some() || array_float) && (element_int.is_some() || element_float) {
        return Some(DataType::Float64);
    }
    match (array_element, element) {
        (DataType::Timestamp(..), DataType::Timestamp(..)) => Some(array_element.clone()),
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(..)) => Some(element.clone()),
        (DataType::Timestamp(..), DataType::Date32 | DataType::Date64) => {
            Some(array_element.clone())
        }
        _ => None,
    }
}

fn array_element_type(array_type: &DataType) -> Option<&DataType> {
    match array_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type())
        }
        _ => None,
    }
}

fn widened_list(array_type: &DataType, element: DataType) -> DataType {
    match array_type {
        DataType::LargeList(_) => {
            DataType::LargeList(Arc::new(Field::new_list_field(element, true)))
        }
        _ => DataType::new_list(element, true),
    }
}

fn spark_return_type(arg_types: &[DataType], name: &str) -> Result<DataType> {
    let [array_type, element_type] = arg_types else {
        return plan_err!(
            "'{name}' expects (array, element), got {} argument(s)",
            arg_types.len()
        );
    };
    let element = match array_type {
        DataType::Null => element_type.clone(),
        _ => match array_element_type(array_type) {
            Some(array_element) => spark_common_element(array_element, element_type)
                .ok_or_else(|| diff_types_error(name, array_type, element_type))?,
            None => {
                return plan_err!("'{name}' argument 1 must be an ARRAY, got {array_type}");
            }
        },
    };
    Ok(widened_list(array_type, element))
}

fn spark_coerce_args(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    let [array_type, element_type] = arg_types else {
        return plan_err!(
            "'{name}' expects (array, element), got {} argument(s)",
            arg_types.len()
        );
    };
    if matches!(array_type, DataType::Null) {
        return Ok(vec![
            DataType::new_list(element_type.clone(), true),
            element_type.clone(),
        ]);
    }
    let Some(array_element) = array_element_type(array_type) else {
        return Err(diff_types_error(name, array_type, element_type));
    };
    match spark_common_element(array_element, element_type) {
        Some(common) => Ok(vec![widened_list(array_type, common.clone()), common]),
        None => Err(diff_types_error(name, array_type, element_type)),
    }
}

fn spark_return_field(args: &ReturnFieldArgs<'_>, name: &str) -> Result<FieldRef> {
    let Some(array_field) = args.arg_fields.first() else {
        return exec_err!("'{name}' requires 2 arguments, got 0");
    };
    let element_type = args
        .arg_fields
        .get(1)
        .map_or(DataType::Null, |field| field.data_type().clone());
    let data_type = spark_return_type(&[array_field.data_type().clone(), element_type], name)?;
    Ok(Arc::new(Field::new(
        name,
        data_type,
        array_field.is_nullable(),
    )))
}

fn preserve_input_nulls(input: &ArrayRef, result: ArrayRef) -> Result<ArrayRef> {
    if input.data_type() == &DataType::Null {
        return Ok(make_array(ArrayData::new_null(
            result.data_type(),
            result.len(),
        )));
    }
    let Some(nulls) = input.nulls() else {
        return Ok(result);
    };
    if nulls.null_count() == 0 {
        return Ok(result);
    }
    let rebuilt = result
        .to_data()
        .into_builder()
        .nulls(Some(nulls.clone()))
        .build()?;
    Ok(make_array(rebuilt))
}

fn finish_preserved(preserved: ArrayRef, all_scalar: bool) -> Result<ColumnarValue> {
    if all_scalar {
        return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &preserved, 0,
        )?));
    }
    Ok(ColumnarValue::Array(preserved))
}

fn invoke_preserved(args: &ScalarFunctionArgs, prepend: bool) -> Result<ColumnarValue> {
    let all_scalar = args
        .args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let input = Arc::clone(&arrays[0]);
    if input.null_count() == input.len() {
        let all_null = make_array(ArrayData::new_null(
            args.return_field.data_type(),
            input.len(),
        ));
        return finish_preserved(all_null, all_scalar);
    }
    let mut call_args: Vec<ColumnarValue> = arrays.into_iter().map(ColumnarValue::Array).collect();
    let mut call_fields = args.arg_fields.clone();
    if prepend {
        call_args.swap(0, 1);
        call_fields.swap(0, 1);
    }
    let kernel = if prepend {
        array_prepend_udf()
    } else {
        array_append_udf()
    };
    let result = kernel.invoke_with_args(ScalarFunctionArgs {
        args: call_args,
        arg_fields: call_fields,
        number_rows: args.number_rows,
        return_field: Arc::clone(&args.return_field),
        config_options: Arc::clone(&args.config_options),
    })?;
    let ColumnarValue::Array(result) = result else {
        return exec_err!("array_append kernel did not produce an array");
    };
    finish_preserved(preserve_input_nulls(&input, result)?, all_scalar)
}

impl ScalarUDFImpl for SparkArrayAppend {
    crate::shim_udf_boilerplate!("array_append");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        spark_return_type(arg_types, self.name())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        spark_return_field(&args, self.name())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        spark_coerce_args(arg_types, self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_preserved(&args, false)
    }
}

impl ScalarUDFImpl for SparkArrayPrepend {
    crate::shim_udf_boilerplate!("array_prepend");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        spark_return_type(arg_types, self.name())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        spark_return_field(&args, self.name())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        spark_coerce_args(arg_types, self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_preserved(&args, true)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::ScalarValue;
    use datafusion::execution::config::SessionConfig;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn run(sql: &str) -> datafusion::common::Result<Vec<datafusion::arrow::array::RecordBatch>> {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async { ctx.sql(sql).await?.collect().await })
    }

    fn shown(sql: &str) -> String {
        let batches = run(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0), 0)
            .expect("scalar")
            .to_string()
    }

    #[test]
    fn array_append_keeps_the_input_rows_null_where_spark_does() {
        assert_eq!(
            shown("SELECT array_append(CAST(NULL AS ARRAY<INT>), 9)"),
            ""
        );
        assert_eq!(shown("SELECT array_append(array(1, 2), 9)"), "[1, 2, 9]");
        assert_eq!(
            shown("SELECT array_append(array(1, 2), CAST(NULL AS INT))"),
            "[1, 2, ]"
        );
        assert_eq!(shown("SELECT array_append(array(), 4)"), "[4]");
    }

    #[test]
    fn array_prepend_keeps_spark_order_and_nulls() {
        assert_eq!(
            shown("SELECT array_prepend(CAST(NULL AS ARRAY<INT>), 9)"),
            ""
        );
        assert_eq!(shown("SELECT array_prepend(array(1, 2), 9)"), "[9, 1, 2]");
        assert_eq!(
            shown("SELECT array_prepend(array(1, 2), CAST(NULL AS INT))"),
            "[, 1, 2]"
        );
    }

    #[test]
    fn array_append_refuses_a_mismatched_element_type() {
        let error = run("SELECT array_append(array(1, 2), 'x')")
            .expect_err("a string element into array<int> must refuse");
        let message = error.to_string();
        assert!(
            message.contains("DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"),
            "{message}"
        );
        assert!(
            message.contains("[\"ARRAY<BIGINT>\", \"STRING\"]"),
            "{message}"
        );
        let error = run("SELECT array_prepend(array('1', '2'), 4)")
            .expect_err("an int element into array<string> must refuse");
        let message = error.to_string();
        assert!(
            message.contains("DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"),
            "{message}"
        );
        assert!(
            message.contains("[\"ARRAY<STRING>\", \"BIGINT\"]"),
            "{message}"
        );
    }

    #[test]
    fn coerce_types_widens_along_sparks_tightest_common_type() {
        let udf = spark_array_append_udf();
        let list = |element: DataType| DataType::new_list(element, true);
        for (array_element, element, want) in [
            (DataType::Int8, DataType::Int32, DataType::Int32),
            (DataType::Int16, DataType::Int8, DataType::Int16),
            (DataType::Int32, DataType::Int64, DataType::Int64),
            (DataType::Int64, DataType::Int16, DataType::Int64),
            (DataType::Int32, DataType::Float64, DataType::Float64),
            (DataType::Int64, DataType::Float32, DataType::Float64),
            (DataType::Float32, DataType::Float64, DataType::Float64),
            (DataType::Float64, DataType::Int8, DataType::Float64),
        ] {
            let got = udf
                .coerce_types(&[list(array_element.clone()), element])
                .expect("widening pair must coerce");
            assert_eq!(got, vec![list(want.clone()), want]);
        }
        let timestamp = DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        );
        let got = udf
            .coerce_types(&[list(DataType::Date32), timestamp.clone()])
            .expect("date array + timestamp element widens to timestamp");
        assert_eq!(got, vec![list(timestamp.clone()), timestamp.clone()]);
        let got = udf
            .coerce_types(&[list(timestamp.clone()), DataType::Date32])
            .expect("timestamp array + date element widens to timestamp");
        assert_eq!(got, vec![list(timestamp.clone()), timestamp]);
    }

    #[test]
    fn coerce_types_passes_identical_and_null_types() {
        let udf = spark_array_append_udf();
        let list = |element: DataType| DataType::new_list(element, true);
        let nested = list(DataType::Int32);
        let got = udf
            .coerce_types(&[list(nested.clone()), nested.clone()])
            .expect("equal nested types pass unchanged");
        assert_eq!(got, vec![list(nested.clone()), nested]);
        let got = udf
            .coerce_types(&[list(DataType::Int32), DataType::Null])
            .expect("a NULL element takes the array element type");
        assert_eq!(got, vec![list(DataType::Int32), DataType::Int32]);
        let got = udf
            .coerce_types(&[DataType::Null, DataType::Int32])
            .expect("a NULL array coerces to a typed list");
        assert_eq!(got, vec![list(DataType::Int32), DataType::Int32]);
    }

    #[test]
    fn coerce_types_refuses_everything_off_the_ladder() {
        let udf = spark_array_prepend_udf();
        let list = |element: DataType| DataType::new_list(element, true);
        for (array_type, element) in [
            (list(DataType::Utf8), DataType::Int32),
            (list(DataType::Int32), DataType::Utf8),
            (list(DataType::Int32), DataType::Decimal128(10, 4)),
            (list(DataType::Int32), DataType::Decimal128(2, 1)),
            (list(DataType::Float64), DataType::Decimal128(2, 1)),
            (
                list(DataType::Decimal128(10, 2)),
                DataType::Decimal128(10, 4),
            ),
            (list(DataType::Decimal128(10, 2)), DataType::Int32),
            (list(DataType::Decimal128(10, 2)), DataType::Float64),
            (list(DataType::Int32), DataType::Boolean),
            (list(list(DataType::Int32)), list(DataType::Int64)),
            (DataType::Int32, DataType::Int32),
        ] {
            let message = udf
                .coerce_types(&[array_type.clone(), element.clone()])
                .expect_err("off-ladder pair must refuse")
                .to_string();
            assert!(
                message.contains("DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"),
                "{array_type:?} vs {element:?}: {message}"
            );
        }
    }

    #[test]
    fn array_append_widens_a_date_array_to_timestamp() {
        assert_eq!(
            shown("SELECT array_append(array(DATE '2024-01-02'), TIMESTAMP '2024-05-06 07:08:09')"),
            "[2024-01-02T00:00:00, 2024-05-06T07:08:09]"
        );
    }

    #[test]
    fn all_null_input_short_circuits_to_a_typed_null_result() {
        assert_eq!(
            shown("SELECT array_append(CAST(NULL AS ARRAY<INT>), 9)"),
            ""
        );
        let batches = run("SELECT array_append(CAST(NULL AS ARRAY<DOUBLE>), 1.5) AS r")
            .expect("all-null input");
        let field = batches[0].schema().field(0).clone();
        assert!(matches!(field.data_type(), DataType::List(_)), "{field:?}");
        assert!(batches[0].column(0).is_null(0));
    }

    #[test]
    fn null_graft_honours_a_sliced_inputs_own_offset() {
        let item_field = Arc::new(Field::new("item", DataType::Int32, true));
        let input = ListArray::new(
            Arc::clone(&item_field),
            OffsetBuffer::new(vec![0, 2, 2, 3].into()),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let sliced: ArrayRef = Arc::new(input.slice(1, 2));
        let result: ArrayRef = Arc::new(ListArray::new(
            item_field,
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(Int32Array::from(vec![9, 9])),
            None,
        ));
        let grafted = preserve_input_nulls(&sliced, result).expect("graft");
        assert!(grafted.is_null(0), "the sliced NULL row must stay NULL");
        assert!(grafted.is_valid(1), "the sliced valid row must stay valid");
        let scalar = ScalarValue::try_from_array(&grafted, 1).expect("row 1 scalar");
        assert_eq!(scalar.to_string(), "[9]");
    }

    #[test]
    fn udf_invoke_grafts_nulls_from_a_sliced_array_argument() {
        let item_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_type = DataType::List(Arc::clone(&item_field));
        let input = ListArray::new(
            item_field,
            OffsetBuffer::new(vec![0, 2, 2, 3].into()),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let sliced: ArrayRef = Arc::new(input.slice(1, 2));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(sliced),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(9))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("a", list_type.clone(), true)),
                Arc::new(Field::new("e", DataType::Int32, true)),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("r", list_type, true)),
            config_options: SessionConfig::new().options().clone(),
        };
        let udf = spark_array_append_udf();
        let ColumnarValue::Array(out) = udf.invoke_with_args(args).expect("invoke") else {
            panic!("expected an array result");
        };
        assert!(out.is_null(0), "row 0 is the sliced NULL row");
        let scalar = ScalarValue::try_from_array(&out, 1).expect("row 1 scalar");
        assert_eq!(scalar.to_string(), "[3, 9]");
    }
}
