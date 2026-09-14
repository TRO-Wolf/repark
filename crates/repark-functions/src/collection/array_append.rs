use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayData, ArrayRef, make_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
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
            signature: Signature::array_and_element(Volatility::Immutable),
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
            signature: Signature::array_and_element(Volatility::Immutable),
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

fn spark_return_type(arg_types: &[DataType]) -> Result<DataType> {
    let [array_type, element_type] = arg_types else {
        return plan_err!(
            "array_append/array_prepend expects (array, element), got {} argument(s)",
            arg_types.len()
        );
    };
    match array_type {
        DataType::Null => Ok(DataType::new_list(element_type.clone(), true)),
        DataType::List(field) | DataType::FixedSizeList(field, _) => Ok(DataType::List(Arc::new(
            Field::new(field.name(), field.data_type().clone(), true),
        ))),
        DataType::LargeList(field) => Ok(DataType::LargeList(Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            true,
        )))),
        other => plan_err!("'array_append' argument 1 must be an ARRAY, got {other}"),
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
    let data_type = spark_return_type(&[array_field.data_type().clone(), element_type])?;
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

fn invoke_preserved(args: &ScalarFunctionArgs, prepend: bool) -> Result<ColumnarValue> {
    let all_scalar = args
        .args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let input = Arc::clone(&arrays[0]);
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
    let preserved = preserve_input_nulls(&input, result)?;
    if all_scalar {
        return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &preserved, 0,
        )?));
    }
    Ok(ColumnarValue::Array(preserved))
}

impl ScalarUDFImpl for SparkArrayAppend {
    crate::shim_udf_boilerplate!("array_append");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        spark_return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        spark_return_field(&args, self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_preserved(&args, false)
    }
}

impl ScalarUDFImpl for SparkArrayPrepend {
    crate::shim_udf_boilerplate!("array_prepend");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        spark_return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        spark_return_field(&args, self.name())
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
        assert!(error.to_string().contains("array_append"), "{error}");
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
