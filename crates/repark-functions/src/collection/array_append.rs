use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayData, ArrayRef, make_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::functions_nested::concat::{array_append_udf, array_prepend_udf};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::session_time_zone::session_time_zone_from_options;

mod coerce;

use coerce::{
    array_element_type, convert_columnar, spark_coerce_args, spark_return_field, spark_return_type,
    widened_list,
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
    let Some(target_element) = array_element_type(args.return_field.data_type()).cloned() else {
        return exec_err!("array function return type is not an array");
    };
    let zone = session_time_zone_from_options(args.config_options.as_ref())
        .parse::<Tz>()
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "session timezone could not be resolved at query time ({error})"
            ))
        })?;
    let array_type = args.args[0].data_type();
    let element_type = args.args[1].data_type();
    let array = convert_columnar(
        &args.args[0],
        &array_type,
        &widened_list(&array_type, target_element.clone()),
        zone,
    )?;
    let element = convert_columnar(&args.args[1], &element_type, &target_element, zone)?;
    let arrays = ColumnarValue::values_to_arrays(&[array, element])?;
    let input = Arc::clone(&arrays[0]);
    if input.null_count() == input.len() {
        let all_null = make_array(ArrayData::new_null(
            args.return_field.data_type(),
            input.len(),
        ));
        return finish_preserved(all_null, all_scalar);
    }
    let mut call_args: Vec<ColumnarValue> = arrays
        .iter()
        .map(|array| ColumnarValue::Array(Arc::clone(array)))
        .collect();
    let mut call_fields: Vec<FieldRef> = args
        .arg_fields
        .iter()
        .zip(arrays.iter())
        .map(|(field, array)| {
            Arc::new(Field::new(
                field.name(),
                array.data_type().clone(),
                field.is_nullable(),
            ))
        })
        .collect();
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
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use datafusion::common::ScalarValue;
    use datafusion::execution::config::SessionConfig;
    use datafusion::prelude::SessionContext;

    use super::coerce::spark_common_element;
    use super::*;
    use crate::instant_ts::{ltz_timestamp_type, ntz_timestamp_type};

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
    fn spark_common_element_resolves_the_recursive_tightest_common_type() {
        let list = |element: DataType| DataType::new_list(element, true);
        let ntz = ntz_timestamp_type();
        let ltz = ltz_timestamp_type();
        let timestamp_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let map = |key: DataType, value: DataType| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        [
                            Arc::new(Field::new("key", key, false)),
                            Arc::new(Field::new("value", value, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        let structure = |name: &str, data_type: DataType| {
            DataType::Struct([Arc::new(Field::new(name, data_type, true))].into())
        };
        for (array_element, element, want) in [
            (DataType::Int8, DataType::Int32, DataType::Int32),
            (DataType::Int16, DataType::Int8, DataType::Int16),
            (DataType::Int32, DataType::Int64, DataType::Int64),
            (DataType::Int64, DataType::Int16, DataType::Int64),
            (DataType::Int32, DataType::Float64, DataType::Float64),
            (DataType::Float32, DataType::Int32, DataType::Float32),
            (DataType::Float32, DataType::Int64, DataType::Float32),
            (DataType::Float16, DataType::Int32, DataType::Float16),
            (DataType::Float16, DataType::Float64, DataType::Float64),
            (DataType::Float64, DataType::Float32, DataType::Float64),
            (DataType::Float64, DataType::Int8, DataType::Float64),
            (DataType::Utf8, DataType::Utf8, DataType::Utf8),
            (DataType::Binary, DataType::Binary, DataType::Binary),
            (DataType::Boolean, DataType::Boolean, DataType::Boolean),
            (DataType::Null, DataType::Int32, DataType::Int32),
            (DataType::Int32, DataType::Null, DataType::Int32),
            (DataType::Date32, DataType::Date64, DataType::Date32),
            (DataType::Date64, DataType::Date64, DataType::Date64),
            (DataType::Date32, timestamp_ns.clone(), ltz.clone()),
            (timestamp_ns.clone(), DataType::Date32, ltz.clone()),
            (DataType::Date32, ntz.clone(), ntz.clone()),
            (ntz.clone(), timestamp_ns.clone(), ltz.clone()),
            (timestamp_ns.clone(), timestamp_ns.clone(), ltz.clone()),
            (ntz.clone(), ntz.clone(), ntz.clone()),
            (
                list(DataType::Int32),
                list(DataType::Int64),
                list(DataType::Int64),
            ),
            (
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
                list(DataType::Int64),
                list(DataType::Int64),
            ),
            (
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3),
                list(DataType::Int64),
                list(DataType::Int64),
            ),
            (
                list(list(DataType::Int32)),
                list(list(DataType::Int64)),
                list(list(DataType::Int64)),
            ),
            (
                list(DataType::Null),
                list(DataType::Int64),
                list(DataType::Int64),
            ),
            (
                map(DataType::Utf8, DataType::Int32),
                map(DataType::Utf8, DataType::Int64),
                map(DataType::Utf8, DataType::Int64),
            ),
            (
                structure("x", DataType::Int32),
                structure("X", DataType::Int64),
                structure("x", DataType::Int64),
            ),
        ] {
            assert_eq!(
                spark_common_element(&array_element, &element),
                Some(want.clone()),
                "common type of {array_element} + {element}"
            );
        }
    }

    #[test]
    fn coerce_types_validates_without_rewriting_args() {
        let udf = spark_array_append_udf();
        let list = |element: DataType| DataType::new_list(element, true);
        let got = udf
            .coerce_types(&[list(DataType::Int32), DataType::Int64])
            .expect("int + bigint validates");
        assert_eq!(got, vec![list(DataType::Int32), DataType::Int64]);
        let nested = list(DataType::Int32);
        let got = udf
            .coerce_types(&[list(nested.clone()), nested.clone()])
            .expect("equal nested types pass unchanged");
        assert_eq!(got, vec![list(nested.clone()), nested]);
        let got = udf
            .coerce_types(&[list(DataType::Int32), DataType::Null])
            .expect("a NULL element validates");
        assert_eq!(got, vec![list(DataType::Int32), DataType::Null]);
        let got = udf
            .coerce_types(&[DataType::Null, DataType::Int32])
            .expect("a NULL array validates");
        assert_eq!(got, vec![DataType::Null, DataType::Int32]);
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let got = udf
            .coerce_types(&[list(timestamp.clone()), timestamp.clone()])
            .expect("timestamp + timestamp validates");
        assert_eq!(got, vec![list(timestamp.clone()), timestamp.clone()]);
        let got = udf
            .coerce_types(&[list(DataType::Date32), timestamp.clone()])
            .expect("date + timestamp validates");
        assert_eq!(got, vec![list(DataType::Date32), timestamp]);
    }

    #[test]
    fn return_type_uses_microsecond_timestamps_and_recursive_widening() {
        let udf = spark_array_append_udf();
        let list = |element: DataType| DataType::new_list(element, true);
        let timestamp_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert_eq!(
            udf.return_type(&[list(DataType::Date32), timestamp_ns.clone()])
                .expect("date + timestamp"),
            list(ltz_timestamp_type())
        );
        assert_eq!(
            udf.return_type(&[list(timestamp_ns.clone()), timestamp_ns])
                .expect("timestamp + timestamp"),
            list(ltz_timestamp_type())
        );
        assert_eq!(
            udf.return_type(&[list(ntz_timestamp_type()), DataType::Date32])
                .expect("ntz + date"),
            list(ntz_timestamp_type())
        );
        let inner = list(DataType::Int64);
        assert_eq!(
            udf.return_type(&[list(list(DataType::Int32)), inner.clone()])
                .expect("nested array"),
            list(inner)
        );
        assert_eq!(
            udf.return_type(&[list(DataType::Float32), DataType::Int32])
                .expect("float + int"),
            list(DataType::Float32)
        );
    }

    #[test]
    fn coerce_types_refuses_everything_off_the_ladder() {
        let udf = spark_array_prepend_udf();
        let list = |element: DataType| DataType::new_list(element, true);
        let structure = |name: &str| {
            DataType::Struct([Arc::new(Field::new(name, DataType::Int32, true))].into())
        };
        let map = |value: DataType| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        [
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new("value", value, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
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
            (DataType::Int32, DataType::Int32),
            (list(DataType::UInt32), DataType::Int32),
            (list(DataType::UInt64), DataType::UInt32),
            (
                list(DataType::Time64(TimeUnit::Microsecond)),
                DataType::Time64(TimeUnit::Nanosecond),
            ),
            (
                list(DataType::Duration(TimeUnit::Second)),
                DataType::Duration(TimeUnit::Microsecond),
            ),
            (
                list(DataType::Interval(
                    datafusion::arrow::datatypes::IntervalUnit::MonthDayNano,
                )),
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
            ),
            (
                list(DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                )),
                DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            ),
            (list(list(DataType::Int32)), list(DataType::Utf8)),
            (list(structure("x")), structure("z")),
            (
                list(DataType::Struct(
                    [
                        Arc::new(Field::new("x", DataType::Int32, true)),
                        Arc::new(Field::new("y", DataType::Int32, true)),
                    ]
                    .into(),
                )),
                structure("x"),
            ),
            (list(map(DataType::Int32)), map(DataType::Utf8)),
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
            "[2024-01-02T00:00:00Z, 2024-05-06T07:08:09Z]"
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
