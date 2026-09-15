use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, MutableArrayData, make_array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DFSchema, DataFusionError, Result, exec_err};
use datafusion::logical_expr::{ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs};

use super::array_insert::tightest_common;

#[derive(Debug)]
pub(crate) struct ArrayConcatPlan {
    pub(crate) result: DataType,
}

pub(crate) fn is_list_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

pub(crate) fn is_binary_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

pub(crate) fn is_text_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn all_list_args(args: &[Expr], schema: &DFSchema) -> bool {
    !args.is_empty()
        && args.iter().all(|arg| {
            arg.get_type(schema)
                .is_ok_and(|data_type| is_list_family(&data_type) || data_type == DataType::Null)
        })
}

pub(crate) fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "VOID".to_owned(),
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 => "TINYINT".to_owned(),
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INT".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            format!("{data_type}")
        }
        DataType::Float16 | DataType::Float32 => "FLOAT".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_owned(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".to_owned(),
        DataType::Date32 | DataType::Date64 => "DATE".to_owned(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_owned(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        other => format!("{other}"),
    }
}

fn widen_elements(left: DataType, right: &DataType) -> Option<DataType> {
    if *right == DataType::Null {
        return Some(left);
    }
    if left == DataType::Null {
        return Some(right.clone());
    }
    if is_text_family(&left) && is_text_family(right) {
        return Some(DataType::Utf8);
    }
    if is_binary_family(&left) && is_binary_family(right) {
        return Some(DataType::Binary);
    }
    if let (Some(left_field), Some(right_field)) =
        (list_element_field(&left), list_element_field(right))
    {
        let widened = widen_elements(left_field.data_type().clone(), right_field.data_type())?;
        let contains_null = left_field.is_nullable() || right_field.is_nullable();
        return Some(DataType::List(Arc::new(Field::new(
            "element",
            widened,
            contains_null,
        ))));
    }
    tightest_common(&left, right)
}

fn list_element_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field)
        }
        _ => None,
    }
}

pub(crate) fn plan_array_concat(arg_types: &[DataType]) -> Result<ArrayConcatPlan> {
    let mut widened = DataType::Null;
    let mut contains_null = false;
    for arg_type in arg_types {
        match arg_type {
            DataType::Null => contains_null = true,
            listed if is_list_family(listed) => {
                let field = list_element_field(listed).ok_or_else(|| {
                    DataFusionError::Plan("concat reached an unshaped list".to_owned())
                })?;
                contains_null = contains_null || field.is_nullable();
                widened = widen_elements(widened, field.data_type())
                    .ok_or_else(|| data_diff_types(arg_types, field.data_type()))?;
            }
            other => return Err(data_diff_types(arg_types, other)),
        }
    }
    let element = Arc::new(Field::new("element", widened, contains_null));
    let result = DataType::List(Arc::clone(&element));
    Ok(ArrayConcatPlan { result })
}

fn data_diff_types(arg_types: &[DataType], _offender: &DataType) -> DataFusionError {
    let rendered: Vec<String> = arg_types.iter().map(spark_type_name).collect();
    let pair = if rendered.len() == 2 {
        format!("(\"{}\" or \"{}\")", rendered[0], rendered[1])
    } else {
        format!("({})", rendered.join(", "))
    };
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"concat(...)\" due to data type \
         mismatch: Input to `concat` should all be the same type, but it's {pair}."
    ))
}

fn offset_as_usize(offset: i32) -> Result<usize> {
    usize::try_from(offset)
        .map_err(|_| DataFusionError::Execution("concat list offset does not fit usize".to_owned()))
}

pub(crate) fn invoke_array_concat(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ScalarFunctionArgs {
        args: arg_values,
        return_field,
        ..
    } = args;
    let DataType::List(result_field) = return_field.data_type() else {
        return exec_err!("concat array arm needs a list return");
    };
    if arg_values.is_empty() {
        return exec_err!("concat array arm requires at least one argument");
    }
    let target = DataType::List(Arc::clone(result_field));
    let arrays = ColumnarValue::values_to_arrays(&arg_values)?;
    let mut lists: Vec<ListArray> = Vec::with_capacity(arrays.len());
    for array in &arrays {
        let shaped: ArrayRef = if array.data_type() == &target {
            Arc::clone(array)
        } else {
            cast(array.as_ref(), &target)?
        };
        lists.push(shaped.as_list::<i32>().clone());
    }
    let row_count = lists.first().map_or(0, ListArray::len);
    let child_data: Vec<_> = lists.iter().map(|list| list.values().to_data()).collect();
    let child_refs: Vec<_> = child_data.iter().collect();
    let child_len: usize = child_data
        .iter()
        .map(datafusion::arrow::array::ArrayData::len)
        .sum();
    let mut mutable = MutableArrayData::new(child_refs, false, child_len);
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    for row in 0..row_count {
        if lists.iter().any(|list| list.is_null(row)) {
            validity.push(false);
            any_null = true;
            offsets.push(mutable.len().try_into().map_err(|_| {
                DataFusionError::Execution("concat row count does not fit i32".to_owned())
            })?);
            continue;
        }
        validity.push(true);
        for (index, list) in lists.iter().enumerate() {
            let bounds = list.value_offsets();
            let start = offset_as_usize(bounds[row])?;
            let end = offset_as_usize(bounds[row + 1])?;
            mutable.extend(index, start, end);
        }
        offsets.push(mutable.len().try_into().map_err(|_| {
            DataFusionError::Execution("concat row count does not fit i32".to_owned())
        })?);
    }
    let values = make_array(mutable.freeze());
    let nulls = if any_null {
        Some(NullBuffer::from(validity))
    } else {
        None
    };
    Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
        Arc::clone(result_field),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    )?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn value_of(ctx: &SessionContext, sql: &str) -> Vec<ScalarValue> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let column = batches[0].column(0);
        (0..column.len())
            .map(|row| {
                ScalarValue::try_from_array(column.as_ref(), row)
                    .unwrap_or_else(|error| panic!("scalar {sql}: {error}"))
            })
            .collect()
    }

    fn int_cells(scalar: &ScalarValue) -> Option<Vec<Option<i64>>> {
        match scalar {
            ScalarValue::List(values) => {
                if values.is_null(0) {
                    return None;
                }
                let inner = values.value(0);
                match inner.data_type() {
                    DataType::Int32 => {
                        let numbers = inner.as_primitive::<Int32Type>();
                        Some(
                            (0..numbers.len())
                                .map(|slot| {
                                    if numbers.is_null(slot) {
                                        None
                                    } else {
                                        Some(i64::from(numbers.value(slot)))
                                    }
                                })
                                .collect(),
                        )
                    }
                    DataType::Int64 => {
                        let numbers = inner
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                            .expect("int64 values");
                        Some(
                            (0..numbers.len())
                                .map(|slot| {
                                    if numbers.is_null(slot) {
                                        None
                                    } else {
                                        Some(numbers.value(slot))
                                    }
                                })
                                .collect(),
                        )
                    }
                    other => panic!("unexpected element {other}"),
                }
            }
            other => panic!("unexpected scalar {other}"),
        }
    }

    fn list_of(element: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("element", element, true)))
    }

    #[test]
    fn int_and_bigint_widen_to_bigint() {
        let plan = plan_array_concat(&[list_of(DataType::Int32), list_of(DataType::Int64)])
            .expect("widen");
        assert_eq!(
            plan.result,
            DataType::List(Arc::new(Field::new("element", DataType::Int64, true)))
        );
    }

    #[test]
    fn int_and_decimal_widen_to_decimal_11_1() {
        let plan = plan_array_concat(&[
            list_of(DataType::Int32),
            list_of(DataType::Decimal128(2, 1)),
        ])
        .expect("widen");
        assert_eq!(
            plan.result,
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Decimal128(11, 1),
                true
            )))
        );
    }

    #[test]
    fn mixed_array_and_string_refuses_data_diff_types() {
        let error = plan_array_concat(&[
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            DataType::Utf8,
        ])
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("DATATYPE_MISMATCH.DATA_DIFF_TYPES"),
            "{error}"
        );
        assert!(
            error.contains("ARRAY<INT>") && error.contains("STRING"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn concat_arrays_joins_values_and_propagates_null() {
        let ctx = ctx();
        let joined = value_of(&ctx, "SELECT concat(array(1), array(2))").await;
        assert_eq!(joined.len(), 1);
        assert_eq!(int_cells(&joined[0]), Some(vec![Some(1), Some(2)]));
        let with_null_element = value_of(
            &ctx,
            "SELECT concat(array(CAST(1 AS BIGINT), CAST(NULL AS BIGINT)), array(CAST(2 AS BIGINT)))",
        )
        .await;
        assert_eq!(with_null_element.len(), 1);
        assert_eq!(
            int_cells(&with_null_element[0]),
            Some(vec![Some(1), None, Some(2)])
        );
        let nulled = value_of(&ctx, "SELECT concat(array(1), CAST(NULL AS ARRAY<INT>))").await;
        assert_eq!(nulled.len(), 1);
        assert_eq!(int_cells(&nulled[0]), None);
    }

    #[tokio::test]
    async fn concat_result_type_widens_and_reports_plan_nullability() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT concat(array(1), array(CAST(2 AS BIGINT)))")
            .await
            .expect("plan widen")
            .collect()
            .await
            .expect("execute widen");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Int64, true)))
        );
        let scalar = ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0).expect("scalar");
        let ScalarValue::List(values) = &scalar else {
            panic!("expected list, got {scalar:?}");
        };
        let row = values.value(0);
        let inner = row
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("widened values");
        assert_eq!(inner.values(), &[1, 2]);
    }

    #[tokio::test]
    async fn pipe_operator_over_arrays_resolves_the_concat_kernel() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT array(1) || array(2)")
            .await
            .expect("plan pipe")
            .collect()
            .await
            .expect("execute pipe");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Int32, true)))
        );
        assert!(!batches[0].schema().field(0).is_nullable());
        let joined = value_of(&ctx, "SELECT array(1) || array(2)").await;
        assert_eq!(joined.len(), 1);
        assert_eq!(int_cells(&joined[0]), Some(vec![Some(1), Some(2)]));
    }

    #[tokio::test]
    async fn concat_decimal_widen_matches_spark_range_scale() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT concat(array(1), array(CAST(1.5 AS DECIMAL(2, 1))))")
            .await
            .expect("plan decimal widen")
            .collect()
            .await
            .expect("execute decimal widen");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new(
                "element",
                DataType::Decimal128(11, 1),
                true
            )))
        );
    }
}
