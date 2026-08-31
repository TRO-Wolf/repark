//! Shared list helpers for Spark higher-order kernels.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, GenericListArray, Int32Builder, LargeListArray,
    ListArray, OffsetBufferBuilder, OffsetSizeTrait, new_empty_array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::filter as arrow_filter;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{
    Result, ScalarValue, exec_err, plan_err,
    utils::{adjust_offsets_for_slice, list_values, take_function_args},
};
use datafusion::logical_expr::{ColumnarValue, LambdaParametersProgress, ValueOrLambda};

pub(crate) fn value_lambda_pair<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a L)> {
    let [value, lambda] = take_function_args(name, args)?;
    let (ValueOrLambda::Value(value), ValueOrLambda::Lambda(lambda)) = (value, lambda) else {
        return plan_err!(
            "{name} expects a value followed by a lambda, got {value:?} and {lambda:?}"
        );
    };
    Ok((value, lambda))
}

pub(crate) fn coerce_single_list_arg(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let list = if arg_types.len() == 1 {
        &arg_types[0]
    } else {
        return plan_err!(
            "{name} function requires 1 value arguments, got {}",
            arg_types.len()
        );
    };
    let coerced = match list {
        DataType::List(_) | DataType::LargeList(_) => list.clone(),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::clone(field))
        }
        DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
        DataType::Null => DataType::new_list(DataType::Null, true),
        _ => return plan_err!("{name} expected a list as first argument, got {list}"),
    };
    Ok(vec![coerced])
}

pub(crate) fn list_element_field(name: &str, list: &FieldRef) -> Result<FieldRef> {
    match list.data_type() {
        DataType::List(field) | DataType::LargeList(field) => Ok(Arc::clone(field)),
        other => plan_err!("{name} expected a list, got {other}"),
    }
}

pub(crate) fn element_and_index_parameters(
    name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    let (list, _lambda) = value_lambda_pair(name, fields)?;
    let element = list_element_field(name, list)?;
    let index = Arc::new(Field::new("index", DataType::Int32, true));
    Ok(LambdaParametersProgress::Complete(vec![vec![
        element, index,
    ]]))
}

pub(crate) enum ListValuesResult {
    EarlyReturn(ColumnarValue),
    Values(ArrayRef),
}

pub(crate) fn extract_list_values(
    list_array: &ArrayRef,
    return_type: &DataType,
) -> Result<ListValuesResult> {
    if list_array.null_count() == list_array.len() {
        return Ok(ListValuesResult::EarlyReturn(ColumnarValue::Scalar(
            ScalarValue::try_new_null(return_type)?,
        )));
    }
    let values = list_values(list_array)?;
    if values.is_empty()
        && list_array.null_count() == 0
        && matches!(return_type, DataType::List(_) | DataType::LargeList(_))
    {
        return Ok(ListValuesResult::EarlyReturn(ColumnarValue::Scalar(
            ScalarValue::new_default(return_type)?,
        )));
    }
    Ok(ListValuesResult::Values(values))
}

pub(crate) fn evaluate_element_and_index(
    lambda: &datafusion::logical_expr::LambdaArgument,
    element: &ArrayRef,
    index: &ArrayRef,
    spread: impl Fn(&[ArrayRef]) -> Result<Vec<ArrayRef>>,
) -> Result<ColumnarValue> {
    let element_param = || Ok(Arc::clone(element));
    let index_param = || Ok(Arc::clone(index));
    lambda.evaluate(&[&element_param, &index_param], spread)
}

pub(crate) fn list_element_index_array(list_array: &dyn Array) -> Result<ArrayRef> {
    match list_array.data_type() {
        DataType::List(_) => indices_from_list(list_array.as_list::<i32>()),
        DataType::LargeList(_) => indices_from_list(list_array.as_list::<i64>()),
        other => exec_err!("expected list, got {other}"),
    }
}

fn indices_from_list<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> Result<ArrayRef> {
    let offsets = adjust_offsets_for_slice(list);
    let value_count = offsets.last().map_or(0, |offset| offset.as_usize());
    let mut builder = Int32Builder::with_capacity(value_count);
    for row in 0..list.len() {
        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        for position in 0..(end - start) {
            let index = i32::try_from(position).map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "list element index {position} does not fit in INT"
                ))
            })?;
            builder.append_value(index);
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(crate) fn assemble_transformed_list(
    list_array: &ArrayRef,
    transformed_values: ArrayRef,
    field: FieldRef,
) -> Result<ArrayRef> {
    match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            Ok(Arc::new(ListArray::new(
                field,
                adjust_offsets_for_slice(list),
                transformed_values,
                list.nulls().cloned(),
            )) as ArrayRef)
        }
        DataType::LargeList(_) => {
            let large_list = list_array.as_list::<i64>();
            Ok(Arc::new(LargeListArray::new(
                field,
                adjust_offsets_for_slice(large_list),
                transformed_values,
                large_list.nulls().cloned(),
            )))
        }
        other => exec_err!("expected list, got {other}"),
    }
}

pub(crate) fn list_field_from_return(name: &str, return_field: &Field) -> Result<FieldRef> {
    match return_field.data_type() {
        DataType::List(field) | DataType::LargeList(field) => Ok(Arc::clone(field)),
        other => exec_err!("{name} expected return_field to be a list, got {other}"),
    }
}

pub(crate) fn require_boolean_lambda(name: &str, lambda_field: &Field) -> Result<()> {
    if lambda_field.data_type() == &DataType::Boolean {
        return Ok(());
    }
    plan_err!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}\" due to data type \
         mismatch: The lambda requires the \"BOOLEAN\" type, however the lambda has the type \
         \"{}\".",
        lambda_field.data_type()
    )
}

pub(crate) fn filter_list_values<O: OffsetSizeTrait>(
    values: &ArrayRef,
    predicate: &BooleanArray,
    offsets: &OffsetBuffer<O>,
) -> Result<(ArrayRef, OffsetBuffer<O>)> {
    let num_sublists = offsets.len().saturating_sub(1);
    let mut builder = OffsetBufferBuilder::<O>::new(num_sublists);
    let has_nulls = predicate.null_count() > 0;
    for i in 0..num_sublists {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        let count = if has_nulls {
            (start..end)
                .filter(|&j| predicate.is_valid(j) && predicate.value(j))
                .count()
        } else {
            predicate
                .values()
                .slice(start, end - start)
                .count_set_bits()
        };
        builder.push_length(count);
    }
    let new_offsets = builder.finish();
    if new_offsets.last() == offsets.last() {
        return Ok((Arc::clone(values), offsets.clone()));
    }
    let filtered_values = arrow_filter(values.as_ref(), predicate)?;
    Ok((filtered_values, new_offsets))
}

pub(crate) fn empty_filtered_list(list_array: &ArrayRef, field: FieldRef) -> Result<ArrayRef> {
    let n = list_array.len();
    let empty_values = new_empty_array(field.data_type());
    Ok(match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32; n + 1])),
                empty_values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(_) => {
            let large_list = list_array.as_list::<i64>();
            Arc::new(LargeListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i64; n + 1])),
                empty_values,
                large_list.nulls().cloned(),
            ))
        }
        other => return exec_err!("expected list, got {other}"),
    })
}
