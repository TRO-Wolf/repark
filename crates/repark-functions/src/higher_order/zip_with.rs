//! Spark `zip_with` — pairwise lambda over two arrays, null-padding the shorter.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, ListArray, OffsetSizeTrait, UInt32Builder,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{take, take_arrays};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef};
use datafusion::common::{
    Result, ScalarValue, exec_err, plan_err,
    utils::{adjust_offsets_for_slice, take_function_args},
};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::list_element_field;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkZipWith {
    signature: HigherOrderSignature,
}

impl Default for SparkZipWith {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkZipWith {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Lambda(()),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

fn coerce_list(name: &str, list: &DataType) -> Result<DataType> {
    match list {
        DataType::List(_) | DataType::LargeList(_) => Ok(list.clone()),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            Ok(DataType::List(Arc::clone(field)))
        }
        DataType::LargeListView(field) => Ok(DataType::LargeList(Arc::clone(field))),
        DataType::Null => Ok(DataType::new_list(DataType::Null, true)),
        other => plan_err!("{name} expected a list, got {other}"),
    }
}

impl HigherOrderUDFImpl for SparkZipWith {
    fn name(&self) -> &'static str {
        "zip_with"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!(
                "{} requires 2 value arguments, got {}",
                self.name(),
                arg_types.len()
            );
        }
        Ok(vec![
            coerce_list(self.name(), &arg_types[0])?,
            coerce_list(self.name(), &arg_types[1])?,
        ])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [left, right, lambda] = take_function_args(self.name(), fields)?;
        let (ValueOrLambda::Value(left), ValueOrLambda::Value(right), ValueOrLambda::Lambda(_)) =
            (left, right, lambda)
        else {
            return plan_err!("{} expects two lists followed by a lambda", self.name());
        };
        Ok(LambdaParametersProgress::Complete(vec![vec![
            list_element_field(self.name(), left)?,
            list_element_field(self.name(), right)?,
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let [left, right, lambda] = take_function_args(self.name(), args.arg_fields)?;
        let (
            ValueOrLambda::Value(left),
            ValueOrLambda::Value(right),
            ValueOrLambda::Lambda(lambda),
        ) = (left, right, lambda)
        else {
            return plan_err!("{} expects two lists followed by a lambda", self.name());
        };
        let field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));
        let return_type = DataType::List(field);
        Ok(Arc::new(Field::new(
            "",
            return_type,
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [left, right, lambda] = take_function_args(self.name(), &args.args)?;
        let (
            ValueOrLambda::Value(left),
            ValueOrLambda::Value(right),
            ValueOrLambda::Lambda(lambda),
        ) = (left, right, lambda)
        else {
            return plan_err!("{} expects two lists followed by a lambda", self.name());
        };
        let left_array = left.to_array(args.number_rows)?;
        let right_array = right.to_array(args.number_rows)?;
        if left_array.len() != right_array.len() {
            return exec_err!(
                "{} left and right arrays must have the same number of rows",
                self.name()
            );
        }
        if left_array.null_count() == left_array.len()
            || right_array.null_count() == right_array.len()
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                args.return_type(),
            )?));
        }
        let padded = pad_pair(left_array.as_ref(), right_array.as_ref())?;
        let left_param = || Ok(Arc::clone(&padded.left_values));
        let right_param = || Ok(Arc::clone(&padded.right_values));
        let transformed = lambda
            .evaluate(&[&left_param, &right_param], |arrays| {
                let indices = zip_row_numbers(&padded.offsets, left_array.len())?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(padded.left_values.len())?;
        let field = match args.return_field.data_type() {
            DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
            other => {
                return exec_err!(
                    "{} expected return_field to be a list, got {other}",
                    self.name()
                );
            }
        };
        let nulls = union_nulls(left_array.nulls(), right_array.nulls());
        let result = Arc::new(ListArray::try_new(
            field,
            padded.offsets,
            transformed,
            nulls,
        )?) as ArrayRef;
        Ok(ColumnarValue::Array(result))
    }
}

struct PaddedPair {
    left_values: ArrayRef,
    right_values: ArrayRef,
    offsets: OffsetBuffer<i32>,
}

fn pad_pair(left: &dyn Array, right: &dyn Array) -> Result<PaddedPair> {
    match (left.data_type(), right.data_type()) {
        (DataType::List(_), DataType::List(_)) => {
            pad_generic(left.as_list::<i32>(), right.as_list::<i32>())
        }
        (DataType::LargeList(_), DataType::LargeList(_)) => {
            pad_generic(left.as_list::<i64>(), right.as_list::<i64>())
        }
        (DataType::List(_), DataType::LargeList(_)) => {
            pad_generic(left.as_list::<i32>(), right.as_list::<i64>())
        }
        (DataType::LargeList(_), DataType::List(_)) => {
            pad_generic(left.as_list::<i64>(), right.as_list::<i32>())
        }
        (left_type, right_type) => exec_err!("expected lists, got {left_type} and {right_type}"),
    }
}

fn pad_generic<O1: OffsetSizeTrait, O2: OffsetSizeTrait>(
    left: &GenericListArray<O1>,
    right: &GenericListArray<O2>,
) -> Result<PaddedPair> {
    let left_offsets = adjust_offsets_for_slice(left);
    let right_offsets = adjust_offsets_for_slice(right);
    let left_first = left_offsets.first().map_or(0, |offset| offset.as_usize());
    let right_first = right_offsets.first().map_or(0, |offset| offset.as_usize());
    let mut left_indices = UInt32Builder::new();
    let mut right_indices = UInt32Builder::new();
    let mut lengths = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) {
            lengths.push(0);
            continue;
        }
        let left_len = left_offsets[row + 1].as_usize() - left_offsets[row].as_usize();
        let right_len = right_offsets[row + 1].as_usize() - right_offsets[row].as_usize();
        let max_len = left_len.max(right_len);
        lengths.push(max_len);
        for position in 0..max_len {
            if position < left_len {
                let index = u32::try_from(left_offsets[row].as_usize() - left_first + position)
                    .map_err(|_| {
                        datafusion::error::DataFusionError::Execution(
                            "zip_with left index does not fit in u32".to_string(),
                        )
                    })?;
                left_indices.append_value(index);
            } else {
                left_indices.append_null();
            }
            if position < right_len {
                let index = u32::try_from(right_offsets[row].as_usize() - right_first + position)
                    .map_err(|_| {
                    datafusion::error::DataFusionError::Execution(
                        "zip_with right index does not fit in u32".to_string(),
                    )
                })?;
                right_indices.append_value(index);
            } else {
                right_indices.append_null();
            }
        }
    }
    let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
    Ok(PaddedPair {
        left_values: take(left.values().as_ref(), &left_indices.finish(), None)?,
        right_values: take(right.values().as_ref(), &right_indices.finish(), None)?,
        offsets,
    })
}

fn zip_row_numbers(offsets: &OffsetBuffer<i32>, rows: usize) -> Result<ArrayRef> {
    let mut builder = datafusion::arrow::array::UInt32Builder::with_capacity(
        offsets.last().map_or(0, |offset| offset.as_usize()),
    );
    for row in 0..rows {
        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let row_number = u32::try_from(row).map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "zip_with row does not fit in u32".to_string(),
            )
        })?;
        for _ in start..end {
            builder.append_value(row_number);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn union_nulls(
    left: Option<&datafusion::arrow::buffer::NullBuffer>,
    right: Option<&datafusion::arrow::buffer::NullBuffer>,
) -> Option<datafusion::arrow::buffer::NullBuffer> {
    datafusion::arrow::buffer::NullBuffer::union(left, right)
}

pub fn zip_with_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkZipWith::new())));
    Arc::clone(&INSTANCE)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;

    use super::{HigherOrderReturnFieldArgs, HigherOrderUDFImpl, SparkZipWith, ValueOrLambda};

    /// pins: fnp-4c-higher-order-kernels/C-006
    #[test]
    fn return_field_is_nullable_when_only_the_right_array_is_nullable() {
        let kernel = SparkZipWith::new();
        let left = Arc::new(Field::new(
            "left",
            DataType::new_list(DataType::Int32, true),
            false,
        ));
        let right = Arc::new(Field::new(
            "right",
            DataType::new_list(DataType::Int32, true),
            true,
        ));
        let lambda = Arc::new(Field::new("", DataType::Int32, true));
        let arg_fields = [
            ValueOrLambda::Value(left),
            ValueOrLambda::Value(right),
            ValueOrLambda::Lambda(lambda),
        ];
        let scalar_arguments: [Option<&ScalarValue>; 3] = [None, None, None];
        let field = kernel
            .return_field_from_args(HigherOrderReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .expect("return field");
        assert!(field.is_nullable());
    }
}
