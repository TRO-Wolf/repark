//! Spark `map_zip_with` — ternary `(k, v1, v2)` over the union of two maps' keys.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{ArrayRef, UInt32Builder};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{take, take_arrays};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err, utils::take_function_args};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::map_common::{
    coerce_two_map_args, flatten_map, map_key_value_fields, rebuild_map, refuse_duplicate_keys,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapZipWith {
    signature: HigherOrderSignature,
}

impl Default for SparkMapZipWith {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapZipWith {
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

impl HigherOrderUDFImpl for SparkMapZipWith {
    fn name(&self) -> &'static str {
        "map_zip_with"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_two_map_args(self.name(), arg_types)
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
            return plan_err!("{} expects two maps followed by a lambda", self.name());
        };
        let (key, value1) = map_key_value_fields(self.name(), left)?;
        let (_key2, value2) = map_key_value_fields(self.name(), right)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![
            key, value1, value2,
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
            return plan_err!("{} expects two maps followed by a lambda", self.name());
        };
        let (key, _value) = map_key_value_fields(self.name(), left)?;
        let DataType::Map(_, ordered) = left.data_type() else {
            return plan_err!("{} expected a map", self.name());
        };
        let new_value = Arc::new(Field::new(
            "value",
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key, new_value].into()),
            false,
        ));
        Ok(Arc::new(Field::new(
            "",
            DataType::Map(entries, *ordered),
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
            return exec_err!("{} expects two maps followed by a lambda", self.name());
        };
        let left_array = left.to_array(args.number_rows)?;
        let right_array = right.to_array(args.number_rows)?;
        if left_array.null_count() == left_array.len() {
            return Ok(ColumnarValue::Array(
                datafusion::arrow::array::new_null_array(args.return_type(), left_array.len()),
            ));
        }
        let left_flat = flatten_map(self.name(), &left_array)?;
        let right_flat = flatten_map(self.name(), &right_array)?;
        refuse_duplicate_keys(
            left_flat.keys.as_ref(),
            &left_flat.offsets,
            left_flat.nulls.as_ref(),
        )?;
        refuse_duplicate_keys(
            right_flat.keys.as_ref(),
            &right_flat.offsets,
            right_flat.nulls.as_ref(),
        )?;
        let aligned = align_maps(&left_flat, &right_flat, left_array.len())?;
        let key_param = || Ok(Arc::clone(&aligned.keys));
        let v1_param = || Ok(Arc::clone(&aligned.values1));
        let v2_param = || Ok(Arc::clone(&aligned.values2));
        let new_values = lambda
            .evaluate(&[&key_param, &v1_param, &v2_param], |arrays| {
                Ok(take_arrays(arrays, &aligned.row_numbers, None)?)
            })?
            .into_array(aligned.keys.len())?;
        let nulls = datafusion::arrow::buffer::NullBuffer::union(
            left_flat.nulls.as_ref(),
            right_flat.nulls.as_ref(),
        );
        let (key_field, value_field) = match args.return_field.data_type() {
            DataType::Map(entries, _) => map_key_value_fields(
                self.name(),
                &Arc::new(Field::new(
                    "",
                    DataType::Map(Arc::clone(entries), false),
                    true,
                )),
            )?,
            other => return exec_err!("{} expected a map return, got {other}", self.name()),
        };
        let rebuilt = rebuild_map(
            aligned.keys,
            new_values,
            aligned.offsets,
            nulls,
            key_field.as_ref(),
            value_field.as_ref(),
            left_flat.ordered,
        )?;
        Ok(ColumnarValue::Array(rebuilt))
    }
}

struct AlignedMaps {
    keys: ArrayRef,
    values1: ArrayRef,
    values2: ArrayRef,
    offsets: OffsetBuffer<i32>,
    row_numbers: ArrayRef,
}

fn align_maps(
    left: &super::map_common::FlatMap,
    right: &super::map_common::FlatMap,
    rows: usize,
) -> Result<AlignedMaps> {
    let mut key_indices = UInt32Builder::new();
    let mut v1_indices = UInt32Builder::new();
    let mut v2_indices = UInt32Builder::new();
    let mut row_numbers = UInt32Builder::new();
    let mut lengths = Vec::with_capacity(rows);
    for row in 0..rows {
        if left.nulls.as_ref().is_some_and(|nulls| nulls.is_null(row))
            || right.nulls.as_ref().is_some_and(|nulls| nulls.is_null(row))
        {
            lengths.push(0);
            continue;
        }
        let left_start = left.offsets[row].as_usize();
        let left_end = left.offsets[row + 1].as_usize();
        let right_start = right.offsets[row].as_usize();
        let right_end = right.offsets[row + 1].as_usize();
        let mut right_lookup: HashMap<ScalarValue, usize> =
            HashMap::with_capacity(right_end.saturating_sub(right_start));
        for index in right_start..right_end {
            let key = ScalarValue::try_from_array(right.keys.as_ref(), index)?;
            right_lookup.insert(key, index);
        }
        let mut used_right: HashSet<usize> = HashSet::new();
        let row_number = u32::try_from(row).map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "map_zip_with row does not fit in u32".to_string(),
            )
        })?;
        let mut count = 0usize;
        for index in left_start..left_end {
            let key = ScalarValue::try_from_array(left.keys.as_ref(), index)?;
            push_u32(&mut key_indices, index)?;
            push_u32(&mut v1_indices, index)?;
            match right_lookup.get(&key) {
                Some(right_index) => {
                    push_u32(&mut v2_indices, *right_index)?;
                    used_right.insert(*right_index);
                }
                None => v2_indices.append_null(),
            }
            row_numbers.append_value(row_number);
            count += 1;
        }
        for index in right_start..right_end {
            if used_right.contains(&index) {
                continue;
            }
            let shifted = left.keys.len() + index;
            push_u32(&mut key_indices, shifted)?;
            v1_indices.append_null();
            push_u32(&mut v2_indices, index)?;
            row_numbers.append_value(row_number);
            count += 1;
        }
        lengths.push(count);
    }
    let concatenated_keys =
        datafusion::arrow::compute::concat(&[left.keys.as_ref(), right.keys.as_ref()])?;
    Ok(AlignedMaps {
        keys: take(concatenated_keys.as_ref(), &key_indices.finish(), None)?,
        values1: take(left.values.as_ref(), &v1_indices.finish(), None)?,
        values2: take(right.values.as_ref(), &v2_indices.finish(), None)?,
        offsets: OffsetBuffer::from_lengths(lengths),
        row_numbers: Arc::new(row_numbers.finish()),
    })
}

fn push_u32(builder: &mut UInt32Builder, index: usize) -> Result<()> {
    let value = u32::try_from(index).map_err(|_| {
        datafusion::error::DataFusionError::Execution("map index does not fit in u32".to_string())
    })?;
    builder.append_value(value);
    Ok(())
}

pub fn map_zip_with_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkMapZipWith::new())));
    Arc::clone(&INSTANCE)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;

    use super::{HigherOrderReturnFieldArgs, HigherOrderUDFImpl, SparkMapZipWith, ValueOrLambda};

    fn map_field(name: &str, nullable: bool) -> Arc<Field> {
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Int32, true)),
                ]
                .into(),
            ),
            false,
        ));
        Arc::new(Field::new(name, DataType::Map(entries, false), nullable))
    }

    /// pins: fnp-4c-higher-order-kernels/C-010
    #[test]
    fn return_field_is_nullable_when_only_the_right_map_is_nullable() {
        let kernel = SparkMapZipWith::new();
        let left = map_field("left", false);
        let right = map_field("right", true);
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
