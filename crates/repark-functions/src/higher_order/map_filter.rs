//! Spark `map_filter` — keep entries whose `(k, v)` predicate is true.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err, utils::take_function_args};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::require_boolean_lambda;
use super::map_common::{
    coerce_single_map_arg, filter_map_entries, flatten_map, map_key_value_fields, map_row_numbers,
    rebuild_map,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFilter {
    signature: HigherOrderSignature,
}

impl Default for SparkMapFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapFilter {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkMapFilter {
    fn name(&self) -> &'static str {
        "map_filter"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_single_map_arg(self.name(), arg_types)
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [map, _lambda] = take_function_args(self.name(), fields)?;
        let ValueOrLambda::Value(map) = map else {
            return plan_err!("{} expects a map as the first argument", self.name());
        };
        let (key, value) = map_key_value_fields(self.name(), map)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![key, value]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let [map, lambda] = take_function_args(self.name(), args.arg_fields)?;
        let (ValueOrLambda::Value(map), ValueOrLambda::Lambda(lambda)) = (map, lambda) else {
            return plan_err!("{} expects a map followed by a lambda", self.name());
        };
        require_boolean_lambda(self.name(), lambda)?;
        Ok(Arc::new(Field::new(
            "",
            map.data_type().clone(),
            map.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [map, lambda] = take_function_args(self.name(), &args.args)?;
        let (ValueOrLambda::Value(map), ValueOrLambda::Lambda(lambda)) = (map, lambda) else {
            return exec_err!("{} expects a map followed by a lambda", self.name());
        };
        let map_array = map.to_array(args.number_rows)?;
        if map_array.null_count() == map_array.len() {
            return Ok(ColumnarValue::Array(
                datafusion::arrow::array::new_null_array(args.return_type(), map_array.len()),
            ));
        }
        let flat = flatten_map(self.name(), &map_array)?;
        let keys_param = || Ok(Arc::clone(&flat.keys));
        let values_param = || Ok(Arc::clone(&flat.values));
        let predicate_output = lambda.evaluate(&[&keys_param, &values_param], |arrays| {
            let indices = map_row_numbers(&flat.offsets, map_array.len())?;
            Ok(take_arrays(arrays, &indices, None)?)
        })?;
        let (key_field, value_field) = map_key_value_fields(
            self.name(),
            &Arc::new(Field::new("", map_array.data_type().clone(), true)),
        )?;
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) = &predicate_output {
            return Ok(ColumnarValue::Array(map_array));
        }
        if let ColumnarValue::Scalar(ScalarValue::Boolean(_)) = &predicate_output {
            let empty_offsets = datafusion::arrow::buffer::OffsetBuffer::from_lengths(vec![
                0usize;
                map_array.len()
            ]);
            let rebuilt = rebuild_map(
                datafusion::arrow::array::new_empty_array(flat.keys.data_type()),
                datafusion::arrow::array::new_empty_array(flat.values.data_type()),
                empty_offsets,
                flat.nulls,
                key_field.as_ref(),
                value_field.as_ref(),
                flat.ordered,
            )?;
            return Ok(ColumnarValue::Array(rebuilt));
        }
        let predicate = predicate_output.into_array(flat.keys.len())?;
        let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
            return exec_err!(
                "{} lambda must return boolean, got {}",
                self.name(),
                predicate.data_type()
            );
        };
        let (filtered_keys, filtered_values, new_offsets) =
            filter_map_entries(&flat.keys, &flat.values, predicate, &flat.offsets)?;
        let rebuilt = rebuild_map(
            filtered_keys,
            filtered_values,
            new_offsets,
            flat.nulls,
            key_field.as_ref(),
            value_field.as_ref(),
            flat.ordered,
        )?;
        Ok(ColumnarValue::Array(rebuilt))
    }
}

pub fn map_filter_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkMapFilter::new())));
    Arc::clone(&INSTANCE)
}
