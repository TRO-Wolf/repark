//! Spark `transform_keys` — `(k, v) -> new_key`.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err, plan_err, utils::take_function_args};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::map_common::{
    coerce_single_map_arg, flatten_map, map_key_value_fields, map_row_numbers, rebuild_map,
    refuse_duplicate_keys, refuse_null_keys,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTransformKeys {
    signature: HigherOrderSignature,
}

impl Default for SparkTransformKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTransformKeys {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkTransformKeys {
    fn name(&self) -> &'static str {
        "transform_keys"
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
        let (_key, value) = map_key_value_fields(self.name(), map)?;
        let DataType::Map(_, ordered) = map.data_type() else {
            return plan_err!("{} expected a map", self.name());
        };
        let new_key = Arc::new(Field::new("key", lambda.data_type().clone(), false));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![new_key, value].into()),
            false,
        ));
        Ok(Arc::new(Field::new(
            "",
            DataType::Map(entries, *ordered),
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
        let new_keys = lambda
            .evaluate(&[&keys_param, &values_param], |arrays| {
                let indices = map_row_numbers(&flat.offsets, map_array.len())?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(flat.keys.len())?;
        refuse_null_keys(new_keys.as_ref())?;
        refuse_duplicate_keys(new_keys.as_ref(), &flat.offsets, flat.nulls.as_ref())?;
        let (new_key_field, value_field) = match args.return_field.data_type() {
            DataType::Map(entries, _) => map_key_value_fields(self.name(), &{
                Arc::new(Field::new(
                    "",
                    DataType::Map(Arc::clone(entries), false),
                    true,
                ))
            })?,
            other => return exec_err!("{} expected a map return, got {other}", self.name()),
        };
        let rebuilt = rebuild_map(
            new_keys,
            flat.values,
            flat.offsets,
            flat.nulls,
            new_key_field.as_ref(),
            value_field.as_ref(),
            flat.ordered,
        )?;
        Ok(ColumnarValue::Array(rebuilt))
    }
}

pub fn transform_keys_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkTransformKeys::new())));
    Arc::clone(&INSTANCE)
}
