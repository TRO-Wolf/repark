//! Spark `forall` — three-valued all-match (`NOT exists(x -> NOT p(x))`).

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, AsArray, BooleanArray, BooleanBuilder, new_null_array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef};
use datafusion::common::{
    Result, exec_datafusion_err, exec_err,
    utils::{adjust_offsets_for_slice, list_values, list_values_row_number, take_function_args},
};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::{
    coerce_single_list_arg, list_element_field, require_boolean_lambda, value_lambda_pair,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkForAll {
    signature: HigherOrderSignature,
}

impl Default for SparkForAll {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkForAll {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

fn all_match_for_range(predicate: &BooleanArray, start: usize, end: usize) -> Option<bool> {
    let any_false = (start..end).any(|j| predicate.is_valid(j) && !predicate.value(j));
    if any_false {
        return Some(false);
    }
    let any_null = (start..end).any(|j| predicate.is_null(j));
    if any_null { None } else { Some(true) }
}

impl HigherOrderUDFImpl for SparkForAll {
    fn name(&self) -> &'static str {
        "forall"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_single_list_arg(self.name(), arg_types)
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (list, _lambda) = value_lambda_pair(self.name(), fields)?;
        let field = list_element_field(self.name(), list)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![field]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let (list, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        require_boolean_lambda(self.name(), lambda)?;
        let _ = list;
        Ok(Arc::new(Field::new("", DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)] =
            take_function_args(self.name(), &args.args)?
        else {
            return exec_err!("{} expects a value followed by a lambda", self.name());
        };
        let list_array = list.to_array(args.number_rows)?;
        if list_array.null_count() == list_array.len() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                list_array.len(),
            )));
        }
        let list_values = list_values(&list_array)?;
        let values_param = || Ok(Arc::clone(&list_values));
        let predicate_results = lambda
            .evaluate(&[&values_param], |arrays| {
                let indices = list_values_row_number(&list_array)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(list_values.len())?;
        let predicate_bool = predicate_results
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                exec_datafusion_err!("{} predicate must return boolean array", self.name())
            })?;
        let mut values = BooleanBuilder::with_capacity(list_array.len());
        match list_array.data_type() {
            DataType::List(_) => {
                let list_typed = list_array.as_list::<i32>();
                let offsets = adjust_offsets_for_slice(list_typed);
                for i in 0..list_typed.len() {
                    let start = offsets[i].as_usize();
                    let end = offsets[i + 1].as_usize();
                    values.append_option(all_match_for_range(predicate_bool, start, end));
                }
            }
            DataType::LargeList(_) => {
                let list_typed = list_array.as_list::<i64>();
                let offsets = adjust_offsets_for_slice(list_typed);
                for i in 0..list_typed.len() {
                    let start = offsets[i].as_usize();
                    let end = offsets[i + 1].as_usize();
                    values.append_option(all_match_for_range(predicate_bool, start, end));
                }
            }
            other => return exec_err!("expected list, got {other}"),
        }
        let (boolean_buffer, predicate_nulls) = values.finish().into_parts();
        let nulls = NullBuffer::union(list_array.nulls(), predicate_nulls.as_ref());
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            boolean_buffer,
            nulls,
        ))))
    }
}

pub fn forall_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkForAll::new())));
    Arc::clone(&INSTANCE)
}
