//! Spark `filter` — unary or `(element, index)` predicate over an array.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{
    Result, ScalarValue, exec_err,
    utils::{adjust_offsets_for_slice, list_values_row_number},
};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::{
    ListValuesResult, coerce_single_list_arg, element_and_index_parameters, empty_filtered_list,
    extract_list_values, filter_list_values, list_field_from_return, require_boolean_lambda,
    value_lambda_pair,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFilter {
    signature: HigherOrderSignature,
}

impl Default for SparkFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFilter {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkFilter {
    fn name(&self) -> &'static str {
        "filter"
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
        element_and_index_parameters(self.name(), fields)
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let (list, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        require_boolean_lambda(self.name(), lambda)?;
        Ok(Arc::new(Field::new(
            "",
            list.data_type().clone(),
            list.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;
        let list_values = match extract_list_values(&list_array, args.return_type())? {
            ListValuesResult::EarlyReturn(value) => return Ok(value),
            ListValuesResult::Values(values) => values,
        };
        let field = list_field_from_return(self.name(), args.return_field.as_ref())?;
        let predicate_output = super::lambda_utils::evaluate_element_and_index(
            lambda,
            &list_values,
            list_array.as_ref(),
            |arrays| {
                let indices = list_values_row_number(&list_array)?;
                Ok(take_arrays(arrays, &indices, None)?)
            },
        )?;
        if let ColumnarValue::Scalar(ScalarValue::Boolean(flag)) = &predicate_output {
            return match flag {
                Some(true) => Ok(ColumnarValue::Array(list_array)),
                _ => Ok(ColumnarValue::Array(empty_filtered_list(
                    &list_array,
                    field,
                )?)),
            };
        }
        let predicate = predicate_output.into_array(list_values.len())?;
        let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
            return exec_err!(
                "{} lambda must return boolean, got {}",
                self.name(),
                predicate.data_type()
            );
        };
        let filtered_list = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                let adjusted_offsets = adjust_offsets_for_slice(list);
                let (filtered_values, new_offsets) =
                    filter_list_values(&list_values, predicate, &adjusted_offsets)?;
                Arc::new(datafusion::arrow::array::ListArray::new(
                    field,
                    new_offsets,
                    filtered_values,
                    list.nulls().cloned(),
                )) as datafusion::arrow::array::ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list::<i64>();
                let adjusted_offsets = adjust_offsets_for_slice(large_list);
                let (filtered_values, new_offsets) =
                    filter_list_values(&list_values, predicate, &adjusted_offsets)?;
                Arc::new(datafusion::arrow::array::LargeListArray::new(
                    field,
                    new_offsets,
                    filtered_values,
                    large_list.nulls().cloned(),
                ))
            }
            other => exec_err!("expected list, got {other}")?,
        };
        Ok(ColumnarValue::Array(filtered_list))
    }
}

pub fn filter_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkFilter::new())));
    Arc::clone(&INSTANCE)
}
