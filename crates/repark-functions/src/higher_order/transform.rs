//! Spark `transform` — unary or `(element, index)` over an array.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{
    Result, plan_err,
    utils::{list_values_row_number, take_function_args},
};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::{
    ListValuesResult, assemble_transformed_list, coerce_single_list_arg,
    element_and_index_parameters, extract_list_values, list_element_index_array,
    list_field_from_return,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTransform {
    signature: HigherOrderSignature,
}

impl Default for SparkTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTransform {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkTransform {
    fn name(&self) -> &'static str {
        "transform"
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
        let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)] =
            take_function_args(self.name(), args.arg_fields)?
        else {
            return plan_err!("{} expects a value followed by a lambda", self.name());
        };
        let field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));
        let return_type = match list.data_type() {
            DataType::List(_) => DataType::List(field),
            DataType::LargeList(_) => DataType::LargeList(field),
            other => plan_err!("expected list, got {other}")?,
        };
        Ok(Arc::new(Field::new("", return_type, list.is_nullable())))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [list, lambda] = take_function_args(self.name(), &args.args)?;
        let (ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)) = (list, lambda) else {
            return plan_err!("{} expects a value followed by a lambda", self.name());
        };
        let list_array = list.to_array(args.number_rows)?;
        let list_values = match extract_list_values(&list_array, args.return_type())? {
            ListValuesResult::EarlyReturn(value) => return Ok(value),
            ListValuesResult::Values(values) => values,
        };
        let index_array = list_element_index_array(list_array.as_ref())?;
        let transformed_values = super::lambda_utils::evaluate_element_and_index(
            lambda,
            &list_values,
            &index_array,
            |arrays| {
                let indices = list_values_row_number(&list_array)?;
                Ok(take_arrays(arrays, &indices, None)?)
            },
        )?
        .into_array(list_values.len())?;
        let field = list_field_from_return(self.name(), args.return_field.as_ref())?;
        let transformed_list = assemble_transformed_list(&list_array, transformed_values, field)?;
        Ok(ColumnarValue::Array(transformed_list))
    }
}

pub fn transform_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkTransform::new())));
    Arc::clone(&INSTANCE)
}
