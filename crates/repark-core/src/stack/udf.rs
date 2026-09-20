use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use repark_common::spark_error;

#[must_use]
pub fn stack_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StackUdf::new()))
}

#[derive(Debug)]
struct StackUdf {
    signature: Signature,
}

impl StackUdf {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for StackUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for StackUdf {}

impl Hash for StackUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for StackUdf {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "stack"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("stack must be rewritten to UnpivotExec")
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            let count = arg_types.len().to_string();
            let rendered = spark_error::message(
                spark_error::WRONG_NUM_ARGS_WITHOUT_SUGGESTION,
                &[("actualNumber", count.as_str())],
            );
            return plan_err!("{rendered}");
        }
        match &arg_types[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(arg_types.to_vec()),
            other => {
                let actual = other.to_string();
                let rendered = spark_error::message(
                    spark_error::DATATYPE_MISMATCH_UNEXPECTED_INPUT_TYPE,
                    &[("actualType", actual.as_str())],
                );
                plan_err!("{rendered}")
            }
        }
    }
}
