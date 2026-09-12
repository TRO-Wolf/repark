use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

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
            return plan_err!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `stack` requires > 1 parameters but \
                 the actual number is {}. SQLSTATE: 42605",
                arg_types.len()
            );
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
            other => plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve stack due to data \
                 type mismatch: The first parameter requires the \"INT\" type, however the \
                 argument has the type \"{other}\". SQLSTATE: 42K09"
            ),
        }
    }
}
