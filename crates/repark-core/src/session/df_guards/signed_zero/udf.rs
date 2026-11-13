use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, Float32Type, Float64Type};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use repark_common::float_canon::{
    canonical_f32, canonical_f64, is_canonical_f32, is_canonical_f64,
};
use repark_common::udf_names::NORMALIZE_ZERO_NAME;

static NORMALIZE_ZERO: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(NormalizeZero::new())));

#[must_use]
pub(crate) fn normalize_zero_udf() -> Arc<ScalarUDF> {
    Arc::clone(&NORMALIZE_ZERO)
}

#[must_use]
pub(crate) fn normalize_zero_call(arg: Expr) -> Expr {
    normalize_zero_udf().call(vec![arg])
}

#[must_use]
pub(crate) fn is_normalize_zero_call(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call) if call.name() == NORMALIZE_ZERO_NAME)
}

#[derive(Debug)]
struct NormalizeZero {
    signature: Signature,
}

impl NormalizeZero {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for NormalizeZero {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for NormalizeZero {}

impl Hash for NormalizeZero {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for NormalizeZero {
    fn name(&self) -> &str {
        NORMALIZE_ZERO_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [data_type @ (DataType::Float32 | DataType::Float64)] => Ok(data_type.clone()),
            other => plan_err!("{NORMALIZE_ZERO_NAME} takes one FLOAT or DOUBLE, got {other:?}"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("{NORMALIZE_ZERO_NAME} takes one argument");
        };
        self.return_type(&[field.data_type().clone()])?;
        Ok(Arc::new(field.as_ref().clone().with_name(self.name())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut values = args.args;
        let (Some(value), true) = (values.pop(), values.is_empty()) else {
            return exec_err!("{NORMALIZE_ZERO_NAME} takes one argument");
        };
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(normalize_array(array)?)),
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(normalize_scalar(scalar)?)),
        }
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        match <[Expr; 1]>::try_from(args) {
            Ok([inner]) if is_normalize_zero_call(&inner) => {
                Ok(ExprSimplifyResult::Simplified(inner))
            }
            Ok([inner]) => Ok(ExprSimplifyResult::Original(vec![inner])),
            Err(args) => Ok(ExprSimplifyResult::Original(args)),
        }
    }
}

pub(crate) fn normalize_array(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Float64 => {
            let values = array.as_primitive::<Float64Type>();
            if values.values().iter().all(|value| is_canonical_f64(*value)) {
                return Ok(array);
            }
            Ok(Arc::new(values.unary::<_, Float64Type>(canonical_f64)))
        }
        DataType::Float32 => {
            let values = array.as_primitive::<Float32Type>();
            if values.values().iter().all(|value| is_canonical_f32(*value)) {
                return Ok(array);
            }
            Ok(Arc::new(values.unary::<_, Float32Type>(canonical_f32)))
        }
        other => exec_err!("{NORMALIZE_ZERO_NAME} takes FLOAT or DOUBLE, got {other}"),
    }
}

fn normalize_scalar(scalar: ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Float64(value) => Ok(ScalarValue::Float64(value.map(canonical_f64))),
        ScalarValue::Float32(value) => Ok(ScalarValue::Float32(value.map(canonical_f32))),
        other => exec_err!(
            "{NORMALIZE_ZERO_NAME} takes FLOAT or DOUBLE, got {}",
            other.data_type()
        ),
    }
}

#[must_use]
pub(crate) fn canonical_literal(scalar: &ScalarValue) -> Option<ScalarValue> {
    match scalar {
        ScalarValue::Float64(Some(value)) if !is_canonical_f64(*value) => {
            Some(ScalarValue::Float64(Some(canonical_f64(*value))))
        }
        ScalarValue::Float32(Some(value)) if !is_canonical_f32(*value) => {
            Some(ScalarValue::Float32(Some(canonical_f32(*value))))
        }
        _ => None,
    }
}

#[must_use]
pub(crate) fn is_float(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}
