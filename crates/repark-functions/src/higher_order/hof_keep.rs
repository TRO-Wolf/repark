use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::logical_expr::expr::LambdaVariable;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn hof_keep_udf() -> Arc<ScalarUDF> {
    static INSTANCE: LazyLock<Arc<ScalarUDF>> =
        LazyLock::new(|| Arc::new(ScalarUDF::from(HofKeep::new())));
    Arc::clone(&INSTANCE)
}

#[must_use]
pub fn keep_call(body: Expr, params: &[String]) -> Expr {
    let mut args = Vec::with_capacity(1 + params.len());
    args.push(body);
    args.extend(
        params
            .iter()
            .map(|name| Expr::LambdaVariable(LambdaVariable::new(name.clone(), None))),
    );
    hof_keep_udf().call(args)
}

#[derive(Debug)]
struct HofKeep {
    signature: Signature,
}

impl HofKeep {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for HofKeep {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for HofKeep {}

impl Hash for HofKeep {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for HofKeep {
    crate::shim_udf_boilerplate!("__hof_keep");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [first, ..] = arg_types else {
            return plan_err!("'__hof_keep' requires at least 1 argument, got 0");
        };
        Ok(first.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [first, ..] = args.arg_fields else {
            return plan_err!("'__hof_keep' requires at least 1 argument, got 0");
        };
        Ok(Arc::clone(first))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [first, ..] = args.args.as_slice() else {
            return exec_err!("'__hof_keep' requires at least 1 argument, got 0");
        };
        Ok(ColumnarValue::Array(first.to_array(args.number_rows)?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::ColumnarValue;

    use super::hof_keep_udf;

    #[test]
    fn keep_returns_the_first_field_verbatim() {
        let udf = hof_keep_udf();
        let body = Arc::new(Field::new("body", DataType::Int32, false));
        let params = Arc::new(Field::new("x", DataType::Int32, true));
        let field = udf
            .return_field_from_args(datafusion::logical_expr::ReturnFieldArgs {
                arg_fields: &[body, params],
                scalar_arguments: &[None, None],
            })
            .expect("two fields resolve");
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(!field.is_nullable());
    }

    #[test]
    fn keep_evaluates_to_its_first_argument() {
        let udf = hof_keep_udf();
        let values = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1), None])));
        let out = udf
            .invoke_with_args(datafusion::logical_expr::ScalarFunctionArgs {
                args: vec![values],
                arg_fields: vec![Arc::new(Field::new("body", DataType::Int32, true))],
                number_rows: 2,
                return_field: Arc::new(Field::new("body", DataType::Int32, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .expect("one argument evaluates");
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        let ints = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 survives");
        assert_eq!(ints.len(), 2);
        assert!(ints.is_null(1));
    }
}
