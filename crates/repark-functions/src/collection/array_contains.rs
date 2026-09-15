use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn array_contains_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayContains::new()).with_aliases(["array_has"]))
}

#[derive(Debug)]
struct SparkArrayContains {
    signature: Signature,
    inner: Arc<ScalarUDF>,
}

impl SparkArrayContains {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            inner: Arc::new(ScalarUDF::from(
                datafusion_spark::function::array::array_contains::SparkArrayContains::new(),
            )),
        }
    }
}

impl PartialEq for SparkArrayContains {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArrayContains {}

impl Hash for SparkArrayContains {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkArrayContains {
    crate::shim_udf_boilerplate!("array_contains");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let Some(needle) = args.arg_fields.get(1) else {
            return Err(DataFusionError::Plan(
                "'array_contains' expects two arguments".to_string(),
            ));
        };
        if needle.data_type() == &DataType::Null {
            return Err(null_type_error());
        }
        let haystack = &args.arg_fields[0];
        let nullable = haystack.is_nullable()
            || needle.is_nullable()
            || contains_null_element(haystack.data_type());
        Ok(Arc::new(Field::new(
            "array_contains",
            DataType::Boolean,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [haystack, needle] if needle == &DataType::Null => Err(null_type_error()),
            [haystack, _] if haystack == &DataType::Null => Ok(vec![
                DataType::List(Arc::new(Field::new_list_field(DataType::Null, true))),
                DataType::Null,
            ]),
            [haystack, _] => {
                let element = list_element_type(haystack)
                    .ok_or_else(|| unexpected_input_type("array_contains", haystack))?;
                Ok(vec![haystack.clone(), element.clone()])
            }
            _ => exec_err!(
                "'array_contains' expects two arguments, got {}",
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args
            .arg_fields
            .get(1)
            .is_some_and(|field| field.data_type() == &DataType::Null)
        {
            return Err(null_type_error());
        }
        self.inner.invoke_with_args(args)
    }
}

fn list_element_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type())
        }
        _ => None,
    }
}

fn unexpected_input_type(name: &str, got: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}(<expr>, ...)\" due \
         to data type mismatch: The first parameter requires the \"ARRAY\" type, however the \
         argument has the type \"{got}\"."
    ))
}

fn contains_null_element(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            field.is_nullable()
        }
        _ => false,
    }
}

fn null_type_error() -> DataFusionError {
    DataFusionError::Plan(
        "[DATATYPE_MISMATCH.NULL_TYPE] Cannot resolve \"array_contains(<expr>, NULL)\" due \
         to data type mismatch: Null typed values cannot be used as arguments of \
         `array_contains`."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> ScalarValue {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)
            .unwrap_or_else(|error| panic!("scalar {sql}: {error}"))
    }

    #[tokio::test]
    async fn three_valued_null_semantics() {
        let ctx = ctx();
        assert_eq!(
            one(&ctx, "SELECT array_contains(array(1, NULL), 2)").await,
            ScalarValue::Boolean(None)
        );
        assert_eq!(
            one(&ctx, "SELECT array_contains(array(1, NULL), 1)").await,
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            one(&ctx, "SELECT array_contains(array(1, 2), 3)").await,
            ScalarValue::Boolean(Some(false))
        );
        assert_eq!(
            one(&ctx, "SELECT array_contains(CAST(NULL AS ARRAY<INT>), 1)").await,
            ScalarValue::Boolean(None)
        );
    }

    #[tokio::test]
    async fn null_typed_needle_refuses_with_spark_class() {
        let ctx = ctx();
        let error = match ctx.sql("SELECT array_contains(array(1, 2), NULL)").await {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .err()
                .unwrap_or_else(|| panic!("should refuse"))
                .to_string(),
        };
        assert!(error.contains("DATATYPE_MISMATCH.NULL_TYPE"), "{error}");
    }
}
