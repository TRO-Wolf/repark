use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        spark_array_constructor_udf("array", &[], datafusion_spark::function::array::array()),
        make_array_udf(),
    ]
}

#[must_use]
pub fn make_array_udf() -> Arc<ScalarUDF> {
    spark_array_constructor_udf(
        "make_array",
        &["make_list"],
        datafusion::functions_nested::make_array::make_array_udf(),
    )
}

fn spark_array_constructor_udf(
    name: &'static str,
    aliases: &[&'static str],
    inner: Arc<ScalarUDF>,
) -> Arc<ScalarUDF> {
    let udf = ScalarUDF::from(SparkArrayConstructor::new(name, inner));
    if aliases.is_empty() {
        Arc::new(udf)
    } else {
        Arc::new(udf.with_aliases(aliases.iter().copied()))
    }
}

#[derive(Debug)]
struct SparkArrayConstructor {
    name: &'static str,
    inner: Arc<ScalarUDF>,
    signature: Signature,
}

impl SparkArrayConstructor {
    fn new(name: &'static str, inner: Arc<ScalarUDF>) -> Self {
        Self {
            name,
            inner,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArrayConstructor {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SparkArrayConstructor {}

impl Hash for SparkArrayConstructor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for SparkArrayConstructor {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let contains_null = args.arg_fields.iter().any(|field| field.is_nullable());
        let declared = self.inner.return_field_from_args(args)?;
        let data_type = match declared.data_type() {
            DataType::List(element) => DataType::List(Arc::new(
                element.as_ref().clone().with_nullable(contains_null),
            )),
            DataType::LargeList(element) => DataType::LargeList(Arc::new(
                element.as_ref().clone().with_nullable(contains_null),
            )),
            DataType::FixedSizeList(element, size) => DataType::FixedSizeList(
                Arc::new(element.as_ref().clone().with_nullable(contains_null)),
                *size,
            ),
            other => other.clone(),
        };
        Ok(Arc::new(
            declared
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_nullable(false),
        ))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let data_type = args.return_field.data_type().clone();
        let value = self.inner.inner().invoke_with_args(args)?;
        Ok(crate::promise_retag::retag_to_promise(value, &data_type))
    }
}
