use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn size_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSize::new("size")))
}

#[must_use]
pub fn cardinality_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSize::new("cardinality")))
}

#[derive(Debug)]
struct SparkSize {
    name: &'static str,
    signature: Signature,
}

impl SparkSize {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSize {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SparkSize {}

impl Hash for SparkSize {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for SparkSize {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name, DataType::Int32, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if is_collection(data_type) => Ok(vec![data_type.clone()]),
            [data_type] => Err(unexpected_input_type(self.name, data_type)),
            _ => exec_err!(
                "'{}' expects one argument, got {}",
                self.name,
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(arg) = args.args.first() else {
            return exec_err!("'{}' expects one argument", self.name);
        };
        let array = arg.to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(size_inner(&array)?))
    }
}

fn is_collection(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

fn unexpected_input_type(name: &str, got: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}(<expr>)\" due to \
         data type mismatch: The first parameter requires the \"ARRAY\" or \"MAP\" type, \
         however the argument has the type \"{got}\"."
    ))
}

fn size_inner(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) | DataType::FixedSizeList(_, _) => Ok(
            datafusion::arrow::compute::kernels::length::length(array.as_ref())?,
        ),
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let values: Vec<i32> = list
                .offsets()
                .windows(2)
                .map(|pair| i32::try_from(pair[1] - pair[0]).unwrap_or(i32::MAX))
                .collect();
            Ok(Arc::new(Int32Array::new(
                values.into(),
                list.logical_nulls(),
            )))
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let values: Vec<i32> = map
                .offsets()
                .windows(2)
                .map(|pair| pair[1] - pair[0])
                .collect();
            Ok(Arc::new(Int32Array::new(
                values.into(),
                map.logical_nulls(),
            )))
        }
        other => exec_err!("'size'/'cardinality' on unsupported type {other}"),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    async fn field_and_value(ctx: &SessionContext, sql: &str) -> (DataType, bool, ScalarValue) {
        let frame = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"));
        let field = frame.schema().field(0).clone();
        let batches = frame
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        let value = ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)
            .unwrap_or_else(|error| panic!("scalar {sql}: {error}"));
        (field.data_type().clone(), field.is_nullable(), value)
    }

    #[tokio::test]
    async fn size_null_array_is_nullable_int_null() {
        let ctx = ctx();
        let (data_type, nullable, value) =
            field_and_value(&ctx, "SELECT size(CAST(NULL AS ARRAY<INT>))").await;
        assert_eq!(data_type, DataType::Int32);
        assert!(nullable);
        assert_eq!(value, ScalarValue::Int32(None));
        let (data_type, nullable, value) = field_and_value(&ctx, "SELECT size(array(1, 2))").await;
        assert_eq!(data_type, DataType::Int32);
        assert!(!nullable);
        assert_eq!(value, ScalarValue::Int32(Some(2)));
        let (_, _, value) = field_and_value(&ctx, "SELECT size(map(1, 2))").await;
        assert_eq!(value, ScalarValue::Int32(Some(1)));
    }

    #[tokio::test]
    async fn cardinality_answers_int() {
        let ctx = ctx();
        let (data_type, nullable, value) =
            field_and_value(&ctx, "SELECT cardinality(CAST(NULL AS ARRAY<INT>))").await;
        assert_eq!(data_type, DataType::Int32);
        assert!(nullable);
        assert_eq!(value, ScalarValue::Int32(None));
    }
}
