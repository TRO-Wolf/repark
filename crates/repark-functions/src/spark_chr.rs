use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

#[must_use]
pub fn chr_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkChr::chr()))
}

#[must_use]
pub fn char_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkChr::char_name()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![chr_udf(), char_udf()]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[derive(Debug)]
struct SparkChr {
    name: &'static str,
    signature: Signature,
}

impl SparkChr {
    fn chr() -> Self {
        Self {
            name: "chr",
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn char_name() -> Self {
        Self {
            name: "char",
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkChr {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SparkChr {}

impl Hash for SparkChr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

fn is_numeric_argument(data_type: &DataType) -> bool {
    *data_type == DataType::Null || data_type.is_numeric()
}

fn chr_spark(code: i64) -> String {
    if code < 0 {
        String::new()
    } else {
        let byte = u8::try_from(code % 256).unwrap_or(0);
        String::from(char::from(byte))
    }
}

impl ScalarUDFImpl for SparkChr {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if is_numeric_argument(data_type) => Ok(vec![DataType::Int64]),
            [data_type] => {
                exec_err!(
                    "'{}' argument 1 must be numeric, got {data_type}",
                    self.name
                )
            }
            _ => exec_err!(
                "'{}' requires 1 argument, got {}",
                self.name,
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [value] = arrays.as_slice() else {
            return exec_err!("'{}' requires 1 argument, got {}", self.name, arrays.len());
        };
        let casted = cast(value.as_ref(), &DataType::Int64)?;
        let values = casted.as_primitive::<Int64Type>();
        let mut builder = StringBuilder::with_capacity(values.len(), values.len());
        for row in 0..values.len() {
            if values.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(chr_spark(values.value(row)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::chr_spark;

    #[test]
    fn modulo_256_and_negative_empty() {
        assert_eq!(chr_spark(0), "\u{0}");
        assert_eq!(chr_spark(65), "A");
        assert_eq!(chr_spark(255), "ÿ");
        assert_eq!(chr_spark(256), "\u{0}");
        assert_eq!(chr_spark(300), ",");
        assert_eq!(chr_spark(321), "A");
        assert_eq!(chr_spark(65601), "A");
        assert_eq!(chr_spark(-1), "");
        assert_eq!(chr_spark(-256), "");
        assert_eq!(chr_spark(-300), "");
    }
}
