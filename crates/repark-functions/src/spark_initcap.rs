use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

#[must_use]
pub fn initcap_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkInitCap::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![initcap_udf()]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[derive(Debug)]
struct SparkInitCap {
    signature: Signature,
}

impl SparkInitCap {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkInitCap {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkInitCap {}

impl Hash for SparkInitCap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_utf8_family(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => true,
        DataType::Dictionary(_, value_type) => is_utf8_family(value_type),
        _ => false,
    }
}

fn initcap_spark(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut at_word_start = true;
    for character in input.chars() {
        if character == ' ' {
            at_word_start = true;
            out.push(character);
        } else if at_word_start {
            for upper in character.to_uppercase() {
                out.push(upper);
            }
            at_word_start = false;
        } else {
            for lower in character.to_lowercase() {
                out.push(lower);
            }
        }
    }
    out
}

fn apply_initcap(array: &ArrayRef) -> ArrayRef {
    let strings = array.as_string::<i32>();
    let mut builder = StringBuilder::with_capacity(strings.len(), strings.len() * 8);
    for row in 0..strings.len() {
        if strings.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(initcap_spark(strings.value(row)));
        }
    }
    Arc::new(builder.finish())
}

impl ScalarUDFImpl for SparkInitCap {
    crate::shim_udf_boilerplate!("initcap");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if is_utf8_family(data_type) => Ok(vec![DataType::Utf8]),
            [data_type] => exec_err!("'initcap' argument 1 must be a string, got {data_type}"),
            _ => exec_err!("'initcap' requires 1 argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [value] = arrays.as_slice() else {
            return exec_err!("'initcap' requires 1 argument, got {}", arrays.len());
        };
        let casted = cast(value.as_ref(), &DataType::Utf8)?;
        Ok(ColumnarValue::Array(apply_initcap(&casted)))
    }
}

#[cfg(test)]
mod tests {
    use super::initcap_spark;

    #[test]
    fn space_only_word_break() {
        assert_eq!(initcap_spark("a-b"), "A-b");
        assert_eq!(initcap_spark("foo.bar"), "Foo.bar");
        assert_eq!(initcap_spark("o'neil"), "O'neil");
        assert_eq!(initcap_spark("ab_cd"), "Ab_cd");
        assert_eq!(initcap_spark("x\ty"), "X\ty");
        assert_eq!(initcap_spark("a-b c.d"), "A-b C.d");
        assert_eq!(initcap_spark("hello world"), "Hello World");
        assert_eq!(initcap_spark("  leading"), "  Leading");
        assert_eq!(initcap_spark(""), "");
        assert_eq!(initcap_spark("Ünï"), "Ünï");
        assert_eq!(initcap_spark("SPARK"), "Spark");
        assert_eq!(initcap_spark("a  b"), "A  B");
        assert_eq!(initcap_spark("a\nb"), "A\nb");
        assert_eq!(initcap_spark("1abc"), "1abc");
    }
}
