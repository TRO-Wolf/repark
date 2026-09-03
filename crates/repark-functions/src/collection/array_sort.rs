use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn array_sort_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArraySort::new()))
}

#[must_use]
pub fn sort_array_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSortArray::new()))
}

fn inner_sort() -> Arc<ScalarUDF> {
    datafusion::functions_nested::sort::array_sort_udf()
}

fn with_order_and_nulls(
    mut args: ScalarFunctionArgs,
    order: &str,
    nulls: &str,
) -> Result<ScalarFunctionArgs> {
    let Some(array) = args.args.first().cloned() else {
        return exec_err!("array_sort missing array argument");
    };
    let Some(array_field) = args.arg_fields.first().cloned() else {
        return exec_err!("array_sort missing array field");
    };
    args.args = vec![
        array,
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(order.to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(nulls.to_string()))),
    ];
    args.arg_fields = vec![
        array_field,
        Arc::new(Field::new("order", DataType::Utf8, false)),
        Arc::new(Field::new("nulls", DataType::Utf8, false)),
    ];
    Ok(args)
}

#[derive(Debug)]
struct SparkArraySort {
    signature: Signature,
}

impl SparkArraySort {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArraySort {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArraySort {}

impl Hash for SparkArraySort {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkArraySort {
    crate::shim_udf_boilerplate!("array_sort");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        inner_sort().inner().return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        inner_sort().inner().return_field_from_args(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [array] if matches!(array, DataType::List(_) | DataType::Null) => {
                Ok(vec![array.clone()])
            }
            [array, order] if matches!(array, DataType::List(_) | DataType::Null) => {
                Ok(vec![array.clone(), order.clone()])
            }
            other => exec_err!("'array_sort' expects an array, got {other:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let order = match args.args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))))
                if text.eq_ignore_ascii_case("DESC") =>
            {
                "DESC"
            }
            Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))) => "DESC",
            _ => "ASC",
        };
        let forwarded = with_order_and_nulls(args, order, "NULLS LAST")?;
        inner_sort().invoke_with_args(forwarded)
    }
}

#[derive(Debug)]
struct SparkSortArray {
    signature: Signature,
}

impl SparkSortArray {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSortArray {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSortArray {}

impl Hash for SparkSortArray {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkSortArray {
    crate::shim_udf_boilerplate!("sort_array");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        inner_sort().inner().return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        inner_sort().inner().return_field_from_args(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [array] if matches!(array, DataType::List(_) | DataType::Null) => {
                Ok(vec![array.clone()])
            }
            [array, flag] if matches!(array, DataType::List(_) | DataType::Null) => {
                Ok(vec![array.clone(), flag.clone()])
            }
            other => exec_err!("'sort_array' expects an array, got {other:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let descending = match args.args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))) => true,
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(text)))) => {
                text.eq_ignore_ascii_case("DESC")
            }
            _ => false,
        };
        let (order, nulls) = if descending {
            ("DESC", "NULLS LAST")
        } else {
            ("ASC", "NULLS FIRST")
        };
        let forwarded = with_order_and_nulls(args, order, nulls)?;
        inner_sort().invoke_with_args(forwarded)
    }
}
