use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, MutableArrayData, StringArray, make_array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

#[must_use]
pub fn reverse_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkReverse::new()))
}

#[derive(Debug)]
struct SparkReverse {
    signature: Signature,
}

impl SparkReverse {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkReverse {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkReverse {}

impl Hash for SparkReverse {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn reverse_return(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            Ok(arg_type.clone())
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
            Ok(DataType::Utf8)
        }
        other => Err(DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"reverse(<expr>)\" due \
             to data type mismatch: The first parameter requires the \"STRING or ARRAY\" type, \
             however the argument has the type \"{}\".",
            crate::collection::spark_type_name(other)
        ))),
    }
}

impl ScalarUDFImpl for SparkReverse {
    crate::shim_udf_boilerplate!("reverse");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let first = arg_types
            .first()
            .ok_or_else(|| DataFusionError::Plan("'reverse' expects one argument".to_owned()))?;
        reverse_return(first)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let first = args
            .arg_fields
            .first()
            .ok_or_else(|| DataFusionError::Plan("'reverse' expects one argument".to_owned()))?;
        Ok(Arc::new(Field::new(
            "reverse",
            reverse_return(first.data_type())?,
            first.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let first = arg_types
            .first()
            .ok_or_else(|| DataFusionError::Plan("'reverse' expects one argument".to_owned()))?;
        match first {
            DataType::List(_) => Ok(vec![first.clone()]),
            DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
                Ok(vec![DataType::List(Arc::clone(field))])
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(vec![DataType::Utf8])
            }
            _ => reverse_return(first).map(|_| vec![first.clone()]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            return_field,
            ..
        } = args;
        let [input] = arg_values.as_slice() else {
            return exec_err!("'reverse' expects one argument");
        };
        match return_field.data_type() {
            DataType::List(_) => reverse_list(input, return_field),
            _ => reverse_string(input),
        }
    }
}

fn reverse_string(input: &ColumnarValue) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(input))?;
    let array = &arrays[0];
    let shaped: ArrayRef = if array.data_type() == &DataType::Utf8 {
        Arc::clone(array)
    } else {
        cast(array.as_ref(), &DataType::Utf8)?
    };
    let strings = shaped.as_string::<i32>();
    let mut reversed: Vec<Option<String>> = Vec::with_capacity(strings.len());
    for row in 0..strings.len() {
        if strings.is_null(row) {
            reversed.push(None);
        } else {
            reversed.push(Some(strings.value(row).chars().rev().collect()));
        }
    }
    Ok(ColumnarValue::Array(Arc::new(StringArray::from(reversed))))
}

fn reverse_list(input: &ColumnarValue, return_field: FieldRef) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(input))?;
    let array = &arrays[0];
    let DataType::List(field) = return_field.data_type() else {
        return exec_err!("reverse list arm needs a list return");
    };
    let target = DataType::List(Arc::clone(field));
    let shaped: ArrayRef = if array.data_type() == &target {
        Arc::clone(array)
    } else {
        cast(array.as_ref(), &target)?
    };
    let lists = shaped.as_list::<i32>();
    let row_count = lists.len();
    let child = lists.values().to_data();
    let mut mutable = MutableArrayData::new(vec![&child], false, row_count);
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    for row in 0..row_count {
        if lists.is_null(row) {
            validity.push(false);
            any_null = true;
            offsets.push(into_i32(mutable.len())?);
            continue;
        }
        validity.push(true);
        let bounds = lists.value_offsets();
        let start = usize::try_from(bounds[row]).map_err(|_| {
            DataFusionError::Execution("reverse list offset does not fit usize".to_owned())
        })?;
        let end = usize::try_from(bounds[row + 1]).map_err(|_| {
            DataFusionError::Execution("reverse list offset does not fit usize".to_owned())
        })?;
        for slot in (start..end).rev() {
            mutable.extend(0, slot, slot + 1);
        }
        offsets.push(into_i32(mutable.len())?);
    }
    let values = make_array(mutable.freeze());
    let nulls = if any_null {
        Some(NullBuffer::from(validity))
    } else {
        None
    };
    Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    )?)))
}

fn into_i32(value: usize) -> Result<i32> {
    i32::try_from(value)
        .map_err(|_| DataFusionError::Execution("reverse row count does not fit i32".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> ScalarValue {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)
            .unwrap_or_else(|error| panic!("scalar {sql}: {error}"))
    }

    #[tokio::test]
    async fn reverse_array_reverses_elements_and_keeps_type() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT reverse(array(1, 2, 3))")
            .await
            .expect("plan reverse")
            .collect()
            .await
            .expect("execute reverse");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Int32, true)))
        );
        let scalar = one(&ctx, "SELECT reverse(array(1, 2, 3))").await;
        let ScalarValue::List(values) = &scalar else {
            panic!("expected list, got {scalar:?}");
        };
        let inner = values
            .value(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("int values")
            .clone();
        assert_eq!(inner.values(), &[3, 2, 1]);
    }

    #[tokio::test]
    async fn reverse_string_and_null_shapes() {
        let ctx = ctx();
        assert_eq!(
            one(&ctx, "SELECT reverse('abc')").await,
            ScalarValue::Utf8(Some("cba".to_owned()))
        );
        assert_eq!(
            one(&ctx, "SELECT reverse(NULL)").await,
            ScalarValue::Utf8(None)
        );
        let nulled = one(&ctx, "SELECT reverse(CAST(NULL AS ARRAY<INT>))").await;
        let ScalarValue::List(values) = &nulled else {
            panic!("expected list, got {nulled:?}");
        };
        assert!(values.is_null(0), "NULL array reverses to NULL");
    }

    #[tokio::test]
    async fn reverse_non_string_non_array_refuses() {
        let ctx = ctx();
        let error = ctx
            .sql("SELECT reverse(1)")
            .await
            .err()
            .unwrap_or_else(|| panic!("should refuse"))
            .to_string();
        assert!(
            error.contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
            "{error}"
        );
    }
}
