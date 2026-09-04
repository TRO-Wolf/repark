use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn flatten_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFlatten::new()))
}

#[derive(Debug)]
struct SparkFlatten {
    signature: Signature,
}

impl SparkFlatten {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkFlatten {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFlatten {}

impl Hash for SparkFlatten {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn inner_element_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::List(field) => match field.data_type() {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => Ok(inner.data_type().clone()),
            other => exec_err!("'flatten' expects ARRAY<ARRAY<T>>, got ARRAY<{other}>"),
        },
        DataType::Null => Ok(DataType::Null),
        other => exec_err!("'flatten' expects ARRAY<ARRAY<T>>, got {other}"),
    }
}

fn offset_as_usize(offset: i32) -> Result<usize> {
    if let Ok(value) = usize::try_from(offset) {
        Ok(value)
    } else {
        exec_err!("'flatten' offset does not fit usize")
    }
}

fn flatten_list(list: &ListArray, element_type: &DataType) -> Result<ArrayRef> {
    let inner_list = list.values().as_list::<i32>();
    let outer_offsets = list.value_offsets();
    let inner_offsets = inner_list.value_offsets();
    let row_count = list.len();
    let mut mapped: Vec<i32> = Vec::with_capacity(row_count + 1);
    for outer in outer_offsets.iter().take(row_count + 1) {
        let index = offset_as_usize(*outer)?;
        mapped.push(inner_offsets[index]);
    }
    let mut valid: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    for row in 0..row_count {
        if list.is_null(row) {
            valid.push(false);
            any_null = true;
            continue;
        }
        let start = offset_as_usize(outer_offsets[row])?;
        let end = offset_as_usize(outer_offsets[row + 1])?;
        let row_valid = !(start..end).any(|slot| inner_list.is_null(slot));
        if !row_valid {
            any_null = true;
        }
        valid.push(row_valid);
    }
    let nulls = if any_null {
        Some(NullBuffer::from(valid))
    } else {
        None
    };
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", element_type.clone(), true)),
        OffsetBuffer::new(mapped.into()),
        Arc::clone(inner_list.values()),
        nulls,
    )?))
}

impl ScalarUDFImpl for SparkFlatten {
    crate::shim_udf_boilerplate!("flatten");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element = inner_element_type(&arg_types[0])?;
        Ok(DataType::List(Arc::new(Field::new("item", element, true))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let data_type = match args.arg_fields.first() {
            Some(field) => self.return_type(&[field.data_type().clone()])?,
            None => return exec_err!("'flatten' requires 1 argument"),
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if matches!(data_type, DataType::List(_) | DataType::Null) => {
                Ok(vec![data_type.clone()])
            }
            [data_type] => exec_err!("'flatten' expects ARRAY<ARRAY<T>>, got {data_type}"),
            _ => exec_err!("'flatten' requires 1 argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [array] = arrays.as_slice() else {
            return exec_err!("'flatten' requires 1 argument, got {}", arrays.len());
        };
        if matches!(array.data_type(), DataType::Null) || array.is_empty() {
            return Ok(ColumnarValue::Array(Arc::clone(array)));
        }
        let list = array.as_list::<i32>();
        let element_type = inner_element_type(array.data_type())?;
        Ok(ColumnarValue::Array(flatten_list(list, &element_type)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::common::config::ConfigOptions;
    use std::time::Instant;

    fn invoke(udf: &ScalarUDF, array: ArrayRef, rows: usize) -> ArrayRef {
        let field = Arc::new(Field::new("a", array.data_type().clone(), true));
        let return_field = Arc::new(Field::new(
            "r",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        match udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields: vec![field],
                number_rows: rows,
                return_field,
                config_options: Arc::new(ConfigOptions::default()),
            })
            .expect("flatten invoke")
        {
            ColumnarValue::Array(out) => out,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        }
    }

    fn packed_rows(rows: usize) -> ListArray {
        let mut values = Vec::with_capacity(rows.saturating_mul(3));
        let mut inner_offsets = Vec::with_capacity(rows.saturating_mul(2).saturating_add(1));
        inner_offsets.push(0i32);
        let mut inner_len = 0i32;
        for _ in 0..rows {
            values.extend_from_slice(&[1, 2, 3]);
            inner_len += 2;
            inner_offsets.push(inner_len);
            inner_len += 1;
            inner_offsets.push(inner_len);
        }
        let mut outer_offsets = Vec::with_capacity(rows.saturating_add(1));
        outer_offsets.push(0i32);
        for row in 1..=rows {
            let offset = i32::try_from(row.saturating_mul(2)).expect("outer offset fits i32");
            outer_offsets.push(offset);
        }
        let inner = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(inner_offsets.into()),
            Arc::new(Int32Array::from(values)),
            None,
        );
        ListArray::new(
            Arc::new(Field::new("item", inner.data_type().clone(), true)),
            OffsetBuffer::new(outer_offsets.into()),
            Arc::new(inner),
            None,
        )
    }

    #[test]
    fn null_sub_array_row_is_null() {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let inner = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 2, 2, 3].into()),
            values,
            Some(vec![true, false, true].into()),
        );
        let outer = ListArray::new(
            Arc::new(Field::new("item", inner.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 3].into()),
            Arc::new(inner),
            None,
        );
        let out = invoke(flatten_udf().as_ref(), Arc::new(outer), 2);
        let list = out.as_list::<i32>();
        assert!(!list.is_null(0));
        assert_eq!(list.value(0).len(), 2);
        assert!(list.is_null(1));
    }

    #[test]
    #[ignore]
    fn one_million_rows_within_three_times_datafusion() {
        let rows = 1_000_000_usize;
        let fixture = packed_rows(rows);
        let array: ArrayRef = Arc::new(fixture);
        let ours = flatten_udf();
        let baseline = datafusion::functions_nested::flatten::flatten_udf();
        let _ = invoke(ours.as_ref(), Arc::clone(&array), rows);
        let _ = invoke(baseline.as_ref(), Arc::clone(&array), rows);
        let start = Instant::now();
        let ours_out = invoke(ours.as_ref(), Arc::clone(&array), rows);
        let ours_elapsed = start.elapsed();
        let start = Instant::now();
        let baseline_out = invoke(baseline.as_ref(), Arc::clone(&array), rows);
        let baseline_elapsed = start.elapsed();
        assert_eq!(ours_out.len(), baseline_out.len());
        assert!(
            ours_elapsed <= baseline_elapsed.saturating_mul(3),
            "repark {:?} datafusion {:?}",
            ours_elapsed,
            baseline_elapsed
        );
        eprintln!("flatten bench repark={ours_elapsed:?} datafusion={baseline_elapsed:?}");
    }
}
