use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, ListArray, new_null_array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{concat, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn array_insert_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayInsert::new()))
}

#[derive(Debug)]
struct SparkArrayInsert {
    signature: Signature,
}

impl SparkArrayInsert {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArrayInsert {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArrayInsert {}

impl Hash for SparkArrayInsert {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn element_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => Ok(field.data_type().clone()),
        DataType::Null => Ok(DataType::Null),
        other => exec_err!("'array_insert' argument 1 must be an ARRAY, got {other}"),
    }
}

fn index_of_zero() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(
        "[INVALID_INDEX_OF_ZERO] The index 0 is invalid. An index shall be either < 0 or > 0 \
         (the first element has index 1). SQLSTATE: 22003"
            .to_string(),
    )
}

fn insertion_slot(position: i32, length: i32) -> i32 {
    if position > 0 {
        position - 1
    } else {
        length + position + 1
    }
}

struct RowPlan {
    leading_nulls: usize,
    slot: usize,
    trailing_nulls: usize,
}

fn plan_row(position: i32, length: i32) -> RowPlan {
    let slot = insertion_slot(position, length);
    if slot < 0 {
        let gap = usize::try_from(-slot).unwrap_or(0);
        return RowPlan {
            leading_nulls: 0,
            slot: 0,
            trailing_nulls: gap,
        };
    }
    let target = usize::try_from(slot).unwrap_or(0);
    let existing = usize::try_from(length).unwrap_or(0);
    if target > existing {
        RowPlan {
            leading_nulls: existing,
            slot: target,
            trailing_nulls: 0,
        }
    } else {
        RowPlan {
            leading_nulls: target,
            slot: target,
            trailing_nulls: 0,
        }
    }
}

impl ScalarUDFImpl for SparkArrayInsert {
    crate::shim_udf_boilerplate!("array_insert");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element = element_type(&arg_types[0])?;
        Ok(DataType::List(Arc::new(Field::new("item", element, true))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let Some(first) = args.arg_fields.first() else {
            return exec_err!("'array_insert' requires 3 arguments, got 0");
        };
        let data_type = self.return_type(&[first.data_type().clone()])?;
        let nullable = args
            .arg_fields
            .iter()
            .take(2)
            .any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [array, position, value] = arg_types else {
            return exec_err!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `array_insert` requires 3 parameters \
                 but the actual number is {}",
                arg_types.len()
            );
        };
        if !matches!(position, DataType::Int8 | DataType::Int16 | DataType::Int32) {
            return exec_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] the second parameter of \
                 `array_insert` requires the \"INT\" type, got {position}"
            );
        }
        let element = element_type(array)?;
        let widened = if element == DataType::Null {
            value.clone()
        } else {
            element
        };
        Ok(vec![
            DataType::List(Arc::new(Field::new("item", widened.clone(), true))),
            DataType::Int32,
            widened,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [source, positions, values] = arrays.as_slice() else {
            return exec_err!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `array_insert` requires 3 parameters \
                 but the actual number is {}",
                arrays.len()
            );
        };
        let Some(lists) = source.as_any().downcast_ref::<ListArray>() else {
            return exec_err!(
                "'array_insert' argument 1 must be an ARRAY, got {}",
                source.data_type()
            );
        };
        let Some(slots) = positions.as_any().downcast_ref::<Int32Array>() else {
            return exec_err!("'array_insert' argument 2 must be an INT");
        };
        let element = element_type(source.data_type())?;
        let mut pieces: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(lists.len() + 1);
        let mut present: Vec<bool> = Vec::with_capacity(lists.len());
        let mut length = 0_i32;
        offsets.push(0);
        for row in 0..lists.len() {
            if lists.is_null(row) || slots.is_null(row) {
                present.push(false);
                offsets.push(length);
                continue;
            }
            let position = slots.value(row);
            if position == 0 {
                return Err(index_of_zero());
            }
            let existing = lists.value(row);
            let existing_length = i32::try_from(existing.len()).unwrap_or(0);
            let plan = plan_row(position, existing_length);
            let head = existing.slice(0, plan.leading_nulls.min(existing.len()));
            let gap = plan.slot.saturating_sub(existing.len());
            let inserted = take(
                values.as_ref(),
                &Int32Array::from(vec![i32::try_from(row).unwrap_or(0)]),
                None,
            )?;
            let tail_start = plan.leading_nulls.min(existing.len());
            let tail = existing.slice(tail_start, existing.len() - tail_start);
            pieces.push(head);
            if gap > 0 {
                pieces.push(new_null_array(&element, gap));
            }
            pieces.push(inserted);
            if plan.trailing_nulls > 0 {
                pieces.push(new_null_array(&element, plan.trailing_nulls));
            }
            pieces.push(tail);
            let added = existing_length + 1 + i32::try_from(gap + plan.trailing_nulls).unwrap_or(0);
            length += added;
            present.push(true);
            offsets.push(length);
        }
        let references: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
        let joined: ArrayRef = if references.is_empty() {
            new_null_array(&element, 0)
        } else {
            concat(&references)?
        };
        let nulls = if present.iter().all(|found| *found) {
            None
        } else {
            Some(NullBuffer::from(present))
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", element, true)),
            OffsetBuffer::new(offsets.into()),
            joined,
            nulls,
        )?)))
    }
}
