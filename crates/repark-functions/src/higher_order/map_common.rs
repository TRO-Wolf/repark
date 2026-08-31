//! Shared map helpers for Spark map higher-order kernels.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, MapArray, OffsetBufferBuilder, StructArray, UInt32Builder,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::filter as arrow_filter;
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef, Fields};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};

pub(crate) fn coerce_single_map_arg(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return plan_err!(
            "{name} function requires 1 value arguments, got {}",
            arg_types.len()
        );
    }
    match &arg_types[0] {
        DataType::Map(_, _) => Ok(vec![arg_types[0].clone()]),
        DataType::Null => plan_err!("{name} expected a map as first argument, got Null"),
        other => plan_err!("{name} expected a map as first argument, got {other}"),
    }
}

pub(crate) fn coerce_two_map_args(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return plan_err!("{name} requires 2 value arguments, got {}", arg_types.len());
    }
    Ok(vec![
        coerce_map_type(name, &arg_types[0])?,
        coerce_map_type(name, &arg_types[1])?,
    ])
}

fn coerce_map_type(name: &str, data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Map(_, _) => Ok(data_type.clone()),
        other => plan_err!("{name} expected a map, got {other}"),
    }
}

pub(crate) fn map_key_value_fields(name: &str, map: &FieldRef) -> Result<(FieldRef, FieldRef)> {
    let DataType::Map(entries, _) = map.data_type() else {
        return plan_err!("{name} expected a map, got {}", map.data_type());
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return plan_err!("{name} expected map entries to be a struct");
    };
    if fields.len() != 2 {
        return plan_err!("{name} expected map entries with key and value fields");
    }
    Ok((Arc::clone(&fields[0]), Arc::clone(&fields[1])))
}

pub(crate) struct FlatMap {
    pub keys: ArrayRef,
    pub values: ArrayRef,
    pub offsets: OffsetBuffer<i32>,
    pub nulls: Option<NullBuffer>,
    pub ordered: bool,
}

pub(crate) fn flatten_map(name: &str, array: &ArrayRef) -> Result<FlatMap> {
    let DataType::Map(_entries_field, ordered) = array.data_type() else {
        return exec_err!("{name} expected a map, got {}", array.data_type());
    };
    let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!("{name} expected MapArray"))
    })?;
    let offsets = map.offsets();
    let first = offsets.first().copied().unwrap_or(0).as_usize();
    let last = offsets.last().copied().unwrap_or(0).as_usize();
    let keys = map.keys().slice(first, last.saturating_sub(first));
    let values = map.values().slice(first, last.saturating_sub(first));
    let adjusted: Vec<i32> = offsets.iter().map(|offset| *offset - offsets[0]).collect();
    Ok(FlatMap {
        keys,
        values,
        offsets: OffsetBuffer::new(datafusion::arrow::buffer::ScalarBuffer::from(adjusted)),
        nulls: map.nulls().cloned(),
        ordered: *ordered,
    })
}

pub(crate) fn rebuild_map(
    keys: ArrayRef,
    values: ArrayRef,
    offsets: OffsetBuffer<i32>,
    nulls: Option<NullBuffer>,
    key_field: &Field,
    value_field: &Field,
    ordered: bool,
) -> Result<ArrayRef> {
    let entries_fields: Fields = vec![
        Arc::new(Field::new(
            key_field.name(),
            keys.data_type().clone(),
            key_field.is_nullable(),
        )),
        Arc::new(Field::new(
            value_field.name(),
            values.data_type().clone(),
            value_field.is_nullable(),
        )),
    ]
    .into();
    let entries = StructArray::try_new(entries_fields.clone(), vec![keys, values], None)?;
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(entries_fields),
        false,
    ));
    Ok(Arc::new(MapArray::try_new(
        entries_field,
        offsets,
        entries,
        nulls,
        ordered,
    )?) as ArrayRef)
}

pub(crate) fn refuse_null_keys(keys: &dyn Array) -> Result<()> {
    if keys.null_count() == 0 {
        return Ok(());
    }
    exec_err!("[NULL_MAP_KEY] Cannot use null as map key.")
}

pub(crate) fn refuse_duplicate_keys(
    keys: &dyn Array,
    offsets: &OffsetBuffer<i32>,
    nulls: Option<&NullBuffer>,
) -> Result<()> {
    for row in 0..offsets.len().saturating_sub(1) {
        if nulls.is_some_and(|buffer| buffer.is_null(row)) {
            continue;
        }
        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let mut seen: HashSet<ScalarValue> = HashSet::with_capacity(end.saturating_sub(start));
        for index in start..end {
            if keys.is_null(index) {
                continue;
            }
            let key = ScalarValue::try_from_array(keys, index)?;
            if !seen.insert(key.clone()) {
                return exec_err!(
                    "[DUPLICATED_MAP_KEY] Duplicate map key {key} was found, please check the input data.\n\
                     If you want to remove the duplicated keys, you can set \"spark.sql.mapKeyDedupPolicy\" \
                     to \"LAST_WIN\" so that the key inserted at last takes precedence."
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn filter_map_entries(
    keys: &ArrayRef,
    values: &ArrayRef,
    predicate: &BooleanArray,
    offsets: &OffsetBuffer<i32>,
) -> Result<(ArrayRef, ArrayRef, OffsetBuffer<i32>)> {
    let num_maps = offsets.len().saturating_sub(1);
    let mut builder = OffsetBufferBuilder::<i32>::new(num_maps);
    for i in 0..num_maps {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        let count = (start..end)
            .filter(|&j| predicate.is_valid(j) && predicate.value(j))
            .count();
        builder.push_length(count);
    }
    let new_offsets = builder.finish();
    let filtered_keys = arrow_filter(keys.as_ref(), predicate)?;
    let filtered_values = arrow_filter(values.as_ref(), predicate)?;
    Ok((filtered_keys, filtered_values, new_offsets))
}

pub(crate) fn map_row_numbers(offsets: &OffsetBuffer<i32>, rows: usize) -> Result<ArrayRef> {
    let value_count = offsets.last().map_or(0, |offset| offset.as_usize());
    let mut builder = UInt32Builder::with_capacity(value_count);
    for row in 0..rows {
        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let row_number = u32::try_from(row).map_err(|_| {
            datafusion::error::DataFusionError::Execution("map row does not fit in u32".to_string())
        })?;
        for _ in start..end {
            builder.append_value(row_number);
        }
    }
    Ok(Arc::new(builder.finish()))
}
