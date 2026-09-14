use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    StructArray, TimestampMicrosecondArray, make_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, FieldRef, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs};

use crate::datetime::localize_wall_micros_in_zone;
use crate::instant_ts::{ltz_timestamp_type, ntz_timestamp_type};

const NUMERIC_ORDER: [DataType; 6] = [
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
];

fn numeric_rank(data_type: &DataType) -> Option<usize> {
    NUMERIC_ORDER.iter().position(|t| t == data_type)
}

fn ladder_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Float16 => &DataType::Float32,
        other => other,
    }
}

fn is_ntz(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

fn is_list_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..)
    )
}

fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        DataType::UInt32 => "INT UNSIGNED".to_string(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP_NTZ".to_string(),
        DataType::Timestamp(..) => "TIMESTAMP".to_string(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(entries) if entries.len() >= 2 => format!(
                "MAP<{}, {}>",
                spark_type_name(entries[0].data_type()),
                spark_type_name(entries[1].data_type())
            ),
            other => other.to_string().to_uppercase(),
        },
        DataType::Struct(fields) => {
            let names = fields
                .iter()
                .map(|field| {
                    let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
                    format!(
                        "{}: {}{nullable}",
                        field.name(),
                        spark_type_name(field.data_type())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT<{names}>")
        }
        other => other.to_string().to_uppercase(),
    }
}

fn diff_types_error(name: &str, array_type: &DataType, element_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES] Cannot resolve \"{name}\" due to data \
         type mismatch: Input to `{name}` should have been \"ARRAY\" followed by a value with \
         same element type, but it's [\"{}\", \"{}\"].",
        spark_type_name(array_type),
        spark_type_name(element_type)
    ))
}

pub(super) fn spark_common_element(
    array_element: &DataType,
    element: &DataType,
) -> Option<DataType> {
    if let (DataType::Timestamp(..), DataType::Timestamp(..)) = (array_element, element) {
        return Some(if is_ntz(array_element) && is_ntz(element) {
            ntz_timestamp_type()
        } else {
            ltz_timestamp_type()
        });
    }
    if array_element == element {
        return Some(element.clone());
    }
    if matches!(
        (array_element, element),
        (
            DataType::Date32 | DataType::Date64,
            DataType::Date32 | DataType::Date64
        )
    ) {
        return Some(DataType::Date32);
    }
    let timestamp_peer = match (array_element, element) {
        (DataType::Date32 | DataType::Date64, peer @ DataType::Timestamp(..))
        | (peer @ DataType::Timestamp(..), DataType::Date32 | DataType::Date64) => Some(peer),
        _ => None,
    };
    if let Some(peer) = timestamp_peer {
        return Some(if is_ntz(peer) {
            ntz_timestamp_type()
        } else {
            ltz_timestamp_type()
        });
    }
    if matches!(array_element, DataType::Null) {
        return Some(element.clone());
    }
    if matches!(element, DataType::Null) {
        return Some(array_element.clone());
    }
    if let (Some(a), Some(e)) = (
        numeric_rank(ladder_type(array_element)),
        numeric_rank(ladder_type(element)),
    ) {
        return Some(NUMERIC_ORDER[a.max(e)].clone());
    }
    match (array_element, element) {
        (a, e) if is_list_type(a) && is_list_type(e) => {
            let inner = spark_common_element(array_element_type(a)?, array_element_type(e)?)?;
            Some(DataType::new_list(inner, true))
        }
        (DataType::Map(a_field, a_sorted), DataType::Map(e_field, _)) => {
            spark_common_map(a_field, *a_sorted, e_field)
        }
        (DataType::Struct(a_fields), DataType::Struct(e_fields)) => {
            if a_fields.len() != e_fields.len() {
                return None;
            }
            let mut fields = Vec::with_capacity(a_fields.len());
            for (a_field, e_field) in a_fields.iter().zip(e_fields.iter()) {
                if !a_field.name().eq_ignore_ascii_case(e_field.name()) {
                    return None;
                }
                let common = spark_common_element(a_field.data_type(), e_field.data_type())?;
                fields.push(Arc::new(Field::new(
                    a_field.name(),
                    common,
                    a_field.is_nullable(),
                )));
            }
            Some(DataType::Struct(fields.into()))
        }
        _ => None,
    }
}

fn spark_common_map(
    array_field: &FieldRef,
    array_sorted: bool,
    element_field: &FieldRef,
) -> Option<DataType> {
    let (DataType::Struct(a_entries), DataType::Struct(e_entries)) =
        (array_field.data_type(), element_field.data_type())
    else {
        return None;
    };
    if a_entries.len() != 2 || e_entries.len() != 2 {
        return None;
    }
    let key = spark_common_element(a_entries[0].data_type(), e_entries[0].data_type())?;
    let value = spark_common_element(a_entries[1].data_type(), e_entries[1].data_type())?;
    let entries = Field::new(
        array_field.name(),
        DataType::Struct(
            [
                Arc::new(Field::new(
                    a_entries[0].name(),
                    key,
                    a_entries[0].is_nullable(),
                )),
                Arc::new(Field::new(
                    a_entries[1].name(),
                    value,
                    a_entries[1].is_nullable(),
                )),
            ]
            .into(),
        ),
        array_field.is_nullable(),
    );
    Some(DataType::Map(Arc::new(entries), array_sorted))
}

pub(super) fn array_element_type(array_type: &DataType) -> Option<&DataType> {
    match array_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type())
        }
        _ => None,
    }
}

pub(super) fn widened_list(array_type: &DataType, element: DataType) -> DataType {
    match array_type {
        DataType::LargeList(_) => {
            DataType::LargeList(Arc::new(Field::new_list_field(element, true)))
        }
        _ => DataType::new_list(element, true),
    }
}

pub(super) fn spark_return_type(arg_types: &[DataType], name: &str) -> Result<DataType> {
    let [array_type, element_type] = arg_types else {
        return plan_err!(
            "'{name}' expects (array, element), got {} argument(s)",
            arg_types.len()
        );
    };
    let element = match array_type {
        DataType::Null => element_type.clone(),
        _ => match array_element_type(array_type) {
            Some(array_element) => spark_common_element(array_element, element_type)
                .ok_or_else(|| diff_types_error(name, array_type, element_type))?,
            None => {
                return plan_err!("'{name}' argument 1 must be an ARRAY, got {array_type}");
            }
        },
    };
    Ok(widened_list(array_type, element))
}

pub(super) fn spark_coerce_args(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    let [array_type, element_type] = arg_types else {
        return plan_err!(
            "'{name}' expects (array, element), got {} argument(s)",
            arg_types.len()
        );
    };
    if matches!(array_type, DataType::Null) {
        return Ok(arg_types.to_vec());
    }
    let Some(array_element) = array_element_type(array_type) else {
        return Err(diff_types_error(name, array_type, element_type));
    };
    match spark_common_element(array_element, element_type) {
        Some(_) => Ok(arg_types.to_vec()),
        None => Err(diff_types_error(name, array_type, element_type)),
    }
}

pub(super) fn spark_return_field(args: &ReturnFieldArgs<'_>, name: &str) -> Result<FieldRef> {
    let Some(array_field) = args.arg_fields.first() else {
        return exec_err!("'{name}' requires 2 arguments, got 0");
    };
    let element_type = args
        .arg_fields
        .get(1)
        .map_or(DataType::Null, |field| field.data_type().clone());
    let data_type = spark_return_type(&[array_field.data_type().clone(), element_type], name)?;
    Ok(Arc::new(Field::new(
        name,
        data_type,
        array_field.is_nullable(),
    )))
}

fn localize_wall_column(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Tz,
) -> Result<ArrayRef> {
    let walls: Vec<Option<i64>> = match source {
        DataType::Date32 | DataType::Date64 => {
            let days = cast(array.as_ref(), &DataType::Date32)?;
            let days = days.as_primitive::<Date32Type>();
            (0..days.len())
                .map(|row| {
                    (!days.is_null(row)).then(|| i64::from(days.value(row)) * 86_400 * 1_000_000)
                })
                .collect()
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let micros = array.as_primitive::<TimestampMicrosecondType>();
            (0..micros.len())
                .map(|row| (!micros.is_null(row)).then(|| micros.value(row)))
                .collect()
        }
        other => return exec_err!("cannot localize a {other} wall clock"),
    };
    let mut builder = TimestampMicrosecondArray::builder(walls.len());
    for wall in walls {
        match wall.and_then(|wall| localize_wall_micros_in_zone(wall, zone)) {
            Some(micros) => builder.append_value(micros),
            None => builder.append_null(),
        }
    }
    let DataType::Timestamp(_, Some(zone_name)) = target else {
        return exec_err!("localize target is not a zoned timestamp: {target}");
    };
    Ok(Arc::new(builder.finish().with_timezone(zone_name.as_ref())))
}

fn convert_list(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Tz,
) -> Result<ArrayRef> {
    let Some(source_element) = array_element_type(source) else {
        return exec_err!("list conversion source is not a list: {source}");
    };
    let Some(target_element) = array_element_type(target) else {
        return exec_err!("list conversion target is not a list: {target}");
    };
    let field = Arc::new(Field::new_list_field(target_element.clone(), true));
    match source {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let values = convert_array(list.values(), source_element, target_element, zone)?;
            Ok(Arc::new(ListArray::new(
                field,
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let values = convert_array(list.values(), source_element, target_element, zone)?;
            if let DataType::LargeList(_) = target {
                return Ok(Arc::new(LargeListArray::new(
                    field,
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                )));
            }
            let offsets = OffsetBuffer::new(
                list.offsets()
                    .iter()
                    .map(|offset| i32::try_from(*offset))
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "large list offsets exceed i32: {error}"
                        ))
                    })?,
            );
            Ok(Arc::new(ListArray::new(
                field,
                offsets,
                values,
                list.nulls().cloned(),
            )))
        }
        DataType::FixedSizeList(_, size) => {
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("expected a fixed-size list, got {source}"))
                })?;
            let values = convert_array(list.values(), source_element, target_element, zone)?;
            if let DataType::FixedSizeList(_, target_size) = target {
                return Ok(Arc::new(FixedSizeListArray::new(
                    field,
                    *target_size,
                    values,
                    list.nulls().cloned(),
                )));
            }
            let rows = i32::try_from(list.len()).map_err(|error| {
                DataFusionError::Execution(format!("fixed-size list too long: {error}"))
            })?;
            let offsets = OffsetBuffer::new((0..=rows).map(|row| row * *size).collect());
            Ok(Arc::new(ListArray::new(
                field,
                offsets,
                values,
                list.nulls().cloned(),
            )))
        }
        other => exec_err!("list conversion source is not a list: {other}"),
    }
}

fn convert_struct(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Tz,
) -> Result<ArrayRef> {
    let (DataType::Struct(source_fields), DataType::Struct(target_fields)) = (source, target)
    else {
        return exec_err!("struct conversion requires struct types: {source} -> {target}");
    };
    let structure = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution(format!("expected a struct array: {source}")))?;
    let mut columns = Vec::with_capacity(target_fields.len());
    for (index, target_field) in target_fields.iter().enumerate() {
        columns.push(convert_array(
            structure.column(index),
            source_fields[index].data_type(),
            target_field.data_type(),
            zone,
        )?);
    }
    Ok(Arc::new(StructArray::new(
        target_fields.clone(),
        columns,
        structure.nulls().cloned(),
    )))
}

fn convert_map(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Tz,
) -> Result<ArrayRef> {
    let (DataType::Map(source_field, _), DataType::Map(target_field, target_sorted)) =
        (source, target)
    else {
        return exec_err!("map conversion requires map types: {source} -> {target}");
    };
    let (DataType::Struct(source_entries), DataType::Struct(target_entries)) =
        (source_field.data_type(), target_field.data_type())
    else {
        return exec_err!("map entries must be structs: {source} -> {target}");
    };
    let map = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Execution(format!("expected a map array: {source}")))?;
    let entries = map.entries();
    let keys = convert_array(
        entries.column(0),
        source_entries[0].data_type(),
        target_entries[0].data_type(),
        zone,
    )?;
    let values = convert_array(
        entries.column(1),
        source_entries[1].data_type(),
        target_entries[1].data_type(),
        zone,
    )?;
    let new_entries = StructArray::new(
        target_entries.clone(),
        vec![keys, values],
        entries.nulls().cloned(),
    );
    Ok(Arc::new(MapArray::new(
        Arc::clone(target_field),
        map.offsets().clone(),
        new_entries,
        map.nulls().cloned(),
        *target_sorted,
    )))
}

fn convert_array(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Tz,
) -> Result<ArrayRef> {
    if source == target {
        return Ok(Arc::clone(array));
    }
    match (source, target) {
        (DataType::Null, _) => Ok(make_array(ArrayData::new_null(target, array.len()))),
        (
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
        ) => localize_wall_column(array, source, target, zone),
        (a, b) if is_list_type(a) && is_list_type(b) => convert_list(array, a, b, zone),
        (DataType::Struct(_), DataType::Struct(_)) => convert_struct(array, source, target, zone),
        (DataType::Map(..), DataType::Map(..)) => convert_map(array, source, target, zone),
        _ => Ok(cast(array.as_ref(), target)?),
    }
}

pub(super) fn convert_columnar(
    value: &ColumnarValue,
    source: &DataType,
    target: &DataType,
    zone: Tz,
) -> Result<ColumnarValue> {
    if source == target {
        return Ok(value.clone());
    }
    match value {
        ColumnarValue::Scalar(scalar) => {
            let converted = convert_array(&scalar.to_array()?, source, target, zone)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &converted, 0,
            )?))
        }
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(convert_array(
            array, source, target, zone,
        )?)),
    }
}
