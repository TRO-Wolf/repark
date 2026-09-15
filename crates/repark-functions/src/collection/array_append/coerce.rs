use std::cmp::Ordering;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Offset, TimeZone};
use datafusion::arrow::array::builder::NullBufferBuilder;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    StructArray, TimestampMicrosecondArray, make_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, FieldRef, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs};

use crate::datetime::{datetime_from_micros, localize_wall_micros_in_zone};
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

struct DaySpan {
    start_day: i64,
    end_day: i64,
    offset_secs: Option<i32>,
}

struct ZoneSpans {
    zone: Tz,
    spans: Vec<DaySpan>,
}

impl ZoneSpans {
    fn new(zone: Tz) -> Self {
        Self {
            zone,
            spans: Vec::new(),
        }
    }

    fn resolve_day(&self, day: i64) -> Option<i32> {
        let midnight = midnight_naive(day)?;
        let next = midnight_naive(day.checked_add(1)?)?;
        let (Some(start), Some(end)) = (
            self.zone.offset_from_local_datetime(&midnight).single(),
            self.zone.offset_from_local_datetime(&next).single(),
        ) else {
            return None;
        };
        (start.fix() == end.fix()).then(|| start.fix().local_minus_utc())
    }

    fn offset_for_day(&mut self, day: i64) -> Option<i32> {
        let index = self.spans.partition_point(|span| span.end_day <= day);
        if index < self.spans.len() && self.spans[index].start_day <= day {
            return self.spans[index].offset_secs;
        }
        if let Some(offset) = self.resolve_day(day) {
            let mut start = day;
            let mut end = day + 1;
            if index > 0 {
                let left = &self.spans[index - 1];
                if left.end_day == day && left.offset_secs == Some(offset) {
                    start = left.start_day;
                    self.spans.remove(index - 1);
                }
            }
            let index = self.spans.partition_point(|span| span.end_day <= day);
            if index < self.spans.len() {
                let right = &self.spans[index];
                if right.start_day == day + 1 && right.offset_secs == Some(offset) {
                    end = right.end_day;
                    self.spans.remove(index);
                }
            }
            let index = self.spans.partition_point(|span| span.end_day <= day);
            self.spans.insert(
                index,
                DaySpan {
                    start_day: start,
                    end_day: end,
                    offset_secs: Some(offset),
                },
            );
            return Some(offset);
        }
        self.spans.insert(
            index,
            DaySpan {
                start_day: day,
                end_day: day + 1,
                offset_secs: None,
            },
        );
        None
    }

    fn localize_wall(&mut self, wall_micros: i64) -> Option<i64> {
        let day = wall_micros.div_euclid(86_400_000_000);
        match self.offset_for_day(day) {
            Some(offset) => {
                let micros = wall_micros.checked_sub(i64::from(offset) * 1_000_000)?;
                datetime_from_micros(micros).map(|_| micros)
            }
            None => localize_wall_micros_in_zone(wall_micros, self.zone),
        }
    }
}

fn midnight_naive(day: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp(day.checked_mul(86_400)?, 0).map(|instant| instant.naive_utc())
}

fn push_localized(
    localizer: &mut ZoneSpans,
    wall: Option<i64>,
    values: &mut Vec<i64>,
    nulls: &mut NullBufferBuilder,
) {
    if let Some(micros) = wall.and_then(|wall| localizer.localize_wall(wall)) {
        values.push(micros);
        nulls.append_non_null();
    } else {
        values.push(0);
        nulls.append_null();
    }
}

fn localize_wall_column(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Option<Tz>,
) -> Result<ArrayRef> {
    let DataType::Timestamp(_, Some(zone_name)) = target else {
        return exec_err!("localize target is not a zoned timestamp: {target}");
    };
    let Some(zone) = zone else {
        return exec_err!("wall-clock localization requires a session time zone");
    };
    let mut localizer = ZoneSpans::new(zone);
    let mut values = Vec::with_capacity(array.len());
    let mut nulls = NullBufferBuilder::new(array.len());
    match source {
        DataType::Date32 | DataType::Date64 => {
            let days = cast(array.as_ref(), &DataType::Date32)?;
            let days = days.as_primitive::<Date32Type>();
            for row in 0..days.len() {
                let wall =
                    (!days.is_null(row)).then(|| i64::from(days.value(row)) * 86_400 * 1_000_000);
                push_localized(&mut localizer, wall, &mut values, &mut nulls);
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let micros = array.as_primitive::<TimestampMicrosecondType>();
            for row in 0..micros.len() {
                let wall = (!micros.is_null(row)).then(|| micros.value(row));
                push_localized(&mut localizer, wall, &mut values, &mut nulls);
            }
        }
        other => return exec_err!("cannot localize a {other} wall clock"),
    }
    let stamps = TimestampMicrosecondArray::new(values.into(), nulls.finish());
    Ok(Arc::new(stamps.with_timezone(zone_name.as_ref())))
}

fn time_unit_multiple(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

pub(super) fn rescale_timestamp_column(
    array: &ArrayRef,
    source_unit: TimeUnit,
    target_unit: TimeUnit,
    target: &DataType,
) -> Result<ArrayRef> {
    let ints = cast(array.as_ref(), &DataType::Int64)?;
    let ints = ints.as_primitive::<Int64Type>();
    let source_size = time_unit_multiple(source_unit);
    let target_size = time_unit_multiple(target_unit);
    let converted = match source_size.cmp(&target_size) {
        Ordering::Greater => match source_size / target_size {
            1_000 => ints.unary::<_, Int64Type>(|value| value / 1_000),
            1_000_000 => ints.unary::<_, Int64Type>(|value| value / 1_000_000),
            1_000_000_000 => ints.unary::<_, Int64Type>(|value| value / 1_000_000_000),
            divisor => ints.unary::<_, Int64Type>(|value| value / divisor),
        },
        Ordering::Equal => ints.clone(),
        Ordering::Less => match target_size / source_size {
            1_000 => ints.unary_opt::<_, Int64Type>(|value| value.checked_mul(1_000)),
            1_000_000 => ints.unary_opt::<_, Int64Type>(|value| value.checked_mul(1_000_000)),
            1_000_000_000 => {
                ints.unary_opt::<_, Int64Type>(|value| value.checked_mul(1_000_000_000))
            }
            factor => ints.unary_opt::<_, Int64Type>(|value| value.checked_mul(factor)),
        },
    };
    Ok(make_array(
        converted
            .to_data()
            .into_builder()
            .data_type(target.clone())
            .build()?,
    ))
}

fn offset_bound(offset: i64) -> Result<usize> {
    usize::try_from(offset)
        .map_err(|error| DataFusionError::Execution(format!("list offset out of range: {error}")))
}

fn convert_list(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Option<Tz>,
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
            let base = list.offsets()[0];
            let first = offset_bound(i64::from(base))?;
            let end = offset_bound(i64::from(list.offsets()[list.len()]))?;
            let window = list.values().slice(first, end - first);
            let values = convert_array(&window, source_element, target_element, zone)?;
            let offsets =
                OffsetBuffer::new(list.offsets().iter().map(|offset| *offset - base).collect());
            Ok(Arc::new(ListArray::new(
                field,
                offsets,
                values,
                list.nulls().cloned(),
            )))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let base = list.offsets()[0];
            let first = offset_bound(base)?;
            let end = offset_bound(list.offsets()[list.len()])?;
            let window = list.values().slice(first, end - first);
            let values = convert_array(&window, source_element, target_element, zone)?;
            let rebased = list
                .offsets()
                .iter()
                .map(|offset| *offset - base)
                .collect::<Vec<_>>();
            if let DataType::LargeList(_) = target {
                return Ok(Arc::new(LargeListArray::new(
                    field,
                    OffsetBuffer::new(rebased.into()),
                    values,
                    list.nulls().cloned(),
                )));
            }
            let offsets = OffsetBuffer::new(
                rebased
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
            let width = usize::try_from(*size).map_err(|error| {
                DataFusionError::Execution(format!("fixed-size list width: {error}"))
            })?;
            let window = list
                .values()
                .slice(list.offset() * width, list.len() * width);
            let values = convert_array(&window, source_element, target_element, zone)?;
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
    zone: Option<Tz>,
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
    zone: Option<Tz>,
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
    let base = map.offsets()[0];
    let first = offset_bound(i64::from(base))?;
    let end = offset_bound(i64::from(map.offsets()[map.len()]))?;
    let entries = map.entries().slice(first, end - first);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution(format!("expected map entries: {source}")))?;
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
    let offsets = OffsetBuffer::new(map.offsets().iter().map(|offset| *offset - base).collect());
    Ok(Arc::new(MapArray::new(
        Arc::clone(target_field),
        offsets,
        new_entries,
        map.nulls().cloned(),
        *target_sorted,
    )))
}

fn convert_array(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
    zone: Option<Tz>,
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
        (
            DataType::Timestamp(source_unit, source_zone),
            DataType::Timestamp(target_unit, target_zone),
        ) if source_zone == target_zone => {
            rescale_timestamp_column(array, *source_unit, *target_unit, target)
        }
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
    zone: Option<Tz>,
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
