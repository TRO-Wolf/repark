use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray,
    Float64Array, Int64Array, ListArray, MapArray, StringBuilder, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, FieldRef, Fields, TimeUnit};

use crate::cdf_infer::cells::{Cdf, Cell, CellKind, Ctx, py_str};

type Slot<'c, 'py> = Option<&'c Cell<'py>>;

fn slot_cell<'c, 'py>(slot: Slot<'c, 'py>) -> Option<&'c Cell<'py>> {
    slot.filter(|cell| !matches!(cell.kind, CellKind::Null))
}

fn null_buffer(col: &[Slot<'_, '_>]) -> Option<NullBuffer> {
    let mut any_null = false;
    let validity: Vec<bool> = col
        .iter()
        .map(|slot| {
            let valid = slot_cell(*slot).is_some();
            any_null |= !valid;
            valid
        })
        .collect();
    if any_null {
        Some(NullBuffer::new(BooleanBuffer::from(validity)))
    } else {
        None
    }
}

fn scalar_slots<'c, 'py, T>(
    col: &[Slot<'c, 'py>],
    convert: impl Fn(&'c Cell<'py>) -> Result<Option<T>, Cdf>,
) -> Result<Vec<Option<T>>, Cdf> {
    let mut values: Vec<Option<T>> = Vec::with_capacity(col.len());
    for slot in col {
        values.push(match slot_cell(*slot) {
            None => None,
            Some(cell) => convert(cell)?,
        });
    }
    Ok(values)
}

fn ts_us(cell: &Cell<'_>, utc_zero: bool) -> Result<i64, Cdf> {
    let CellKind::Dt { wall_us, off_us } = &cell.kind else {
        return Err(Cdf::Fallback);
    };
    match off_us {
        Some(offset) => Ok(wall_us - offset),
        None => {
            if utc_zero {
                Ok(*wall_us)
            } else {
                Err(Cdf::Fallback)
            }
        }
    }
}

fn dec_unscaled(cell: &Cell<'_>) -> Result<i128, Cdf> {
    let CellKind::Dec { unscaled, .. } = &cell.kind else {
        return Err(Cdf::Fallback);
    };
    Ok(*unscaled)
}

fn struct_child_cell<'c, 'py>(
    cell: &'c Cell<'py>,
    index: usize,
    name: &str,
    field_count: usize,
) -> Result<Slot<'c, 'py>, Cdf> {
    match &cell.kind {
        CellKind::Null => Ok(None),
        CellKind::Dict(pairs) => {
            for (key, value) in pairs {
                if let CellKind::Str(key_name) = &key.kind
                    && key_name.as_str() == name
                {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
        CellKind::Row { fields, vals } => Ok(fields
            .iter()
            .position(|field| field == name)
            .map(|pos| &vals[pos])),
        CellKind::Tup(items) | CellKind::List(items) => {
            if items.len() != field_count {
                return Err(Cdf::Fallback);
            }
            Ok(Some(&items[index]))
        }
        _ => Err(Cdf::Fallback),
    }
}

fn build_struct<'py>(
    col: &[Slot<'_, 'py>],
    fields: &Fields,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<ArrayRef, Cdf> {
    let fill = Cell {
        obj: None,
        kind: CellKind::Fill,
    };
    let mut children: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        let mut slots = Vec::with_capacity(col.len());
        for slot in col {
            slots.push(match slot_cell(*slot) {
                None => Some(&fill),
                Some(cell) => {
                    if matches!(cell.kind, CellKind::Fill) {
                        Some(&fill)
                    } else {
                        struct_child_cell(cell, index, field.name(), fields.len())?
                    }
                }
            });
        }
        children.push(build_column(&slots, field.data_type(), cx, depth + 1)?);
    }
    let array = StructArray::try_new(fields.clone(), children, null_buffer(col))
        .map_err(|_| Cdf::Fallback)?;
    Ok(Arc::new(array))
}

fn build_list<'c, 'py>(
    col: &[Slot<'c, 'py>],
    child: &FieldRef,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<ArrayRef, Cdf> {
    let mut offsets: Vec<i32> = Vec::with_capacity(col.len() + 1);
    offsets.push(0);
    let mut slots: Vec<Slot<'c, 'py>> = Vec::new();
    for slot in col {
        match slot_cell(*slot) {
            None => offsets.push(i32::try_from(slots.len()).map_err(|_| Cdf::Fallback)?),
            Some(cell) => match &cell.kind {
                CellKind::List(items) | CellKind::Tup(items) => {
                    for item in items {
                        slots.push(Some(item));
                    }
                    offsets.push(i32::try_from(slots.len()).map_err(|_| Cdf::Fallback)?);
                }
                CellKind::Fill => {
                    offsets.push(i32::try_from(slots.len()).map_err(|_| Cdf::Fallback)?);
                }
                _ => return Err(Cdf::Fallback),
            },
        }
    }
    let values = build_column(&slots, child.data_type(), cx, depth + 1)?;
    let array = ListArray::try_new(
        child.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        null_buffer(col),
    )
    .map_err(|_| Cdf::Fallback)?;
    Ok(Arc::new(array))
}

fn build_fixed_list<'py>(
    col: &[Slot<'_, 'py>],
    child: &FieldRef,
    size: i32,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<ArrayRef, Cdf> {
    let width = usize::try_from(size).map_err(|_| Cdf::Fallback)?;
    let fill = Cell {
        obj: None,
        kind: CellKind::Fill,
    };
    let mut slots = Vec::new();
    for slot in col {
        match slot_cell(*slot) {
            None => {
                for _ in 0..width {
                    slots.push(None);
                }
            }
            Some(cell) => match &cell.kind {
                CellKind::List(items) | CellKind::Tup(items) => {
                    if items.len() != width {
                        return Err(Cdf::Fallback);
                    }
                    for item in items {
                        slots.push(Some(item));
                    }
                }
                CellKind::Fill => {
                    for _ in 0..width {
                        slots.push(Some(&fill));
                    }
                }
                _ => return Err(Cdf::Fallback),
            },
        }
    }
    let values = build_column(&slots, child.data_type(), cx, depth + 1)?;
    let array = FixedSizeListArray::try_new(child.clone(), size, values, null_buffer(col))
        .map_err(|_| Cdf::Fallback)?;
    Ok(Arc::new(array))
}

fn build_map<'c, 'py>(
    col: &[Slot<'c, 'py>],
    entries_field: &FieldRef,
    ordered: bool,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<ArrayRef, Cdf> {
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(Cdf::Fallback);
    };
    let Some(key_field) = entry_fields.first() else {
        return Err(Cdf::Fallback);
    };
    let Some(value_field) = entry_fields.get(1) else {
        return Err(Cdf::Fallback);
    };
    let mut offsets: Vec<i32> = Vec::with_capacity(col.len() + 1);
    offsets.push(0);
    let mut key_slots: Vec<Slot<'c, 'py>> = Vec::new();
    let mut value_slots: Vec<Slot<'c, 'py>> = Vec::new();
    for slot in col {
        match slot_cell(*slot) {
            None => offsets.push(i32::try_from(key_slots.len()).map_err(|_| Cdf::Fallback)?),
            Some(cell) => match &cell.kind {
                CellKind::Dict(pairs) => {
                    for (key, value) in pairs {
                        key_slots.push(Some(key));
                        value_slots.push(Some(value));
                    }
                    offsets.push(i32::try_from(key_slots.len()).map_err(|_| Cdf::Fallback)?);
                }
                CellKind::Fill => {
                    offsets.push(i32::try_from(key_slots.len()).map_err(|_| Cdf::Fallback)?);
                }
                _ => return Err(Cdf::Fallback),
            },
        }
    }
    let keys = build_column(&key_slots, key_field.data_type(), cx, depth + 1)?;
    let values = build_column(&value_slots, value_field.data_type(), cx, depth + 1)?;
    let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)
        .map_err(|_| Cdf::Fallback)?;
    let array = MapArray::try_new(
        entries_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        null_buffer(col),
        ordered,
    )
    .map_err(|_| Cdf::Fallback)?;
    Ok(Arc::new(array))
}

fn build_timestamp(
    col: &[Slot<'_, '_>],
    unit: TimeUnit,
    tz: Option<&Arc<str>>,
    utc_zero: bool,
) -> Result<ArrayRef, Cdf> {
    let mut values: Vec<Option<i64>> = Vec::with_capacity(col.len());
    for slot in col {
        values.push(match slot_cell(*slot) {
            None => None,
            Some(cell) => {
                let instant = if matches!(cell.kind, CellKind::Fill) {
                    0
                } else {
                    ts_us(cell, utc_zero)?
                };
                Some(match unit {
                    TimeUnit::Second => instant.div_euclid(1_000_000),
                    TimeUnit::Millisecond => instant.div_euclid(1_000),
                    TimeUnit::Microsecond => instant,
                    TimeUnit::Nanosecond => instant.checked_mul(1_000).ok_or(Cdf::Fallback)?,
                })
            }
        });
    }
    let zone = tz.cloned();
    let array: ArrayRef = match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone)),
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone))
        }
    };
    Ok(array)
}

fn build_bool(col: &[Slot<'_, '_>]) -> Result<ArrayRef, Cdf> {
    let values = scalar_slots(col, |cell| match &cell.kind {
        CellKind::Bool(value) => Ok(Some(*value)),
        CellKind::Fill => Ok(Some(false)),
        _ => Err(Cdf::Fallback),
    })?;
    Ok(Arc::new(BooleanArray::from(values)))
}

fn build_int64(col: &[Slot<'_, '_>]) -> Result<ArrayRef, Cdf> {
    let values = scalar_slots(col, |cell| match &cell.kind {
        CellKind::Int(value) => Ok(Some(*value)),
        CellKind::Fill => Ok(Some(0)),
        _ => Err(Cdf::Fallback),
    })?;
    Ok(Arc::new(Int64Array::from(values)))
}

#[allow(clippy::cast_precision_loss)]
fn build_float64(col: &[Slot<'_, '_>]) -> Result<ArrayRef, Cdf> {
    let values = scalar_slots(col, |cell| match &cell.kind {
        CellKind::Float(value) => Ok(Some(*value)),
        CellKind::Int(value) => Ok(Some(*value as f64)),
        CellKind::Fill => Ok(Some(0.0)),
        _ => Err(Cdf::Fallback),
    })?;
    Ok(Arc::new(Float64Array::from(values)))
}

fn build_utf8(col: &[Slot<'_, '_>]) -> Result<ArrayRef, Cdf> {
    let mut builder = StringBuilder::new();
    for slot in col {
        match slot_cell(*slot) {
            None => builder.append_null(),
            Some(cell) => match &cell.kind {
                CellKind::Str(value) => builder.append_value(value.as_str()),
                CellKind::Fill => builder.append_value(""),
                _ => {
                    let Some(obj) = &cell.obj else {
                        return Err(Cdf::Fallback);
                    };
                    builder.append_value(py_str(obj)?.as_str());
                }
            },
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_binary(col: &[Slot<'_, '_>]) -> Result<ArrayRef, Cdf> {
    let values = scalar_slots(col, |cell| match &cell.kind {
        CellKind::Bin(value) => Ok(Some(value.as_slice())),
        CellKind::Str(value) => Ok(Some(value.as_bytes())),
        CellKind::Fill => Ok(Some(b"".as_slice())),
        _ => Err(Cdf::Fallback),
    })?;
    Ok(Arc::new(BinaryArray::from(values)))
}

fn build_date32(col: &[Slot<'_, '_>]) -> Result<ArrayRef, Cdf> {
    let values = scalar_slots(col, |cell| match &cell.kind {
        CellKind::Date(days) => Ok(Some(*days)),
        CellKind::Dt { wall_us, .. } => Ok(Some(
            i32::try_from(wall_us.div_euclid(86_400_000_000)).map_err(|_| Cdf::Fallback)?,
        )),
        CellKind::Fill => Ok(Some(0)),
        _ => Err(Cdf::Fallback),
    })?;
    Ok(Arc::new(Date32Array::from(values)))
}

fn build_decimal(col: &[Slot<'_, '_>]) -> Result<ArrayRef, Cdf> {
    let values = scalar_slots(col, |cell| match &cell.kind {
        CellKind::Fill => Ok(Some(0)),
        _ => Ok(Some(dec_unscaled(cell)?)),
    })?;
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(38, 18)
        .map_err(|_| Cdf::Fallback)?;
    Ok(Arc::new(array))
}

pub(crate) fn build_column<'py>(
    col: &[Slot<'_, 'py>],
    data_type: &DataType,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<ArrayRef, Cdf> {
    if depth > 100 {
        return Err(Cdf::Fallback);
    }
    match data_type {
        DataType::Boolean => build_bool(col),
        DataType::Int64 => build_int64(col),
        DataType::Float64 => build_float64(col),
        DataType::Utf8 | DataType::LargeUtf8 => build_utf8(col),
        DataType::Binary => build_binary(col),
        DataType::Date32 => build_date32(col),
        DataType::Timestamp(unit, tz) => {
            build_timestamp(col, *unit, tz.as_ref(), cx.session_tz_utc)
        }
        DataType::Decimal128(38, 18) => build_decimal(col),
        DataType::Struct(fields) => build_struct(col, fields, cx, depth),
        DataType::List(child) => build_list(col, child, cx, depth),
        DataType::FixedSizeList(child, size) => build_fixed_list(col, child, *size, cx, depth),
        DataType::Map(entries, ordered) => build_map(col, entries, *ordered, cx, depth),
        _ => Err(Cdf::Fallback),
    }
}
