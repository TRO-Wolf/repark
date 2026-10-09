use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, MapArray, OffsetSizeTrait,
    StructArray,
};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields};
use datafusion::common::{DataFusionError, Result};

use super::{Conversion, is_temporal_source, target_type, timestamp_ns_target};

fn children(data_type: &DataType) -> Option<Vec<&FieldRef>> {
    match data_type {
        DataType::Struct(fields) => Some(fields.iter().collect()),
        DataType::Map(entries, _) => children(entries.data_type()),
        other => element(other).map(|field| vec![field]),
    }
}

fn element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

pub(super) fn conformed_type(source: &DataType, target: &DataType, zoned: bool) -> DataType {
    match (source, target) {
        (DataType::Struct(sources), DataType::Struct(targets)) => {
            DataType::Struct(conformed_fields(sources, targets, zoned))
        }
        (DataType::Map(entries, sorted), DataType::Map(targets, _)) => {
            let conformed = conformed_field(entries, targets.data_type(), zoned);
            let conformed = match conformed.data_type() {
                DataType::Struct(fields) if fields.len() == 2 && !fields[0].is_nullable() => {
                    conformed
                }
                DataType::Struct(fields) if fields.len() == 2 => {
                    let key = Arc::new(fields[0].as_ref().clone().with_nullable(false));
                    let pair = Fields::from(vec![key, Arc::clone(&fields[1])]);
                    Arc::new(
                        conformed
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Struct(pair)),
                    )
                }
                _ => Arc::clone(entries),
            };
            DataType::Map(conformed, *sorted)
        }
        (DataType::List(field), _) => match element(target) {
            Some(to) => DataType::List(conformed_field(field, to.data_type(), zoned)),
            None => source.clone(),
        },
        (DataType::LargeList(field), _) => match element(target) {
            Some(to) => DataType::LargeList(conformed_field(field, to.data_type(), zoned)),
            None => source.clone(),
        },
        (DataType::FixedSizeList(field, size), _) => match element(target) {
            Some(to) => {
                DataType::FixedSizeList(conformed_field(field, to.data_type(), zoned), *size)
            }
            None => source.clone(),
        },
        _ if timestamp_ns_target(target) == Some(zoned) && is_temporal_source(source) => {
            target_type(zoned)
        }
        _ => source.clone(),
    }
}

fn conformed_fields(sources: &Fields, targets: &Fields, zoned: bool) -> Fields {
    let positional = !sources
        .iter()
        .any(|source| targets.iter().any(|target| target.name() == source.name()));
    sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let paired = if positional {
                targets.get(index)
            } else {
                targets.iter().find(|target| target.name() == source.name())
            };
            match paired {
                Some(target) => conformed_field(source, target.data_type(), zoned),
                None => Arc::clone(source),
            }
        })
        .collect()
}

fn conformed_field(source: &FieldRef, target: &DataType, zoned: bool) -> FieldRef {
    let conformed = conformed_type(source.data_type(), target, zoned);
    if conformed == *source.data_type() {
        return Arc::clone(source);
    }
    let nullable = source.is_nullable() || children(&conformed).is_none();
    Arc::new(
        source
            .as_ref()
            .clone()
            .with_data_type(conformed)
            .with_nullable(nullable),
    )
}

pub(super) fn conform(
    conversion: &Conversion,
    array: &ArrayRef,
    out: &DataType,
) -> Result<ArrayRef> {
    if array.data_type() == out {
        return Ok(Arc::clone(array));
    }
    match out {
        DataType::Struct(fields) => {
            let source = array
                .as_struct_opt()
                .ok_or_else(|| unexpected(array, out))?;
            Ok(Arc::new(conform_struct(conversion, source, fields)?))
        }
        DataType::Map(entries, sorted) => {
            let source = array.as_map_opt().ok_or_else(|| unexpected(array, out))?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(unexpected(array, out));
            };
            let pairs = conform_struct(conversion, source.entries(), fields)?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(entries),
                source.offsets().clone(),
                pairs,
                source.nulls().cloned(),
                *sorted,
            )?))
        }
        DataType::List(field) => conform_list::<i32>(conversion, array, field, out),
        DataType::LargeList(field) => conform_list::<i64>(conversion, array, field, out),
        DataType::FixedSizeList(field, size) => {
            let source = array
                .as_fixed_size_list_opt()
                .ok_or_else(|| unexpected(array, out))?;
            let values = conform(conversion, source.values(), field.data_type())?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                values,
                source.nulls().cloned(),
            )?))
        }
        _ => conversion.convert(array),
    }
}

fn conform_struct(
    conversion: &Conversion,
    source: &StructArray,
    fields: &Fields,
) -> Result<StructArray> {
    let columns = source
        .columns()
        .iter()
        .zip(fields)
        .map(|(column, field)| conform(conversion, column, field.data_type()))
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(StructArray::try_new(
        fields.clone(),
        columns,
        source.nulls().cloned(),
    )?)
}

fn conform_list<O: OffsetSizeTrait>(
    conversion: &Conversion,
    array: &ArrayRef,
    field: &FieldRef,
    out: &DataType,
) -> Result<ArrayRef> {
    let source = array
        .as_list_opt::<O>()
        .ok_or_else(|| unexpected(array, out))?;
    let values = conform(conversion, source.values(), field.data_type())?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?))
}

fn unexpected(array: &ArrayRef, out: &DataType) -> DataFusionError {
    DataFusionError::Internal(format!(
        "'{}' cannot conform \"{}\" to \"{out}\"",
        super::TIMESTAMP_NS_CAST_NAME,
        array.data_type()
    ))
}
