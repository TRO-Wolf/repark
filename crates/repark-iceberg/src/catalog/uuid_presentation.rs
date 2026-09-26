use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, RecordBatch,
    StringArray, make_array,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Error, ErrorKind};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[allow(clippy::missing_errors_doc)]
pub fn presented_arrow_schema(schema: &IcebergSchema) -> iceberg::Result<ArrowSchema> {
    Ok(restyled_schema(
        &schema_to_arrow_schema(schema)?,
        schema,
        true,
    ))
}

pub(crate) fn stored_arrow_schema(presented: &SchemaRef, schema: &IcebergSchema) -> SchemaRef {
    Arc::new(restyled_schema(presented, schema, false))
}

fn restyled_schema(arrow: &ArrowSchema, schema: &IcebergSchema, present: bool) -> ArrowSchema {
    ArrowSchema::new_with_metadata(
        arrow
            .fields()
            .iter()
            .map(|field| Arc::new(restyled_field(field, schema, present)))
            .collect::<Fields>(),
        arrow.metadata().clone(),
    )
}

fn restyled_field(field: &Field, schema: &IcebergSchema, present: bool) -> Field {
    let restyle = |child: &Arc<Field>| Arc::new(restyled_field(child, schema, present));
    let data_type = match field.data_type() {
        DataType::FixedSizeBinary(16) if present && is_uuid_field(field, schema) => DataType::Utf8,
        DataType::Utf8 if !present && is_uuid_field(field, schema) => DataType::FixedSizeBinary(16),
        DataType::Struct(children) => DataType::Struct(children.iter().map(restyle).collect()),
        DataType::List(element) => DataType::List(restyle(element)),
        DataType::LargeList(element) => DataType::LargeList(restyle(element)),
        DataType::Map(entries, sorted) => DataType::Map(restyle(entries), *sorted),
        other => other.clone(),
    };
    Field::new(field.name().clone(), data_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

fn is_uuid_field(field: &Field, schema: &IcebergSchema) -> bool {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|id| id.parse::<i32>().ok())
        .and_then(|id| schema.field_by_id(id))
        .is_some_and(|nested| {
            matches!(
                nested.field_type.as_ref(),
                Type::Primitive(PrimitiveType::Uuid)
            )
        })
}

pub(crate) fn store_presented_batch(
    batch: &RecordBatch,
    stored: &SchemaRef,
) -> Result<RecordBatch> {
    if batch.num_columns() != stored.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "cannot store {} presented columns as {} table columns",
            batch.num_columns(),
            stored.fields().len()
        )));
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for ((column, field), target) in batch
        .columns()
        .iter()
        .zip(batch.schema_ref().fields())
        .zip(stored.fields())
    {
        columns.push(convert_uuid_column(column, target.data_type())?);
        fields.push(
            field
                .as_ref()
                .clone()
                .with_data_type(target.data_type().clone()),
        );
    }
    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new_with_metadata(
            fields,
            batch.schema_ref().metadata().clone(),
        )),
        columns,
    )?)
}

pub(crate) fn presents_uuid(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (DataType::FixedSizeBinary(16), DataType::Utf8)
    ) || carries_uuid_pair(source, target)
}

pub(crate) fn convert_uuid_column(column: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let source = column.data_type();
    if source == target {
        return Ok(Arc::clone(column));
    }
    match (source, target) {
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::FixedSizeBinary(16),
        ) => store_uuid_text(column),
        (
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            DataType::FixedSizeBinary(16),
        ) => store_uuid_text(&binary_as_text(column)),
        (DataType::FixedSizeBinary(16), DataType::Utf8) => Ok(render_uuid_text(column)),
        (DataType::Struct(_), DataType::Struct(_))
        | (DataType::List(_), DataType::List(_))
        | (DataType::LargeList(_), DataType::LargeList(_))
        | (DataType::Map(_, _), DataType::Map(_, _))
            if carries_uuid_pair(source, target) =>
        {
            convert_children(column, target)
        }
        _ => Ok(cast_with_options(
            column,
            target,
            &CastOptions {
                safe: false,
                ..CastOptions::default()
            },
        )?),
    }
}

fn child_types(data_type: &DataType) -> Vec<&DataType> {
    match data_type {
        DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect(),
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            vec![field.data_type()]
        }
        _ => Vec::new(),
    }
}

fn carries_uuid_pair(source: &DataType, target: &DataType) -> bool {
    let (from, to) = (child_types(source), child_types(target));
    from.len() == to.len()
        && from.iter().zip(&to).any(|(from, to)| match (from, to) {
            (DataType::FixedSizeBinary(16), DataType::Utf8)
            | (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::FixedSizeBinary(16),
            ) => true,
            _ => carries_uuid_pair(from, to),
        })
}

fn convert_children(column: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let data = column.to_data();
    let children = data
        .child_data()
        .iter()
        .zip(child_types(target))
        .map(|(child, child_target)| {
            convert_uuid_column(&make_array(child.clone()), child_target)
                .map(|array| array.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    let rebuilt = data
        .into_builder()
        .data_type(target.clone())
        .child_data(children)
        .build()?;
    Ok(make_array(rebuilt))
}

fn render_uuid_text(column: &ArrayRef) -> ArrayRef {
    let bytes = column.as_fixed_size_binary();
    let rendered: StringArray = (0..bytes.len())
        .map(|row| {
            (!bytes.is_null(row))
                .then(|| <[u8; 16]>::try_from(bytes.value(row)).ok())
                .flatten()
                .map(|value| uuid::Uuid::from_bytes(value).hyphenated().to_string())
        })
        .collect();
    Arc::new(rendered)
}

fn binary_as_text(column: &ArrayRef) -> ArrayRef {
    let decode =
        |value: Option<&[u8]>| value.map(|bytes| String::from_utf8_lossy(bytes).into_owned());
    let text: StringArray = match column.data_type() {
        DataType::LargeBinary => column.as_binary::<i64>().iter().map(decode).collect(),
        DataType::BinaryView => column.as_binary_view().iter().map(decode).collect(),
        _ => column.as_binary::<i32>().iter().map(decode).collect(),
    };
    Arc::new(text)
}

fn store_uuid_text(column: &ArrayRef) -> Result<ArrayRef> {
    let text = cast_with_options(column, &DataType::Utf8, &CastOptions::default())?;
    let text = text.as_string::<i32>();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(text.len(), 16);
    for row in 0..text.len() {
        if text.is_null(row) {
            builder.append_null();
            continue;
        }
        let parsed = parse_uuid_text(text.value(row)).map_err(|message| {
            DataFusionError::External(Box::new(Error::new(ErrorKind::DataInvalid, message)))
        })?;
        builder.append_value(parsed)?;
    }
    let stored: FixedSizeBinaryArray = builder.finish();
    Ok(Arc::new(stored))
}

pub(crate) fn parse_uuid_text(value: &str) -> std::result::Result<[u8; 16], String> {
    if value.encode_utf16().count() > 36 {
        return Err("UUID string too large".to_string());
    }
    let chars: Vec<char> = value.chars().collect();
    let dashes: Vec<usize> = chars
        .iter()
        .enumerate()
        .filter(|(_, c)| **c == '-')
        .map(|(index, _)| index)
        .collect();
    let &[d1, d2, d3, d4] = dashes.as_slice() else {
        return Err(format!("Invalid UUID string: {value}"));
    };
    let group = |start: usize, end: usize| chars.get(start..end).unwrap_or(&[]);
    let g1 = parse_java_hex_long(group(0, d1))? & 0xffff_ffff;
    let g2 = parse_java_hex_long(group(d1 + 1, d2))? & 0xffff;
    let g3 = parse_java_hex_long(group(d2 + 1, d3))? & 0xffff;
    let g4 = parse_java_hex_long(group(d3 + 1, d4))? & 0xffff;
    let g5 = parse_java_hex_long(group(d4 + 1, chars.len()))? & 0xffff_ffff_ffff;
    let most = (g1 << 32) | (g2 << 16) | g3;
    let least = (g4 << 48) | g5;
    let mut out = [0u8; 16];
    for (slot, byte) in out
        .iter_mut()
        .zip(most.to_be_bytes().into_iter().chain(least.to_be_bytes()))
    {
        *slot = byte;
    }
    Ok(out)
}

fn parse_java_hex_long(group: &[char]) -> std::result::Result<u64, String> {
    let text: String = group.iter().collect();
    let error_at =
        |index: usize| format!("NumberFormatException: Error at index {index} in: \"{text}\"");
    let Some(first) = group.first() else {
        return Err("NumberFormatException: ".to_string());
    };
    let start = if *first == '+' {
        1
    } else if *first < '0' {
        return Err(error_at(0));
    } else {
        0
    };
    if group.len() <= start {
        return Err(error_at(start));
    }
    let limit = -i64::MAX;
    let multmin = limit / 16;
    let mut result: i64 = 0;
    for (index, c) in group.iter().enumerate().skip(start) {
        let Some(digit) = c.to_digit(16).map(i64::from) else {
            return Err(error_at(index));
        };
        if result < multmin {
            return Err(error_at(index));
        }
        result *= 16;
        if result < limit + digit {
            return Err(error_at(index));
        }
        result -= digit;
    }
    Ok(result.unsigned_abs())
}
