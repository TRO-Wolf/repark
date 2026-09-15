use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::partition_discovery::{
    DiscoveredPartitions, PartitionValue, cast_raw_partition_value, invalid_partition_message,
    user_partition_type,
};
use crate::{Error, Result};

pub(crate) type AppliedTextSchema = (SchemaRef, Vec<Field>, HashMap<PathBuf, Vec<PartitionValue>>);

pub(crate) fn text_schema_with_partitions(partitions: &[Field]) -> SchemaRef {
    let mut fields = Vec::with_capacity(partitions.len() + 1);
    fields.push(Field::new("value", DataType::Utf8, true));
    fields.extend(partitions.iter().cloned());
    Arc::new(Schema::new(fields))
}

pub(crate) fn text_schema_with_data(data: &str, partitions: &[Field]) -> SchemaRef {
    let mut fields = Vec::with_capacity(partitions.len() + 1);
    fields.push(Field::new(data, DataType::Utf8, true));
    fields.extend(partitions.iter().cloned());
    Arc::new(Schema::new(fields))
}

pub(crate) fn text_schema_error() -> Error {
    Error::Analysis(
        "text schema must be a single string field (the scan only serves struct<value:string>)"
            .to_string(),
    )
}

#[allow(clippy::missing_errors_doc)]
fn overlay_partition_row(
    final_fields: &[Field],
    final_types: &[DataType],
    displays: &[Option<String>],
    current: &[PartitionValue],
    current_raw: &[Option<String>],
    zone: Tz,
) -> Result<Vec<PartitionValue>> {
    let mut row = Vec::with_capacity(final_fields.len());
    for (index, field) in final_fields.iter().enumerate() {
        let Some(display) = displays[index].as_deref() else {
            let kept = current.get(index).cloned().unwrap_or(PartitionValue::Null);
            row.push(kept);
            continue;
        };
        let raw = current_raw.get(index).and_then(|slot| slot.as_ref());
        let Some(text) = raw else {
            row.push(PartitionValue::Null);
            continue;
        };
        let Some(typed) = cast_raw_partition_value(text, &final_types[index], zone) else {
            return Err(Error::Iceberg(invalid_partition_message(
                text,
                display,
                field.name(),
            )));
        };
        row.push(typed);
    }
    Ok(row)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn apply_user_text_schema(
    files: Vec<PathBuf>,
    partitions: DiscoveredPartitions,
    user_schema: Option<Vec<(String, String)>>,
    session_zone: &str,
) -> Result<AppliedTextSchema> {
    let Some(user) = user_schema else {
        let schema = text_schema_with_partitions(&partitions.fields);
        return Ok((schema, partitions.fields, partitions.values));
    };
    if partitions.fields.is_empty() {
        if user.len() != 1 || user[0].1.to_lowercase() != "string" {
            return Err(text_schema_error());
        }
        let schema = text_schema_with_data(&user[0].0, &[]);
        return Ok((schema, Vec::new(), HashMap::new()));
    }
    let mut lowered: HashMap<String, (String, String)> = HashMap::new();
    for (name, kind) in &user {
        lowered.insert(name.to_lowercase(), (name.clone(), kind.clone()));
    }
    let mut part_lowers = HashSet::new();
    for field in &partitions.fields {
        part_lowers.insert(field.name().to_lowercase());
    }
    let mut data: Vec<(String, String)> = Vec::new();
    for (name, kind) in &user {
        if !part_lowers.contains(&name.to_lowercase()) {
            data.push((name.clone(), kind.clone()));
        }
    }
    let data_name = if data.is_empty() {
        String::from("value")
    } else {
        if data.len() != 1 || data[0].1.to_lowercase() != "string" {
            return Err(text_schema_error());
        }
        data[0].0.clone()
    };
    let mut final_fields: Vec<Field> = Vec::with_capacity(partitions.fields.len());
    let mut final_types: Vec<DataType> = Vec::with_capacity(partitions.fields.len());
    let mut displays: Vec<Option<String>> = Vec::with_capacity(partitions.fields.len());
    for field in &partitions.fields {
        let key = field.name().to_lowercase();
        if let Some((_, kind)) = lowered.get(&key) {
            let normalized = kind.to_lowercase();
            let Some((arrow_type, display)) =
                user_partition_type(normalized.as_str(), session_zone)
            else {
                return Err(Error::Analysis(format!(
                    "text partition column `{}` has unsupported type `{kind}`",
                    field.name()
                )));
            };
            final_fields.push(Field::new(field.name(), arrow_type.clone(), true));
            final_types.push(arrow_type);
            displays.push(Some(display));
        } else {
            final_fields.push(field.clone());
            final_types.push(field.data_type().clone());
            displays.push(None);
        }
    }
    let zone: Tz = session_zone.parse().map_err(|error| {
        Error::Analysis(format!(
            "text read cannot parse session time zone {session_zone:?}: {error}"
        ))
    })?;
    let mut ordered_files = files;
    ordered_files.sort();
    let mut final_values: HashMap<PathBuf, Vec<PartitionValue>> = HashMap::new();
    for file in &ordered_files {
        let empty_values: Vec<PartitionValue> = Vec::new();
        let current = partitions.values.get(file).unwrap_or(&empty_values);
        let empty_raw: Vec<Option<String>> = Vec::new();
        let current_raw = partitions.raw.get(file).unwrap_or(&empty_raw);
        let row = overlay_partition_row(
            &final_fields,
            &final_types,
            &displays,
            current,
            current_raw,
            zone,
        )?;
        final_values.insert(file.clone(), row);
    }
    for file in partitions.values.keys() {
        if !final_values.contains_key(file) {
            let width = final_fields.len();
            final_values.insert(file.clone(), vec![PartitionValue::Null; width]);
        }
    }
    let schema = text_schema_with_data(&data_name, &final_fields);
    Ok((schema, final_fields, final_values))
}
