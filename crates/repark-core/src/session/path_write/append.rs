use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, SchemaRef};

use repark_common::{Error, Result};

use crate::session::ReparkSession;

use super::WriteFormat;
use super::writer_bool;

fn quoted_list(names: &[String]) -> String {
    format!(
        "[{}]",
        names
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn append_types_compatible(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }
    match (left, right) {
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        )
        | (
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
        ) => true,
        (DataType::List(left_field), DataType::List(right_field))
        | (DataType::LargeList(left_field), DataType::LargeList(right_field)) => {
            append_types_compatible(left_field.data_type(), right_field.data_type())
        }
        (
            DataType::FixedSizeList(left_field, left_size),
            DataType::FixedSizeList(right_field, right_size),
        ) => {
            left_size == right_size
                && append_types_compatible(left_field.data_type(), right_field.data_type())
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            left_fields.len() == right_fields.len()
                && left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .all(|(left, right)| {
                        left.name() == right.name()
                            && append_types_compatible(left.data_type(), right.data_type())
                    })
        }
        _ => false,
    }
}

fn strip_partition_names<'a>(names: &[&'a String], partition_by: &[String]) -> Vec<&'a String> {
    let folded: HashSet<String> = partition_by
        .iter()
        .map(|name| name.to_lowercase())
        .collect();
    names
        .iter()
        .filter(|name| !folded.contains(&name.to_lowercase()))
        .copied()
        .collect()
}

async fn validate_append_parquet(
    session: &ReparkSession,
    source_schema: &SchemaRef,
    partition_by: &[String],
    part_urls: &[String],
) -> Result<()> {
    let source_names: Vec<String> = source_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let source_refs: Vec<&String> = source_names.iter().collect();
    let expected = strip_partition_names(&source_refs, partition_by);
    let expected_by_case: HashMap<String, &String> = expected
        .iter()
        .map(|name| (name.to_lowercase(), *name))
        .collect();
    let source_type_by_case: HashMap<String, DataType> = source_schema
        .fields()
        .iter()
        .filter(|field| expected_by_case.contains_key(&field.name().to_lowercase()))
        .map(|field| (field.name().to_lowercase(), field.data_type().clone()))
        .collect();
    let mut dest_type_by_case: HashMap<String, DataType> = HashMap::new();
    let mut dest_name_by_case: HashMap<String, String> = HashMap::new();
    for part_url in part_urls {
        let destination = session.read_parquet(part_url).await?;
        let dest_names: Vec<String> = destination
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let dest_refs: Vec<&String> = dest_names.iter().collect();
        let stripped = strip_partition_names(&dest_refs, partition_by);
        let stripped_keys: HashSet<String> =
            stripped.iter().map(|name| name.to_lowercase()).collect();
        for field in destination.schema().fields() {
            let key = field.name().to_lowercase();
            if stripped_keys.contains(&key) && !dest_type_by_case.contains_key(&key) {
                dest_type_by_case.insert(key.clone(), field.data_type().clone());
                dest_name_by_case.insert(key, field.name().clone());
            }
        }
    }
    let dest_keys: HashSet<String> = dest_type_by_case.keys().cloned().collect();
    let expected_keys: HashSet<String> = expected_by_case.keys().cloned().collect();
    if dest_keys != expected_keys {
        let mut missing: Vec<String> = dest_keys
            .difference(&expected_keys)
            .map(|key| dest_name_by_case[key].clone())
            .collect();
        missing.sort();
        let mut extra: Vec<&String> = expected_keys
            .difference(&dest_keys)
            .map(|key| expected_by_case[key])
            .collect();
        extra.sort();
        let mut schema_names: Vec<String> = dest_name_by_case.values().cloned().collect();
        schema_names.sort();
        let missing_text = if missing.is_empty() {
            String::new()
        } else {
            format!("; missing from the DataFrame: {}", quoted_list(&missing))
        };
        let extra_text = if extra.is_empty() {
            String::new()
        } else {
            format!(
                "; extra in the DataFrame: {}",
                quoted_list(&extra.into_iter().cloned().collect::<Vec<_>>())
            )
        };
        return Err(Error::Analysis(format!(
            "cannot append DataFrame columns {} to path parquet schema {}: column sets \
             differ{missing_text}{extra_text} (path mode('append') refuses silent null-fill / \
             schema drift)",
            quoted_list(&source_names),
            quoted_list(&schema_names)
        )));
    }
    for (key, source_type) in &source_type_by_case {
        let dest_type = &dest_type_by_case[key];
        if !append_types_compatible(source_type, dest_type) {
            return Err(Error::Analysis(format!(
                "cannot append column '{}': type mismatch source={source_type} vs path={dest_type} \
                 (path mode('append') refuses type-incompatible merge)",
                expected_by_case[key]
            )));
        }
    }
    Ok(())
}

async fn validate_append_csv(
    session: &ReparkSession,
    source_schema: &SchemaRef,
    partition_by: &[String],
    options: &HashMap<String, String>,
    part_urls: &[String],
) -> Result<()> {
    let mut header_on = true;
    let mut read_options = HashMap::new();
    for (key, value) in options {
        let lowered = key.to_lowercase();
        if lowered == "header" {
            header_on = writer_bool(value);
        } else if lowered == "sep" || lowered == "delimiter" {
            read_options.insert("sep".to_string(), value.clone());
        } else if lowered == "quote" {
            read_options.insert("quote".to_string(), value.clone());
        } else if lowered == "escape" {
            read_options.insert("escape".to_string(), value.clone());
        }
    }
    if !header_on {
        return Ok(());
    }
    read_options.insert("header".to_string(), "true".to_string());
    let source_names: Vec<String> = source_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let source_refs: Vec<&String> = source_names.iter().collect();
    let expected = strip_partition_names(&source_refs, partition_by);
    let expected_keys: HashSet<String> = expected.iter().map(|name| name.to_lowercase()).collect();
    let Some(first) = part_urls.first() else {
        return Ok(());
    };
    let destination = session.read_csv(first, &read_options).await?;
    let dest_names: Vec<String> = destination
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let dest_refs: Vec<&String> = dest_names.iter().collect();
    let stripped = strip_partition_names(&dest_refs, partition_by);
    let dest_keys: HashSet<String> = stripped.iter().map(|name| name.to_lowercase()).collect();
    if dest_keys != expected_keys {
        return Err(Error::Analysis(format!(
            "cannot append DataFrame columns {} to path csv header {}: column sets differ \
             (path mode('append') refuses silent null-fill / schema drift)",
            quoted_list(&source_names),
            quoted_list(&stripped.into_iter().cloned().collect::<Vec<_>>())
        )));
    }
    Ok(())
}

async fn validate_append_json(
    session: &ReparkSession,
    source_schema: &SchemaRef,
    partition_by: &[String],
    part_urls: &[String],
) -> Result<()> {
    let source_names: Vec<String> = source_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let source_refs: Vec<&String> = source_names.iter().collect();
    let expected = strip_partition_names(&source_refs, partition_by);
    let expected_keys: HashSet<String> = expected.iter().map(|name| name.to_lowercase()).collect();
    let Some(first) = part_urls.first() else {
        return Ok(());
    };
    let destination = session.read_json(first, &HashMap::new()).await?;
    let dest_names: Vec<String> = destination
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let dest_refs: Vec<&String> = dest_names.iter().collect();
    let stripped = strip_partition_names(&dest_refs, partition_by);
    let dest_keys: HashSet<String> = stripped.iter().map(|name| name.to_lowercase()).collect();
    if dest_keys != expected_keys {
        let mut keys: Vec<String> = stripped.into_iter().cloned().collect();
        keys.sort();
        return Err(Error::Analysis(format!(
            "cannot append DataFrame columns {} to path json keys {}: column sets differ \
             (path mode('append') refuses silent null-fill / schema drift)",
            quoted_list(&source_names),
            quoted_list(&keys)
        )));
    }
    Ok(())
}

impl ReparkSession {
    pub(super) async fn validate_append(
        &self,
        format: &WriteFormat,
        source_schema: &SchemaRef,
        partition_by: &[String],
        options: &HashMap<String, String>,
        part_urls: &[String],
    ) -> Result<()> {
        match format {
            WriteFormat::Parquet => {
                validate_append_parquet(self, source_schema, partition_by, part_urls).await
            }
            WriteFormat::Csv => {
                validate_append_csv(self, source_schema, partition_by, options, part_urls).await
            }
            WriteFormat::Json => {
                validate_append_json(self, source_schema, partition_by, part_urls).await
            }
        }
    }
}
