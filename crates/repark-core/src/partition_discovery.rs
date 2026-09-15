use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Date32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field};
use chrono::NaiveDate;
use datafusion::error::DataFusionError;

use crate::Error;

const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PartitionValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Date32(i32),
    Text(String),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoveredPartitions {
    pub(crate) fields: Vec<Field>,
    pub(crate) values: HashMap<PathBuf, Vec<PartitionValue>>,
}

fn unescape_partition_value(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let hex_value = |digit: u8| {
        if digit.is_ascii_digit() {
            digit - b'0'
        } else {
            digit.to_ascii_lowercase() - b'a' + 10
        }
    };
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        let high = bytes.get(index + 1).copied().filter(u8::is_ascii_hexdigit);
        let low = bytes.get(index + 2).copied().filter(u8::is_ascii_hexdigit);
        if byte == b'%'
            && let Some((high, low)) = high.zip(low)
        {
            out.push(hex_value(high) * 16 + hex_value(low));
            index += 3;
        } else {
            out.push(byte);
            index += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| raw.to_string())
}

pub(crate) fn partition_path_specs(root: &Path, file: &Path) -> Vec<(String, Option<String>)> {
    let relative = file.strip_prefix(root).unwrap_or(file);
    let Some(parent) = relative.parent() else {
        return Vec::new();
    };
    let mut specs = Vec::new();
    for component in parent.components() {
        let text = component.as_os_str().to_string_lossy();
        if let Some((key, raw)) = text.split_once('=')
            && !key.is_empty()
        {
            let value = if raw == HIVE_DEFAULT_PARTITION || raw.is_empty() {
                None
            } else {
                Some(unescape_partition_value(raw))
            };
            specs.push((key.to_string(), value));
        }
    }
    specs
}

fn looks_like_date(text: &str) -> bool {
    text.len() == 10 && NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()
}

fn infer_partition_type(candidates: &[&str]) -> DataType {
    if candidates.iter().all(|text| text.parse::<i32>().is_ok()) {
        DataType::Int32
    } else if candidates.iter().all(|text| text.parse::<i64>().is_ok()) {
        DataType::Int64
    } else if candidates.iter().all(|text| text.parse::<f64>().is_ok()) {
        DataType::Float64
    } else if candidates.iter().all(|text| looks_like_date(text)) {
        DataType::Date32
    } else {
        DataType::Utf8
    }
}

fn parse_partition_value(text: &str, data_type: &DataType) -> PartitionValue {
    match data_type {
        DataType::Int32 => text
            .parse::<i32>()
            .map_or(PartitionValue::Null, PartitionValue::Int32),
        DataType::Int64 => text
            .parse::<i64>()
            .map_or(PartitionValue::Null, PartitionValue::Int64),
        DataType::Float64 => text
            .parse::<f64>()
            .map_or(PartitionValue::Null, PartitionValue::Float64),
        DataType::Date32 => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()
            .and_then(|date| {
                i32::try_from(
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                        .num_days(),
                )
                .ok()
            })
            .map_or(PartitionValue::Null, PartitionValue::Date32),
        _ => PartitionValue::Text(text.to_string()),
    }
}

pub(crate) fn canonical_partition_text(value: &PartitionValue) -> Option<String> {
    match value {
        PartitionValue::Null => None,
        PartitionValue::Int32(number) => Some(number.to_string()),
        PartitionValue::Int64(number) => Some(number.to_string()),
        PartitionValue::Float64(number) => Some(number.to_string()),
        PartitionValue::Date32(days) => Some(days.to_string()),
        PartitionValue::Text(text) => Some(text.clone()),
    }
}

macro_rules! finish_partition_numbers {
    ($taken:expr, $builder:ty, $scalar:ty) => {{
        let mut out = <$builder>::new();
        for index in 0..$taken.len() {
            if $taken.is_null(index) {
                out.append_null();
            } else {
                let parsed: $scalar = $taken.value(index).parse().map_err(|_| {
                    DataFusionError::Execution(
                        "partition value does not match its inferred type".to_string(),
                    )
                })?;
                out.append_value(parsed);
            }
        }
        Arc::new(out.finish())
    }};
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn finish_partition_columns(
    builders: Vec<StringBuilder>,
    types: &[DataType],
    count: usize,
) -> Result<Vec<ArrayRef>, DataFusionError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(builders.len());
    for (mut builder, data_type) in builders.into_iter().zip(types.iter()) {
        let taken: StringArray = builder.finish();
        let taken = taken.slice(0, count);
        let column: ArrayRef = match data_type {
            DataType::Int32 => finish_partition_numbers!(taken, Int32Builder, i32),
            DataType::Int64 => finish_partition_numbers!(taken, Int64Builder, i64),
            DataType::Float64 => finish_partition_numbers!(taken, Float64Builder, f64),
            DataType::Date32 => finish_partition_numbers!(taken, Date32Builder, i32),
            _ => Arc::new(taken),
        };
        columns.push(column);
    }
    Ok(columns)
}

pub(crate) fn plan_partition_slots(
    plan: &[Option<usize>],
    partitions: usize,
    types: &[DataType],
) -> (Vec<Option<usize>>, Vec<DataType>) {
    let mut slots: Vec<Option<usize>> = vec![None; partitions];
    let mut included = Vec::new();
    for column in plan {
        if let Some(provider) = column
            && slots[*provider].is_none()
        {
            slots[*provider] = Some(included.len());
            included.push(types[*provider].clone());
        }
    }
    (slots, included)
}

pub(crate) fn push_partition_row(
    parts: &mut [StringBuilder],
    slots: &[Option<usize>],
    current: &[Option<String>],
) {
    for (provider, slot) in slots.iter().enumerate() {
        if let Some(index) = slot {
            match current.get(provider).and_then(|value| value.as_ref()) {
                None => parts[*index].append_null(),
                Some(text) => parts[*index].append_value(text),
            }
        }
    }
}

pub(crate) struct TextRowSink<'a> {
    pub(crate) value: Option<&'a mut StringBuilder>,
    pub(crate) parts: &'a mut [StringBuilder],
    pub(crate) slots: &'a [Option<usize>],
    pub(crate) current: &'a [Option<String>],
    pub(crate) blank: &'a mut usize,
}

pub(crate) fn emit_text_row(
    sink: &mut TextRowSink<'_>,
    piece: &[u8],
    limit: Option<usize>,
    emitted: usize,
) -> bool {
    let pending = if let Some(builder) = sink.value.as_ref() {
        builder.len()
    } else if let Some(builder) = sink.parts.first() {
        builder.len()
    } else {
        *sink.blank
    };
    if limit.is_some_and(|max| emitted + pending >= max) {
        return false;
    }
    match sink.value.as_mut() {
        Some(builder) => builder.append_value(&String::from_utf8_lossy(piece)),
        None if sink.parts.is_empty() => *sink.blank += 1,
        None => {}
    }
    push_partition_row(sink.parts, sink.slots, sink.current);
    true
}

pub(crate) fn order_batch_columns(
    plan: &[Option<usize>],
    value: Option<&ArrayRef>,
    finished: &[ArrayRef],
    slots: &[Option<usize>],
) -> Result<Vec<ArrayRef>, DataFusionError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(plan.len());
    for column in plan {
        match column {
            None => columns.push(value.cloned().ok_or_else(|| {
                DataFusionError::Internal("text scan lost its value column".to_string())
            })?),
            Some(provider) => {
                let slot = slots[*provider].ok_or_else(|| {
                    DataFusionError::Internal("text scan lost its partition column".to_string())
                })?;
                columns.push(Arc::clone(&finished[slot]));
            }
        }
    }
    Ok(columns)
}

pub(crate) fn user_partition_type(name: &str) -> Option<(DataType, &'static str)> {
    match name {
        "string" => Some((DataType::Utf8, "STRING")),
        "int" | "integer" => Some((DataType::Int32, "INT")),
        "bigint" | "long" => Some((DataType::Int64, "BIGINT")),
        "double" => Some((DataType::Float64, "DOUBLE")),
        "date" => Some((DataType::Date32, "DATE")),
        _ => None,
    }
}

pub(crate) fn cast_raw_partition_value(raw: &str, data_type: &DataType) -> Option<PartitionValue> {
    match data_type {
        DataType::Int32 => raw.parse::<i32>().ok().map(PartitionValue::Int32),
        DataType::Int64 => raw.parse::<i64>().ok().map(PartitionValue::Int64),
        DataType::Float64 => raw.parse::<f64>().ok().map(PartitionValue::Float64),
        DataType::Date32 => NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .ok()
            .and_then(|date| {
                i32::try_from(
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                        .num_days(),
                )
                .ok()
            })
            .map(PartitionValue::Date32),
        _ => Some(PartitionValue::Text(raw.to_string())),
    }
}

pub(crate) fn invalid_partition_message(raw: &str, display: &str, column: &str) -> String {
    format!(
        "[INVALID_PARTITION_VALUE] Failed to cast value '{raw}' to data type \"{display}\" for partition column `{column}`. Ensure the value matches the expected data type for this partition column. SQLSTATE: 42846"
    )
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn discover_partitions(
    root: &Path,
    files: &[PathBuf],
) -> Result<DiscoveredPartitions, Error> {
    let mut by_depth: HashMap<usize, Vec<Vec<String>>> = HashMap::new();
    let mut dir_by_depth: HashMap<usize, Vec<PathBuf>> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    let mut raws: HashMap<String, Vec<String>> = HashMap::new();
    let mut per_file: HashMap<PathBuf, HashMap<String, Option<String>>> = HashMap::new();
    for file in files {
        let mut seen: HashMap<String, Option<String>> = HashMap::new();
        let mut keys: Vec<String> = Vec::new();
        for (key, value) in partition_path_specs(root, file) {
            keys.push(key.clone());
            if !raws.contains_key(&key) {
                names.push(key.clone());
                raws.insert(key.clone(), Vec::new());
            }
            if let Some(text) = &value
                && let Some(texts) = raws.get_mut(&key)
            {
                texts.push(text.clone());
            }
            seen.insert(key, value);
        }
        if !keys.is_empty() {
            let depth = keys.len();
            let entry = by_depth.entry(depth).or_default();
            if !entry.contains(&keys) {
                entry.push(keys);
                dir_by_depth.entry(depth).or_default().push(
                    file.parent()
                        .map_or_else(|| root.to_path_buf(), Path::to_path_buf),
                );
            }
        }
        per_file.insert(file.clone(), seen);
    }
    let mut bad: Option<(Vec<Vec<String>>, Vec<PathBuf>)> = None;
    for (depth, lists) in &by_depth {
        if lists.len() > 1 {
            let dirs = dir_by_depth.get(depth).cloned().unwrap_or_default();
            if bad.is_none() {
                bad = Some((lists.clone(), dirs));
            }
        }
    }
    if let Some((lists, dirs)) = bad {
        let mut message = String::from(
            "[CONFLICTING_PARTITION_COLUMN_NAMES] Conflicting partition column names detected:\n\n",
        );
        for (index, keys) in lists.iter().enumerate() {
            let _ = writeln!(
                message,
                "\tPartition column name list #{index}: {}",
                keys.join(", ")
            );
        }
        message.push_str(
            "\nFor partitioned table directories, data files should only live in leaf directories.\nAnd directories at the same level should have the same partition column name.\nPlease check the following directories for unexpected files or inconsistent partition column names:\n\n",
        );
        for (index, dir) in dirs.iter().enumerate() {
            if index > 0 {
                message.push('\n');
            }
            let _ = write!(message, "\tfile:{}", dir.display());
        }
        message.push_str(" SQLSTATE: KD009");
        return Err(Error::Iceberg(message));
    }
    let mut fields = Vec::with_capacity(names.len());
    let mut types: HashMap<String, DataType> = HashMap::new();
    for name in &names {
        let empty = Vec::new();
        let candidates: Vec<&str> = raws
            .get(name)
            .unwrap_or(&empty)
            .iter()
            .map(String::as_str)
            .collect();
        let data_type = if candidates.is_empty() {
            DataType::Utf8
        } else {
            infer_partition_type(&candidates)
        };
        fields.push(Field::new(name, data_type.clone(), true));
        types.insert(name.clone(), data_type);
    }
    let mut values: HashMap<PathBuf, Vec<PartitionValue>> = HashMap::new();
    for file in files {
        let empty = HashMap::new();
        let seen = per_file.get(file).unwrap_or(&empty);
        let mut row = Vec::with_capacity(names.len());
        for name in &names {
            let value = match seen.get(name).and_then(|slot| slot.as_ref()) {
                None => PartitionValue::Null,
                Some(text) => parse_partition_value(text, &types[name]),
            };
            row.push(value);
        }
        values.insert(file.clone(), row);
    }
    Ok(DiscoveredPartitions { fields, values })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(root: &Path, segments: &[&str], name: &str) -> PathBuf {
        let mut path = root.to_path_buf();
        for segment in segments {
            path.push(segment);
        }
        path.push(name);
        path
    }

    #[test]
    fn partition_unescape_decodes_hex_and_keeps_broken() {
        assert_eq!(unescape_partition_value("a%2Fb"), "a/b");
        assert_eq!(unescape_partition_value("50%25"), "50%");
        assert_eq!(unescape_partition_value("c%3ad"), "c:d");
        assert_eq!(unescape_partition_value("100%"), "100%");
        assert_eq!(unescape_partition_value("x%zz"), "x%zz");
    }

    #[test]
    fn partition_discovery_reads_keys_in_directory_order() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["d=2024-01-02", "t=a"], "part-00000.txt"),
            leaf(&root, &["d=2024-01-03", "t=b"], "part-00000.txt"),
        ];
        let discovered = discover_partitions(&root, &files).unwrap();
        assert_eq!(
            discovered
                .fields
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            vec![
                (String::from("d"), DataType::Date32),
                (String::from("t"), DataType::Utf8),
            ]
        );
        assert_eq!(
            discovered.values[&files[0]],
            vec![
                PartitionValue::Date32(19724),
                PartitionValue::Text(String::from("a")),
            ]
        );
    }

    #[test]
    fn partition_discovery_maps_default_to_null_and_infers_types() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=__HIVE_DEFAULT_PARTITION__"], "part-00000.txt"),
            leaf(&root, &["k=7"], "part-00001.txt"),
            leaf(&root, &["n=1.50", "b=true"], "part-00000.txt"),
        ];
        let discovered = discover_partitions(&root, &files).unwrap();
        assert_eq!(discovered.values[&files[0]][0], PartitionValue::Null);
        assert_eq!(discovered.values[&files[1]][0], PartitionValue::Int32(7));
        let names: Vec<String> = discovered
            .fields
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(
            names,
            vec![String::from("k"), String::from("n"), String::from("b")]
        );
        assert_eq!(
            discovered.values[&files[2]][1],
            PartitionValue::Float64(1.5)
        );
        assert_eq!(
            discovered.values[&files[2]][2],
            PartitionValue::Text(String::from("true"))
        );
    }

    #[test]
    fn partition_discovery_ignores_leaf_paths_without_base() {
        let root = PathBuf::from("/root/k=x");
        let files = vec![leaf(&root, &[], "part-00000.txt")];
        let discovered = discover_partitions(&root, &files).unwrap();
        assert!(discovered.fields.is_empty());
        assert_eq!(discovered.values[&files[0]], Vec::new());
    }

    #[test]
    fn partition_discovery_prefers_bigint_over_double_for_long() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=3000000000"], "part-00000.txt"),
            leaf(&root, &["k=9"], "part-00001.txt"),
        ];
        let discovered = discover_partitions(&root, &files).unwrap();
        assert_eq!(discovered.fields[0].data_type(), &DataType::Int64);
        assert_eq!(
            discovered.values[&files[0]][0],
            PartitionValue::Int64(3_000_000_000)
        );
    }

    #[test]
    fn partition_discovery_refuses_conflicting_names_at_same_depth() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=x"], "part-00000.txt"),
            leaf(&root, &["n=y"], "part-00000.txt"),
        ];
        let error = discover_partitions(&root, &files).unwrap_err();
        let text = error.to_string();
        assert!(text.starts_with("[CONFLICTING_PARTITION_COLUMN_NAMES]"));
        assert!(text.contains("Partition column name list #0: k"));
        assert!(text.contains("Partition column name list #1: n"));
        assert!(text.contains("SQLSTATE: KD009"));
    }

    #[test]
    fn partition_user_type_maps_probe_shapes() {
        assert_eq!(
            user_partition_type("string"),
            Some((DataType::Utf8, "STRING"))
        );
        assert_eq!(user_partition_type("int"), Some((DataType::Int32, "INT")));
        assert_eq!(
            user_partition_type("bigint"),
            Some((DataType::Int64, "BIGINT"))
        );
        assert_eq!(
            user_partition_type("double"),
            Some((DataType::Float64, "DOUBLE"))
        );
        assert_eq!(
            user_partition_type("date"),
            Some((DataType::Date32, "DATE"))
        );
        assert_eq!(user_partition_type("boolean"), None);
        assert_eq!(
            cast_raw_partition_value("007", &DataType::Int32),
            Some(PartitionValue::Int32(7))
        );
        assert_eq!(
            cast_raw_partition_value("+7", &DataType::Int32),
            Some(PartitionValue::Int32(7))
        );
        assert_eq!(cast_raw_partition_value("y", &DataType::Int32), None);
        assert_eq!(
            invalid_partition_message("y", "INT", "k"),
            "[INVALID_PARTITION_VALUE] Failed to cast value 'y' to data type \"INT\" for partition column `k`. Ensure the value matches the expected data type for this partition column. SQLSTATE: 42846"
        );
    }
}
