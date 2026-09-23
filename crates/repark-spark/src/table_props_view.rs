use iceberg::spec::{
    FormatVersion, NullOrder, Schema as IcebergSchema, SortDirection, SortField, SortOrder,
    TableMetadata, Transform,
};
use repark_core::prop_key_is_secret;

use crate::describe_show::REDACTION_REPLACEMENT_TEXT;

const ICEBERG_RESERVED_PROPERTIES: [&str; 7] = [
    "provider",
    "format",
    "current-snapshot-id",
    "location",
    "format-version",
    "sort-order",
    "identifier-fields",
];

const SPARK_RESERVED_PROPERTIES: [&str; 6] = [
    "owner",
    "comment",
    "location",
    "provider",
    "external",
    "is_managed_location",
];

const JAVA_HASH_SET_INITIAL_CAPACITY: usize = 16;

pub(crate) fn spark_table_properties(metadata: &TableMetadata) -> Vec<(String, String)> {
    let stored = metadata.properties();
    let schema = metadata.current_schema();
    let file_format = stored
        .get("write.format.default")
        .map_or("parquet", String::as_str);
    let mut pairs = vec![
        ("format".to_string(), format!("iceberg/{file_format}")),
        (
            "current-snapshot-id".to_string(),
            metadata
                .current_snapshot_id()
                .map_or_else(|| "none".to_string(), |id| id.to_string()),
        ),
        (
            "format-version".to_string(),
            format_version_number(metadata.format_version()).to_string(),
        ),
    ];
    let sort_order = metadata.default_sort_order();
    if !sort_order.is_unsorted() {
        pairs.push((
            "sort-order".to_string(),
            describe_sort_order(schema, sort_order),
        ));
    }
    let identifier_fields = identifier_field_names(schema);
    if !identifier_fields.is_empty() {
        pairs.push((
            "identifier-fields".to_string(),
            format!("[{}]", identifier_fields.join(",")),
        ));
    }
    pairs.extend(
        stored
            .iter()
            .filter(|(key, _)| !is_reserved_property(key))
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    for (key, value) in &mut pairs {
        if prop_key_is_secret(key) {
            *value = REDACTION_REPLACEMENT_TEXT.to_string();
        }
    }
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    pairs
}

fn is_reserved_property(key: &str) -> bool {
    ICEBERG_RESERVED_PROPERTIES.contains(&key) || SPARK_RESERVED_PROPERTIES.contains(&key)
}

fn format_version_number(version: FormatVersion) -> u8 {
    match version {
        FormatVersion::V1 => 1,
        FormatVersion::V2 => 2,
        FormatVersion::V3 => 3,
    }
}

fn describe_sort_order(schema: &IcebergSchema, order: &SortOrder) -> String {
    order
        .fields
        .iter()
        .map(|field| describe_sort_field(schema, field))
        .collect::<Vec<String>>()
        .join(", ")
}

fn describe_sort_field(schema: &IcebergSchema, field: &SortField) -> String {
    let source = source_column_name(schema, field.source_id);
    let term = match &field.transform {
        Transform::Identity => source,
        Transform::Bucket(buckets) => format!("bucket({buckets}, {source})"),
        Transform::Truncate(width) => format!("truncate({source}, {width})"),
        Transform::Year => format!("years({source})"),
        Transform::Month => format!("months({source})"),
        Transform::Day => format!("days({source})"),
        Transform::Hour => format!("hours({source})"),
        Transform::Void => format!("void({source})"),
        Transform::Unknown => format!("unknown({source})"),
    };
    let direction = match field.direction {
        SortDirection::Ascending => "ASC",
        SortDirection::Descending => "DESC",
    };
    let nulls = match field.null_order {
        NullOrder::First => "NULLS FIRST",
        NullOrder::Last => "NULLS LAST",
    };
    format!("{term} {direction} {nulls}")
}

fn source_column_name(schema: &IcebergSchema, source_id: i32) -> String {
    schema
        .name_by_field_id(source_id)
        .map_or_else(|| source_id.to_string(), str::to_string)
}

fn identifier_field_names(schema: &IcebergSchema) -> Vec<String> {
    let mut ids: Vec<i32> = schema.identifier_field_ids().collect();
    ids.sort_unstable();
    java_hash_set_order(
        ids.into_iter()
            .map(|id| source_column_name(schema, id))
            .collect(),
    )
}

fn java_hash_set_order(names: Vec<String>) -> Vec<String> {
    let mut capacity = JAVA_HASH_SET_INITIAL_CAPACITY;
    while names.len().saturating_mul(4) > capacity.saturating_mul(3) {
        capacity = capacity.saturating_mul(2);
    }
    let mut placed: Vec<(usize, usize, String)> = names
        .into_iter()
        .enumerate()
        .map(|(inserted, name)| (java_hash_bucket(&name, capacity), inserted, name))
        .collect();
    placed.sort_unstable_by_key(|(bucket, inserted, _)| (*bucket, *inserted));
    placed.into_iter().map(|(_, _, name)| name).collect()
}

fn java_hash_bucket(text: &str, capacity: usize) -> usize {
    let hash = text
        .encode_utf16()
        .fold(0_i32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        })
        .cast_unsigned();
    let spread = hash ^ (hash >> 16);
    usize::try_from(spread).unwrap_or(usize::MAX) & (capacity - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_hash_set_order_matches_the_measured_identifier_fields() {
        let names = ["id", "zz", "a"].map(str::to_string).to_vec();
        assert_eq!(java_hash_set_order(names), vec!["zz", "a", "id"]);
        let names = ["id", "data"].map(str::to_string).to_vec();
        assert_eq!(java_hash_set_order(names), vec!["data", "id"]);
    }

    #[test]
    fn java_hash_set_order_grows_past_the_load_factor() {
        let names: Vec<String> = ('h'..='t').map(String::from).collect();
        assert_eq!(java_hash_set_order(names.clone()), names);
        let twelve: Vec<String> = names.into_iter().take(12).collect();
        let ordered = java_hash_set_order(twelve);
        assert_eq!(
            ordered,
            ["p", "q", "r", "s", "h", "i", "j", "k", "l", "m", "n", "o"]
                .map(str::to_string)
                .to_vec()
        );
    }

    #[test]
    fn reserved_keys_cover_both_the_iceberg_and_spark_sets() {
        for key in [
            "owner",
            "comment",
            "provider",
            "location",
            "current-snapshot-id",
            "format-version",
            "identifier-fields",
            "sort-order",
            "format",
            "external",
            "is_managed_location",
        ] {
            assert!(is_reserved_property(key), "{key}");
        }
        assert!(!is_reserved_property("write.format.default"));
    }
}
