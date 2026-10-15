use std::borrow::Cow;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use repark_common::spark_error;

use super::store_assign::{
    ansi_store_assignable, normalize_for_assignment, without_field_metadata,
};
use super::struct_pairing::{StorePairs, StructPairing, pair_store_value_at, store_pair_sql};

#[must_use]
pub fn store_assignment_cast_sql(expr: &str, target: &DataType) -> String {
    paired_assignment_cast_sql(expr, target, StructPairing::by_name(false), "``")
}

#[must_use]
pub fn paired_assignment_cast_sql(
    expr: &str,
    target: &DataType,
    pairing: StructPairing,
    root: &str,
) -> String {
    let paired = store_pair_sql(expr, target, pairing, "``", root);
    let expr = paired.as_deref().unwrap_or(expr);
    match super::ntz_store::store_kernel_sql(expr, target) {
        Some(stored) => stored,
        None => nested_leaf_cast_sql(expr, target),
    }
}

#[must_use]
pub fn paired_nested_cast_sql(
    expr: &str,
    target: &DataType,
    pairing: StructPairing,
    root: &str,
) -> String {
    let paired = store_pair_sql(expr, target, pairing, "``", root);
    nested_leaf_cast_sql(paired.as_deref().unwrap_or(expr), target)
}

#[must_use]
pub fn nested_leaf_cast_sql(expr: &str, target: &DataType) -> String {
    if let Some(wall) = super::ntz_store::wall_cast_sql(expr, target) {
        return wall;
    }
    let type_name = without_field_metadata(target)
        .to_string()
        .replace('\'', "''");
    if super::ntz_store::is_ltz_instant_target(target) {
        let instant = super::ntz_store::ltz_instant_cast_sql(expr);
        return format!("arrow_cast(({instant}), '{type_name}')");
    }
    format!("arrow_cast(({expr}), '{type_name}')")
}

#[must_use]
pub fn incompatible_update_message(
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    incompatible_message(table, column, source, target, false)
}

#[must_use]
pub fn incompatible_store_message(
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    incompatible_message(table, column, source, target, true)
}

fn incompatible_message(
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
    name_decimals: bool,
) -> Option<String> {
    let source = normalize_for_assignment(source);
    let target = normalize_for_assignment(target);
    if ansi_store_assignable(source, target) {
        return None;
    }
    if super::struct_pairing::holds_struct_type(source)
        || super::struct_pairing::holds_struct_type(target)
        || matches!(source, DataType::Map(_, _))
        || matches!(target, DataType::Map(_, _))
    {
        return incompatible_nested_message(column, source, target);
    }
    let from = store_type_name(source, name_decimals)?;
    let to = store_type_name(target, name_decimals)?;
    Some(spark_error::message(
        spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
        &[
            ("tableName", table),
            ("columnName", column),
            ("fromType", &from),
            ("toType", &to),
        ],
    ))
}

fn store_type_name(data_type: &DataType, name_decimals: bool) -> Option<Cow<'static, str>> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale)
            if name_decimals =>
        {
            Some(Cow::Owned(format!("DECIMAL({precision},{scale})")))
        }
        other => spark_update_type_name(other).map(Cow::Borrowed),
    }
}

#[must_use]
pub fn incompatible_nested_message(
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    incompatible_paired_message(StructPairing::by_name(false), column, source, target)
}

#[must_use]
pub fn incompatible_paired_message(
    pairing: StructPairing,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    let pairs = match pair_store_value_at(pairing, "``", column, source, target) {
        Ok(pairs) => pairs,
        Err(DataFusionError::Plan(text)) => return Some(text),
        Err(other) => return Some(other.to_string()),
    };
    unassignable_leaf_message("``", &pairs)
}

#[must_use]
pub fn unassignable_leaf_message(table: &str, pairs: &StorePairs) -> Option<String> {
    pairs.leaves().iter().find_map(|leaf| {
        let from = normalize_for_assignment(&leaf.source);
        let to = normalize_for_assignment(&leaf.target);
        if ansi_store_assignable(from, to) {
            return None;
        }
        Some(spark_error::message(
            spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
            &[
                ("tableName", table),
                ("columnName", &leaf.path),
                ("fromType", &spark_leaf_type_name(from)?),
                ("toType", &spark_leaf_type_name(to)?),
            ],
        ))
    })
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_unassignable_leaves(table: &str, pairs: &StorePairs) -> Result<()> {
    match unassignable_leaf_message(table, pairs) {
        Some(text) => Err(DataFusionError::Plan(text)),
        None => Ok(()),
    }
}

fn spark_leaf_type_name(data_type: &DataType) -> Option<Cow<'static, str>> {
    if let Some(flat) = spark_update_type_name(data_type) {
        return Some(Cow::Borrowed(flat));
    }
    spark_catalog_string(data_type).map(|text| Cow::Owned(text.to_uppercase()))
}

fn spark_quote_if_needed(name: &str) -> String {
    let plain = !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.chars().all(|c| c.is_ascii_digit());
    if plain {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

fn spark_catalog_string(data_type: &DataType) -> Option<String> {
    match data_type {
        DataType::Struct(fields) => {
            let members = fields
                .iter()
                .map(|field| {
                    Some(format!(
                        "{}:{}",
                        spark_quote_if_needed(field.name()),
                        spark_catalog_string(field.data_type())?
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(format!("struct<{}>", members.join(",")))
        }
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::ListView(element)
        | DataType::LargeListView(element)
        | DataType::FixedSizeList(element, _) => Some(format!(
            "array<{}>",
            spark_catalog_string(element.data_type())?
        )),
        DataType::Map(entries, _) => {
            let DataType::Struct(pair) = entries.data_type() else {
                return None;
            };
            let [key, value] = pair.iter().collect::<Vec<_>>()[..] else {
                return None;
            };
            Some(format!(
                "map<{},{}>",
                spark_catalog_string(key.data_type())?,
                spark_catalog_string(value.data_type())?
            ))
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => Some(format!("decimal({precision},{scale})")),
        DataType::Dictionary(_, values) => spark_catalog_string(values),
        other => spark_update_type_name(other).map(str::to_lowercase),
    }
}

fn spark_update_type_name(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some("STRING"),
        DataType::Int8 => Some("TINYINT"),
        DataType::Int16 => Some("SMALLINT"),
        DataType::Int32 => Some("INT"),
        DataType::Int64 => Some("BIGINT"),
        DataType::Float32 => Some("FLOAT"),
        DataType::Float64 => Some("DOUBLE"),
        DataType::Boolean => Some("BOOLEAN"),
        DataType::Date32 | DataType::Date64 => Some("DATE"),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some("TIMESTAMP_NTZ"),
        DataType::Timestamp(_, _) => Some("TIMESTAMP"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Some("BINARY"),
        DataType::Null => Some("VOID"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    use super::{
        incompatible_nested_message, incompatible_update_message, store_assignment_cast_sql,
    };

    fn map_of(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key, false),
                        Field::new("value", value, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    fn struct_of(names: &[&str]) -> DataType {
        DataType::Struct(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int32, true))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    #[test]
    fn a_map_key_or_value_that_cannot_store_assign_names_its_path_as_spark_does() {
        let key = incompatible_update_message(
            "`sc`.`ns`.`t`",
            "`c`",
            &map_of(DataType::Utf8, DataType::Utf8),
            &map_of(DataType::Int32, DataType::Utf8),
        );
        assert_eq!(
            key.as_deref(),
            Some(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data \
                 for the table ``: Cannot safely cast `c`.`key` \"STRING\" to \"INT\". SQLSTATE: KD000"
            )
        );
        let value = incompatible_update_message(
            "`sc`.`ns`.`t`",
            "`c`",
            &map_of(DataType::Utf8, DataType::Date32),
            &map_of(DataType::Utf8, DataType::Int32),
        );
        assert_eq!(
            value.as_deref(),
            Some(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data \
                 for the table ``: Cannot safely cast `c`.`value` \"DATE\" to \"INT\". SQLSTATE: KD000"
            )
        );
    }

    #[test]
    fn a_map_struct_value_missing_a_field_cannot_find_its_data() {
        let text = incompatible_nested_message(
            "`c`",
            &map_of(DataType::Utf8, struct_of(&["a"])),
            &map_of(DataType::Utf8, struct_of(&["a", "b"])),
        );
        assert_eq!(
            text.as_deref(),
            Some(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for \
                 the table ``: Cannot find data for the output column `c`.`value`.`b`. SQLSTATE: \
                 KD000"
            )
        );
    }

    #[test]
    fn a_map_whose_key_and_value_store_assign_has_no_message() {
        assert!(
            incompatible_update_message(
                "``",
                "`c`",
                &map_of(DataType::Utf8, DataType::Int64),
                &map_of(DataType::Utf8, DataType::Int32),
            )
            .is_none()
        );
        assert!(
            incompatible_nested_message(
                "`c`",
                &map_of(DataType::Int32, struct_of(&["a", "b"])),
                &map_of(DataType::Utf8, struct_of(&["b", "a"])),
            )
            .is_none()
        );
    }

    #[test]
    fn the_cast_names_the_target_type_without_field_ids() {
        let target = DataType::Struct(
            vec![Field::new("a", DataType::Int32, true).with_metadata(
                std::collections::HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    "3".to_string(),
                )]),
            )]
            .into(),
        );
        assert_eq!(
            store_assignment_cast_sql("s.v", &target),
            "arrow_cast((__repark_store_pair__((s.v), 'Struct(\"a\": Int32)', 'by_name', '``', \
             '``')), 'Struct(\"a\": Int32)')"
        );
        assert_eq!(
            store_assignment_cast_sql("'it''s'", &DataType::Utf8),
            "arrow_cast(('it''s'), 'Utf8')"
        );
    }

    #[test]
    fn a_wall_target_casts_through_the_kernel_of_its_unit() {
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let zoned_nanos = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert_eq!(
            store_assignment_cast_sql("s.v", &micros),
            "__repark_cast_timestamp_ntz__((s.v))"
        );
        assert_eq!(
            store_assignment_cast_sql("s.v", &nanos),
            "__repark_cast_timestamp_ns__((s.v))"
        );
        assert_eq!(
            store_assignment_cast_sql("s.v", &zoned_nanos),
            "__repark_cast_timestamptz_ns__((s.v))"
        );
        assert_eq!(
            super::nested_leaf_cast_sql("s.v", &zoned_nanos),
            "arrow_cast((s.v), 'Timestamp(ns, \"UTC\")')"
        );
        assert_eq!(
            super::nested_leaf_cast_sql("s.v", &micros),
            "__repark_cast_timestamp_ntz__((s.v))"
        );
    }

    #[test]
    fn utf8_to_int64_stamps_the_condition() {
        let text = incompatible_update_message(
            "`ice`.`sales`.`t`",
            "`id`",
            &DataType::Utf8,
            &DataType::Int64,
        )
        .expect("Utf8 to Int64 must stamp");
        assert!(
            text.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
            "{text}"
        );
        assert!(
            text.contains("Cannot write incompatible data for the table"),
            "{text}"
        );
        assert!(text.contains("SQLSTATE: KD000"), "{text}");
        assert!(
            text.contains("Cannot safely cast `id` \"STRING\" to \"BIGINT\""),
            "{text}"
        );
    }

    #[test]
    fn int32_to_int64_returns_none() {
        assert!(
            incompatible_update_message(
                "`ice`.`sales`.`t`",
                "`id`",
                &DataType::Int32,
                &DataType::Int64
            )
            .is_none()
        );
    }

    #[test]
    fn type_outside_the_name_list_returns_none() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert!(
            incompatible_update_message("`ice`.`sales`.`t`", "`id`", &list, &DataType::Int64)
                .is_none()
        );
    }
}
