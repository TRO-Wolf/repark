use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use repark_common::spark_error;

use super::store_assign::{
    ansi_store_assignable, normalize_for_assignment, without_field_metadata,
};

#[must_use]
pub fn store_assignment_cast_sql(expr: &str, target: &DataType) -> String {
    let type_name = without_field_metadata(target)
        .to_string()
        .replace('\'', "''");
    format!("arrow_cast(({expr}), '{type_name}')")
}

#[must_use]
pub fn incompatible_update_message(
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    let source = normalize_for_assignment(source);
    let target = normalize_for_assignment(target);
    if ansi_store_assignable(source, target) {
        return None;
    }
    if matches!(source, DataType::Map(_, _)) || matches!(target, DataType::Map(_, _)) {
        return incompatible_nested_message(column, source, target);
    }
    let from = spark_update_type_name(source)?;
    let to = spark_update_type_name(target)?;
    Some(spark_error::message(
        spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
        &[
            ("tableName", table),
            ("columnName", column),
            ("fromType", from),
            ("toType", to),
        ],
    ))
}

#[must_use]
pub fn incompatible_nested_message(
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    let source = normalize_for_assignment(source);
    let target = normalize_for_assignment(target);
    match (source, target) {
        (DataType::Map(from, _), DataType::Map(to, _)) => {
            let (DataType::Struct(from), DataType::Struct(to)) = (from.data_type(), to.data_type())
            else {
                return None;
            };
            from.iter().zip(to.iter()).zip(["key", "value"]).find_map(
                |((source, target), label)| {
                    incompatible_nested_message(
                        &format!("{column}.`{label}`"),
                        source.data_type(),
                        target.data_type(),
                    )
                },
            )
        }
        (DataType::Struct(from), DataType::Struct(to)) => to.iter().find_map(|target| {
            let child = format!("{column}.`{}`", target.name());
            match struct_member(from, target.name()) {
                Some(source) => {
                    incompatible_nested_message(&child, source.data_type(), target.data_type())
                }
                None => Some(format!(
                    "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible \
                     data for the table ``: Cannot find data for the output column {child}. \
                     SQLSTATE: KD000"
                )),
            }
        }),
        _ if ansi_store_assignable(source, target) => None,
        _ => Some(spark_error::message(
            spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
            &[
                ("tableName", "``"),
                ("columnName", column),
                ("fromType", spark_update_type_name(source)?),
                ("toType", spark_update_type_name(target)?),
            ],
        )),
    }
}

fn struct_member<'a>(fields: &'a Fields, name: &str) -> Option<&'a Arc<Field>> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .or_else(|| {
            fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(name))
        })
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
        DataType::Timestamp(_, _) => Some("TIMESTAMP"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Some("BINARY"),
        DataType::Null => Some("VOID"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field};
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
            "arrow_cast((s.v), 'Struct(\"a\": Int32)')"
        );
        assert_eq!(
            store_assignment_cast_sql("'it''s'", &DataType::Utf8),
            "arrow_cast(('it''s'), 'Utf8')"
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
