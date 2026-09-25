use datafusion::arrow::datatypes::DataType;
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
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    use super::{incompatible_update_message, store_assignment_cast_sql};

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
