use datafusion::arrow::datatypes::DataType as ArrowDataType;

use crate::type_table::{ArrowNameSurface, arrow_name_at_depth};

#[must_use]
pub fn spark_ddl_type_name(data_type: &ArrowDataType) -> String {
    spark_ddl_type_name_at_depth(data_type, 0)
}

#[must_use]
pub fn spark_ddl_type_name_at_depth(data_type: &ArrowDataType, depth: usize) -> String {
    arrow_name_at_depth(data_type, ArrowNameSurface::Describe, depth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_table::{SPARK_TYPE_NAME_DEPTH_FALLBACK, SPARK_TYPE_NAME_MAX_DEPTH};
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn spark_ddl_type_name_spells_describe_primitives() {
        let cases = vec![
            (ArrowDataType::Int8, "tinyint"),
            (ArrowDataType::Int16, "smallint"),
            (ArrowDataType::Int32, "int"),
            (ArrowDataType::Int64, "bigint"),
            (ArrowDataType::UInt64, "bigint"),
            (ArrowDataType::Float32, "float"),
            (ArrowDataType::Float64, "double"),
            (ArrowDataType::Boolean, "boolean"),
            (ArrowDataType::Utf8, "string"),
            (ArrowDataType::Binary, "binary"),
            (ArrowDataType::Date32, "date"),
            (
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                "timestamp_ntz",
            ),
            (
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "timestamp",
            ),
            (ArrowDataType::Decimal128(10, 2), "decimal(10,2)"),
        ];
        for (data_type, expected) in cases {
            assert_eq!(spark_ddl_type_name(&data_type), expected);
        }
    }

    #[test]
    fn spark_ddl_type_name_spells_nested_types() {
        let list = ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true)));
        assert_eq!(spark_ddl_type_name(&list), "array<bigint>");
        let structured = ArrowDataType::Struct(
            vec![
                Field::new("ts_day", ArrowDataType::Date32, false),
                Field::new("id", ArrowDataType::Int64, true),
            ]
            .into(),
        );
        assert_eq!(
            spark_ddl_type_name(&structured),
            "struct<ts_day:date,id:bigint>"
        );
    }

    #[test]
    fn spark_ddl_type_name_deep_nesting_is_depth_bounded() {
        let nest_levels = SPARK_TYPE_NAME_MAX_DEPTH.saturating_mul(4).max(128);
        let mut data_type = ArrowDataType::Int32;
        for _ in 0..nest_levels {
            data_type = ArrowDataType::List(Arc::new(Field::new("item", data_type, true)));
        }
        let name = spark_ddl_type_name(&data_type);
        assert!(name.contains(SPARK_TYPE_NAME_DEPTH_FALLBACK));
        assert!(name.starts_with("array<"));
        assert!(name.len() < nest_levels * 8);
    }
}
