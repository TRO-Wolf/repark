//! Spark DDL type spellings shared by the Spark door and the Python binding.

use datafusion::arrow::datatypes::DataType as ArrowDataType;

const SPARK_TYPE_NAME_MAX_DEPTH: usize = 32;

const SPARK_TYPE_NAME_DEPTH_FALLBACK: &str = "...";

#[must_use]
pub fn spark_ddl_type_name(data_type: &ArrowDataType) -> String {
    spark_ddl_type_name_at_depth(data_type, 0)
}

#[must_use]
pub fn spark_ddl_type_name_at_depth(data_type: &ArrowDataType, depth: usize) -> String {
    if depth >= SPARK_TYPE_NAME_MAX_DEPTH {
        return SPARK_TYPE_NAME_DEPTH_FALLBACK.to_string();
    }
    match data_type {
        ArrowDataType::Int8 => "tinyint".to_string(),
        ArrowDataType::Int16 => "smallint".to_string(),
        ArrowDataType::Int32
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32 => "int".to_string(),
        ArrowDataType::Int64 | ArrowDataType::UInt64 => "bigint".to_string(),
        ArrowDataType::Float16 | ArrowDataType::Float32 => "float".to_string(),
        ArrowDataType::Float64 => "double".to_string(),
        ArrowDataType::Boolean => "boolean".to_string(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            "string".to_string()
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => {
            "binary".to_string()
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => "date".to_string(),
        ArrowDataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        ArrowDataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            format!(
                "array<{}>",
                spark_ddl_type_name_at_depth(field.data_type(), depth + 1)
            )
        }
        ArrowDataType::Map(entries, _) => {
            if let ArrowDataType::Struct(fields) = entries.data_type()
                && fields.len() >= 2
            {
                let key = spark_ddl_type_name_at_depth(fields[0].data_type(), depth + 1);
                let value = spark_ddl_type_name_at_depth(fields[1].data_type(), depth + 1);
                return format!("map<{key},{value}>");
            }
            format!("{data_type:?}")
        }
        ArrowDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| {
                    let child = spark_ddl_type_name_at_depth(field.data_type(), depth + 1);
                    format!("{}:{child}", field.name())
                })
                .collect();
            format!("struct<{}>", parts.join(","))
        }
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
