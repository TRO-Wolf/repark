use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};

use crate::type_table::SPARK_TYPE_NAME_MAX_DEPTH;

enum TreeEntry<'a> {
    Field(&'a Field, String, usize),
    ArrayElement(&'a Field, String, usize),
    MapKey(&'a Field, String, usize),
    MapValue(&'a Field, String, usize),
}

pub(crate) fn spark_tree_string(schema: &Schema) -> String {
    let mut lines = vec!["root".to_string()];
    let mut entries = Vec::new();
    push_fields(&mut entries, schema.fields(), " |".to_string(), 0);
    while let Some(entry) = entries.pop() {
        match entry {
            TreeEntry::Field(field, prefix, depth) => {
                lines.push(format!(
                    "{prefix}-- {}: {} (nullable = {})",
                    field.name(),
                    tree_type_name(field.data_type()),
                    field.is_nullable()
                ));
                push_children(&mut entries, field.data_type(), &prefix, depth);
            }
            TreeEntry::ArrayElement(field, prefix, depth) => {
                lines.push(format!(
                    "{prefix}-- element: {} (containsNull = {})",
                    tree_type_name(field.data_type()),
                    field.is_nullable()
                ));
                push_children(&mut entries, field.data_type(), &prefix, depth);
            }
            TreeEntry::MapKey(field, prefix, depth) => {
                lines.push(format!(
                    "{prefix}-- key: {}",
                    tree_type_name(field.data_type())
                ));
                push_children(&mut entries, field.data_type(), &prefix, depth);
            }
            TreeEntry::MapValue(field, prefix, depth) => {
                lines.push(format!(
                    "{prefix}-- value: {} (valueContainsNull = {})",
                    tree_type_name(field.data_type()),
                    field.is_nullable()
                ));
                push_children(&mut entries, field.data_type(), &prefix, depth);
            }
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

fn push_children<'a>(
    entries: &mut Vec<TreeEntry<'a>>,
    data_type: &'a DataType,
    prefix: &str,
    depth: usize,
) {
    if depth >= SPARK_TYPE_NAME_MAX_DEPTH {
        return;
    }
    let child_prefix = format!("{prefix}    |");
    let child_depth = depth + 1;
    match data_type {
        DataType::Struct(fields) => push_fields(entries, fields, child_prefix, child_depth),
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => entries.push(TreeEntry::ArrayElement(
            field.as_ref(),
            child_prefix,
            child_depth,
        )),
        DataType::Map(field, _) => {
            let DataType::Struct(fields) = field.data_type() else {
                return;
            };
            let Some(key) = fields.first() else {
                return;
            };
            let Some(value) = fields.get(1) else {
                return;
            };
            entries.push(TreeEntry::MapValue(
                value.as_ref(),
                child_prefix.clone(),
                child_depth,
            ));
            entries.push(TreeEntry::MapKey(key.as_ref(), child_prefix, child_depth));
        }
        _ => {}
    }
}

fn push_fields<'a>(
    entries: &mut Vec<TreeEntry<'a>>,
    fields: &'a Fields,
    prefix: String,
    depth: usize,
) {
    for field in fields.iter().rev() {
        entries.push(TreeEntry::Field(field.as_ref(), prefix.clone(), depth));
    }
}

fn tree_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "void".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 | DataType::UInt8 => "byte".to_string(),
        DataType::Int16 | DataType::UInt16 => "short".to_string(),
        DataType::Int32 | DataType::UInt32 => "integer".to_string(),
        DataType::Int64 | DataType::UInt64 => "long".to_string(),
        DataType::Float16 | DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("decimal({precision},{scale})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        DataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "binary".to_string(),
        DataType::Struct(_) => "struct".to_string(),
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _) => "array".to_string(),
        DataType::Map(_, _) => "map".to_string(),
        _ => data_type.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn tree_string_uses_spark_names_and_nested_layout() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "st",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int32, true),
                        Field::new(
                            "items",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        assert_eq!(
            spark_tree_string(&schema),
            "root\n |-- id: long (nullable = false)\n |-- st: struct (nullable = true)\n \
             |    |-- x: integer (nullable = true)\n |    |-- items: array (nullable = true)\n \
             |    |    |-- element: string (containsNull = true)\n |-- ts: timestamp (nullable = true)\n"
        );
    }
}
