use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionContext};

const MAX_NESTED_TYPE_DEPTH: usize = 32;

#[must_use]
pub fn relax_schema_to_nullable(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| relax_field(field, 0))
        .collect::<Vec<Field>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

pub(crate) async fn read_parquet_nullable(
    context: &SessionContext,
    path: &str,
) -> DataFusionResult<DataFrame> {
    let inferred = context
        .read_parquet(path, ParquetReadOptions::default())
        .await?;
    let relaxed = relax_schema_to_nullable(inferred.schema().as_arrow());
    let sparked = promote_parquet_null_types(&relaxed);
    context
        .read_parquet(path, ParquetReadOptions::default().schema(&sparked))
        .await
}

fn promote_parquet_null_types(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| promote_field(field, 0))
        .collect::<Vec<Field>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn promote_field(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field
            .clone()
            .with_data_type(promote_leaf(field.data_type()));
    }
    field
        .clone()
        .with_data_type(promote_data_type(field.data_type(), depth))
}

fn promote_leaf(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Null => DataType::Int32,
        other => other.clone(),
    }
}

fn promote_data_type(data_type: &DataType, depth: usize) -> DataType {
    let child = depth + 1;
    match data_type {
        DataType::Null => DataType::Int32,
        DataType::Struct(fields) => DataType::Struct(promote_fields(fields, child)),
        DataType::List(inner) => DataType::List(Arc::new(promote_field(inner, child))),
        DataType::LargeList(inner) => DataType::LargeList(Arc::new(promote_field(inner, child))),
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(promote_field(inner, child)), *size)
        }
        DataType::ListView(inner) => DataType::ListView(Arc::new(promote_field(inner, child))),
        DataType::LargeListView(inner) => {
            DataType::LargeListView(Arc::new(promote_field(inner, child)))
        }
        DataType::Map(entries, sorted) => promote_map(entries, child).map_or_else(
            || data_type.clone(),
            |rebuilt| DataType::Map(rebuilt, *sorted),
        ),
        other => other.clone(),
    }
}

fn promote_fields(fields: &Fields, depth: usize) -> Fields {
    Fields::from(
        fields
            .iter()
            .map(|field| promote_field(field, depth))
            .collect::<Vec<Field>>(),
    )
}

fn promote_map(entries: &Field, depth: usize) -> Option<Arc<Field>> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() >= 2 => {
            let key = fields[0].as_ref().clone();
            let value = promote_field(fields[1].as_ref(), depth);
            Some(Arc::new(entries.clone().with_data_type(DataType::Struct(
                Fields::from(vec![key, value]),
            ))))
        }
        _ => None,
    }
}

fn relax_field(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field.clone().with_nullable(true);
    }
    field
        .clone()
        .with_data_type(relax_data_type(field.data_type(), depth))
        .with_nullable(true)
}

fn relax_data_type(data_type: &DataType, depth: usize) -> DataType {
    let child = depth + 1;
    match data_type {
        DataType::Struct(fields) => DataType::Struct(relax_fields(fields, child)),
        DataType::List(inner) => DataType::List(Arc::new(relax_field(inner, child))),
        DataType::LargeList(inner) => DataType::LargeList(Arc::new(relax_field(inner, child))),
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(relax_field(inner, child)), *size)
        }
        DataType::ListView(inner) => DataType::ListView(Arc::new(relax_field(inner, child))),
        DataType::LargeListView(inner) => {
            DataType::LargeListView(Arc::new(relax_field(inner, child)))
        }
        DataType::Map(entries, sorted) => relax_map(entries, child).map_or_else(
            || data_type.clone(),
            |rebuilt| DataType::Map(rebuilt, *sorted),
        ),
        _ => data_type.clone(),
    }
}

fn relax_fields(fields: &Fields, depth: usize) -> Fields {
    Fields::from(
        fields
            .iter()
            .map(|field| relax_field(field, depth))
            .collect::<Vec<Field>>(),
    )
}

fn relax_map(entries: &Field, depth: usize) -> Option<Arc<Field>> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() >= 2 => {
            let key = fields[0].as_ref().clone();
            let value = relax_field(fields[1].as_ref(), depth);
            Some(Arc::new(entries.clone().with_data_type(DataType::Struct(
                Fields::from(vec![key, value]),
            ))))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, false)
    }

    #[test]
    fn flat_required_fields_become_nullable() {
        let schema = Schema::new(vec![
            required("id", DataType::Utf8),
            required("part", DataType::Int32),
        ]);
        let relaxed = relax_schema_to_nullable(&schema);
        assert!(
            relaxed
                .field_with_name("id")
                .expect("id present")
                .is_nullable()
        );
        assert!(
            relaxed
                .field_with_name("part")
                .expect("part present")
                .is_nullable()
        );
        assert_eq!(
            relaxed
                .field_with_name("id")
                .expect("id present")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn nested_struct_list_and_map_values_become_nullable() {
        let schema = Schema::new(vec![
            required(
                "top",
                DataType::Struct(Fields::from(vec![required("inner", DataType::Utf8)])),
            ),
            required(
                "vals",
                DataType::List(Arc::new(required("element", DataType::Int32))),
            ),
            required(
                "props",
                DataType::Map(
                    Arc::new(required(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            required("key", DataType::Utf8),
                            required("value", DataType::Utf8),
                        ])),
                    )),
                    false,
                ),
            ),
        ]);
        let relaxed = relax_schema_to_nullable(&schema);
        assert!(
            relaxed
                .field_with_name("top")
                .expect("top present")
                .is_nullable()
        );
        assert!(
            relaxed
                .field_with_name("vals")
                .expect("vals present")
                .is_nullable()
        );
        assert!(
            relaxed
                .field_with_name("props")
                .expect("props present")
                .is_nullable()
        );
        let DataType::Struct(top) = relaxed
            .field_with_name("top")
            .expect("top present")
            .data_type()
        else {
            panic!("top stays a struct");
        };
        assert!(top[0].is_nullable());
        let DataType::List(element) = relaxed
            .field_with_name("vals")
            .expect("vals present")
            .data_type()
        else {
            panic!("vals stays a list");
        };
        assert!(element.is_nullable());
        let DataType::Map(entries, _) = relaxed
            .field_with_name("props")
            .expect("props present")
            .data_type()
        else {
            panic!("props stays a map");
        };
        let DataType::Struct(pair) = entries.data_type() else {
            panic!("map entries stay a struct");
        };
        assert!(!pair[0].is_nullable());
        assert!(pair[1].is_nullable());
    }

    #[test]
    fn already_nullable_schema_is_unchanged() {
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, true)]);
        assert_eq!(relax_schema_to_nullable(&schema), schema);
    }

    #[test]
    fn schema_and_field_metadata_survive() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("k".to_string(), "v".to_string());
        let field = required("id", DataType::Utf8).with_metadata(metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], metadata.clone());
        let relaxed = relax_schema_to_nullable(&schema);
        assert_eq!(relaxed.metadata(), &metadata);
        assert_eq!(
            relaxed
                .field_with_name("id")
                .expect("id present")
                .metadata(),
            &metadata
        );
    }

    #[test]
    fn deep_nesting_completes_with_nullable_flags() {
        let mut data_type = DataType::Int32;
        for level in 0..600 {
            data_type = DataType::Struct(Fields::from(vec![Field::new(
                format!("n{level}"),
                data_type,
                false,
            )]));
        }
        let schema = Schema::new(vec![Field::new("root", data_type, false)]);
        let relaxed = relax_schema_to_nullable(&schema);
        assert!(
            relaxed
                .field_with_name("root")
                .expect("root present")
                .is_nullable()
        );
        let mut flags = Vec::new();
        let mut current = relaxed
            .field_with_name("root")
            .expect("root present")
            .data_type();
        for _ in 0..600 {
            let DataType::Struct(fields) = current else {
                panic!("nest stays structs");
            };
            flags.push(fields[0].is_nullable());
            current = fields[0].data_type();
        }
        assert!(flags.iter().take(33).all(|flag| *flag));
        assert!(flags.iter().skip(33).all(|flag| !flag));
    }

    #[test]
    fn parquet_null_list_becomes_int32_list() {
        let schema = Schema::new(vec![Field::new(
            "user_properties",
            DataType::List(Arc::new(Field::new("element", DataType::Null, true))),
            true,
        )]);
        let promoted = promote_parquet_null_types(&schema);
        let DataType::List(element) = promoted
            .field_with_name("user_properties")
            .expect("user_properties present")
            .data_type()
        else {
            panic!("user_properties stays a list");
        };
        assert_eq!(element.data_type(), &DataType::Int32);
        assert!(element.is_nullable());
    }

    #[test]
    fn parquet_scalar_null_becomes_int32() {
        let schema = Schema::new(vec![Field::new("bare", DataType::Null, true)]);
        let promoted = promote_parquet_null_types(&schema);
        assert_eq!(
            promoted
                .field_with_name("bare")
                .expect("bare present")
                .data_type(),
            &DataType::Int32
        );
    }

    #[test]
    fn parquet_null_inside_struct_and_map_value_becomes_int32() {
        let schema = Schema::new(vec![
            Field::new(
                "wrap",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::Null,
                    true,
                )])),
                true,
            ),
            Field::new(
                "props",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Null, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]);
        let promoted = promote_parquet_null_types(&schema);
        let DataType::Struct(wrap) = promoted
            .field_with_name("wrap")
            .expect("wrap present")
            .data_type()
        else {
            panic!("wrap stays a struct");
        };
        assert_eq!(wrap[0].data_type(), &DataType::Int32);
        let DataType::Map(entries, _) = promoted
            .field_with_name("props")
            .expect("props present")
            .data_type()
        else {
            panic!("props stays a map");
        };
        let DataType::Struct(pair) = entries.data_type() else {
            panic!("map entries stay a struct");
        };
        assert_eq!(pair[0].data_type(), &DataType::Utf8);
        assert_eq!(pair[1].data_type(), &DataType::Int32);
    }

    #[test]
    fn relax_leaves_null_list_element_as_null() {
        let schema = Schema::new(vec![Field::new(
            "user_properties",
            DataType::List(Arc::new(Field::new("element", DataType::Null, true))),
            true,
        )]);
        let relaxed = relax_schema_to_nullable(&schema);
        let DataType::List(element) = relaxed
            .field_with_name("user_properties")
            .expect("user_properties present")
            .data_type()
        else {
            panic!("user_properties stays a list");
        };
        assert_eq!(element.data_type(), &DataType::Null);
    }

    #[test]
    fn promote_preserves_int32_lists() {
        let schema = Schema::new(vec![Field::new(
            "scores",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            true,
        )]);
        assert_eq!(promote_parquet_null_types(&schema), schema);
    }
}
