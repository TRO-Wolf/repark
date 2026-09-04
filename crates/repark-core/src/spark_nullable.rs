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
    context
        .read_parquet(path, ParquetReadOptions::default().schema(&relaxed))
        .await
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
}
