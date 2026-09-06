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
        .map(|field| relax_field(field))
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

enum RelaxJob {
    Field(Field),
    MapEntries(Box<RelaxMapEntries>),
}

struct RelaxMapEntries {
    entries: Field,
    key: Field,
    value: Field,
}

struct RelaxRecord {
    job: RelaxJob,
    child_count: usize,
}

fn relax_field(field: &Field) -> Field {
    let mut order: Vec<RelaxRecord> = Vec::new();
    let mut stack: Vec<RelaxJob> = vec![RelaxJob::Field(field.clone())];
    while let Some(job) = stack.pop() {
        match job {
            RelaxJob::Field(node) => {
                let children = relax_children(node.data_type());
                order.push(RelaxRecord {
                    job: RelaxJob::Field(node),
                    child_count: children.len(),
                });
                stack.extend(children);
            }
            RelaxJob::MapEntries(job) => {
                stack.push(RelaxJob::Field(job.value.clone()));
                order.push(RelaxRecord {
                    job: RelaxJob::MapEntries(job),
                    child_count: 1,
                });
            }
        }
    }
    let mut values: Vec<Field> = Vec::new();
    let mut finished = field.clone();
    for record in order.into_iter().rev() {
        let start = values
            .len()
            .checked_sub(record.child_count)
            .unwrap_or(values.len());
        let kids: Vec<Field> = values.drain(start..).collect();
        finished = match record.job {
            RelaxJob::Field(template) => rebuild_field(&template, kids),
            RelaxJob::MapEntries(job) => {
                rebuild_map_entries(&job.entries, &job.key, &job.value, kids)
            }
        };
        values.push(finished.clone());
    }
    finished
}

fn relax_children(data_type: &DataType) -> Vec<RelaxJob> {
    match data_type {
        DataType::Struct(fields) => fields
            .iter()
            .map(|child| RelaxJob::Field(child.as_ref().clone()))
            .collect(),
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _) => vec![RelaxJob::Field(inner.as_ref().clone())],
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() >= 2 => {
                vec![RelaxJob::MapEntries(Box::new(RelaxMapEntries {
                    entries: entries.as_ref().clone(),
                    key: pair[0].as_ref().clone(),
                    value: pair[1].as_ref().clone(),
                }))]
            }
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn rebuild_field(template: &Field, kids: Vec<Field>) -> Field {
    template
        .clone()
        .with_data_type(rebuild_data_type(template.data_type(), kids))
        .with_nullable(true)
}

fn rebuild_data_type(data_type: &DataType, kids: Vec<Field>) -> DataType {
    let mut kids = kids.into_iter();
    match data_type {
        DataType::Struct(_) => DataType::Struct(Fields::from(kids.collect::<Vec<Field>>())),
        DataType::List(_) => DataType::List(Arc::new(relax_child_or(data_type, kids.next()))),
        DataType::LargeList(_) => {
            DataType::LargeList(Arc::new(relax_child_or(data_type, kids.next())))
        }
        DataType::ListView(_) => {
            DataType::ListView(Arc::new(relax_child_or(data_type, kids.next())))
        }
        DataType::LargeListView(_) => {
            DataType::LargeListView(Arc::new(relax_child_or(data_type, kids.next())))
        }
        DataType::FixedSizeList(_, size) => {
            DataType::FixedSizeList(Arc::new(relax_child_or(data_type, kids.next())), *size)
        }
        DataType::Map(_, sorted) => match kids.next() {
            Some(entries) => DataType::Map(Arc::new(entries), *sorted),
            None => data_type.clone(),
        },
        _ => data_type.clone(),
    }
}

fn relax_child_or(data_type: &DataType, kid: Option<Field>) -> Field {
    kid.unwrap_or_else(|| relax_child_placeholder(data_type))
}

fn relax_child_placeholder(data_type: &DataType) -> Field {
    match data_type {
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _) => inner.as_ref().clone(),
        _ => Field::new("element", DataType::Null, true),
    }
}

fn rebuild_map_entries(entries: &Field, key: &Field, value: &Field, kids: Vec<Field>) -> Field {
    let mut kids = kids.into_iter();
    let relaxed = kids.next().unwrap_or_else(|| value.clone());
    entries
        .clone()
        .with_data_type(DataType::Struct(Fields::from(vec![key.clone(), relaxed])))
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

    fn nested_schema(depth: usize) -> Schema {
        let mut data_type = DataType::Int32;
        for level in 0..depth {
            data_type = DataType::Struct(Fields::from(vec![Field::new(
                format!("n{level}"),
                data_type,
                false,
            )]));
        }
        Schema::new(vec![Field::new("root", data_type, false)])
    }

    fn relax_flags(depth: usize) -> Vec<bool> {
        let relaxed = relax_schema_to_nullable(&nested_schema(depth));
        let mut flags = vec![
            relaxed
                .field_with_name("root")
                .expect("root present")
                .is_nullable(),
        ];
        let mut current = relaxed
            .field_with_name("root")
            .expect("root present")
            .data_type();
        for _ in 0..depth {
            let DataType::Struct(fields) = current else {
                panic!("nest stays structs");
            };
            flags.push(fields[0].is_nullable());
            current = fields[0].data_type();
        }
        flags
    }

    #[test]
    fn relax_covers_depth_40() {
        assert_eq!(relax_flags(40), vec![true; 41]);
    }

    #[test]
    fn relax_covers_depth_200() {
        assert_eq!(relax_flags(200), vec![true; 201]);
    }

    #[test]
    fn deep_nesting_completes_with_nullable_flags() {
        assert_eq!(relax_flags(600), vec![true; 601]);
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
