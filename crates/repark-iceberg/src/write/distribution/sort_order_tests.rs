use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::tests::{declare_order, iceberg_schema, memory_catalog, shuffled_full_batches};
use super::{default_sort_is_declared, sort_batches_by_default_order};

fn struct_schema() -> SchemaRef {
    use datafusion::arrow::datatypes::Fields;

    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "st",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            true,
        ),
    ]))
}

fn struct_iceberg_schema() -> IcebergSchema {
    use iceberg::spec::StructType;

    IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(
                2,
                "st",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(3, "a", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(4, "b", Type::Primitive(PrimitiveType::String)).into(),
                ])),
            )
            .into(),
        ])
        .build()
        .expect("struct schema")
}

fn struct_batch(keys: &[Option<i64>]) -> RecordBatch {
    use datafusion::arrow::array::StructArray;
    use datafusion::arrow::buffer::NullBuffer;

    let schema = struct_schema();
    let rows = keys.len();
    let values: Vec<Option<i64>> = keys
        .iter()
        .map(|key| key.unwrap_or(i64::MIN))
        .map(Some)
        .collect();
    let validity: Vec<bool> = keys.iter().map(Option::is_some).collect();
    let labels: Vec<Option<&str>> = keys.iter().map(|key| key.map(|_| "x")).collect();
    let fields = match schema.field_with_name("st").expect("st field").data_type() {
        DataType::Struct(children) => children.clone(),
        other => panic!("st is a struct, got {other}"),
    };
    let strukt = StructArray::new(
        fields,
        vec![
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(labels)),
        ],
        Some(validity.into_iter().collect::<NullBuffer>()),
    );
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![0; rows])), Arc::new(strukt)],
    )
    .expect("struct batch")
}

fn batch_nested_keys(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    use datafusion::arrow::array::{Array, StructArray};

    let mut keys = Vec::new();
    for batch in batches {
        let column = batch.column_by_name("st").expect("batch has `st`");
        let strukt = column
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("`st` is a struct");
        let values = strukt
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("`st.a` is Int64");
        for row in 0..batch.num_rows() {
            if strukt.is_null(row) || values.is_null(row) {
                keys.push(None);
            } else {
                keys.push(Some(values.value(row)));
            }
        }
    }
    keys
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_sort_field_sorts_on_the_nested_value() {
    use crate::write::sort_order::WriteSortField;
    use iceberg::spec::{NullOrder, SortDirection};

    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let namespace = NamespaceIdent::new("ns".into());
    let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("nested".to_string())
                .schema(struct_iceberg_schema())
                .build(),
        )
        .await
        .expect("create struct table");
    let table = declare_order(
        &catalog,
        "nested",
        vec![WriteSortField {
            name: "st.a".to_string(),
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        }],
    )
    .await;
    assert_eq!(table.metadata().default_sort_order().fields[0].source_id, 3);
    let batches = vec![
        struct_batch(&[Some(3), None, Some(1)]),
        struct_batch(&[Some(4), Some(0), Some(2)]),
    ];
    let sorted = sort_batches_by_default_order(&table, batches)
        .await
        .expect("nested sort succeeds");
    assert_eq!(
        batch_nested_keys(&sorted),
        vec![None, Some(0), Some(1), Some(2), Some(3), Some(4)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transform_sort_order_refuses_the_write_loud() {
    use iceberg::spec::{NullOrder, SortDirection, SortField, SortOrder};

    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let namespace = NamespaceIdent::new("ns".into());
    let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
    let order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(1)
                .transform(Transform::Bucket(4))
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        )
        .build_unbound()
        .expect("bucket sort order");
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("bucketed".to_string())
                .schema(iceberg_schema())
                .sort_order(order)
                .build(),
        )
        .await
        .expect("create bucket-ordered table");
    let table = catalog
        .load_table(&TableIdent::new(namespace, "bucketed".into()))
        .await
        .expect("load table");
    assert!(default_sort_is_declared(&table));
    let error = sort_batches_by_default_order(&table, shuffled_full_batches())
        .await
        .expect_err("a transform sort order refuses the write");
    assert!(
        error
            .to_string()
            .contains("only identity sort fields are supported"),
        "{error}"
    );
}
