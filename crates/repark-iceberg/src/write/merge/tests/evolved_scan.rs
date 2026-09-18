use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use iceberg::expr::{Predicate, Reference};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{Datum, NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::merge::{TargetScanStream, new_partition_sink, scratch_schema};

type Row = (i64, Option<String>, Option<String>);

enum Evolution {
    AddColumn,
    RenameColumn,
    RenameKey,
    SwapNames,
}

struct Layout {
    names: [&'static str; 3],
    seed: [Option<&'static str>; 4],
    renames: &'static [(&'static str, &'static str)],
}

fn layout(evolution: &Evolution) -> Layout {
    match evolution {
        Evolution::AddColumn => Layout {
            names: ["id", "v", ""],
            seed: [Some("a"), Some("b"), None, None],
            renames: &[],
        },
        Evolution::RenameColumn => Layout {
            names: ["id", "w", "extra"],
            seed: [Some("a"), Some("b"), Some("e1"), None],
            renames: &[("w", "v")],
        },
        Evolution::RenameKey => Layout {
            names: ["k", "v", "extra"],
            seed: [Some("a"), Some("b"), Some("e1"), None],
            renames: &[("k", "id")],
        },
        Evolution::SwapNames => Layout {
            names: ["id", "extra", "v"],
            seed: [Some("a"), Some("b"), Some("e1"), None],
            renames: &[("extra", "tmp"), ("v", "extra"), ("tmp", "v")],
        },
    }
}

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse.path().to_str().expect("utf-8 path").to_string();
    Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "mem",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("memory catalog"),
    )
}

fn seed_batch(layout: &Layout) -> RecordBatch {
    let mut fields = vec![
        Field::new(layout.names[0], DataType::Int64, true),
        Field::new(layout.names[1], DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1_i64, 2])),
        Arc::new(StringArray::from(vec![layout.seed[0], layout.seed[1]])),
    ];
    if !layout.names[2].is_empty() {
        fields.push(Field::new(layout.names[2], DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(vec![
            layout.seed[2],
            layout.seed[3],
        ])));
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).expect("seed batch")
}

async fn evolved_table(warehouse: &TempDir, evolution: &Evolution) -> Table {
    let catalog = memory_catalog(warehouse).await;
    let layout = layout(evolution);
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let mut fields = vec![
        NestedField::optional(1, layout.names[0], Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::optional(2, layout.names[1], Type::Primitive(PrimitiveType::String)).into(),
    ];
    if !layout.names[2].is_empty() {
        fields.push(
            NestedField::optional(3, layout.names[2], Type::Primitive(PrimitiveType::String))
                .into(),
        );
    }
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .expect("schema at the first write");
    let creation = TableCreation::builder()
        .name("evolved".to_string())
        .schema(schema)
        .partition_spec(UnboundPartitionSpec::builder().build())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    let ident = TableIdent::new(namespace, "evolved".to_string());
    let mut table = crate::write::append::append(&catalog, &ident, vec![seed_batch(&layout)])
        .await
        .expect("append the only data file");
    if matches!(evolution, Evolution::AddColumn) {
        let tx = Transaction::new(&table);
        let action = tx
            .update_schema()
            .add_column("extra", Type::Primitive(PrimitiveType::String));
        table = action
            .apply(tx)
            .expect("apply ADD COLUMN")
            .commit(catalog.as_ref())
            .await
            .expect("commit ADD COLUMN");
    }
    for (from, to) in layout.renames {
        let tx = Transaction::new(&table);
        let action = tx.update_schema().rename_column(from, to);
        table = action
            .apply(tx)
            .expect("apply RENAME COLUMN")
            .commit(catalog.as_ref())
            .await
            .expect("commit RENAME COLUMN");
    }
    table
}

fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("scratch column {name}"));
    let values = column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} is Utf8"));
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index).to_string()))
        .collect()
}

async fn scanned_rows(table: &Table, filter: Option<Predicate>, planned: bool) -> Vec<Row> {
    let write_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("current write schema"),
    );
    let scratch = scratch_schema(&write_schema);
    let pin = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let mut stream = TargetScanStream::new(
        table.clone(),
        pin,
        Arc::clone(&scratch),
        &write_schema,
        filter,
        Some(1),
        None,
    );
    if planned {
        stream = stream.with_partition_sink(new_partition_sink());
    }
    let mut batches = stream.execute(Arc::new(TaskContext::default()));
    let mut rows = Vec::new();
    while let Some(batch) = batches.next().await {
        let batch = batch.expect("the pinned target scan reads under the current schema");
        assert_eq!(batch.schema(), scratch);
        let ids = batch
            .column_by_name("id")
            .expect("id")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64")
            .values()
            .to_vec();
        let values = strings(&batch, "v");
        let extras = strings(&batch, "extra");
        for (index, id) in ids.into_iter().enumerate() {
            rows.push((id, values[index].clone(), extras[index].clone()));
        }
    }
    rows.sort();
    rows
}

fn row(id: i64, value: Option<&str>, extra: Option<&str>) -> Row {
    (id, value.map(str::to_string), extra.map(str::to_string))
}

#[tokio::test]
async fn target_scan_after_add_column_null_fills_the_added_column() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = evolved_table(&warehouse, &Evolution::AddColumn).await;
    for planned in [false, true] {
        assert_eq!(
            scanned_rows(&table, None, planned).await,
            vec![row(1, Some("a"), None), row(2, Some("b"), None)],
            "planned={planned}"
        );
    }
}

#[tokio::test]
async fn target_scan_after_rename_reads_the_renamed_column_by_field_id() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = evolved_table(&warehouse, &Evolution::RenameColumn).await;
    for planned in [false, true] {
        assert_eq!(
            scanned_rows(&table, None, planned).await,
            vec![row(1, Some("a"), Some("e1")), row(2, Some("b"), None)],
            "planned={planned}"
        );
    }
}

#[tokio::test]
async fn target_scan_after_swapping_two_names_keeps_each_value_under_its_field_id() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = evolved_table(&warehouse, &Evolution::SwapNames).await;
    for planned in [false, true] {
        assert_eq!(
            scanned_rows(&table, None, planned).await,
            vec![row(1, Some("a"), Some("e1")), row(2, Some("b"), None)],
            "planned={planned}"
        );
    }
}

#[tokio::test]
async fn target_scan_residual_on_a_renamed_key_keeps_the_matching_row() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = evolved_table(&warehouse, &Evolution::RenameKey).await;
    let residual = Reference::new("id")
        .greater_than_or_equal_to(Datum::long(1))
        .and(Reference::new("id").less_than_or_equal_to(Datum::long(1)));
    for planned in [false, true] {
        let rows = scanned_rows(&table, Some(residual.clone()), planned).await;
        assert!(
            rows.contains(&row(1, Some("a"), Some("e1"))),
            "planned={planned} {rows:?}"
        );
        assert!(
            rows.iter()
                .all(|scanned| scanned == &row(1, Some("a"), Some("e1"))
                    || scanned == &row(2, Some("b"), None)),
            "planned={planned} {rows:?}"
        );
    }
}

#[tokio::test]
async fn target_scan_residual_on_a_swapped_name_filters_the_current_field() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = evolved_table(&warehouse, &Evolution::SwapNames).await;
    let residual = Reference::new("v").equal_to(Datum::string("a"));
    for planned in [false, true] {
        let rows = scanned_rows(&table, Some(residual.clone()), planned).await;
        assert!(
            rows.contains(&row(1, Some("a"), Some("e1"))),
            "planned={planned} {rows:?}"
        );
        assert!(
            rows.iter()
                .all(|scanned| scanned == &row(1, Some("a"), Some("e1"))
                    || scanned == &row(2, Some("b"), None)),
            "planned={planned} {rows:?}"
        );
    }
}

async fn two_file_table_then_add_column(warehouse: &TempDir) -> Table {
    let catalog = memory_catalog(warehouse).await;
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema at the first write");
    let creation = TableCreation::builder()
        .name("two_files".to_string())
        .schema(schema)
        .partition_spec(UnboundPartitionSpec::builder().build())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    let ident = TableIdent::new(namespace, "two_files".to_string());
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    let mut table = None;
    for (id, value) in [(1_i64, "a"), (2, "b")] {
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![value])),
            ],
        )
        .expect("one-row batch");
        table = Some(
            crate::write::append::append(&catalog, &ident, vec![batch])
                .await
                .expect("append one data file"),
        );
    }
    let table = table.expect("two appends");
    let tx = Transaction::new(&table);
    let action = tx
        .update_schema()
        .add_column("extra", Type::Primitive(PrimitiveType::String));
    action
        .apply(tx)
        .expect("apply ADD COLUMN")
        .commit(catalog.as_ref())
        .await
        .expect("commit ADD COLUMN")
}

#[tokio::test]
async fn target_scan_residual_on_an_unchanged_column_still_prunes_after_add_column() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = two_file_table_then_add_column(&warehouse).await;
    let residual = Reference::new("id")
        .greater_than_or_equal_to(Datum::long(2))
        .and(Reference::new("id").less_than_or_equal_to(Datum::long(2)));
    for planned in [false, true] {
        assert_eq!(
            scanned_rows(&table, Some(residual.clone()), planned).await,
            vec![row(2, Some("b"), None)],
            "planned={planned}"
        );
    }
}
