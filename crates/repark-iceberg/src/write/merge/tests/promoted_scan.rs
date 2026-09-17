use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array, Float32Array, Float64Array};
use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::merge::{
    FILE_PATH_COL, POS_COL, TargetScanStream, conform_scan_batch, scratch_schema,
};

fn scanned_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("f", DataType::Float32, false),
        Field::new("d", DataType::Decimal128(9, 2), false),
        Field::new(FILE_PATH_COL, DataType::Utf8, false),
        Field::new(POS_COL, DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![5, 6])),
        Arc::new(Float32Array::from(vec![1.5, 2.5])),
        Arc::new(
            Decimal128Array::from(vec![125_i128, 250])
                .with_precision_and_scale(9, 2)
                .expect("decimal(9,2)"),
        ),
        Arc::new(StringArray::from(vec!["old.parquet", "old.parquet"])),
        Arc::new(Int64Array::from(vec![0_i64, 1])),
    ];
    RecordBatch::try_new(schema, columns).expect("scanned batch")
}

fn promoted_write_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("f", DataType::Float64, false),
        Field::new("d", DataType::Decimal128(12, 2), false),
    ]))
}

fn assert_promoted_values(batch: &RecordBatch) {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id widens to Int64");
    assert_eq!(ids.values().to_vec(), vec![5_i64, 6]);
    let floats = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f widens to Float64");
    assert_eq!(floats.values().to_vec(), vec![1.5_f64, 2.5]);
    let decimals = batch
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("d widens to Decimal128(12, 2)");
    assert_eq!(decimals.data_type(), &DataType::Decimal128(12, 2));
    assert_eq!(decimals.values().to_vec(), vec![125_i128, 250]);
}

#[test]
fn conform_scan_batch_widens_legally_promoted_columns() {
    let scratch = scratch_schema(&promoted_write_schema());
    let conformed = conform_scan_batch(&scratch, &scanned_batch())
        .expect("a legally promoted column conforms to the current type");
    assert_eq!(conformed.schema(), scratch);
    assert_promoted_values(&conformed);
}

#[test]
fn conform_scan_batch_still_refuses_an_illegal_narrowing() {
    let scratch = scratch_schema(&Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("f", DataType::Float64, false),
        Field::new("d", DataType::Decimal128(12, 2), false),
    ])));
    let error = conform_scan_batch(&scratch, &scanned_batch())
        .expect_err("int32 to int16 is not an Iceberg promotion");
    assert!(
        error.to_string().contains("column types must match"),
        "{error}"
    );
}

async fn single_era_promoted_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, Table) {
    let path = warehouse.path().to_str().expect("utf-8 path").to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "mem",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "f", Type::Primitive(PrimitiveType::Float)).into(),
            NestedField::required(
                3,
                "d",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            )
            .into(),
        ])
        .build()
        .expect("pre-promotion schema");
    let creation = TableCreation::builder()
        .name("promoted".to_string())
        .schema(schema)
        .partition_spec(UnboundPartitionSpec::builder().build())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    let ident = TableIdent::new(namespace, "promoted".to_string());
    let seed = scanned_batch().project(&[0, 1, 2]).expect("data columns");
    let mut table = crate::write::append::append(&catalog, &ident, vec![seed])
        .await
        .expect("append the pre-promotion file");
    for (column, promoted) in [
        ("id", PrimitiveType::Long),
        ("f", PrimitiveType::Double),
        (
            "d",
            PrimitiveType::Decimal {
                precision: 12,
                scale: 2,
            },
        ),
    ] {
        let tx = Transaction::new(&table);
        let action = tx.update_schema().update_column(column, promoted);
        table = action
            .apply(tx)
            .expect("apply the promotion")
            .commit(catalog.as_ref())
            .await
            .expect("commit the promotion");
    }
    (catalog, table)
}

#[tokio::test]
async fn target_scan_over_a_single_era_promoted_table_yields_the_current_types() {
    let warehouse = TempDir::new().expect("warehouse");
    let (_catalog, table) = single_era_promoted_table(&warehouse).await;
    let write_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("current write schema"),
    );
    let scratch = scratch_schema(&write_schema);
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let stream = TargetScanStream::new(
        table.clone(),
        snapshot_id,
        Arc::clone(&scratch),
        &write_schema,
        None,
        Some(1),
        None,
    );
    let mut batches = stream.execute(Arc::new(TaskContext::default()));
    let mut rows = 0;
    while let Some(batch) = batches.next().await {
        let batch = batch.expect("the single-era scan conforms to the promoted types");
        assert_promoted_values(&batch);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}
