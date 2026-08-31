//! pins: v3-4-serve-lineage-columns/C-017, C-019, C-020

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::prelude::SessionContext;
use iceberg::metadata_columns::RESERVED_FIELD_ID_ROW_ID;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, NestedField, PrimitiveType,
    Schema, Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use tempfile::TempDir;

use super::super::{LineageColumnsTableProvider, memory_catalog};

fn id_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema")
}

fn write_stored_row_id_parquet(path: &std::path::Path, ids: &[i32], row_ids: &[Option<i64>]) {
    let arrow_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        datafusion::arrow::datatypes::Field::new(
            "_row_id",
            datafusion::arrow::datatypes::DataType::Int64,
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            RESERVED_FIELD_ID_ROW_ID.to_string(),
        )])),
    ]));
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(row_ids.to_vec())),
        ],
    )
    .expect("batch");
    let file = fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

fn local_dir(location: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(location.strip_prefix("file://").unwrap_or(location))
}

async fn stored_row_id_table(catalog: &Arc<dyn Catalog>) -> iceberg::table::Table {
    catalog
        .create_namespace(&NamespaceIdent::new("sales".into()), HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "stored".into());
    let table = catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name("stored".into())
                .schema(id_schema())
                .format_version(FormatVersion::V3)
                .build(),
        )
        .await
        .expect("create v3");
    let data_dir = local_dir(table.metadata().location()).join("data");
    fs::create_dir_all(&data_dir).expect("data dir");
    let parquet_path = data_dir.join("stored-row-id.parquet");
    write_stored_row_id_parquet(&parquet_path, &[1, 2, 3], &[Some(777), None, Some(999)]);
    let file_size = fs::metadata(&parquet_path).expect("stat").len();
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(parquet_path.to_string_lossy().into_owned())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file_size)
        .record_count(3)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .first_row_id(Some(1000))
        .build()
        .expect("data file");
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file]);
    let tx = action.apply(tx).expect("apply");
    tx.commit(catalog.as_ref()).await.expect("commit")
}

fn lineage_pairs(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Vec<(i32, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .expect("id")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32");
        let row_ids = batch
            .column_by_name("_row_id")
            .expect("_row_id")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        for index in 0..batch.num_rows() {
            rows.push((
                ids.value(index),
                (!row_ids.is_null(index)).then(|| row_ids.value(index)),
            ));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

#[tokio::test]
async fn stored_row_id_wins_over_first_row_id_plus_position() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    let table = stored_row_id_table(&catalog).await;
    let provider = LineageColumnsTableProvider::try_new(table).expect("provider");
    let ctx = SessionContext::new();
    ctx.register_table("stored", Arc::new(provider))
        .expect("register");
    let batches = ctx
        .sql("SELECT id, _row_id FROM stored ORDER BY id")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let rows = lineage_pairs(&batches);
    assert_eq!(rows.len(), 3, "three input rows");
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, Some(777), "stored _row_id must win on row 0");
    assert_eq!(rows[2].0, 3);
    assert_eq!(rows[2].1, Some(999), "stored _row_id must win on row 2");
    let derived_middle = rows[1].1.expect("NULL stored _row_id must derive");
    assert_ne!(
        rows[0].1,
        Some(derived_middle - 1),
        "stored 777 must differ from derived first_row_id+0"
    );
}

#[tokio::test]
async fn filter_on_id_keeps_matching_lineage_row() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    let table = stored_row_id_table(&catalog).await;
    let provider = LineageColumnsTableProvider::try_new(table).expect("provider");
    let ctx = SessionContext::new();
    ctx.register_table("stored", Arc::new(provider))
        .expect("register");
    let batches = ctx
        .sql("SELECT id, _row_id FROM stored WHERE id = 1")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    assert_eq!(lineage_pairs(&batches), vec![(1, Some(777))]);
}

#[test]
fn try_new_with_snapshot_is_removed() {
    let source = include_str!("../lineage_columns.rs");
    assert!(
        !source.contains("try_new_with_snapshot"),
        "dead snapshot constructor must not remain as an unwired promise"
    );
    assert!(
        !source.contains("snapshot_id"),
        "current-snapshot provider must not carry a snapshot pin"
    );
}
