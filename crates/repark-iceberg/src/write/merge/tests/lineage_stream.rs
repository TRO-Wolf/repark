use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::error::Result;
use futures::StreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
};
use iceberg::spec::{
    DataFile, FormatVersion, NestedField, NullOrder, PrimitiveType, Schema, SortDirection,
    SortField, SortOrder, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

use super::super::row_lineage::write_partitioned_lineage_files_with;
use crate::write::write_options::WriterStagingOverrides;

async fn v3_partitioned_table(warehouse: &TempDir, order: Option<SortOrder>) -> Table {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse")
        .to_string();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "mem",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
        )
        .await
        .expect("memory catalog");
    let namespace = NamespaceIdent::new("ns".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "part", Transform::Identity)
        .expect("identity partition field")
        .build();
    let creation = TableCreation::builder()
        .name("lineage".to_string())
        .schema(schema)
        .partition_spec(spec)
        .format_version(FormatVersion::V3);
    let creation = match order {
        Some(order) => creation.sort_order(order).build(),
        None => creation.build(),
    };
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create v3 table");
    catalog
        .load_table(&TableIdent::new(namespace, "lineage".into()))
        .await
        .expect("load")
}

fn id_ascending() -> SortOrder {
    SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(1)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        )
        .build_unbound()
        .expect("id order")
}

fn lineage_field(name: &str, field_id: i32) -> Field {
    Field::new(name, DataType::Int64, true).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

fn rewrite_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("part", DataType::Int32, false),
        lineage_field(RESERVED_COL_NAME_ROW_ID, RESERVED_FIELD_ID_ROW_ID),
        lineage_field(
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ),
    ]))
}

fn rewrite_batch(rows: &[(i32, i32, i64, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        rewrite_schema(),
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(rows.iter().map(|row| row.3).collect::<Int64Array>()),
        ],
    )
    .expect("rewrite batch")
}

fn rewrite_batches() -> Vec<RecordBatch> {
    vec![
        rewrite_batch(&[(5, 0, 50, Some(3)), (3, 1, 30, None)]),
        rewrite_batch(&[]),
        rewrite_batch(&[(1, 0, 10, None), (7, 1, 70, Some(2))]),
        rewrite_batch(&[(4, 0, 40, Some(1))]),
    ]
}

fn parquet_files_under(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .map(|path| {
            if path.is_dir() {
                parquet_files_under(&path)
            } else {
                usize::from(path.extension().is_some_and(|ext| ext == "parquet"))
            }
        })
        .sum()
}

async fn write_probed(table: &Table, root: PathBuf) -> (Vec<DataFile>, usize) {
    let seen_before_last = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&seen_before_last);
    let batches = rewrite_batches();
    let last = batches.len() - 1;
    let stream = futures::stream::iter(batches.into_iter().enumerate()).map(move |(at, batch)| {
        if at == last {
            probe.store(parquet_files_under(&root), Ordering::SeqCst);
        }
        Ok::<_, datafusion::error::DataFusionError>(batch)
    });
    let files: Result<Vec<DataFile>> =
        write_partitioned_lineage_files_with(table, stream, &WriterStagingOverrides::none()).await;
    (
        files.expect("lineage write"),
        seen_before_last.load(Ordering::SeqCst),
    )
}

fn column(file: &DataFile, name: &str) -> Vec<Option<i64>> {
    let path = file.file_path();
    let local = path.strip_prefix("file://").unwrap_or(path);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(local).expect("open"))
            .expect("parquet reader")
            .build()
            .expect("reader");
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.expect("parquet batch");
        let array = datafusion::arrow::compute::cast(
            batch.column_by_name(name).expect("column"),
            &DataType::Int64,
        )
        .expect("cast");
        let array = array.as_any().downcast_ref::<Int64Array>().expect("i64");
        values.extend((0..array.len()).map(|at| array.is_valid(at).then(|| array.value(at))));
    }
    values
}

fn file_for_part(files: &[DataFile], part: i64) -> &DataFile {
    files
        .iter()
        .find(|file| column(file, "part").first() == Some(&Some(part)))
        .expect("partition file")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsorted_v3_lineage_rewrite_streams_and_keeps_arrival_order() {
    let _: &str = "pins: ice-sorted-insert-1/C-006";
    let warehouse = TempDir::new().expect("warehouse");
    let table = v3_partitioned_table(&warehouse, None).await;
    let (files, seen_before_last) = write_probed(&table, warehouse.path().to_path_buf()).await;
    assert!(
        seen_before_last > 0,
        "an unsorted rewrite must reach the writer before the stream ends"
    );
    assert_eq!(files.len(), 2);
    assert!(files.iter().all(|file| file.sort_order_id() == Some(0)));
    let zero = file_for_part(&files, 0);
    assert_eq!(zero.record_count(), 3);
    assert_eq!(column(zero, "id"), vec![Some(5), Some(1), Some(4)]);
    assert_eq!(
        column(zero, RESERVED_COL_NAME_ROW_ID),
        vec![Some(50), Some(10), Some(40)]
    );
    assert_eq!(
        column(zero, RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER),
        vec![Some(3), None, Some(1)]
    );
    let one = file_for_part(&files, 1);
    assert_eq!(one.record_count(), 2);
    assert_eq!(column(one, "id"), vec![Some(3), Some(7)]);
    assert_eq!(
        column(one, RESERVED_COL_NAME_ROW_ID),
        vec![Some(30), Some(70)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_order_v3_lineage_rewrite_sorts_before_the_writer_opens() {
    let _: &str = "pins: ice-sorted-insert-1/C-006, C-010";
    let warehouse = TempDir::new().expect("warehouse");
    let table = v3_partitioned_table(&warehouse, Some(id_ascending())).await;
    let (files, seen_before_last) = write_probed(&table, warehouse.path().to_path_buf()).await;
    assert_eq!(
        seen_before_last, 0,
        "a declared-order rewrite must not open a file before the sort"
    );
    assert_eq!(files.len(), 2);
    assert!(files.iter().all(|file| file.sort_order_id() == Some(1)));
    let zero = file_for_part(&files, 0);
    assert_eq!(column(zero, "id"), vec![Some(1), Some(4), Some(5)]);
    assert_eq!(
        column(zero, RESERVED_COL_NAME_ROW_ID),
        vec![Some(10), Some(40), Some(50)]
    );
    assert_eq!(
        column(zero, RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER),
        vec![None, Some(1), Some(3)]
    );
    let one = file_for_part(&files, 1);
    assert_eq!(column(one, "id"), vec![Some(3), Some(7)]);
}
