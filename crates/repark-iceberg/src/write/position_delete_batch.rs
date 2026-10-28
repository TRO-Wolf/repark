use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use iceberg::spec::TableProperties;

use crate::write::position_delete::PositionDeletePair;

pub(crate) const POSITION_DELETE_BATCH_BYTE_BOUND: usize = 256 * 1024 * 1024;

pub(crate) fn position_delete_batch_rows(properties: &HashMap<String, String>) -> Result<usize> {
    match properties.get(TableProperties::PROPERTY_WRITE_DELETE_POSITION_DELETE_BATCH_ROWS) {
        None => Ok(TableProperties::PROPERTY_WRITE_DELETE_POSITION_DELETE_BATCH_ROWS_DEFAULT),
        Some(value) => match value.parse::<usize>() {
            Ok(rows) if rows > 0 => Ok(rows),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid value '{value}' for table property '{}'",
                TableProperties::PROPERTY_WRITE_DELETE_POSITION_DELETE_BATCH_ROWS
            ))),
        },
    }
}

pub(crate) fn chunk_position_delete_pairs(
    pairs: &[PositionDeletePair],
    row_bound: usize,
    byte_bound: usize,
) -> Vec<Range<usize>> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < pairs.len() {
        let mut rows: usize = 0;
        let mut bytes: usize = 0;
        let mut end = start;
        while end < pairs.len() {
            let next_bytes = bytes.saturating_add(pairs[end].0.len());
            if rows > 0 && (rows >= row_bound || next_bytes > byte_bound) {
                break;
            }
            bytes = next_bytes;
            rows += 1;
            end += 1;
        }
        chunks.push(start..end);
        start = end;
    }
    chunks
}

pub(crate) fn position_delete_chunk_batch(
    schema: &SchemaRef,
    chunk: &[PositionDeletePair],
) -> Result<RecordBatch> {
    let bytes = chunk
        .iter()
        .fold(0_usize, |sum, (path, _)| sum.saturating_add(path.len()));
    if i32::try_from(bytes).is_err() {
        return Err(DataFusionError::Internal(format!(
            "position-delete batch of {} rows holds {bytes} path bytes, past the Utf8 \
             StringArray limit",
            chunk.len()
        )));
    }
    let paths: Vec<&str> = chunk.iter().map(|(path, _)| path.as_ref()).collect();
    let positions: Vec<i64> = chunk.iter().map(|(_, position)| *position).collect();
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ],
    )?)
}

pub(crate) fn position_delete_batches(
    schema: &SchemaRef,
    pairs: &[PositionDeletePair],
    row_bound: usize,
) -> Result<Vec<RecordBatch>> {
    chunk_position_delete_pairs(pairs, row_bound, POSITION_DELETE_BATCH_BYTE_BOUND)
        .into_iter()
        .map(|range| position_delete_chunk_batch(schema, &pairs[range]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numbered_pairs(count: usize, path_len: usize) -> Vec<PositionDeletePair> {
        (0..count)
            .map(|index| {
                let position = i64::try_from(index).expect("index fits i64");
                let path = format!("{index:0>path_len$}");
                (Arc::from(path.as_str()), position)
            })
            .collect()
    }

    fn batch_rows_of(value: &str) -> Result<usize> {
        position_delete_batch_rows(&HashMap::from([(
            TableProperties::PROPERTY_WRITE_DELETE_POSITION_DELETE_BATCH_ROWS.to_string(),
            value.to_string(),
        )]))
    }

    fn pair_schema() -> SchemaRef {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]))
    }

    #[test]
    fn chunker_cuts_at_the_row_bound() {
        let pairs = numbered_pairs(25, 8);
        assert_eq!(
            chunk_position_delete_pairs(&pairs, 10, usize::MAX),
            vec![0..10, 10..20, 20..25]
        );
        assert_eq!(
            chunk_position_delete_pairs(&pairs, 25, usize::MAX),
            vec![0..25]
        );
    }

    #[test]
    fn chunker_cuts_before_the_path_that_crosses_the_byte_bound() {
        let pairs = numbered_pairs(5, 100);
        assert_eq!(
            chunk_position_delete_pairs(&pairs, usize::MAX, 250),
            vec![0..2, 2..4, 4..5]
        );
        assert_eq!(
            chunk_position_delete_pairs(&pairs, usize::MAX, 200),
            vec![0..2, 2..4, 4..5]
        );
        assert_eq!(
            chunk_position_delete_pairs(&pairs, usize::MAX, 199),
            vec![0..1, 1..2, 2..3, 3..4, 4..5]
        );
        assert_eq!(
            chunk_position_delete_pairs(&pairs, 3, 250),
            vec![0..2, 2..4, 4..5]
        );
    }

    #[test]
    fn chunker_never_emits_an_empty_chunk() {
        assert!(chunk_position_delete_pairs(&[], 10, 10).is_empty());
        let pairs = numbered_pairs(4, 64);
        let chunks = chunk_position_delete_pairs(&pairs, 1, 1);
        assert_eq!(chunks, vec![0..1, 1..2, 2..3, 3..4]);
        assert!(chunks.iter().all(|range| !range.is_empty()));
    }

    #[test]
    fn chunker_places_every_pair_once_in_order() {
        let pairs = numbered_pairs(1_000, 13);
        for (row_bound, byte_bound) in [(1, usize::MAX), (7, usize::MAX), (usize::MAX, 100)] {
            let chunks = chunk_position_delete_pairs(&pairs, row_bound, byte_bound);
            let mut next = 0;
            for range in &chunks {
                assert_eq!(range.start, next, "chunks must be contiguous");
                assert!(range.end > range.start, "a chunk is never empty");
                assert!(range.len() <= row_bound);
                next = range.end;
            }
            assert_eq!(next, pairs.len(), "every pair lands in a chunk");
            let rejoined: Vec<i64> = chunks
                .iter()
                .flat_map(|range| pairs[range.clone()].iter().map(|(_, position)| *position))
                .collect();
            let expected: Vec<i64> = pairs.iter().map(|(_, position)| *position).collect();
            assert_eq!(rejoined, expected);
        }
    }

    #[test]
    fn chunk_batch_carries_the_chunk_in_order() {
        let pairs = numbered_pairs(5, 4);
        let batch = position_delete_chunk_batch(&pair_schema(), &pairs[1..4]).expect("batch");
        assert_eq!(batch.num_rows(), 3);
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 paths");
        assert_eq!(
            paths.iter().flatten().collect::<Vec<_>>(),
            vec!["0001", "0002", "0003"]
        );
    }

    #[test]
    fn chunk_batch_guard_refuses_path_bytes_past_i32_max() {
        let shared: Arc<str> = Arc::from("p".repeat(1024 * 1024).as_str());
        let chunk: Vec<PositionDeletePair> = (0..2048_i64)
            .map(|position| (Arc::clone(&shared), position))
            .collect();
        let error = position_delete_chunk_batch(&pair_schema(), &chunk)
            .expect_err("a chunk past i32::MAX path bytes must refuse");
        assert!(
            matches!(
                &error,
                DataFusionError::Internal(message)
                    if message.contains("2048 rows")
                        && message.contains("2147483648 path bytes")
                        && message.contains("Utf8 StringArray limit")
            ),
            "{error}"
        );
    }

    #[test]
    fn batch_rows_reader_defaults_when_unset() {
        assert_eq!(
            position_delete_batch_rows(&HashMap::new()).expect("default"),
            1_048_576
        );
        assert_eq!(
            position_delete_batch_rows(&HashMap::new()).expect("default"),
            TableProperties::PROPERTY_WRITE_DELETE_POSITION_DELETE_BATCH_ROWS_DEFAULT
        );
    }

    #[test]
    fn batch_rows_reader_accepts_a_positive_value() {
        assert_eq!(batch_rows_of("7").expect("valid"), 7);
        assert_eq!(batch_rows_of("1").expect("valid"), 1);
    }

    #[test]
    fn batch_rows_reader_refuses_zero_negative_and_non_numeric() {
        for value in ["0", "-3", "abc", ""] {
            let error = batch_rows_of(value).expect_err("must refuse");
            assert!(
                matches!(
                    &error,
                    DataFusionError::Plan(message) if message == &format!(
                        "Invalid value '{value}' for table property \
                         'write.delete.position-delete-batch-rows'"
                    )
                ),
                "{error}"
            );
        }
    }

    async fn seeded_table(
        warehouse: &tempfile::TempDir,
        location: Option<String>,
        batch_rows: Option<&str>,
    ) -> (iceberg::table::Table, Arc<str>) {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use iceberg::{NamespaceIdent, TableCreation, TableIdent};
        let catalog = crate::catalog::memory_catalog(warehouse.path().to_str().expect("utf-8"))
            .await
            .expect("memory catalog");
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace");
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("schema");
        let properties = batch_rows.map(|value| {
            (
                TableProperties::PROPERTY_WRITE_DELETE_POSITION_DELETE_BATCH_ROWS.to_string(),
                value.to_string(),
            )
        });
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .location_opt(location)
                    .schema(schema)
                    .properties(properties)
                    .build(),
            )
            .await
            .expect("create table");
        let ident = TableIdent::new(namespace, "t".to_string());
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let table = crate::write::append::append(&catalog, &ident, vec![batch])
            .await
            .expect("append");
        let tasks: Vec<iceberg::scan::FileScanTask> = futures::TryStreamExt::try_collect(
            table
                .scan()
                .build()
                .expect("scan")
                .plan_files()
                .await
                .expect("plan files"),
        )
        .await
        .expect("collect tasks");
        assert_eq!(tasks.len(), 1, "one data file");
        let path: Arc<str> = Arc::from(tasks[0].data_file_path());
        (table, path)
    }

    async fn write_pairs(
        table: &iceberg::table::Table,
        pairs: &[PositionDeletePair],
    ) -> Result<Vec<iceberg::spec::DataFile>> {
        crate::write::position_delete::write_position_deletes(
            table,
            pairs,
            crate::write::concurrency::WriteConcurrency::new(1).expect("K=1"),
            &crate::write::write_options::WriterStagingOverrides::none(),
        )
        .await
    }

    fn position_delete_file_count(root: &std::path::Path) -> usize {
        let mut pending = vec![root.to_path_buf()];
        let mut count = 0;
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).expect("read dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("pos-del"))
                {
                    count += 1;
                }
            }
        }
        count
    }

    fn pairs_on(path: &Arc<str>, count: usize) -> Vec<PositionDeletePair> {
        (0..count)
            .map(|index| {
                let position = i64::try_from(index).expect("index fits i64");
                (Arc::clone(path), position)
            })
            .collect()
    }

    #[tokio::test]
    async fn partition_writer_writes_every_pair_across_bounded_batches() {
        let warehouse = tempfile::TempDir::new().expect("warehouse");
        let (table, path) = seeded_table(&warehouse, None, Some("2")).await;
        let files = write_pairs(&table, &pairs_on(&path, 5))
            .await
            .expect("bounded batches write");
        assert!(!files.is_empty());
        let rows: u64 = files
            .iter()
            .map(iceberg::spec::DataFile::record_count)
            .sum();
        assert_eq!(rows, 5);
    }

    #[tokio::test]
    async fn partition_writer_refuses_batch_rows_below_one_before_any_file() {
        let warehouse = tempfile::TempDir::new().expect("warehouse");
        let (table, path) = seeded_table(&warehouse, None, Some("0")).await;
        let error = write_pairs(&table, &pairs_on(&path, 5))
            .await
            .expect_err("a batch size below one must refuse");
        assert!(
            matches!(
                &error,
                DataFusionError::Plan(message) if message
                    == "Invalid value '0' for table property \
                        'write.delete.position-delete-batch-rows'"
            ),
            "{error}"
        );
        assert_eq!(position_delete_file_count(warehouse.path()), 0);
    }

    #[tokio::test]
    #[ignore = "stress: writes more than i32::MAX bytes of position-delete path text"]
    async fn stress_partition_writer_covers_path_text_past_i32_max() {
        let warehouse = tempfile::TempDir::new().expect("warehouse");
        let mut location = warehouse.path().to_path_buf();
        for segment in 0..13 {
            location.push(format!("{segment:x<250}"));
        }
        let location = location.to_str().expect("utf-8").to_string();
        let (table, path) = seeded_table(&warehouse, Some(location), None).await;
        let limit = usize::try_from(i32::MAX).expect("i32::MAX fits usize");
        let count = limit / path.len() + 1_000;
        let pairs = pairs_on(&path, count);
        assert!(count * path.len() > limit, "the fixture passes i32::MAX");
        let files = write_pairs(&table, &pairs)
            .await
            .expect("bounded batches cover the partition without an offset overflow");
        let rows: u64 = files
            .iter()
            .map(iceberg::spec::DataFile::record_count)
            .sum();
        assert_eq!(rows, u64::try_from(count).expect("count fits u64"));
    }

    async fn registered_identity_batches(
        ctx: &datafusion::prelude::SessionContext,
        name: &str,
    ) -> Vec<RecordBatch> {
        let provider = ctx.table_provider(name).await.expect("registered provider");
        let memory = provider
            .downcast_ref::<datafusion::datasource::MemTable>()
            .expect("identity table is a MemTable");
        assert_eq!(memory.batches.len(), 1, "one MemTable partition");
        memory.batches[0].read().await.clone()
    }

    fn scanned_pairs(batches: &[RecordBatch]) -> Vec<(String, i64)> {
        batches
            .iter()
            .flat_map(|batch| {
                let paths = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 paths")
                    .iter()
                    .map(|path| path.expect("non-null path").to_string())
                    .collect::<Vec<_>>();
                let positions = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 positions")
                    .values()
                    .to_vec();
                paths.into_iter().zip(positions)
            })
            .collect()
    }

    #[tokio::test]
    async fn identity_table_over_the_row_bound_registers_several_batches_in_order() {
        let ctx = datafusion::prelude::SessionContext::new();
        let pairs = numbered_pairs(25, 8);
        let name = crate::write::predicate_dml::register_identity_table(&ctx, &pairs, 10)
            .expect("register identity table");
        let stored = registered_identity_batches(&ctx, &name).await;
        assert_eq!(
            stored.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![10, 10, 5]
        );
        let expected: Vec<(String, i64)> = pairs
            .iter()
            .map(|(path, position)| (path.to_string(), *position))
            .collect();
        assert_eq!(scanned_pairs(&stored), expected);
        let scanned = ctx
            .table(name.as_str())
            .await
            .expect("table")
            .collect()
            .await
            .expect("scan identity table");
        assert_eq!(scanned_pairs(&scanned), expected);
    }

    #[tokio::test]
    #[ignore = "stress: registers more than i32::MAX bytes of identity path text"]
    async fn stress_identity_table_covers_path_text_past_i32_max() {
        let ctx = datafusion::prelude::SessionContext::new();
        let shared: Arc<str> = Arc::from("p".repeat(1024 * 1024).as_str());
        let pairs = pairs_on(&shared, 2_100);
        let limit = usize::try_from(i32::MAX).expect("i32::MAX fits usize");
        assert!(
            pairs.len() * shared.len() > limit,
            "the fixture passes i32::MAX"
        );
        let name = crate::write::predicate_dml::register_identity_table(
            &ctx,
            &pairs,
            TableProperties::PROPERTY_WRITE_DELETE_POSITION_DELETE_BATCH_ROWS_DEFAULT,
        )
        .expect("bounded batches register without an offset overflow");
        let stored = registered_identity_batches(&ctx, &name).await;
        assert!(
            stored.len() >= 9,
            "the byte bound cuts at least nine batches"
        );
        assert!(stored.iter().all(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .is_some_and(|paths| paths.value_data().len() <= POSITION_DELETE_BATCH_BYTE_BOUND)
        }));
        assert_eq!(
            stored.iter().map(RecordBatch::num_rows).sum::<usize>(),
            pairs.len()
        );
    }
}
