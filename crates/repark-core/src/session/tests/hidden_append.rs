use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::session::ReparkSession;

type AppendCase = (&'static str, Vec<(&'static str, Vec<u8>)>, bool, Vec<i64>);

fn parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), None).expect("open the parquet writer");
    writer.write(batch).expect("write the batch");
    writer.close().expect("close the parquet writer");
    buffer
}

fn id_bytes(id: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))])
        .expect("an id batch");
    parquet_bytes(&batch)
}

fn other_schema_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["z"]))])
        .expect("a string batch");
    parquet_bytes(&batch)
}

async fn s3_write(seed: Vec<(&str, Vec<u8>)>, mode: &str) -> (bool, Vec<i64>, Vec<String>) {
    let session = ReparkSession::new().expect("a session");
    let memory = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = memory.clone();
    session
        .register_s3_bucket_store_for_test("fx-bucket", &store)
        .expect("register the in-memory bucket");
    for (key, bytes) in seed {
        memory
            .put(&ObjectPath::from(key), bytes.into())
            .await
            .expect("seed a key");
    }
    let frame = session
        .sql("SELECT CAST(20 AS BIGINT) AS id")
        .await
        .expect("a frame");
    let written = session
        .write_path(
            &frame,
            "s3://fx-bucket/cell/t",
            "parquet",
            mode,
            &HashMap::new(),
            &[],
        )
        .await
        .is_ok();
    let mut ids = Vec::new();
    if let Ok(read) = session.read_parquet("s3://fx-bucket/cell/t").await
        && let Ok(batches) = read.collect().await
    {
        for batch in batches {
            if let Some(column) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                ids.extend(
                    (0..column.len())
                        .filter(|row| column.is_valid(*row))
                        .map(|row| column.value(row)),
                );
            }
        }
    }
    ids.sort_unstable();
    let mut keys = Vec::new();
    let mut listed = memory.list(None);
    while let Some(meta) = listed.next().await {
        keys.push(meta.expect("a key").location.to_string());
    }
    (written, ids, keys)
}

#[tokio::test]
async fn an_s3_append_beside_hidden_keys_validates_only_what_spark_reads() {
    let cases: Vec<AppendCase> = vec![
        (
            "plain",
            vec![("cell/t/a.parquet", id_bytes(10))],
            true,
            vec![10, 20],
        ),
        (
            "underscore",
            vec![
                ("cell/t/a.parquet", id_bytes(10)),
                ("cell/t/_b.parquet", id_bytes(11)),
            ],
            true,
            vec![10, 20],
        ),
        (
            "dot",
            vec![
                ("cell/t/a.parquet", id_bytes(10)),
                ("cell/t/.c.parquet", id_bytes(12)),
            ],
            true,
            vec![10, 20],
        ),
        (
            "underscore-other-schema",
            vec![
                ("cell/t/a.parquet", id_bytes(10)),
                ("cell/t/_b.parquet", other_schema_bytes()),
            ],
            true,
            vec![10, 20],
        ),
        (
            "dot-not-parquet",
            vec![
                ("cell/t/a.parquet", id_bytes(10)),
                ("cell/t/.c.parquet", b"junk".to_vec()),
            ],
            true,
            vec![10, 20],
        ),
        (
            "temporary-not-parquet",
            vec![
                ("cell/t/a.parquet", id_bytes(10)),
                ("cell/t/_temporary/0/part-9.parquet", b"junk".to_vec()),
            ],
            true,
            vec![10, 20],
        ),
        (
            "only-hidden",
            vec![
                ("cell/t/_SUCCESS", Vec::new()),
                ("cell/t/_b.parquet", id_bytes(11)),
            ],
            true,
            vec![20],
        ),
        (
            "visible-other-schema",
            vec![
                ("cell/t/a.parquet", id_bytes(10)),
                ("cell/t/d.parquet", other_schema_bytes()),
            ],
            false,
            vec![],
        ),
    ];
    for (label, seed, writes, ids) in cases {
        let (written, read, _keys) = s3_write(seed, "append").await;
        assert_eq!(written, writes, "{label}");
        if writes {
            assert_eq!(read, ids, "{label}");
        }
    }
}

#[tokio::test]
async fn an_s3_overwrite_still_clears_hidden_keys() {
    let (written, ids, keys) = s3_write(
        vec![
            ("cell/t/a.parquet", id_bytes(10)),
            ("cell/t/_b.parquet", id_bytes(11)),
            ("cell/t/_SUCCESS", Vec::new()),
        ],
        "overwrite",
    )
    .await;
    assert!(written);
    assert_eq!(ids, vec![20]);
    assert!(
        keys.iter()
            .all(|key| !key.ends_with("/a.parquet") && !key.ends_with("/_b.parquet")),
        "{keys:?}"
    );
}
