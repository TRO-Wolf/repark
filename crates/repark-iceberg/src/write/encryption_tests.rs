use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_common::Generation;
use tempfile::TempDir;
use uuid::Uuid;

use super::sink_offsets::{CommitStamp, commit_stamp_only};
use crate::microbatch::error::MicroBatchError;
use crate::microbatch::offset::{
    Epoch, FilePosition, InputOffset, OffsetFormatVersion, OffsetVector, QueryId, RunId, SinkDoor,
    SinkRecord, SnapshotId, TableUuid,
};
use crate::write::encryption::ENCRYPTION_KEY_ID_PROPERTY;

fn id_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema")
}

fn stamp() -> CommitStamp {
    CommitStamp {
        record: SinkRecord {
            format: OffsetFormatVersion::CURRENT,
            query: QueryId::new(
                Uuid::parse_str("aaaaaaaa-0000-4000-8000-0000000000a1").expect("uuid"),
            ),
            run: RunId::new(Uuid::parse_str("bbbbbbbb-0000-4000-8000-0000000000b2").expect("uuid")),
            epoch: Epoch::new(0),
            generation: Generation::new(1).expect("generation"),
            offsets: OffsetVector::single(InputOffset {
                table: TableUuid::new(
                    Uuid::parse_str("0b0b0b0b-0000-4000-8000-00000000b0b0").expect("uuid"),
                ),
                table_name: String::from("bronze.events"),
                snapshot: SnapshotId::new(100),
                position: FilePosition::new(1),
            }),
        },
        door: SinkDoor::ForeachBatch,
    }
}

async fn fixture(
    warehouse: &TempDir,
    properties: HashMap<String, String>,
) -> (Arc<dyn Catalog>, TableIdent) {
    let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("silver".to_string()), HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::new(NamespaceIdent::new("silver".to_string()), "enc".to_string());
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name("enc".to_string())
                .schema(id_schema())
                .properties(properties)
                .build(),
        )
        .await
        .expect("create table");
    (catalog, ident)
}

fn count_objects(root: &std::path::Path) -> usize {
    let mut pending = vec![root.to_path_buf()];
    let mut count = 0usize;
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                count += 1;
            }
        }
    }
    count
}

#[tokio::test]
async fn stamp_only_commit_onto_keyed_sink_refuses_without_writing() {
    let warehouse = TempDir::new().expect("warehouse");
    let (catalog, ident) = fixture(
        &warehouse,
        HashMap::from([(
            ENCRYPTION_KEY_ID_PROPERTY.to_string(),
            "review-test-key".to_string(),
        )]),
    )
    .await;
    let table = catalog.load_table(&ident).await.expect("load");
    let snapshots_before: Vec<i64> = table
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect();
    let objects_before = count_objects(warehouse.path());
    let error = commit_stamp_only(&catalog, &table, &stamp(), None)
        .await
        .expect_err("stamp must refuse");
    assert_eq!(
        error,
        MicroBatchError::EncryptedSinkRefused {
            sink: String::from("silver.enc")
        }
    );
    let message = error.to_string();
    for needle in [
        "silver.enc",
        "encryption.key-id",
        "no table encryption",
        "plaintext",
        "ENC-1",
    ] {
        assert!(
            message.contains(needle),
            "refusal must name {needle}, got: {message}"
        );
    }
    assert!(
        !message.contains("review-test-key"),
        "refusal must never echo the key, got: {message}"
    );
    let table = catalog.load_table(&ident).await.expect("reload");
    let snapshots_after: Vec<i64> = table
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect();
    assert_eq!(snapshots_after, snapshots_before);
    assert_eq!(count_objects(warehouse.path()), objects_before);
}

#[tokio::test]
async fn stamp_only_commit_onto_lookalike_sink_runs() {
    let warehouse = TempDir::new().expect("warehouse");
    let (catalog, ident) = fixture(
        &warehouse,
        HashMap::from([(
            "encryption.key-id-x".to_string(),
            "review-test-key".to_string(),
        )]),
    )
    .await;
    let table = catalog.load_table(&ident).await.expect("load");
    commit_stamp_only(&catalog, &table, &stamp(), None)
        .await
        .expect("lookalike stamp runs");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(table.metadata().snapshots().count(), 1);
}
