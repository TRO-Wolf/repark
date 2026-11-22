use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;

use crate::partition_overwrite_mode::PLAIN_DYNAMIC_OVERWRITE_CARD;
use crate::{PartitionOverwriteMode, PartitionOverwriteModeConfig, ReparkSession};

const URL: &str = "s3://guard-bucket/cell/t";

const SEED: &str = "SELECT * FROM VALUES (1, 'x', 10), (1, 'y', 20), (2, 'x', 30) AS t(a, b, v)";

const COMPOSITE: &str = "SELECT * FROM VALUES (1, 'x', 40), (3, 'z', 50) AS t(a, b, v)";

const EMPTY: &str = "SELECT * FROM VALUES (1, 'x', 40) AS t(a, b, v) WHERE false";

fn keys() -> Vec<String> {
    vec!["a".to_string(), "b".to_string()]
}

fn set_mode(session: &ReparkSession, mode: PartitionOverwriteMode) {
    let state = session.context().state_ref();
    let mut state = state.write();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(PartitionOverwriteModeConfig { mode });
}

fn emulated() -> (ReparkSession, Arc<InMemory>) {
    let session = ReparkSession::new().unwrap();
    let memory = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = memory.clone();
    session
        .register_s3_bucket_store_for_test("guard-bucket", &store)
        .unwrap();
    (session, memory)
}

async fn objects(memory: &InMemory) -> Vec<(String, Vec<u8>)> {
    let mut listed = memory.list(None);
    let mut out = Vec::new();
    while let Some(meta) = listed.next().await {
        let location = meta.unwrap().location;
        let bytes = memory.get(&location).await.unwrap().bytes().await.unwrap();
        out.push((location.to_string(), bytes.to_vec()));
    }
    out.sort();
    out
}

async fn seeded(format: &str) -> (ReparkSession, Arc<InMemory>, Vec<(String, Vec<u8>)>) {
    let (session, memory) = emulated();
    let seed = session.sql(SEED).await.unwrap();
    session
        .write_path(&seed, URL, format, "error", &HashMap::new(), &keys())
        .await
        .unwrap();
    let before = objects(&memory).await;
    assert!(before.len() >= 3, "{before:?}");
    (session, memory, before)
}

#[tokio::test]
async fn effective_dynamic_refuses_a_partitioned_s3_overwrite_with_every_object_intact() {
    for format in ["parquet", "csv", "json"] {
        for (source, option) in [
            (COMPOSITE, None),
            (EMPTY, None),
            (COMPOSITE, Some("DyNaMiC")),
        ] {
            let (session, memory, before) = seeded(format).await;
            let mut options = HashMap::new();
            match option {
                Some(value) => {
                    options.insert("PartitionOverwriteMODE".to_string(), value.to_string());
                }
                None => set_mode(&session, PartitionOverwriteMode::Dynamic),
            }
            let frame = session.sql(source).await.unwrap();
            let error = session
                .write_path(&frame, URL, format, "overwrite", &options, &keys())
                .await
                .unwrap_err();
            assert!(
                matches!(error, crate::Error::NotImplemented(_)),
                "{format} {option:?}: {error:?}"
            );
            assert!(
                error.to_string().contains(PLAIN_DYNAMIC_OVERWRITE_CARD),
                "{error}"
            );
            assert_eq!(objects(&memory).await, before, "{format} {option:?}");
        }
    }
}

#[tokio::test]
async fn the_refusal_comes_before_the_bucket_store_lookup() {
    let session = ReparkSession::new().unwrap();
    set_mode(&session, PartitionOverwriteMode::Dynamic);
    let frame = session.sql(COMPOSITE).await.unwrap();
    let error = session
        .write_path(
            &frame,
            "s3://unregistered-bucket/cell/t",
            "parquet",
            "overwrite",
            &HashMap::new(),
            &keys(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(PLAIN_DYNAMIC_OVERWRITE_CARD),
        "{error}"
    );
    assert!(
        !error.to_string().contains("register_configured_catalogs"),
        "{error}"
    );
}

#[tokio::test]
async fn a_dynamic_session_keeps_append_unpartitioned_and_static_option_branches() {
    let (session, memory, before) = seeded("parquet").await;
    set_mode(&session, PartitionOverwriteMode::Dynamic);
    let frame = session.sql(COMPOSITE).await.unwrap();
    session
        .write_path(&frame, URL, "parquet", "append", &HashMap::new(), &keys())
        .await
        .unwrap();
    let appended = objects(&memory).await;
    assert!(before.iter().all(|object| appended.contains(object)));
    assert!(appended.len() > before.len());
    session
        .write_path(&frame, URL, "parquet", "overwrite", &HashMap::new(), &[])
        .await
        .unwrap();
    let replaced = objects(&memory).await;
    assert!(
        replaced.iter().all(|(name, _)| !name.contains("a=")),
        "{replaced:?}"
    );
    let mut options = HashMap::new();
    options.insert("partitionOverwriteMode".to_string(), "STATIC".to_string());
    let error = session
        .write_path(&frame, URL, "parquet", "overwrite", &options, &keys())
        .await
        .unwrap_err();
    assert!(
        !error.to_string().contains(PLAIN_DYNAMIC_OVERWRITE_CARD),
        "{error}"
    );
    assert_eq!(objects(&memory).await, replaced);
}
