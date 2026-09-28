use super::super::*;

use bytes::Bytes;
use iceberg::io::{LocalFsStorageFactory, Storage, StorageConfig, StorageFactory};
use tempfile::TempDir;

fn no_overwrite_storage() -> Arc<dyn Storage> {
    let factory = NoOverwriteStorageFactory::new(Arc::new(LocalFsStorageFactory));
    factory.build(&StorageConfig::new()).unwrap()
}

fn fresh_path(dir: &TempDir) -> String {
    format!("{}/t/data/00000-0-a.parquet", dir.path().to_str().unwrap())
}

#[tokio::test]
async fn a_first_writer_on_a_fresh_path_succeeds() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir);
    let storage = no_overwrite_storage();

    let mut writer = storage.writer(&path).await.unwrap();
    writer.write(Bytes::from_static(b"payload")).await.unwrap();
    writer.close().await.unwrap();

    assert!(storage.exists(&path).await.unwrap());
}

#[tokio::test]
async fn a_second_writer_on_an_existing_path_fails_like_write() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir);
    let storage = no_overwrite_storage();
    storage
        .write(&path, Bytes::from_static(b"first"))
        .await
        .unwrap();

    let Err(refused) = storage.writer(&path).await else {
        panic!("a second writer on an existing path must fail")
    };
    assert_eq!(refused.kind(), iceberg::ErrorKind::Unexpected);
    assert!(
        refused
            .to_string()
            .contains("second write of an existing path refused")
    );

    let write_refused = storage
        .write(&path, Bytes::from_static(b"second"))
        .await
        .expect_err("a second write on an existing path must fail");
    assert_eq!(refused.to_string(), write_refused.to_string());
}
