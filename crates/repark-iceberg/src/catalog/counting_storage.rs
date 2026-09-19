use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::Result;
use iceberg::io::{
    FileInfo, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use crate::catalog::io_stats::{
    IcebergFileClass, IcebergIoCounters, IcebergIoOp, classify_iceberg_path, ranged_read_op,
};

pub const GLUE_DEFAULT_CONFIGURED_SCHEME: &str = "s3a";

pub const S3TABLES_DEFAULT_CONFIGURED_SCHEME: &str = "s3";

#[must_use]
pub fn glue_default_storage_factory() -> OpenDalStorageFactory {
    OpenDalStorageFactory::S3 {
        configured_scheme: GLUE_DEFAULT_CONFIGURED_SCHEME.to_string(),
        customized_credential_load: None,
    }
}

#[must_use]
pub fn s3tables_default_storage_factory() -> OpenDalStorageFactory {
    OpenDalStorageFactory::S3 {
        configured_scheme: S3TABLES_DEFAULT_CONFIGURED_SCHEME.to_string(),
        customized_credential_load: None,
    }
}

#[derive(Debug, Clone)]
pub struct CountingStorageFactory {
    inner: Arc<dyn StorageFactory>,
    counters: Arc<IcebergIoCounters>,
}

impl CountingStorageFactory {
    #[must_use]
    pub fn new(inner: Arc<dyn StorageFactory>, counters: Arc<IcebergIoCounters>) -> Self {
        Self { inner, counters }
    }

    #[must_use]
    pub fn inner(&self) -> &Arc<dyn StorageFactory> {
        &self.inner
    }

    #[must_use]
    pub fn counters(&self) -> &Arc<IcebergIoCounters> {
        &self.counters
    }
}

impl Serialize for CountingStorageFactory {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("CountingStorageFactory", 1)?;
        state.serialize_field("inner", &*self.inner)?;
        state.end()
    }
}

impl StorageFactory for CountingStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(CountingStorage {
            inner: self.inner.build(config)?,
            counters: Arc::clone(&self.counters),
        }))
    }

    fn typetag_name(&self) -> &'static str {
        "CountingStorageFactory"
    }

    fn typetag_deserialize(&self) {}
}

#[derive(Debug, Clone)]
pub struct CountingStorage {
    inner: Arc<dyn Storage>,
    counters: Arc<IcebergIoCounters>,
}

impl CountingStorage {
    #[must_use]
    pub fn new(inner: Arc<dyn Storage>, counters: Arc<IcebergIoCounters>) -> Self {
        Self { inner, counters }
    }

    fn record(&self, op: IcebergIoOp, path: &str, bytes: u64) {
        self.counters.record_path(op, path, 1, bytes);
    }
}

impl Serialize for CountingStorage {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("CountingStorage", 1)?;
        state.serialize_field("inner", &*self.inner)?;
        state.end()
    }
}

fn byte_len(bytes: &Bytes) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

#[async_trait]
impl Storage for CountingStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.record(IcebergIoOp::Exists, path, 0);
        self.inner.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.record(IcebergIoOp::Metadata, path, 0);
        self.inner.metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let result = self.inner.read(path).await;
        let bytes = result.as_ref().map_or(0, byte_len);
        self.record(IcebergIoOp::Read, path, bytes);
        result
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let inner = self.inner.reader(path).await?;
        Ok(Box::new(CountingFileRead {
            inner,
            counters: Arc::clone(&self.counters),
            class: classify_iceberg_path(path),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.record(IcebergIoOp::Write, path, byte_len(&bs));
        self.inner.write(path, bs).await
    }

    async fn write_new(&self, path: &str, bs: Bytes) -> Result<()> {
        self.record(IcebergIoOp::Write, path, byte_len(&bs));
        self.inner.write_new(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let inner = self.inner.writer(path).await?;
        Ok(Box::new(CountingFileWrite {
            inner,
            counters: Arc::clone(&self.counters),
            class: classify_iceberg_path(path),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.record(IcebergIoOp::Delete, path, 0);
        self.inner.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.record(IcebergIoOp::Delete, path, 0);
        self.inner.delete_prefix(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<FileInfo>> {
        self.record(IcebergIoOp::List, prefix, 0);
        self.inner.list(prefix).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn typetag_name(&self) -> &'static str {
        "CountingStorage"
    }

    fn typetag_deserialize(&self) {}
}

struct CountingFileRead {
    inner: Box<dyn FileRead>,
    counters: Arc<IcebergIoCounters>,
    class: IcebergFileClass,
}

#[async_trait]
impl FileRead for CountingFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let result = self.inner.read(range).await;
        let (op, bytes) = match &result {
            Ok(returned) => (ranged_read_op(self.class, returned), byte_len(returned)),
            Err(_) => (IcebergIoOp::RangedRead, 0),
        };
        self.counters.record(op, self.class, 1, bytes);
        result
    }
}

struct CountingFileWrite {
    inner: Box<dyn FileWrite>,
    counters: Arc<IcebergIoCounters>,
    class: IcebergFileClass,
}

#[async_trait]
impl FileWrite for CountingFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.counters
            .record(IcebergIoOp::Write, self.class, 0, byte_len(&bs));
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<()> {
        self.counters.record(IcebergIoOp::Write, self.class, 1, 0);
        self.inner.close().await
    }
}
