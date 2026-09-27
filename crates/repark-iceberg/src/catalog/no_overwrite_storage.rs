use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::Result;
use iceberg::io::{
    FileInfo, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

#[derive(Debug, Clone)]
pub struct NoOverwriteStorageFactory {
    inner: Arc<dyn StorageFactory>,
}

impl NoOverwriteStorageFactory {
    #[must_use]
    pub fn new(inner: Arc<dyn StorageFactory>) -> Self {
        Self { inner }
    }

    #[must_use]
    pub fn inner(&self) -> &Arc<dyn StorageFactory> {
        &self.inner
    }
}

impl Serialize for NoOverwriteStorageFactory {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("NoOverwriteStorageFactory", 1)?;
        state.serialize_field("inner", &*self.inner)?;
        state.end()
    }
}

impl StorageFactory for NoOverwriteStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(NoOverwriteStorage {
            inner: self.inner.build(config)?,
        }))
    }

    fn typetag_name(&self) -> &'static str {
        "NoOverwriteStorageFactory"
    }

    fn typetag_deserialize(&self) {}
}

#[derive(Debug, Clone)]
pub struct NoOverwriteStorage {
    inner: Arc<dyn Storage>,
}

impl NoOverwriteStorage {
    #[must_use]
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self { inner }
    }
}

impl Serialize for NoOverwriteStorage {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("NoOverwriteStorage", 1)?;
        state.serialize_field("inner", &*self.inner)?;
        state.end()
    }
}

#[async_trait]
impl Storage for NoOverwriteStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.inner.metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.inner.read(path).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.inner.reader(path).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        if self.inner.exists(path).await? {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("second write of an existing path refused: {path}"),
            ));
        }
        self.inner.write(path, bs).await
    }

    async fn write_new(&self, path: &str, bs: Bytes) -> Result<()> {
        self.inner.write_new(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.inner.writer(path).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.inner.delete_prefix(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<FileInfo>> {
        self.inner.list(prefix).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn typetag_name(&self) -> &'static str {
        "NoOverwriteStorage"
    }

    fn typetag_deserialize(&self) {}
}
