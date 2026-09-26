use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result};
use iceberg_datafusion::IcebergTableProvider;

pub(crate) struct UuidTextSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
}

impl UuidTextSchemaProvider {
    pub(crate) fn wrap(inner: Arc<dyn SchemaProvider>) -> Arc<dyn SchemaProvider> {
        Arc::new(Self { inner })
    }
}

impl Debug for UuidTextSchemaProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UuidTextSchemaProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for UuidTextSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let Some(provider) = self.inner.table(name).await? else {
            return Ok(None);
        };
        let any = provider.as_ref() as &dyn std::any::Any;
        if let Some(iceberg) = any.downcast_ref::<IcebergTableProvider>() {
            let presented = iceberg.clone().with_uuid_as_string(true);
            return Ok(Some(Arc::new(presented)));
        }
        Ok(Some(provider))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}
