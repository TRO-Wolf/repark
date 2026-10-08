use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result};

use super::catalog::PostgresSource;
use crate::discover::ScanSource;
use crate::error::{ConnectError, read_only_ddl};
use crate::ident::{PgIdent, QualifiedRelation};

#[derive(Debug)]
pub struct PostgresSchemaProvider {
    source: Arc<PostgresSource>,
    schema: String,
}

impl PostgresSchemaProvider {
    #[must_use]
    pub fn new(source: Arc<PostgresSource>, schema: &str) -> PostgresSchemaProvider {
        PostgresSchemaProvider {
            source,
            schema: schema.to_string(),
        }
    }
}

pub(crate) fn external(error: ConnectError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

impl PostgresSchemaProvider {
    fn named(&self, error: ConnectError) -> DataFusionError {
        external(error).context(format!("database source `{}`", self.source.name()))
    }

    fn read_only(&self) -> DataFusionError {
        DataFusionError::NotImplemented(read_only_ddl(self.source.name()))
    }
}

#[async_trait]
impl SchemaProvider for PostgresSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let schema = PgIdent::new(self.schema.as_str()).map_err(|error| self.named(error))?;
        let table = PgIdent::new(name).map_err(|error| self.named(error))?;
        let relation = ScanSource::Relation(QualifiedRelation::new(schema, table));
        match self.source.resolve(relation).await {
            Ok(table) => Ok(Some(Arc::new(table))),
            Err(ConnectError::RelationNotFound { .. }) => Ok(None),
            Err(error) => Err(self.named(error)),
        }
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(self.read_only())
    }

    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(self.read_only())
    }

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}
