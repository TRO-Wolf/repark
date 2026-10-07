use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result};

use super::catalog::PostgresSource;
use crate::discover::ScanSource;
use crate::error::ConnectError;
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

#[async_trait]
impl SchemaProvider for PostgresSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let schema = PgIdent::new(self.schema.as_str()).map_err(external)?;
        let table = PgIdent::new(name).map_err(external)?;
        let relation = ScanSource::Relation(QualifiedRelation::new(schema, table));
        match self.source.resolve(relation).await {
            Ok(table) => Ok(Some(Arc::new(table))),
            Err(ConnectError::RelationNotFound { .. }) => Ok(None),
            Err(error) => Err(external(error)),
        }
    }

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}
