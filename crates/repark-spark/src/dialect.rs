//! [`SparkDialect`] adapts the session SQL seam to the Spark statement router.

use async_trait::async_trait;
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::DataFrame;
use repark_core::{EngineContext, SqlDialect};
use std::sync::Arc;

/// Route each session `sql()` call through the Spark router.
#[derive(Debug, Clone, Copy, Default)]
pub struct SparkDialect;

#[async_trait(?Send)]
impl SqlDialect for SparkDialect {
    fn on_session_built(&self, ctx: &datafusion::prelude::SessionContext) {
        let default_schema = {
            let builtin = datafusion::prelude::SessionConfig::new()
                .options()
                .catalog
                .clone();
            let state = ctx.state_ref();
            let mut guard = state.write();
            let options = guard.config_mut().options_mut();
            if options.catalog.default_catalog == builtin.default_catalog {
                options.catalog.default_catalog = "spark_catalog".to_string();
            }
            if options.catalog.default_schema == builtin.default_schema {
                options.catalog.default_schema = "default".to_string();
            }
            options.catalog.default_schema.clone()
        };
        if ctx.catalog("spark_catalog").is_none() {
            let provider = datafusion::catalog::MemoryCatalogProvider::new();
            if provider
                .register_schema(
                    &default_schema,
                    Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
                )
                .is_ok()
            {
                ctx.register_catalog("spark_catalog", Arc::new(provider));
            }
        }
    }

    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame> {
        match cx.overwrite_intent {
            repark_iceberg::write::OverwriteIntent::Session => {
                crate::execute_with_read_only(cx.ctx, cx.catalogs, query, cx.read_only).await
            }
            intent => {
                let write_options = crate::write_options::StatementWriteOptions {
                    overwrite_intent: intent,
                    ..crate::write_options::StatementWriteOptions::empty()
                };
                crate::execute_with_statement_options(
                    cx.ctx,
                    cx.catalogs,
                    query,
                    cx.read_only,
                    &write_options,
                )
                .await
            }
        }
    }

    async fn execute_with_write_options(
        &self,
        cx: EngineContext<'_>,
        query: &str,
        options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<DataFrame> {
        let mut pairs: Vec<(String, String)> = options
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        pairs.sort();
        let mut write_options = crate::write_options::StatementWriteOptions::validate(pairs)?;
        write_options.overwrite_intent = cx.overwrite_intent;
        crate::execute_with_statement_options(
            cx.ctx,
            cx.catalogs,
            query,
            cx.read_only,
            &write_options,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
