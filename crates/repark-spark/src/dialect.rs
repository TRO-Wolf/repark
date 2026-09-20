//! [`SparkDialect`] adapts the session SQL seam to the Spark statement router.

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use repark_core::{EngineContext, SqlDialect};

/// Route each session `sql()` call through the Spark router.
#[derive(Debug, Clone, Copy, Default)]
pub struct SparkDialect;

#[async_trait(?Send)]
impl SqlDialect for SparkDialect {
    fn on_session_built(&self, ctx: &datafusion::prelude::SessionContext) {
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
