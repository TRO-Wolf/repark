//! [`SparkDialect`] adapts the session SQL seam to the Spark statement router.

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use repark_core::{EngineContext, SqlDialect};

/// Route each session `sql()` call through the Spark router.
#[derive(Debug, Clone, Copy, Default)]
pub struct SparkDialect;

#[async_trait(?Send)]
impl SqlDialect for SparkDialect {
    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame> {
        let mut write_options = match cx.overwrite_intent {
            repark_iceberg::write::OverwriteIntent::Session => {
                crate::write_options::StatementWriteOptions::empty()
            }
            intent => crate::write_options::StatementWriteOptions {
                overwrite_intent: intent,
                ..crate::write_options::StatementWriteOptions::empty()
            },
        };
        write_options.verbatim_override = cx.verbatim_override;
        crate::router::execute_in_session(
            cx.ctx,
            cx.catalogs,
            query,
            cx.read_only,
            &write_options,
            cx.temp_views,
        )
        .await
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
        write_options.source_by_name = cx.source_by_name;
        write_options.verbatim_override = cx.verbatim_override;
        crate::router::execute_in_session(
            cx.ctx,
            cx.catalogs,
            query,
            cx.read_only,
            &write_options,
            cx.temp_views,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
