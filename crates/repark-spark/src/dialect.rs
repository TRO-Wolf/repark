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
        if cx.force_static_overwrite {
            crate::execute_static_overwrite(cx.ctx, cx.catalogs, query, cx.read_only).await
        } else {
            crate::execute_with_read_only(cx.ctx, cx.catalogs, query, cx.read_only).await
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
        write_options.force_static_overwrite = cx.force_static_overwrite;
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
