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
}

#[cfg(test)]
mod tests;
