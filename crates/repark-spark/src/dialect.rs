//! [`SparkDialect`] adapts the session SQL seam to the Spark statement router.
//!
//! Install it with `ReparkSessionBuilder::with_sql_dialect` and pair it with `SparkExtension`.

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use repark_core::{EngineContext, SqlDialect};

/// ===========================================================================================
/// Route each session `sql()` call through the Spark router.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, Default)]
pub struct SparkDialect;

#[async_trait(?Send)]
impl SqlDialect for SparkDialect {
    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame> {
        crate::execute_with_read_only(cx.ctx, cx.catalogs, query, cx.read_only).await
    }
}

#[cfg(test)]
mod tests;
