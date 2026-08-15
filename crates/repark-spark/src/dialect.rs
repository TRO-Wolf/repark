//! [`SparkDialect`] — the Spark door's [`SqlDialect`] impl (seam-adaptation edit class).
//!
//! v1's `ReparkSession::sql` called `repark_sql::execute_with_read_only(ctx, catalogs, query,
//! read_only)` positionally; the phase-1 seam hands the same three inputs as an
//! [`EngineContext`] struct. This adapter unpacks the struct back into that positional call —
//! nothing else. Install it with `ReparkSessionBuilder::with_sql_dialect` (paired with the
//! `SparkExtension` for full v1 Spark semantics — extensions are session-scoped).

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use repark_core::{EngineContext, SqlDialect};

/// ===========================================================================================
/// The Spark statement front end: routes every `sql()` call through the ported v1 router
/// ([`crate::execute_with_read_only`]).
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
