//! [`AnsiDialect`] — the ANSI door's [`SqlDialect`] implementation.

use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};
use repark_core::{EngineContext, SqlDialect};

/// The ANSI/Trino-flavoured statement front end.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnsiDialect;

#[async_trait(?Send)]
impl SqlDialect for AnsiDialect {
    fn on_session_built(&self, ctx: &SessionContext) {
        repark_functions::integer_spark::install_integer_overflow(ctx);
        repark_functions::spark_log1p::register(ctx);
    }

    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame> {
        crate::router::execute(cx, query).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use datafusion::prelude::SessionContext;
    use repark_core::CatalogRegistry;

    use super::*;

    /// The dialect reaches the router: a plain read runs through `execute` unchanged.
    #[tokio::test]
    async fn ansi_dialect_execute_runs_the_router() {
        let ctx = SessionContext::new();
        let catalogs = CatalogRegistry::new();
        let read_only = HashSet::new();
        let frame = AnsiDialect
            .execute(
                EngineContext::new(&ctx, &catalogs, &read_only),
                "SELECT 1 AS a",
            )
            .await
            .expect("a plain SELECT must run");
        let batches = frame.collect().await.expect("collect");
        assert_eq!(batches[0].num_rows(), 1);
    }

    /// The dialect carries the router's guards, so scripts refuse through the seam.
    #[tokio::test]
    async fn dialect_carries_the_guard_set() {
        let ctx = SessionContext::new();
        let catalogs = CatalogRegistry::new();
        let read_only = HashSet::new();
        let err = AnsiDialect
            .execute(
                EngineContext::new(&ctx, &catalogs, &read_only),
                "SELECT 1; SELECT 2",
            )
            .await
            .expect_err("a script must refuse through the seam")
            .to_string();
        assert!(err.contains("[PARSE_SYNTAX_ERROR]"), "guard class: {err}");
    }
}
