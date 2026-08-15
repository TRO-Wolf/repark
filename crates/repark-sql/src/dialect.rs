//! [`AnsiDialect`] — the ANSI door's [`SqlDialect`] implementation.
//!
//! The seam is frozen at phase-2 start (design §3): `execute(EngineContext<'_>, &str)`, nothing
//! more. This adapter is deliberately a one-liner onto [`crate::router::execute`] — all the
//! behavior lives in the router and its handlers, so the seam stays the thing it was frozen as.
//!
//! Install it as the session default with `ReparkSessionBuilder::with_sql_dialect`, or pass it to
//! `ReparkSession::sql_with` to run one statement through this door on a session whose default is
//! another. **There is no `SessionExtension`**: this door installs nothing into the session,
//! because native/ANSI semantics ARE stock DataFusion. That asymmetry with the Spark door is the
//! design, not an omission — and it is exactly why cross-door equivalence rows must use TWO
//! sessions (extensions are session-scoped, so a Spark-extended session has Spark expression
//! semantics through every door, including this one).

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use repark_core::{EngineContext, SqlDialect};

/// ===========================================================================================
/// The ANSI/Trino-flavoured statement front end.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, Default)]
pub struct AnsiDialect;

#[async_trait(?Send)]
impl SqlDialect for AnsiDialect {
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

    /// The dialect carries the router's guards — a script refuses through the seam, not only
    /// through the router's own entry point.
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
