use std::sync::Arc;

use datafusion::prelude::DataFrame;
use repark_common::Result;

use crate::dialect::{EngineContext, SqlDialect};
use crate::engine_err_for_sql;
use crate::session::ReparkSession;
use crate::session::spill::maybe_apply_runtime_set;

pub(crate) async fn sql_with_overwrite_flag(
    session: &ReparkSession,
    dialect: &Arc<dyn SqlDialect>,
    query: &str,
    force_static_overwrite: bool,
) -> Result<DataFrame> {
    // Intercept SET of `datafusion.runtime.memory_limit` and refuse SET of `temp_directory`.
    if let Some(frame) = maybe_apply_runtime_set(session.context(), query)? {
        return Ok(frame);
    }
    session.trim_iceberg_caches();
    // Clone the registry (cheap — keys + `Arc`s) so no lock is held across the `await`.
    let catalogs = session.catalogs_snapshot();
    let read_only = session.postgres_catalog_names_snapshot();
    dialect
        .execute(
            EngineContext {
                ctx: session.context(),
                catalogs: &catalogs,
                read_only: &read_only,
                force_static_overwrite,
            },
            query,
        )
        .await
        .map_err(|error| engine_err_for_sql(query, error))
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn sql_static_overwrite(&self, query: &str) -> Result<DataFrame> {
        let dialect = Arc::clone(&self.dialect);
        sql_with_overwrite_flag(self, &dialect, query, true).await
    }
}
