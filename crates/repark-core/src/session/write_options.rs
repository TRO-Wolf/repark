use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::DataFrame;
use repark_common::Result;

use crate::dialect::{EngineContext, SqlDialect};
use crate::error_map::engine_err_for_sql;
use crate::session::ReparkSession;

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn sql_with_write_options(
        &self,
        query: &str,
        options: &HashMap<String, String>,
        force_static_overwrite: bool,
    ) -> Result<DataFrame> {
        let dialect = Arc::clone(&self.dialect);
        self.sql_with_write_options_inner(&dialect, query, options, force_static_overwrite)
            .await
    }

    pub(crate) async fn sql_with_write_options_inner(
        &self,
        dialect: &Arc<dyn SqlDialect>,
        query: &str,
        options: &HashMap<String, String>,
        force_static_overwrite: bool,
    ) -> Result<DataFrame> {
        if let Some(frame) = super::spill::maybe_apply_runtime_set(self.context(), query)? {
            return Ok(frame);
        }
        self.trim_iceberg_caches();
        let catalogs = self.catalogs_snapshot();
        let read_only = self.postgres_catalog_names_snapshot();
        dialect
            .execute_with_write_options(
                EngineContext {
                    ctx: self.context(),
                    catalogs: &catalogs,
                    read_only: &read_only,
                    force_static_overwrite,
                },
                query,
                options,
            )
            .await
            .map_err(|error| engine_err_for_sql(query, error))
    }
}
