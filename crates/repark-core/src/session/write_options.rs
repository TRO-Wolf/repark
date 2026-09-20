use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::DataFrame;
use repark_common::Result;

use crate::dialect::{EngineContext, SqlDialect};
use crate::error_map::engine_err_for_sql;
use crate::session::ReparkSession;

impl ReparkSession {
    #[must_use]
    pub fn set_iceberg_session_write_conf(&self, key: &str, value: &str) -> bool {
        let state_lock = self.context().state_ref();
        let mut state = state_lock.write();
        repark_iceberg::write::apply_session_write_key(state.config_mut().options_mut(), key, value)
    }

    #[must_use]
    pub fn unset_iceberg_session_write_conf(&self, key: &str) -> bool {
        let state_lock = self.context().state_ref();
        let mut state = state_lock.write();
        repark_iceberg::write::unset_session_write_key(state.config_mut().options_mut(), key)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn sql_with_write_options(
        &self,
        query: &str,
        options: &HashMap<String, String>,
        overwrite_intent: crate::OverwriteIntent,
    ) -> Result<DataFrame> {
        let dialect = Arc::clone(&self.dialect);
        self.sql_with_write_options_inner(&dialect, query, options, overwrite_intent)
            .await
    }

    pub(crate) async fn sql_with_write_options_inner(
        &self,
        dialect: &Arc<dyn SqlDialect>,
        query: &str,
        options: &HashMap<String, String>,
        overwrite_intent: crate::OverwriteIntent,
    ) -> Result<DataFrame> {
        if let Some(frame) = super::spill::maybe_apply_runtime_set(self.context(), query)? {
            return Ok(frame);
        }
        self.trim_iceberg_caches().await;
        let catalogs = self.catalogs_snapshot();
        let read_only = self.postgres_catalog_names_snapshot();
        let mut cx = EngineContext::new_with_time_zone(
            self.context(),
            &catalogs,
            &read_only,
            self.session_time_zone().as_ref().clone(),
        );
        cx.overwrite_intent = overwrite_intent;
        dialect
            .execute_with_write_options(cx, query, options)
            .await
            .map_err(|error| engine_err_for_sql(query, error))
    }
}
