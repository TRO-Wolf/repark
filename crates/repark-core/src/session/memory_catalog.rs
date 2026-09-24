use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use repark_common::{Error, Result};

use crate::catalog_state::{LocationPolicy, memory_warehouse_fallback_root};
use crate::session::ReparkSession;

impl ReparkSession {
    pub(crate) async fn register_memory_catalog_with_props(
        &self,
        name: &str,
        warehouse: &str,
        props: HashMap<String, String>,
    ) -> Result<()> {
        if self
            .catalogs
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_registered(name)
        {
            return Err(Error::DataFusion(format!(
                "catalog '{name}' is already registered — re-registering an in-memory catalog \
                 would orphan its tables (their metadata lives in the replaced handle)"
            )));
        }
        let catalog = self.memory_catalog_handle(warehouse, props).await?;
        let root = memory_warehouse_fallback_root(warehouse);
        self.register_iceberg_catalog_with_policy(
            name,
            catalog,
            LocationPolicy::TempFallbackAllowed { root: root.clone() },
        )
        .await?;
        let mut catalogs = RwLock::write(&self.catalogs).unwrap_or_else(PoisonError::into_inner);
        catalogs.note_local_warehouse_root(warehouse.to_string());
        catalogs.set_warehouse_layout_root(name, root);
        Ok(())
    }
}
