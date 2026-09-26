use std::collections::HashMap;

use datafusion::prelude::SessionConfig;
use repark_core::CatalogRegistry;
use repark_functions::session_names::SessionDefaults;

const SESSION_CATALOG_NAME: &str = "spark_catalog";

pub(crate) fn with_configured_defaults(
    config: SessionConfig,
    conf: &HashMap<String, String>,
) -> SessionConfig {
    let catalog = CatalogRegistry::configured_default_catalog(conf)
        .unwrap_or_else(|| SESSION_CATALOG_NAME.to_string());
    let namespace = CatalogRegistry::default_namespace_for(&catalog).to_string();
    config.with_option_extension(SessionDefaults { catalog, namespace })
}
