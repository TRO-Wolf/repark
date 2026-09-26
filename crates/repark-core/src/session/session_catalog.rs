use std::collections::HashMap;
use std::hash::BuildHasher;

use crate::catalog_state::session_catalog::SESSION_CATALOG_NAME;
use crate::session::ReparkSession;

pub const AUTO_MEMORY_CATALOG_KEY: &str = "repark.sql.autoMemoryCatalog";

const CATALOG_PREFIXES: [&str; 2] = ["spark.sql.catalog.", "repark.sql.catalog."];

fn names_session_catalog(key: &str) -> bool {
    CATALOG_PREFIXES.iter().any(|prefix| {
        key.strip_prefix(prefix).is_some_and(|rest| {
            rest == SESSION_CATALOG_NAME
                || rest
                    .strip_prefix(SESSION_CATALOG_NAME)
                    .is_some_and(|tail| tail.starts_with('.'))
        })
    })
}

impl ReparkSession {
    #[must_use]
    pub fn auto_session_catalog_wanted<S: BuildHasher>(
        config: &HashMap<String, String, S>,
    ) -> bool {
        let switched_off = config.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case(AUTO_MEMORY_CATALOG_KEY)
                && matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "false" | "0" | "no"
                )
        });
        !switched_off && !config.keys().any(|key| names_session_catalog(key))
    }
}
