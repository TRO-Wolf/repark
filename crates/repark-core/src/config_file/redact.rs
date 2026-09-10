use std::collections::{BTreeMap, HashMap};

use crate::catalog_config::prop_key_is_secret;

pub(crate) const REDACTED: &str = "***";

pub(crate) fn redact_value(key: &str, value: &str) -> String {
    if prop_key_is_secret(key) {
        REDACTED.to_string()
    } else {
        value.to_string()
    }
}

#[allow(dead_code)]
pub(crate) fn redact_config(config: &HashMap<String, String>) -> BTreeMap<String, String> {
    config
        .iter()
        .map(|(key, value)| (key.clone(), redact_value(key, value)))
        .collect()
}
