use std::collections::{BTreeMap, HashMap};

pub(crate) use repark_common::redaction::redact_value;

#[allow(dead_code)]
pub(crate) fn redact_config(config: &HashMap<String, String>) -> BTreeMap<String, String> {
    config
        .iter()
        .map(|(key, value)| (key.clone(), redact_value(key, value)))
        .collect()
}
