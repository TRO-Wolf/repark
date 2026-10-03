use std::collections::HashSet;

use datafusion::common::{DataFusionError, Result};
use repark_common::java_case::string_lowered;
use repark_common::names::{NameRule, column_already_exists};

#[allow(clippy::missing_errors_doc)]
pub fn refuse_folded_duplicate_keys(keys: &[String], rule: NameRule) -> Result<()> {
    if matches!(rule, NameRule::Exact) {
        return Ok(());
    }
    let mut seen = HashSet::with_capacity(keys.len());
    for key in keys {
        let lowered = string_lowered(key);
        if seen.contains(&lowered) {
            return Err(DataFusionError::Plan(column_already_exists(&lowered)));
        }
        seen.insert(lowered);
    }
    Ok(())
}
