use std::collections::HashMap;

use datafusion::common::Result;
use datafusion::prelude::DataFrame;

use super::case_bind::rename_output_fields;

const DUPLICATE_PREFIX: &str = "__repark_dup_";

#[must_use]
pub fn duplicate_tolerant_names(displays: &[String]) -> Option<Vec<String>> {
    let mut counts: HashMap<&str, usize> = HashMap::with_capacity(displays.len());
    for display in displays {
        *counts.entry(display.as_str()).or_default() += 1;
    }
    if counts.len() == displays.len() {
        return None;
    }
    Some(
        displays
            .iter()
            .enumerate()
            .map(|(position, display)| {
                if counts.get(display.as_str()).copied().unwrap_or(0) > 1 {
                    format!("{DUPLICATE_PREFIX}{position}_{display}")
                } else {
                    display.clone()
                }
            })
            .collect(),
    )
}

#[must_use]
pub fn display_name(engine: &str) -> Option<&str> {
    let rest = engine.strip_prefix(DUPLICATE_PREFIX)?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    rest[digits..].strip_prefix('_')
}

#[allow(clippy::missing_errors_doc)]
pub fn rename_duplicate_tolerant(frame: DataFrame, displays: &[String]) -> Result<DataFrame> {
    match duplicate_tolerant_names(displays) {
        Some(names) => rename_output_fields(frame, &names),
        None => Ok(frame),
    }
}
