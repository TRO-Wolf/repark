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

#[must_use]
pub fn duplicate_display_names<'a>(
    engines: impl IntoIterator<Item = &'a str> + Clone,
) -> Option<Vec<String>> {
    if !engines
        .clone()
        .into_iter()
        .any(|engine| display_name(engine).is_some())
    {
        return None;
    }
    Some(
        engines
            .into_iter()
            .map(|engine| display_name(engine).unwrap_or(engine).to_string())
            .collect(),
    )
}

#[must_use]
pub fn first_duplicate_display<'a>(engines: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    engines.into_iter().find_map(display_name)
}
