use std::collections::{HashMap, HashSet};

use arrow::datatypes::Schema;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Column, Result, plan_err};
use datafusion::logical_expr::Expr;
use datafusion::prelude::DataFrame;

pub const DISPLAY_NAME_KEY: &str = "repark.display";

#[must_use]
pub fn duplicate_tolerant_names(displays: &[String]) -> Option<Vec<String>> {
    let mut counts: HashMap<&str, usize> = HashMap::with_capacity(displays.len());
    for display in displays {
        *counts.entry(display.as_str()).or_default() += 1;
    }
    if counts.len() == displays.len() {
        return None;
    }
    let mut taken: HashSet<String> = displays.iter().cloned().collect();
    Some(
        displays
            .iter()
            .enumerate()
            .map(|(position, display)| {
                if counts.get(display.as_str()).copied().unwrap_or(0) < 2 {
                    return display.clone();
                }
                let mut name = format!("__repark_dup_{position}_{display}");
                while !taken.insert(name.clone()) {
                    name.push('_');
                }
                name
            })
            .collect(),
    )
}

#[allow(clippy::missing_errors_doc)]
pub fn rename_duplicate_tolerant(frame: DataFrame, displays: &[String]) -> Result<DataFrame> {
    let Some(names) = duplicate_tolerant_names(displays) else {
        return Ok(frame);
    };
    let schema = frame.schema();
    if names.len() != schema.fields().len() {
        return plan_err!(
            "rename needs one name per output field: {} fields, {} names",
            schema.fields().len(),
            names.len()
        );
    }
    let projection = schema
        .iter()
        .zip(names.iter().zip(displays.iter()))
        .map(|((qualifier, field), (name, display))| {
            let recorded = FieldMetadata::from(HashMap::from([(
                DISPLAY_NAME_KEY.to_string(),
                display.clone(),
            )]));
            Expr::Column(Column::new(qualifier.cloned(), field.name()))
                .alias_with_metadata(name, Some(recorded))
        })
        .collect::<Vec<_>>();
    frame.select(projection)
}

#[must_use]
pub fn recorded_display_names(schema: &Schema) -> Option<Vec<String>> {
    if !schema
        .fields()
        .iter()
        .any(|field| field.metadata().contains_key(DISPLAY_NAME_KEY))
    {
        return None;
    }
    Some(
        schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .metadata()
                    .get(DISPLAY_NAME_KEY)
                    .unwrap_or(field.name())
                    .clone()
            })
            .collect(),
    )
}
