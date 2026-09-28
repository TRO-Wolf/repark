use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Column, DFSchema, DataFusionError, Result, TableReference};
use repark_common::names::NameRule;

use super::case_bind::unresolved_column;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Bound,
    Ambiguous,
    Missing,
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_df_names(
    frame_schema: &DFSchema,
    names: &[String],
    rule: NameRule,
) -> Result<Vec<(String, String, String, Disposition)>> {
    names
        .iter()
        .map(|written| resolve_one_name(frame_schema, written, rule))
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn match_display_names(
    written: &[String],
    held: &[String],
    rule: NameRule,
) -> Result<Vec<(String, Vec<String>, Disposition)>> {
    written
        .iter()
        .map(|name| match_one_display(name, held, rule))
        .collect()
}

#[must_use]
pub fn unresolved_subset_name(name: &str, fields: &[String]) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Cannot resolve column name \"{name}\" among ({}).",
        fields.join(", ")
    ))
}

fn resolve_one_name(
    frame_schema: &DFSchema,
    written: &str,
    rule: NameRule,
) -> Result<(String, String, String, Disposition)> {
    let mut whole: Vec<(String, String)> = frame_schema
        .iter()
        .filter(|(_, field)| rule.matches(written, field.name()))
        .map(|(qualifier, field)| {
            (
                qualifier
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                field.name().clone(),
            )
        })
        .collect();
    if whole.len() == 1 {
        let (qualifier, engine) = whole.swap_remove(0);
        return Ok((written.to_string(), qualifier, engine, Disposition::Bound));
    }
    if whole.len() > 1 {
        return settle(
            frame_schema,
            &Column::new_unqualified(written),
            rule,
            Disposition::Ambiguous,
        );
    }
    let probe = Column::from_qualified_name_ignore_case(written);
    if probe.relation.is_none() {
        return settle(
            frame_schema,
            &Column::new_unqualified(written),
            rule,
            Disposition::Missing,
        );
    }
    let mut narrowed: Vec<(String, String)> = frame_schema
        .iter()
        .filter(|(qualifier, field)| {
            qualifier.as_ref().is_some_and(|held| {
                probe
                    .relation
                    .as_ref()
                    .is_some_and(|want| qualifier_matches(want, held, rule))
            }) && rule.matches(&probe.name, field.name())
        })
        .map(|(qualifier, field)| {
            (
                qualifier
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                field.name().clone(),
            )
        })
        .collect();
    if narrowed.len() == 1 {
        let (qualifier, engine) = narrowed.swap_remove(0);
        return Ok((written.to_string(), qualifier, engine, Disposition::Bound));
    }
    let column = Column::new(probe.relation, probe.name);
    if narrowed.is_empty() {
        settle(frame_schema, &column, rule, Disposition::Missing)
    } else {
        settle(frame_schema, &column, rule, Disposition::Ambiguous)
    }
}

fn qualifier_matches(written: &TableReference, held: &TableReference, rule: NameRule) -> bool {
    let want = [written.catalog(), written.schema(), Some(written.table())];
    let held = [held.catalog(), held.schema(), Some(held.table())];
    want.into_iter()
        .zip(held)
        .all(|(want, held)| match (want, held) {
            (Some(want), Some(held)) => rule.matches(want, held),
            (None, _) => true,
            (Some(_), None) => false,
        })
}

fn settle(
    frame_schema: &DFSchema,
    column: &Column,
    rule: NameRule,
    disposition: Disposition,
) -> Result<(String, String, String, Disposition)> {
    if matches!(rule, NameRule::Exact) {
        return Err(unresolved_column(column, frame_schema));
    }
    let written = match &column.relation {
        Some(relation) => format!("{relation}.{}", column.name),
        None => column.name.clone(),
    };
    Ok((written, String::new(), String::new(), disposition))
}

fn match_one_display(
    name: &str,
    held: &[String],
    rule: NameRule,
) -> Result<(String, Vec<String>, Disposition)> {
    let hits: Vec<String> = held
        .iter()
        .filter(|candidate| rule.matches(name, candidate))
        .cloned()
        .collect();
    let disposition = match hits.len() {
        1 => Disposition::Bound,
        0 => Disposition::Missing,
        _ => Disposition::Ambiguous,
    };
    if matches!(rule, NameRule::Exact) && !matches!(disposition, Disposition::Bound) {
        return Err(unresolved_display_miss(name, held));
    }
    Ok((name.to_string(), hits, disposition))
}

fn unresolved_display_miss(name: &str, held: &[String]) -> DataFusionError {
    let mut seen: Vec<&str> = Vec::new();
    for candidate in held {
        if !seen.contains(&candidate.as_str()) {
            seen.push(candidate.as_str());
        }
    }
    let fields = seen
        .into_iter()
        .map(|candidate| Field::new(candidate, DataType::Null, true))
        .collect::<Vec<_>>();
    match DFSchema::from_unqualified_fields(fields.into(), HashMap::new()) {
        Ok(schema) => unresolved_column(&Column::new_unqualified(name), &schema),
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::DFSchema;

    use super::{Disposition, match_display_names, resolve_df_names, unresolved_subset_name};
    use repark_common::names::NameRule::{Exact, IgnoreCase};

    fn schema(names: &[(&str, &str)]) -> DFSchema {
        let fields = names
            .iter()
            .map(|(qualifier, name)| {
                (
                    Some((*qualifier).into()),
                    Arc::new(Field::new(*name, DataType::Int64, true)),
                )
            })
            .collect::<Vec<_>>();
        DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
    }

    fn unresolved(reference: &str, options: &str) -> String {
        format!(
            "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
             function parameter with name {reference} cannot be resolved. Did you mean one of the \
             following? [{options}]. SQLSTATE: 42703"
        )
    }

    #[test]
    fn select_names_bind_and_refuse_by_rule() {
        let frame = schema(&[("t", "id"), ("t", "Data"), ("t", "s")]);
        let bound = resolve_df_names(&frame, &["Data".to_string()], Exact).unwrap();
        assert_eq!(
            bound,
            vec![(
                "Data".to_string(),
                "t".to_string(),
                "Data".to_string(),
                Disposition::Bound
            )]
        );
        let folded = resolve_df_names(&frame, &["ID".to_string()], IgnoreCase).unwrap();
        assert_eq!(
            folded,
            vec![(
                "ID".to_string(),
                "t".to_string(),
                "id".to_string(),
                Disposition::Bound
            )]
        );
        let error = resolve_df_names(&frame, &["ID".to_string()], Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(error, unresolved("`ID`", "`id`, `Data`, `s`"));
        let error = resolve_df_names(&frame, &["nope".to_string()], Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(error, unresolved("`nope`", "`id`, `Data`, `s`"));
        let missing = resolve_df_names(&frame, &["nope".to_string()], IgnoreCase).unwrap();
        assert_eq!(
            missing,
            vec![(
                "nope".to_string(),
                String::new(),
                String::new(),
                Disposition::Missing
            )]
        );
        let twins = schema(&[("t", "id"), ("t", "ID")]);
        let exact = resolve_df_names(&twins, &["id".to_string()], Exact).unwrap();
        assert_eq!(
            exact,
            vec![(
                "id".to_string(),
                "t".to_string(),
                "id".to_string(),
                Disposition::Bound
            )]
        );
    }

    #[test]
    fn qualified_names_split_on_the_last_dot() {
        let single = schema(&[("t", "ID"), ("t", "data")]);
        let narrowed = resolve_df_names(&single, &["l.ID".to_string()], IgnoreCase).unwrap();
        assert_eq!(narrowed[0].3, Disposition::Missing);
        let frame = schema(&[("l", "id"), ("l", "Data"), ("r", "id"), ("r", "Data")]);
        let bound = resolve_df_names(&frame, &["l.ID".to_string()], IgnoreCase).unwrap();
        assert_eq!(
            bound,
            vec![(
                "l.ID".to_string(),
                "l".to_string(),
                "id".to_string(),
                Disposition::Bound
            )]
        );
        let dotted = schema(&[("t", "a.b")]);
        let whole = resolve_df_names(&dotted, &["a.b".to_string()], IgnoreCase).unwrap();
        assert_eq!(
            whole,
            vec![(
                "a.b".to_string(),
                "t".to_string(),
                "a.b".to_string(),
                Disposition::Bound
            )]
        );
        let missing = resolve_df_names(&frame, &["t.nope".to_string()], IgnoreCase).unwrap();
        assert_eq!(missing[0].3, Disposition::Missing);
        let twins = schema(&[("t", "id"), ("t", "ID")]);
        let ambiguous = resolve_df_names(&twins, &["t.id".to_string()], IgnoreCase).unwrap();
        assert_eq!(ambiguous[0].3, Disposition::Ambiguous);
        let error = resolve_df_names(&frame, &["t.id".to_string()], Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(error, unresolved("`t`.`id`", "`id`, `Data`, `id`, `Data`"));
    }

    #[test]
    fn display_names_fan_out_and_the_subset_text_is_legacy() {
        let held = ["id".to_string(), "ID".to_string()];
        let fanned = match_display_names(&["id".to_string()], &held, IgnoreCase).unwrap();
        assert_eq!(
            fanned,
            vec![(
                "id".to_string(),
                vec!["id".to_string(), "ID".to_string()],
                Disposition::Ambiguous
            )]
        );
        let missing = match_display_names(&["nope".to_string()], &held, IgnoreCase).unwrap();
        assert_eq!(missing[0].2, Disposition::Missing);
        let error = match_display_names(&["NOPE".to_string()], &held, Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(error, unresolved("`NOPE`", "`id`, `ID`"));
        let dupes = ["x".to_string(), "x".to_string()];
        let error = match_display_names(&["x".to_string()], &dupes, Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(error, unresolved("`x`", "`x`"));
        let subset = unresolved_subset_name("nope", &held).to_string();
        assert_eq!(
            subset,
            "Error during planning: Cannot resolve column name \"nope\" among (id, ID)."
        );
    }
}
