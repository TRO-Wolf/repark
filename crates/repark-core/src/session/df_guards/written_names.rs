use std::collections::HashMap;
use std::ops::ControlFlow;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Column, DFSchema, DataFusionError, Result, TableReference};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Ident, visit_expressions_mut};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use repark_common::names::{NameRule, column_already_exists, folded_duplicate};

use super::case_bind::{Hit, ambiguous_reference, unresolved_column};
use super::sort_names::written_column;

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

#[allow(clippy::missing_errors_doc)]
pub fn match_resolver_names(
    written: &[String],
    held: &[String],
    rule: NameRule,
) -> Result<Vec<(String, Vec<String>, Disposition)>> {
    written
        .iter()
        .map(|name| match_one_display_by(name, held, rule, NameRule::resolver_matches))
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn match_subset_names(
    written: &[String],
    held: &[String],
    rule: NameRule,
) -> Result<Vec<(String, Vec<String>)>> {
    written
        .iter()
        .map(|name| match_one_subset(name, held, rule))
        .collect()
}

#[must_use]
pub fn rewrite_join_condition_aliases(
    left_schema: &DFSchema,
    right_schema: &DFSchema,
    condition_sql: &str,
    left_view: &str,
    right_view: &str,
    rule: NameRule,
) -> String {
    let Ok(mut parser) = Parser::new(&DatabricksDialect {}).try_with_sql(condition_sql) else {
        return condition_sql.to_string();
    };
    let Ok(mut condition) = parser.parse_expr() else {
        return condition_sql.to_string();
    };
    if parser.peek_token().token != Token::EOF {
        return condition_sql.to_string();
    }
    let mut rewritten = false;
    let _ = visit_expressions_mut(&mut condition, |node| {
        if let SqlExpr::CompoundIdentifier(parts) = node
            && let [qualifier, name] = parts.as_slice()
            && let Some((view, engine)) = join_alias_target(
                left_schema,
                right_schema,
                qualifier.value.as_str(),
                name.value.as_str(),
                left_view,
                right_view,
                rule,
            )
        {
            *node =
                SqlExpr::CompoundIdentifier(vec![Ident::new(view), Ident::with_quote('`', engine)]);
            rewritten = true;
        }
        ControlFlow::<()>::Continue(())
    });
    if rewritten {
        condition.to_string()
    } else {
        condition_sql.to_string()
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_qualified_display_names(
    frame_schema: &DFSchema,
    displays: &[String],
    names: &[String],
    rule: NameRule,
) -> Result<Vec<(String, String, Disposition)>> {
    names
        .iter()
        .map(|written| resolve_one_display(frame_schema, displays, written, rule))
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_ambiguous_display_name(
    frame_schema: &DFSchema,
    displays: &[String],
    written: &str,
    rule: NameRule,
) -> Result<()> {
    let paired = frame_schema.fields().len() == displays.len();
    let column = Column::from_qualified_name_ignore_case(written);
    let mut hits: Vec<Hit<'_>> = Vec::new();
    for (index, display) in displays.iter().enumerate() {
        if !rule.matches(written, display) {
            continue;
        }
        if paired {
            let (qualifier, field) = frame_schema.qualified_field(index);
            hits.push((qualifier, field.as_ref()));
        } else if let Some(field) = frame_schema.fields().first() {
            hits.push((None, field.as_ref()));
        }
    }
    if hits.len() < 2 {
        return Ok(());
    }
    Err(ambiguous_reference(&column, &hits))
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_folded_duplicate_keys(keys: &[String], rule: NameRule) -> Result<()> {
    if matches!(rule, NameRule::Exact) {
        return Ok(());
    }
    if let Some(twin) = folded_duplicate(keys) {
        return Err(DataFusionError::Plan(column_already_exists(&twin)));
    }
    Ok(())
}

#[must_use]
pub fn unresolved_display_name(written: &[String], held: &[String]) -> DataFusionError {
    let column = match written {
        [table, name] => Column::new(Some(TableReference::bare(table.as_str())), name),
        [schema, table, name] => Column::new(
            Some(TableReference::partial(schema.as_str(), table.as_str())),
            name,
        ),
        _ => Column::new_unqualified(written.join(".")),
    };
    unresolved_display_miss(&column, held)
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
    let shown = if written.contains('`') {
        written_column(written)
    } else {
        Column::new_unqualified(written)
    };
    if whole.len() > 1 {
        return settle(frame_schema, &shown, rule, Disposition::Ambiguous);
    }
    let probe = Column::from_qualified_name_ignore_case(written);
    if probe.relation.is_none() {
        return settle(frame_schema, &shown, rule, Disposition::Missing);
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
    let column = if written.contains('`') {
        shown
    } else {
        Column::new(probe.relation, probe.name)
    };
    if narrowed.is_empty() {
        settle(frame_schema, &column, rule, Disposition::Missing)
    } else {
        settle(frame_schema, &column, rule, Disposition::Ambiguous)
    }
}

pub(super) fn qualifier_matches(
    written: &TableReference,
    held: &TableReference,
    rule: NameRule,
) -> bool {
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

fn join_alias_target<'a>(
    left_schema: &DFSchema,
    right_schema: &DFSchema,
    qualifier: &str,
    name: &str,
    left_view: &'a str,
    right_view: &'a str,
    rule: NameRule,
) -> Option<(&'a str, String)> {
    let want = TableReference::Bare {
        table: qualifier.into(),
    };
    match (
        side_engine(left_schema, &want, name, rule),
        side_engine(right_schema, &want, name, rule),
    ) {
        (Some(engine), None) => Some((left_view, engine)),
        (None, Some(engine)) => Some((right_view, engine)),
        _ => None,
    }
}

fn side_engine(
    schema: &DFSchema,
    want: &TableReference,
    name: &str,
    rule: NameRule,
) -> Option<String> {
    let mut hits = schema
        .iter()
        .filter(|(held, field)| {
            held.as_ref()
                .is_some_and(|held| qualifier_matches(want, held, rule))
                && rule.matches(name, field.name())
        })
        .map(|(_, field)| field.name().clone());
    let engine = hits.next()?;
    hits.next().is_none().then_some(engine)
}

fn resolve_one_display(
    frame_schema: &DFSchema,
    displays: &[String],
    written: &str,
    rule: NameRule,
) -> Result<(String, String, Disposition)> {
    let paired = frame_schema.fields().len() == displays.len();
    if written.contains('`') && written_column(written).relation.is_none() {
        return Ok((written.to_string(), String::new(), Disposition::Missing));
    }
    let Some((qualifier, last_segment)) = written.rsplit_once('.') else {
        return Ok((written.to_string(), String::new(), Disposition::Missing));
    };
    if !paired {
        return Ok((written.to_string(), String::new(), Disposition::Missing));
    }
    let want = TableReference::Bare {
        table: qualifier.into(),
    };
    let mut distinct: Vec<Hit<'_>> = Vec::new();
    for ((held, field), _) in
        frame_schema
            .iter()
            .zip(displays.iter())
            .filter(|((held, _), display)| {
                held.as_ref()
                    .is_some_and(|held| qualifier_matches(&want, held, rule))
                    && rule.matches(last_segment, display)
            })
    {
        if !distinct.iter().any(|(_, seen)| seen.name() == field.name()) {
            distinct.push((held, field.as_ref()));
        }
    }
    let column = Column::new(Some(want), last_segment);
    match distinct.as_slice() {
        [] if matches!(rule, NameRule::Exact) => Err(unresolved_display_miss(&column, displays)),
        [] => Ok((written.to_string(), String::new(), Disposition::Missing)),
        [(_, field)] => Ok((
            written.to_string(),
            field.name().clone(),
            Disposition::Bound,
        )),
        _ => Err(ambiguous_reference(&column, &distinct)),
    }
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

fn match_one_subset(name: &str, held: &[String], rule: NameRule) -> Result<(String, Vec<String>)> {
    let hits: Vec<String> = held
        .iter()
        .filter(|candidate| rule.resolver_matches(name, candidate))
        .cloned()
        .collect();
    if hits.is_empty() {
        return Err(unresolved_subset_name(name, held));
    }
    Ok((name.to_string(), hits))
}

fn match_one_display(
    name: &str,
    held: &[String],
    rule: NameRule,
) -> Result<(String, Vec<String>, Disposition)> {
    match_one_display_by(name, held, rule, NameRule::matches)
}

fn match_one_display_by(
    name: &str,
    held: &[String],
    rule: NameRule,
    same: fn(NameRule, &str, &str) -> bool,
) -> Result<(String, Vec<String>, Disposition)> {
    let hits: Vec<String> = held
        .iter()
        .filter(|candidate| same(rule, name, candidate))
        .cloned()
        .collect();
    let disposition = match hits.len() {
        1 => Disposition::Bound,
        0 => Disposition::Missing,
        _ => Disposition::Ambiguous,
    };
    if matches!(rule, NameRule::Exact) && !matches!(disposition, Disposition::Bound) {
        let column = if name.contains('`') {
            written_column(name)
        } else {
            Column::new_unqualified(name)
        };
        return Err(unresolved_display_miss(&column, held));
    }
    Ok((name.to_string(), hits, disposition))
}

fn unresolved_display_miss(column: &Column, held: &[String]) -> DataFusionError {
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
        Ok(schema) => unresolved_column(column, &schema),
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{DFSchema, TableReference};

    use super::{
        Disposition, match_display_names, match_resolver_names, match_subset_names,
        refuse_ambiguous_display_name, refuse_folded_duplicate_keys, resolve_df_names,
        resolve_qualified_display_names, rewrite_join_condition_aliases, unresolved_subset_name,
    };
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

    fn ambiguous(reference: &str, options: &str) -> String {
        format!(
            "Error during planning: [AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, \
             could be: [{options}]. SQLSTATE: 42704"
        )
    }

    fn bare_schema(names: &[&str]) -> DFSchema {
        let fields = names
            .iter()
            .map(|name| (None, Arc::new(Field::new(*name, DataType::Int64, true))))
            .collect::<Vec<_>>();
        DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
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
    fn bare_twin_select_refuses_spark_ambiguous() {
        let frame = schema(&[
            ("__repark_cdf_left", "id"),
            ("__repark_cdf_left", "Data"),
            ("__repark_cdf_right", "id"),
            ("__repark_cdf_right", "Val"),
        ]);
        let displays = ["id", "Data", "id", "Val"].map(str::to_string);
        let error = refuse_ambiguous_display_name(&frame, &displays, "ID", IgnoreCase)
            .unwrap_err()
            .to_string();
        assert_eq!(error, ambiguous("`ID`", "`ID`, `ID`"));
        let single = refuse_ambiguous_display_name(&frame, &displays, "Val", IgnoreCase);
        assert!(single.is_ok());
        let engine = bare_schema(&["left_id", "right_id"]);
        let twins = ["ID", "id"].map(str::to_string);
        let error = refuse_ambiguous_display_name(&engine, &twins, "iD", IgnoreCase)
            .unwrap_err()
            .to_string();
        assert_eq!(error, ambiguous("`iD`", "`iD`, `iD`"));
        let short = bare_schema(&["left_id"]);
        let error = refuse_ambiguous_display_name(&short, &twins, "iD", IgnoreCase)
            .unwrap_err()
            .to_string();
        assert_eq!(error, ambiguous("`iD`", "`iD`, `iD`"));
    }

    #[test]
    fn aliased_twin_select_names_qualified_candidates() {
        let frame = schema(&[("l", "id"), ("l", "Name"), ("r", "id"), ("r", "Name")]);
        let displays = ["id", "Name", "id", "Name"].map(str::to_string);
        let error = refuse_ambiguous_display_name(&frame, &displays, "ID", IgnoreCase)
            .unwrap_err()
            .to_string();
        assert_eq!(error, ambiguous("`ID`", "`l`.`ID`, `r`.`ID`"));
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

    #[test]
    fn subset_names_fan_out_and_miss_with_the_legacy_text() {
        let twins = ["id".to_string(), "ID".to_string()];
        let fanned = match_subset_names(&["id".to_string()], &twins, IgnoreCase).unwrap();
        assert_eq!(
            fanned,
            vec![("id".to_string(), vec!["id".to_string(), "ID".to_string()])]
        );
        let error = match_subset_names(&["nope".to_string()], &twins, IgnoreCase)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: Cannot resolve column name \"nope\" among (id, ID)."
        );
        let exact = match_subset_names(&["id".to_string()], &twins, Exact).unwrap();
        assert_eq!(exact, vec![("id".to_string(), vec!["id".to_string()])]);
        let doubled = ["v".to_string(), "v".to_string()];
        let exact_twins = match_subset_names(&["v".to_string()], &doubled, Exact).unwrap();
        assert_eq!(exact_twins, vec![("v".to_string(), doubled.to_vec())]);
        let pair = ["id".to_string(), "Data".to_string()];
        let error = match_subset_names(&["ID".to_string()], &pair, Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: Cannot resolve column name \"ID\" among (id, Data)."
        );
        let triple = ["id".to_string(), "Data".to_string(), "s".to_string()];
        let error = match_subset_names(&["NOPE".to_string()], &triple, Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: Cannot resolve column name \"NOPE\" among (id, Data, s)."
        );
    }

    #[test]
    fn join_condition_aliases_rebind_through_the_side_schemas() {
        let left = schema(&[("l", "id"), ("l", "Data")]);
        let right = schema(&[("r", "id"), ("r", "Data")]);
        let folded = rewrite_join_condition_aliases(
            &left,
            &right,
            "(`l`.`ID` = `r`.`ID`)",
            "JL",
            "JR",
            IgnoreCase,
        );
        assert_eq!(folded, "(JL.`id` = JR.`id`)");
        let folded_qualifier = rewrite_join_condition_aliases(
            &left,
            &right,
            "(`L`.`ID` = `R`.`ID`)",
            "JL",
            "JR",
            IgnoreCase,
        );
        assert_eq!(folded_qualifier, "(JL.`id` = JR.`id`)");
        let exact = rewrite_join_condition_aliases(
            &left,
            &right,
            "(`l`.`id` = `r`.`id`)",
            "JL",
            "JR",
            Exact,
        );
        assert_eq!(exact, "(JL.`id` = JR.`id`)");
        assert_eq!(
            rewrite_join_condition_aliases(
                &left,
                &right,
                "(`l`.`ID` = `r`.`ID`)",
                "JL",
                "JR",
                Exact
            ),
            "(`l`.`ID` = `r`.`ID`)"
        );
        assert_eq!(
            rewrite_join_condition_aliases(
                &left,
                &right,
                "(`L`.`id` = `r`.`id`)",
                "JL",
                "JR",
                Exact
            ),
            "(`L`.`id` = JR.`id`)"
        );
        assert_eq!(
            rewrite_join_condition_aliases(
                &left,
                &right,
                "(`L`.`id` = `r`.`id`)",
                "JL",
                "JR",
                IgnoreCase
            ),
            "(JL.`id` = JR.`id`)"
        );
        for rule in [Exact, IgnoreCase] {
            assert_eq!(
                rewrite_join_condition_aliases(
                    &left,
                    &right,
                    "(`x`.`id` = `r`.`id`)",
                    "JL",
                    "JR",
                    rule
                ),
                "(`x`.`id` = JR.`id`)"
            );
        }
        let shared_only = schema(&[("x", "id")]);
        assert_eq!(
            rewrite_join_condition_aliases(
                &shared_only,
                &shared_only,
                "(`x`.`id` = `x`.`id`)",
                "JL",
                "JR",
                IgnoreCase
            ),
            "(`x`.`id` = `x`.`id`)"
        );
    }

    #[test]
    fn join_condition_aliases_leave_other_references_untouched() {
        let left = schema(&[("l", "id"), ("l", "Data")]);
        let right = schema(&[("r", "id"), ("r", "Data")]);
        for rule in [Exact, IgnoreCase] {
            for untouched in [
                "(`id` = `Data`)",
                "(JL.`id` = JR.`id`)",
                "(`l`.`id` =",
                "l.id = r.id AND l.Data = 'a' 'b'",
                "l.id = r.id ORDER BY 1",
            ] {
                assert_eq!(
                    rewrite_join_condition_aliases(&left, &right, untouched, "JL", "JR", rule),
                    untouched
                );
            }
        }
        let twins = schema(&[("l", "id"), ("l", "ID")]);
        assert_eq!(
            rewrite_join_condition_aliases(
                &twins,
                &right,
                "(`l`.`id` = `r`.`id`)",
                "JL",
                "JR",
                IgnoreCase
            ),
            "(`l`.`id` = JR.`id`)"
        );
    }

    #[test]
    fn qualified_display_names_pair_schema_positions_with_displays() {
        let child = schema(&[("l", "e0"), ("l", "e1"), ("r", "e2"), ("r", "e3")]);
        let displays = ["id", "Data", "id", "Data"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let bound = resolve_qualified_display_names(
            &child,
            &displays,
            &["l.ID".to_string(), "r.data".to_string()],
            IgnoreCase,
        )
        .unwrap();
        assert_eq!(
            bound,
            vec![
                ("l.ID".to_string(), "e0".to_string(), Disposition::Bound),
                ("r.data".to_string(), "e3".to_string(), Disposition::Bound),
            ]
        );
        let missing = resolve_qualified_display_names(
            &child,
            &displays,
            &["x.id".to_string(), "l.nope".to_string(), "id".to_string()],
            IgnoreCase,
        )
        .unwrap();
        assert!(missing.iter().all(|row| row.2 == Disposition::Missing));
        let exact =
            resolve_qualified_display_names(&child, &displays, &["l.id".to_string()], Exact)
                .unwrap();
        assert_eq!(
            exact,
            vec![("l.id".to_string(), "e0".to_string(), Disposition::Bound)]
        );
        let error =
            resolve_qualified_display_names(&child, &displays, &["l.ID".to_string()], Exact)
                .unwrap_err()
                .to_string();
        assert_eq!(error, unresolved("`l`.`ID`", "`id`, `Data`"));
        let twin_displays = ["id".to_string(), "id".to_string()];
        let unqualified = DFSchema::from_unqualified_fields(
            vec![
                Field::new("e0", DataType::Int64, true),
                Field::new("e1", DataType::Int64, true),
            ]
            .into(),
            HashMap::new(),
        )
        .unwrap();
        let missing = resolve_qualified_display_names(
            &unqualified,
            &twin_displays,
            &["l.id".to_string()],
            IgnoreCase,
        )
        .unwrap();
        assert_eq!(missing[0].2, Disposition::Missing);
        let error = resolve_qualified_display_names(
            &unqualified,
            &twin_displays,
            &["l.ID".to_string()],
            Exact,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(error, unresolved("`l`.`ID`", "`id`"));
        let short = ["id".to_string()];
        let unpaired =
            resolve_qualified_display_names(&child, &short, &["l.id".to_string()], Exact).unwrap();
        assert_eq!(unpaired[0].2, Disposition::Missing);
    }

    #[test]
    fn qualified_display_multi_hit_refuses_unless_same_engine() {
        let dupes = schema(&[("l", "e0"), ("l", "e1")]);
        let twin_displays = ["id".to_string(), "id".to_string()];
        for rule in [IgnoreCase, Exact] {
            let error = resolve_qualified_display_names(
                &dupes,
                &twin_displays,
                &["l.id".to_string()],
                rule,
            )
            .unwrap_err()
            .to_string();
            assert_eq!(
                error,
                "Error during planning: [AMBIGUOUS_REFERENCE] Reference `l`.`id` is ambiguous, \
                 could be: [`l`.`id`, `l`.`id`]. SQLSTATE: 42704",
                "{rule:?}"
            );
        }
        let twice = DFSchema::new_with_metadata(
            vec![
                (
                    Some(TableReference::Bare { table: "l".into() }),
                    Arc::new(Field::new("e0", DataType::Int64, true)),
                ),
                (
                    Some(TableReference::Full {
                        catalog: "c".into(),
                        schema: "s".into(),
                        table: "l".into(),
                    }),
                    Arc::new(Field::new("e0", DataType::Int64, true)),
                ),
            ],
            HashMap::new(),
        )
        .unwrap();
        let bound = resolve_qualified_display_names(
            &twice,
            &twin_displays,
            &["l.id".to_string()],
            IgnoreCase,
        )
        .unwrap();
        assert_eq!(
            bound,
            vec![("l.id".to_string(), "e0".to_string(), Disposition::Bound)]
        );
    }

    #[test]
    fn display_match_fans_out_under_ignore_case_and_is_exact_under_exact() {
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
        let exact = match_display_names(&["id".to_string()], &held, Exact).unwrap();
        assert_eq!(
            exact,
            vec![("id".to_string(), vec!["id".to_string()], Disposition::Bound)]
        );
        let keys = ["ID".to_string(), "id".to_string()];
        assert!(refuse_folded_duplicate_keys(&keys, Exact).is_ok());
        assert!(
            refuse_folded_duplicate_keys(&["a".to_string(), "b".to_string()], IgnoreCase).is_ok()
        );
        let error = refuse_folded_duplicate_keys(&keys, IgnoreCase)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: [COLUMN_ALREADY_EXISTS] The column `id` already exists. \
             Choose another name or rename the existing column. SQLSTATE: 42711"
        );
    }

    #[test]
    fn resolver_names_keep_equals_ignore_case_where_lookup_lowers() {
        let held = ["id".to_string(), "v".to_string()];
        let resolver = match_resolver_names(&["ıd".to_string()], &held, IgnoreCase).unwrap();
        assert_eq!(
            resolver,
            vec![("ıd".to_string(), vec!["id".to_string()], Disposition::Bound)]
        );
        let lookup = match_display_names(&["ıd".to_string()], &held, IgnoreCase).unwrap();
        assert_eq!(lookup[0].2, Disposition::Missing);
        let subset = match_subset_names(&["ıd".to_string()], &held, IgnoreCase).unwrap();
        assert_eq!(subset, vec![("ıd".to_string(), vec!["id".to_string()])]);
    }
}
