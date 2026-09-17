use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::TableIdent;
use repark_iceberg::write::merge::{
    InsertAction, MatchedAction, MergeSpec, NotMatchedBySourceAction,
};

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn maybe_rewrite_merge_fragments(
    ctx: &SessionContext,
    catalog_name: &str,
    spec: &mut MergeSpec,
) -> Result<()> {
    let case_insensitive = repark_core::column_resolution::column_resolution_is_case_insensitive(
        ctx.state().config().options(),
    );
    spec.case_insensitive = case_insensitive;
    if case_insensitive {
        rewrite_merge_fragments(ctx, catalog_name, spec).await?;
    }
    Ok(())
}

async fn scope_field_names(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(sql, &dialect)?;
    let plan = state.statement_to_plan(statement).await?;
    Ok(plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect())
}

async fn rewrite_merge_fragments(
    ctx: &SessionContext,
    catalog_name: &str,
    spec: &mut MergeSpec,
) -> Result<()> {
    let target_sql = format!(
        "SELECT * FROM {} LIMIT 0",
        spec_target_from_sql(catalog_name, &spec.target)
    );
    let target_fields = scope_field_names(ctx, &target_sql).await?;
    let source_sql = format!(
        "SELECT * FROM {} AS {} LIMIT 0",
        spec.source_from_sql, spec.source_alias
    );
    let source_fields = scope_field_names(ctx, &source_sql).await?;
    let scopes = [
        (spec.target_alias.as_str(), target_fields.as_slice()),
        (spec.source_alias.as_str(), source_fields.as_slice()),
    ];
    spec.on_sql =
        repark_core::column_resolution::rewrite_fragment_case(&spec.on_sql, &scopes, true, None)?;
    for clause in &mut spec.matched {
        if let Some(predicate) = clause.predicate_sql.as_mut() {
            *predicate = repark_core::column_resolution::rewrite_fragment_case(
                predicate, &scopes, true, None,
            )?;
        }
        if let MatchedAction::Update { assignments } = &mut clause.action {
            for (_, value) in assignments {
                *value = repark_core::column_resolution::rewrite_fragment_case(
                    value, &scopes, true, None,
                )?;
            }
        }
    }
    for clause in &mut spec.not_matched {
        if let Some(predicate) = clause.predicate_sql.as_mut() {
            *predicate = repark_core::column_resolution::rewrite_fragment_case(
                predicate,
                &scopes,
                true,
                Some(spec.source_alias.as_str()),
            )?;
        }
        if let InsertAction::Explicit { values_sql, .. } = &mut clause.action {
            for value in values_sql {
                *value = repark_core::column_resolution::rewrite_fragment_case(
                    value,
                    &scopes,
                    true,
                    Some(spec.source_alias.as_str()),
                )?;
            }
        }
    }
    for clause in &mut spec.not_matched_by_source {
        if let Some(predicate) = clause.predicate_sql.as_mut() {
            *predicate = repark_core::column_resolution::rewrite_fragment_case(
                predicate,
                &scopes,
                true,
                Some(spec.target_alias.as_str()),
            )?;
        }
        if let NotMatchedBySourceAction::Update { assignments } = &mut clause.action {
            for (_, value) in assignments {
                *value = repark_core::column_resolution::rewrite_fragment_case(
                    value,
                    &scopes,
                    true,
                    Some(spec.target_alias.as_str()),
                )?;
            }
        }
    }
    Ok(())
}

fn spec_target_from_sql(catalog_name: &str, target: &TableIdent) -> String {
    let mut parts = vec![catalog_name.to_string(), target.namespace().to_string()];
    parts.push(target.name().to_string());
    parts
        .iter()
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}
