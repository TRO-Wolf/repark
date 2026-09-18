use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;
use repark_iceberg::write::merge::{
    InsertAction, MatchedAction, MergeSpec, NotMatchedBySourceAction,
};

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn maybe_rewrite_merge_fragments(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &mut MergeSpec,
) -> Result<()> {
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    spec.case_insensitive = case_insensitive;
    if case_insensitive {
        rewrite_merge_fragments(ctx, catalog, spec).await?;
    }
    Ok(())
}

async fn source_field_names(ctx: &SessionContext, spec: &MergeSpec) -> Result<Vec<String>> {
    if !spec.source_from_sql.starts_with('(')
        && let Ok(provider) = ctx
            .table_provider(TableReference::from(spec.source_from_sql.as_str()))
            .await
    {
        return Ok(provider
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect());
    }
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let sql = format!(
        "SELECT * FROM {} AS {} LIMIT 0",
        spec.source_from_sql, spec.source_alias
    );
    let statement = state.sql_to_statement(&sql, &dialect)?;
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
    catalog: &Arc<dyn Catalog>,
    spec: &mut MergeSpec,
) -> Result<()> {
    let table = catalog
        .load_table(&spec.target)
        .await
        .map_err(crate::iceberg_err)?;
    let target_fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let source_fields = source_field_names(ctx, spec).await?;
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
