use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;
use repark_common::names::NameRule;
use repark_iceberg::write::merge::{
    InsertAction, MatchedAction, MergeSpec, NotMatchedBySourceAction,
};

pub(crate) mod exact;

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn maybe_rewrite_merge_fragments(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &mut MergeSpec,
) -> Result<()> {
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    spec.case_insensitive = case_insensitive;
    if case_insensitive {
        if spec.source_from_sql.starts_with('(') && spec.source_from_sql.ends_with(')') {
            let inner = spec.source_from_sql[1..spec.source_from_sql.len() - 1].to_string();
            let folded =
                repark_core::column_resolution::fold_query_text(&ctx.state(), &inner).await?;
            spec.source_from_sql = format!("({folded})");
        }
        rewrite_merge_fragments(ctx, catalog, spec, NameRule::IgnoreCase).await?;
    } else {
        rewrite_merge_fragments(ctx, catalog, spec, NameRule::Exact).await?;
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
    let plan = if crate::spark_door_case_insensitive(state.config().options()) {
        state.statement_to_plan(statement).await?
    } else {
        let mut exact = state.clone();
        exact
            .config_mut()
            .options_mut()
            .sql_parser
            .enable_ident_normalization = false;
        exact.statement_to_plan(statement).await?
    };
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
    rule: NameRule,
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
    let fix = |sql: &str, home: Option<&str>| match rule {
        NameRule::Exact => exact::check_fragment_exact(sql, &scopes, home),
        NameRule::IgnoreCase => {
            repark_core::column_resolution::rewrite_fragment_case(sql, &scopes, true, home)
        }
    };
    spec.on_sql = fix(&spec.on_sql, None)?;
    for clause in &mut spec.matched {
        if let Some(predicate) = clause.predicate_sql.as_mut() {
            *predicate = fix(predicate, None)?;
        }
        if let MatchedAction::Update { assignments } = &mut clause.action {
            for (_, value) in assignments {
                *value = fix(value, None)?;
            }
        }
    }
    for clause in &mut spec.not_matched {
        if let Some(predicate) = clause.predicate_sql.as_mut() {
            *predicate = fix(predicate, Some(spec.source_alias.as_str()))?;
        }
        if let InsertAction::Explicit { values_sql, .. } = &mut clause.action {
            for value in values_sql {
                *value = fix(value, Some(spec.source_alias.as_str()))?;
            }
        }
    }
    for clause in &mut spec.not_matched_by_source {
        if let Some(predicate) = clause.predicate_sql.as_mut() {
            *predicate = fix(predicate, Some(spec.target_alias.as_str()))?;
        }
        if let NotMatchedBySourceAction::Update { assignments } = &mut clause.action {
            for (_, value) in assignments {
                *value = fix(value, Some(spec.target_alias.as_str()))?;
            }
        }
    }
    Ok(())
}
