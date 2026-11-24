use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;

use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Query, Visit, Visitor};
use repark_core::CatalogRegistry;
use repark_iceberg::write::merge::{
    InsertAction, MatchedAction, MergeSpec, NotMatchedBySourceAction,
};
use repark_iceberg::write::predicate_dml::PredicateDmlSpec;

use crate::catalog_ops::namespace_schema_name;
use crate::spark_ast;

use super::{ViewPlanCache, resolve_live_views, validate_write_plan, walk_plan};

pub(crate) fn validate_write_query<'a>(
    ctx: &'a SessionContext,
    catalogs: &'a CatalogRegistry,
    source: &'a Query,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let sql = source.to_string();
        match spark_ast::execute_passthrough(ctx, catalogs, &sql).await {
            Ok(frame) => validate_write_plan(ctx, frame.logical_plan()).await,
            Err(error) if is_cast_error(&error) => Err(error),
            Err(_) => Ok(()),
        }
    })
}

struct ExplicitCastFinder(bool);

impl Visitor for ExplicitCastFinder {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        match expr {
            SqlExpr::Cast { .. } => {
                self.0 = true;
                ControlFlow::Break(())
            }
            SqlExpr::Function(function) => {
                let name = function
                    .name
                    .0
                    .last()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if matches!(
                    name.as_str(),
                    "cast"
                        | "to_timestamp"
                        | "to_timestamp_ltz"
                        | "to_timestamp_ntz"
                        | "double"
                        | "float"
                        | "int"
                        | "integer"
                        | "bigint"
                        | "smallint"
                        | "tinyint"
                        | "decimal"
                        | "boolean"
                        | "date"
                        | "timestamp"
                        | "timestamp_ntz"
                ) {
                    self.0 = true;
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

pub(crate) fn sql_expr_has_explicit_cast(expr: &SqlExpr) -> bool {
    let mut finder = ExplicitCastFinder(false);
    let _ = expr.visit(&mut finder);
    finder.0
}

pub(crate) async fn validate_identity_dml_predicate(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    spec: &PredicateDmlSpec,
) -> Result<()> {
    let target_sql = qualified_table_sql(catalog_name, &spec.target);
    let alias = quote_backtick(&spec.target_alias);
    let probe = format!(
        "SELECT 1 FROM {target_sql} AS {alias} WHERE ({})",
        spec.selection_sql
    );
    validate_probe_sql(ctx, catalogs, &probe).await
}

pub(crate) fn validate_merge_spec<'a>(
    ctx: &'a SessionContext,
    catalogs: &'a CatalogRegistry,
    catalog_name: &'a str,
    spec: &'a MergeSpec,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(validate_merge_spec_inner(ctx, catalogs, catalog_name, spec))
}

async fn validate_merge_spec_inner(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    spec: &MergeSpec,
) -> Result<()> {
    let source_alias = quote_backtick(&spec.source_alias);
    let source_probe = format!("SELECT * FROM {} AS {source_alias}", spec.source_from_sql);
    validate_probe_sql(ctx, catalogs, &source_probe).await?;

    let target_sql = qualified_table_sql(catalog_name, &spec.target);
    let target_alias = quote_backtick(&spec.target_alias);
    let on_probe = format!(
        "SELECT 1 FROM {target_sql} AS {target_alias} LEFT JOIN {} AS {source_alias} ON ({})",
        spec.source_from_sql, spec.on_sql
    );
    validate_probe_sql(ctx, catalogs, &on_probe).await?;

    let joined_from = format!(
        "{target_sql} AS {target_alias} LEFT JOIN {} AS {source_alias} ON false",
        spec.source_from_sql
    );
    for clause in &spec.matched {
        if clause_predicate_is_const_false(
            ctx,
            catalogs,
            &joined_from,
            clause.predicate_sql.as_deref(),
        )
        .await?
        {
            continue;
        }
        if let MatchedAction::Update { assignments } = &clause.action {
            let exprs: Vec<&str> = assignments
                .iter()
                .map(|(_, value)| value.as_str())
                .filter(|value| !value.trim().eq_ignore_ascii_case("DEFAULT"))
                .collect();
            validate_merge_exprs(ctx, catalogs, &joined_from, &exprs).await?;
        }
    }
    for clause in &spec.not_matched {
        if clause_predicate_is_const_false(
            ctx,
            catalogs,
            &joined_from,
            clause.predicate_sql.as_deref(),
        )
        .await?
        {
            continue;
        }
        if let InsertAction::Explicit { values_sql, .. } = &clause.action {
            let exprs: Vec<&str> = values_sql
                .iter()
                .map(String::as_str)
                .filter(|value| !value.trim().eq_ignore_ascii_case("DEFAULT"))
                .collect();
            validate_merge_exprs(ctx, catalogs, &joined_from, &exprs).await?;
        }
    }
    let target_only_from = format!("{target_sql} AS {target_alias}");
    for clause in &spec.not_matched_by_source {
        if clause_predicate_is_const_false(
            ctx,
            catalogs,
            &target_only_from,
            clause.predicate_sql.as_deref(),
        )
        .await?
        {
            continue;
        }
        if let NotMatchedBySourceAction::Update { assignments } = &clause.action {
            let exprs: Vec<&str> = assignments
                .iter()
                .map(|(_, value)| value.as_str())
                .filter(|value| !value.trim().eq_ignore_ascii_case("DEFAULT"))
                .collect();
            validate_merge_exprs(ctx, catalogs, &target_only_from, &exprs).await?;
        }
    }
    Ok(())
}

async fn clause_predicate_is_const_false(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    from_sql: &str,
    predicate_sql: Option<&str>,
) -> Result<bool> {
    let Some(predicate) = predicate_sql else {
        return Ok(false);
    };
    let probe = format!("SELECT ({predicate}) AS __pred FROM {from_sql}");
    let frame = match spark_ast::execute_passthrough(ctx, catalogs, &probe).await {
        Ok(frame) => frame,
        Err(error) if is_cast_error(&error) => return Err(error),
        Err(_) => return Ok(false),
    };
    let options = ctx.state().config().options().clone();
    let mut cache = ViewPlanCache::new();
    resolve_live_views(frame.logical_plan(), &options, 0, &mut cache).await;
    let envs = walk_plan(frame.logical_plan(), &options, None, 0, false, &cache)?;
    let all_false = !envs.is_empty()
        && envs
            .iter()
            .all(|env| matches!(env.first(), Some(Some(ScalarValue::Boolean(Some(false))))));
    Ok(all_false)
}

async fn validate_merge_exprs(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    from_sql: &str,
    exprs: &[&str],
) -> Result<()> {
    if exprs.is_empty() {
        return Ok(());
    }
    let select_list = exprs
        .iter()
        .enumerate()
        .map(|(index, expr)| format!("({expr}) AS __merge_expr_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let probe = format!("SELECT {select_list} FROM {from_sql}");
    validate_probe_sql(ctx, catalogs, &probe).await
}

async fn validate_probe_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    probe_sql: &str,
) -> Result<()> {
    match spark_ast::execute_passthrough(ctx, catalogs, probe_sql).await {
        Ok(frame) => validate_write_plan(ctx, frame.logical_plan()).await,
        Err(error) if is_cast_error(&error) => Err(error),
        Err(_) => Ok(()),
    }
}

pub(super) fn is_cast_error(error: &DataFusionError) -> bool {
    let text = error.to_string();
    text.contains("[CAST_INVALID_INPUT]")
        || text.contains("[CAST_OVERFLOW]")
        || text.contains("[CAST_OVERFLOW_IN_TABLE_INSERT]")
        || text.contains("[NUMERIC_VALUE_OUT_OF_RANGE")
        || text.contains("[NUMERIC_OUT_OF_SUPPORTED_RANGE]")
        || text.contains("[INVALID_FORMAT")
        || text.contains("[CANNOT_PARSE_TIMESTAMP]")
        || text.contains("[CANNOT_PARSE_INTERVAL]")
        || text.contains("[DATETIME_OVERFLOW]")
        || text.contains("[DATETIME_FIELD_OUT_OF_BOUNDS")
        || text.contains("[CONVERSION_INVALID_INPUT]")
        || text.contains("[DATATYPE_MISMATCH.CAST_")
}

fn qualified_table_sql(catalog_name: &str, ident: &iceberg::TableIdent) -> String {
    let namespace = namespace_schema_name(ident.namespace());
    format!(
        "{}.{}.{}",
        quote_backtick(catalog_name),
        quote_backtick(&namespace),
        quote_backtick(ident.name())
    )
}

fn quote_backtick(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}
