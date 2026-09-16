use std::sync::Arc;

use datafusion::catalog::default_table_source::{provider_as_source, source_as_provider};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column as DFColumn, TableReference};
use datafusion::execution::SessionState;
use datafusion::functions::core::get_field;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableScan};
use datafusion::prelude::{DataFrame, col, lit};

use crate::{Result, engine_err};

use super::augment::augment_scan;
use super::error::FileMetadataError;
use super::scan::{FileKind, FileMetadataScan};
use super::status::{FileMetadataStatus, WalkOutcome, file_metadata_status, walk_plan};
use super::udf::METADATA_FIELD_NAMES;
use super::{METADATA_COLUMN_NAME, SHADOW_METADATA_COLUMN_NAME};
fn column_targets_hidden(column: &datafusion::common::Column, hidden: &str) -> bool {
    column.name == hidden
        || column
            .relation
            .as_ref()
            .is_some_and(|relation| relation.to_string() == hidden)
}

fn first_mentioned_name(exprs: &[Expr], hidden: &str) -> Option<String> {
    let mut found = None;
    for expr in exprs {
        let _ = expr.apply(|node| {
            if let Expr::Column(column) = node
                && column_targets_hidden(column, hidden)
            {
                found = Some(match &column.relation {
                    Some(relation) => format!("{relation}.{}", column.name),
                    None => column.name.clone(),
                });
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if found.is_some() {
            break;
        }
    }
    found
}

#[must_use]
pub fn expr_mentions_file_metadata(expr: &Expr) -> bool {
    expr_mentions_hidden(expr, METADATA_COLUMN_NAME)
        || expr_mentions_hidden(expr, SHADOW_METADATA_COLUMN_NAME)
}

fn expr_mentions_hidden(expr: &Expr, hidden: &str) -> bool {
    let mut mentioned = false;
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node
            && column_targets_hidden(column, hidden)
        {
            mentioned = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    mentioned
}
pub(crate) fn hidden_field_or_reject(
    field: &str,
    hidden: &str,
    with_row_index: bool,
) -> Option<String> {
    if !METADATA_FIELD_NAMES.contains(&field) || (!with_row_index && field == "row_index") {
        return Some(format!("{hidden}.{field}"));
    }
    None
}

pub(crate) fn drop_redundant_column_alias(expr: Expr) -> Expr {
    if let Expr::Alias(alias) = &expr
        && let Expr::Column(column) = alias.expr.as_ref()
        && column.relation.is_none()
        && column.name == alias.name
    {
        return alias.expr.as_ref().clone();
    }
    expr
}

pub(crate) fn rewrite_metadata_refs(
    exprs: Vec<Expr>,
    hidden: &str,
    with_row_index: bool,
) -> std::result::Result<Vec<Expr>, FileMetadataError> {
    for expr in &exprs {
        let mut rejected = None;
        let _ = expr.apply(|node| {
            if let Expr::Column(column) = node
                && column
                    .relation
                    .as_ref()
                    .is_some_and(|relation| relation.to_string() == hidden)
                && let Some(name) =
                    hidden_field_or_reject(column.name.as_str(), hidden, with_row_index)
            {
                rejected = Some(name);
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if let Some(name) = rejected {
            return Err(FileMetadataError::unresolved(&name));
        }
    }
    exprs
        .into_iter()
        .map(|expr| {
            if let Expr::Column(column) = &expr
                && column
                    .relation
                    .as_ref()
                    .is_some_and(|relation| relation.to_string() == hidden)
            {
                let field = column.name.clone();
                let field_expr = get_field().call(vec![col(hidden), lit(field.clone())]);
                return Ok(field_expr.alias(field));
            }
            expr.transform_up(|node| {
                if let Expr::Column(column) = &node
                    && column
                        .relation
                        .as_ref()
                        .is_some_and(|relation| relation.to_string() == hidden)
                {
                    let field = column.name.clone();
                    let field_expr = get_field().call(vec![col(hidden), lit(field)]);
                    return Ok(Transformed::yes(field_expr));
                }
                Ok(Transformed::no(node))
            })
            .map(|transformed| transformed.data)
            .map_err(|_| FileMetadataError::unresolved(hidden))
        })
        .collect()
}
pub(crate) fn mark_file_scan(frame: DataFrame, kind: FileKind) -> Result<DataFrame> {
    let (state, plan) = frame.into_parts();
    let marked = match plan {
        LogicalPlan::TableScan(scan) => {
            let provider = source_as_provider(&scan.source).map_err(engine_err)?;
            if provider
                .as_ref()
                .downcast_ref::<FileMetadataScan>()
                .is_some()
            {
                LogicalPlan::TableScan(scan)
            } else {
                let marker = provider_as_source(Arc::new(FileMetadataScan::new(provider, kind)));
                LogicalPlan::TableScan(
                    TableScan::try_new(
                        scan.table_name.clone(),
                        marker,
                        scan.projection.clone(),
                        scan.filters.clone(),
                        scan.fetch,
                    )
                    .map_err(engine_err)?,
                )
            }
        }
        other => other,
    };
    Ok(DataFrame::new(state, marked))
}
fn strip_scan_qualifier(expr: &Expr, table: &TableReference) -> datafusion::error::Result<Expr> {
    expr.clone()
        .transform_up(|leaf| {
            if let Expr::Column(column) = &leaf
                && column.relation.as_ref() == Some(table)
            {
                return Ok(Transformed::yes(Expr::Column(DFColumn::new(
                    None::<TableReference>,
                    column.name.clone(),
                ))));
            }
            Ok(Transformed::no(leaf))
        })
        .map(|transformed| transformed.data)
}

fn widen_projections(
    plan: &LogicalPlan,
    hidden: &str,
    table: Option<&TableReference>,
) -> datafusion::error::Result<LogicalPlan> {
    plan.clone()
        .transform_up(|node| {
            if let LogicalPlan::Projection(projection) = &node {
                let mut expr = projection
                    .expr
                    .iter()
                    .map(|one| match table {
                        Some(name) => strip_scan_qualifier(one, name),
                        None => Ok(one.clone()),
                    })
                    .collect::<datafusion::error::Result<Vec<_>>>()?;
                if projection
                    .input
                    .schema()
                    .has_column_with_unqualified_name(hidden)
                    && !node.schema().has_column_with_unqualified_name(hidden)
                {
                    expr.push(col(hidden));
                }
                let rebuilt = LogicalPlanBuilder::from(Arc::clone(&projection.input))
                    .project(expr)?
                    .build()?;
                return Ok(Transformed::yes(rebuilt));
            }
            if let LogicalPlan::Filter(filter) = &node
                && let Some(name) = table
            {
                let rebuilt = LogicalPlan::Filter(datafusion::logical_expr::Filter::try_new(
                    strip_scan_qualifier(&filter.predicate, name)?,
                    Arc::clone(&filter.input),
                )?);
                return Ok(Transformed::yes(rebuilt));
            }
            if let LogicalPlan::Sort(sort) = &node
                && let Some(name) = table
            {
                let expr = sort
                    .expr
                    .iter()
                    .map(|sort_expr| {
                        Ok(datafusion::logical_expr::SortExpr {
                            expr: strip_scan_qualifier(&sort_expr.expr, name)?,
                            asc: sort_expr.asc,
                            nulls_first: sort_expr.nulls_first,
                        })
                    })
                    .collect::<datafusion::error::Result<Vec<_>>>()?;
                let rebuilt = LogicalPlan::Sort(datafusion::logical_expr::Sort {
                    expr,
                    input: Arc::clone(&sort.input),
                    fetch: sort.fetch,
                });
                return Ok(Transformed::yes(rebuilt));
            }
            Ok(Transformed::no(node))
        })
        .map(|transformed| transformed.data)
}
#[allow(clippy::missing_errors_doc)]
pub async fn ensure_file_metadata(
    state: &SessionState,
    plan: LogicalPlan,
    exprs: Vec<Expr>,
) -> std::result::Result<(LogicalPlan, Vec<Expr>), FileMetadataError> {
    let status = file_metadata_status(&plan);
    let hidden = match status {
        FileMetadataStatus::Shadowed => SHADOW_METADATA_COLUMN_NAME,
        _ => METADATA_COLUMN_NAME,
    };
    if !exprs.iter().any(|expr| expr_mentions_hidden(expr, hidden)) {
        return Ok((plan, exprs));
    }
    match walk_plan(&plan) {
        WalkOutcome::DeadAttributeLoss => {
            let requested = first_mentioned_name(&exprs, hidden).unwrap_or(hidden.to_string());
            let available = plan
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            Err(FileMetadataError::missing(hidden, &available, &requested))
        }
        WalkOutcome::DeadOther => {
            let name = first_mentioned_name(&exprs, hidden).unwrap_or(hidden.to_string());
            Err(FileMetadataError::unresolved(&name))
        }
        WalkOutcome::Found { scan, kind } => {
            if scan.projection.is_some() || !scan.filters.is_empty() || scan.fetch.is_some() {
                let name = first_mentioned_name(&exprs, hidden).unwrap_or(hidden.to_string());
                return Err(FileMetadataError::unresolved(&name));
            }
            let with_row_index = matches!(kind, FileKind::Parquet);
            let augmented = augment_scan(state, &plan, &scan, &kind).await?;
            let widened = if hidden == METADATA_COLUMN_NAME {
                widen_projections(&augmented, hidden, Some(&scan.table_name))
                    .map_err(|error| FileMetadataError::engine(error.to_string()))?
            } else {
                augmented
            };
            let exprs = exprs.into_iter().map(drop_redundant_column_alias).collect();
            let rewritten = rewrite_metadata_refs(exprs, hidden, with_row_index)?;
            Ok((widened, rewritten))
        }
        WalkOutcome::Realized { with_row_index } => {
            let widened = if hidden == METADATA_COLUMN_NAME {
                widen_projections(&plan, hidden, None)
                    .map_err(|error| FileMetadataError::engine(error.to_string()))?
            } else {
                plan
            };
            let exprs = exprs.into_iter().map(drop_redundant_column_alias).collect();
            let rewritten = rewrite_metadata_refs(exprs, hidden, with_row_index)?;
            Ok((widened, rewritten))
        }
    }
}
