use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, SelectItem, TableFactor, TableWithJoins,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use iceberg::expr::Predicate;

use super::{PredicateDmlSpec, is_simple_select_body, object_name_parts};
use crate::write::scan_prune::{BareEquality, residual_bounds_predicate, scan_pruning_from_ctx};

struct ResidualHint {
    target_column: String,
    source_from_sql: String,
    source_column: String,
}

fn last_ident(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn first_ident(expr: &Expr) -> Option<String> {
    match expr {
        Expr::CompoundIdentifier(parts) => parts.first().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn plain_source_table(from: &TableWithJoins) -> Option<(String, Vec<String>)> {
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, alias, .. } = &from.relation else {
        return None;
    };
    let mut aliases = vec![name.to_string()];
    if let Some(last) = object_name_parts(name).last() {
        aliases.push(last.clone());
    }
    if let Some(alias) = alias {
        aliases.push(alias.name.value.clone());
    }
    Some((name.to_string(), aliases))
}

fn correlated_key_columns(
    selection: &Expr,
    target_alias: &str,
    source_aliases: &[String],
) -> Option<(String, String)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = selection
    else {
        return None;
    };
    let belongs_to_target = |expr: &Expr| {
        first_ident(expr).is_some_and(|owner| owner.eq_ignore_ascii_case(target_alias))
    };
    let belongs_to_source = |expr: &Expr| {
        first_ident(expr).is_some_and(|owner| {
            source_aliases
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&owner))
        })
    };
    if belongs_to_target(left) && belongs_to_source(right) {
        return Some((last_ident(left)?, last_ident(right)?));
    }
    if belongs_to_source(left) && belongs_to_target(right) {
        return Some((last_ident(right)?, last_ident(left)?));
    }
    None
}

fn residual_hint(selection_sql: &str, target_alias: &str) -> Option<ResidualHint> {
    let expr = Parser::new(&GenericDialect {})
        .try_with_sql(selection_sql)
        .ok()?
        .parse_expr()
        .ok()?;
    match &expr {
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            let target_column = last_ident(expr)?;
            let select = is_simple_select_body(subquery)?;
            let (source_from_sql, _) = plain_source_table(&select.from[0])?;
            let (SelectItem::UnnamedExpr(projected)
            | SelectItem::ExprWithAlias {
                expr: projected, ..
            }) = &select.projection[0]
            else {
                return None;
            };
            Some(ResidualHint {
                target_column,
                source_from_sql,
                source_column: last_ident(projected)?,
            })
        }
        Expr::Exists {
            subquery,
            negated: false,
        } => {
            let select = is_simple_select_body(subquery)?;
            let (source_from_sql, source_aliases) = plain_source_table(&select.from[0])?;
            let (target_column, source_column) =
                correlated_key_columns(select.selection.as_ref()?, target_alias, &source_aliases)?;
            Some(ResidualHint {
                target_column,
                source_from_sql,
                source_column,
            })
        }
        _ => None,
    }
}

pub(super) async fn identity_scan_residual(
    ctx: &SessionContext,
    spec: &PredicateDmlSpec,
    write_schema: &datafusion::arrow::datatypes::Schema,
) -> Option<Predicate> {
    if !scan_pruning_from_ctx(ctx) {
        return None;
    }
    let hint = residual_hint(&spec.selection_sql, &spec.target_alias)?;
    residual_bounds_predicate(
        ctx,
        &hint.source_from_sql,
        "__repark_dml_src",
        write_schema,
        &[BareEquality {
            target_column: hint.target_column,
            source_column: hint.source_column,
        }],
    )
    .await
}
