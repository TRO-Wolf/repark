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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Target,
    Source,
}

fn owner_side(expr: &Expr, target_alias: &str, source_aliases: &[String]) -> Option<Side> {
    let Expr::CompoundIdentifier(parts) = expr else {
        return None;
    };
    if parts.len() != 2 {
        return None;
    }
    let owner = parts[0].value.as_str();
    let target = owner.eq_ignore_ascii_case(target_alias);
    let source = source_aliases
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(owner));
    match (target, source) {
        (true, false) => Some(Side::Target),
        (false, true) => Some(Side::Source),
        _ => None,
    }
}

fn source_shadows_target(target_alias: &str, source_aliases: &[String]) -> bool {
    source_aliases
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(target_alias))
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
    let left_side = owner_side(left, target_alias, source_aliases)?;
    let right_side = owner_side(right, target_alias, source_aliases)?;
    match (left_side, right_side) {
        (Side::Target, Side::Source) => Some((last_ident(left)?, last_ident(right)?)),
        (Side::Source, Side::Target) => Some((last_ident(right)?, last_ident(left)?)),
        _ => None,
    }
}

fn projected_source_column(
    projected: &Expr,
    target_alias: &str,
    source_aliases: &[String],
) -> Option<String> {
    match projected {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(_) => match owner_side(projected, target_alias, source_aliases)? {
            Side::Source => last_ident(projected),
            Side::Target => None,
        },
        _ => None,
    }
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
            let (source_from_sql, source_aliases) = plain_source_table(&select.from[0])?;
            if source_shadows_target(target_alias, &source_aliases) {
                return None;
            }
            let (SelectItem::UnnamedExpr(projected)
            | SelectItem::ExprWithAlias {
                expr: projected, ..
            }) = &select.projection[0]
            else {
                return None;
            };
            Some(ResidualHint {
                target_column,
                source_column: projected_source_column(projected, target_alias, &source_aliases)?,
                source_from_sql,
            })
        }
        Expr::Exists {
            subquery,
            negated: false,
        } => {
            let select = is_simple_select_body(subquery)?;
            let (source_from_sql, source_aliases) = plain_source_table(&select.from[0])?;
            if source_shadows_target(target_alias, &source_aliases) {
                return None;
            }
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
