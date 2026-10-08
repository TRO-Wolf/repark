use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, TableReference};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    Expr, ExprSchemable, Filter, Join, JoinType, Limit, LogicalPlan, Projection, Sort, try_cast,
};
use repark_common::names::NameRule;

use super::attr_id::AttrId;

pub const HIDDEN_PREFIX: &str = "__repark_using__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenKey {
    pub right: bool,
    pub side_position: usize,
    pub alias: String,
    pub display: String,
}

#[must_use]
pub fn hidden_alias(relation: Option<&TableReference>, field: &str) -> String {
    match relation {
        Some(relation) => format!("{HIDDEN_PREFIX}{}__{field}", relation.table()),
        None => format!("{HIDDEN_PREFIX}_{field}"),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn shown_key(
    join_type: JoinType,
    left: &Column,
    right: &Column,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<Expr> {
    let (Some(left), Some(right)) = (qualified(left, left_schema), qualified(right, right_schema))
    else {
        return Ok(Expr::Column(left.clone()));
    };
    let (left, right) = (&left, &right);
    let left_type = Expr::Column(left.clone()).get_type(left_schema)?;
    let right_type = Expr::Column(right.clone()).get_type(right_schema)?;
    let right_expr = typed_right(right, &left_type, &right_type);
    let shown = match join_type {
        JoinType::Right if left_type == right_type => return Ok(Expr::Column(right.clone())),
        JoinType::Right => {
            let id = right_schema
                .qualified_field_from_column(right)
                .ok()
                .and_then(|(_, field)| AttrId::of(field))
                .unwrap_or_else(AttrId::mint);
            return Ok(Expr::Alias(
                Alias::new(right_expr, right.relation.clone(), left.name.clone())
                    .with_metadata(Some(id.metadata())),
            ));
        }
        JoinType::Full => coalesce(vec![Expr::Column(left.clone()), right_expr]),
        _ => return Ok(Expr::Column(left.clone())),
    };
    Ok(Expr::Alias(
        Alias::new(shown, left.relation.clone(), left.name.clone())
            .with_metadata(Some(AttrId::mint().metadata())),
    ))
}

fn qualified(column: &Column, schema: &DFSchema) -> Option<Column> {
    if column.relation.is_some() {
        return Some(column.clone());
    }
    let (qualifier, field) = schema
        .qualified_field_with_unqualified_name(&column.name)
        .ok()?;
    qualifier.map(|qualifier| Column::new(Some(qualifier.clone()), field.name()))
}

fn typed_right(right: &Column, left_type: &DataType, right_type: &DataType) -> Expr {
    let column = Expr::Column(right.clone());
    if left_type == right_type {
        column
    } else {
        try_cast(column, left_type.clone())
    }
}

fn merge_site(plan: &LogicalPlan) -> Option<(&Projection, &Join)> {
    let mut node = plan;
    loop {
        match node {
            LogicalPlan::Filter(filter) => node = filter.input.as_ref(),
            LogicalPlan::Sort(sort) => node = sort.input.as_ref(),
            LogicalPlan::Limit(limit) => node = limit.input.as_ref(),
            LogicalPlan::Projection(projection) => {
                if let LogicalPlan::Join(join) = projection.input.as_ref() {
                    return Some((projection, join));
                }
                if !passes_every_column(projection) {
                    return None;
                }
                node = projection.input.as_ref();
            }
            _ => return None,
        }
    }
}

fn passes_every_column(projection: &Projection) -> bool {
    projection.expr.len() == projection.input.schema().fields().len()
        && projection
            .expr
            .iter()
            .all(|expr| plain_column(expr).is_some())
}

fn plain_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => plain_column(&alias.expr),
        _ => None,
    }
}

fn side_position(schema: &DFSchema, column: &Column) -> Option<usize> {
    schema.iter().position(|(qualifier, field)| {
        field.name() == &column.name && qualifier == column.relation.as_ref()
    })
}

fn join_hidden(projection: &Projection, join: &Join) -> Vec<(HiddenKey, Column)> {
    if matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
        return Vec::new();
    }
    let shown = projection
        .expr
        .iter()
        .filter_map(|expr| match expr {
            Expr::Column(column) => Some(column),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut hidden = Vec::new();
    for (left, right) in &join.on {
        let (Expr::Column(left), Expr::Column(right)) = (left, right) else {
            continue;
        };
        for (is_right, column, schema) in [
            (false, left, join.left.schema()),
            (true, right, join.right.schema()),
        ] {
            if shown.contains(&column) {
                continue;
            }
            let Some(position) = side_position(schema, column) else {
                continue;
            };
            hidden.push((
                HiddenKey {
                    right: is_right,
                    side_position: position,
                    alias: hidden_alias(column.relation.as_ref(), &column.name),
                    display: column.name.clone(),
                },
                column.clone(),
            ));
        }
    }
    hidden
}

#[must_use]
pub fn using_hidden_keys(plan: &LogicalPlan) -> Vec<HiddenKey> {
    merge_site(plan)
        .map(|(projection, join)| join_hidden(projection, join))
        .unwrap_or_default()
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

#[must_use]
pub fn hidden_names_in<'a>(exprs: impl IntoIterator<Item = &'a Expr>) -> Vec<String> {
    let mut names = Vec::new();
    for expr in exprs {
        let _ = expr.apply(|node| {
            if let Expr::Column(column) = node
                && column.relation.is_none()
                && column.name.starts_with(HIDDEN_PREFIX)
                && !names.contains(&column.name)
            {
                names.push(column.name.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    names
}

#[allow(clippy::missing_errors_doc)]
pub fn expose_hidden_keys(plan: &LogicalPlan, names: &[String]) -> Result<Option<LogicalPlan>> {
    if names.is_empty() {
        return Ok(None);
    }
    match plan {
        LogicalPlan::Filter(filter) => {
            let Some(input) = expose_hidden_keys(filter.input.as_ref(), names)? else {
                return Ok(None);
            };
            Filter::try_new(filter.predicate.clone(), Arc::new(input))
                .map(LogicalPlan::Filter)
                .map(Some)
        }
        LogicalPlan::Sort(sort) => {
            let Some(input) = expose_hidden_keys(sort.input.as_ref(), names)? else {
                return Ok(None);
            };
            Ok(Some(LogicalPlan::Sort(Sort {
                expr: sort.expr.clone(),
                input: Arc::new(input),
                fetch: sort.fetch,
            })))
        }
        LogicalPlan::Limit(limit) => {
            let Some(input) = expose_hidden_keys(limit.input.as_ref(), names)? else {
                return Ok(None);
            };
            Ok(Some(LogicalPlan::Limit(Limit {
                skip: limit.skip.clone(),
                fetch: limit.fetch.clone(),
                input: Arc::new(input),
            })))
        }
        LogicalPlan::Projection(projection) => expose_in_projection(projection, names),
        _ => Ok(None),
    }
}

fn expose_in_projection(projection: &Projection, names: &[String]) -> Result<Option<LogicalPlan>> {
    let mut exprs = projection.expr.clone();
    if let LogicalPlan::Join(join) = projection.input.as_ref() {
        let hidden = join_hidden(projection, join);
        for name in names {
            let Some((_, column)) = hidden.iter().find(|(key, _)| &key.alias == name) else {
                return Ok(None);
            };
            exprs.push(Expr::Column(column.clone()).alias(name));
        }
        return Projection::try_new(exprs, Arc::clone(&projection.input))
            .map(LogicalPlan::Projection)
            .map(Some);
    }
    if !passes_every_column(projection) {
        return Ok(None);
    }
    let Some(input) = expose_hidden_keys(projection.input.as_ref(), names)? else {
        return Ok(None);
    };
    exprs.extend(
        names
            .iter()
            .map(|name| Expr::Column(Column::new_unqualified(name))),
    );
    Projection::try_new(exprs, Arc::new(input))
        .map(LogicalPlan::Projection)
        .map(Some)
}

#[must_use]
pub fn output_columns(schema: &DFSchema) -> Vec<Expr> {
    schema
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn rebind_key_name(
    expr: Expr,
    qualifier: Option<&str>,
    key: &str,
    alias: &str,
    rule: NameRule,
    keep_name: bool,
) -> Result<Expr> {
    let shown = expr.schema_name().to_string();
    let aliased = matches!(expr, Expr::Alias(_));
    let dotted = qualifier.map(|qualifier| format!("{qualifier}.{key}"));
    let rebound = expr.transform(|node| match node {
        Expr::Column(column) if names_key(&column, qualifier, dotted.as_deref(), key, rule) => Ok(
            Transformed::yes(Expr::Column(Column::new_unqualified(alias))),
        ),
        other => Ok(Transformed::no(other)),
    })?;
    if rebound.transformed && keep_name && !aliased {
        return Ok(rebound.data.alias(shown));
    }
    Ok(rebound.data)
}

fn names_key(
    column: &Column,
    qualifier: Option<&str>,
    dotted: Option<&str>,
    key: &str,
    rule: NameRule,
) -> bool {
    match (&column.relation, qualifier, dotted) {
        (None, None, _) => rule.matches(key, &column.name),
        (None, Some(_), Some(dotted)) => rule.matches(dotted, &column.name),
        (Some(relation), Some(qualifier), _) => {
            relation.schema().is_none()
                && rule.matches(qualifier, relation.table())
                && rule.matches(key, &column.name)
        }
        _ => false,
    }
}

#[must_use]
pub fn hidden_names_in_text(plan: &LogicalPlan, text: &str) -> Vec<String> {
    if !text.contains(HIDDEN_PREFIX) {
        return Vec::new();
    }
    using_hidden_keys(plan)
        .into_iter()
        .map(|key| key.alias)
        .filter(|alias| text.contains(alias.as_str()))
        .collect()
}
