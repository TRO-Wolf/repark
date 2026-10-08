use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, TableReference};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    Expr, ExprSchemable, Filter, Join, JoinType, Limit, LogicalPlan, Projection, Sort, cast,
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
    match join_type {
        JoinType::Right => Ok(Expr::Column(right)),
        JoinType::Full => {
            let left_type = Expr::Column(left.clone()).get_type(left_schema)?;
            let right_type = Expr::Column(right.clone()).get_type(right_schema)?;
            let merged = full_key(
                Expr::Column(left.clone()),
                &left_type,
                Expr::Column(right),
                &right_type,
            );
            Ok(match merged {
                Some(merged) => Expr::Alias(
                    Alias::new(merged, left.relation.clone(), left.name.clone())
                        .with_metadata(Some(AttrId::mint().metadata())),
                ),
                None => Expr::Column(left),
            })
        }
        _ => Ok(Expr::Column(left)),
    }
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

fn integral_digits(kind: &DataType) -> Option<u8> {
    match kind {
        DataType::Int8 => Some(3),
        DataType::Int16 => Some(5),
        DataType::Int32 => Some(10),
        DataType::Int64 => Some(20),
        _ => None,
    }
}

fn is_text(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn decimal_of(kind: &DataType) -> Option<(u8, i8)> {
    match kind {
        DataType::Decimal128(precision, scale) if *scale >= 0 => Some((*precision, *scale)),
        other => integral_digits(other).map(|digits| (digits, 0)),
    }
}

#[must_use]
pub fn spark_key_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    let floating = |kind: &DataType| matches!(kind, DataType::Float32 | DataType::Float64);
    match (left, right) {
        (DataType::Date32, DataType::Timestamp(..)) => return Some(right.clone()),
        (DataType::Timestamp(..), DataType::Date32) => return Some(left.clone()),
        _ => {}
    }
    if let (Some(left_digits), Some(right_digits)) = (integral_digits(left), integral_digits(right))
    {
        return Some(if left_digits >= right_digits {
            left.clone()
        } else {
            right.clone()
        });
    }
    if (floating(left) || floating(right))
        && [left, right]
            .iter()
            .all(|kind| floating(kind) || decimal_of(kind).is_some() || is_text(kind))
    {
        return Some(DataType::Float64);
    }
    if (is_text(left) && integral_digits(right).is_some())
        || (is_text(right) && integral_digits(left).is_some())
    {
        return Some(DataType::Int64);
    }
    let (left_precision, left_scale) = decimal_of(left)?;
    let (right_precision, right_scale) = decimal_of(right)?;
    let scale = left_scale.max(right_scale);
    let whole = (i16::from(left_precision) - i16::from(left_scale))
        .max(i16::from(right_precision) - i16::from(right_scale));
    let precision = u8::try_from(whole + i16::from(scale)).ok()?;
    (precision <= 38).then_some(DataType::Decimal128(precision, scale))
}

#[must_use]
pub fn full_key(
    left: Expr,
    left_type: &DataType,
    right: Expr,
    right_type: &DataType,
) -> Option<Expr> {
    let wide = spark_key_type(left_type, right_type)?;
    let widened = |expr: Expr, held: &DataType| {
        if held == &wide {
            expr
        } else {
            cast(expr, wide.clone())
        }
    };
    Some(coalesce(vec![
        widened(left, left_type),
        widened(right, right_type),
    ]))
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
    let below = projection
        .input
        .schema()
        .iter()
        .filter(|(_, field)| !field.name().starts_with(HIDDEN_PREFIX))
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect::<Vec<_>>();
    let shown = projection
        .expr
        .iter()
        .filter(|expr| {
            !matches!(expr, Expr::Column(column)
                if column.relation.is_none() && column.name.starts_with(HIDDEN_PREFIX))
        })
        .collect::<Vec<_>>();
    shown.len() == below.len()
        && shown
            .iter()
            .zip(below.iter())
            .all(|(expr, held)| plain_column(expr) == Some(held))
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
    if matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        || join
            .schema
            .fields()
            .iter()
            .any(|field| field.name().starts_with(HIDDEN_PREFIX))
    {
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
            let alias = hidden_alias(column.relation.as_ref(), &column.name);
            hidden.push((
                HiddenKey {
                    right: is_right,
                    side_position: position,
                    alias,
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
    let held = |name: &String| {
        projection.expr.iter().any(
            |expr| matches!(expr, Expr::Alias(alias) if alias.relation.is_none() && &alias.name == name),
        ) || projection
            .expr
            .iter()
            .any(|expr| matches!(expr, Expr::Column(column) if column.relation.is_none() && &column.name == name))
    };
    if let LogicalPlan::Join(join) = projection.input.as_ref() {
        let hidden = join_hidden(projection, join);
        for name in names.iter().filter(|name| !held(name)) {
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
    let below = projection.input.schema();
    let missing = names
        .iter()
        .filter(|name| !below.has_column_with_unqualified_name(name))
        .cloned()
        .collect::<Vec<_>>();
    let input = if missing.is_empty() {
        Arc::clone(&projection.input)
    } else {
        match expose_hidden_keys(projection.input.as_ref(), &missing)? {
            Some(input) => Arc::new(input),
            None => return Ok(None),
        }
    };
    exprs.extend(
        names
            .iter()
            .filter(|name| !held(name))
            .map(|name| Expr::Column(Column::new_unqualified(name))),
    );
    Projection::try_new(exprs, input)
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

#[must_use]
pub fn shown_columns(schema: &DFSchema) -> Vec<Expr> {
    schema
        .iter()
        .filter(|(_, field)| !field.name().starts_with(HIDDEN_PREFIX))
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
pub fn hidden_names_in_text(text: &str) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    if !text.contains(HIDDEN_PREFIX) {
        return names;
    }
    for word in text.split(|letter: char| !(letter.is_ascii_alphanumeric() || letter == '_')) {
        if word.starts_with(HIDDEN_PREFIX) && !names.iter().any(|held| held == word) {
            names.push(word.to_string());
        }
    }
    names
}
