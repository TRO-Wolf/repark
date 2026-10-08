use std::collections::HashSet;

use datafusion::common::{Column, DFSchema, Result, TableReference};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};

pub struct ExactJoin<'a> {
    pub left_alias: &'a str,
    pub right_alias: &'a str,
    pub join_type: Option<JoinType>,
    pub keys: Option<ExactKeys<'a>>,
    pub left_outputs: &'a [(String, String)],
    pub right_outputs: &'a [(String, String)],
}

#[derive(Clone, Copy)]
pub struct ExactKeys<'a> {
    pub left: &'a str,
    pub right: &'a str,
    pub left_first: bool,
}

#[allow(clippy::missing_errors_doc)]
pub fn join_exact_sides(
    left: LogicalPlan,
    right: LogicalPlan,
    join: &ExactJoin<'_>,
) -> Result<Option<LogicalPlan>> {
    let (Some(left_ref), Some(right_ref)) = (
        home_reference(join.left_alias),
        home_reference(join.right_alias),
    ) else {
        return Ok(None);
    };
    if !side_is_exact(left.schema(), join.left_outputs)
        || !side_is_exact(right.schema(), join.right_outputs)
    {
        return Ok(None);
    }
    let condition = match (join.join_type, join.keys) {
        (None, _) => None,
        (Some(_), None) => return Ok(None),
        (Some(join_type), Some(keys)) => {
            if !held_once(left.schema(), keys.left) || !held_once(right.schema(), keys.right) {
                return Ok(None);
            }
            let left_key = side_column(&left_ref, keys.left);
            let right_key = side_column(&right_ref, keys.right);
            let condition = if keys.left_first {
                left_key.eq(right_key)
            } else {
                right_key.eq(left_key)
            };
            Some((join_type, condition))
        }
    };
    let left_side = LogicalPlanBuilder::from(left).alias(left_ref.clone())?;
    let right_side = LogicalPlanBuilder::from(right)
        .alias(right_ref.clone())?
        .build()?;
    let joined = match condition {
        None => left_side.cross_join(right_side)?,
        Some((join_type, condition)) => left_side.join_on(right_side, join_type, [condition])?,
    };
    let outputs = side_outputs(&left_ref, join.left_outputs)
        .chain(side_outputs(&right_ref, join.right_outputs))
        .collect::<Vec<_>>();
    joined.project(outputs)?.build().map(Some)
}

fn home_reference(alias: &str) -> Option<TableReference> {
    let inner = alias.strip_prefix('`')?.strip_suffix('`')?;
    let mut segments = inner.split("`.`");
    let (catalog, schema, table) = (segments.next()?, segments.next()?, segments.next()?);
    if segments.next().is_some() || ![catalog, schema, table].into_iter().all(is_plain) {
        return None;
    }
    Some(TableReference::full(catalog, schema, table))
}

fn is_plain(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn side_is_exact(schema: &DFSchema, outputs: &[(String, String)]) -> bool {
    let mut folded = HashSet::with_capacity(schema.fields().len());
    schema
        .fields()
        .iter()
        .all(|field| is_plain(field.name()) && folded.insert(field.name().to_ascii_lowercase()))
        && outputs
            .iter()
            .all(|(source, output)| is_plain(output) && held_once(schema, source))
}

fn held_once(schema: &DFSchema, name: &str) -> bool {
    schema
        .fields()
        .iter()
        .filter(|field| field.name() == name)
        .count()
        == 1
}

fn side_column(side: &TableReference, name: &str) -> Expr {
    Expr::Column(Column::new(Some(side.clone()), name))
}

fn side_outputs<'a>(
    side: &'a TableReference,
    outputs: &'a [(String, String)],
) -> impl Iterator<Item = Expr> + 'a {
    outputs.iter().map(move |(source, output)| {
        let column = side_column(side, source);
        if source == output {
            column
        } else {
            column.alias(output)
        }
    })
}
