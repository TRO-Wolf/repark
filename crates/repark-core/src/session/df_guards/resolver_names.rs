use std::collections::{BTreeSet, HashSet};

use datafusion::common::{
    Column, DFSchema, DataFusionError, Result, plan_datafusion_err, plan_err,
};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlanBuilder};
use repark_common::names::NameRule;
use repark_common::spark_error;

use super::case_bind::{sql_id, unique_case_match};

fn unresolved_join_key(key: &str, side: &str, schema: &DFSchema) -> DataFusionError {
    let mut columns: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    columns.sort_unstable();
    let rendered = columns
        .iter()
        .map(|name| sql_id(None, name))
        .collect::<Vec<_>>()
        .join(", ");
    let column = sql_id(None, key);
    plan_datafusion_err!(
        "{}",
        spark_error::message(
            spark_error::UNRESOLVED_USING_COLUMN_FOR_JOIN,
            &[
                ("column", column.as_str()),
                ("side", side),
                ("columns", rendered.as_str()),
            ],
        )
    )
}

fn unresolved_union_name(name: &str, right: &DataFrame) -> DataFusionError {
    let held = right
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "Cannot resolve column name \"{name}\" among ({held})."
    ))
}

fn bind_name(schema: &DFSchema, name: &str, rule: NameRule) -> Column {
    let bare = Column::new_unqualified(name);
    if schema.has_column_with_unqualified_name(name) {
        return bare;
    }
    match rule {
        NameRule::Exact => bare,
        NameRule::IgnoreCase => {
            unique_case_match(&bare, schema, NameRule::resolver_matches).unwrap_or(bare)
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn join_on_named_keys(
    left: DataFrame,
    right: DataFrame,
    keys: &[String],
    join_type: JoinType,
    rule: NameRule,
) -> Result<DataFrame> {
    if matches!(rule, NameRule::Exact) {
        for key in keys {
            if !left.schema().has_column_with_unqualified_name(key) {
                return Err(unresolved_join_key(key, "left", left.schema()));
            }
            if !right.schema().has_column_with_unqualified_name(key) {
                return Err(unresolved_join_key(key, "right", right.schema()));
            }
        }
    }
    let left_keys: Vec<Column> = keys
        .iter()
        .map(|key| bind_name(left.schema(), key, rule))
        .collect();
    let right_keys: Vec<Column> = keys
        .iter()
        .map(|key| bind_name(right.schema(), key, rule))
        .collect();
    let (state, left_plan) = left.into_parts();
    let plan = LogicalPlanBuilder::from(left_plan)
        .join(
            right.into_unoptimized_plan(),
            join_type,
            (left_keys, right_keys),
            None,
        )?
        .build()?;
    let joined = DataFrame::new(state, plan);
    if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
        return Ok(joined);
    }
    let mut seen: HashSet<String> = HashSet::new();
    let projection: Vec<Expr> = joined
        .schema()
        .iter()
        .filter(|(_, field)| {
            let matched = keys
                .iter()
                .find(|key| rule.resolver_matches(key, field.name()));
            matched.is_none_or(|key| seen.insert(key.clone()))
        })
        .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect();
    joined.select(projection)
}

#[allow(clippy::missing_errors_doc)]
pub fn union_by_folded_name(
    left: DataFrame,
    right: DataFrame,
    allow_missing: bool,
    rule: NameRule,
) -> Result<DataFrame> {
    if matches!(rule, NameRule::Exact) && !allow_missing {
        for name in left.schema().fields().iter().map(|field| field.name()) {
            if !right.schema().has_column_with_unqualified_name(name) {
                return Err(unresolved_union_name(name, &right));
            }
        }
    }
    let respelled: Vec<(Expr, bool)> = right
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            let held = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            let bound = bind_name(left.schema(), field.name(), rule);
            if bound.name == *field.name() {
                (held, false)
            } else {
                (held.alias(bound.name), true)
            }
        })
        .collect();
    let right = if respelled.iter().any(|(_, renamed)| *renamed) {
        right.select(
            respelled
                .into_iter()
                .map(|(expr, _)| expr)
                .collect::<Vec<_>>(),
        )?
    } else {
        right
    };
    if !allow_missing {
        let left_names = field_names(&left);
        let right_names = field_names(&right);
        if left_names != right_names {
            let mismatched = left_names
                .symmetric_difference(&right_names)
                .map(|name| format!("'{name}'"))
                .collect::<Vec<_>>()
                .join(", ");
            return plan_err!(
                "Union can only be performed on inputs with the same columns unless \
                 allowMissingColumns=True; mismatched columns: [{mismatched}]"
            );
        }
    }
    left.union_by_name(right)
}

fn field_names(frame: &DataFrame) -> BTreeSet<String> {
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}
