use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    Column, DFSchema, DataFusionError, Location, Result, Span, Spans, TableReference,
    plan_datafusion_err, plan_err,
};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlanBuilder};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, visit_expressions};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use repark_common::spark_error;

use super::attr_id::same_relation;

pub use super::attr_id::{
    AttrId, Resolution, alias_with_fresh_id, attribute_ids, copy_attribute_ids, stamp, strip,
    strip_for_execution, strip_schema_ids,
};
pub use super::attr_id::{
    join_collisions, plan_is_relation, plan_is_stamped, remint_shared, remint_with_map, resolve,
};
pub use super::attr_lineage::{
    projection_source_ids, sort_hits_meet_at_join, sort_sourced_twin_engine,
};
pub use super::frame_lineage::{AttrRef, FrameId, FrameKind, FrameNode};
pub use super::frame_lineage::{all_ids, ambiguous, ambiguous_images, renewed_absent, shared_ids};
pub use super::predicate_names::fold_frame_qualifiers;
pub use super::self_join::{AttrRefText, JoinSide, Prepared, PreparedCondition, Refusal};
pub use super::self_join::{SELF_JOIN_CONDITION, SelfJoinRules, check_refs, missing_condition};
pub use super::self_join::{missing_message, parse_attr_refs, prepare_join_condition};
pub use super::self_join::{quoted_names, self_join_message};
pub use super::sort_names::{FreeNameOffense, free_expr_names, refuse_free_names};
pub use super::sort_names::{SortShape, bind_free_names, bind_qualified_free_refs};
pub use super::sort_names::{engine_field_is_unique, grandchild_key, grandchild_qualified_key};
pub use super::sort_names::{join_dup_below_wrappers, join_output_sources};
pub use super::sort_names::{
    project_input_spelling, qualifier_star_positions, sort_shape, union_dup_below_wrappers,
};
pub use super::subquery::resolve_bound_expr_with;
pub use super::written_names::refuse_folded_duplicate_keys;
pub use repark_common::names::{NameHit, NameRule};

pub(crate) type Hit<'a> = (Option<&'a TableReference>, &'a Field);

const ATTRIBUTE_MARK: Location = Location {
    line: u64::MAX,
    column: u64::MAX,
};

#[must_use]
pub fn attribute_reference(name: &str) -> Expr {
    Expr::Column(Column {
        relation: None,
        name: name.to_string(),
        spans: Spans(vec![Span::new(ATTRIBUTE_MARK, ATTRIBUTE_MARK)]),
    })
}

#[must_use]
pub fn attribute_copy_name(name: &str) -> String {
    name.bytes()
        .fold(String::from("__repark_attr_"), |mut spelled, byte| {
            let _ = write!(spelled, "{byte:02x}");
            spelled
        })
}

#[must_use]
pub fn attribute_copy_name_in(schema: &DFSchema, name: &str) -> String {
    let mut spelled = attribute_copy_name(name);
    while schema.fields().iter().any(|field| *field.name() == spelled) {
        spelled.push('_');
    }
    spelled
}

#[allow(clippy::missing_errors_doc)]
pub fn with_attribute_copies(frame: DataFrame) -> Result<DataFrame> {
    let schema = frame.schema();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for field in schema.fields() {
        *counts.entry(field.name().as_str()).or_default() += 1;
    }
    let held = schema
        .iter()
        .map(|(qualifier, field)| (Column::new(qualifier.cloned(), field.name()), field.name()))
        .collect::<Vec<_>>();
    let copies = held
        .iter()
        .filter(|(_, name)| counts.get(name.as_str()) == Some(&1))
        .map(|(column, name)| {
            Expr::Column(column.clone()).alias(attribute_copy_name_in(schema, name))
        });
    let projection = held
        .iter()
        .map(|(column, _)| Expr::Column(column.clone()))
        .chain(copies)
        .collect::<Vec<_>>();
    frame.select(projection)
}

#[allow(clippy::missing_errors_doc)]
pub fn rename_output_fields(frame: DataFrame, names: &[String]) -> Result<DataFrame> {
    let schema = frame.schema();
    if names.len() != schema.fields().len() {
        return plan_err!(
            "rename needs one name per output field: {} fields, {} names",
            schema.fields().len(),
            names.len()
        );
    }
    let projection = schema
        .iter()
        .zip(names.iter())
        .map(|((qualifier, field), name)| {
            Expr::Column(Column::new(qualifier.cloned(), field.name())).alias(name)
        })
        .collect::<Vec<_>>();
    frame.select(projection)
}

#[must_use]
pub fn is_scratch_relation(table: &str) -> bool {
    table.starts_with("_repark_") || table.starts_with("__repark_")
}

fn is_attribute(column: &Column) -> bool {
    column.spans.iter().any(|span| span.start == ATTRIBUTE_MARK)
}

pub(crate) fn bind_names(expr: Expr, frame_schema: &DFSchema, rule: NameRule) -> Result<Expr> {
    expr.transform(|node| {
        Ok(match node {
            Expr::Column(column) if is_attribute(&column) => Transformed::no(Expr::Column(column)),
            Expr::Column(column) => {
                let hits = case_hits(&column, [frame_schema], rule);
                if hits.len() > 1 {
                    return Err(ambiguous_reference(&column, &hits));
                }
                match rule {
                    NameRule::Exact if hits.is_empty() => {
                        if case_hits(&column, [frame_schema], NameRule::IgnoreCase).is_empty() {
                            Transformed::no(Expr::Column(column))
                        } else {
                            return Err(unresolved_column(&column, frame_schema));
                        }
                    }
                    NameRule::Exact => Transformed::no(Expr::Column(column)),
                    NameRule::IgnoreCase => match unique_case_match(&column, frame_schema) {
                        Some(bound) => Transformed::yes(Expr::Column(bound)),
                        None => Transformed::no(Expr::Column(column)),
                    },
                }
            }
            Expr::Alias(alias) => match written_segment(&alias.expr, &alias.name) {
                Some(segment) => Transformed::yes(Expr::Alias(Alias {
                    name: segment,
                    ..alias
                })),
                None => Transformed::no(Expr::Alias(alias)),
            },
            other => Transformed::no(other),
        })
    })
    .map(|transformed| transformed.data)
}

fn case_hits<'a>(
    column: &Column,
    schemas: impl IntoIterator<Item = &'a DFSchema>,
    rule: NameRule,
) -> Vec<Hit<'a>> {
    schemas
        .into_iter()
        .flat_map(DFSchema::iter)
        .filter(|(qualifier, field)| {
            rule.matches(&column.name, field.name())
                && column.relation.as_ref().is_none_or(|written| {
                    qualifier.is_some_and(|held| same_relation(written, held, rule))
                })
        })
        .map(|(qualifier, field)| (qualifier, field.as_ref()))
        .collect()
}

fn sql_id(relation: Option<&TableReference>, name: &str) -> String {
    let spelled = relation.filter(|relation| !is_scratch_relation(relation.table()));
    let parts = spelled.map_or_else(Vec::new, |relation| {
        [
            relation.catalog(),
            relation.schema(),
            Some(relation.table()),
        ]
        .into_iter()
        .flatten()
        .collect()
    });
    parts
        .into_iter()
        .chain(std::iter::once(name))
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) fn ambiguous_reference(column: &Column, hits: &[Hit<'_>]) -> DataFusionError {
    let mut options = hits
        .iter()
        .map(|(qualifier, _)| sql_id(*qualifier, &column.name))
        .collect::<Vec<_>>();
    options.sort();
    let reference = sql_id(column.relation.as_ref(), &column.name);
    plan_datafusion_err!(
        "{}",
        spark_error::message(
            spark_error::AMBIGUOUS_REFERENCE,
            &[("reference", &reference), ("options", &options.join(", "))],
        )
    )
}

#[must_use]
pub fn unresolved_column(column: &Column, frame_schema: &DFSchema) -> DataFusionError {
    let reference = sql_id(column.relation.as_ref(), &column.name);
    let suggestions = frame_schema
        .iter()
        .filter(|(qualifier, field)| {
            qualifier.is_none_or(|held| !is_scratch_relation(held.table()))
                && !is_scratch_relation(field.name())
        })
        .map(|(_, field)| sql_id(None, field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    plan_datafusion_err!(
        "{}",
        spark_error::message(
            spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
            &[("columnName", &reference), ("suggestions", &suggestions)],
        )
    )
}

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

#[allow(clippy::missing_errors_doc)]
pub fn refuse_ambiguous_condition(condition_sql: &str, sides: &[&DFSchema]) -> Result<()> {
    let Ok(condition) = Parser::new(&DatabricksDialect {})
        .try_with_sql(condition_sql)
        .and_then(|mut parser| parser.parse_expr())
    else {
        return Ok(());
    };
    let refusal = visit_expressions(&condition, |node| match node {
        SqlExpr::Identifier(ident) => {
            let column = Column::new_unqualified(ident.value.as_str());
            let hits = case_hits(&column, sides.iter().copied(), NameRule::IgnoreCase);
            if hits.len() > 1 {
                ControlFlow::Break(ambiguous_reference(&column, &hits))
            } else {
                ControlFlow::Continue(())
            }
        }
        _ => ControlFlow::Continue(()),
    });
    match refusal {
        ControlFlow::Break(error) => Err(error),
        ControlFlow::Continue(()) => Ok(()),
    }
}

#[must_use]
pub fn free_sql_names(sql: &str, select_item: bool) -> Vec<(Vec<String>, String)> {
    if select_item {
        if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, &format!("SELECT {sql}")) {
            return sql_statement_names(&statements);
        }
        return Vec::new();
    }
    if let Ok(parsed) = Parser::new(&DatabricksDialect {})
        .try_with_sql(sql)
        .and_then(|mut parser| parser.parse_expr())
    {
        return sql_expr_names(&parsed);
    }
    if let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, &format!("SELECT ({sql})")) {
        return sql_statement_names(&statements);
    }
    Vec::new()
}

fn sql_statement_names(
    statements: &[datafusion::sql::sqlparser::ast::Statement],
) -> Vec<(Vec<String>, String)> {
    use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
    for statement in statements {
        let Statement::Query(query) = statement else {
            return Vec::new();
        };
        if query.with.is_some() {
            return Vec::new();
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Vec::new();
        };
        if !select.from.is_empty() {
            return Vec::new();
        }
    }
    let mut names = Vec::new();
    let mut nested = false;
    for statement in statements {
        let _: ControlFlow<()> = visit_expressions(statement, |node| {
            collect_sql_ident(node, &mut names, &mut nested);
            ControlFlow::<()>::Continue(())
        });
    }
    if nested { Vec::new() } else { names }
}

fn sql_expr_names(parsed: &SqlExpr) -> Vec<(Vec<String>, String)> {
    let mut names = Vec::new();
    let mut nested = false;
    let _: ControlFlow<()> = visit_expressions(parsed, |node| {
        collect_sql_ident(node, &mut names, &mut nested);
        ControlFlow::<()>::Continue(())
    });
    if nested { Vec::new() } else { names }
}

fn collect_sql_ident(node: &SqlExpr, names: &mut Vec<(Vec<String>, String)>, nested: &mut bool) {
    match node {
        SqlExpr::Identifier(ident) => {
            names.push((Vec::new(), ident.value.clone()));
        }
        SqlExpr::CompoundIdentifier(parts) => {
            if let Some((last, head)) = parts.split_last() {
                names.push((
                    head.iter().map(|part| part.value.clone()).collect(),
                    last.value.clone(),
                ));
            }
        }
        SqlExpr::Subquery(_) | SqlExpr::Exists { .. } | SqlExpr::InSubquery { .. } => {
            *nested = true;
        }
        _ => (),
    }
}

#[must_use]
pub fn sql_mentions_duplicate(sql: &str, displays: &[String], rule: NameRule) -> bool {
    let dups = displays
        .iter()
        .filter(|display| {
            displays
                .iter()
                .filter(|other| rule.matches(other, display))
                .count()
                > 1
        })
        .collect::<Vec<_>>();
    if dups.is_empty() {
        return false;
    }
    if dups.iter().any(|display| {
        !display
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }) {
        return true;
    }
    sql_text_tokens(sql)
        .iter()
        .any(|token| dups.iter().any(|display| rule.matches(token, display)))
}

fn sql_text_tokens(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut word: Option<usize> = None;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'`' {
            if let Some(begin) = word.take() {
                tokens.push(&sql[begin..index]);
            }
            match sql[index + 1..].find('`') {
                Some(end) => {
                    tokens.push(&sql[index + 1..index + 1 + end]);
                    index += end + 2;
                }
                None => index += 1,
            }
            continue;
        }
        if bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' {
            if word.is_none() {
                word = Some(index);
            }
        } else if let Some(begin) = word.take() {
            tokens.push(&sql[begin..index]);
        }
        index += 1;
    }
    if let Some(begin) = word {
        tokens.push(&sql[begin..]);
    }
    tokens
}

#[allow(clippy::missing_errors_doc)]
pub fn requalify_join_sides(joined: DataFrame, sides: &[&DFSchema]) -> Result<DataFrame> {
    let held = sides
        .iter()
        .flat_map(|side| side.iter())
        .collect::<Vec<_>>();
    if held.len() != joined.schema().fields().len() {
        return Ok(joined);
    }
    let projection = joined
        .schema()
        .iter()
        .zip(held)
        .map(|((qualifier, field), (side, _))| {
            Expr::Column(Column::new(qualifier.cloned(), field.name()))
                .alias_qualified(side.cloned(), field.name())
        })
        .collect::<Vec<_>>();
    Ok(joined.clone().select(projection).unwrap_or(joined))
}

#[allow(clippy::missing_errors_doc)]
pub fn bind_projection_expr(expr: Expr, frame_schema: &DFSchema, rule: NameRule) -> Result<Expr> {
    let written = match &expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Cast(cast) => cast_child_name(&cast.expr),
        Expr::TryCast(cast) => cast_child_name(&cast.expr),
        _ => None,
    };
    let bound = super::subquery::resolve_bound_expr_with(expr, frame_schema, rule)?;
    Ok(match (written, &bound) {
        (Some(written), Expr::Column(held)) if held.name != written => {
            let relation = held.relation.clone();
            bound.alias_qualified(relation, written)
        }
        (Some(written), Expr::Cast(_) | Expr::TryCast(_)) => bound.alias(written),
        _ => bound,
    })
}

fn cast_child_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Cast(cast) => cast_child_name(&cast.expr),
        Expr::TryCast(cast) => cast_child_name(&cast.expr),
        _ => None,
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_written_names(
    frame_schema: &DFSchema,
    names: &[String],
    rule: NameRule,
) -> Result<Vec<(String, String)>> {
    let held: Vec<String> = frame_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    names
        .iter()
        .map(|written| {
            let column = Column::new_unqualified(written);
            match rule.lookup(written, &held) {
                NameHit::One(hit) => Ok((written.clone(), hit.to_string())),
                NameHit::Many(_) => {
                    let hits = case_hits(&column, [frame_schema], rule);
                    Err(ambiguous_reference(&column, &hits))
                }
                NameHit::CaseOnly(_) | NameHit::None => {
                    Err(unresolved_column(&column, frame_schema))
                }
            }
        })
        .collect()
}

fn written_segment(expr: &Expr, alias: &str) -> Option<String> {
    let Expr::Column(Column {
        relation: Some(relation),
        name,
        ..
    }) = expr
    else {
        return None;
    };
    let prefix = relation.to_string();
    let written = alias.get(prefix.len() + 1..)?;
    let qualified = alias.get(..=prefix.len())?;
    (qualified.eq_ignore_ascii_case(&format!("{prefix}.")) && written.eq_ignore_ascii_case(name))
        .then(|| written.to_string())
}

fn unique_case_match(column: &Column, frame_schema: &DFSchema) -> Option<Column> {
    let exact = match &column.relation {
        Some(_) => frame_schema.has_column(column),
        None => frame_schema.has_column_with_unqualified_name(&column.name),
    };
    if exact {
        return None;
    }
    let hits = case_hits(column, [frame_schema], NameRule::IgnoreCase);
    let (first_qualifier, first_field) = hits.first()?;
    let first = Column::new(first_qualifier.cloned(), first_field.name());
    hits.iter()
        .all(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()) == first)
        .then_some(first)
}

fn bind_name(schema: &DFSchema, name: &str, rule: NameRule) -> Column {
    let bare = Column::new_unqualified(name);
    if schema.has_column_with_unqualified_name(name) {
        return bare;
    }
    match rule {
        NameRule::Exact => bare,
        NameRule::IgnoreCase => unique_case_match(&bare, schema).unwrap_or(bare),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn drop_named_columns(
    frame: DataFrame,
    names: &[String],
    references: &[String],
    attributes: &[String],
    rule: NameRule,
) -> Result<DataFrame> {
    let schema = frame.schema();
    let mut hits = names
        .iter()
        .flat_map(|name| case_hits(&Column::new_unqualified(name.as_str()), [schema], rule))
        .collect::<Vec<_>>();
    for reference in references
        .iter()
        .map(Column::from_qualified_name_ignore_case)
    {
        let found = case_hits(&reference, [schema], rule);
        if found.len() > 1 {
            return Err(ambiguous_reference(&reference, &found));
        }
        hits.extend(found);
    }
    hits.extend(
        schema
            .iter()
            .filter(|(_, field)| attributes.contains(field.name()))
            .map(|(qualifier, field)| (qualifier, field.as_ref())),
    );
    let targets = hits
        .into_iter()
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect::<Vec<_>>();
    frame.drop_columns(&targets)
}

#[allow(clippy::missing_errors_doc)]
pub fn join_on_named_keys(
    left: DataFrame,
    right: DataFrame,
    keys: &[String],
    join_type: JoinType,
    rule: NameRule,
    left_node: Arc<FrameNode>,
    right_node: Arc<FrameNode>,
) -> Result<(DataFrame, Arc<FrameNode>)> {
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
    let left_schema = left.schema().clone();
    let right_outputs = attribute_ids(right.schema())
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let shared = shared_ids(&left_node, &right_outputs);
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
        let remint = shared
            .iter()
            .map(|id| (id.clone(), AttrId::mint()))
            .collect();
        let node = FrameNode::join(joined.schema(), left_node, right_node, remint, false)?;
        return Ok((joined, node));
    }
    let mut seen: HashSet<String> = HashSet::new();
    let mut right_kept = 0usize;
    let projection: Vec<Expr> = joined
        .schema()
        .iter()
        .enumerate()
        .filter(|(index, (_, field))| {
            let matched = keys.iter().find(|key| rule.matches(key, field.name()));
            let keep = matched.is_none_or(|key| seen.insert(key.clone()));
            right_kept += usize::from(keep && *index >= left_schema.fields().len());
            keep
        })
        .map(|(_, (qualifier, field))| Expr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect();
    let right_start = projection.len() - right_kept;
    let (state, plan) = joined.select(projection)?.into_parts();
    let (plan, remint) = remint_shared(plan, right_start, &shared)?;
    let schema = plan.schema().clone();
    let node = FrameNode::join(&schema, left_node, right_node, remint, true)?;
    Ok((DataFrame::new(state, plan), node))
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
