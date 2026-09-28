use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write;
use std::ops::ControlFlow;

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

pub use super::cast_names::bind_projection_expr;
pub use super::subquery::resolve_bound_expr_with;
pub use super::written_names::{Disposition, refuse_folded_duplicate_keys, unresolved_subset_name};
pub use super::written_names::{match_display_names, match_subset_names, resolve_df_names};
pub use super::written_names::{resolve_qualified_display_names, rewrite_join_condition_aliases};
pub use repark_common::names::{NameHit, NameRule};

type Hit<'a> = (Option<&'a TableReference>, &'a Field);

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

#[must_use]
pub fn is_scratch_relation(table: &str) -> bool {
    table.starts_with("_repark_") || table.starts_with("__repark_")
}

fn is_attribute(column: &Column) -> bool {
    column.spans.iter().any(|span| span.start == ATTRIBUTE_MARK)
}

pub(super) fn bind_names(expr: Expr, frame_schema: &DFSchema, rule: NameRule) -> Result<Expr> {
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

fn ambiguous_reference(column: &Column, hits: &[Hit<'_>]) -> DataFusionError {
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

pub(super) fn unresolved_column(column: &Column, frame_schema: &DFSchema) -> DataFusionError {
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

fn same_relation(written: &TableReference, held: &TableReference, rule: NameRule) -> bool {
    let written_parts = [written.catalog(), written.schema(), Some(written.table())];
    let held_parts = [held.catalog(), held.schema(), Some(held.table())];
    written_parts
        .iter()
        .zip(held_parts.iter())
        .all(
            |(written_part, held_part)| match (written_part, held_part) {
                (Some(written_part), Some(held_part)) => rule.matches(written_part, held_part),
                (Some(_), None) => false,
                (None, _) => true,
            },
        )
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
            let matched = keys.iter().find(|key| rule.matches(key, field.name()));
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{Column, DFSchema, JoinType, TableReference};
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::{Expr, col, lit};
    use datafusion::prelude::SessionContext;

    use super::{
        NameRule::{Exact, IgnoreCase},
        attribute_reference, bind_names, bind_projection_expr, drop_named_columns,
        join_on_named_keys, refuse_ambiguous_condition, requalify_join_sides, union_by_folded_name,
    };

    fn frame(names: &[(&str, &str)]) -> DFSchema {
        let fields = names
            .iter()
            .map(|(qualifier, name)| {
                (
                    Some((*qualifier).into()),
                    Arc::new(Field::new(*name, DataType::Int64, true)),
                )
            })
            .collect::<Vec<_>>();
        DFSchema::new_with_metadata(fields, std::collections::HashMap::new()).unwrap()
    }

    #[test]
    fn folded_reference_binds_the_single_spelled_field() {
        let schema = frame(&[("t", "ID"), ("t", "data")]);
        let bound = bind_names(col("ID").gt(lit(1i64)), &schema, IgnoreCase).unwrap();
        assert_eq!(
            bound,
            Expr::Column(Column::new(Some("t"), "ID")).gt(lit(1i64))
        );
    }

    fn refusal(expr: Expr, schema: &DFSchema) -> String {
        bind_names(expr, schema, IgnoreCase)
            .unwrap_err()
            .to_string()
    }

    fn ambiguous(reference: &str, options: &str) -> String {
        format!(
            "Error during planning: [AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, \
             could be: [{options}]. SQLSTATE: 42704"
        )
    }

    fn unresolved(reference: &str, options: &str) -> String {
        format!(
            "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
             function parameter with name {reference} cannot be resolved. Did you mean one of the \
             following? [{options}]. SQLSTATE: 42703"
        )
    }

    #[test]
    fn exact_and_ambiguous_references_stay() {
        let schema = frame(&[("t", "id"), ("u", "ID")]);
        assert_eq!(
            refusal(col("id"), &schema),
            ambiguous("`id`", "`t`.`id`, `u`.`id`")
        );
        let twins = frame(&[("t", "Id"), ("u", "ID")]);
        assert_eq!(
            refusal(col("id"), &twins),
            ambiguous("`id`", "`t`.`id`, `u`.`id`")
        );
        let exact = frame(&[("t", "id"), ("t", "ID")]);
        assert_eq!(
            refusal(Expr::Column(Column::new(Some("t"), "id")), &exact),
            ambiguous("`t`.`id`", "`t`.`id`, `t`.`id`")
        );
    }

    #[test]
    fn ambiguous_candidates_render_sorted_like_spark() {
        let joined = frame(&[("a", "id"), ("b", "ID")]);
        assert_eq!(
            refusal(col("id"), &joined),
            ambiguous("`id`", "`a`.`id`, `b`.`id`")
        );
        assert_eq!(
            refusal(
                Expr::Column(Column::new_unqualified("ID")).gt(lit(1i64)),
                &joined
            ),
            ambiguous("`ID`", "`a`.`ID`, `b`.`ID`")
        );
        let scratch = frame(&[("__repark_cdf_54fd", "id"), ("__repark_cdf_54fd", "ID")]);
        assert_eq!(
            refusal(col("id"), &scratch),
            ambiguous("`id`", "`id`, `id`")
        );
        let reversed = frame(&[("b", "id"), ("a", "ID")]);
        assert_eq!(
            refusal(col("id"), &reversed),
            ambiguous("`id`", "`a`.`id`, `b`.`id`")
        );
        let narrowed = Expr::Column(Column::new(Some("a"), "Id"));
        assert_eq!(
            bind_names(narrowed, &joined, IgnoreCase).unwrap(),
            Expr::Column(Column::new(Some("a"), "id"))
        );
    }

    fn sides() -> (DFSchema, DFSchema) {
        let held = TableReference::full("sc", "ns", "t_vz_1");
        let left = DFSchema::new_with_metadata(
            vec![
                (
                    Some(held.clone()),
                    Arc::new(Field::new("ID", DataType::Int32, true)),
                ),
                (
                    Some(held),
                    Arc::new(Field::new("data", DataType::Utf8, true)),
                ),
            ],
            std::collections::HashMap::new(),
        )
        .unwrap();
        let right = DFSchema::from_unqualified_fields(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("w", DataType::Utf8, false),
            ]
            .into(),
            std::collections::HashMap::new(),
        )
        .unwrap();
        (left, right)
    }

    #[test]
    fn unqualified_and_catalog_candidates_render_like_spark() {
        let (left, right) = sides();
        let joined = left.join(&right).unwrap();
        assert_eq!(
            refusal(col("id"), &joined),
            ambiguous("`id`", "`id`, `sc`.`ns`.`t_vz_1`.`id`")
        );
        assert_eq!(
            refuse_ambiguous_condition("(`ID` = `id`)", &[&left, &right])
                .unwrap_err()
                .to_string(),
            ambiguous("`ID`", "`ID`, `sc`.`ns`.`t_vz_1`.`ID`")
        );
        assert_eq!(
            refuse_ambiguous_condition("(`id` = `ID`)", &[&right, &left])
                .unwrap_err()
                .to_string(),
            ambiguous("`id`", "`id`, `sc`.`ns`.`t_vz_1`.`id`")
        );
        assert!(refuse_ambiguous_condition("(l.`ID` = r.`id`)", &[&left, &right]).is_ok());
        assert!(refuse_ambiguous_condition("(`data` = `w`)", &[&left, &right]).is_ok());
    }

    #[tokio::test]
    async fn requalified_join_carries_each_side_relation() {
        let context = SessionContext::new();
        let left = context
            .sql(r#"SELECT "ID", data FROM (SELECT 1 AS "ID", 'a' AS data) t"#)
            .await
            .unwrap();
        let right = context.sql("SELECT 1 AS id, 'q' AS w").await.unwrap();
        let joined = context
            .sql(r#"SELECT l."ID", l.data, r.id, r.w FROM (SELECT 1 AS "ID", 'a' AS data) l CROSS JOIN (SELECT 1 AS id, 'q' AS w) r"#)
            .await
            .unwrap();
        let requalified = requalify_join_sides(joined, &[left.schema(), right.schema()]).unwrap();
        let qualifiers = requalified
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.map(ToString::to_string), field.name().clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            qualifiers,
            vec![
                (Some("t".to_string()), "ID".to_string()),
                (Some("t".to_string()), "data".to_string()),
                (None, "id".to_string()),
                (None, "w".to_string()),
            ]
        );
        assert_eq!(
            refusal(col("id"), requalified.schema()),
            ambiguous("`id`", "`id`, `t`.`id`")
        );
    }

    #[test]
    fn qualified_reference_binds_through_its_relation() {
        let schema = frame(&[("t", "ID"), ("t", "data")]);
        let written = Expr::Column(Column::new(Some("t"), "id"));
        assert_eq!(
            bind_names(written, &schema, IgnoreCase).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID"))
        );
        let other = Expr::Column(Column::new(Some("x"), "id"));
        assert_eq!(
            bind_names(other.clone(), &schema, IgnoreCase).unwrap(),
            other
        );
    }

    #[test]
    fn qualified_alias_names_the_written_segment() {
        let schema = frame(&[("t", "ID")]);
        let aliased = Expr::Column(Column::new(Some("t"), "id")).alias("t.ID");
        assert_eq!(
            bind_names(aliased, &schema, IgnoreCase).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID")).alias("ID")
        );
        let chosen = Expr::Column(Column::new(Some("t"), "id")).alias("other");
        assert_eq!(
            bind_names(chosen, &schema, IgnoreCase).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID")).alias("other")
        );
    }

    #[test]
    fn projection_keeps_the_written_spelling() {
        let schema = frame(&[("t", "ID")]);
        assert_eq!(
            bind_projection_expr(col("id"), &schema, IgnoreCase).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID")).alias_qualified(Some("t"), "id")
        );
        let held = Expr::Column(Column::new(Some("t"), "ID"));
        assert_eq!(
            bind_projection_expr(held.clone(), &schema, IgnoreCase).unwrap(),
            held
        );
    }

    #[test]
    fn attribute_reference_binds_exactly_where_a_written_one_refuses() {
        let twins = frame(&[("t", "id"), ("t", "ID")]);
        let attribute = attribute_reference("ID");
        assert_eq!(
            bind_names(attribute.clone(), &twins, IgnoreCase).unwrap(),
            attribute
        );
        assert_eq!(
            bind_projection_expr(attribute.clone(), &twins, IgnoreCase).unwrap(),
            attribute
        );
        assert_eq!(
            refusal(Expr::Column(Column::new_unqualified("ID")), &twins),
            ambiguous("`ID`", "`t`.`ID`, `t`.`ID`")
        );
    }

    async fn spelled(sql: &str) -> DataFrame {
        SessionContext::new().sql(sql).await.unwrap()
    }

    fn names(frame: &DataFrame) -> Vec<String> {
        frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    async fn row_count(frame: DataFrame) -> usize {
        frame.count().await.unwrap()
    }

    #[tokio::test]
    async fn drop_removes_every_folded_match() {
        let frame = spelled(r#"SELECT 1 AS "ID", 'a' AS data"#).await;
        let dropped =
            drop_named_columns(frame.clone(), &["id".to_string()], &[], &[], IgnoreCase).unwrap();
        assert_eq!(names(&dropped), vec!["data".to_string()]);
        let both = ["ID".to_string(), "DATA".to_string()];
        assert!(
            names(&drop_named_columns(frame.clone(), &both, &[], &[], IgnoreCase).unwrap())
                .is_empty()
        );
        let absent =
            drop_named_columns(frame, &["nope".to_string()], &[], &[], IgnoreCase).unwrap();
        assert_eq!(names(&absent), vec!["ID".to_string(), "data".to_string()]);
    }

    #[tokio::test]
    async fn qualified_drop_binds_through_its_relation() {
        let frame = spelled(r#"SELECT "ID", data FROM (SELECT 1 AS "ID", 'a' AS data) t"#).await;
        let whole = vec!["ID".to_string(), "data".to_string()];
        for written in ["t.ID", "t.id", "T.Id"] {
            let dropped =
                drop_named_columns(frame.clone(), &[], &[written.to_string()], &[], IgnoreCase)
                    .unwrap();
            assert_eq!(names(&dropped), vec!["data".to_string()]);
        }
        for written in ["t.ID", "u.id"] {
            let named =
                drop_named_columns(frame.clone(), &[written.to_string()], &[], &[], IgnoreCase)
                    .unwrap();
            assert_eq!(names(&named), whole);
        }
        let unmatched =
            drop_named_columns(frame, &[], &["u.id".to_string()], &[], IgnoreCase).unwrap();
        assert_eq!(names(&unmatched), whole);
        let joined = spelled(
            r#"SELECT a.id, b."ID" FROM (SELECT 1 AS id, 1 AS "ID") a JOIN (SELECT 1 AS id, 1 AS "ID") b ON a.id = b.id"#,
        )
        .await;
        let right = drop_named_columns(joined.clone(), &[], &["b.id".to_string()], &[], IgnoreCase)
            .unwrap();
        assert_eq!(names(&right), vec!["id".to_string()]);
        let left = drop_named_columns(joined, &[], &["A.ID".to_string()], &[], IgnoreCase).unwrap();
        assert_eq!(names(&left), vec!["ID".to_string()]);
    }

    #[tokio::test]
    async fn attribute_drop_is_exact_and_a_two_hit_reference_refuses() {
        let twins = spelled(r#"SELECT 1 AS id, 2 AS "ID""#).await;
        let exact =
            drop_named_columns(twins.clone(), &[], &[], &["ID".to_string()], IgnoreCase).unwrap();
        assert_eq!(names(&exact), vec!["id".to_string()]);
        let error = drop_named_columns(twins.clone(), &[], &["id".to_string()], &[], IgnoreCase)
            .unwrap_err()
            .to_string();
        assert_eq!(error, ambiguous("`id`", "`id`, `id`"));
        let named = drop_named_columns(twins, &["id".to_string()], &[], &[], IgnoreCase).unwrap();
        assert!(names(&named).is_empty());
    }

    #[tokio::test]
    async fn join_binds_each_side_and_keeps_one_key() {
        let left = spelled(r#"SELECT 1 AS "ID", 'a' AS data"#).await;
        let right = spelled("SELECT 1 AS id, 'q' AS w").await;
        let keys = ["Id".to_string()];
        let joined = join_on_named_keys(
            left.clone(),
            right.clone(),
            &keys,
            JoinType::Inner,
            IgnoreCase,
        )
        .unwrap();
        assert_eq!(
            names(&joined),
            vec!["ID".to_string(), "data".to_string(), "w".to_string()]
        );
        assert_eq!(row_count(joined).await, 1);
        let semi = join_on_named_keys(left, right, &keys, JoinType::LeftSemi, IgnoreCase).unwrap();
        assert_eq!(names(&semi), vec!["ID".to_string(), "data".to_string()]);
    }

    #[tokio::test]
    async fn union_respells_the_right_and_refuses_a_mismatch() {
        let left = spelled(r#"SELECT 1 AS "ID", 'a' AS data"#).await;
        let right = spelled(r#"SELECT 2 AS id, 'b' AS "Data""#).await;
        let unioned = union_by_folded_name(left.clone(), right, false, IgnoreCase).unwrap();
        assert_eq!(names(&unioned), vec!["ID".to_string(), "data".to_string()]);
        assert_eq!(row_count(unioned).await, 2);
        let narrow = spelled("SELECT 2 AS id").await;
        let error = union_by_folded_name(left.clone(), narrow.clone(), false, IgnoreCase)
            .unwrap_err()
            .to_string();
        assert!(error.ends_with("mismatched columns: ['data']"), "{error}");
        let filled = union_by_folded_name(left, narrow, true, IgnoreCase).unwrap();
        assert_eq!(names(&filled), vec!["ID".to_string(), "data".to_string()]);
        assert_eq!(row_count(filled).await, 2);
    }

    #[test]
    fn exact_rule_refuses_a_case_only_match() {
        let schema = frame(&[("t", "id"), ("t", "Data"), ("t", "s")]);
        let written = Expr::Column(Column::from_qualified_name_ignore_case("ID"));
        let error = bind_names(written, &schema, Exact).unwrap_err().to_string();
        assert_eq!(error, unresolved("`ID`", "`id`, `Data`, `s`"));
        let qualified = Expr::Column(Column::from_qualified_name_ignore_case("T.id"));
        let error = bind_names(qualified, &schema, Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(error, unresolved("`T`.`id`", "`id`, `Data`, `s`"));
        let held = Expr::Column(Column::new(Some("t"), "Data"));
        assert_eq!(bind_names(held.clone(), &schema, Exact).unwrap(), held);
        let unknown = col("nope");
        assert_eq!(
            bind_names(unknown.clone(), &schema, Exact).unwrap(),
            unknown
        );
    }

    #[test]
    fn ignore_case_rule_is_unchanged() {
        let schema = frame(&[("t", "ID"), ("t", "data")]);
        let held = Expr::Column(Column::new(Some("t"), "ID"));
        assert_eq!(bind_names(held.clone(), &schema, IgnoreCase).unwrap(), held);
        assert_eq!(
            bind_names(col("id"), &schema, IgnoreCase).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID"))
        );
        let twins = frame(&[("t", "id"), ("t", "ID")]);
        assert_eq!(
            bind_names(col("id"), &twins, IgnoreCase)
                .unwrap_err()
                .to_string(),
            ambiguous("`id`", "`t`.`id`, `t`.`id`")
        );
    }

    #[tokio::test]
    async fn frame_functions_follow_the_rule() {
        let frame = spelled(r#"SELECT 1 AS "ID", 'a' AS data"#).await;
        let kept = drop_named_columns(frame.clone(), &["id".to_string()], &[], &[], Exact).unwrap();
        assert_eq!(names(&kept), vec!["ID".to_string(), "data".to_string()]);
        let dropped = drop_named_columns(frame, &["id".to_string()], &[], &[], IgnoreCase).unwrap();
        assert_eq!(names(&dropped), vec!["data".to_string()]);
        let left = spelled(r#"SELECT 1 AS id, 'a' AS "Data""#).await;
        let right = spelled(r#"SELECT 1 AS "ID", 'q' AS w"#).await;
        let error = join_on_named_keys(
            left.clone(),
            right.clone(),
            &["ID".to_string()],
            JoinType::Inner,
            Exact,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            error,
            "Error during planning: [UNRESOLVED_USING_COLUMN_FOR_JOIN] USING column `ID` cannot \
             be resolved on the left side of the join. The left-side columns: [`Data`, `id`]. \
             SQLSTATE: 42703"
        );
        let wide = spelled(r#"SELECT 1 AS "ID""#).await;
        let narrow = spelled("SELECT 2 AS id").await;
        let error = union_by_folded_name(wide, narrow, false, Exact)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: Cannot resolve column name \"ID\" among (id)."
        );
    }
}
