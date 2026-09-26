use std::collections::{BTreeSet, HashSet};

use datafusion::arrow::datatypes::Field;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result, TableReference, plan_err};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlanBuilder};

type Hit<'a> = (Option<&'a TableReference>, &'a Field);

pub(super) fn bind_case_insensitive(expr: Expr, frame_schema: &DFSchema) -> Result<Expr> {
    expr.transform(|node| {
        Ok(match node {
            Expr::Column(column) => match unique_case_match(&column, frame_schema) {
                Some(bound) => Transformed::yes(Expr::Column(bound)),
                None => Transformed::no(Expr::Column(column)),
            },
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

fn case_hits<'a>(column: &Column, schemas: impl IntoIterator<Item = &'a DFSchema>) -> Vec<Hit<'a>> {
    schemas
        .into_iter()
        .flat_map(DFSchema::iter)
        .filter(|(qualifier, field)| {
            field.name().eq_ignore_ascii_case(&column.name)
                && column.relation.as_ref().is_none_or(|written| {
                    qualifier.is_some_and(|held| same_relation(written, held))
                })
        })
        .map(|(qualifier, field)| (qualifier, field.as_ref()))
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn bind_projection_expr(expr: Expr, frame_schema: &DFSchema) -> Result<Expr> {
    let written = match &expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    };
    let bound = super::subquery::resolve_bound_expr(expr, frame_schema)?;
    Ok(match (written, &bound) {
        (Some(written), Expr::Column(held)) if held.name != written => {
            let relation = held.relation.clone();
            bound.alias_qualified(relation, written)
        }
        _ => bound,
    })
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
    let hits = case_hits(column, [frame_schema]);
    let (first_qualifier, first_field) = hits.first()?;
    let first = Column::new(first_qualifier.cloned(), first_field.name());
    hits.iter()
        .all(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()) == first)
        .then_some(first)
}

fn same_relation(written: &TableReference, held: &TableReference) -> bool {
    let written_parts = [written.catalog(), written.schema(), Some(written.table())];
    let held_parts = [held.catalog(), held.schema(), Some(held.table())];
    written_parts
        .iter()
        .zip(held_parts.iter())
        .all(
            |(written_part, held_part)| match (written_part, held_part) {
                (Some(written_part), Some(held_part)) => {
                    written_part.eq_ignore_ascii_case(held_part)
                }
                (Some(_), None) => false,
                (None, _) => true,
            },
        )
}

fn bind_name(schema: &DFSchema, name: &str) -> Column {
    let bare = Column::new_unqualified(name);
    if schema.has_column_with_unqualified_name(name) {
        return bare;
    }
    unique_case_match(&bare, schema).unwrap_or(bare)
}

#[allow(clippy::missing_errors_doc)]
pub fn drop_named_columns(
    frame: DataFrame,
    names: &[String],
    references: &[String],
) -> Result<DataFrame> {
    let written = names
        .iter()
        .map(|name| Column::new_unqualified(name.as_str()))
        .chain(
            references
                .iter()
                .map(Column::from_qualified_name_ignore_case),
        )
        .collect::<Vec<_>>();
    let targets = written
        .iter()
        .flat_map(|column| case_hits(column, [frame.schema()]))
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
) -> Result<DataFrame> {
    let left_keys: Vec<Column> = keys
        .iter()
        .map(|key| bind_name(left.schema(), key))
        .collect();
    let right_keys: Vec<Column> = keys
        .iter()
        .map(|key| bind_name(right.schema(), key))
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
            let folded = field.name().to_ascii_lowercase();
            !keys.iter().any(|key| key.eq_ignore_ascii_case(&folded)) || seen.insert(folded)
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
) -> Result<DataFrame> {
    let respelled: Vec<(Expr, bool)> = right
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            let held = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            let bound = bind_name(left.schema(), field.name());
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
    use datafusion::common::{Column, DFSchema, JoinType};
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::{Expr, col, lit};
    use datafusion::prelude::SessionContext;

    use super::{
        bind_case_insensitive, bind_projection_expr, drop_named_columns, join_on_named_keys,
        union_by_folded_name,
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
        let bound = bind_case_insensitive(col("ID").gt(lit(1i64)), &schema).unwrap();
        assert_eq!(
            bound,
            Expr::Column(Column::new(Some("t"), "ID")).gt(lit(1i64))
        );
    }

    #[test]
    fn exact_and_ambiguous_references_stay() {
        let schema = frame(&[("t", "id"), ("u", "ID")]);
        assert_eq!(
            bind_case_insensitive(col("id"), &schema).unwrap(),
            col("id")
        );
        let twins = frame(&[("t", "Id"), ("u", "ID")]);
        assert_eq!(bind_case_insensitive(col("id"), &twins).unwrap(), col("id"));
        let qualified = Expr::Column(Column::new(Some("t"), "id"));
        let exact = frame(&[("t", "id"), ("t", "ID")]);
        assert_eq!(
            bind_case_insensitive(qualified.clone(), &exact).unwrap(),
            qualified
        );
    }

    #[test]
    fn qualified_reference_binds_through_its_relation() {
        let schema = frame(&[("t", "ID"), ("t", "data")]);
        let written = Expr::Column(Column::new(Some("t"), "id"));
        assert_eq!(
            bind_case_insensitive(written, &schema).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID"))
        );
        let other = Expr::Column(Column::new(Some("x"), "id"));
        assert_eq!(
            bind_case_insensitive(other.clone(), &schema).unwrap(),
            other
        );
    }

    #[test]
    fn qualified_alias_names_the_written_segment() {
        let schema = frame(&[("t", "ID")]);
        let aliased = Expr::Column(Column::new(Some("t"), "id")).alias("t.ID");
        assert_eq!(
            bind_case_insensitive(aliased, &schema).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID")).alias("ID")
        );
        let chosen = Expr::Column(Column::new(Some("t"), "id")).alias("other");
        assert_eq!(
            bind_case_insensitive(chosen, &schema).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID")).alias("other")
        );
    }

    #[test]
    fn projection_keeps_the_written_spelling() {
        let schema = frame(&[("t", "ID")]);
        assert_eq!(
            bind_projection_expr(col("id"), &schema).unwrap(),
            Expr::Column(Column::new(Some("t"), "ID")).alias_qualified(Some("t"), "id")
        );
        let held = Expr::Column(Column::new(Some("t"), "ID"));
        assert_eq!(bind_projection_expr(held.clone(), &schema).unwrap(), held);
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
        let dropped = drop_named_columns(frame.clone(), &["id".to_string()], &[]).unwrap();
        assert_eq!(names(&dropped), vec!["data".to_string()]);
        let both = ["ID".to_string(), "DATA".to_string()];
        assert!(names(&drop_named_columns(frame.clone(), &both, &[]).unwrap()).is_empty());
        let absent = drop_named_columns(frame, &["nope".to_string()], &[]).unwrap();
        assert_eq!(names(&absent), vec!["ID".to_string(), "data".to_string()]);
    }

    #[tokio::test]
    async fn qualified_drop_binds_through_its_relation() {
        let frame = spelled(r#"SELECT "ID", data FROM (SELECT 1 AS "ID", 'a' AS data) t"#).await;
        let whole = vec!["ID".to_string(), "data".to_string()];
        for written in ["t.ID", "t.id", "T.Id"] {
            let dropped = drop_named_columns(frame.clone(), &[], &[written.to_string()]).unwrap();
            assert_eq!(names(&dropped), vec!["data".to_string()]);
        }
        for written in ["t.ID", "u.id"] {
            let named = drop_named_columns(frame.clone(), &[written.to_string()], &[]).unwrap();
            assert_eq!(names(&named), whole);
        }
        let unmatched = drop_named_columns(frame, &[], &["u.id".to_string()]).unwrap();
        assert_eq!(names(&unmatched), whole);
        let joined = spelled(
            r#"SELECT a.id, b."ID" FROM (SELECT 1 AS id, 1 AS "ID") a JOIN (SELECT 1 AS id, 1 AS "ID") b ON a.id = b.id"#,
        )
        .await;
        let right = drop_named_columns(joined.clone(), &[], &["b.id".to_string()]).unwrap();
        assert_eq!(names(&right), vec!["id".to_string()]);
        let left = drop_named_columns(joined, &[], &["A.ID".to_string()]).unwrap();
        assert_eq!(names(&left), vec!["ID".to_string()]);
    }

    #[tokio::test]
    async fn join_binds_each_side_and_keeps_one_key() {
        let left = spelled(r#"SELECT 1 AS "ID", 'a' AS data"#).await;
        let right = spelled("SELECT 1 AS id, 'q' AS w").await;
        let keys = ["Id".to_string()];
        let joined =
            join_on_named_keys(left.clone(), right.clone(), &keys, JoinType::Inner).unwrap();
        assert_eq!(
            names(&joined),
            vec!["ID".to_string(), "data".to_string(), "w".to_string()]
        );
        assert_eq!(row_count(joined).await, 1);
        let semi = join_on_named_keys(left, right, &keys, JoinType::LeftSemi).unwrap();
        assert_eq!(names(&semi), vec!["ID".to_string(), "data".to_string()]);
    }

    #[tokio::test]
    async fn union_respells_the_right_and_refuses_a_mismatch() {
        let left = spelled(r#"SELECT 1 AS "ID", 'a' AS data"#).await;
        let right = spelled(r#"SELECT 2 AS id, 'b' AS "Data""#).await;
        let unioned = union_by_folded_name(left.clone(), right, false).unwrap();
        assert_eq!(names(&unioned), vec!["ID".to_string(), "data".to_string()]);
        assert_eq!(row_count(unioned).await, 2);
        let narrow = spelled("SELECT 2 AS id").await;
        let error = union_by_folded_name(left.clone(), narrow.clone(), false)
            .unwrap_err()
            .to_string();
        assert!(error.ends_with("mismatched columns: ['data']"), "{error}");
        let filled = union_by_folded_name(left, narrow, true).unwrap();
        assert_eq!(names(&filled), vec!["ID".to_string(), "data".to_string()]);
        assert_eq!(row_count(filled).await, 2);
    }
}
