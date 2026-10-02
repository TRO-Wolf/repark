use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Column, DFSchema, JoinType, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::SessionContext;

use crate::frame_names::NameRule::{Exact, IgnoreCase};
use crate::frame_names::{
    FrameNode, attribute_reference, bind_projection_expr, drop_named_columns, join_on_named_keys,
    refuse_ambiguous_condition, requalify_join_sides, stamp, union_by_folded_name,
};
use crate::session::df_guards::case_bind::bind_names;

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
    let qualified = Expr::Column(Column::new(Some("t"), "id"));
    let exact = frame(&[("t", "id"), ("t", "ID")]);
    assert_eq!(
        refusal(qualified, &exact),
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

fn stamped(frame: DataFrame) -> DataFrame {
    let (state, plan) = frame.into_parts();
    DataFrame::new(state, stamp(plan).unwrap())
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
        names(&drop_named_columns(frame.clone(), &both, &[], &[], IgnoreCase).unwrap()).is_empty()
    );
    let absent = drop_named_columns(frame, &["nope".to_string()], &[], &[], IgnoreCase).unwrap();
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
        let named = drop_named_columns(frame.clone(), &[written.to_string()], &[], &[], IgnoreCase)
            .unwrap();
        assert_eq!(names(&named), whole);
    }
    let unmatched = drop_named_columns(frame, &[], &["u.id".to_string()], &[], IgnoreCase).unwrap();
    assert_eq!(names(&unmatched), whole);
    let joined = spelled(
        r#"SELECT a.id, b."ID" FROM (SELECT 1 AS id, 1 AS "ID") a JOIN (SELECT 1 AS id, 1 AS "ID") b ON a.id = b.id"#,
    )
    .await;
    let right =
        drop_named_columns(joined.clone(), &[], &["b.id".to_string()], &[], IgnoreCase).unwrap();
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
    let left = stamped(spelled(r#"SELECT 1 AS "ID", 'a' AS data"#).await);
    let right = stamped(spelled("SELECT 1 AS id, 'q' AS w").await);
    let keys = ["Id".to_string()];
    let left_node = FrameNode::root(left.schema()).unwrap();
    let right_node = FrameNode::root(right.schema()).unwrap();
    let (joined, _) = join_on_named_keys(
        left.clone(),
        right.clone(),
        &keys,
        JoinType::Inner,
        IgnoreCase,
        Arc::clone(&left_node),
        Arc::clone(&right_node),
    )
    .unwrap();
    assert_eq!(
        names(&joined),
        vec!["ID".to_string(), "data".to_string(), "w".to_string()]
    );
    assert_eq!(row_count(joined).await, 1);
    let (semi, _) = join_on_named_keys(
        left,
        right,
        &keys,
        JoinType::LeftSemi,
        IgnoreCase,
        left_node,
        right_node,
    )
    .unwrap();
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
    let left_node = FrameNode::root(&DFSchema::empty()).unwrap();
    let right_node = FrameNode::root(&DFSchema::empty()).unwrap();
    let error = join_on_named_keys(
        left.clone(),
        right.clone(),
        &["ID".to_string()],
        JoinType::Inner,
        Exact,
        left_node,
        right_node,
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
