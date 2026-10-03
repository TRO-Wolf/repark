use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, Result};

use crate::frame_names::{
    AttrId, FrameNode, JoinSide, NameRule, Prepared, SelfJoinRules, prepare_join_condition,
    sort_hits_meet_at_join,
};

const KEY: &str = "repark.attr";

const ON: SelfJoinRules = SelfJoinRules {
    fail_ambiguous: true,
    auto_resolve: true,
};

struct Frame {
    node: Arc<FrameNode>,
    schema: DFSchema,
    displays: Vec<String>,
    qualifiers: BTreeMap<String, Vec<String>>,
}

fn frame(names: &[&str], alias: &str) -> Frame {
    let ids = names.iter().map(|_| AttrId::mint()).collect::<Vec<_>>();
    let fields = ids
        .iter()
        .enumerate()
        .map(|(position, id)| {
            Field::new(format!("c{position}"), DataType::Int64, true)
                .with_metadata(HashMap::from([(KEY.to_string(), id.as_str().to_string())]))
        })
        .collect::<Vec<_>>();
    let schema = DFSchema::try_from(Schema::new(fields)).unwrap();
    Frame {
        node: FrameNode::root(&schema).unwrap(),
        schema,
        displays: names.iter().map(ToString::to_string).collect(),
        qualifiers: ids
            .iter()
            .map(|id| (id.as_str().to_string(), vec![alias.to_string()]))
            .collect(),
    }
}

fn side<'a>(frame: &'a Frame, alias: &'a str) -> JoinSide<'a> {
    JoinSide {
        node: &frame.node,
        schema: &frame.schema,
        displays: &frame.displays,
        alias,
        qualifiers: Some(&frame.qualifiers),
    }
}

fn prepare(left: &Frame, right: &Frame, condition: &str, rule: NameRule) -> Result<String> {
    match prepare_join_condition(
        condition,
        &side(left, "_l"),
        &side(right, "_r"),
        &HashMap::new(),
        rule,
        ON,
    )? {
        Prepared::Condition(prepared) => Ok(prepared.sql),
        Prepared::Refused(refusal) => panic!("expected a prepared condition, got {refusal:?}"),
    }
}

#[test]
fn alias_qualified_free_names_bind_to_their_side_under_the_rule() {
    let left = frame(&["id", "v"], "l");
    let right = frame(&["id", "w"], "r");
    for (written, rule) in [
        ("l.id = r.id", NameRule::IgnoreCase),
        ("L.ID = R.Id", NameRule::IgnoreCase),
        ("l.id = r.id", NameRule::Exact),
    ] {
        assert_eq!(
            prepare(&left, &right, written, rule).unwrap(),
            "_l.`c0` = _r.`c0`",
            "{written}"
        );
    }
    assert_eq!(
        prepare(
            &left,
            &right,
            "(l.id + l.v) = (r.id + r.w)",
            NameRule::IgnoreCase
        )
        .unwrap(),
        "(_l.`c0` + _l.`c1`) = (_r.`c0` + _r.`c1`)"
    );
}

#[test]
fn unmatched_qualifiers_keep_their_text() {
    let left = frame(&["id", "v"], "l");
    let right = frame(&["id", "w"], "r");
    for written in ["(L.id = r.id)", "(x.id = 1)", "(l.nope = 1)", "(id = 1)"] {
        let prepared = prepare(&left, &right, written, NameRule::Exact).unwrap();
        assert!(!prepared.contains("_l."), "{written} -> {prepared}");
    }
    assert_eq!(
        prepare(&left, &right, "(L.id = r.id)", NameRule::Exact).unwrap(),
        "(L.id = _r.`c0`)"
    );
}

#[test]
fn a_qualifier_held_by_both_sides_stays_unbound() {
    let left = frame(&["id"], "t");
    let right = frame(&["id"], "t");
    assert_eq!(
        prepare(&left, &right, "t.id = 1", NameRule::IgnoreCase).unwrap(),
        "t.id = 1"
    );
}

#[test]
fn a_lambda_parameter_shadows_the_qualifier_inside_its_body_only() {
    let left = frame(&["id", "v"], "l");
    let right = frame(&["id", "w"], "r");
    assert_eq!(
        prepare(
            &left,
            &right,
            "exists(array(l.v), l -> l > r.w)",
            NameRule::IgnoreCase
        )
        .unwrap(),
        "exists(array(_l.`c1`), l -> l > _r.`c1`)"
    );
    assert_eq!(
        prepare(
            &left,
            &right,
            "exists(array(l.v), L -> L.x > 1)",
            NameRule::IgnoreCase
        )
        .unwrap(),
        "exists(array(_l.`c1`), L -> L.x > 1)"
    );
    assert_eq!(
        prepare(
            &left,
            &right,
            "exists(array(l.v), L -> l.id > 1)",
            NameRule::Exact
        )
        .unwrap(),
        "exists(array(_l.`c1`), L -> _l.`c0` > 1)"
    );
}

#[test]
fn two_attributes_under_one_qualifier_refuse_ambiguous() {
    let left = frame(&["id", "ID"], "l");
    let right = frame(&["id"], "r");
    let error = prepare(&left, &right, "l.id = r.id", NameRule::IgnoreCase).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Error during planning: [AMBIGUOUS_REFERENCE] Reference `l`.`id` is ambiguous, could \
         be: [`l`.`id`, `l`.`ID`]. SQLSTATE: 42704"
    );
    assert_eq!(
        prepare(&left, &right, "l.ID = r.id", NameRule::Exact).unwrap(),
        "_l.`c1` = _r.`c0`"
    );
}

#[test]
fn a_struct_field_after_the_bound_column_is_kept() {
    let left = frame(&["s"], "l");
    let right = frame(&["id"], "r");
    assert_eq!(
        prepare(&left, &right, "l.s.a = r.id", NameRule::IgnoreCase).unwrap(),
        "_l.`c0`.a = _r.`c0`"
    );
}

async fn planned(sql: &str) -> datafusion::logical_expr::LogicalPlan {
    datafusion::prelude::SessionContext::new()
        .sql(sql)
        .await
        .unwrap()
        .logical_plan()
        .clone()
}

#[tokio::test]
async fn sort_twins_from_two_join_positions_meet_at_the_join() {
    let joined = "SELECT l.id, l.v AS a, r.v AS b FROM (SELECT 1 AS id, 10 AS v) l \
                  JOIN (SELECT 1 AS id, 20 AS v) r ON l.id = r.id";
    let plan = planned(joined).await;
    assert!(sort_hits_meet_at_join(&plan, &[1, 2]));
    assert!(!sort_hits_meet_at_join(&plan, &[1]));
    let wrapped = planned(&format!("SELECT DISTINCT * FROM ({joined}) LIMIT 5")).await;
    assert!(sort_hits_meet_at_join(&wrapped, &[1, 2]));
    let computed = planned(
        "SELECT l.v, r.v + 1 AS w FROM (SELECT 1 AS id, 10 AS v) l \
         JOIN (SELECT 1 AS id, 20 AS v) r ON l.id = r.id",
    )
    .await;
    assert!(!sort_hits_meet_at_join(&computed, &[0, 1]));
    let single = planned("SELECT v, v + 1 AS w FROM (SELECT 1 AS v) t").await;
    assert!(!sort_hits_meet_at_join(&single, &[0, 1]));
}
