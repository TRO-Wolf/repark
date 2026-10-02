use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, Result};

use crate::frame_names::{
    AttrId, FrameNode, JoinSide, NameRule, Prepared, Refusal, SelfJoinRules, check_refs,
    missing_message, parse_attr_refs, prepare_join_condition, self_join_message, shared_ids,
};

const KEY: &str = "repark.attr";

const ON: SelfJoinRules = SelfJoinRules {
    fail_ambiguous: true,
    auto_resolve: true,
};

const OFF: SelfJoinRules = SelfJoinRules {
    fail_ambiguous: false,
    auto_resolve: true,
};

const NOAUTO: SelfJoinRules = SelfJoinRules {
    fail_ambiguous: true,
    auto_resolve: false,
};

struct Frame {
    node: Arc<FrameNode>,
    schema: DFSchema,
    displays: Vec<String>,
}

fn schema_of(ids: &[AttrId]) -> DFSchema {
    let fields = ids
        .iter()
        .enumerate()
        .map(|(position, id)| {
            Field::new(format!("c{position}"), DataType::Int64, true)
                .with_metadata(HashMap::from([(KEY.to_string(), id.as_str().to_string())]))
        })
        .collect::<Vec<_>>();
    DFSchema::try_from(Schema::new(fields)).unwrap()
}

fn displays(names: &[&str]) -> Vec<String> {
    names.iter().map(ToString::to_string).collect()
}

fn root(names: &[&str]) -> Frame {
    let ids = names.iter().map(|_| AttrId::mint()).collect::<Vec<_>>();
    let schema = schema_of(&ids);
    Frame {
        node: FrameNode::root(&schema).unwrap(),
        schema,
        displays: displays(names),
    }
}

fn derived(parent: &Frame, columns: &[(&str, Option<usize>)]) -> Frame {
    let ids = columns
        .iter()
        .map(|(_, kept)| kept.map_or_else(AttrId::mint, |at| parent.node.outputs()[at].clone()))
        .collect::<Vec<_>>();
    let schema = schema_of(&ids);
    Frame {
        node: FrameNode::derived(&schema, Arc::clone(&parent.node)).unwrap(),
        schema,
        displays: columns.iter().map(|(name, _)| name.to_string()).collect(),
    }
}

fn same(parent: &Frame) -> Frame {
    let columns = parent
        .displays
        .iter()
        .enumerate()
        .map(|(position, name)| (name.as_str(), Some(position)))
        .collect::<Vec<_>>();
    derived(parent, &columns)
}

fn join(left: &Frame, right: &Frame) -> Frame {
    let remint = shared_ids(&left.node, right.node.outputs())
        .into_iter()
        .map(|id| (id, AttrId::mint()))
        .collect::<HashMap<_, _>>();
    let outputs = left
        .node
        .outputs()
        .iter()
        .chain(
            &right
                .node
                .outputs()
                .iter()
                .map(|id| remint.get(id).unwrap_or(id).clone())
                .collect::<Vec<_>>(),
        )
        .cloned()
        .collect::<Vec<_>>();
    let schema = schema_of(&outputs);
    Frame {
        node: FrameNode::join(
            &schema,
            Arc::clone(&left.node),
            Arc::clone(&right.node),
            remint,
            true,
        )
        .unwrap(),
        schema,
        displays: left
            .displays
            .iter()
            .chain(&right.displays)
            .cloned()
            .collect(),
    }
}

fn tok(frame: &Frame, display: &str) -> String {
    let position = frame
        .displays
        .iter()
        .position(|held| held == display)
        .unwrap();
    format!(
        "__REPARK_ATTR_{}__F{}____",
        frame.node.outputs()[position].as_str(),
        frame.node.id().get()
    )
}

fn side<'a>(frame: &'a Frame, alias: &'a str) -> JoinSide<'a> {
    JoinSide {
        node: &frame.node,
        schema: &frame.schema,
        displays: &frame.displays,
        alias,
    }
}

fn prepare_named(
    left: &Frame,
    right: &Frame,
    condition: &str,
    names: &HashMap<AttrId, String>,
    rules: SelfJoinRules,
) -> Result<Prepared> {
    prepare_join_condition(
        condition,
        &side(left, "_l"),
        &side(right, "_r"),
        names,
        NameRule::IgnoreCase,
        rules,
    )
}

fn prepare(left: &Frame, right: &Frame, condition: &str, rules: SelfJoinRules) -> Result<Prepared> {
    prepare_named(left, right, condition, &HashMap::new(), rules)
}

fn refused(prepared: Result<Prepared>) -> Vec<String> {
    match prepared.unwrap() {
        Prepared::Refused(Refusal::SelfJoin { names }) => names,
        other => panic!("expected the 1182 refusal, got {other:?}"),
    }
}

fn answered(prepared: Result<Prepared>) -> String {
    match prepared.unwrap() {
        Prepared::Condition(condition) => condition.sql,
        other @ Prepared::Refused(_) => panic!("expected a prepared condition, got {other:?}"),
    }
}

fn failed(prepared: Result<Prepared>) -> String {
    prepared.unwrap_err().to_string()
}

struct Env {
    d: Frame,
    f: Frame,
    g: Frame,
    a: Frame,
    b: Frame,
    w: Frame,
    r: Frame,
    s: Frame,
    e: Frame,
}

fn env() -> Env {
    let d = root(&["id", "v"]);
    Env {
        f: same(&d),
        g: same(&d),
        a: same(&d),
        b: same(&d),
        w: derived(&d, &[("id", None), ("v", Some(1))]),
        r: derived(&d, &[("id", Some(0)), ("z", None)]),
        s: derived(&d, &[("id", Some(0))]),
        e: root(&["id", "w"]),
        d,
    }
}

fn names(held: &[&str]) -> Vec<String> {
    displays(held)
}

#[test]
fn b_alias_gt_id_v() {
    let Env { d, a, .. } = env();
    let condition = format!("({} > {})", tok(&d, "id"), tok(&a, "v"));
    assert_eq!(refused(prepare(&a, &d, &condition, ON)), names(&["id"]));
}

#[test]
fn b_single_left_tok() {
    let Env { d, f, .. } = env();
    let condition = format!("({} > 1)", tok(&d, "id"));
    assert_eq!(refused(prepare(&d, &f, &condition, ON)), names(&["id"]));
}

#[test]
fn b_single_right_tok() {
    let Env { d, f, .. } = env();
    let condition = format!("({} > 15)", tok(&f, "v"));
    assert_eq!(refused(prepare(&d, &f, &condition, ON)), names(&["v"]));
}

#[test]
fn b_self_eq_lit() {
    let Env { d, .. } = env();
    let condition = format!("({} = 1)", tok(&d, "id"));
    assert_eq!(answered(prepare(&d, &d, &condition, ON)), "(_l.`c0` = 1)");
}

#[test]
fn b_derived_eq_lit() {
    let Env { d, f, .. } = env();
    let condition = format!("({} = 1)", tok(&d, "id"));
    assert_eq!(refused(prepare(&d, &f, &condition, ON)), names(&["id"]));
}

#[test]
fn b_self_gt_lit() {
    let Env { d, .. } = env();
    let condition = format!("({} > 1)", tok(&d, "id"));
    assert_eq!(refused(prepare(&d, &d, &condition, ON)), names(&["id"]));
}

#[test]
fn b_unrelated_single() {
    let Env { d, e, .. } = env();
    let condition = format!("({} > 1)", tok(&d, "id"));
    let Prepared::Condition(prepared) = prepare(&d, &e, &condition, ON).unwrap() else {
        panic!("an unrelated join answers");
    };
    assert_eq!(prepared.sql, "(_l.`c0` > 1)");
    assert!(prepared.remint.is_empty());
}

#[test]
fn b_lt_rev() {
    let Env { d, f, .. } = env();
    let condition = format!("({} < {})", tok(&f, "id"), tok(&d, "id"));
    assert_eq!(
        refused(prepare(&d, &f, &condition, ON)),
        names(&["id", "id"])
    );
}

#[test]
fn d_self_eq() {
    let Env { d, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&d, "id"));
    let Prepared::Condition(prepared) = prepare(&d, &d, &condition, ON).unwrap() else {
        panic!("the trivially-true equality is rewritten");
    };
    assert_eq!(prepared.sql, "(_l.`c0` = _r.`c0`)");
    assert_eq!(prepared.remint.len(), 2);
}

#[test]
fn d_self_eq_left() {
    let Env { d, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "(_l.`c0` = _r.`c0`)"
    );
}

#[test]
fn d_self_eqns() {
    let Env { d, .. } = env();
    let condition = format!("({} IS NOT DISTINCT FROM {})", tok(&d, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "(_l.`c0` IS NOT DISTINCT FROM _r.`c0`)"
    );
}

#[test]
fn d_self_eq_cast() {
    let Env { d, .. } = env();
    let operand = format!("CAST({} AS STRING)", tok(&d, "id"));
    let condition = format!("({operand} = {operand})");
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "(CAST(_l.`c0` AS STRING) = CAST(_l.`c0` AS STRING))"
    );
}

#[test]
fn d_self_eq_and_eq() {
    let Env { d, .. } = env();
    let (id, v) = (tok(&d, "id"), tok(&d, "v"));
    let condition = format!("(({id} = {id}) AND ({v} = {v}))");
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "((_l.`c0` = _r.`c0`) AND (_l.`c1` = _r.`c1`))"
    );
}

#[test]
fn d_self_eq_and_gt() {
    let Env { d, .. } = env();
    let (id, v) = (tok(&d, "id"), tok(&d, "v"));
    let condition = format!("(({id} = {id}) AND ({v} > 15))");
    assert_eq!(refused(prepare(&d, &d, &condition, ON)), names(&["v"]));
}

#[test]
fn d_self_eq_or_eq() {
    let Env { d, .. } = env();
    let (id, v) = (tok(&d, "id"), tok(&d, "v"));
    let condition = format!("(({id} = {id}) OR ({v} = {v}))");
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "((_l.`c0` = _r.`c0`) OR (_l.`c1` = _r.`c1`))"
    );
}

#[test]
fn d_self_id_eq_v() {
    let Env { d, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&d, "v"));
    assert_eq!(
        refused(prepare(&d, &d, &condition, ON)),
        names(&["id", "v"])
    );
}

#[test]
fn d_derived_eq() {
    let Env { d, f, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&f, "id"));
    assert_eq!(
        answered(prepare(&d, &f, &condition, ON)),
        "(_l.`c0` = _r.`c0`)"
    );
}

#[test]
fn d_derived_eq_rev() {
    let Env { d, f, .. } = env();
    let condition = format!("({} = {})", tok(&f, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &f, &condition, ON)),
        "(_l.`c0` = _r.`c0`)"
    );
}

#[test]
fn d_wc_eq_v() {
    let Env { d, w, .. } = env();
    let condition = format!("({} = {})", tok(&d, "v"), tok(&w, "v"));
    assert_eq!(
        answered(prepare(&d, &w, &condition, ON)),
        "(_l.`c1` = _r.`c1`)"
    );
}

#[test]
fn d_derived_eq_plus0() {
    let Env { d, f, .. } = env();
    let condition = format!("({} = ({} + 0))", tok(&d, "id"), tok(&f, "id"));
    assert_eq!(
        refused(prepare(&d, &f, &condition, ON)),
        names(&["id", "id"])
    );
}

#[test]
fn d_self_eq_sel_d() {
    let Env { d, .. } = env();
    let joined = join(&d, &d);
    let refusal = check_refs(&joined.node, &joined.displays, &[&tok(&d, "id")], ON).unwrap();
    assert_eq!(
        refusal,
        Some(Refusal::SelfJoin {
            names: names(&["id"])
        })
    );
}

#[test]
fn d_self_eq_noauto() {
    let Env { d, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &d, &condition, NOAUTO)),
        "(_l.`c0` = _l.`c0`)"
    );
}

#[test]
fn d_derived_eq_noauto() {
    let Env { d, f, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&f, "id"));
    assert_eq!(
        answered(prepare(&d, &f, &condition, NOAUTO)),
        "(_l.`c0` = _l.`c0`)"
    );
}

const AMBIGUOUS_ID: &str = "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: \
                            [`id`, `id`]. SQLSTATE: 42704";

#[test]
fn g_eq3_count() {
    let Env { d, f, g, .. } = env();
    let first = join(&d, &f);
    let condition = format!("({} = {})", tok(&d, "id"), tok(&g, "id"));
    assert!(failed(prepare(&first, &g, &condition, ON)).ends_with(AMBIGUOUS_ID));
}

#[test]
fn g_eq3_unrel_mid() {
    let Env { d, f, e, .. } = env();
    let first = join(&d, &e);
    let condition = format!("({} = {})", tok(&d, "id"), tok(&f, "id"));
    assert!(failed(prepare(&first, &f, &condition, ON)).ends_with(AMBIGUOUS_ID));
}

#[test]
fn g_eq3_unrel_mid_sel_e() {
    let Env { d, f, e, .. } = env();
    let first = join(&d, &e);
    let condition = format!("({} = {})", tok(&d, "id"), tok(&f, "id"));
    assert!(failed(prepare(&first, &f, &condition, ON)).ends_with(AMBIGUOUS_ID));
}

#[test]
fn g_eq3_e_first() {
    let Env { d, f, e, .. } = env();
    let first = join(&e, &d);
    let condition = format!("({} = {})", tok(&d, "id"), tok(&f, "id"));
    assert!(failed(prepare(&first, &f, &condition, ON)).ends_with(AMBIGUOUS_ID));
}

#[test]
fn g_eq3_cross_tok() {
    let Env { d, f, e, .. } = env();
    let first = join(&d, &e);
    let condition = format!("({} = {})", tok(&e, "id"), tok(&f, "id"));
    assert_eq!(refused(prepare(&first, &f, &condition, ON)), names(&["id"]));
}

#[test]
fn g_eq3_right_nested() {
    let Env { d, f, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&f, "id"));
    assert_eq!(
        answered(prepare(&d, &f, &condition, ON)),
        "(_l.`c0` = _r.`c0`)"
    );
}

#[test]
fn g_eq3_str() {
    let Env { d, .. } = env();
    let condition = "(`x`.`id` = `y`.`id`)";
    assert_eq!(answered(prepare(&d, &d, condition, ON)), condition);
}

#[test]
fn i_rewrite_name_missing() {
    let Env { d, r, .. } = env();
    let condition = format!("({} = {})", tok(&d, "v"), tok(&d, "v"));
    assert!(failed(prepare(&d, &r, &condition, ON)).ends_with(
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
         name `v` cannot be resolved. Did you mean one of the following? [`id`, `z`]. \
         SQLSTATE: 42703"
    ));
}

#[test]
fn f_left_anti_gt() {
    let Env { d, f, .. } = env();
    let condition = format!("({} > {})", tok(&d, "id"), tok(&f, "id"));
    assert_eq!(
        refused(prepare(&d, &f, &condition, ON)),
        names(&["id", "id"])
    );
}

#[test]
fn e_parent_alias_gt() {
    let Env { a, b, .. } = env();
    let condition = format!("({} > {})", tok(&a, "id"), tok(&b, "id"));
    assert_eq!(refused(prepare(&a, &b, &condition, ON)), names(&["id"]));
}

#[test]
fn p_h2_equi_count() {
    let x1 = root(&["x"]);
    let condition = format!("({} = {})", tok(&x1, "x"), tok(&x1, "x"));
    assert_eq!(
        answered(prepare(&x1, &x1, &condition, ON)),
        "(_l.`c0` = _r.`c0`)"
    );
}

#[test]
fn p_h2_compound_same() {
    let xyz = root(&["x", "y"]);
    let arm = format!("({} + {})", tok(&xyz, "x"), tok(&xyz, "y"));
    let condition = format!("({arm} = {arm})");
    assert_eq!(
        refused(prepare(&xyz, &xyz, &condition, ON)),
        names(&["x", "y", "x", "y"])
    );
}

#[test]
fn p_s4_mixed_compound_arms() {
    let xy = root(&["x", "y"]);
    let (l, r) = (same(&xy), same(&xy));
    let condition = format!(
        "(({} + {}) = ({} + {}))",
        tok(&l, "x"),
        tok(&r, "y"),
        tok(&l, "y"),
        tok(&r, "x")
    );
    assert_eq!(refused(prepare(&l, &r, &condition, ON)), names(&["y", "x"]));
}

#[test]
fn p_s4_third_frame() {
    let (k1, k2, k3) = (root(&["k"]), root(&["k"]), root(&["k"]));
    let held = HashMap::from([(k3.node.outputs()[0].clone(), "k".to_string())]);
    let condition = format!("({} = 1)", tok(&k3, "k"));
    let refusal = prepare_named(&k1, &k2, &condition, &held, ON).unwrap();
    assert_eq!(
        refusal,
        Prepared::Refused(Refusal::Missing {
            names: names(&["k"]),
            input: names(&["k", "k"]),
            operation: names(&["k"]),
        })
    );
}

#[test]
fn k_off_cond_missing() {
    let Env { d, f, .. } = env();
    let k = root(&["k"]);
    let held = HashMap::from([(k.node.outputs()[0].clone(), "k".to_string())]);
    let condition = format!("({} = {})", tok(&d, "id"), tok(&k, "k"));
    let Prepared::Refused(Refusal::Missing {
        names: missing,
        input,
        operation,
    }) = prepare_named(&d, &f, &condition, &held, OFF).unwrap()
    else {
        panic!("a third frame's id is missing");
    };
    assert_eq!(missing, names(&["k"]));
    assert!(operation.is_empty());
    assert_eq!(
        missing_message(&missing, &input, &operation),
        "[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT] Resolved attribute(s) \"k\" \
         missing from \"id\", \"v\", \"id\", \"v\" in operator !Join.  SQLSTATE: XX000"
    );
}

#[test]
fn missing_name_without_a_source_is_loud() {
    let (k1, k2, k3) = (root(&["k"]), root(&["k"]), root(&["k"]));
    let condition = format!("({} = 1)", tok(&k3, "k"));
    assert!(failed(prepare(&k1, &k2, &condition, ON)).contains("no display name"));
}

#[test]
fn h_off_lt_rev() {
    let Env { d, f, .. } = env();
    let condition = format!("({} < {})", tok(&f, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &f, &condition, OFF)),
        "(_l.`c0` < _l.`c0`)"
    );
}

#[test]
fn h_off_alias_gt_id_v() {
    let Env { d, a, .. } = env();
    let condition = format!("({} > {})", tok(&d, "id"), tok(&a, "v"));
    assert_eq!(
        answered(prepare(&a, &d, &condition, OFF)),
        "(_l.`c0` > _l.`c1`)"
    );
}

#[test]
fn h_off_single_left_tok() {
    let Env { d, f, .. } = env();
    let condition = format!("({} > 1)", tok(&d, "id"));
    assert_eq!(answered(prepare(&d, &f, &condition, OFF)), "(_l.`c0` > 1)");
}

#[test]
fn h_off_self_eq() {
    let Env { d, .. } = env();
    let condition = format!("({} = {})", tok(&d, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &d, &condition, OFF)),
        "(_l.`c0` = _r.`c0`)"
    );
}

#[test]
fn h_off_self_eq_and_gt() {
    let Env { d, .. } = env();
    let (id, v) = (tok(&d, "id"), tok(&d, "v"));
    let condition = format!("(({id} = {id}) AND ({v} > 15))");
    assert_eq!(
        answered(prepare(&d, &d, &condition, OFF)),
        "((_l.`c0` = _r.`c0`) AND (_l.`c1` > 15))"
    );
}

#[test]
fn x_self_eq_castcast() {
    let Env { d, .. } = env();
    let operand = format!("CAST(CAST({} AS STRING) AS INT)", tok(&d, "id"));
    let condition = format!("({operand} = {operand})");
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "(CAST(CAST(_l.`c0` AS STRING) AS INT) = CAST(CAST(_l.`c0` AS STRING) AS INT))"
    );
}

#[test]
fn x_self_eq_halfcast() {
    let Env { d, .. } = env();
    let condition = format!("({} = CAST({} AS INT))", tok(&d, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "(_l.`c0` = CAST(_l.`c0` AS INT))"
    );
}

#[test]
fn x_self_ne() {
    let Env { d, .. } = env();
    let condition = format!("(NOT ({} = {}))", tok(&d, "id"), tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "(NOT (_l.`c0` = _r.`c0`))"
    );
}

#[test]
fn x_self_lit_rev() {
    let Env { d, .. } = env();
    let condition = format!("(1 = {})", tok(&d, "id"));
    assert_eq!(answered(prepare(&d, &d, &condition, ON)), "(1 = _l.`c0`)");
}

#[test]
fn x_self_eq_litexpr() {
    let Env { d, .. } = env();
    let condition = format!("({} = (1 + 1))", tok(&d, "id"));
    assert_eq!(
        answered(prepare(&d, &d, &condition, ON)),
        "(_l.`c0` = (1 + 1))"
    );
}

#[test]
fn x_self_eq_rand() {
    let Env { d, .. } = env();
    let condition = format!("({} = rand())", tok(&d, "id"));
    assert_eq!(refused(prepare(&d, &d, &condition, ON)), names(&["id"]));
}

#[test]
fn x_self_in() {
    let Env { d, .. } = env();
    let condition = format!("({} IN (1, 2))", tok(&d, "id"));
    assert_eq!(refused(prepare(&d, &d, &condition, ON)), names(&["id"]));
}

#[test]
fn x_derived_v_twice() {
    let Env { d, f, .. } = env();
    let v = tok(&f, "v");
    let condition = format!("(({v} > 1) AND ({v} < 100))");
    assert_eq!(refused(prepare(&d, &f, &condition, ON)), names(&["v", "v"]));
}

#[test]
fn x_on_cond_shared_v() {
    let Env { d, s, .. } = env();
    let condition = format!("({} > 15)", tok(&d, "v"));
    assert_eq!(answered(prepare(&s, &d, &condition, ON)), "(_r.`c1` > 15)");
}

#[test]
fn x_off_cond_shared_v() {
    let Env { d, s, .. } = env();
    let condition = format!("({} > 15)", tok(&d, "v"));
    assert_eq!(answered(prepare(&s, &d, &condition, OFF)), "(_r.`c1` > 15)");
}

#[test]
fn x_on_cond_shared_eq() {
    let Env { d, s, .. } = env();
    let condition = format!("({} = {})", tok(&s, "id"), tok(&d, "v"));
    assert_eq!(
        answered(prepare(&s, &d, &condition, ON)),
        "(_l.`c0` = _r.`c1`)"
    );
}

#[test]
fn a_inner_sel_f_v() {
    let Env { d, f, .. } = env();
    let joined = join(&d, &f);
    let refusal = check_refs(&joined.node, &joined.displays, &[&tok(&f, "v")], ON).unwrap();
    assert_eq!(
        refusal,
        Some(Refusal::SelfJoin {
            names: names(&["v"])
        })
    );
    assert_eq!(
        check_refs(&joined.node, &joined.displays, &[&tok(&f, "v")], OFF).unwrap(),
        None
    );
}

#[test]
fn p_v_left_join_right_parent() {
    let Env { d, f, .. } = env();
    let joined = join(&d, &f);
    let parts = [tok(&d, "id"), tok(&f, "id"), tok(&f, "id")];
    let parts = parts.iter().map(String::as_str).collect::<Vec<_>>();
    assert_eq!(
        check_refs(&joined.node, &joined.displays, &parts, ON).unwrap(),
        Some(Refusal::SelfJoin {
            names: names(&["id", "id"])
        })
    );
}

#[test]
fn i_window_f() {
    let Env { d, f, .. } = env();
    let joined = join(&d, &f);
    let part = format!("row_number() OVER (ORDER BY {} ASC)", tok(&f, "v"));
    assert_eq!(
        check_refs(&joined.node, &joined.displays, &[&part], ON).unwrap(),
        None
    );
}

#[test]
fn tokens_inside_quoted_spans_are_text() {
    let Env { d, .. } = env();
    let quoted = format!("'{}' || `it``s` || 'it''s {}'", tok(&d, "id"), tok(&d, "v"));
    let parsed = parse_attr_refs(&format!("({} = {quoted})", tok(&d, "v"))).unwrap();
    assert_eq!(parsed.refs.len(), 1);
    assert_eq!(parsed.refs[0].attr, d.node.outputs()[1]);
    assert_eq!(parsed.refs[0].frame, d.node.id());
    assert!(parsed.text.starts_with("(__rp_ref_0 = '__REPARK_ATTR_"));
}

#[test]
fn qualified_tokens_parse() {
    let Env { d, .. } = env();
    let token = tok(&d, "id").replace("____", "__l\\|r__");
    let parsed = parse_attr_refs(&format!("({token} > 1)")).unwrap();
    assert_eq!(parsed.text, "(__rp_ref_0 > 1)");
    assert_eq!(parsed.refs[0].frame, d.node.id());
}

#[test]
fn a_token_without_a_frame_field_is_loud() {
    let refused = parse_attr_refs("(__REPARK_ATTR_a1____ = 1)").unwrap_err();
    assert!(refused.to_string().contains("carries no frame field"));
    assert!(parse_attr_refs("(__REPARK_ATTR___F1____ = 1)").is_err());
    assert!(parse_attr_refs("(__REPARK_ATTR_a1__F__ = 1)").is_err());
    assert!(parse_attr_refs("(__rp_ref_0 = 1)").is_err());
}

#[test]
fn the_1182_message_is_spark_template() {
    assert_eq!(
        self_join_message(
            &names(&["id", "id"]),
            "spark.sql.analyzer.failAmbiguousSelfJoin"
        ),
        "Column id, id are ambiguous. It's probably because you joined several Datasets \
         together, and some of these Datasets are the same. This column points to one of the \
         Datasets but Spark is unable to figure out which one. Please alias the Datasets with \
         different names via `Dataset.as` before joining them, and specify the column using \
         qualified name, e.g. `df.as(\"a\").join(df.as(\"b\"), $\"a.id\" > $\"b.id\")`. You can \
         also set spark.sql.analyzer.failAmbiguousSelfJoin to false to disable this check."
    );
}
