use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::common::JoinType;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;

use crate::frame_names::NameRule::IgnoreCase;
use crate::frame_names::{
    AttrId, AttrRef, FrameNode, all_ids, ambiguous, attribute_ids, join_collisions,
    join_on_named_keys, remint_shared, remint_with_map, renewed_absent, shared_ids,
};

const KEY: &str = "repark.attr";

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

fn root(width: usize) -> Arc<FrameNode> {
    let ids = (0..width).map(|_| AttrId::mint()).collect::<Vec<_>>();
    FrameNode::root(&schema_of(&ids)).unwrap()
}

fn derived(parent: &Arc<FrameNode>, outputs: &[AttrId]) -> Arc<FrameNode> {
    FrameNode::derived(&schema_of(outputs), Arc::clone(parent)).unwrap()
}

fn same(parent: &Arc<FrameNode>) -> Arc<FrameNode> {
    derived(parent, parent.outputs())
}

fn set_op(first: &Arc<FrameNode>, others: &[&Arc<FrameNode>]) -> Arc<FrameNode> {
    let others = others.iter().map(|other| Arc::clone(other)).collect();
    FrameNode::set_op(&schema_of(first.outputs()), Arc::clone(first), others).unwrap()
}

fn join(left: &Arc<FrameNode>, right: &Arc<FrameNode>, emits_right: bool) -> Arc<FrameNode> {
    let shared = shared_ids(left, right.outputs());
    let remint = shared
        .into_iter()
        .map(|id| (id, AttrId::mint()))
        .collect::<HashMap<_, _>>();
    let mut outputs = left.outputs().to_vec();
    if emits_right {
        outputs.extend(
            right
                .outputs()
                .iter()
                .map(|id| remint.get(id).unwrap_or(id).clone()),
        );
    }
    FrameNode::join(
        &schema_of(&outputs),
        Arc::clone(left),
        Arc::clone(right),
        remint,
        emits_right,
    )
    .unwrap()
}

fn reference(frame: &Arc<FrameNode>, position: usize) -> AttrRef {
    AttrRef {
        attr: frame.outputs()[position].clone(),
        frame: frame.id(),
    }
}

fn verdict(target: &Arc<FrameNode>, refs: &[AttrRef]) -> Vec<usize> {
    let visible = target.outputs().iter().cloned().collect::<HashSet<_>>();
    ambiguous(target, &visible, refs)
}

struct Env {
    d: Arc<FrameNode>,
    f: Arc<FrameNode>,
    f2: Arc<FrameNode>,
    w: Arc<FrameNode>,
    s: Arc<FrameNode>,
    e: Arc<FrameNode>,
}

const ID: usize = 0;
const V: usize = 1;

fn env() -> Env {
    let base = root(2);
    let shifted = derived(&base, &[AttrId::mint(), base.outputs()[V].clone()]);
    let only_id = derived(&base, &base.outputs()[..1]);
    Env {
        f: same(&base),
        f2: same(&base),
        w: shifted,
        s: only_id,
        e: root(2),
        d: base,
    }
}

#[test]
fn a_inner_sel_f_v_refuses() {
    let Env { d, f, .. } = env();
    let joined = join(&d, &f, true);
    assert!(joined.renews());
    assert_eq!(verdict(&joined, &[reference(&f, V)]), vec![0]);
}

#[test]
fn a_rev_left_sel_f_answers() {
    let Env { d, f, .. } = env();
    let joined = join(&f, &d, true);
    assert!(joined.renews());
    assert!(verdict(&joined, &[reference(&f, V)]).is_empty());
}

#[test]
fn a_sibling_absent_answers() {
    let Env { d, f, f2, .. } = env();
    let joined = join(&d, &f2, true);
    assert!(verdict(&joined, &[reference(&f, V)]).is_empty());
}

#[test]
fn a_sel_only_id_left_sel_d_v_refuses() {
    let Env { d, s, .. } = env();
    assert_eq!(
        shared_ids(&s, d.outputs()),
        d.outputs().iter().cloned().collect::<HashSet<_>>()
    );
    let joined = join(&s, &d, true);
    assert_eq!(verdict(&joined, &[reference(&d, V)]), vec![0]);
}

#[test]
fn a_wc_sel_w_id_answers() {
    let Env { d, w, .. } = env();
    let joined = join(&d, &w, true);
    assert!(verdict(&joined, &[reference(&w, ID)]).is_empty());
}

#[test]
fn a_wc_sel_w_v_refuses() {
    let Env { d, w, .. } = env();
    let joined = join(&d, &w, true);
    assert_eq!(
        verdict(&joined, &[reference(&w, ID), reference(&w, V)]),
        vec![1]
    );
}

#[test]
fn f_left_semi_sel_f_answers() {
    let Env { d, f, .. } = env();
    let semi = join(&d, &f, false);
    assert_eq!(semi.outputs(), d.outputs());
    assert!(verdict(&semi, &[reference(&f, V)]).is_empty());
}

#[test]
fn k_on_union_first_input_refuses() {
    let Env { d, .. } = env();
    let u = set_op(&d, &[&same(&d)]);
    let joined = join(&d, &u, true);
    assert_eq!(verdict(&joined, &[reference(&u, V)]), vec![0]);
}

#[test]
fn k_on_union_second_input_answers() {
    let Env { d, f, .. } = env();
    let u = set_op(&d, &[&same(&d)]);
    let joined = join(&d, &u, true);
    assert!(verdict(&joined, &[reference(&f, V)]).is_empty());
}

#[test]
fn k_on_union_named_second_input_answers() {
    let Env { d, f, .. } = env();
    let un = set_op(&d, &[&f]);
    let joined = join(&d, &un, true);
    assert!(verdict(&joined, &[reference(&f, V)]).is_empty());
    assert_eq!(verdict(&joined, &[reference(&un, V)]), vec![0]);
    assert_eq!(verdict(&joined, &[reference(&d, V)]), vec![0]);
}

#[test]
fn k_on_union_named_first_input_refuses() {
    let Env { d, f, .. } = env();
    let uf = set_op(&f, &[&d]);
    let joined = join(&d, &uf, true);
    assert_eq!(verdict(&joined, &[reference(&f, V)]), vec![0]);
}

#[test]
fn g_eq3_cross_tok_refuses() {
    let Env { d, e, f, .. } = env();
    let first = join(&d, &e, true);
    assert!(!first.renews());
    let second = join(&first, &f, true);
    assert!(second.renews());
    let refs = [reference(&e, ID), reference(&f, ID)];
    assert_eq!(verdict(&second, &refs), vec![1]);
}

#[test]
fn k_off_shared_sel_ref_is_renewed_absent() {
    let Env { d, s, .. } = env();
    let joined = join(&s, &d, true);
    let outputs = joined.outputs().iter().cloned().collect::<HashSet<_>>();
    let refs = [reference(&s, ID), reference(&d, V)];
    assert_eq!(renewed_absent(&joined, &refs, &outputs), vec![1]);
    let plain = join(&d, &root(2), true);
    let outputs = plain.outputs().iter().cloned().collect::<HashSet<_>>();
    assert!(renewed_absent(&plain, &[reference(&d, V)], &outputs).is_empty());
}

#[test]
fn lineage_reaches_set_op_inputs_and_join_rights_and_ids_grow() {
    let Env { d, e, f, .. } = env();
    let u = set_op(&e, &[&f]);
    let joined = join(&e, &u, false);
    let reached = all_ids(&joined);
    assert!(d.outputs().iter().all(|id| reached.contains(id)));
    assert!(d.id() < f.id() && f.id() < u.id() && u.id() < joined.id());
    assert!(!u.renews());
    assert!(!same(&d).renews());
    assert!(same(&join(&d, &f, true)).renews());
}

#[test]
fn a_node_over_an_unstamped_field_is_an_internal_error() {
    let unstamped = DFSchema::try_from(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let error = FrameNode::root(&unstamped.unwrap()).unwrap_err();
    assert!(error.to_string().contains("carries no attribute id"));
}

#[test]
fn remint_shared_renews_the_shared_set_not_only_output_collisions() {
    let context = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = (0..3)
        .map(|_| Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)
        .collect();
    let held = (0..3).map(|_| AttrId::mint()).collect::<Vec<_>>();
    let tagged = ["a", "b", "c"]
        .iter()
        .zip(&held)
        .map(|(name, id)| {
            col(*name).alias_with_metadata(
                *name,
                Some(HashMap::from([(KEY.to_string(), id.as_str().to_string())]).into()),
            )
        })
        .collect::<Vec<_>>();
    let frame = context
        .read_batch(RecordBatch::try_new(schema, columns).unwrap())
        .unwrap()
        .select(tagged)
        .unwrap();
    let plan = frame.logical_plan().clone();
    assert!(join_collisions(&plan, 1).unwrap().is_empty());
    let shared = HashSet::from([held[2].clone()]);
    let (reminted, remint) = remint_shared(plan, 1, &shared).unwrap();
    let after = attribute_ids(reminted.schema());
    assert_eq!(remint.len(), 1);
    assert_eq!(after[..2], [Some(held[0].clone()), Some(held[1].clone())]);
    assert_eq!(after[2].as_ref(), remint.get(&held[2]));
    assert_ne!(after[2], Some(held[2].clone()));
}

fn tagged_frame(context: &SessionContext, names: &[&str], held: &[AttrId]) -> DataFrame {
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = names
        .iter()
        .map(|_| Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)
        .collect();
    let tagged = names
        .iter()
        .zip(held)
        .map(|(name, id)| {
            col(*name).alias_with_metadata(
                *name,
                Some(HashMap::from([(KEY.to_string(), id.as_str().to_string())]).into()),
            )
        })
        .collect::<Vec<_>>();
    context
        .read_batch(RecordBatch::try_new(schema, columns).unwrap())
        .unwrap()
        .select(tagged)
        .unwrap()
}

#[test]
fn remint_with_map_applies_exactly_the_given_map() {
    let context = SessionContext::new();
    let held = (0..3).map(|_| AttrId::mint()).collect::<Vec<_>>();
    let frame = tagged_frame(&context, &["a", "b", "c"], &held);
    let fixed = AttrId::mint();
    let map = HashMap::from([
        (held[2].clone(), fixed.clone()),
        (AttrId::mint(), AttrId::mint()),
    ]);
    let reminted = remint_with_map(frame.logical_plan().clone(), 1, &map).unwrap();
    let after = attribute_ids(reminted.schema());
    assert_eq!(after[..2], [Some(held[0].clone()), Some(held[1].clone())]);
    assert_eq!(after[2], Some(fixed));
}

#[test]
fn named_key_join_renews_a_lineage_shared_id_below_the_left_output() {
    let context = SessionContext::new();
    let held = (0..2).map(|_| AttrId::mint()).collect::<Vec<_>>();
    let parent = tagged_frame(&context, &["id", "v"], &held);
    let parent_node = FrameNode::root(parent.schema()).unwrap();
    let narrow = parent
        .clone()
        .select(vec![col("id")])
        .unwrap()
        .alias("n")
        .unwrap();
    let narrow_node = FrameNode::derived(narrow.schema(), Arc::clone(&parent_node)).unwrap();
    let parent = parent.alias("p").unwrap();
    let (joined, node) = join_on_named_keys(
        narrow,
        parent,
        &["id".to_string()],
        JoinType::Inner,
        IgnoreCase,
        narrow_node,
        Arc::clone(&parent_node),
    )
    .unwrap();
    let after = attribute_ids(joined.schema());
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], Some(held[0].clone()));
    assert_ne!(after[1], Some(held[1].clone()));
    assert!(node.renews());
}
