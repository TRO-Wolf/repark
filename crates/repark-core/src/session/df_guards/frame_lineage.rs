use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::BuildHasher;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::common::{DFSchema, Result, internal_datafusion_err};

use super::attr_id::{AttrId, attribute_ids};

static NEXT_FRAME: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FrameId(u64);

impl FrameId {
    #[must_use]
    pub fn mint() -> Self {
        Self(NEXT_FRAME.fetch_add(1, Ordering::Relaxed))
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttrRef {
    pub attr: AttrId,
    pub frame: FrameId,
}

#[derive(Debug)]
pub enum FrameKind {
    Root,
    Derived(Arc<FrameNode>),
    SetOp {
        first: Arc<FrameNode>,
        others: Vec<Arc<FrameNode>>,
    },
    Join {
        left: Arc<FrameNode>,
        right: Arc<FrameNode>,
        remint: HashMap<AttrId, AttrId>,
        emits_right: bool,
    },
}

#[derive(Debug)]
pub struct FrameNode {
    id: FrameId,
    outputs: Vec<AttrId>,
    kind: FrameKind,
    renews: bool,
}

impl FrameNode {
    #[allow(clippy::missing_errors_doc)]
    pub fn root(schema: &DFSchema) -> Result<Arc<Self>> {
        Self::build(schema, FrameKind::Root)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn derived(schema: &DFSchema, parent: Arc<Self>) -> Result<Arc<Self>> {
        Self::build(schema, FrameKind::Derived(parent))
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn set_op(
        schema: &DFSchema,
        first: Arc<Self>,
        others: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>> {
        Self::build(schema, FrameKind::SetOp { first, others })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn join(
        schema: &DFSchema,
        left: Arc<Self>,
        right: Arc<Self>,
        remint: HashMap<AttrId, AttrId>,
        emits_right: bool,
    ) -> Result<Arc<Self>> {
        Self::build(
            schema,
            FrameKind::Join {
                left,
                right,
                remint,
                emits_right,
            },
        )
    }

    fn build(schema: &DFSchema, kind: FrameKind) -> Result<Arc<Self>> {
        let outputs = attribute_ids(schema)
            .into_iter()
            .enumerate()
            .map(|(position, id)| {
                id.ok_or_else(|| {
                    internal_datafusion_err!(
                        "frame lineage: output {position} carries no attribute id"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let renews = match &kind {
            FrameKind::Root => false,
            FrameKind::Derived(parent) => parent.renews,
            FrameKind::SetOp { first, others } => {
                first.renews || others.iter().any(|other| other.renews)
            }
            FrameKind::Join {
                left,
                right,
                remint,
                ..
            } => !remint.is_empty() || left.renews || right.renews,
        };
        Ok(Arc::new(Self {
            id: FrameId::mint(),
            outputs,
            kind,
            renews,
        }))
    }

    #[must_use]
    pub fn id(&self) -> FrameId {
        self.id
    }

    #[must_use]
    pub fn outputs(&self) -> &[AttrId] {
        &self.outputs
    }

    #[must_use]
    pub fn kind(&self) -> &FrameKind {
        &self.kind
    }

    #[must_use]
    pub fn renews(&self) -> bool {
        self.renews
    }

    fn inputs(&self) -> Vec<&FrameNode> {
        match &self.kind {
            FrameKind::Root => Vec::new(),
            FrameKind::Derived(parent) => vec![parent.as_ref()],
            FrameKind::SetOp { first, others } => std::iter::once(first)
                .chain(others)
                .map(AsRef::as_ref)
                .collect(),
            FrameKind::Join { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        }
    }
}

fn reachable(target: &FrameNode) -> Vec<&FrameNode> {
    let mut seen = HashSet::new();
    let mut found = Vec::new();
    let mut stack = vec![target];
    while let Some(node) = stack.pop() {
        if seen.insert(ptr::from_ref(node)) {
            found.push(node);
            stack.extend(node.inputs());
        }
    }
    found
}

#[must_use]
pub fn all_ids(node: &FrameNode) -> HashSet<AttrId> {
    reachable(node)
        .into_iter()
        .flat_map(|node| node.outputs.iter().cloned())
        .collect()
}

#[must_use]
pub fn shared_ids(left: &FrameNode, right_outputs: &[AttrId]) -> HashSet<AttrId> {
    let held = all_ids(left);
    right_outputs
        .iter()
        .filter(|id| held.contains(*id))
        .cloned()
        .collect()
}

type Link<'a> = (Option<usize>, &'a HashMap<AttrId, AttrId>);

fn image<'a>(attr: &'a AttrId, mut chain: Option<usize>, links: &[Link<'a>]) -> &'a AttrId {
    let mut image = attr;
    while let Some(&(outer, remint)) = chain.and_then(|index| links.get(index)) {
        image = remint.get(image).unwrap_or(image);
        chain = outer;
    }
    image
}

#[must_use]
pub fn ambiguous<S: BuildHasher>(
    target: &FrameNode,
    visible: &HashSet<AttrId, S>,
    refs: &[AttrRef],
) -> Vec<usize> {
    if !target.renews {
        return Vec::new();
    }
    let Some(oldest) = refs.iter().map(|reference| reference.frame).min() else {
        return Vec::new();
    };
    let mut links: Vec<Link<'_>> = Vec::new();
    let mut seen = HashSet::new();
    let mut hits = BTreeSet::new();
    let mut stack: Vec<(&FrameNode, Option<usize>)> = vec![(target, None)];
    while let Some((node, chain)) = stack.pop() {
        if node.id < oldest
            || (chain.is_none() && !node.renews)
            || !seen.insert((ptr::from_ref(node), chain))
        {
            continue;
        }
        for (index, reference) in refs.iter().enumerate() {
            if reference.frame == node.id {
                let renewed = image(&reference.attr, chain, &links);
                if renewed != &reference.attr && visible.contains(renewed) {
                    hits.insert(index);
                }
            }
        }
        match &node.kind {
            FrameKind::Root => {}
            FrameKind::Derived(parent) => stack.push((parent.as_ref(), chain)),
            FrameKind::SetOp { first, .. } => stack.push((first.as_ref(), chain)),
            FrameKind::Join {
                left,
                right,
                remint,
                emits_right,
            } => {
                stack.push((left.as_ref(), chain));
                if *emits_right {
                    let inner = if remint.is_empty() {
                        chain
                    } else {
                        links.push((chain, remint));
                        Some(links.len() - 1)
                    };
                    stack.push((right.as_ref(), inner));
                }
            }
        }
    }
    hits.into_iter().collect()
}

#[must_use]
pub fn renewed_absent<S: BuildHasher>(
    target: &FrameNode,
    refs: &[AttrRef],
    outputs: &HashSet<AttrId, S>,
) -> Vec<usize> {
    if !target.renews {
        return Vec::new();
    }
    let renewed = reachable(target)
        .into_iter()
        .filter_map(|node| match &node.kind {
            FrameKind::Join { remint, .. } => Some(remint.keys()),
            _ => None,
        })
        .flatten()
        .collect::<HashSet<_>>();
    refs.iter()
        .enumerate()
        .filter(|(_, reference)| {
            renewed.contains(&reference.attr) && !outputs.contains(&reference.attr)
        })
        .map(|(index, _)| index)
        .collect()
}
