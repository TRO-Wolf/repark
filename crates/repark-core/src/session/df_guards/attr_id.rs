use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::datatypes::Field;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{
    Column, DFSchema, Result, TableReference, internal_datafusion_err, internal_err,
};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use repark_common::names::NameRule;

const ATTR_KEY: &str = "repark.attr";

static NEXT_ATTR: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttrId(String);

impl AttrId {
    #[must_use]
    pub fn mint() -> Self {
        Self(format!(
            "a{:012x}",
            NEXT_ATTR.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn of(field: &Field) -> Option<Self> {
        field.metadata().get(ATTR_KEY).cloned().map(Self)
    }

    fn metadata(&self) -> FieldMetadata {
        FieldMetadata::from(BTreeMap::from([(ATTR_KEY.to_string(), self.0.clone())]))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Bound(Vec<usize>),
    Ambiguous(Vec<usize>),
    Missing,
}

#[must_use]
pub fn attribute_ids(schema: &DFSchema) -> Vec<Option<AttrId>> {
    schema
        .fields()
        .iter()
        .map(|field| AttrId::of(field))
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn stamp(plan: LogicalPlan) -> Result<LogicalPlan> {
    if let LogicalPlan::Projection(projection) = &plan {
        return stamp_projection(projection).map(|stamped| stamped.unwrap_or(plan));
    }
    let first = match &plan {
        LogicalPlan::Union(union) => union
            .inputs
            .first()
            .map(|input| first_input_ids(input))
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    let minted = plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            AttrId::of(field).is_none().then(|| {
                first
                    .get(position)
                    .cloned()
                    .flatten()
                    .unwrap_or_else(AttrId::mint)
            })
        })
        .collect::<Vec<_>>();
    if minted.iter().all(Option::is_none) {
        return Ok(plan);
    }
    project_ids(plan, &minted)
}

fn first_input_ids(input: &LogicalPlan) -> Vec<Option<AttrId>> {
    let ids = attribute_ids(input.schema());
    let LogicalPlan::Projection(projection) = input else {
        return ids;
    };
    let below = projection.input.schema();
    projection
        .expr
        .iter()
        .zip(ids)
        .map(|(expr, id)| {
            id.or_else(|| match expr {
                Expr::Column(column) => below
                    .maybe_index_of_column(column)
                    .and_then(|index| AttrId::of(below.field(index))),
                _ => None,
            })
        })
        .collect()
}

fn stamp_projection(projection: &Projection) -> Result<Option<LogicalPlan>> {
    let mut changed = false;
    let expr = projection
        .expr
        .iter()
        .zip(projection.schema.iter())
        .map(|(expr, (qualifier, field))| {
            if carries_own_id(expr) || (is_column(expr) && AttrId::of(field).is_some()) {
                return expr.clone();
            }
            changed = true;
            with_id(expr.clone(), qualifier, field.name(), &AttrId::mint())
        })
        .collect::<Vec<_>>();
    if !changed {
        return Ok(None);
    }
    Projection::try_new(expr, Arc::clone(&projection.input))
        .map(|stamped| Some(LogicalPlan::Projection(stamped)))
}

fn carries_own_id(expr: &Expr) -> bool {
    let Expr::Alias(alias) = expr else {
        return false;
    };
    alias
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.inner().contains_key(ATTR_KEY))
}

fn is_column(expr: &Expr) -> bool {
    let mut inner = expr;
    while let Expr::Alias(alias) = inner {
        inner = &alias.expr;
    }
    matches!(inner, Expr::Column(_))
}

fn with_id(expr: Expr, qualifier: Option<&TableReference>, name: &str, id: &AttrId) -> Expr {
    match expr {
        Expr::Alias(alias) => {
            let mut metadata = alias.metadata.clone().unwrap_or_default();
            metadata.extend(id.metadata());
            Expr::Alias(alias.with_metadata(Some(metadata)))
        }
        other => other.alias_qualified_with_metadata(qualifier.cloned(), name, Some(id.metadata())),
    }
}

fn project_ids(plan: LogicalPlan, ids: &[Option<AttrId>]) -> Result<LogicalPlan> {
    let expr = plan
        .schema()
        .iter()
        .zip(ids)
        .map(|((qualifier, field), id)| {
            let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            match id {
                Some(id) => with_id(column, qualifier, field.name(), id),
                None => column,
            }
        })
        .collect::<Vec<_>>();
    Projection::try_new(expr, Arc::new(plan)).map(LogicalPlan::Projection)
}

#[allow(clippy::missing_errors_doc)]
pub fn remint_join_collisions(plan: LogicalPlan, left_width: usize) -> Result<LogicalPlan> {
    let fields = plan.schema().fields();
    if left_width > fields.len() {
        return internal_err!(
            "join re-mint: left width {left_width} exceeds the {} joined fields",
            fields.len()
        );
    }
    let (left, right) = fields.split_at(left_width);
    let held = left
        .iter()
        .filter_map(|field| AttrId::of(field))
        .collect::<HashSet<_>>();
    let mut fresh = HashMap::new();
    let reminted = left
        .iter()
        .map(|_| None)
        .chain(right.iter().map(|field| {
            AttrId::of(field)
                .filter(|id| held.contains(id))
                .map(|id| fresh.entry(id).or_insert_with(AttrId::mint).clone())
        }))
        .collect::<Vec<_>>();
    if reminted.iter().all(Option::is_none) {
        return Ok(plan);
    }
    project_ids(plan, &reminted)
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve(
    schema: &DFSchema,
    written: &str,
    qualifier: Option<&str>,
    rule: NameRule,
    displays: &[String],
) -> Result<Resolution> {
    if displays.len() != schema.fields().len() {
        return internal_err!(
            "resolve: {} display names for {} fields",
            displays.len(),
            schema.fields().len()
        );
    }
    let relation = qualifier.map(|text| TableReference::parse_str_normalized(text, true));
    let mut hits = Vec::new();
    let mut ids = BTreeSet::new();
    for (position, ((held, field), display)) in schema.iter().zip(displays).enumerate() {
        let relation_matches = match (&relation, held) {
            (None, _) => true,
            (Some(relation), Some(held)) => same_relation(relation, held, rule),
            (Some(_), None) => false,
        };
        if !relation_matches || !rule.matches(written, display) {
            continue;
        }
        let id = AttrId::of(field).ok_or_else(|| {
            internal_datafusion_err!(
                "resolve: field {position} ({}) carries no attribute id",
                field.name()
            )
        })?;
        hits.push(position);
        ids.insert(id);
    }
    Ok(match ids.len() {
        0 => Resolution::Missing,
        1 => Resolution::Bound(hits),
        _ => Resolution::Ambiguous(hits),
    })
}

pub(super) fn same_relation(
    written: &TableReference,
    held: &TableReference,
    rule: NameRule,
) -> bool {
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
