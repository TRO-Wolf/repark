use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::hash::BuildHasher;
use std::ops::{ControlFlow, Range};
use std::sync::Arc;

use datafusion::common::{DFSchema, Result, internal_datafusion_err, plan_datafusion_err};
use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr as SqlExpr, visit_expressions};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use repark_common::names::NameRule;
use repark_common::spark_error;

use super::attr_id::{AttrId, Resolution, attribute_ids, resolve};
use super::frame_lineage::{AttrRef, FrameId, FrameNode, ambiguous_images, shared_ids};

const TOKEN: &str = "__REPARK_ATTR_";

const PLACEHOLDER: &str = "__rp_ref_";

const NONDETERMINISTIC: &[&str] = &[
    "input_file_block_length",
    "input_file_block_start",
    "input_file_name",
    "monotonically_increasing_id",
    "rand",
    "randn",
    "random",
    "randstr",
    "shuffle",
    "spark_partition_id",
    "uniform",
    "uuid",
];

pub const SELF_JOIN_CONDITION: &str = "_LEGACY_ERROR_TEMP_1182";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfJoinRules {
    pub fail_ambiguous: bool,
    pub auto_resolve: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    SelfJoin {
        names: Vec<String>,
    },
    Missing {
        names: Vec<String>,
        input: Vec<String>,
        operation: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCondition {
    pub sql: String,
    pub remint: HashMap<AttrId, AttrId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Prepared {
    Condition(PreparedCondition),
    Refused(Refusal),
}

#[derive(Debug, Clone, Copy)]
pub struct JoinSide<'a> {
    pub node: &'a Arc<FrameNode>,
    pub schema: &'a DFSchema,
    pub displays: &'a [String],
    pub alias: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrRefText {
    pub text: String,
    pub refs: Vec<AttrRef>,
    spans: Vec<Range<usize>>,
}

#[must_use]
pub fn self_join_message(names: &[String], config: &str) -> String {
    format!(
        "Column {} are ambiguous. It's probably because you joined several Datasets together, \
         and some of these Datasets are the same. This column points to one of the Datasets but \
         Spark is unable to figure out which one. Please alias the Datasets with different names \
         via `Dataset.as` before joining them, and specify the column using qualified name, e.g. \
         `df.as(\"a\").join(df.as(\"b\"), $\"a.id\" > $\"b.id\")`. You can also set {config} to \
         false to disable this check.",
        names.join(", ")
    )
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_attr_refs(sql: &str) -> Result<AttrRefText> {
    if sql.contains(PLACEHOLDER) {
        return Err(internal_datafusion_err!(
            "self-join references: the text already holds a `{PLACEHOLDER}` name"
        ));
    }
    let bytes = sql.as_bytes();
    let mut text = String::with_capacity(sql.len());
    let mut refs = Vec::new();
    let mut spans = Vec::new();
    let mut copied = 0;
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        if matches!(byte, b'\'' | b'"' | b'`') {
            index = after_quoted(bytes, index, byte);
        } else if bytes[index..].starts_with(TOKEN.as_bytes()) {
            let (reference, end) = attr_token(sql, index)?;
            text.push_str(&sql[copied..index]);
            let _ = write!(text, "{PLACEHOLDER}{}", refs.len());
            refs.push(reference);
            spans.push(index..end);
            copied = end;
            index = end;
        } else {
            index += 1;
        }
    }
    text.push_str(&sql[copied..]);
    Ok(AttrRefText { text, refs, spans })
}

fn after_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut index = start + 1;
    while let Some(&byte) = bytes.get(index) {
        if byte == b'\\' && quote != b'`' {
            index += 2;
        } else if byte != quote {
            index += 1;
        } else if bytes.get(index + 1) == Some(&quote) {
            index += 2;
        } else {
            return index + 1;
        }
    }
    bytes.len()
}

fn attr_token(sql: &str, start: usize) -> Result<(AttrRef, usize)> {
    let malformed = || {
        internal_datafusion_err!(
            "self-join references: malformed attribute token at byte {start}; a token is \
             `{TOKEN}<id>__F<frame>__<qualifiers>__`"
        )
    };
    let body = &sql[start + TOKEN.len()..];
    let id_len = body.bytes().take_while(u8::is_ascii_alphanumeric).count();
    if id_len == 0 {
        return Err(malformed());
    }
    let after_id = &body[id_len..];
    let Some(frame_field) = after_id.strip_prefix("__F") else {
        return Err(internal_datafusion_err!(
            "self-join references: attribute token at byte {start} carries no frame field"
        ));
    };
    let digits = frame_field.bytes().take_while(u8::is_ascii_digit).count();
    let frame = frame_field[..digits]
        .parse::<u64>()
        .map_err(|_| malformed())?;
    let qualifiers = frame_field[digits..]
        .strip_prefix("__")
        .ok_or_else(malformed)?;
    let run = qualifiers
        .char_indices()
        .find(|(_, held)| !(held.is_alphanumeric() || matches!(held, '_' | '\\' | '|')))
        .map_or(qualifiers.len(), |(position, _)| position);
    let close = qualifiers[..run].rfind("__").ok_or_else(malformed)?;
    let end = sql.len() - qualifiers.len() + close + 2;
    let reference = AttrRef {
        attr: AttrId::from_token(&body[..id_len]),
        frame: FrameId::from_raw(frame),
    };
    Ok((reference, end))
}

fn parse_condition(text: &str) -> Result<SqlExpr> {
    Parser::new(&DatabricksDialect {})
        .try_with_sql(text)
        .and_then(|mut parser| parser.parse_expr())
        .map_err(|error| plan_datafusion_err!("self-join references do not parse: {error}"))
}

fn placeholder(expr: &SqlExpr) -> Option<usize> {
    let SqlExpr::Identifier(ident) = expr else {
        return None;
    };
    if ident.quote_style.is_some() {
        return None;
    }
    ident.value.strip_prefix(PLACEHOLDER)?.parse().ok()
}

fn placeholders(expr: &SqlExpr) -> Vec<usize> {
    let mut found = Vec::new();
    let _ = visit_expressions(expr, |node| {
        found.extend(placeholder(node));
        ControlFlow::<()>::Continue(())
    });
    found
}

fn attr_with_cast(expr: &SqlExpr) -> Option<(usize, bool)> {
    let mut node = expr;
    let mut bare = true;
    loop {
        match node {
            SqlExpr::Nested(inner) => node = inner,
            SqlExpr::Cast { expr: inner, .. } => {
                bare = false;
                node = inner;
            }
            other => return placeholder(other).map(|index| (index, bare)),
        }
    }
}

fn foldable(expr: &SqlExpr) -> bool {
    visit_expressions(expr, |node| match node {
        SqlExpr::Identifier(_)
        | SqlExpr::CompoundIdentifier(_)
        | SqlExpr::Subquery(_)
        | SqlExpr::Exists { .. }
        | SqlExpr::InSubquery { .. } => ControlFlow::Break(()),
        SqlExpr::Function(function)
            if function.over.is_some()
                || NONDETERMINISTIC
                    .contains(&function.name.to_string().to_ascii_lowercase().as_str()) =>
        {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_continue()
}

#[derive(Debug, Default)]
struct Shape {
    exempt: HashSet<usize>,
    windowed: HashSet<usize>,
    pairs: Vec<(usize, usize)>,
}

impl Shape {
    fn of(expr: &SqlExpr, attrs: &[AttrId], same_frame: bool) -> Self {
        let mut shape = Self::default();
        let _ = visit_expressions(expr, |node| {
            match node {
                SqlExpr::Function(function) if function.over.is_some() => {
                    shape.windowed.extend(placeholders(node));
                }
                SqlExpr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq | BinaryOperator::Spaceship,
                    right,
                }
                | SqlExpr::IsNotDistinctFrom(left, right) => {
                    shape.equality(left, right, attrs, same_frame);
                }
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
        shape.pairs.sort_unstable();
        shape
    }

    fn equality(&mut self, left: &SqlExpr, right: &SqlExpr, attrs: &[AttrId], same_frame: bool) {
        match (attr_with_cast(left), attr_with_cast(right)) {
            (Some((first, first_bare)), Some((second, second_bare)))
                if attrs.get(first).is_some() && attrs.get(first) == attrs.get(second) =>
            {
                self.exempt.extend([first, second]);
                if first_bare && second_bare {
                    self.pairs.push((first, second));
                }
            }
            (Some((first, _)), None) if same_frame && foldable(right) => {
                self.exempt.insert(first);
            }
            (None, Some((second, _))) if same_frame && foldable(left) => {
                self.exempt.insert(second);
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    Left(usize),
    Right(usize),
}

fn side_ids(side: &JoinSide<'_>) -> Result<Vec<AttrId>> {
    if side.displays.len() != side.schema.fields().len() {
        return Err(internal_datafusion_err!(
            "self-join condition: {} display names for {} fields",
            side.displays.len(),
            side.schema.fields().len()
        ));
    }
    attribute_ids(side.schema)
        .into_iter()
        .enumerate()
        .map(|(position, id)| {
            id.ok_or_else(|| {
                internal_datafusion_err!(
                    "self-join condition: join input field {position} carries no attribute id"
                )
            })
        })
        .collect()
}

fn quoted(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn display_at<'a>(ids: &[AttrId], displays: &[&'a String], id: &AttrId) -> Result<&'a String> {
    ids.iter()
        .position(|held| held == id)
        .and_then(|position| displays.get(position).copied())
        .ok_or_else(|| {
            internal_datafusion_err!("self-join references: image {} is not visible", id.as_str())
        })
}

fn bind(attr: &AttrId, left: &[AttrId], right: &[AttrId]) -> Option<Bound> {
    if let Some(position) = left.iter().position(|id| id == attr) {
        return Some(Bound::Left(position));
    }
    right.iter().position(|id| id == attr).map(Bound::Right)
}

fn rendered(side: &JoinSide<'_>, position: usize) -> String {
    format!(
        "{}.{}",
        side.alias,
        quoted(side.schema.field(position).name())
    )
}

fn resolved_by_name(side: &JoinSide<'_>, name: &str, rule: NameRule) -> Result<String> {
    match resolve(side.schema, name, None, rule, side.displays, None)? {
        Resolution::Bound(hits) => hits
            .first()
            .map(|&position| rendered(side, position))
            .ok_or_else(|| internal_datafusion_err!("self-join rewrite: bound without a hit")),
        Resolution::Ambiguous(hits) => {
            let options = hits
                .iter()
                .filter_map(|&position| side.displays.get(position))
                .map(|display| quoted(display))
                .collect::<Vec<_>>()
                .join(", ");
            Err(plan_datafusion_err!(
                "{}",
                spark_error::message(
                    spark_error::AMBIGUOUS_REFERENCE,
                    &[("reference", &quoted(name)), ("options", &options)],
                )
            ))
        }
        Resolution::Missing => {
            let suggestions = side
                .displays
                .iter()
                .map(|display| quoted(display))
                .collect::<Vec<_>>()
                .join(", ");
            Err(plan_datafusion_err!(
                "{}",
                spark_error::message(
                    spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
                    &[("columnName", &quoted(name)), ("suggestions", &suggestions)],
                )
            ))
        }
    }
}

fn missing_refusal<S: BuildHasher>(
    missing: &[&AttrId],
    names: &HashMap<AttrId, String, S>,
    input: Vec<String>,
    rule: NameRule,
) -> Result<Refusal> {
    let names = missing
        .iter()
        .map(|attr| {
            names.get(*attr).cloned().ok_or_else(|| {
                internal_datafusion_err!(
                    "self-join condition: no display name for missing attribute {}",
                    attr.as_str()
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let operation = names
        .iter()
        .filter(|name| input.iter().any(|held| rule.matches(name, held)))
        .cloned()
        .collect();
    Ok(Refusal::Missing {
        names,
        input,
        operation,
    })
}

struct Detection<'a> {
    left: &'a JoinSide<'a>,
    right: &'a JoinSide<'a>,
    remint: &'a HashMap<AttrId, AttrId>,
    visible_ids: &'a [AttrId],
    displays: &'a [&'a String],
}

impl Detection<'_> {
    fn refusal(&self, refs: &[AttrRef], shape: &Shape) -> Result<Option<Refusal>> {
        let tested = (0..refs.len())
            .filter(|index| !shape.exempt.contains(index) && !shape.windowed.contains(index))
            .filter_map(|index| refs.get(index).cloned())
            .collect::<Vec<_>>();
        let provisional = FrameNode::join(
            &DFSchema::empty(),
            Arc::clone(self.left.node),
            Arc::clone(self.right.node),
            self.remint.clone(),
            true,
        )?;
        let visible = self.visible_ids.iter().cloned().collect::<HashSet<_>>();
        let images = ambiguous_images(&provisional, &visible, &tested);
        if images.is_empty() {
            return Ok(None);
        }
        let names = images
            .iter()
            .map(|(_, image)| display_at(self.visible_ids, self.displays, image).cloned())
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Refusal::SelfJoin { names }))
    }
}

fn rewrite_pairs(
    sides: (&JoinSide<'_>, &JoinSide<'_>),
    pairs: &[(usize, usize)],
    bound: &[Option<Bound>],
    texts: &mut [String],
    rule: NameRule,
) -> Result<()> {
    let (left, right) = sides;
    for &(first, second) in pairs {
        let name = match bound.get(first) {
            Some(Some(Bound::Left(position))) => left.displays.get(*position),
            Some(Some(Bound::Right(position))) => right.displays.get(*position),
            _ => None,
        }
        .ok_or_else(|| internal_datafusion_err!("self-join rewrite: operand {first} unbound"))?;
        let on_left = resolved_by_name(left, name, rule)?;
        let on_right = resolved_by_name(right, name, rule)?;
        if let Some(text) = texts.get_mut(first) {
            *text = on_left;
        }
        if let Some(text) = texts.get_mut(second) {
            *text = on_right;
        }
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn prepare_join_condition<S: BuildHasher>(
    cond_sql: &str,
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    names: &HashMap<AttrId, String, S>,
    rule: NameRule,
    rules: SelfJoinRules,
) -> Result<Prepared> {
    let left_ids = side_ids(left)?;
    let right_ids = side_ids(right)?;
    let shared = shared_ids(left.node, &right_ids);
    let remint = shared
        .iter()
        .map(|id| (id.clone(), AttrId::mint()))
        .collect::<HashMap<_, _>>();
    let parsed = parse_attr_refs(cond_sql)?;
    if parsed.refs.is_empty() {
        return Ok(Prepared::Condition(PreparedCondition {
            sql: cond_sql.to_string(),
            remint,
        }));
    }
    let renewed = right_ids
        .iter()
        .map(|id| remint.get(id).unwrap_or(id).clone())
        .collect::<Vec<_>>();
    let refs = parsed
        .refs
        .iter()
        .map(|reference| match remint.get(&reference.attr) {
            Some(image) if !left_ids.contains(&reference.attr) => AttrRef {
                attr: image.clone(),
                frame: reference.frame,
            },
            _ => reference.clone(),
        })
        .collect::<Vec<_>>();
    let attrs = refs
        .iter()
        .map(|reference| reference.attr.clone())
        .collect::<Vec<_>>();
    let shape = Shape::of(
        &parse_condition(&parsed.text)?,
        &attrs,
        left.node.id() == right.node.id(),
    );
    let visible_ids = left_ids.iter().chain(&renewed).cloned().collect::<Vec<_>>();
    let displays = left
        .displays
        .iter()
        .chain(right.displays)
        .collect::<Vec<_>>();
    let detection = Detection {
        left,
        right,
        remint: &remint,
        visible_ids: &visible_ids,
        displays: &displays,
    };
    if rules.fail_ambiguous
        && let Some(refusal) = detection.refusal(&refs, &shape)?
    {
        return Ok(Prepared::Refused(refusal));
    }
    let bound = attrs
        .iter()
        .map(|attr| bind(attr, &left_ids, &renewed))
        .collect::<Vec<_>>();
    let mut missing: Vec<&AttrId> = Vec::new();
    for (attr, side) in attrs.iter().zip(&bound) {
        if side.is_none() && !missing.contains(&attr) {
            missing.push(attr);
        }
    }
    if !missing.is_empty() {
        let input = displays.iter().map(|display| (*display).clone()).collect();
        return missing_refusal(&missing, names, input, rule).map(Prepared::Refused);
    }
    let mut texts = bound
        .iter()
        .map(|side| match side {
            Some(Bound::Left(position)) => rendered(left, *position),
            Some(Bound::Right(position)) => rendered(right, *position),
            None => String::new(),
        })
        .collect::<Vec<_>>();
    if rules.auto_resolve && left_ids.iter().any(|id| right_ids.contains(id)) {
        rewrite_pairs((left, right), &shape.pairs, &bound, &mut texts, rule)?;
    }
    Ok(Prepared::Condition(PreparedCondition {
        sql: spliced(cond_sql, &parsed.spans, &texts),
        remint,
    }))
}

fn spliced(sql: &str, spans: &[Range<usize>], texts: &[String]) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0;
    for (span, text) in spans.iter().zip(texts) {
        out.push_str(&sql[copied..span.start]);
        out.push_str(text);
        copied = span.end;
    }
    out.push_str(&sql[copied..]);
    out
}

#[allow(clippy::missing_errors_doc)]
pub fn check_refs(
    target: &FrameNode,
    displays: &[String],
    sql_parts: &[&str],
    rules: SelfJoinRules,
) -> Result<Option<Refusal>> {
    if !rules.fail_ambiguous || !target.renews() {
        return Ok(None);
    }
    if displays.len() != target.outputs().len() {
        return Err(internal_datafusion_err!(
            "self-join references: {} display names for {} outputs",
            displays.len(),
            target.outputs().len()
        ));
    }
    let mut refs: Vec<AttrRef> = Vec::new();
    for part in sql_parts {
        let parsed = parse_attr_refs(part)?;
        if parsed.refs.is_empty() {
            continue;
        }
        let windowed = Shape::of(&parse_condition(&parsed.text)?, &[], false).windowed;
        for (index, reference) in parsed.refs.into_iter().enumerate() {
            if !windowed.contains(&index) && !refs.contains(&reference) {
                refs.push(reference);
            }
        }
    }
    let visible = target.outputs().iter().cloned().collect::<HashSet<_>>();
    let images = ambiguous_images(target, &visible, &refs);
    if images.is_empty() {
        return Ok(None);
    }
    let shown = displays.iter().collect::<Vec<_>>();
    let names = images
        .iter()
        .map(|(_, image)| display_at(target.outputs(), &shown, image).cloned())
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(Refusal::SelfJoin { names }))
}

#[must_use]
pub fn quoted_names(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

#[must_use]
pub fn missing_condition(operation: &[String]) -> &'static str {
    if operation.is_empty() {
        "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT"
    } else {
        "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION"
    }
}

#[must_use]
pub fn missing_message(names: &[String], input: &[String], operation: &[String]) -> String {
    let missing = quoted_names(names);
    let available = quoted_names(input);
    let condition = missing_condition(operation);
    if operation.is_empty() {
        format!(
            "[{condition}] Resolved attribute(s) {missing} missing from {available} in operator \
             !Join.  SQLSTATE: XX000"
        )
    } else {
        format!(
            "[{condition}] Resolved attribute(s) {missing} missing from {available} in operator \
             !Join. Attribute(s) with the same name appear in the operation: {}. Please check if \
             the right attribute(s) are used. SQLSTATE: XX000",
            quoted_names(operation)
        )
    }
}
