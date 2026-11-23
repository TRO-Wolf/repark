use std::borrow::Cow;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DataFusionError, NullEquality, Result, ScalarValue};
use datafusion::config::Dialect as ParserDialect;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_nested::expr_fn::range;
use datafusion::logical_expr::{
    Expr, ExprSchemable, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder,
    Projection, col, lit, when,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Query, SetExpr, SetOperator, SetQuantifier, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::{DatabricksDialect, Dialect};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Word};

use crate::struct_walk::peel_encoding;

const RULE_NAME: &str = "setop_all_multiset";
const AFTER_RULE: &str = "struct_setop_names";
const KEY_PREFIX: &str = "__repark_setop_all_key_";
const LEFT_COUNT: &str = "__repark_setop_all_left";
const RIGHT_COUNT: &str = "__repark_setop_all_right";
const COPIES: &str = "__repark_setop_all_copies";

type Tags = &'static [(i64, &'static str)];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Multiset {
    Intersect,
    Except,
}

#[derive(Debug, Default)]
pub struct MultisetSetOperations;

impl AnalyzerRule for MultisetSetOperations {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if !holds_multiset_join(&plan)? {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(|node| {
            if let LogicalPlan::Join(join) = &node
                && let Some(kind) = multiset_join(join)
            {
                return Ok(Transformed::yes(rewrite_multiset(join, kind)?));
            }
            Ok(Transformed::no(node))
        })
        .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        RULE_NAME
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn insert_multiset_set_operations(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == AFTER_RULE) else {
        return Err(DataFusionError::Internal(format!(
            "the multiset set-operation rule requires the {AFTER_RULE} analyzer rule"
        )));
    };
    rules.insert(position + 1, Arc::new(MultisetSetOperations));
    Ok(rules)
}

fn holds_multiset_join(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Join(join) = node
            && multiset_join(join).is_some()
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

#[must_use]
pub fn is_multiset_set_operation(join: &Join) -> bool {
    multiset_join(join).is_some()
}

pub(crate) fn multiset_join(join: &Join) -> Option<Multiset> {
    let kind = match join.join_type {
        JoinType::LeftSemi => Multiset::Intersect,
        JoinType::LeftAnti => Multiset::Except,
        _ => return None,
    };
    if matches!(join.left.as_ref(), LogicalPlan::Distinct(_))
        || join.null_equality != NullEquality::NullEqualsNull
        || join.filter.is_some()
        || join.join_constraint != JoinConstraint::On
    {
        return None;
    }
    let left = join.left.schema().fields();
    let right = join.right.schema().fields();
    if join.on.is_empty() || join.on.len() != left.len() || join.on.len() != right.len() {
        return None;
    }
    let positional = join.on.iter().zip(left.iter().zip(right.iter())).all(
        |((left_key, right_key), (left_field, right_field))| {
            names_column(left_key, left_field.name()) && names_column(right_key, right_field.name())
        },
    );
    positional.then_some(kind)
}

fn beneath_casts(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => beneath_casts(&alias.expr),
        Expr::Cast(cast) => beneath_casts(&cast.expr),
        Expr::TryCast(cast) => beneath_casts(&cast.expr),
        other => other,
    }
}

fn names_column(expr: &Expr, name: &str) -> bool {
    matches!(beneath_casts(expr), Expr::Column(column) if column.name == name)
}

fn positional_key(key: &Expr, schema: &DFSchema, index: usize) -> Result<Expr> {
    let column = Expr::Column(Column::from(schema.qualified_field(index)));
    key.clone()
        .transform_up(|node| match node {
            Expr::Column(_) => Ok(Transformed::yes(column.clone())),
            Expr::Alias(alias) => Ok(Transformed::yes(*alias.expr)),
            other => Ok(Transformed::no(other)),
        })
        .data()
}

fn tagged_side(
    plan: &Arc<LogicalPlan>,
    keys: Vec<Expr>,
    tags: &[(i64, &str)],
) -> Result<LogicalPlan> {
    let mut exprs = keys;
    exprs.extend(tags.iter().map(|(tag, name)| lit(*tag).alias(*name)));
    LogicalPlanBuilder::from(Arc::clone(plan))
        .project(exprs)?
        .build()
}

fn decimal_shape(data_type: &DataType) -> Option<(i16, i16)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            Some((i16::from(*precision), i16::from(*scale)))
        }
        _ => None,
    }
}

fn integer_max_digits(data_type: &DataType) -> Option<i16> {
    match data_type {
        DataType::Int8 | DataType::UInt8 => Some(3),
        DataType::Int16 | DataType::UInt16 => Some(5),
        DataType::Int32 | DataType::UInt32 => Some(10),
        DataType::Int64 => Some(19),
        DataType::UInt64 => Some(20),
        _ => None,
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn variable_list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

pub(crate) fn is_lossless_round_trip(out_type: &DataType, key_type: &DataType) -> bool {
    let out_type = peel_encoding(out_type);
    let key_type = peel_encoding(key_type);
    if out_type == key_type {
        return true;
    }
    if is_string_type(out_type) && is_string_type(key_type) {
        return true;
    }
    if (is_binary_type(out_type) || matches!(out_type, DataType::FixedSizeBinary(_)))
        && is_binary_type(key_type)
    {
        return true;
    }
    if let (Some((out_precision, out_scale)), Some((key_precision, key_scale))) =
        (decimal_shape(out_type), decimal_shape(key_type))
    {
        return key_scale >= out_scale
            && (key_precision - key_scale) >= (out_precision - out_scale);
    }
    if let (Some(digits), Some((key_precision, key_scale))) =
        (integer_max_digits(out_type), decimal_shape(key_type))
    {
        return key_scale >= 0 && (key_precision - key_scale) >= digits;
    }
    if let (Some(out_elem), Some(key_elem)) = (
        variable_list_element(out_type),
        variable_list_element(key_type),
    ) {
        return is_lossless_round_trip(out_elem.data_type(), key_elem.data_type());
    }
    match (out_type, key_type) {
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (
            DataType::UInt8,
            DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64,
        )
        | (
            DataType::UInt16,
            DataType::UInt32 | DataType::UInt64 | DataType::Int32 | DataType::Int64,
        )
        | (DataType::UInt32, DataType::UInt64 | DataType::Int64)
        | (DataType::Float16, DataType::Float32 | DataType::Float64)
        | (DataType::Float32, DataType::Float64)
        | (
            DataType::Int8 | DataType::UInt8 | DataType::Int16 | DataType::UInt16,
            DataType::Float32 | DataType::Float64,
        )
        | (DataType::Int32 | DataType::UInt32, DataType::Float64)
        | (DataType::Date32, DataType::Date64)
        | (
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond),
        )
        | (
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond),
        )
        | (DataType::Time64(TimeUnit::Microsecond), DataType::Time64(TimeUnit::Nanosecond)) => true,
        (DataType::Timestamp(out_unit, out_tz), DataType::Timestamp(key_unit, key_tz))
            if out_tz == key_tz =>
        {
            matches!(
                (out_unit, key_unit),
                (
                    TimeUnit::Second,
                    TimeUnit::Millisecond | TimeUnit::Microsecond
                ) | (TimeUnit::Millisecond, TimeUnit::Microsecond)
            )
        }
        (
            DataType::FixedSizeList(out_elem, out_len),
            DataType::FixedSizeList(key_elem, key_len),
        ) if out_len == key_len => {
            is_lossless_round_trip(out_elem.data_type(), key_elem.data_type())
        }
        (DataType::FixedSizeList(out_elem, _), other) => {
            variable_list_element(other).is_some_and(|key_elem| {
                is_lossless_round_trip(out_elem.data_type(), key_elem.data_type())
            })
        }
        (DataType::Struct(out_fields), DataType::Struct(key_fields)) => {
            out_fields.len() == key_fields.len()
                && out_fields
                    .iter()
                    .zip(key_fields.iter())
                    .all(|(out_field, key_field)| {
                        out_field.name() == key_field.name()
                            && is_lossless_round_trip(out_field.data_type(), key_field.data_type())
                    })
        }
        _ => false,
    }
}

fn lossy_round_trip_refusal(
    column: &str,
    out_type: &DataType,
    key_type: &DataType,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "multiset set operation cannot round-trip column `{column}` from output type {out_type} \
         through coerced key type {key_type} losslessly; cast both sides to a common type \
         explicitly (SETOP-MIXED-TYPE-COERCION-1)"
    ))
}

fn rewrite_multiset(join: &Join, kind: Multiset) -> Result<LogicalPlan> {
    let left_schema = join.left.schema();
    let right_schema = join.right.schema();
    let mut left_keys = Vec::with_capacity(join.on.len());
    let mut right_keys = Vec::with_capacity(join.on.len());
    let mut key_types = Vec::with_capacity(join.on.len());
    for (index, (left_key, right_key)) in join.on.iter().enumerate() {
        let name = format!("{KEY_PREFIX}{index}");
        let left_key = positional_key(left_key, left_schema, index)?;
        let right_key = positional_key(right_key, right_schema, index)?;
        let key_type = left_key.get_type(left_schema)?;
        let right_key = if right_key.get_type(right_schema)? == key_type {
            right_key
        } else {
            right_key.cast_to(&key_type, right_schema)?
        };
        left_keys.push(left_key.alias(&name));
        right_keys.push(right_key.alias(&name));
        key_types.push(key_type);
    }
    for ((_, field), key_type) in join.schema.iter().zip(key_types.iter()) {
        if field.data_type() != &DataType::Null
            && field.data_type() != key_type
            && !is_lossless_round_trip(field.data_type(), key_type)
        {
            return Err(lossy_round_trip_refusal(
                field.name(),
                field.data_type(),
                key_type,
            ));
        }
    }
    let (left_tags, right_tags): (Tags, Tags) = match kind {
        Multiset::Intersect => (
            &[(1, LEFT_COUNT), (0, RIGHT_COUNT)],
            &[(0, LEFT_COUNT), (1, RIGHT_COUNT)],
        ),
        Multiset::Except => (&[(1, LEFT_COUNT)], &[(-1, LEFT_COUNT)]),
    };
    let left = tagged_side(&join.left, left_keys, left_tags)?;
    let right = tagged_side(&join.right, right_keys, right_tags)?;
    let groups = (0..join.on.len())
        .map(|index| col(format!("{KEY_PREFIX}{index}")))
        .collect::<Vec<_>>();
    let counted = LogicalPlanBuilder::from(left).union(right)?;
    let (counts, copies) = match kind {
        Multiset::Intersect => {
            let left_count = col(LEFT_COUNT);
            let right_count = col(RIGHT_COUNT);
            let fewer = when(left_count.clone().lt(right_count.clone()), left_count)
                .otherwise(right_count)?;
            (
                vec![
                    sum(col(LEFT_COUNT)).alias(LEFT_COUNT),
                    sum(col(RIGHT_COUNT)).alias(RIGHT_COUNT),
                ],
                fewer,
            )
        }
        Multiset::Except => (
            vec![sum(col(LEFT_COUNT)).alias(LEFT_COUNT)],
            when(col(LEFT_COUNT).gt(lit(0_i64)), col(LEFT_COUNT)).otherwise(lit(0_i64))?,
        ),
    };
    let mut replicated = groups.clone();
    replicated.push(range(lit(0_i64), copies, lit(1_i64)).alias(COPIES));
    let unnested = counted
        .aggregate(groups, counts)?
        .project(replicated)?
        .unnest_column(COPIES)?
        .build()?;
    let unnested_schema = Arc::clone(unnested.schema());
    let outputs = join
        .schema
        .iter()
        .zip(key_types.iter())
        .enumerate()
        .map(|(index, ((qualifier, field), key_type))| {
            let expr = if field.data_type() == &DataType::Null {
                lit(ScalarValue::Null)
            } else {
                let key = col(format!("{KEY_PREFIX}{index}"));
                if key_type == field.data_type() {
                    key
                } else {
                    key.cast_to(field.data_type(), &unnested_schema)?
                }
            };
            Ok(expr.alias_qualified(qualifier.cloned(), field.name()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        outputs,
        Arc::new(unnested),
        Arc::clone(&join.schema),
    )?))
}

fn may_name_minus_all(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let has = |needle: &[u8]| {
        bytes
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
    };
    has(b"minus") && has(b"all")
}

fn byte_offset(line_starts: &[usize], sql: &str, location: Location) -> Option<usize> {
    let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
    let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
    let start = *line_starts.get(line)?;
    let rest = sql.get(start..)?;
    rest.char_indices()
        .nth(column)
        .map(|(offset, _)| start + offset)
}

const MINUS: &str = "MINUS";
const EXCEPT: &str = "EXCEPT";
const DDL_DML_STARTERS: &[&str] = &[
    "CREATE", "REPLACE", "MERGE", "INSERT", "EXPLAIN", "ALTER", "UPDATE", "DELETE",
];

#[derive(Clone, Copy)]
enum ParseKind {
    Statements,
    Expr,
    Query,
}

#[derive(Default)]
struct SetOpCounts {
    minus_all: usize,
    except_all: usize,
}

impl SetOpCounts {
    fn visit_set_expr(&mut self, set_expr: &SetExpr) {
        if let SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } = set_expr
        {
            if *set_quantifier == SetQuantifier::All {
                match op {
                    SetOperator::Minus => self.minus_all += 1,
                    SetOperator::Except => self.except_all += 1,
                    _ => {}
                }
            }
            self.visit_set_expr(left);
            self.visit_set_expr(right);
        }
    }
}

impl Visitor for SetOpCounts {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.visit_set_expr(&query.body);
        ControlFlow::Continue(())
    }
}

fn count_set_ops<T: Visit>(node: &T) -> SetOpCounts {
    let mut counts = SetOpCounts::default();
    let _ = node.visit(&mut counts);
    counts
}

fn parse_counts(
    dialect: &dyn Dialect,
    tokens: &[TokenWithSpan],
    kind: ParseKind,
) -> Option<SetOpCounts> {
    let mut parser = Parser::new(dialect).with_tokens_with_locations(tokens.to_vec());
    match kind {
        ParseKind::Statements => {
            let statements = parser.parse_statements().ok()?;
            Some(count_set_ops(&statements))
        }
        ParseKind::Expr => {
            let expr = parser.parse_expr().ok()?;
            matches!(parser.peek_token().token, Token::EOF).then(|| count_set_ops(&expr))
        }
        ParseKind::Query => {
            let query = parser.parse_query().ok()?;
            matches!(parser.peek_token().token, Token::EOF).then(|| count_set_ops(query.as_ref()))
        }
    }
}

fn except_token() -> Token {
    Token::Word(Word {
        value: EXCEPT.to_string(),
        quote_style: None,
        keyword: Keyword::EXCEPT,
    })
}

fn resolve_in_slice(
    dialect: &dyn Dialect,
    slice: &[TokenWithSpan],
    offset: usize,
    candidates: &[(usize, usize)],
    kind: ParseKind,
) -> Option<Vec<usize>> {
    let base = parse_counts(dialect, slice, kind)?;
    if base.minus_all == 0 {
        return Some(Vec::new());
    }
    if base.minus_all == candidates.len() {
        return Some(candidates.iter().map(|&(_, start)| start).collect());
    }
    let mut probe = slice.to_vec();
    let mut accepted = Vec::with_capacity(base.minus_all);
    for &(token_index, byte_start) in candidates {
        let local = token_index - offset;
        let saved = std::mem::replace(&mut probe[local].token, except_token());
        let is_operator = parse_counts(dialect, &probe, kind).is_some_and(|after| {
            after.minus_all + 1 == base.minus_all && after.except_all == base.except_all + 1
        });
        probe[local].token = saved;
        if is_operator {
            accepted.push(byte_start);
        }
    }
    Some(accepted)
}

fn candidate_minus_all_tokens(tokens: &[TokenWithSpan], sql: &str) -> Option<Vec<(usize, usize)>> {
    let line_starts = std::iter::once(0)
        .chain(sql.match_indices('\n').map(|(index, _)| index + 1))
        .collect::<Vec<_>>();
    let mut pending: Option<(usize, usize)> = None;
    let mut candidates = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if matches!(token.token, Token::Whitespace(_)) {
            continue;
        }
        let word = match &token.token {
            Token::Word(word) if word.quote_style.is_none() => Some(word.value.as_str()),
            _ => None,
        };
        if let (Some((minus_index, start)), Some(value)) = (pending, word)
            && value.eq_ignore_ascii_case("ALL")
        {
            candidates.push((minus_index, start));
            pending = None;
            continue;
        }
        pending = match word {
            Some(value) if value.eq_ignore_ascii_case(MINUS) => {
                let start = byte_offset(&line_starts, sql, token.span.start)?;
                let spelled = sql.get(start..start + MINUS.len())?;
                if !spelled.eq_ignore_ascii_case(MINUS) {
                    return None;
                }
                Some((index, start))
            }
            _ => None,
        };
    }
    Some(candidates)
}

fn is_ddl_dml_starter(tokens: &[TokenWithSpan]) -> bool {
    tokens
        .iter()
        .find(|token| !matches!(token.token, Token::Whitespace(_)))
        .is_some_and(|token| match &token.token {
            Token::Word(word) if word.quote_style.is_none() => DDL_DML_STARTERS
                .iter()
                .any(|starter| word.value.eq_ignore_ascii_case(starter)),
            _ => false,
        })
}

fn trimmed_statement_end(tokens: &[TokenWithSpan]) -> usize {
    let mut end = tokens.len();
    while end > 0
        && matches!(
            tokens[end - 1].token,
            Token::Whitespace(_) | Token::SemiColon | Token::EOF
        )
    {
        end -= 1;
    }
    end
}

fn balanced_paren_ranges(tokens: &[TokenWithSpan]) -> Vec<(usize, usize)> {
    let mut stack = Vec::new();
    let mut ranges = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        match token.token {
            Token::LParen => stack.push(index),
            Token::RParen => {
                if let Some(open) = stack.pop() {
                    ranges.push((open, index));
                }
            }
            _ => {}
        }
    }
    ranges.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| (right.1 - right.0).cmp(&(left.1 - left.0)))
    });
    ranges
}

fn resolve_ddl_dml_fallback(
    dialect: &dyn Dialect,
    tokens: &[TokenWithSpan],
    candidates: &[(usize, usize)],
) -> Vec<usize> {
    let end = trimmed_statement_end(tokens);
    let mut resolved = vec![false; candidates.len()];
    let mut accepted = Vec::new();
    let mut depth = 0usize;
    let mut prev_as = false;
    for (index, token) in tokens[..end].iter().enumerate() {
        if matches!(token.token, Token::Whitespace(_)) {
            continue;
        }
        let starts_suffix = depth == 0
            && index > 0
            && match &token.token {
                Token::Word(word) if word.quote_style.is_none() => {
                    word.value.eq_ignore_ascii_case("SELECT")
                        || word.value.eq_ignore_ascii_case("WITH")
                }
                Token::LParen => prev_as,
                _ => false,
            };
        if starts_suffix {
            let slice_candidates = candidates
                .iter()
                .copied()
                .filter(|&(token_index, _)| token_index >= index && token_index < end)
                .collect::<Vec<_>>();
            if !slice_candidates.is_empty()
                && let Some(found) = resolve_in_slice(
                    dialect,
                    &tokens[index..end],
                    index,
                    &slice_candidates,
                    ParseKind::Query,
                )
            {
                accepted.extend(found);
                for (slot, &(token_index, _)) in candidates.iter().enumerate() {
                    if token_index >= index && token_index < end {
                        resolved[slot] = true;
                    }
                }
                break;
            }
        }
        prev_as = matches!(
            &token.token,
            Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("AS")
        );
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    if resolved.iter().all(|&done| done) {
        return accepted;
    }
    let parens = balanced_paren_ranges(&tokens[..end]);
    for slot in 0..candidates.len() {
        if resolved[slot] {
            continue;
        }
        let target = candidates[slot].0;
        for &(open, close) in &parens {
            if !(open < target && target < close) {
                continue;
            }
            let inner = candidates
                .iter()
                .copied()
                .filter(|&(token_index, _)| token_index > open && token_index < close)
                .collect::<Vec<_>>();
            if let Some(found) = resolve_in_slice(
                dialect,
                &tokens[open + 1..close],
                open + 1,
                &inner,
                ParseKind::Query,
            ) {
                accepted.extend(found);
                for (item, &(token_index, _)) in candidates.iter().enumerate() {
                    if token_index > open && token_index < close {
                        resolved[item] = true;
                    }
                }
                break;
            }
        }
    }
    accepted.sort_unstable();
    accepted.dedup();
    accepted
}

pub(crate) fn minus_all_spans(dialect: &dyn Dialect, sql: &str) -> Option<Vec<usize>> {
    if !may_name_minus_all(sql) {
        return None;
    }
    let tokens = Tokenizer::new(dialect, sql).tokenize_with_location().ok()?;
    let candidates = candidate_minus_all_tokens(&tokens, sql)?;
    if candidates.is_empty() {
        return None;
    }
    if let Some(starts) = resolve_in_slice(dialect, &tokens, 0, &candidates, ParseKind::Statements)
        .or_else(|| resolve_in_slice(dialect, &tokens, 0, &candidates, ParseKind::Expr))
    {
        return (!starts.is_empty()).then_some(starts);
    }
    if !is_ddl_dml_starter(&tokens) {
        return None;
    }
    let starts = resolve_ddl_dml_fallback(dialect, &tokens, &candidates);
    (!starts.is_empty()).then_some(starts)
}

fn spliced(sql: &str, starts: &[usize]) -> String {
    let mut out = String::with_capacity(sql.len() + starts.len());
    let mut cursor = 0;
    for &start in starts {
        out.push_str(&sql[cursor..start]);
        out.push_str(EXCEPT);
        cursor = start + MINUS.len();
    }
    out.push_str(&sql[cursor..]);
    out
}

#[must_use]
pub(crate) fn minus_all_with<'a>(dialect: &dyn Dialect, sql: &'a str) -> Cow<'a, str> {
    match minus_all_spans(dialect, sql) {
        Some(starts) => Cow::Owned(spliced(sql, &starts)),
        None => Cow::Borrowed(sql),
    }
}

#[must_use]
pub fn minus_all_as_except_all(parser: ParserDialect, sql: &str) -> Cow<'_, str> {
    if !matches!(parser, ParserDialect::Databricks) || !may_name_minus_all(sql) {
        return Cow::Borrowed(sql);
    }
    minus_all_with(&DatabricksDialect {}, sql)
}

pub(crate) fn minus_all_in_session_sql<'a>(ctx: &SessionContext, sql: &'a str) -> Cow<'a, str> {
    if !may_name_minus_all(sql) {
        return Cow::Borrowed(sql);
    }
    let parser = ctx.state_ref().read().config_options().sql_parser.dialect;
    minus_all_as_except_all(parser, sql)
}

#[cfg(test)]
mod tests;
