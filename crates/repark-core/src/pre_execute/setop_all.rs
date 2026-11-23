use std::borrow::Cow;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DataFusionError, NullEquality, Result};
use datafusion::config::Dialect as ParserDialect;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_nested::expr_fn::range;
use datafusion::logical_expr::{
    Expr, ExprSchemable, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder,
    Projection, col, lit, when,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect, dialect_from_str};
use datafusion::sql::sqlparser::tokenizer::{Location, Token, Tokenizer};

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
            let key = col(format!("{KEY_PREFIX}{index}"));
            let key = if key_type == field.data_type() {
                key
            } else {
                key.cast_to(field.data_type(), &unnested_schema)?
            };
            Ok(key.alias_qualified(qualifier.cloned(), field.name()))
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

pub(crate) fn minus_all_spans(dialect: &dyn Dialect, sql: &str) -> Option<Vec<usize>> {
    if !may_name_minus_all(sql) {
        return None;
    }
    let tokens = Tokenizer::new(dialect, sql).tokenize_with_location().ok()?;
    let line_starts = std::iter::once(0)
        .chain(sql.match_indices('\n').map(|(index, _)| index + 1))
        .collect::<Vec<_>>();
    let mut pending = None;
    let mut starts = Vec::new();
    for token in &tokens {
        if matches!(token.token, Token::Whitespace(_)) {
            continue;
        }
        let word = match &token.token {
            Token::Word(word) if word.quote_style.is_none() => Some(word.value.as_str()),
            _ => None,
        };
        if let (Some(start), Some(value)) = (pending, word)
            && value.eq_ignore_ascii_case("ALL")
        {
            starts.push(start);
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
                Some(start)
            }
            _ => None,
        };
    }
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
    if !may_name_minus_all(sql) {
        return Cow::Borrowed(sql);
    }
    let dialect = dialect_from_str(parser).unwrap_or_else(|| Box::new(GenericDialect {}));
    minus_all_with(dialect.as_ref(), sql)
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
