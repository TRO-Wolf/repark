//! Temporal `RANGE` window-frame conformance for the Spark door (G5b).
//!
//! Interval-bounded `RANGE` frames over a datetime order key
//! (`RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW`) already agree with Spark
//! bit-for-bit — ascending, descending, ties, NULL order keys, DATE keys, partitioned frames
//! (the G5b §0 recon transcript, `task/g5b-temporal-range-ledger.md`). This module closes the
//! **bare-number** bound over a datetime order key, which does not.
//!
//! DataFusion carries a `RANGE` frame offset as `ScalarValue::Utf8` until type coercion, then
//! casts it to the order key's *natural* type — `Interval(MonthDayNano)` for any datetime key
//! (`datafusion_optimizer::analyzer::type_coercion::extract_window_frame_target_type`). Arrow's
//! interval parser reads a unit-less `"1"` as **one month**, so `RANGE BETWEEN 1 PRECEDING`
//! over a timestamp/date key silently becomes a *one-month* window. Spark does neither:
//!
//! | Order key | `RANGE BETWEEN 1 PRECEDING …` in Spark 4.1.2 | before this module |
//! |---|---|---|
//! | `TIMESTAMP` | refuses: `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` | silently 1 MONTH |
//! | `DATE` | accepts, `1` means **one day** | silently 1 MONTH |
//!
//! Both are silent wrong answers on a query that migrates unchanged, which is the exact
//! failure class `docs/testing.md` "Divergence-class claims" exists to catch.
//!
//! **Why two mechanisms.** A window expression's *schema name* embeds its frame
//! (`datafusion_expr::expr`'s `SchemaDisplay` writes `" {window_frame}"`), so rewriting a bound
//! on the planned `LogicalPlan` renames the Window node's output field and strands every parent
//! `Expr::Column` that already references the old name. Refusing needs no rewrite, so the
//! TIMESTAMP arm reads the planned tree directly ([`classify_planned_range_frames`]); the DATE
//! arm restates the **AST** ([`rewrite_bare_range_bounds_to_days`]) and re-plans, where the
//! whole plan is rebuilt consistently.
//!
//! **Scope.** Unit-less numbers under `RANGE` over a datetime key (G5b), plus the G5b-R
//! residuals that share this seam: a **negative** interval offset over a `TIMESTAMP` key
//! (R3 — wrapping `count(*)` / debug panic) and a `DAY TO SECOND` qualified literal (R2).
//! Unquoted `INTERVAL 1 DAY` (R1) fails at first plan, before classify, and needs a pre-plan
//! rewrite in `spark_ast.rs` (not this unit's writable set). Both-bounds-`FOLLOWING` values
//! (R4) are a DataFusion range-search off-by-one at the pin. An interval over a numeric key
//! (R5) is error-class alignment. Those three stay recorded residuals.

use std::ops::ControlFlow;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::WindowFunctionParams;
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, WindowFrameBound, WindowFrameUnits,
};
use datafusion::sql::sqlparser::ast::{
    DateTimeField, Expr as AstExpr, Interval, NamedWindowExpr, Query, SetExpr, Statement,
    UnaryOperator, Value, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut,
    WindowFrame as AstWindowFrame, WindowFrameBound as AstWindowFrameBound,
    WindowFrameUnits as AstWindowFrameUnits, WindowSpec, WindowType,
};
use datafusion::sql::sqlparser::tokenizer::Span;

/// What a planned statement's `RANGE` frames ask the door to do before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeFrameVerdict {
    /// Nothing to do — no bare-number `RANGE` bound sits over a datetime order key, and no
    /// G5b-R interval restatement (negative TIMESTAMP offset / `DAY TO SECOND`) is pending.
    Unchanged,
    /// Restate the AST and re-plan: DATE-keyed unit-less bounds become `INTERVAL '<n>' DAY`,
    /// negative TIMESTAMP interval offsets become a Spark-empty frame, and `DAY TO SECOND`
    /// literals become an Arrow-accepted interval string. Only safe when no numeric-keyed
    /// unit-less bound in the same statement would be caught by that restatement.
    RestateBareBoundsAsDays,
}

/// Offset (in years) of the canonical empty `RANGE` frame used for an inverted negative
/// interval over a `TIMESTAMP` key. A pair of equal `FOLLOWING` bounds this far in the
/// future contains no Iceberg/Spark timestamp (the type's practical horizon is year 9999),
/// so DataFusion's range search returns `[length, length)` — Spark's empty frame (`count`
/// 0, `sum` NULL) without inverted-window wrapping.
const EMPTY_RANGE_OFFSET_YEARS: &str = "10000";

/// ===========================================================================================
/// Classify a freshly-planned statement's `RANGE` window frames against Spark's rules.
///
/// Refuses the TIMESTAMP arm outright (Spark's own error class) and reports whether the DATE
/// arm needs the AST restatement. Reads the plan; never rewrites it.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] carrying Spark's `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` when
/// a unit-less `RANGE` offset sits over a timestamp order key.
pub(crate) fn classify_planned_range_frames(plan: &LogicalPlan) -> Result<RangeFrameVerdict> {
    let mut date_keyed_sites = 0usize;
    let mut other_keyed_sites = 0usize;
    let mut interval_restate_sites = 0usize;
    let mut negative_timestamp_sites = 0usize;
    let mut refusal: Option<DataFusionError> = None;

    plan.apply_with_subqueries(|node| {
        let LogicalPlan::Window(window) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let input_schema = window.input.schema();
        for expr in &window.window_expr {
            expr.apply(|inner| {
                let Expr::WindowFunction(function) = inner else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                match classify_one_frame(&function.params, input_schema) {
                    Err(error) => {
                        refusal.get_or_insert(error);
                        Ok(TreeNodeRecursion::Stop)
                    }
                    Ok(Some(FrameSite::DateKeyed)) => {
                        date_keyed_sites += 1;
                        Ok(TreeNodeRecursion::Continue)
                    }
                    Ok(Some(FrameSite::OtherKeyed)) => {
                        other_keyed_sites += 1;
                        Ok(TreeNodeRecursion::Continue)
                    }
                    Ok(Some(FrameSite::IntervalRestate)) => {
                        interval_restate_sites += 1;
                        Ok(TreeNodeRecursion::Continue)
                    }
                    Ok(Some(FrameSite::NegativeTimestamp)) => {
                        negative_timestamp_sites += 1;
                        Ok(TreeNodeRecursion::Continue)
                    }
                    Ok(None) => Ok(TreeNodeRecursion::Continue),
                }
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    if let Some(error) = refusal {
        return Err(error);
    }
    // The restatement is statement-wide (the AST carries no resolved order-key type), so it is
    // only safe when no unit-less RANGE bound in the statement belongs to a numeric-keyed frame.
    // A mixed DATE/INT bare-number statement keeps its recorded divergence rather than
    // silently re-scaling the numeric frame to days. A mixed negative-TIMESTAMP / numeric-bare
    // statement cannot be restated either — refuse so R3 wrapping cannot ride the mix.
    if negative_timestamp_sites > 0 && other_keyed_sites > 0 {
        return Err(negative_range_offset_error());
    }
    if (date_keyed_sites > 0 || interval_restate_sites > 0 || negative_timestamp_sites > 0)
        && other_keyed_sites == 0
    {
        return Ok(RangeFrameVerdict::RestateBareBoundsAsDays);
    }
    Ok(RangeFrameVerdict::Unchanged)
}

/// Which order-key family / residual a `RANGE` bound was found over.
#[derive(Debug, Clone, Copy)]
enum FrameSite {
    /// `DATE` order key — Spark reads the number as days.
    DateKeyed,
    /// Any non-datetime order key — an ordinary numeric `RANGE`, already correct.
    OtherKeyed,
    /// Interval text Arrow cannot parse as-is (`DAY TO SECOND` → `"1 12:00:00 DAY"`).
    IntervalRestate,
    /// Negative interval offset over a `TIMESTAMP` key (R3 wrapping class).
    NegativeTimestamp,
}

/// ===========================================================================================
/// Classify one window function's frame; `Ok(None)` when it carries no unit-less `RANGE` bound.
/// ===========================================================================================
fn classify_one_frame(
    params: &WindowFunctionParams,
    input_schema: &datafusion::common::DFSchemaRef,
) -> Result<Option<FrameSite>> {
    if params.window_frame.units != WindowFrameUnits::Range {
        return Ok(None);
    }
    // Spark (and DataFusion) allow a value-offset RANGE frame over exactly one order key.
    let [sort] = params.order_by.as_slice() else {
        return Ok(None);
    };
    let bare_bounds: Vec<&str> = [
        &params.window_frame.start_bound,
        &params.window_frame.end_bound,
    ]
    .into_iter()
    .filter_map(bare_number_bound_text)
    .collect();
    let key_type = sort.expr.get_type(input_schema)?;
    if !bare_bounds.is_empty() {
        return match key_type {
            DataType::Date32 | DataType::Date64 => Ok(Some(FrameSite::DateKeyed)),
            DataType::Timestamp(_, _) => Err(range_frame_invalid_type_error(
                &sort.expr.to_string(),
                &params.window_frame.to_string(),
            )),
            _ => Ok(Some(FrameSite::OtherKeyed)),
        };
    }
    let interval_texts = [
        &params.window_frame.start_bound,
        &params.window_frame.end_bound,
    ]
    .into_iter()
    .filter_map(interval_bound_text);
    let mut saw_colon = false;
    let mut saw_negative = false;
    for text in interval_texts {
        if text.trim().starts_with('-') {
            saw_negative = true;
        }
        if text.contains(':') {
            saw_colon = true;
        }
    }
    match key_type {
        DataType::Timestamp(_, _) if saw_negative => Ok(Some(FrameSite::NegativeTimestamp)),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) if saw_colon => {
            Ok(Some(FrameSite::IntervalRestate))
        }
        _ => Ok(None),
    }
}

/// Utf8 offset text of a value bound when it is interval-shaped (`"1 DAY"`, `"-1 DAY"`,
/// `"1 12:00:00 DAY"`). Bare numbers and unbounded bounds yield `None`.
fn interval_bound_text(bound: &WindowFrameBound) -> Option<&str> {
    let scalar = match bound {
        WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value) => value,
        WindowFrameBound::CurrentRow => return None,
    };
    let ScalarValue::Utf8(Some(text)) = scalar else {
        return None;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.contains(char::is_alphabetic) || trimmed.starts_with('-') {
        return Some(trimmed);
    }
    None
}

/// The still-`Utf8` offset text of a value bound, when it is a unit-less number (`"1"`, `"2.5"`,
/// `"-1"`). An interval-shaped offset (`"1 DAY"`) and an unbounded bound both yield `None`.
fn bare_number_bound_text(bound: &WindowFrameBound) -> Option<&str> {
    let scalar = match bound {
        WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value) => value,
        WindowFrameBound::CurrentRow => return None,
    };
    let ScalarValue::Utf8(Some(text)) = scalar else {
        return None;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_alphabetic) {
        return None;
    }
    Some(trimmed)
}

/// ===========================================================================================
/// Spark 4.1.2's refusal for a unit-less `RANGE` offset over a timestamp order key.
///
/// Recorded verbatim from the live oracle in the G5b §0 recon (`spark.sql.ansi.enabled=true`,
/// `local[2]`); only the window-spec rendering is ours, because Spark quotes its own resolved
/// plan text there and we have no equivalent to quote.
/// ===========================================================================================
fn range_frame_invalid_type_error(order_key: &str, window_frame: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE] Cannot resolve \"(ORDER BY {order_key} \
         {window_frame})\" due to data type mismatch: The data type \"TIMESTAMP\" used in the \
         order specification does not support the data type \"INT\" which is used in the range \
         frame. SQLSTATE: 42K09"
    ))
}

/// ===========================================================================================
/// Refusal when a negative TIMESTAMP `RANGE` offset shares a statement with a numeric
/// unit-less bound, so the statement-wide AST restatement cannot run.
///
/// The wrapping class (release `count(*)` = -1 / debug panic) must not survive. Mixed
/// statements take this loud out instead of executing the inverted search.
/// ===========================================================================================
fn negative_range_offset_error() -> DataFusionError {
    DataFusionError::Plan(
        "[UNSUPPORTED.NEGATIVE_RANGE_OFFSET] RANGE frame with a negative interval offset \
         over a TIMESTAMP order key cannot be restated in a statement that also carries a \
         unit-less RANGE bound over a numeric key (G5b-R3). Split the statement, or write a \
         non-negative interval. Spark 4.1.2 returns an empty frame for the inverted spelling; \
         executing it here wraps the sliding-window count. SQLSTATE: 0A000"
            .to_string(),
    )
}

/// ===========================================================================================
/// Restate `RANGE` frame bounds the DATE / G5b-R arms need, then the caller re-plans.
///
/// Applied only when [`classify_planned_range_frames`] returned
/// [`RangeFrameVerdict::RestateBareBoundsAsDays`]:
/// - unit-less numbers become `INTERVAL '<n>' DAY` (DATE key, Spark days not Arrow months);
/// - a negative interval over a `TIMESTAMP` key is sign-normalized, then rewritten to a
///   canonical empty frame when the normalized bounds are inverted (R3);
/// - `DAY TO SECOND` literals become an Arrow-accepted interval string (R2).
///
/// Mirrors [`crate::spark_ast::apply_spark_order_by_defaults`]'s traversal: `post_visit_expr`
/// reaches inline `OVER (…)`, `post_visit_query` the named `WINDOW` clauses.
/// ===========================================================================================
pub(crate) fn rewrite_bare_range_bounds_to_days(statement: &mut Statement) {
    let mut visitor = BareRangeBoundsAsDays;
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = VisitMut::visit(statement, &mut visitor);
}

/// The visitor behind [`rewrite_bare_range_bounds_to_days`].
struct BareRangeBoundsAsDays;

impl VisitorMut for BareRangeBoundsAsDays {
    type Break = std::convert::Infallible;

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        restate_set_expr(&mut query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut AstExpr) -> ControlFlow<Self::Break> {
        if let AstExpr::Function(function) = expr
            && let Some(WindowType::WindowSpec(spec)) = &mut function.over
        {
            restate_window_spec(spec);
        }
        ControlFlow::Continue(())
    }
}

/// Reach the named `WINDOW w AS (…)` clauses of every `SELECT` in a query body. Nested queries
/// get their own `post_visit_query`; set operations are walked to both sides.
fn restate_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            for window in &mut select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &mut window.1 {
                    restate_window_spec(spec);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            restate_set_expr(left);
            restate_set_expr(right);
        }
        _ => {}
    }
}

/// Restate both bounds of one window spec's `RANGE` frame (no-op for `ROWS` / `GROUPS`).
fn restate_window_spec(spec: &mut WindowSpec) {
    let Some(frame) = &mut spec.window_frame else {
        return;
    };
    if frame.units != AstWindowFrameUnits::Range {
        return;
    }
    let flipped_a_negative = normalize_negative_interval_bounds(frame);
    if flipped_a_negative && frame_is_kind_inverted(frame) {
        write_empty_range_frame(frame);
        return;
    }
    restate_bound(&mut frame.start_bound);
    if let Some(end_bound) = &mut frame.end_bound {
        restate_bound(end_bound);
    }
}

/// Flip `INTERVAL '-n' UNIT PRECEDING` ↔ `INTERVAL 'n' UNIT FOLLOWING` (and the inverse).
///
/// DataFusion's start≤end check looks at bound *kind*, not the sign inside the interval
/// scalar, so a negative PRECEDING is not seen as FOLLOWING and the sliding window wraps.
/// Returns whether any bound was flipped.
fn normalize_negative_interval_bounds(frame: &mut AstWindowFrame) -> bool {
    let start_flipped = flip_negative_interval_bound(&mut frame.start_bound);
    let end_flipped = frame
        .end_bound
        .as_mut()
        .is_some_and(flip_negative_interval_bound);
    start_flipped || end_flipped
}

/// Flip one negative interval bound's sign and PRECEDING/FOLLOWING kind. `true` if flipped.
fn flip_negative_interval_bound(bound: &mut AstWindowFrameBound) -> bool {
    {
        let Some(interval) = bound_interval_mut(bound) else {
            return false;
        };
        if !interval_value_is_negative(&interval.value) {
            return false;
        }
        if !strip_interval_value_sign(&mut interval.value) {
            return false;
        }
    }
    let taken = std::mem::replace(bound, AstWindowFrameBound::CurrentRow);
    *bound = match taken {
        AstWindowFrameBound::Preceding(offset) => AstWindowFrameBound::Following(offset),
        AstWindowFrameBound::Following(offset) => AstWindowFrameBound::Preceding(offset),
        AstWindowFrameBound::CurrentRow => AstWindowFrameBound::CurrentRow,
    };
    true
}

/// True when start's kind sits after end's kind (FOLLOWING … CURRENT ROW, etc.).
fn frame_is_kind_inverted(frame: &AstWindowFrame) -> bool {
    let start = bound_kind_rank(&frame.start_bound);
    let end = frame
        .end_bound
        .as_ref()
        .map_or(BoundKindRank::Current, bound_kind_rank);
    start > end
}

/// Order-key rank of a bound kind, ignoring the offset magnitude.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BoundKindRank {
    UnboundedPreceding,
    Preceding,
    Current,
    Following,
    UnboundedFollowing,
}

fn bound_kind_rank(bound: &AstWindowFrameBound) -> BoundKindRank {
    match bound {
        AstWindowFrameBound::CurrentRow => BoundKindRank::Current,
        AstWindowFrameBound::Preceding(None) => BoundKindRank::UnboundedPreceding,
        AstWindowFrameBound::Following(None) => BoundKindRank::UnboundedFollowing,
        AstWindowFrameBound::Preceding(Some(_)) => BoundKindRank::Preceding,
        AstWindowFrameBound::Following(Some(_)) => BoundKindRank::Following,
    }
}

/// `RANGE BETWEEN INTERVAL '10000' YEAR FOLLOWING AND INTERVAL '10000' YEAR FOLLOWING`.
fn write_empty_range_frame(frame: &mut AstWindowFrame) {
    let empty = interval_years(EMPTY_RANGE_OFFSET_YEARS);
    frame.start_bound = AstWindowFrameBound::Following(Some(Box::new(empty.clone())));
    frame.end_bound = Some(AstWindowFrameBound::Following(Some(Box::new(empty))));
}

fn interval_years(years: &str) -> AstExpr {
    AstExpr::Interval(Interval {
        value: Box::new(AstExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(years.to_string()),
            span: Span::empty(),
        })),
        leading_field: Some(DateTimeField::Year),
        leading_precision: None,
        last_field: None,
        fractional_seconds_precision: None,
    })
}

/// `<n> PRECEDING` → `INTERVAL '<n>' DAY PRECEDING`; `DAY TO SECOND` → Arrow-accepted text.
fn restate_bound(bound: &mut AstWindowFrameBound) {
    let offset = match bound {
        AstWindowFrameBound::Preceding(value) | AstWindowFrameBound::Following(value) => value,
        AstWindowFrameBound::CurrentRow => return,
    };
    let Some(expr) = offset else {
        return;
    };
    if let AstExpr::Value(ValueWithSpan {
        value: Value::Number(number, false),
        span,
    }) = expr.as_ref()
    {
        let span = *span;
        let number = number.clone();
        **expr = AstExpr::Interval(Interval {
            value: Box::new(AstExpr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(number),
                span,
            })),
            leading_field: Some(DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        });
        return;
    }
    if let AstExpr::Interval(interval) = expr.as_mut()
        && interval.last_field.is_some()
    {
        restate_day_to_second_interval(interval);
    }
}

/// `INTERVAL '1 12:00:00' DAY TO SECOND` → `INTERVAL '1 days 12 hours 0 minutes 0 seconds'`.
///
/// DataFusion concatenates only the leading field (`"1 12:00:00 DAY"`), which Arrow rejects.
/// Clearing `leading_field` / `last_field` and spelling the parts out is what Arrow's
/// interval parser accepts (G5b-R2 recon).
fn restate_day_to_second_interval(interval: &mut Interval) {
    let Some(literal) = interval_quoted_literal(&interval.value) else {
        return;
    };
    let Some(restated) = restated_day_to_second_literal(literal) else {
        return;
    };
    let span = interval_value_span(&interval.value);
    *interval.value = AstExpr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(restated),
        span,
    });
    interval.leading_field = None;
    interval.last_field = None;
    interval.leading_precision = None;
    interval.fractional_seconds_precision = None;
}

/// `'D H:M:S'` / `'D H:M:S.frac'` → `'D days H hours M minutes S seconds'`.
fn restated_day_to_second_literal(literal: &str) -> Option<String> {
    let trimmed = literal.trim();
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let (days_text, time_text) = unsigned.split_once(' ')?;
    let days: u64 = days_text.parse().ok()?;
    let mut time_parts = time_text.split(':');
    let hours: u64 = time_parts.next()?.parse().ok()?;
    let minutes: u64 = time_parts.next()?.parse().ok()?;
    let seconds_text = time_parts.next().unwrap_or("0");
    let seconds_whole = seconds_text.split('.').next().unwrap_or("0");
    let seconds: u64 = seconds_whole.parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }
    Some(format!(
        "{days} days {hours} hours {minutes} minutes {seconds} seconds"
    ))
}

fn bound_interval(bound: &AstWindowFrameBound) -> Option<&Interval> {
    let offset = match bound {
        AstWindowFrameBound::Preceding(value) | AstWindowFrameBound::Following(value) => {
            value.as_ref()?
        }
        AstWindowFrameBound::CurrentRow => return None,
    };
    match offset.as_ref() {
        AstExpr::Interval(interval) => Some(interval),
        _ => None,
    }
}

fn bound_interval_mut(bound: &mut AstWindowFrameBound) -> Option<&mut Interval> {
    let offset = match bound {
        AstWindowFrameBound::Preceding(value) | AstWindowFrameBound::Following(value) => {
            value.as_mut()?
        }
        AstWindowFrameBound::CurrentRow => return None,
    };
    match offset.as_mut() {
        AstExpr::Interval(interval) => Some(interval),
        _ => None,
    }
}

fn interval_value_is_negative(value: &AstExpr) -> bool {
    match value {
        AstExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::Number(text, _),
            ..
        }) => text.trim().starts_with('-'),
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            ..
        } => true,
        _ => false,
    }
}

fn strip_interval_value_sign(value: &mut AstExpr) -> bool {
    match value {
        AstExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::Number(text, _),
            ..
        }) => {
            let trimmed = text.trim();
            let Some(unsigned) = trimmed.strip_prefix('-') else {
                return false;
            };
            *text = unsigned.to_string();
            true
        }
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            *value = (**expr).clone();
            true
        }
        _ => false,
    }
}

fn interval_quoted_literal(value: &AstExpr) -> Option<&str> {
    match value {
        AstExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text),
            ..
        }) => Some(text.as_str()),
        _ => None,
    }
}

fn interval_value_span(value: &AstExpr) -> Span {
    match value {
        AstExpr::Value(ValueWithSpan { span, .. }) => *span,
        _ => Span::empty(),
    }
}

/// True when a `RANGE` bound is a residual interval this module restates (negative or
/// field-qualified) or a unit-less number (the DATE arm).
fn bound_needs_conform(bound: &AstWindowFrameBound) -> bool {
    if bound_is_bare_number(bound) {
        return true;
    }
    let Some(interval) = bound_interval(bound) else {
        return false;
    };
    interval.last_field.is_some() || interval_value_is_negative(&interval.value)
}

/// The AST frame shape a caller can cheaply test for before paying for a second planning pass.
/// True when any `RANGE` frame in the statement carries a unit-less numeric bound, a
/// negative interval, or a field-qualified interval (`DAY TO SECOND`).
pub(crate) fn statement_has_bare_range_bound(statement: &Statement) -> bool {
    let mut visitor = BareRangeBoundProbe { found: false };
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = Visit::visit(statement, &mut visitor);
    visitor.found
}

/// The visitor behind [`statement_has_bare_range_bound`].
struct BareRangeBoundProbe {
    found: bool,
}

impl Visitor for BareRangeBoundProbe {
    type Break = std::convert::Infallible;

    fn post_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        probe_set_expr(&mut self.found, &query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &AstExpr) -> ControlFlow<Self::Break> {
        if let AstExpr::Function(function) = expr
            && let Some(WindowType::WindowSpec(spec)) = &function.over
        {
            probe_window_spec(&mut self.found, spec);
        }
        ControlFlow::Continue(())
    }
}

/// Named-`WINDOW`-clause half of [`statement_has_bare_range_bound`].
fn probe_set_expr(found: &mut bool, body: &SetExpr) {
    match body {
        SetExpr::Select(select) => {
            for window in &select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &window.1 {
                    probe_window_spec(found, spec);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            probe_set_expr(found, left);
            probe_set_expr(found, right);
        }
        _ => {}
    }
}

/// True-set one window spec's contribution to the cheap AST probe.
fn probe_window_spec(found: &mut bool, spec: &WindowSpec) {
    let Some(frame) = &spec.window_frame else {
        return;
    };
    if frame.units != AstWindowFrameUnits::Range {
        return;
    }
    if bound_needs_conform(&frame.start_bound) {
        *found = true;
    }
    if frame.end_bound.as_ref().is_some_and(bound_needs_conform) {
        *found = true;
    }
}

/// True when an AST frame bound is a unit-less numeric literal.
fn bound_is_bare_number(bound: &AstWindowFrameBound) -> bool {
    let offset = match bound {
        AstWindowFrameBound::Preceding(value) | AstWindowFrameBound::Following(value) => value,
        AstWindowFrameBound::CurrentRow => return false,
    };
    offset.as_ref().is_some_and(|expr| {
        matches!(
            expr.as_ref(),
            AstExpr::Value(ValueWithSpan {
                value: Value::Number(_, false),
                ..
            })
        )
    })
}
