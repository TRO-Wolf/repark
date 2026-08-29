//! Temporal `RANGE` window-frame conformance for the Spark door.
//!
//! DataFusion reads a unit-less datetime bound as an interval whose bare `1` means one month.
//! Spark refuses that bound for `TIMESTAMP` and reads it as one day for `DATE`. The timestamp path
//! refuses on the planned tree; the date path rewrites the AST and re-plans so parent column names
//! remain consistent. Negative or inverted timestamp intervals become Spark-empty frames, while
//! interval bounds over numeric keys restate to their numeric magnitude. Mixed datetime and
//! numeric statements stay on the first plan because a statement-wide rewrite cannot separate them.

use std::collections::HashSet;
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
    /// G5b-R interval restatement (negative TIMESTAMP offset / `DAY TO SECOND` / numeric
    /// key) is pending.
    Unchanged,
    /// Restate the AST and re-plan: DATE-keyed unit-less bounds become `INTERVAL '<n>' DAY`,
    /// negative / value-inverted TIMESTAMP interval offsets become a Spark-empty frame
    /// (`FILTER (WHERE false)` over a current-row frame), and `DAY TO SECOND` literals
    /// become an Arrow-accepted interval string. Only safe when no numeric-keyed unit-less
    /// bound in the same statement would be caught by that restatement.
    RestateBareBoundsAsDays,
    /// Restate every `RANGE` `INTERVAL 'n' UNIT` bound to the unit-less number `n`
    /// (Spark 4.1.2 over a numeric order key: unit ignored). Only safe when the
    /// statement has no datetime-keyed interval frame that must stay an interval.
    RestateIntervalBoundsAsNumeric,
}

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
    let mut datetime_interval_sites = 0usize;
    let mut numeric_interval_sites = 0usize;
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
                    Ok(Some(FrameSite::DatetimeInterval)) => {
                        datetime_interval_sites += 1;
                        Ok(TreeNodeRecursion::Continue)
                    }
                    Ok(Some(FrameSite::NumericKeyedInterval)) => {
                        numeric_interval_sites += 1;
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
    // silently re-scaling the numeric frame to days. A mixed inverted-TIMESTAMP / numeric-bare
    // statement cannot be restated either — refuse so R3 wrapping cannot ride the mix.
    if negative_timestamp_sites > 0 && other_keyed_sites > 0 {
        return Err(negative_range_offset_error());
    }
    if numeric_interval_sites > 0
        && date_keyed_sites == 0
        && interval_restate_sites == 0
        && negative_timestamp_sites == 0
        && datetime_interval_sites == 0
    {
        return Ok(RangeFrameVerdict::RestateIntervalBoundsAsNumeric);
    }
    if (date_keyed_sites > 0 || interval_restate_sites > 0 || negative_timestamp_sites > 0)
        && other_keyed_sites == 0
        && numeric_interval_sites == 0
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
    /// Negative or value-inverted interval over a `TIMESTAMP` key (R3 wrapping class).
    NegativeTimestamp,
    /// Working-path interval over a datetime key — must not be restated to a unit-less `n`.
    DatetimeInterval,
    /// Interval bound over a numeric order key (R5 — Spark reads the leading `n`, unit ignored).
    NumericKeyedInterval,
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
    let inverted = planned_frame_is_value_inverted(&params.window_frame);
    match key_type {
        DataType::Timestamp(_, _)
            if inverted && planned_frame_is_same_kind_magnitude_invert(&params.window_frame) =>
        {
            // Spark 4.1.2 refuses same-kind magnitude invert (`2 FOLLOWING AND 1 FOLLOWING`,
            // including after `-2 PRECEDING AND -1 PRECEDING` flips). Empty is the
            // CURRENT-ROW kind-invert class only. Never execute: wrapping is the DF defect.
            Err(window_frame_wrong_comparison_error(
                &params.window_frame.to_string(),
            ))
        }
        DataType::Timestamp(_, _) if inverted || saw_negative => {
            Ok(Some(FrameSite::NegativeTimestamp))
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) if saw_colon => {
            Ok(Some(FrameSite::IntervalRestate))
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            if planned_frame_has_interval_bound(&params.window_frame) =>
        {
            Ok(Some(FrameSite::DatetimeInterval))
        }
        _ if is_numeric_order_key(&key_type)
            && planned_frame_has_interval_bound(&params.window_frame) =>
        {
            Ok(Some(FrameSite::NumericKeyedInterval))
        }
        _ => Ok(None),
    }
}

fn is_numeric_order_key(key_type: &DataType) -> bool {
    matches!(
        key_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn planned_frame_has_interval_bound(frame: &datafusion::logical_expr::WindowFrame) -> bool {
    bound_is_interval_shaped(&frame.start_bound) || bound_is_interval_shaped(&frame.end_bound)
}

fn bound_is_interval_shaped(bound: &WindowFrameBound) -> bool {
    if interval_bound_text(bound).is_some() {
        return true;
    }
    let scalar = match bound {
        WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value) => value,
        WindowFrameBound::CurrentRow => return false,
    };
    matches!(
        scalar,
        ScalarValue::IntervalMonthDayNano(_)
            | ScalarValue::IntervalYearMonth(_)
            | ScalarValue::IntervalDayTime(_)
    )
}

/// True when, after sign-normalizing each bound, the start sits strictly after the end.
///
/// Kind invert (`FOLLOWING` then `CURRENT ROW`) and same-kind magnitude invert
/// (`2 FOLLOWING AND 1 FOLLOWING`, including after `-2 PRECEDING AND -1 PRECEDING` flips)
/// are both invert. DataFusion's start≤end check is kind-only, so the magnitude case wraps
/// (`count(*)` = -1) unless we restated it first.
fn planned_frame_is_value_inverted(frame: &datafusion::logical_expr::WindowFrame) -> bool {
    if frame.units != WindowFrameUnits::Range {
        return false;
    }
    matches!(
        position_is_strictly_after(
            planned_bound_position(&frame.start_bound),
            planned_bound_position(&frame.end_bound),
        ),
        Some(true)
    )
}

/// Same-kind magnitude invert: both bounds are finite offsets and start sits after end
/// (`2 FOLLOWING AND 1 FOLLOWING`). Distinct from kind invert (`FOLLOWING` vs `CURRENT ROW`).
fn planned_frame_is_same_kind_magnitude_invert(
    frame: &datafusion::logical_expr::WindowFrame,
) -> bool {
    matches!(
        (
            planned_bound_position(&frame.start_bound),
            planned_bound_position(&frame.end_bound),
        ),
        (SignedBound::Offset(_), SignedBound::Offset(_))
    ) && planned_frame_is_value_inverted(frame)
}

fn planned_bound_position(bound: &WindowFrameBound) -> SignedBound {
    match bound {
        WindowFrameBound::CurrentRow => SignedBound::Current,
        WindowFrameBound::Preceding(value) if value.is_null() => SignedBound::UnboundedPreceding,
        WindowFrameBound::Following(value) if value.is_null() => SignedBound::UnboundedFollowing,
        WindowFrameBound::Preceding(value) => match scalar_offset_axis(value) {
            Some(axis) => SignedBound::Offset(negate_axis(axis)),
            None => SignedBound::Unknown,
        },
        WindowFrameBound::Following(value) => match scalar_offset_axis(value) {
            Some(axis) => SignedBound::Offset(axis),
            None => SignedBound::Unknown,
        },
    }
}

fn scalar_offset_axis(value: &ScalarValue) -> Option<Axis> {
    match value {
        ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => {
            parse_interval_axis(text)
        }
        _ => None,
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

/// Spark 4.1.2's refusal when a RANGE frame's lower bound sits after its upper bound
/// (same-kind magnitude invert after sign-normalize). Recorded live under MARKER=y1-g5br-fix.
fn window_frame_wrong_comparison_error(window_frame: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON] Cannot resolve \
         \"{window_frame}\" due to data type mismatch: The lower bound of a window frame \
         must be less than or equal to the upper bound. SQLSTATE: 42K09"
    ))
}

/// ===========================================================================================
/// Restate `RANGE` frame bounds the DATE / G5b-R arms need, then the caller re-plans.
///
/// Restate DATE bounds as day intervals and preserve the surrounding AST so window schema
/// references remain valid when the caller re-plans.
/// ===========================================================================================
pub(crate) fn rewrite_bare_range_bounds_to_days(statement: &mut Statement) {
    let mut visitor = BareRangeBoundsAsDays {
        inverted_named_windows: HashSet::new(),
    };
    // The visitor's Break type is uninhabited — traversal always completes.
    let _ = VisitMut::visit(statement, &mut visitor);
}

/// ===========================================================================================
/// Quote `INTERVAL 1 DAY` (Number) as `INTERVAL '1' DAY` in `RANGE` frame bounds (R1).
///
/// DataFusion's `convert_frame_bound_to_scalar_value` accepts only `SingleQuotedString`
/// inside an interval bound. Spark 4.1.2 accepts the unquoted spelling and answers the
/// same table as the quoted form. Called from `spark_ast` *before* first plan, and again
/// on every restatement re-parse (the rewrite starts from the original SQL text).
/// ===========================================================================================
pub(crate) fn quote_unquoted_interval_range_bounds(statement: &mut Statement) {
    let mut visitor = QuoteUnquotedIntervalBounds;
    let _ = VisitMut::visit(statement, &mut visitor);
}

struct QuoteUnquotedIntervalBounds;

impl VisitorMut for QuoteUnquotedIntervalBounds {
    type Break = std::convert::Infallible;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        quote_unquoted_in_set_expr(&mut query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut AstExpr) -> ControlFlow<Self::Break> {
        if let AstExpr::Function(function) = expr
            && let Some(WindowType::WindowSpec(spec)) = &mut function.over
        {
            quote_unquoted_in_window_spec(spec);
        }
        ControlFlow::Continue(())
    }
}

fn quote_unquoted_in_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            for window in &mut select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &mut window.1 {
                    quote_unquoted_in_window_spec(spec);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            quote_unquoted_in_set_expr(left);
            quote_unquoted_in_set_expr(right);
        }
        _ => {}
    }
}

fn quote_unquoted_in_window_spec(spec: &mut WindowSpec) {
    let Some(frame) = &mut spec.window_frame else {
        return;
    };
    if frame.units != AstWindowFrameUnits::Range {
        return;
    }
    quote_unquoted_in_bound(&mut frame.start_bound);
    if let Some(end_bound) = &mut frame.end_bound {
        quote_unquoted_in_bound(end_bound);
    }
}

fn quote_unquoted_in_bound(bound: &mut AstWindowFrameBound) {
    let Some(interval) = bound_interval_mut(bound) else {
        return;
    };
    quote_unquoted_interval_value(&mut interval.value);
}

fn quote_unquoted_interval_value(value: &mut AstExpr) {
    match value {
        AstExpr::Value(ValueWithSpan {
            value: Value::Number(text, _),
            span,
        }) => {
            let quoted = text.clone();
            let span = *span;
            *value = AstExpr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(quoted),
                span,
            });
        }
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            if let AstExpr::Value(ValueWithSpan {
                value: Value::Number(text, _),
                span,
            }) = expr.as_ref()
            {
                let quoted = format!("-{text}");
                let span = *span;
                *value = AstExpr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(quoted),
                    span,
                });
            }
        }
        _ => {}
    }
}

/// ===========================================================================================
/// Restate `RANGE` `INTERVAL 'n' UNIT` bounds to the unit-less number `n` (R5).
///
/// Applied only when [`classify_planned_range_frames`] returned
/// [`RangeFrameVerdict::RestateIntervalBoundsAsNumeric`]: every interval site in the
/// statement sits over a numeric order key, so statement-wide conversion matches Spark
/// (unit ignored). The caller re-plans.
/// ===========================================================================================
pub(crate) fn rewrite_interval_range_bounds_to_numeric(statement: &mut Statement) {
    let mut visitor = IntervalBoundsAsNumeric;
    let _ = VisitMut::visit(statement, &mut visitor);
}

struct IntervalBoundsAsNumeric;

impl VisitorMut for IntervalBoundsAsNumeric {
    type Break = std::convert::Infallible;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        numeric_restate_set_expr(&mut query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut AstExpr) -> ControlFlow<Self::Break> {
        if let AstExpr::Function(function) = expr
            && let Some(WindowType::WindowSpec(spec)) = &mut function.over
        {
            numeric_restate_window_spec(spec);
        }
        ControlFlow::Continue(())
    }
}

fn numeric_restate_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            for window in &mut select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &mut window.1 {
                    numeric_restate_window_spec(spec);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            numeric_restate_set_expr(left);
            numeric_restate_set_expr(right);
        }
        _ => {}
    }
}

fn numeric_restate_window_spec(spec: &mut WindowSpec) {
    let Some(frame) = &mut spec.window_frame else {
        return;
    };
    if frame.units != AstWindowFrameUnits::Range {
        return;
    }
    restate_interval_bound_as_numeric(&mut frame.start_bound);
    if let Some(end_bound) = &mut frame.end_bound {
        restate_interval_bound_as_numeric(end_bound);
    }
}

fn restate_interval_bound_as_numeric(bound: &mut AstWindowFrameBound) {
    let offset = match bound {
        AstWindowFrameBound::Preceding(value) | AstWindowFrameBound::Following(value) => value,
        AstWindowFrameBound::CurrentRow => return,
    };
    let Some(expr) = offset else {
        return;
    };
    let AstExpr::Interval(interval) = expr.as_ref() else {
        return;
    };
    let Some(number) = interval_leading_number_text(interval) else {
        return;
    };
    let span = interval_value_span(&interval.value);
    **expr = AstExpr::Value(ValueWithSpan {
        value: Value::Number(number, false),
        span,
    });
}

/// Leading numeric magnitude of `INTERVAL 'n' UNIT` / `INTERVAL 'n …'` / unquoted `n`.
fn interval_leading_number_text(interval: &Interval) -> Option<String> {
    let literal = interval_signed_literal(&interval.value)?;
    let (negative, rest) = strip_leading_minus(literal.trim());
    let number_end = rest
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .unwrap_or(rest.len());
    if number_end == 0 {
        return None;
    }
    let number = rest[..number_end].trim();
    if number.is_empty() || number == "." {
        return None;
    }
    Some(if negative {
        format!("-{number}")
    } else {
        number.to_string()
    })
}

/// The visitor behind [`rewrite_bare_range_bounds_to_days`].
struct BareRangeBoundsAsDays {
    /// Named `WINDOW w AS (…)` specs that were inverted and restated to a current-row
    /// frame. Each `OVER w` reference must also get `FILTER (WHERE false)` so the
    /// restated spec is Spark-empty, not a peer-group window.
    inverted_named_windows: HashSet<String>,
}

impl VisitorMut for BareRangeBoundsAsDays {
    type Break = std::convert::Infallible;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // Named windows must be classified before the SELECT-list functions that
        // reference them (`OVER w`) are visited.
        restate_set_expr(&mut query.body, &mut self.inverted_named_windows);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut AstExpr) -> ControlFlow<Self::Break> {
        let AstExpr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        match &mut function.over {
            Some(WindowType::WindowSpec(spec)) => {
                if restate_window_spec(spec) {
                    apply_false_filter(function);
                }
            }
            Some(WindowType::NamedWindow(name))
                if self.inverted_named_windows.contains(&name.value) =>
            {
                apply_false_filter(function);
            }
            Some(WindowType::NamedWindow(_)) | None => {}
        }
        ControlFlow::Continue(())
    }
}

/// Reach the named `WINDOW w AS (…)` clauses of every `SELECT` in a query body. Nested
/// queries get their own `pre_visit_query`; set operations are walked to both sides.
fn restate_set_expr(body: &mut SetExpr, inverted_named_windows: &mut HashSet<String>) {
    match body {
        SetExpr::Select(select) => {
            for window in &mut select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &mut window.1
                    && restate_window_spec(spec)
                {
                    inverted_named_windows.insert(window.0.value.clone());
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            restate_set_expr(left, inverted_named_windows);
            restate_set_expr(right, inverted_named_windows);
        }
        _ => {}
    }
}

/// Restate both bounds of one window spec's `RANGE` frame (no-op for `ROWS` / `GROUPS`).
///
/// Returns `true` when the frame is value-inverted after sign-normalize and was rewritten
/// to a current-row frame; the caller must attach `FILTER (WHERE false)` so the window is
/// Spark-empty rather than a peer group.
fn restate_window_spec(spec: &mut WindowSpec) -> bool {
    let Some(frame) = &mut spec.window_frame else {
        return false;
    };
    if frame.units != AstWindowFrameUnits::Range {
        return false;
    }
    normalize_negative_interval_bounds(frame);
    restate_bound(&mut frame.start_bound);
    if let Some(end_bound) = &mut frame.end_bound {
        restate_bound(end_bound);
    }
    if frame_is_value_inverted(frame) {
        write_current_row_range_frame(frame);
        return true;
    }
    false
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

/// True when, after reading each bound's signed position, start sits strictly after end.
///
/// Replaces a kind-only check: `-2 PRECEDING AND -1 PRECEDING` sign-normalizes to
/// `2 FOLLOWING AND 1 FOLLOWING` (same kind, magnitude inverted). Kind-only would miss
/// that and DataFusion would wrap.
fn frame_is_value_inverted(frame: &AstWindowFrame) -> bool {
    let start = ast_bound_position(&frame.start_bound);
    let end = frame
        .end_bound
        .as_ref()
        .map_or(SignedBound::Current, ast_bound_position);
    matches!(position_is_strictly_after(start, end), Some(true))
}

/// `RANGE BETWEEN CURRENT ROW AND CURRENT ROW` — a valid (non-inverted) frame. Combined
/// with `FILTER (WHERE false)` this is Spark's empty window without a far-future YEAR pair.
fn write_current_row_range_frame(frame: &mut AstWindowFrame) {
    frame.start_bound = AstWindowFrameBound::CurrentRow;
    frame.end_bound = Some(AstWindowFrameBound::CurrentRow);
}

fn apply_false_filter(function: &mut datafusion::sql::sqlparser::ast::Function) {
    function.filter = Some(Box::new(AstExpr::Value(ValueWithSpan {
        value: Value::Boolean(false),
        span: Span::empty(),
    })));
}

/// Signed bound positions (kind + magnitude) after reading the sign inside an interval.
const NANOS_PER_SECOND: i128 = 1_000_000_000;
const SECONDS_PER_MINUTE: i128 = 60;
const SECONDS_PER_HOUR: i128 = 3_600;
const SECONDS_PER_DAY: i128 = 86_400;

/// A comparable RANGE offset. Distinct axes never compare (1 MONTH vs 1 DAY is unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Axis {
    Months(i128),
    Nanos(i128),
    /// Unit-less number (DATE-arm bare bound before restatement).
    Unitless(i128),
}

/// Position of one frame bound on the order-key line (current row = 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignedBound {
    UnboundedPreceding,
    Offset(Axis),
    Current,
    UnboundedFollowing,
    /// Unparsable offset — do not claim invert (and do not empty a valid frame).
    Unknown,
}

fn negate_axis(axis: Axis) -> Axis {
    match axis {
        Axis::Months(value) => Axis::Months(-value),
        Axis::Nanos(value) => Axis::Nanos(-value),
        Axis::Unitless(value) => Axis::Unitless(-value),
    }
}

fn apply_axis_sign(negative: bool, axis: Axis) -> Axis {
    if negative { negate_axis(axis) } else { axis }
}

fn zero_of(axis: Axis) -> Axis {
    match axis {
        Axis::Months(_) => Axis::Months(0),
        Axis::Nanos(_) => Axis::Nanos(0),
        Axis::Unitless(_) => Axis::Unitless(0),
    }
}

fn axis_is_greater(left: Axis, right: Axis) -> Option<bool> {
    match (left, right) {
        (Axis::Months(left_value), Axis::Months(right_value))
        | (Axis::Nanos(left_value), Axis::Nanos(right_value))
        | (Axis::Unitless(left_value), Axis::Unitless(right_value)) => {
            Some(left_value > right_value)
        }
        _ => None,
    }
}

fn position_is_strictly_after(start: SignedBound, end: SignedBound) -> Option<bool> {
    match (start, end) {
        (SignedBound::Unknown, _) | (_, SignedBound::Unknown) => None,
        (SignedBound::UnboundedPreceding, _)
        | (_, SignedBound::UnboundedFollowing)
        | (SignedBound::Current, SignedBound::Current) => Some(false),
        (SignedBound::UnboundedFollowing, _) | (_, SignedBound::UnboundedPreceding) => Some(true),
        (SignedBound::Offset(start_axis), SignedBound::Offset(end_axis)) => {
            axis_is_greater(start_axis, end_axis)
        }
        (SignedBound::Offset(start_axis), SignedBound::Current) => {
            axis_is_greater(start_axis, zero_of(start_axis))
        }
        (SignedBound::Current, SignedBound::Offset(end_axis)) => {
            axis_is_greater(zero_of(end_axis), end_axis)
        }
    }
}

fn ast_bound_position(bound: &AstWindowFrameBound) -> SignedBound {
    match bound {
        AstWindowFrameBound::CurrentRow => SignedBound::Current,
        AstWindowFrameBound::Preceding(None) => SignedBound::UnboundedPreceding,
        AstWindowFrameBound::Following(None) => SignedBound::UnboundedFollowing,
        AstWindowFrameBound::Preceding(Some(expr)) => match ast_offset_axis(expr) {
            Some(axis) => SignedBound::Offset(negate_axis(axis)),
            None => SignedBound::Unknown,
        },
        AstWindowFrameBound::Following(Some(expr)) => match ast_offset_axis(expr) {
            Some(axis) => SignedBound::Offset(axis),
            None => SignedBound::Unknown,
        },
    }
}

fn ast_offset_axis(expr: &AstExpr) -> Option<Axis> {
    match expr {
        AstExpr::Interval(interval) => ast_interval_axis(interval),
        AstExpr::Value(ValueWithSpan {
            value: Value::Number(text, _) | Value::SingleQuotedString(text),
            ..
        }) => parse_interval_axis(text),
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => Some(negate_axis(ast_offset_axis(expr)?)),
        _ => None,
    }
}

fn ast_interval_axis(interval: &Interval) -> Option<Axis> {
    let literal = interval_signed_literal(&interval.value)?;
    if interval.last_field.is_some() {
        return parse_interval_axis(&literal);
    }
    if let Some(field) = &interval.leading_field {
        let (negative, number_text) = strip_leading_minus(literal.trim());
        let count: i128 = number_text.parse().ok()?;
        return Some(apply_axis_sign(
            negative,
            axis_from_datetime_field(field, count)?,
        ));
    }
    parse_interval_axis(&literal)
}

fn interval_signed_literal(value: &AstExpr) -> Option<String> {
    match value {
        AstExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::Number(text, _),
            ..
        }) => Some(text.clone()),
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => Some(format!("-{}", interval_signed_literal(expr)?)),
        _ => None,
    }
}

fn axis_from_datetime_field(field: &DateTimeField, count: i128) -> Option<Axis> {
    match field {
        DateTimeField::Year | DateTimeField::Years => Some(Axis::Months(count.checked_mul(12)?)),
        DateTimeField::Quarter => Some(Axis::Months(count.checked_mul(3)?)),
        DateTimeField::Month | DateTimeField::Months => Some(Axis::Months(count)),
        DateTimeField::Week(_) | DateTimeField::Weeks => Some(Axis::Nanos(
            count.checked_mul(7 * SECONDS_PER_DAY * NANOS_PER_SECOND)?,
        )),
        DateTimeField::Day | DateTimeField::Days => Some(Axis::Nanos(
            count.checked_mul(SECONDS_PER_DAY * NANOS_PER_SECOND)?,
        )),
        DateTimeField::Hour | DateTimeField::Hours => Some(Axis::Nanos(
            count.checked_mul(SECONDS_PER_HOUR * NANOS_PER_SECOND)?,
        )),
        DateTimeField::Minute | DateTimeField::Minutes => Some(Axis::Nanos(
            count.checked_mul(SECONDS_PER_MINUTE * NANOS_PER_SECOND)?,
        )),
        DateTimeField::Second | DateTimeField::Seconds => {
            Some(Axis::Nanos(count.checked_mul(NANOS_PER_SECOND)?))
        }
        DateTimeField::Millisecond | DateTimeField::Milliseconds => {
            Some(Axis::Nanos(count.checked_mul(1_000_000)?))
        }
        DateTimeField::Microsecond | DateTimeField::Microseconds => {
            Some(Axis::Nanos(count.checked_mul(1_000)?))
        }
        DateTimeField::Nanosecond | DateTimeField::Nanoseconds => Some(Axis::Nanos(count)),
        _ => None,
    }
}

fn strip_leading_minus(text: &str) -> (bool, &str) {
    text.strip_prefix('-')
        .map_or((false, text), |rest| (true, rest.trim()))
}

fn parse_interval_axis(text: &str) -> Option<Axis> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (negative, rest) = strip_leading_minus(trimmed);
    if let Some(axis) = parse_spelled_out_interval(rest) {
        return Some(apply_axis_sign(negative, axis));
    }
    if let Some(axis) = parse_day_to_second_axis(rest) {
        return Some(apply_axis_sign(negative, axis));
    }
    if let Some(axis) = parse_number_and_unit(rest) {
        return Some(apply_axis_sign(negative, axis));
    }
    let count: i128 = rest.parse().ok()?;
    Some(apply_axis_sign(negative, Axis::Unitless(count)))
}

fn parse_spelled_out_interval(text: &str) -> Option<Axis> {
    let tokens: Vec<&str> = text.split_whitespace().collect();
    if tokens.len() < 4 || !tokens.len().is_multiple_of(2) {
        return None;
    }
    let mut nanos = 0_i128;
    let mut months = 0_i128;
    let mut saw_nanos = false;
    let mut saw_months = false;
    for pair in tokens.chunks(2) {
        let count: i128 = pair[0].parse().ok()?;
        match axis_from_unit_name(pair[1], count)? {
            Axis::Nanos(value) => {
                nanos = nanos.checked_add(value)?;
                saw_nanos = true;
            }
            Axis::Months(value) => {
                months = months.checked_add(value)?;
                saw_months = true;
            }
            Axis::Unitless(_) => return None,
        }
    }
    if saw_months && saw_nanos {
        return None;
    }
    if saw_months {
        Some(Axis::Months(months))
    } else if saw_nanos {
        Some(Axis::Nanos(nanos))
    } else {
        None
    }
}

fn parse_day_to_second_axis(text: &str) -> Option<Axis> {
    let unsigned = text.trim();
    let (days_text, time_text) = unsigned.split_once(' ')?;
    if !time_text.contains(':') {
        return None;
    }
    let days: i128 = days_text.parse().ok()?;
    let mut time_parts = time_text.split(':');
    let hours: i128 = time_parts.next()?.parse().ok()?;
    let minutes: i128 = time_parts.next()?.parse().ok()?;
    let seconds_text = time_parts.next().unwrap_or("0");
    if time_parts.next().is_some() {
        return None;
    }
    let seconds_whole = seconds_text.split('.').next().unwrap_or("0");
    let seconds: i128 = seconds_whole.parse().ok()?;
    let nanos = days
        .checked_mul(SECONDS_PER_DAY * NANOS_PER_SECOND)?
        .checked_add(hours.checked_mul(SECONDS_PER_HOUR * NANOS_PER_SECOND)?)?
        .checked_add(minutes.checked_mul(SECONDS_PER_MINUTE * NANOS_PER_SECOND)?)?
        .checked_add(seconds.checked_mul(NANOS_PER_SECOND)?)?;
    Some(Axis::Nanos(nanos))
}

fn parse_number_and_unit(text: &str) -> Option<Axis> {
    let mut parts = text.split_whitespace();
    let number_text = parts.next()?;
    let unit_text = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let count: i128 = number_text.parse().ok()?;
    axis_from_unit_name(unit_text, count)
}

fn axis_from_unit_name(unit: &str, count: i128) -> Option<Axis> {
    let normalized = unit.trim_end_matches(',').to_ascii_lowercase();
    let field = match normalized.as_str() {
        "year" | "years" => DateTimeField::Year,
        "quarter" | "quarters" => DateTimeField::Quarter,
        "month" | "months" => DateTimeField::Month,
        "week" | "weeks" => DateTimeField::Week(None),
        "day" | "days" => DateTimeField::Day,
        "hour" | "hours" => DateTimeField::Hour,
        "minute" | "minutes" => DateTimeField::Minute,
        "second" | "seconds" => DateTimeField::Second,
        "millisecond" | "milliseconds" => DateTimeField::Millisecond,
        "microsecond" | "microseconds" => DateTimeField::Microsecond,
        "nanosecond" | "nanoseconds" => DateTimeField::Nanosecond,
        _ => return None,
    };
    axis_from_datetime_field(&field, count)
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

/// True when a `RANGE` bound needs classify: unit-less number (DATE arm) or any interval
/// (R1 quote is already applied; R2/R3/R5 classify on the planned interval).
fn bound_needs_conform(bound: &AstWindowFrameBound) -> bool {
    bound_is_bare_number(bound) || bound_interval(bound).is_some()
}

/// The AST frame shape a caller can cheaply test for before paying for a second planning pass.
/// True when any `RANGE` frame in the statement carries a unit-less numeric bound or any
/// interval (R2 / R3 / R5 / ordinary quoted interval so classify can see a numeric key).
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
    // Positive same-kind invert (`2 FOLLOWING AND 1 FOLLOWING`) has no negative and no
    // field qualifier, so the cheap probes above miss it — it must still enter classify.
    if frame_is_value_inverted(frame) {
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
