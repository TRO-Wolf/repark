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
//! **Scope.** Only bounds that are unit-less numbers under `RANGE` over a datetime key are
//! touched; an interval-shaped bound, a `ROWS`/`GROUPS` frame, and every numeric key are left
//! exactly as they were. Residual divergences of the surrounding surface (unquoted
//! `INTERVAL 1 DAY`, `DAY TO SECOND` literals, negative offsets, `FOLLOWING`-to-`FOLLOWING`
//! values) are recorded rows in `python/repark/tests/test_window_parity.py`, not silent gaps.

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
    DateTimeField, Expr as AstExpr, Interval, NamedWindowExpr, Query, SetExpr, Statement, Value,
    ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut, WindowFrameBound as AstWindowFrameBound,
    WindowFrameUnits as AstWindowFrameUnits, WindowSpec, WindowType,
};

/// What a planned statement's `RANGE` frames ask the door to do before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeFrameVerdict {
    /// Nothing to do — no bare-number `RANGE` bound sits over a datetime order key.
    Unchanged,
    /// At least one DATE-keyed frame needs its unit-less bounds restated as `INTERVAL '<n>' DAY`,
    /// and no numeric-keyed frame in the same statement would be caught by that restatement.
    RestateBareBoundsAsDays,
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
    // only safe when every unit-less RANGE bound in the statement belongs to a DATE-keyed frame.
    // A statement that mixes a DATE-keyed frame with an ordinary numeric-keyed one keeps its
    // recorded divergence rather than silently re-scaling the numeric frame to days.
    if date_keyed_sites > 0 && other_keyed_sites == 0 {
        return Ok(RangeFrameVerdict::RestateBareBoundsAsDays);
    }
    Ok(RangeFrameVerdict::Unchanged)
}

/// Which order-key family a unit-less `RANGE` bound was found over.
#[derive(Debug, Clone, Copy)]
enum FrameSite {
    /// `DATE` order key — Spark reads the number as days.
    DateKeyed,
    /// Any non-datetime order key — an ordinary numeric `RANGE`, already correct.
    OtherKeyed,
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
    if bare_bounds.is_empty() {
        return Ok(None);
    }
    let key_type = sort.expr.get_type(input_schema)?;
    match key_type {
        DataType::Date32 | DataType::Date64 => Ok(Some(FrameSite::DateKeyed)),
        DataType::Timestamp(_, _) => Err(range_frame_invalid_type_error(
            &sort.expr.to_string(),
            &params.window_frame.to_string(),
        )),
        _ => Ok(Some(FrameSite::OtherKeyed)),
    }
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
/// Restate every unit-less `RANGE` frame bound in the statement as `INTERVAL '<n>' DAY`.
///
/// Spark reads a bare `RANGE` offset over a `DATE` key as a number of days; DataFusion's
/// coercion reads it as months. Applied only when [`classify_planned_range_frames`] returned
/// [`RangeFrameVerdict::RestateBareBoundsAsDays`], i.e. every such bound in the statement sits
/// over a DATE key. Mirrors [`crate::spark_ast::apply_spark_order_by_defaults`]'s traversal:
/// `post_visit_expr` reaches inline `OVER (…)`, `post_visit_query` the named `WINDOW` clauses.
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
    restate_bound(&mut frame.start_bound);
    if let Some(end_bound) = &mut frame.end_bound {
        restate_bound(end_bound);
    }
}

/// `<n> PRECEDING` → `INTERVAL '<n>' DAY PRECEDING`; anything else is untouched.
fn restate_bound(bound: &mut AstWindowFrameBound) {
    let offset = match bound {
        AstWindowFrameBound::Preceding(value) | AstWindowFrameBound::Following(value) => value,
        AstWindowFrameBound::CurrentRow => return,
    };
    let Some(expr) = offset else {
        return;
    };
    let AstExpr::Value(ValueWithSpan {
        value: Value::Number(number, false),
        span,
    }) = expr.as_ref()
    else {
        return;
    };
    **expr = AstExpr::Interval(Interval {
        value: Box::new(AstExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(number.clone()),
            span: *span,
        })),
        leading_field: Some(DateTimeField::Day),
        leading_precision: None,
        last_field: None,
        fractional_seconds_precision: None,
    });
}

/// The AST frame shape a caller can cheaply test for before paying for a second planning pass.
/// True when any `RANGE` frame in the statement carries a unit-less numeric bound.
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
    if bound_is_bare_number(&frame.start_bound) {
        *found = true;
    }
    if frame.end_bound.as_ref().is_some_and(bound_is_bare_number) {
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
