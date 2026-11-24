use std::borrow::Cow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err, internal_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExpressionPlacement, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion::physical_expr::expressions::{LambdaExpr, TryCastExpr};
use datafusion::physical_expr::{HigherOrderFunctionExpr, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

pub(crate) const NULLABLE_FIELD_NAME: &str = "__repark_nullable_field__";

static NULLABLE_FIELD: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(NullableField {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

#[must_use]
pub(crate) fn nullable_field_udf() -> Arc<ScalarUDF> {
    Arc::clone(&NULLABLE_FIELD)
}

#[must_use]
pub(crate) fn nullable_field_call(expr: Expr) -> Expr {
    nullable_field_udf().call(vec![expr])
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NullableField {
    signature: Signature,
}

impl ScalarUDFImpl for NullableField {
    fn name(&self) -> &str {
        NULLABLE_FIELD_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [argument] = args else {
            return plan_err!("{NULLABLE_FIELD_NAME} takes one argument");
        };
        Ok(argument.schema_name().to_string())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return plan_err!("{NULLABLE_FIELD_NAME} takes one argument");
        };
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("{NULLABLE_FIELD_NAME} takes one argument");
        };
        if field.is_nullable() {
            return Ok(Arc::clone(field));
        }
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut values = args.args;
        let (Some(value), true) = (values.pop(), values.is_empty()) else {
            return exec_err!("{NULLABLE_FIELD_NAME} takes one argument");
        };
        Ok(value)
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        let [interval] = input else {
            return plan_err!("{NULLABLE_FIELD_NAME} takes one argument");
        };
        Ok((*interval).clone())
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let [input] = inputs else {
            return plan_err!("{NULLABLE_FIELD_NAME} takes one argument");
        };
        let Some(updated) = input.intersect(interval)? else {
            return Ok(None);
        };
        Ok(Some(vec![updated]))
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        let [input] = inputs else {
            return plan_err!("{NULLABLE_FIELD_NAME} takes one argument");
        };
        Ok(input.sort_properties)
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        Ok(true)
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        args.first()
            .copied()
            .unwrap_or(ExpressionPlacement::KeepInPlace)
    }
}

pub(crate) fn shield_try_cast_plan(plan: &LogicalPlan) -> Result<Cow<'_, LogicalPlan>> {
    if !plan_has_try_cast(plan)? {
        return Ok(Cow::Borrowed(plan));
    }
    let rewritten = plan.clone().transform_up_with_subqueries(shield_node)?;
    if rewritten.transformed {
        Ok(Cow::Owned(rewritten.data))
    } else {
        Ok(Cow::Borrowed(plan))
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "errors are DataFusion tree-rewrite errors; the contract lives in stack/map.md"
)]
pub fn shield_try_cast_expr(expr: Expr) -> Result<Expr> {
    if !expr_has_try_cast(&expr)? {
        return Ok(expr);
    }
    rewrite_expr(expr, false).data()
}

fn plan_has_try_cast(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = node_has_try_cast(node)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn node_has_try_cast(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr_has_try_cast(expr)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn expr_has_try_cast(expr: &Expr) -> Result<bool> {
    expr.exists(|node| Ok(matches!(node, Expr::TryCast(_))))
}

fn shield_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !node_has_try_cast(&plan)? {
        return Ok(Transformed::no(plan));
    }
    let preserver = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        let shielded = rewrite_expr(expr, false)?;
        if shielded.transformed {
            Ok(shielded.update_data(|node| saved.restore(node)))
        } else {
            Ok(shielded)
        }
    })
}

fn rewrite_expr(expr: Expr, inside_consumer: bool) -> Result<Transformed<Expr>> {
    match expr {
        Expr::ScalarFunction(call) if call.name() == NULLABLE_FIELD_NAME => {
            let [inner] = call.args.as_slice() else {
                return Ok(Transformed::no(Expr::ScalarFunction(call)));
            };
            let rewritten = rewrite_expr(inner.clone(), false)?;
            if !rewritten.transformed {
                return Ok(Transformed::no(Expr::ScalarFunction(call)));
            }
            Ok(Transformed::yes(Expr::ScalarFunction(
                ScalarFunction::new_udf(call.func, vec![rewritten.data]),
            )))
        }
        Expr::TryCast(try_cast) => {
            let child = rewrite_expr(*try_cast.expr, false)?;
            let rebuilt = Expr::TryCast(TryCast {
                expr: Box::new(child.data),
                field: try_cast.field,
            });
            if inside_consumer {
                Ok(Transformed::yes(nullable_field_call(rebuilt)))
            } else {
                Ok(Transformed::new(rebuilt, child.transformed, child.tnr))
            }
        }
        Expr::ScalarFunction(_)
        | Expr::HigherOrderFunction(_)
        | Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::Lambda(_) => expr.map_children(|child| rewrite_expr(child, true)),
        other => other.map_children(|child| rewrite_expr(child, inside_consumer)),
    }
}

pub(crate) fn repair_inlined_try_cast_plan(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !plan.exists(|node| Ok(node_needs_physical_repair(node)))? {
        return Ok(plan);
    }
    plan.transform_up(repair_physical_node).data()
}

fn node_needs_physical_repair(node: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        return projection
            .expr()
            .iter()
            .any(|item| physical_expr_needs_repair(&item.expr, false));
    }
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        return physical_expr_needs_repair(filter.predicate(), false);
    }
    false
}

fn physical_expr_needs_repair(expr: &Arc<dyn PhysicalExpr>, inside_consumer: bool) -> bool {
    if expr.downcast_ref::<NullableFieldPhysicalExpr>().is_some() {
        return expr
            .children()
            .into_iter()
            .any(|child| physical_expr_needs_repair(child, false));
    }
    if let Some(call) = expr.downcast_ref::<ScalarFunctionExpr>()
        && call.name() == NULLABLE_FIELD_NAME
    {
        return call
            .args()
            .iter()
            .any(|child| physical_expr_needs_repair(child, false));
    }
    if expr.downcast_ref::<TryCastExpr>().is_some() {
        return inside_consumer
            || expr
                .children()
                .into_iter()
                .any(|child| physical_expr_needs_repair(child, false));
    }
    let child_inside = inside_consumer
        || expr.downcast_ref::<ScalarFunctionExpr>().is_some()
        || expr.downcast_ref::<HigherOrderFunctionExpr>().is_some()
        || expr.downcast_ref::<LambdaExpr>().is_some();
    expr.children()
        .into_iter()
        .any(|child| physical_expr_needs_repair(child, child_inside))
}

fn repair_physical_node(
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        let mut changed = false;
        let mut exprs = Vec::with_capacity(projection.expr().len());
        for item in projection.expr() {
            let repaired = repair_physical_expr(Arc::clone(&item.expr), false)?;
            changed |= repaired.transformed;
            exprs.push(ProjectionExpr::new(repaired.data, item.alias.clone()));
        }
        if !changed {
            return Ok(Transformed::no(node));
        }
        let rebuilt = ProjectionExec::try_new(exprs, Arc::clone(projection.input()))?;
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        let repaired = repair_physical_expr(Arc::clone(filter.predicate()), false)?;
        if !repaired.transformed {
            return Ok(Transformed::no(node));
        }
        let rebuilt = FilterExecBuilder::from(filter)
            .with_predicate(repaired.data)
            .build()?;
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    Ok(Transformed::no(node))
}

fn repair_physical_expr(
    expr: Arc<dyn PhysicalExpr>,
    inside_consumer: bool,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    if expr.downcast_ref::<NullableFieldPhysicalExpr>().is_some() {
        return map_physical_children(expr, false);
    }
    if let Some(call) = expr.downcast_ref::<ScalarFunctionExpr>()
        && call.name() == NULLABLE_FIELD_NAME
    {
        return map_physical_children(expr, false);
    }
    if expr.downcast_ref::<TryCastExpr>().is_some() {
        let rebuilt = map_physical_children(expr, false)?;
        return if inside_consumer {
            Ok(Transformed::yes(Arc::new(NullableFieldPhysicalExpr {
                inner: rebuilt.data,
            }) as Arc<dyn PhysicalExpr>))
        } else {
            Ok(rebuilt)
        };
    }
    let child_inside = inside_consumer
        || expr.downcast_ref::<ScalarFunctionExpr>().is_some()
        || expr.downcast_ref::<HigherOrderFunctionExpr>().is_some()
        || expr.downcast_ref::<LambdaExpr>().is_some();
    map_physical_children(expr, child_inside)
}

fn map_physical_children(
    expr: Arc<dyn PhysicalExpr>,
    inside_consumer: bool,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    let children = expr.children();
    if children.is_empty() {
        return Ok(Transformed::no(expr));
    }
    let mut changed = false;
    let mut updated = Vec::with_capacity(children.len());
    for child in children {
        let rewritten = repair_physical_expr(Arc::clone(child), inside_consumer)?;
        changed |= rewritten.transformed;
        updated.push(rewritten.data);
    }
    if !changed {
        return Ok(Transformed::no(expr));
    }
    Ok(Transformed::yes(expr.with_new_children(updated)?))
}

#[derive(Debug, Eq)]
struct NullableFieldPhysicalExpr {
    inner: Arc<dyn PhysicalExpr>,
}

impl PartialEq for NullableFieldPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Hash for NullableFieldPhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl fmt::Display for NullableFieldPhysicalExpr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

impl PhysicalExpr for NullableFieldPhysicalExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.inner.evaluate(batch)
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        self.inner.evaluate_selection(batch, selection)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let field = self.inner.return_field(input_schema)?;
        if field.is_nullable() || !self.inner.nullable(input_schema)? {
            return Ok(field);
        }
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [inner] = children.as_slice() else {
            return internal_err!(
                "NullableFieldPhysicalExpr expects 1 child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(Self {
            inner: Arc::clone(inner),
        }))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        let [child] = children else {
            return internal_err!(
                "NullableFieldPhysicalExpr expects 1 bound, got {}",
                children.len()
            );
        };
        Ok((*child).clone())
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let [child] = children else {
            return internal_err!(
                "NullableFieldPhysicalExpr expects 1 bound, got {}",
                children.len()
            );
        };
        let Some(updated) = child.intersect(interval)? else {
            return Ok(None);
        };
        Ok(Some(vec![updated]))
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let [child] = children else {
            return internal_err!(
                "NullableFieldPhysicalExpr expects 1 property, got {}",
                children.len()
            );
        };
        Ok(child.clone())
    }

    fn fmt_sql(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(formatter)
    }

    fn is_volatile_node(&self) -> bool {
        self.inner.is_volatile_node()
    }

    fn placement(&self) -> ExpressionPlacement {
        self.inner.placement()
    }
}

#[cfg(test)]
mod tests;
