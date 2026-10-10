use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DataFusionError, ExprSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{Cast, ScalarFunction};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, Projection, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Union, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

pub const NARROWED_BESIDE_NULL_NAME: &str = "__repark_narrowed_beside_null__";
pub const NARROWED_BESIDE_VALUE_NAME: &str = "__repark_narrowed_beside_value__";
const UNTYPED_NULL_TAG: &str = "repark.untyped_null";

static NARROWED_BESIDE_NULL: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(NarrowedBeside::new(
        NARROWED_BESIDE_NULL_NAME,
    )))
});
static NARROWED_BESIDE_VALUE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(NarrowedBeside::new(
        NARROWED_BESIDE_VALUE_NAME,
    )))
});

#[must_use]
pub fn narrowed_beside_udf(untyped_null: bool) -> Arc<ScalarUDF> {
    if untyped_null {
        Arc::clone(&NARROWED_BESIDE_NULL)
    } else {
        Arc::clone(&NARROWED_BESIDE_VALUE)
    }
}

#[must_use]
pub fn holds_nanoseconds(schema: &DFSchema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| nested_nanoseconds(field.data_type()))
}

fn nested_nanoseconds(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(unit, _) => *unit == TimeUnit::Nanosecond,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| nested_nanoseconds(field.data_type())),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => nested_nanoseconds(field.data_type()),
        _ => false,
    }
}

#[must_use]
pub fn is_narrowing_mark(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function) if matches!(
            function.func.name(),
            NARROWED_BESIDE_NULL_NAME | NARROWED_BESIDE_VALUE_NAME
        )
    )
}

fn marked(value: Expr, beside_untyped_null: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        narrowed_beside_udf(beside_untyped_null),
        vec![value],
    ))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NarrowedBeside {
    name: &'static str,
    signature: Signature,
}

impl NarrowedBeside {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NarrowedBeside {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(one_argument)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let value = args.arg_fields.first().ok_or_else(one_argument)?;
        Ok(Arc::new(Field::new(
            self.name(),
            value.data_type().clone(),
            value.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.into_iter().next().ok_or_else(one_argument)
    }

    fn simplify(&self, mut args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        match args.pop() {
            Some(value) if args.is_empty() => Ok(ExprSimplifyResult::Simplified(value)),
            _ => Err(one_argument()),
        }
    }
}

fn one_argument() -> DataFusionError {
    DataFusionError::Plan(format!(
        "'{NARROWED_BESIDE_NULL_NAME}' expects one argument"
    ))
}

#[derive(Debug, Default)]
pub struct FloatStringifyBeforeCoercion;

impl AnalyzerRule for FloatStringifyBeforeCoercion {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let ansi = crate::ansi::spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| {
            let tagged = tag_union_nulls(node)?;
            if tagged.transformed {
                return Ok(tagged);
            }
            crate::java_double::rewrite_float_plan(tagged.data, ansi, true)
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "spark_float_stringify"
    }
}

pub(crate) fn before_coercion(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if let Expr::Cast(cast) = &expr
        && instant(cast.field.data_type())
        && !is_narrowing_mark(&cast.expr)
        && holds(&cast.expr, schema, true)
    {
        return Ok(Transformed::yes(
            crate::timestamp_ns_cast::narrow_timestamp_ns_expr((*cast.expr).clone()),
        ));
    }
    tag_untyped_nulls(expr, schema)
}

fn coerced(expr: &Expr, schema: &DFSchema) -> bool {
    matches!(expr, Expr::Cast(cast)
        if coarser(cast.field.data_type())
            && !is_narrowing_mark(&cast.expr)
            && !null_literal(&cast.expr)
            && holds(&cast.expr, schema, true))
}

pub(crate) fn mark_coerced_narrowing(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::Alias(mut alias) => {
            let value = mark_coerced_narrowing(*alias.expr, schema);
            alias.expr = Box::new(value.data);
            Transformed::new_transformed(Expr::Alias(alias), value.transformed)
        }
        Expr::Cast(cast) if coerced(&Expr::Cast(cast.clone()), schema) => {
            Transformed::yes(Expr::Cast(Cast::new_from_field(
                Box::new(marked(*cast.expr, false)),
                cast.field,
            )))
        }
        other => Transformed::no(other),
    }
}

pub(crate) fn mark_coerced_branches(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let narrowed = |branch: &Expr| coerced(branch, schema);
    let unified = match &expr {
        Expr::Case(_) => any_branch(&expr, narrowed),
        Expr::ScalarFunction(_) => {
            any_branch(&expr, narrowed)
                && any_branch(&expr, |branch| {
                    !narrowed(branch) && holds(branch, schema, false)
                })
        }
        _ => false,
    };
    if !unified {
        return Ok(Transformed::no(expr));
    }
    expr.map_children(|branch| Ok(mark_coerced_narrowing(branch, schema)))
}

pub(crate) fn keep_written_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let over_a_mark = |target: &DataType, source: &Expr| {
        matches!(target, DataType::Timestamp(TimeUnit::Nanosecond, None))
            && matches!(source.get_type(schema), Ok(DataType::Timestamp(_, _)))
            && source
                .exists(|node| Ok(is_narrowing_mark(node)))
                .unwrap_or(false)
    };
    match expr {
        Expr::Cast(cast) if over_a_mark(cast.field.data_type(), &cast.expr) => Transformed::yes(
            crate::timestamp_ns_cast::narrow_timestamp_ns_expr(*cast.expr),
        ),
        other => Transformed::no(other),
    }
}

fn tag_untyped_nulls(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !unifies_branches(&expr)
        || !any_branch(&expr, is_untyped_null)
        || !any_branch(&expr, |branch| holds(branch, schema, true))
    {
        return Ok(Transformed::no(expr));
    }
    expr.map_children(|branch| {
        Ok(if is_untyped_null(&branch) {
            Transformed::yes(Expr::Literal(ScalarValue::Null, Some(tag())))
        } else {
            Transformed::no(branch)
        })
    })
}

pub(crate) fn settle_branches(
    expr: Expr,
    schema: &DFSchema,
    nanoseconds_in_scope: bool,
) -> Result<Transformed<Expr>> {
    if !unifies_branches(&expr) {
        return Ok(Transformed::no(expr));
    }
    let tagged = any_branch(&expr, is_tagged_null);
    let asks = tagged || nanoseconds_in_scope || any_branch(&expr, is_nanosecond_cast);
    let narrows = asks
        && any_branch(&expr, |branch| holds(branch, schema, false))
        && any_branch(&expr, |branch| {
            !is_narrowing_mark(branch) && holds(branch, schema, true)
        });
    if !tagged && !narrows {
        return Ok(Transformed::no(expr));
    }
    expr.map_children(|branch| {
        if is_tagged_null(&branch) {
            branch.transform_up(|node| Ok(untagged(node)))
        } else if narrows && !is_narrowing_mark(&branch) && holds(&branch, schema, true) {
            Ok(Transformed::yes(marked(branch, tagged)))
        } else {
            Ok(Transformed::no(branch))
        }
    })
}

fn is_nanosecond_cast(expr: &Expr) -> bool {
    crate::timestamp_ns_cast::is_timestamp_ns_cast(expr)
}

pub(crate) fn tag_union_nulls(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Union(union) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let mut changed = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        let schema = Arc::clone(input.schema());
        let untyped =
            |index: usize| untyped_column(input, index) && beside_nanoseconds(union, index);
        if !(0..schema.fields().len()).any(untyped) {
            inputs.push(Arc::clone(input));
            continue;
        }
        let (mut exprs, source) = match input.as_ref() {
            LogicalPlan::Projection(projection) => {
                (projection.expr.clone(), Arc::clone(&projection.input))
            }
            _ => (
                schema
                    .iter()
                    .map(|column| Expr::Column(column.into()))
                    .collect(),
                Arc::clone(input),
            ),
        };
        for (index, (qualifier, field)) in schema.iter().enumerate() {
            if untyped(index) {
                exprs[index] = Expr::Literal(ScalarValue::Null, Some(tag()))
                    .alias_qualified(qualifier.cloned(), field.name());
            }
        }
        let rebuilt = Projection::try_new(exprs, source)?;
        inputs.push(Arc::new(LogicalPlan::Projection(rebuilt)));
        changed = true;
    }
    if !changed {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(LogicalPlan::Union(Union {
        inputs,
        schema: Arc::clone(&union.schema),
    })))
}

pub(crate) fn settle_union_branches(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Union(union) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let width = union.schema.fields().len();
    let tagged = |index: usize| {
        union.inputs.iter().any(|input| {
            matches!(input.as_ref(), LogicalPlan::Projection(projection)
                if projection.expr.get(index).is_some_and(|expr| is_tagged_null(peeled(expr))))
        })
    };
    let narrows = |index: usize| {
        let kinds = |nanoseconds: bool| {
            union.inputs.iter().any(|input| {
                matches!(quick_type(input, index), Some(DataType::Timestamp(unit, _))
                    if (unit == TimeUnit::Nanosecond) == nanoseconds)
            })
        };
        kinds(true) && kinds(false)
    };
    let columns: Vec<(bool, bool)> = (0..width)
        .map(|index| (tagged(index), narrows(index)))
        .collect();
    if !columns.iter().any(|(tagged, narrows)| *tagged || *narrows) {
        return Ok(Transformed::no(plan));
    }
    let mut settled = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        let schema = Arc::clone(input.schema());
        let (mut exprs, source) = match input.as_ref() {
            LogicalPlan::Projection(projection) => {
                (projection.expr.clone(), Arc::clone(&projection.input))
            }
            _ => (
                schema
                    .iter()
                    .map(|column| Expr::Column(column.into()))
                    .collect(),
                Arc::clone(input),
            ),
        };
        let mut changed = false;
        for (index, (tagged, narrows)) in columns.iter().enumerate() {
            let Some(branch) = exprs.get(index).cloned() else {
                continue;
            };
            let nanoseconds = *narrows
                && !is_narrowing_mark(peeled(&branch))
                && holds(peeled(&branch), source.schema().as_ref(), true);
            if !nanoseconds && !is_tagged_null(peeled(&branch)) {
                continue;
            }
            let (qualifier, field) = schema.qualified_field(index);
            let value = branch.transform_up(|node| Ok(untagged(node)))?.data;
            let value = match value {
                Expr::Alias(alias) => *alias.expr,
                value => value,
            };
            let value = if nanoseconds {
                marked(value, *tagged)
            } else {
                value
            };
            exprs[index] = value.alias_qualified(qualifier.cloned(), field.name());
            changed = true;
        }
        if changed {
            settled = true;
            let rebuilt = Projection::try_new(exprs, source)?;
            inputs.push(Arc::new(LogicalPlan::Projection(rebuilt)));
        } else {
            inputs.push(Arc::clone(input));
        }
    }
    if !settled {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(LogicalPlan::Union(Union {
        inputs,
        schema: Arc::clone(&union.schema),
    })))
}

fn quick_type(input: &LogicalPlan, index: usize) -> Option<DataType> {
    let declared = || (input.schema().fields().get(index)).map(|field| field.data_type().clone());
    let LogicalPlan::Projection(projection) = input else {
        return declared();
    };
    match projection.expr.get(index).map(peeled) {
        Some(Expr::Column(column)) => (projection.input.schema().field_from_column(column))
            .ok()
            .map(|field| field.data_type().clone()),
        Some(Expr::Cast(cast)) => Some(cast.field.data_type().clone()),
        Some(Expr::Literal(value, _)) => Some(value.data_type()),
        _ => declared(),
    }
}

fn beside_nanoseconds(union: &Union, index: usize) -> bool {
    union.inputs.iter().any(|input| {
        matches!(
            branch_type(input, index),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, _))
        )
    })
}

fn untyped_column(input: &LogicalPlan, index: usize) -> bool {
    let untyped = |data_type: &DataType| data_type == &DataType::Null;
    let LogicalPlan::Projection(projection) = input else {
        return (input.schema().fields().get(index))
            .is_some_and(|field| untyped(field.data_type()));
    };
    match projection.expr.get(index).map(peeled) {
        Some(Expr::Literal(ScalarValue::Null, _)) => true,
        Some(Expr::Column(column)) => (projection.input.schema().field_from_column(column))
            .is_ok_and(|field| untyped(field.data_type())),
        Some(Expr::Cast(cast)) => untyped(cast.field.data_type()),
        _ => false,
    }
}

fn branch_type(input: &LogicalPlan, index: usize) -> Option<DataType> {
    match input {
        LogicalPlan::Projection(projection) => projection
            .expr
            .get(index)?
            .get_type(projection.input.schema().as_ref())
            .ok(),
        other => other
            .schema()
            .fields()
            .get(index)
            .map(|field| field.data_type().clone()),
    }
}

fn peeled(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => peeled(&alias.expr),
        other => other,
    }
}

fn unifies_branches(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(_) | Expr::Case(_))
}

fn any_branch(expr: &Expr, test: impl Fn(&Expr) -> bool) -> bool {
    let mut found = false;
    let walked = expr.apply_children(|branch| {
        found = test(branch);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    walked.is_ok() && found
}

fn is_untyped_null(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Null, None))
}

fn is_tagged_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, Some(metadata)) => {
            value.is_null() && metadata.inner().contains_key(UNTYPED_NULL_TAG)
        }
        Expr::Cast(cast) => is_tagged_null(&cast.expr),
        Expr::TryCast(cast) => is_tagged_null(&cast.expr),
        _ => false,
    }
}

fn untagged(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::Literal(value, Some(metadata)) if metadata.inner().contains_key(UNTYPED_NULL_TAG) => {
            Transformed::yes(Expr::Literal(value, None))
        }
        other => Transformed::no(other),
    }
}

fn tag() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        UNTYPED_NULL_TAG.to_string(),
        String::new(),
    )]))
}

fn holds(expr: &Expr, schema: &DFSchema, nanoseconds: bool) -> bool {
    matches!(
        expr.get_type(schema),
        Ok(DataType::Timestamp(unit, _)) if (unit == TimeUnit::Nanosecond) == nanoseconds
    )
}

fn coarser(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(unit, _) if *unit != TimeUnit::Nanosecond)
}

fn instant(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
    )
}

fn null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => null_literal(&cast.expr),
        Expr::TryCast(cast) => null_literal(&cast.expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
