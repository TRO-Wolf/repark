use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DataFusionError, ExprSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, Projection, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Union, Volatility,
};

pub const NARROWED_BESIDE_NULL_NAME: &str = "__repark_narrowed_beside_null__";
const UNTYPED_NULL_TAG: &str = "repark.untyped_null";

static NARROWED_BESIDE_NULL: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(NarrowedBesideNull::new())));

#[must_use]
pub fn narrowed_beside_null_udf() -> Arc<ScalarUDF> {
    Arc::clone(&NARROWED_BESIDE_NULL)
}

#[must_use]
pub fn is_narrowed_beside_null(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function) if function.func.name() == NARROWED_BESIDE_NULL_NAME
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NarrowedBesideNull {
    signature: Signature,
}

impl NarrowedBesideNull {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NarrowedBesideNull {
    fn name(&self) -> &str {
        NARROWED_BESIDE_NULL_NAME
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

pub(crate) fn tag_untyped_nulls(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
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

pub(crate) fn settle_tagged_nulls(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !unifies_branches(&expr) || !any_branch(&expr, is_tagged_null) {
        return Ok(Transformed::no(expr));
    }
    let narrows = any_branch(&expr, |branch| {
        is_tagged_null(branch) && holds(branch, schema, false)
    }) && any_branch(&expr, |branch| {
        !is_tagged_null(branch) && holds(branch, schema, true)
    });
    let settled = expr
        .map_children(|branch| {
            if is_tagged_null(&branch) {
                branch.transform_up(|node| Ok(untagged(node)))
            } else {
                Ok(Transformed::no(branch))
            }
        })?
        .data;
    Ok(Transformed::yes(if narrows {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            narrowed_beside_null_udf(),
            vec![settled],
        ))
    } else {
        settled
    }))
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

pub(crate) fn settle_union_nulls(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Union(union) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let mut changed = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        let projection = match input.as_ref() {
            LogicalPlan::Projection(projection)
                if projection
                    .expr
                    .iter()
                    .any(|expr| is_tagged_null(peeled(expr))) =>
            {
                projection
            }
            _ => {
                inputs.push(Arc::clone(input));
                continue;
            }
        };
        let mut exprs = Vec::with_capacity(projection.expr.len());
        for (index, expr) in projection.expr.iter().enumerate() {
            exprs.push(if is_tagged_null(peeled(expr)) {
                settle_union_branch(expr.clone(), beside_nanoseconds(union, index))?
            } else {
                expr.clone()
            });
        }
        let rebuilt = Projection::try_new(exprs, Arc::clone(&projection.input))?;
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

fn settle_union_branch(branch: Expr, beside_nanoseconds: bool) -> Result<Expr> {
    let name = branch.schema_name().to_string();
    let settled = branch.transform_up(|node| Ok(untagged(node)))?.data;
    let marked = |value: Expr| {
        if beside_nanoseconds && holds(&value, &DFSchema::empty(), false) {
            Expr::ScalarFunction(ScalarFunction::new_udf(
                narrowed_beside_null_udf(),
                vec![value],
            ))
        } else {
            value
        }
    };
    match settled {
        Expr::Alias(alias) => Ok(Expr::Alias(Alias {
            expr: Box::new(marked(*alias.expr)),
            ..alias
        })),
        value => marked(value).alias_if_changed(name),
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

#[cfg(test)]
mod tests;
