use std::sync::Arc;

use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    Alias, BinaryExpr, Case, Column, DmlStatement, Expr, LogicalPlan, Operator, Projection,
    ScalarFunction, Subquery, Volatility, WriteOp,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::scalar::ScalarValue;

use super::store_cast::{
    is_overflow_store_pair, spark_store_type_name, store_cast_udf_for_target,
    store_int_guard_udf, store_overflow_error,
};

const ANSI_GUARD_NAME: &str = "__repark_ansi_nonzero_divisor__";
const ARROW_CAST_NAME: &str = "arrow_cast";
const MAX_EVAL_DEPTH: usize = 256;
const MAX_LINEAGE_DEPTH: usize = 128;

#[derive(Debug, Clone, Copy)]
pub struct StorePolicy {
    pub raise_overflow: bool,
    pub raise_divzero: bool,
    pub descend_from: bool,
}

pub const DML_INSERT_POLICY: StorePolicy = StorePolicy {
    raise_overflow: true,
    raise_divzero: true,
    descend_from: true,
};

pub const DML_UPDATE_POLICY: StorePolicy = StorePolicy {
    raise_overflow: true,
    raise_divzero: false,
    descend_from: true,
};

pub const MERGE_UPDATE_POLICY: StorePolicy = StorePolicy {
    raise_overflow: true,
    raise_divzero: true,
    descend_from: false,
};

pub const MERGE_INSERT_POLICY: StorePolicy = StorePolicy {
    raise_overflow: true,
    raise_divzero: true,
    descend_from: true,
};

pub const OVERWRITE_SOURCE_POLICY: StorePolicy = StorePolicy {
    raise_overflow: true,
    raise_divzero: true,
    descend_from: true,
};

pub const BY_NAME_POLICY: StorePolicy = StorePolicy {
    raise_overflow: true,
    raise_divzero: true,
    descend_from: true,
};

pub const PREDICATE_DML_POLICY: StorePolicy = StorePolicy {
    raise_overflow: false,
    raise_divzero: false,
    descend_from: true,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct StoreIntOverflow;

impl AnalyzerRule for StoreIntOverflow {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        transform_dml(plan)
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "repark_store_int_overflow"
    }
}

fn transform_dml(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Explain(_) | LogicalPlan::Analyze(_) => Ok(plan),
        LogicalPlan::Dml(dml) => rewrite_dml(*dml),
        other => other
            .map_children(|child| {
                Ok(Transformed::new(
                    transform_dml(child)?,
                    true,
                    TreeNodeRecursion::Continue,
                ))
            })?
            .data(),
    }
}

#[derive(Debug, Clone)]
struct StoreCtx {
    column: String,
    source: String,
    source_type: DataType,
    target: String,
    target_type: DataType,
}

fn store_ctx(column: &str, source_type: &DataType, target_type: &DataType) -> Option<StoreCtx> {
    if !is_overflow_store_pair(source_type, target_type) {
        return None;
    }
    if store_cast_udf_for_target(target_type).is_none() {
        return None;
    }
    let (Some(source), Some(target)) = (
        spark_store_type_name(source_type),
        spark_store_type_name(target_type),
    ) else {
        return None;
    };
    Some(StoreCtx {
        column: column.to_string(),
        source,
        source_type: source_type.clone(),
        target,
        target_type: target_type.clone(),
    })
}

fn peel_alias(expr: &Expr) -> (Option<Alias>, &Expr) {
    match expr {
        Expr::Alias(alias) => (Some(alias.clone()), alias.expr.as_ref()),
        other => (None, other),
    }
}

fn wrap_alias(alias: Option<Alias>, inner: Expr) -> Expr {
    match alias {
        Some(alias) => Expr::Alias(Alias {
            expr: Box::new(inner),
            ..alias
        }),
        None => inner,
    }
}

fn arrow_cast_target(function: &ScalarFunction) -> Option<DataType> {
    if function.func.name() != ARROW_CAST_NAME || function.args.len() != 2 {
        return None;
    }
    let Expr::Literal(ScalarValue::Utf8(Some(name)), _) = &function.args[1] else {
        return None;
    };
    name.parse::<DataType>().ok()
}

fn store_cast_call(ctx: &StoreCtx, value: Expr) -> Option<Expr> {
    let udf = store_cast_udf_for_target(&ctx.target_type)?;
    let column = Expr::Literal(ScalarValue::Utf8(Some(ctx.column.clone())), None);
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        udf,
        vec![value, column],
    )))
}

fn store_guard_call(ctx: &StoreCtx, divisor: Expr) -> Expr {
    let lit = |text: &str| Expr::Literal(ScalarValue::Utf8(Some(text.to_string())), None);
    Expr::ScalarFunction(ScalarFunction::new_udf(
        store_int_guard_udf(),
        vec![
            divisor,
            lit(&ctx.column),
            lit(&ctx.source),
            lit(&ctx.target),
        ],
    ))
}

fn is_immutable_scalar(function: &ScalarFunction) -> bool {
    function.func.signature().volatility == Volatility::Immutable
}

fn is_static_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_, _) => true,
        Expr::Alias(alias) => is_static_expr(&alias.expr),
        Expr::Cast(cast) => is_static_expr(&cast.expr),
        Expr::TryCast(cast) => is_static_expr(&cast.expr),
        Expr::Negative(inner) => is_static_expr(inner),
        Expr::Not(inner) => is_static_expr(inner),
        Expr::BinaryExpr(binary) => is_static_expr(&binary.left) && is_static_expr(&binary.right),
        Expr::Case(case) => {
            case.expr.as_ref().is_none_or(|operand| is_static_expr(operand))
                && case.when_then.iter().all(|(when, then)| {
                    is_static_expr(when) && is_static_expr(then)
                })
                && case.else_expr.as_ref().is_none_or(|or_else| is_static_expr(or_else))
        }
        Expr::ScalarFunction(function) => {
            is_immutable_scalar(function) && function.args.iter().all(is_static_expr)
        }
        Expr::Column(_)
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::SetComparison(_)
        | Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::Placeholder(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::Unnest(_)
        | Expr::GroupingSet(_)
        | Expr::InList(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_)
        | Expr::ScalarVariable(_, _)
        | Expr::HigherOrderFunction(_)
        | Expr::Lambda(_)
        | Expr::LambdaVariable(_) => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ScalarOut {
    Value(ScalarValue),
    DivZero,
    Unknown,
}

fn strict_options() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

fn cast_scalar_value(value: &ScalarValue, target: &DataType) -> Option<ScalarValue> {
    if value.is_null() {
        return match target {
            DataType::Float32 => Some(ScalarValue::Float32(None)),
            DataType::Float64 => Some(ScalarValue::Float64(None)),
            DataType::Decimal128(_, precision, scale) => {
                Some(ScalarValue::Decimal128(None, *precision, *scale))
            }
            DataType::Decimal256(_, precision, scale) => {
                Some(ScalarValue::Decimal256(None, *precision, *scale))
            }
            DataType::Int32 => Some(ScalarValue::Int32(None)),
            DataType::Int64 => Some(ScalarValue::Int64(None)),
            _ => None,
        };
    }
    let array = value.to_array_of_size(1).ok()?;
    let casted = cast_with_options(&array, target, &strict_options()).ok()?;
    ScalarValue::try_from_array(&casted, 0).ok()
}

fn scalar_is_zero(value: &ScalarValue) -> bool {
    if value.is_null() {
        return false;
    }
    match ScalarValue::new_zero(&value.data_type()) {
        Ok(zero) => *value == zero,
        Err(_) => false,
    }
}

fn float_binary(left: f64, op: Operator, right: f64) -> ScalarOut {
    match op {
        Operator::Plus => ScalarOut::Value(ScalarValue::Float64(Some(left + right))),
        Operator::Minus => ScalarOut::Value(ScalarValue::Float64(Some(left - right))),
        Operator::Multiply => ScalarOut::Value(ScalarValue::Float64(Some(left * right))),
        Operator::Divide | Operator::Modulo => {
            if right == 0.0 {
                ScalarOut::DivZero
            } else if op == Operator::Divide {
                ScalarOut::Value(ScalarValue::Float64(Some(left / right)))
            } else {
                ScalarOut::Value(ScalarValue::Float64(Some(left % right)))
            }
        }
        _ => ScalarOut::Unknown,
    }
}

fn int_binary(left: i64, op: Operator, right: i64) -> ScalarOut {
    match op {
        Operator::Plus => left
            .checked_add(right)
            .map_or(ScalarOut::Unknown, |sum| {
                ScalarOut::Value(ScalarValue::Int64(Some(sum)))
            }),
        Operator::Minus => left
            .checked_sub(right)
            .map_or(ScalarOut::Unknown, |difference| {
                ScalarOut::Value(ScalarValue::Int64(Some(difference)))
            }),
        Operator::Multiply => left
            .checked_mul(right)
            .map_or(ScalarOut::Unknown, |product| {
                ScalarOut::Value(ScalarValue::Int64(Some(product)))
            }),
        Operator::Divide | Operator::Modulo => ScalarOut::Unknown,
        _ => ScalarOut::Unknown,
    }
}

fn eval_binary(left: &ScalarValue, op: Operator, right: &ScalarValue) -> ScalarOut {
    if left.is_null() || right.is_null() {
        return ScalarOut::Value(ScalarValue::Null);
    }
    let as_f64 = |value: &ScalarValue| match value {
        ScalarValue::Float32(Some(number)) => Some(f64::from(*number)),
        ScalarValue::Float64(Some(number)) => Some(*number),
        ScalarValue::Int8(Some(number)) => Some(f64::from(*number)),
        ScalarValue::Int16(Some(number)) => Some(f64::from(*number)),
        ScalarValue::Int32(Some(number)) => Some(f64::from(*number)),
        ScalarValue::Int64(Some(number)) => Some(*number as f64),
        _ => None,
    };
    let as_i64 = |value: &ScalarValue| match value {
        ScalarValue::Int8(Some(number)) => Some(i64::from(*number)),
        ScalarValue::Int16(Some(number)) => Some(i64::from(*number)),
        ScalarValue::Int32(Some(number)) => Some(i64::from(*number)),
        ScalarValue::Int64(Some(number)) => Some(*number),
        _ => None,
    };
    match (as_f64(left), as_f64(right)) {
        (Some(first), Some(second))
            if matches!(
                left,
                ScalarValue::Float32(_) | ScalarValue::Float64(_)
            ) || matches!(
                right,
                ScalarValue::Float32(_) | ScalarValue::Float64(_)
            ) =>
        {
            float_binary(first, op, second)
        }
        _ => match (as_i64(left), as_i64(right)) {
            (Some(first), Some(second)) => int_binary(first, op, second),
            _ => ScalarOut::Unknown,
        },
    }
}

fn eval_udf(function: &ScalarFunction, args: &[ScalarValue]) -> ScalarOut {
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::logical_expr::ScalarFunctionArgs;

    if function.func.name() == super::store_cast::STORE_INT_GUARD_NAME
        || function.func.name() == super::store_cast::STORE_INT32_NAME
        || function.func.name() == super::store_cast::STORE_INT64_NAME
    {
        return ScalarOut::Unknown;
    }
    let types: Vec<DataType> = args.iter().map(ScalarValue::data_type).collect();
    let Ok(return_type) = function.func.return_type(&types) else {
        return ScalarOut::Unknown;
    };
    let call = ScalarFunctionArgs {
        args: args
            .iter()
            .cloned()
            .map(ColumnarValue::Scalar)
            .collect(),
        arg_fields: types
            .iter()
            .map(|data_type| Arc::new(Field::new("a", data_type.clone(), true)))
            .collect(),
        number_rows: 1,
        return_field: Arc::new(Field::new("r", return_type, true)),
        config_options: Arc::new(ConfigOptions::new()),
    };
    match function.func.invoke_with_args(call) {
        Ok(ColumnarValue::Scalar(scalar)) => ScalarOut::Value(scalar),
        Ok(ColumnarValue::Array(array)) if array.len() == 1 => {
            match ScalarValue::try_from_array(array.as_ref(), 0) {
                Ok(scalar) => ScalarOut::Value(scalar),
                Err(_) => ScalarOut::Unknown,
            }
        }
        _ => ScalarOut::Unknown,
    }
}

fn eval_pure(expr: &Expr, depth: usize) -> ScalarOut {
    if depth > MAX_EVAL_DEPTH {
        return ScalarOut::Unknown;
    }
    let next = depth + 1;
    match expr {
        Expr::Literal(scalar, _) => ScalarOut::Value(scalar.clone()),
        Expr::Alias(alias) => eval_pure(&alias.expr, next),
        Expr::Negative(inner) => match eval_pure(inner, next) {
            ScalarOut::Value(ScalarValue::Float32(Some(number))) => {
                ScalarOut::Value(ScalarValue::Float32(Some(-number)))
            }
            ScalarOut::Value(ScalarValue::Float64(Some(number))) => {
                ScalarOut::Value(ScalarValue::Float64(Some(number)))
            }
            ScalarOut::Value(ScalarValue::Int64(Some(number))) => number
                .checked_neg()
                .map_or(ScalarOut::Unknown, |negated| {
                    ScalarOut::Value(ScalarValue::Int64(Some(negated)))
                }),
            ScalarOut::Value(ScalarValue::Decimal128(Some(number), precision, scale)) => number
                .checked_neg()
                .map_or(ScalarOut::Unknown, |negated| {
                    ScalarOut::Value(ScalarValue::Decimal128(Some(negated), *precision, *scale))
                }),
            ScalarOut::Value(scalar) if scalar.is_null() => {
                ScalarOut::Value(ScalarValue::Null)
            }
            other => other,
        },
        Expr::Cast(cast) => match eval_pure(&cast.expr, next) {
            ScalarOut::Value(scalar) => cast_scalar_value(&scalar, cast.field.data_type())
                .map_or(ScalarOut::Unknown, ScalarOut::Value),
            other => other,
        },
        Expr::TryCast(_) => ScalarOut::Unknown,
        Expr::BinaryExpr(binary) => match (
            eval_pure(&binary.left, next),
            eval_pure(&binary.right, next),
        ) {
            (ScalarOut::Value(left), ScalarOut::Value(right)) => {
                eval_binary(&left, binary.op, &right)
            }
            (ScalarOut::DivZero, _) | (_, ScalarOut::DivZero) => ScalarOut::DivZero,
            _ => ScalarOut::Unknown,
        },
        Expr::Case(case) => eval_case(case, next),
        Expr::ScalarFunction(function) => {
            if function.func.name() == ANSI_GUARD_NAME && function.args.len() == 1 {
                return match eval_pure(&function.args[0], next) {
                    ScalarOut::Value(scalar) if scalar_is_zero(&scalar) => ScalarOut::DivZero,
                    ScalarOut::Value(scalar) => ScalarOut::Value(scalar),
                    other => other,
                };
            }
            let mut args = Vec::with_capacity(function.args.len());
            for arg in &function.args {
                match eval_pure(arg, next) {
                    ScalarOut::Value(scalar) => args.push(scalar),
                    ScalarOut::DivZero => return ScalarOut::DivZero,
                    ScalarOut::Unknown => return ScalarOut::Unknown,
                }
            }
            eval_udf(function, &args)
        }
        _ => ScalarOut::Unknown,
    }
}

fn eval_case(case: &Case, depth: usize) -> ScalarOut {
    if let Some(operand) = case.expr.as_ref() {
        return eval_simple_case(operand, case, depth);
    }
    for (when, then) in &case.when_then {
        match eval_pure(when, depth) {
            ScalarOut::Value(ScalarValue::Boolean(Some(true))) => {
                return eval_pure(then, depth);
            }
            ScalarOut::Value(ScalarValue::Boolean(Some(false)))
            | ScalarOut::Value(ScalarValue::Boolean(None))
            | ScalarOut::Value(ScalarValue::Null) => continue,
            ScalarOut::Value(_) | ScalarOut::DivZero | ScalarOut::Unknown => {
                return ScalarOut::Unknown;
            }
        }
    }
    case.else_expr.as_ref().map_or(
        ScalarOut::Value(ScalarValue::Null),
        |or_else| eval_pure(or_else, depth),
    )
}

fn scalar_is_nan(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Float32(Some(number)) if number.is_nan()
    ) || matches!(
        value,
        ScalarValue::Float64(Some(number)) if number.is_nan()
    )
}

fn eval_simple_case(operand: &Expr, case: &Case, depth: usize) -> ScalarOut {
    let ScalarOut::Value(target) = eval_pure(operand, depth) else {
        return ScalarOut::Unknown;
    };
    if target.is_null() || scalar_is_nan(&target) {
        return ScalarOut::Unknown;
    }
    for (when, then) in &case.when_then {
        let ScalarOut::Value(candidate) = eval_pure(when, depth) else {
            return ScalarOut::Unknown;
        };
        if candidate.is_null() || scalar_is_nan(&candidate) {
            continue;
        }
        if candidate.data_type() != target.data_type() {
            return ScalarOut::Unknown;
        }
        if candidate == target {
            return eval_pure(then, depth);
        }
    }
    case.else_expr.as_ref().map_or(
        ScalarOut::Value(ScalarValue::Null),
        |or_else| eval_pure(or_else, depth),
    )
}

#[derive(Debug, Clone, PartialEq)]
enum TopVerdict {
    Overflow,
    DivZero,
    Clean,
}

fn check_final_value(value: &ScalarValue, target: &DataType) -> TopVerdict {
    match value {
        ScalarValue::Float32(_)
        | ScalarValue::Float64(_)
        | ScalarValue::Decimal128(_, _, _)
        | ScalarValue::Decimal256(_, _, _) => {
            if cast_scalar_value(value, target).is_some() {
                TopVerdict::Clean
            } else {
                TopVerdict::Overflow
            }
        }
        _ => TopVerdict::Clean,
    }
}

fn lookup_definition(
    node: &LogicalPlan,
    index: usize,
    descend_from: bool,
) -> Option<(Expr, &LogicalPlan)> {
    match node {
        LogicalPlan::Projection(projection) => {
            let definition = projection.expr.get(index)?.clone();
            Some((definition, projection.input.as_ref()))
        }
        LogicalPlan::SubqueryAlias(_) => {
            if !descend_from {
                return None;
            }
            let input = node.inputs().first()?;
            lookup_definition(input, index, descend_from)
        }
        LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Distinct(_) => {
            let input = node.inputs().first()?;
            lookup_definition(input, index, descend_from)
        }
        LogicalPlan::Join(join) => {
            if !descend_from {
                return None;
            }
            let width = join.left.schema().fields().len();
            if index < width {
                lookup_definition(&join.left, index, descend_from)
            } else {
                lookup_definition(&join.right, index - width, descend_from)
            }
        }
        _ => None,
    }
}

fn values_column_cells(values: &[Vec<Expr>], index: usize) -> Option<Vec<Expr>> {
    let mut cells = Vec::with_capacity(values.len());
    for row in values {
        cells.push(row.get(index)?.clone());
    }
    Some(cells)
}

fn zip_rows(
    left: Vec<Expr>,
    right: Vec<Expr>,
    build: &impl Fn(Expr, Expr) -> Expr,
) -> Option<Vec<Expr>> {
    match (left.len(), right.len()) {
        (1, 1) => Some(vec![build(
            left.into_iter().next()?,
            right.into_iter().next()?,
        )]),
        (1, _) => {
            let only = left.into_iter().next()?;
            Some(right.into_iter().map(|item| build(only.clone(), item)).collect())
        }
        (_, 1) => {
            let only = right.into_iter().next()?;
            Some(left.into_iter().map(|item| build(item, only.clone())).collect())
        }
        (same, other) if same == other => Some(
            left.into_iter()
                .zip(right)
                .map(|(first, second)| build(first, second))
                .collect(),
        ),
        _ => None,
    }
}

fn substitute_lineage(
    expr: &Expr,
    scope: &LogicalPlan,
    seen_values: &mut Option<Vec<Vec<Expr>>>,
    descend_from: bool,
    depth: usize,
) -> Option<Vec<Expr>> {
    if depth > MAX_LINEAGE_DEPTH {
        return None;
    }
    let next = depth + 1;
    match expr {
        Expr::Column(column) => {
            let index = scope.schema().index_of_column(column).ok()?;
            if let Some((definition, inner)) = lookup_definition(scope, index, descend_from) {
                return substitute_lineage(&definition, inner, seen_values, descend_from, next);
            }
            match scope {
                LogicalPlan::Values(values) => {
                    if let Some(seen) = seen_values {
                        if *seen != values.values {
                            return None;
                        }
                    } else {
                        *seen_values = Some(values.values.clone());
                    }
                    values_column_cells(&values.values, index)
                }
                _ => None,
            }
        }
        Expr::Alias(alias) => substitute_lineage(&alias.expr, scope, seen_values, descend_from, next),
        Expr::Cast(cast) => {
            let target = cast.field.data_type().clone();
            Some(
                substitute_lineage(&cast.expr, scope, seen_values, descend_from, next)?
                    .into_iter()
                    .map(|inner| {
                        Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(inner), target.clone()))
                    })
                    .collect(),
            )
        }
        Expr::TryCast(cast) => {
            let target = cast.field.data_type().clone();
            Some(
                substitute_lineage(&cast.expr, scope, seen_values, descend_from, next)?
                    .into_iter()
                    .map(|inner| {
                        Expr::TryCast(datafusion::logical_expr::TryCast::new(
                            Box::new(inner),
                            target.clone(),
                        ))
                    })
                    .collect(),
            )
        }
        Expr::Negative(inner) => Some(
            substitute_lineage(inner, scope, seen_values, descend_from, next)?
                .into_iter()
                .map(|item| Expr::Negative(Box::new(item)))
                .collect(),
        ),
        Expr::Not(inner) => Some(
            substitute_lineage(inner, scope, seen_values, descend_from, next)?
                .into_iter()
                .map(|item| Expr::Not(Box::new(item)))
                .collect(),
        ),
        Expr::BinaryExpr(binary) => {
            let left = substitute_lineage(&binary.left, scope, seen_values, descend_from, next)?;
            let right = substitute_lineage(&binary.right, scope, seen_values, descend_from, next)?;
            let op = binary.op;
            zip_rows(left, right, &|first, second| {
                Expr::BinaryExpr(BinaryExpr::new(Box::new(first), op, Box::new(second)))
            })
        }
        Expr::Case(case) => substitute_case(case, scope, seen_values, descend_from, next),
        Expr::ScalarFunction(function) => substitute_call(function, scope, seen_values, descend_from, next),
        Expr::Literal(_, _) => Some(vec![expr.clone()]),
        _ => None,
    }
}

fn substitute_case(
    case: &Case,
    scope: &LogicalPlan,
    seen_values: &mut Option<Vec<Vec<Expr>>>,
    descend_from: bool,
    depth: usize,
) -> Option<Vec<Expr>> {
    let mut parts: Vec<Vec<Expr>> = Vec::new();
    match &case.expr {
        Some(operand) => parts.push(substitute_lineage(operand, scope, seen_values, descend_from, depth)?),
        None => parts.push(vec![Expr::Literal(ScalarValue::Null, None)]),
    }
    for (when, then) in &case.when_then {
        parts.push(substitute_lineage(when, scope, seen_values, descend_from, depth)?);
        parts.push(substitute_lineage(then, scope, seen_values, descend_from, depth)?);
    }
    match &case.else_expr {
        Some(or_else) => parts.push(substitute_lineage(or_else, scope, seen_values, descend_from, depth)?),
        None => parts.push(vec![Expr::Literal(ScalarValue::Null, None)]),
    }
    let rows = zip_parts(parts)?;
    let has_operand = case.expr.is_some();
    let has_else = case.else_expr.is_some();
    let pairs = case.when_then.len();
    Some(
        rows.into_iter()
            .map(|mut items| {
                let or_else = items.pop();
                let mut when_then = Vec::with_capacity(pairs);
                for _ in 0..pairs {
                    let then = items.pop();
                    let when = items.pop();
                    match (when, then) {
                        (Some(first), Some(second)) => {
                            when_then.push((Box::new(first), Box::new(second)));
                        }
                        _ => when_then.push((
                            Box::new(Expr::Literal(ScalarValue::Null, None)),
                            Box::new(Expr::Literal(ScalarValue::Null, None)),
                        )),
                    }
                }
                when_then.reverse();
                let operand = items.pop().filter(|_| has_operand).map(Box::new);
                Expr::Case(Case {
                    expr: operand,
                    when_then,
                    else_expr: or_else.filter(|_| has_else).map(Box::new),
                })
            })
            .collect(),
    )
}

fn zip_parts(parts: Vec<Vec<Expr>>) -> Option<Vec<Vec<Expr>>> {
    let mut width = 1;
    for part in &parts {
        if part.len() != 1 {
            if width != 1 && part.len() != width {
                return None;
            }
            width = part.len();
        }
    }
    let mut rows: Vec<Vec<Expr>> = vec![Vec::with_capacity(parts.len()); width];
    for part in parts {
        if part.len() == 1 {
            let only = part.into_iter().next()?;
            for row in &mut rows {
                row.push(only.clone());
            }
        } else {
            for (row, item) in rows.iter_mut().zip(part) {
                row.push(item);
            }
        }
    }
    Some(rows)
}

fn eval_store_value(
    expr: &Expr,
    scope: &LogicalPlan,
    target: &DataType,
    descend_from: bool,
) -> TopVerdict {
    let mut seen_values = None;
    let Some(trees) = substitute_lineage(expr, scope, &mut seen_values, descend_from, 0) else {
        return TopVerdict::Clean;
    };
    for tree in trees {
        match eval_pure(&tree, 0) {
            ScalarOut::Value(value) => match check_final_value(&value, target) {
                TopVerdict::Clean => continue,
                TopVerdict::Overflow => return TopVerdict::Overflow,
                TopVerdict::DivZero => return TopVerdict::DivZero,
            },
            ScalarOut::DivZero => return TopVerdict::DivZero,
            ScalarOut::Unknown => continue,
        }
    }
    TopVerdict::Clean
}

fn collect_value_columns(expr: &Expr, columns: &mut Vec<Column>) {
    match expr {
        Expr::Column(column) => columns.push(column.clone()),
        Expr::Alias(alias) => collect_value_columns(&alias.expr, columns),
        Expr::Cast(cast) => collect_value_columns(&cast.expr, columns),
        Expr::TryCast(cast) => collect_value_columns(&cast.expr, columns),
        Expr::Negative(inner) | Expr::Not(inner) => collect_value_columns(inner, columns),
        Expr::BinaryExpr(binary) => {
            collect_value_columns(&binary.left, columns);
            collect_value_columns(&binary.right, columns);
        }
        Expr::Case(case) => {
            for (_, then) in &case.when_then {
                collect_value_columns(then, columns);
            }
            if let Some(or_else) = &case.else_expr {
                collect_value_columns(or_else, columns);
            }
        }
        Expr::ScalarFunction(function) => {
            for arg in &function.args {
                collect_value_columns(arg, columns);
            }
        }
        Expr::ScalarSubquery(_) => {}
        _ => {}
    }
}

struct ScopeRewrite<'a> {
    policy: &'a StorePolicy,
    values_door: bool,
}

impl ScopeRewrite<'_> {
    fn swap_guards(&self, expr: Expr, ctx: &StoreCtx) -> Result<Expr> {
        match expr {
            Expr::Column(_) | Expr::Literal(_, _) | Expr::Placeholder(_) => Ok(expr),
            Expr::Alias(alias) => {
                let inner = self.swap_guards(*alias.expr, ctx)?;
                Ok(Expr::Alias(Alias {
                    expr: Box::new(inner),
                    ..alias
                }))
            }
            Expr::Cast(cast) => {
                let inner = self.swap_guards(*cast.expr, ctx)?;
                Ok(Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(inner),
                    cast.field.data_type().clone(),
                )))
            }
            Expr::TryCast(cast) => {
                let inner = self.swap_guards(*cast.expr, ctx)?;
                Ok(Expr::TryCast(datafusion::logical_expr::TryCast::new(
                    Box::new(inner),
                    cast.field.data_type().clone(),
                )))
            }
            Expr::Negative(inner) => Ok(Expr::Negative(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::Not(inner) => Ok(Expr::Not(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::BinaryExpr(binary) => {
                let left = self.swap_guards(*binary.left, ctx)?;
                let right = self.swap_guards(*binary.right, ctx)?;
                Ok(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left),
                    binary.op,
                    Box::new(right),
                )))
            }
            Expr::Case(case) => {
                let mut when_then = Vec::with_capacity(case.when_then.len());
                for (when, then) in case.when_then {
                    when_then.push((when, Box::new(self.swap_guards(*then, ctx)?)));
                }
                let else_expr = case
                    .else_expr
                    .map(|or_else| self.swap_guards(*or_else, ctx).map(Box::new))
                    .transpose()?;
                Ok(Expr::Case(Case {
                    expr: case.expr,
                    when_then,
                    else_expr,
                }))
            }
            Expr::ScalarFunction(function) => {
                if function.func.name() == ANSI_GUARD_NAME && function.args.len() == 1 {
                    let mut args = function.args;
                    let divisor = args.pop();
                    match divisor {
                        Some(first) => {
                            let swapped = self.swap_guards(first, ctx)?;
                            Ok(store_guard_call(ctx, swapped))
                        }
                        None => Ok(Expr::ScalarFunction(function)),
                    }
                } else {
                    let mut args = Vec::with_capacity(function.args.len());
                    for arg in function.args {
                        args.push(self.swap_guards(arg, ctx)?);
                    }
                    let mut rebuilt = function;
                    rebuilt.args = args;
                    Ok(Expr::ScalarFunction(rebuilt))
                }
            }
            Expr::ScalarSubquery(subquery) => {
                let body = self.rewrite_plan_node(
                    Arc::unwrap_or_clone(subquery.subquery),
                    &[(0, ctx.clone())],
                    false,
                )?;
                Ok(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(body),
                    outer_ref_columns: subquery.outer_ref_columns,
                    spans: subquery.spans,
                }))
            }
            Expr::InList(list) => {
                let expr = Box::new(self.swap_guards(*list.expr, ctx)?);
                let mut items = Vec::with_capacity(list.list.len());
                for item in list.list {
                    items.push(self.swap_guards(item, ctx)?);
                }
                Ok(Expr::InList(datafusion::logical_expr::InList {
                    expr,
                    list: items,
                    negated: list.negated,
                }))
            }
            Expr::Between(between) => Ok(Expr::Between(datafusion::logical_expr::Between {
                expr: Box::new(self.swap_guards(*between.expr, ctx)?),
                negated: between.negated,
                low: Box::new(self.swap_guards(*between.low, ctx)?),
                high: Box::new(self.swap_guards(*between.high, ctx)?),
            })),
            Expr::Like(like) => {
                let expr = Box::new(self.swap_guards(*like.expr, ctx)?);
                let pattern = Box::new(self.swap_guards(*like.pattern, ctx)?);
                Ok(Expr::Like(datafusion::logical_expr::Like {
                    expr,
                    pattern,
                    ..like
                }))
            }
            Expr::SimilarTo(like) => {
                let expr = Box::new(self.swap_guards(*like.expr, ctx)?);
                let pattern = Box::new(self.swap_guards(*like.pattern, ctx)?);
                Ok(Expr::SimilarTo(datafusion::logical_expr::Like {
                    expr,
                    pattern,
                    ..like
                }))
            }
            Expr::IsNull(inner) => Ok(Expr::IsNull(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::IsTrue(inner) => Ok(Expr::IsTrue(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::IsNotTrue(inner) => Ok(Expr::IsNotTrue(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::IsFalse(inner) => Ok(Expr::IsFalse(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::IsNotFalse(inner) => Ok(Expr::IsNotFalse(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::IsUnknown(inner) => Ok(Expr::IsUnknown(Box::new(self.swap_guards(*inner, ctx)?))),
            Expr::IsNotUnknown(inner) => {
                Ok(Expr::IsNotUnknown(Box::new(self.swap_guards(*inner, ctx)?)))
            }
            other => Ok(other),
        }
    }

    fn rewrite_plan_node(
        &self,
        plan: LogicalPlan,
        map: &[(usize, StoreCtx)],
    ) -> Result<LogicalPlan> {
        if map.is_empty() {
            return Ok(plan);
        }
        match plan {
            LogicalPlan::Projection(projection) => self.rewrite_projection(projection, map),
            LogicalPlan::SubqueryAlias(alias) => {
                if !self.policy.descend_from {
                    return Ok(LogicalPlan::SubqueryAlias(alias));
                }
                let input = self.rewrite_plan_node(Arc::unwrap_or_clone(alias.input), map)?;
                Ok(LogicalPlan::SubqueryAlias(
                    datafusion::logical_expr::SubqueryAlias::try_new(
                        Arc::new(input),
                        alias.alias,
                    )?,
                ))
            }
            LogicalPlan::Filter(filter) => {
                let input = self.rewrite_plan_node(Arc::unwrap_or_clone(filter.input), map)?;
                Ok(LogicalPlan::Filter(datafusion::logical_expr::Filter::try_new(
                    filter.predicate,
                    Arc::new(input),
                )?))
            }
            LogicalPlan::Sort(sort) => {
                let input = self.rewrite_plan_node(Arc::unwrap_or_clone(sort.input), map)?;
                Ok(LogicalPlan::Sort(datafusion::logical_expr::Sort {
                    expr: sort.expr,
                    input: Arc::new(input),
                    fetch: sort.fetch,
                    schema: sort.schema,
                }))
            }
            LogicalPlan::Limit(limit) => {
                let input = self.rewrite_plan_node(Arc::unwrap_or_clone(limit.input), map)?;
                Ok(LogicalPlan::Limit(datafusion::logical_expr::Limit {
                    skip: limit.skip,
                    fetch: limit.fetch,
                    input: Arc::new(input),
                    schema: limit.schema,
                }))
            }
            LogicalPlan::Distinct(distinct) => {
                let input = match distinct {
                    datafusion::logical_expr::Distinct::All(input) => {
                        let rewritten =
                            self.rewrite_plan_node(Arc::unwrap_or_clone(input), map)?;
                        datafusion::logical_expr::Distinct::All(Arc::new(rewritten))
                    }
                    datafusion::logical_expr::Distinct::On(on) => {
                        let rewritten =
                            self.rewrite_plan_node(Arc::unwrap_or_clone(on.input), map)?;
                        datafusion::logical_expr::Distinct::On(
                            datafusion::logical_expr::DistinctOn {
                                input: Arc::new(rewritten),
                                ..on
                            },
                        )
                    }
                };
                Ok(LogicalPlan::Distinct(input))
            }
            LogicalPlan::Join(join) => {
                if !self.policy.descend_from {
                    return Ok(LogicalPlan::Join(join));
                }
                let width = join.left.schema().fields().len();
                let mut left_map = Vec::new();
                let mut right_map = Vec::new();
                for (output, ctx) in map {
                    if *output < width {
                        left_map.push((*output, ctx.clone()));
                    } else {
                        right_map.push((*output - width, ctx.clone()));
                    }
                }
                let left = self.rewrite_plan_node(Arc::unwrap_or_clone(join.left), &left_map)?;
                let right =
                    self.rewrite_plan_node(Arc::unwrap_or_clone(join.right), &right_map)?;
                Ok(LogicalPlan::Join(datafusion::logical_expr::Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    ..join
                }))
            }
            other => Ok(other),
        }
    }

    fn rewrite_projection(
        &self,
        projection: Projection,
        map: &[(usize, StoreCtx)],
    ) -> Result<LogicalPlan> {
        let input_schema = projection.input.schema();
        let mut exprs = projection.expr;
        let mut claimed: Vec<usize> = Vec::new();
        let mut child_map: Vec<(usize, StoreCtx)> = Vec::new();
        for (output, ctx) in map {
            let Some(expr) = exprs.get(*output).cloned() else {
                continue;
            };
            if !self.values_door {
                let swapped = self.swap_guards(expr.clone(), ctx)?;
                exprs[*output] = swapped;
            }
            let mut columns = Vec::new();
            collect_value_columns(&expr, &mut columns);
            for column in columns {
                let Ok(index) = input_schema.index_of_column(&column) else {
                    continue;
                };
                if claimed.contains(&index) {
                    continue;
                }
                claimed.push(index);
                child_map.push((index, ctx.clone()));
            }
        }
        let input = if child_map.is_empty() {
            Arc::unwrap_or_clone(projection.input)
        } else {
            self.rewrite_plan_node(Arc::unwrap_or_clone(projection.input), &child_map)?
        };
        Ok(LogicalPlan::Projection(Projection::try_new(
            exprs,
            Arc::new(input),
        )?))
    }

    fn process_top_positions(
        &self,
        projection: Projection,
        targets: &[(usize, Field)],
    ) -> Result<LogicalPlan> {
        let input_schema = projection.input.schema();
        let mut exprs = projection.expr;
        let mut map: Vec<(usize, StoreCtx)> = Vec::new();
        for (output, field) in targets {
            let Some(top) = exprs.get(*output).cloned() else {
                continue;
            };
            let (alias, peeled) = peel_alias(&top);
            let (inner, stored) = match peeled {
                Expr::Cast(cast) => match cast.expr.get_type(input_schema.as_ref()) {
                    Ok(data_type) => (cast.expr.as_ref().clone(), data_type),
                    Err(_) => continue,
                },
                Expr::ScalarFunction(function) => match arrow_cast_target(function) {
                    Some(_) => match function.args.first() {
                        Some(first) => match first.get_type(input_schema.as_ref()) {
                            Ok(data_type) => (first.clone(), data_type),
                            Err(_) => continue,
                        },
                        None => continue,
                    },
                    None => match peeled.get_type(input_schema.as_ref()) {
                        Ok(data_type) => (peeled.clone(), data_type),
                        Err(_) => continue,
                    },
                },
                _ => match peeled.get_type(input_schema.as_ref()) {
                    Ok(data_type) => (peeled.clone(), data_type),
                    Err(_) => continue,
                },
            };
            let Some(ctx) = store_ctx(field.name(), &stored, field.data_type()) else {
                continue;
            };
            let outer_cast = match peeled {
                Expr::Cast(_) => true,
                Expr::ScalarFunction(function) => arrow_cast_target(function).is_some(),
                _ => false,
            };
            if is_static_expr(&inner) {
                match eval_store_value(
                    &inner,
                    &projection.input,
                    field.data_type(),
                    self.policy.descend_from,
                ) {
                    TopVerdict::Overflow if self.policy.raise_overflow => {
                        return Err(store_overflow_error(&ctx.column, &ctx.source, &ctx.target));
                    }
                    TopVerdict::DivZero
                        if self.policy.raise_divzero && !self.values_door =>
                    {
                        return Err(store_overflow_error(&ctx.column, &ctx.source, &ctx.target));
                    }
                    _ => {}
                }
            }
            let swapped = if self.values_door {
                inner.clone()
            } else {
                self.swap_guards(inner.clone(), &ctx)?
            };
            let rebuilt = if outer_cast && !is_static_expr(&inner) {
                match store_cast_call(&ctx, swapped) {
                    Some(call) => call,
                    None => rebuild_outer(peeled, inner),
                }
            } else {
                rebuild_outer(peeled, swapped)
            };
            exprs[*output] = wrap_alias(alias, rebuilt);
            map.push((*output, ctx));
        }
        let mut claimed: Vec<usize> = Vec::new();
        let mut child_map: Vec<(usize, StoreCtx)> = Vec::new();
        for (output, ctx) in &map {
            let Some(top) = exprs.get(*output) else {
                continue;
            };
            let mut columns = Vec::new();
            collect_value_columns(top, &mut columns);
            for column in columns {
                let Ok(index) = input_schema.index_of_column(&column) else {
                    continue;
                };
                if claimed.contains(&index) {
                    continue;
                }
                claimed.push(index);
                child_map.push((index, ctx.clone()));
            }
        }
        let input = if child_map.is_empty() {
            Arc::unwrap_or_clone(projection.input)
        } else {
            self.rewrite_plan_node(Arc::unwrap_or_clone(projection.input), &child_map)?
        };
        Ok(LogicalPlan::Projection(Projection::try_new(
            exprs,
            Arc::new(input),
        )?))
    }

    fn process_plan_top(
        &self,
        plan: LogicalPlan,
        targets: &[(usize, Field)],
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection(projection) => self.process_top_positions(projection, targets),
            LogicalPlan::Filter(filter) => {
                let input = self.process_plan_top(Arc::unwrap_or_clone(filter.input), targets)?;
                Ok(LogicalPlan::Filter(datafusion::logical_expr::Filter::try_new(
                    filter.predicate,
                    Arc::new(input),
                )?))
            }
            LogicalPlan::Sort(sort) => {
                let input = self.process_plan_top(Arc::unwrap_or_clone(sort.input), targets)?;
                Ok(LogicalPlan::Sort(datafusion::logical_expr::Sort {
                    expr: sort.expr,
                    input: Arc::new(input),
                    fetch: sort.fetch,
                    schema: sort.schema,
                }))
            }
            LogicalPlan::Limit(limit) => {
                let input = self.process_plan_top(Arc::unwrap_or_clone(limit.input), targets)?;
                Ok(LogicalPlan::Limit(datafusion::logical_expr::Limit {
                    skip: limit.skip,
                    fetch: limit.fetch,
                    input: Arc::new(input),
                    schema: limit.schema,
                }))
            }
            LogicalPlan::Distinct(distinct) => match distinct {
                datafusion::logical_expr::Distinct::All(input) => {
                    let rewritten =
                        self.process_plan_top(Arc::unwrap_or_clone(input), targets)?;
                    Ok(LogicalPlan::Distinct(
                        datafusion::logical_expr::Distinct::All(Arc::new(rewritten)),
                    ))
                }
                datafusion::logical_expr::Distinct::On(on) => {
                    let rewritten =
                        self.process_plan_top(Arc::unwrap_or_clone(on.input), targets)?;
                    Ok(LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(
                        datafusion::logical_expr::DistinctOn {
                            input: Arc::new(rewritten),
                            ..on
                        },
                    )))
                }
            },
            LogicalPlan::SubqueryAlias(alias) => {
                let input = self.process_plan_top(Arc::unwrap_or_clone(alias.input), targets)?;
                Ok(LogicalPlan::SubqueryAlias(
                    datafusion::logical_expr::SubqueryAlias::try_new(Arc::new(input), alias.alias)?,
                ))
            }
            other => Ok(other),
        }
    }
}

fn rebuild_outer(peeled: &Expr, inner: Expr) -> Expr {
    match peeled {
        Expr::Cast(cast) => Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(inner),
            cast.field.data_type().clone(),
        )),
        Expr::ScalarFunction(function) => {
            let mut rebuilt = function.clone();
            if let Some(first) = rebuilt.args.first_mut() {
                *first = inner;
            }
            Expr::ScalarFunction(rebuilt)
        }
        _ => inner,
    }
}

fn eval_values_cells(
    rows: &[Vec<Expr>],
    targets: &[Field],
    policy: &StorePolicy,
) -> Result<()> {
    for row in rows {
        for (cells, field) in row.iter().zip(targets.iter()) {
            if !is_static_expr(cells) {
                continue;
            }
            let stored = match cells.get_type(&datafusion::common::DFSchema::empty()) {
                Ok(data_type) => data_type,
                Err(_) => continue,
            };
            let Some(ctx) = store_ctx(field.name(), &stored, field.data_type()) else {
                continue;
            };
            match eval_pure(cells, 0) {
                ScalarOut::Value(value) => {
                    if matches!(check_final_value(&value, field.data_type()), TopVerdict::Overflow)
                        && policy.raise_overflow
                    {
                        return Err(store_overflow_error(&ctx.column, &ctx.source, &ctx.target));
                    }
                }
                ScalarOut::DivZero | ScalarOut::Unknown => {}
            }
        }
    }
    Ok(())
}

fn rewrite_dml(dml: DmlStatement) -> Result<LogicalPlan> {
    let policy = match dml.op {
        WriteOp::Insert(_) => &DML_INSERT_POLICY,
        WriteOp::Update => &DML_UPDATE_POLICY,
        WriteOp::Delete => return Ok(LogicalPlan::Dml(dml)),
    };
    let targets: Vec<Field> = dml
        .target
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    match dml.input.as_ref() {
        LogicalPlan::Values(values) => {
            eval_values_cells(&values.values, &targets, policy)?;
            Ok(LogicalPlan::Dml(dml))
        }
        LogicalPlan::Projection(projection) => {
            let values_door = matches!(
                projection.input.as_ref(),
                LogicalPlan::Values(_)
            );
            let scope = ScopeRewrite {
                policy,
                values_door,
            };
            let mut positioned: Vec<(usize, Field)> = Vec::new();
            for (index, field) in targets.iter().enumerate() {
                positioned.push((index, field.clone()));
            }
            let input = scope.process_top_positions(projection.clone(), &positioned)?;
            let mut dml = dml;
            dml.input = Arc::new(input);
            Ok(LogicalPlan::Dml(dml))
        }
        _ => Ok(LogicalPlan::Dml(dml)),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn rewrite_store_positions(
    plan: LogicalPlan,
    targets: &[(usize, &Field)],
    policy: &StorePolicy,
) -> Result<LogicalPlan> {
    let positioned: Vec<(usize, Field)> = targets
        .iter()
        .map(|(output, field)| (*output, (*field).clone()))
        .collect();
    let scope = ScopeRewrite {
        policy,
        values_door: false,
    };
    scope.process_plan_top(plan, &positioned)
}

fn substitute_call(
    function: &ScalarFunction,
    scope: &LogicalPlan,
    seen_values: &mut Option<Vec<Vec<Expr>>>,
    descend_from: bool,
    depth: usize,
) -> Option<Vec<Expr>> {
    let mut rows: Vec<Vec<Expr>> = vec![Vec::new()];
    for arg in &function.args {
        let arg_rows = substitute_lineage(arg, scope, seen_values, descend_from, depth)?;
        rows = zip_arg_lists(rows, arg_rows)?;
    }
    Some(
        rows.into_iter()
            .map(|args| {
                let mut rebuilt = function.clone();
                rebuilt.args = args;
                Expr::ScalarFunction(rebuilt)
            })
            .collect(),
    )
}

fn zip_arg_lists(left: Vec<Vec<Expr>>, right: Vec<Expr>) -> Option<Vec<Vec<Expr>>> {
    match (left.len(), right.len()) {
        (1, _) => Some(
            right
                .into_iter()
                .map(|item| {
                    let mut combined = left[0].clone();
                    combined.push(item);
                    combined
                })
                .collect(),
        ),
        (_, 1) => {
            let only = right.into_iter().next()?;
            Some(
                left.into_iter()
                    .map(|mut combined| {
                        combined.push(only.clone());
                        combined
                    })
                    .collect(),
            )
        }
        (same, other) if same == other => Some(
            left.into_iter()
                .zip(right)
                .map(|(mut combined, item)| {
                    combined.push(item);
                    combined
                })
                .collect(),
        ),
        _ => None,
    }
}
