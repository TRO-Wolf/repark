use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::{
    AggregateFunction, Alias, Between, BinaryExpr, Case, Cast, Exists, InList, InSubquery, Like,
    ScalarFunction, TryCast, WindowFunction,
};
use datafusion::logical_expr::{ColumnarValue, Expr, LogicalPlan, Operator, Volatility};
use datafusion::physical_expr::create_physical_expr;
use repark_common::udf_names::{
    CAST_MAP_NAME, NARROW_TIMESTAMP_NS_NAME, TIMESTAMP_NS_CAST_NAME, TIMESTAMP_NTZ_CAST_NAME,
    TIMESTAMPTZ_NS_CAST_NAME,
};
use repark_functions::ansi::spark_ansi_enabled_from_options;
use repark_functions::cast_map::EMPTY_MAP_CAST_NAME;
use repark_functions::timestamp_ntz_cast::TRY_TIMESTAMP_NTZ_CAST_NAME;

use super::walk_plan;

const PARSE_JAVA_DOUBLE: &str = "__repark_parse_java_double__";
const PARSE_JAVA_FLOAT: &str = "__repark_parse_java_float__";
const BOOL_TO_DECIMAL: &str = "__repark_bool_to_decimal__";
const FLOAT_TO_DECIMAL: &str = "__repark_float_to_decimal__";
const TRY_TIMESTAMP_NS_CAST: &str = "__repark_try_cast_timestamp_ns__";
const TRY_TIMESTAMPTZ_NS_CAST: &str = "__repark_try_cast_timestamptz_ns__";
const STRING_INTEGRAL_PREFIX: &str = "__repark_string_to_";
const NARROW_INTEGRAL_PREFIX: &str = "__repark_narrow_to_";
const NARROW_DECIMAL_PREFIX: &str = "__repark_narrow_to_decimal_";
const MAX_EXPR_DEPTH: usize = 128;

pub(super) type Env = [Option<ScalarValue>];

pub(super) struct EvalContext<'a> {
    pub(super) schema: &'a DFSchema,
    pub(super) env: &'a Env,
    pub(super) options: &'a ConfigOptions,
    pub(super) plan_depth: usize,
}

impl EvalContext<'_> {
    fn ansi_enabled(&self) -> bool {
        spark_ansi_enabled_from_options(self.options)
    }

    fn lookup_column(&self, column: &Column) -> Option<ScalarValue> {
        let index = self.schema.index_of_column(column).ok().or_else(|| {
            self.schema
                .index_of_column_by_name(column.relation.as_ref(), &column.name)
        })?;
        self.env.get(index)?.clone()
    }
}

pub(super) fn eval_expr(ctx: &EvalContext<'_>, expr: &Expr) -> Result<Option<ScalarValue>> {
    eval_expr_depth(ctx, expr, 0)
}

fn eval_expr_depth(
    ctx: &EvalContext<'_>,
    expr: &Expr,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    if depth > MAX_EXPR_DEPTH {
        return Ok(None);
    }
    match expr {
        Expr::Literal(scalar, _) => Ok(Some(scalar.clone())),
        Expr::Column(column) => Ok(ctx.lookup_column(column)),
        Expr::Alias(Alias { expr, .. }) => eval_expr_depth(ctx, expr, depth + 1),
        Expr::Cast(cast) => eval_cast(ctx, cast, depth + 1),
        Expr::TryCast(try_cast) => eval_try_cast(ctx, try_cast, depth + 1),
        Expr::Case(case) => eval_case(ctx, case, depth + 1),
        Expr::BinaryExpr(binary) => eval_binary(ctx, binary, depth + 1),
        Expr::ScalarFunction(function) => eval_scalar_function(ctx, function, depth + 1),
        Expr::Not(inner)
        | Expr::Negative(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => eval_unary_expr(ctx, expr, inner, depth + 1),
        Expr::Between(between) => eval_between(ctx, between, depth + 1),
        Expr::InList(in_list) => eval_in_list(ctx, in_list, depth + 1),
        Expr::Like(like) | Expr::SimilarTo(like) => eval_like(ctx, expr, like, depth + 1),
        Expr::ScalarSubquery(subquery) | Expr::Exists(Exists { subquery, .. }) => {
            walk_subquery(ctx, &subquery.subquery)?;
            Ok(None)
        }
        Expr::InSubquery(InSubquery { expr, subquery, .. }) => {
            eval_expr_depth(ctx, expr, depth + 1)?;
            walk_subquery(ctx, &subquery.subquery)?;
            Ok(None)
        }
        Expr::AggregateFunction(AggregateFunction { params, .. }) => {
            for arg in &params.args {
                eval_expr_depth(ctx, arg, depth + 1)?;
            }
            if let Some(filter) = params.filter.as_deref() {
                eval_expr_depth(ctx, filter, depth + 1)?;
            }
            Ok(None)
        }
        Expr::WindowFunction(window) => eval_window_function(ctx, window, depth + 1),
        _ => Ok(None),
    }
}

fn walk_subquery(ctx: &EvalContext<'_>, plan: &LogicalPlan) -> Result<()> {
    let _ = walk_plan(plan, ctx.options, None, ctx.plan_depth + 1, true)?;
    Ok(())
}

fn eval_window_function(
    ctx: &EvalContext<'_>,
    window: &WindowFunction,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    for arg in &window.params.args {
        eval_expr_depth(ctx, arg, depth)?;
    }
    for part in &window.params.partition_by {
        eval_expr_depth(ctx, part, depth)?;
    }
    for sort in &window.params.order_by {
        eval_expr_depth(ctx, &sort.expr, depth)?;
    }
    if let Some(filter) = window.params.filter.as_deref() {
        eval_expr_depth(ctx, filter, depth)?;
    }
    Ok(None)
}

fn eval_unary_expr(
    ctx: &EvalContext<'_>,
    whole: &Expr,
    inner: &Expr,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    let Some(value) = eval_expr_depth(ctx, inner, depth)? else {
        return Ok(None);
    };
    let lit = Box::new(Expr::Literal(value, None));
    let rebuilt = match whole {
        Expr::Not(_) => Expr::Not(lit),
        Expr::Negative(_) => Expr::Negative(lit),
        Expr::IsNull(_) => Expr::IsNull(lit),
        Expr::IsNotNull(_) => Expr::IsNotNull(lit),
        Expr::IsTrue(_) => Expr::IsTrue(lit),
        Expr::IsFalse(_) => Expr::IsFalse(lit),
        Expr::IsNotTrue(_) => Expr::IsNotTrue(lit),
        Expr::IsNotFalse(_) => Expr::IsNotFalse(lit),
        Expr::IsUnknown(_) => Expr::IsUnknown(lit),
        Expr::IsNotUnknown(_) => Expr::IsNotUnknown(lit),
        _ => return Ok(None),
    };
    Ok(eval_literal_expr(ctx.options, &rebuilt).ok())
}

fn eval_between(
    ctx: &EvalContext<'_>,
    between: &Between,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    let expr_val = eval_expr_depth(ctx, &between.expr, depth)?;
    let low_val = eval_expr_depth(ctx, &between.low, depth)?;
    let high_val = eval_expr_depth(ctx, &between.high, depth)?;
    let (Some(expr_val), Some(low_val), Some(high_val)) = (expr_val, low_val, high_val) else {
        return Ok(None);
    };
    let rebuilt = Expr::Between(Between {
        expr: Box::new(Expr::Literal(expr_val, None)),
        negated: between.negated,
        low: Box::new(Expr::Literal(low_val, None)),
        high: Box::new(Expr::Literal(high_val, None)),
    });
    Ok(eval_literal_expr(ctx.options, &rebuilt).ok())
}

fn eval_in_list(
    ctx: &EvalContext<'_>,
    in_list: &InList,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    let expr_val = eval_expr_depth(ctx, &in_list.expr, depth)?;
    let mut item_vals = Vec::with_capacity(in_list.list.len());
    let mut all_const = expr_val.is_some();
    for item in &in_list.list {
        let value = eval_expr_depth(ctx, item, depth)?;
        if value.is_none() {
            all_const = false;
        }
        item_vals.push(value);
    }
    if !all_const {
        return Ok(None);
    }
    let Some(expr_val) = expr_val else {
        return Ok(None);
    };
    let list = item_vals
        .into_iter()
        .flatten()
        .map(|scalar| Expr::Literal(scalar, None))
        .collect();
    let rebuilt = Expr::InList(InList {
        expr: Box::new(Expr::Literal(expr_val, None)),
        list,
        negated: in_list.negated,
    });
    Ok(eval_literal_expr(ctx.options, &rebuilt).ok())
}

fn eval_like(
    ctx: &EvalContext<'_>,
    whole: &Expr,
    like: &Like,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    let expr_val = eval_expr_depth(ctx, &like.expr, depth)?;
    let pattern_val = eval_expr_depth(ctx, &like.pattern, depth)?;
    let (Some(expr_val), Some(pattern_val)) = (expr_val, pattern_val) else {
        return Ok(None);
    };
    let rebuilt_like = Like {
        negated: like.negated,
        expr: Box::new(Expr::Literal(expr_val, None)),
        pattern: Box::new(Expr::Literal(pattern_val, None)),
        escape_char: like.escape_char,
        case_insensitive: like.case_insensitive,
    };
    let rebuilt = match whole {
        Expr::Like(_) => Expr::Like(rebuilt_like),
        Expr::SimilarTo(_) => Expr::SimilarTo(rebuilt_like),
        _ => return Ok(None),
    };
    Ok(eval_literal_expr(ctx.options, &rebuilt).ok())
}

fn eval_binary(
    ctx: &EvalContext<'_>,
    binary: &BinaryExpr,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    if matches!(binary.op, Operator::And | Operator::Or) {
        return eval_boolean_binary(ctx, binary, depth);
    }
    let left_val = eval_expr_depth(ctx, &binary.left, depth)?;
    let right_val = eval_expr_depth(ctx, &binary.right, depth)?;
    let (Some(left_val), Some(right_val)) = (left_val, right_val) else {
        return Ok(None);
    };
    let rebuilt = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Literal(left_val, None)),
        op: binary.op,
        right: Box::new(Expr::Literal(right_val, None)),
    });
    Ok(eval_literal_expr(ctx.options, &rebuilt).ok())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TriBool {
    True,
    False,
    Null,
}

fn eval_boolean_binary(
    ctx: &EvalContext<'_>,
    binary: &BinaryExpr,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    let whole_foldable =
        is_expr_foldable(ctx, &binary.left, 0) && is_expr_foldable(ctx, &binary.right, 0);
    if !whole_foldable {
        eval_expr_depth(ctx, &binary.left, depth)?;
        eval_expr_depth(ctx, &binary.right, depth)?;
        return Ok(None);
    }
    let left_val = eval_expr_depth(ctx, &binary.left, depth)?;
    let left_bool = scalar_as_tri_bool(left_val.as_ref());
    if binary.op == Operator::And && left_bool == Some(TriBool::False) {
        return Ok(Some(ScalarValue::Boolean(Some(false))));
    }
    if binary.op == Operator::Or && left_bool == Some(TriBool::True) {
        return Ok(Some(ScalarValue::Boolean(Some(true))));
    }
    let right_val = eval_expr_depth(ctx, &binary.right, depth)?;
    let right_bool = scalar_as_tri_bool(right_val.as_ref());
    let combined = match (binary.op, left_bool, right_bool) {
        (Operator::And, Some(TriBool::False), _)
        | (Operator::And, _, Some(TriBool::False))
        | (Operator::Or, Some(TriBool::False), Some(TriBool::False)) => {
            Some(ScalarValue::Boolean(Some(false)))
        }
        (Operator::And, Some(TriBool::True), Some(TriBool::True))
        | (Operator::Or, Some(TriBool::True), _)
        | (Operator::Or, _, Some(TriBool::True)) => Some(ScalarValue::Boolean(Some(true))),
        (Operator::And | Operator::Or, Some(_), Some(_)) => Some(ScalarValue::Boolean(None)),
        _ => None,
    };
    Ok(combined)
}

fn scalar_as_tri_bool(value: Option<&ScalarValue>) -> Option<TriBool> {
    match value {
        Some(ScalarValue::Boolean(Some(true))) => Some(TriBool::True),
        Some(ScalarValue::Boolean(Some(false))) => Some(TriBool::False),
        Some(ScalarValue::Boolean(None) | ScalarValue::Null) => Some(TriBool::Null),
        _ => None,
    }
}

fn eval_case(ctx: &EvalContext<'_>, case: &Case, depth: usize) -> Result<Option<ScalarValue>> {
    if let Some(base) = case.expr.as_deref() {
        if !is_expr_foldable(ctx, base, 0) {
            return Ok(None);
        }
        let Some(base_val) = eval_expr_depth(ctx, base, depth)? else {
            return Ok(None);
        };
        for (when_expr, then_expr) in &case.when_then_expr {
            if !is_expr_foldable(ctx, when_expr, 0) {
                return Ok(None);
            }
            let Some(when_val) = eval_expr_depth(ctx, when_expr, depth)? else {
                return Ok(None);
            };
            if !base_val.is_null()
                && !when_val.is_null()
                && scalars_equal(ctx.options, &base_val, &when_val)
            {
                return eval_expr_depth(ctx, then_expr, depth);
            }
        }
    } else {
        for (when_expr, then_expr) in &case.when_then_expr {
            if !is_expr_foldable(ctx, when_expr, 0) {
                return Ok(None);
            }
            let Some(when_val) = eval_expr_depth(ctx, when_expr, depth)? else {
                return Ok(None);
            };
            if matches!(when_val, ScalarValue::Boolean(Some(true))) {
                return eval_expr_depth(ctx, then_expr, depth);
            }
        }
    }
    match case.else_expr.as_deref() {
        Some(else_expr) => eval_expr_depth(ctx, else_expr, depth),
        None => Ok(Some(ScalarValue::Null)),
    }
}

fn scalars_equal(options: &ConfigOptions, left: &ScalarValue, right: &ScalarValue) -> bool {
    if left == right {
        return true;
    }
    let eq = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Literal(left.clone(), None)),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(right.clone(), None)),
    });
    matches!(
        eval_literal_expr(options, &eq),
        Ok(ScalarValue::Boolean(Some(true)))
    )
}

fn eval_cast(ctx: &EvalContext<'_>, cast: &Cast, depth: usize) -> Result<Option<ScalarValue>> {
    let Some(input) = eval_expr_depth(ctx, &cast.expr, depth)? else {
        return Ok(None);
    };
    let target = cast.field.data_type();
    if input.is_null() {
        return Ok(ScalarValue::try_from(target).ok());
    }
    if matches!(
        target,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    ) && is_non_finite_float(&input)
    {
        return Ok(ScalarValue::try_from(target).ok());
    }
    let rebuilt = Expr::Cast(Cast::new_from_field(
        Box::new(Expr::Literal(input.clone(), None)),
        Arc::clone(&cast.field),
    ));
    match eval_literal_expr(ctx.options, &rebuilt) {
        Ok(scalar) => Ok(Some(scalar)),
        Err(error) => {
            if !ctx.ansi_enabled() {
                return Ok(None);
            }
            Err(format_spark_cast_error(&input, target, error))
        }
    }
}

fn is_non_finite_float(input: &ScalarValue) -> bool {
    match input {
        ScalarValue::Float64(Some(value)) => !value.is_finite(),
        ScalarValue::Float32(Some(value)) => !value.is_finite(),
        _ => false,
    }
}

fn eval_try_cast(
    ctx: &EvalContext<'_>,
    try_cast: &TryCast,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    let Some(input) = eval_expr_depth(ctx, &try_cast.expr, depth)? else {
        return Ok(None);
    };
    let rebuilt = Expr::TryCast(TryCast::new_from_field(
        Box::new(Expr::Literal(input, None)),
        Arc::clone(&try_cast.field),
    ));
    Ok(eval_literal_expr(ctx.options, &rebuilt).ok())
}

fn eval_scalar_function(
    ctx: &EvalContext<'_>,
    function: &ScalarFunction,
    depth: usize,
) -> Result<Option<ScalarValue>> {
    let lower = function.func.name().to_ascii_lowercase();
    match lower.as_str() {
        "if" if function.args.len() == 3 => return eval_if(ctx, &function.args, depth),
        "nvl2" if function.args.len() == 3 => return eval_nvl2(ctx, &function.args, depth),
        "coalesce" | "nvl" | "ifnull" => return eval_coalesce(ctx, &function.args, depth),
        _ => {}
    }

    let mut arg_vals = Vec::with_capacity(function.args.len());
    let mut all_const = true;
    for arg in &function.args {
        let value = eval_expr_depth(ctx, arg, depth)?;
        if value.is_none() {
            all_const = false;
        }
        arg_vals.push(value);
    }
    if !all_const {
        return Ok(None);
    }

    let name = function.func.name();
    let is_cast = is_spark_cast_udf(name, &function.args);
    if !is_cast && function.func.signature().volatility == Volatility::Volatile {
        return Ok(None);
    }
    if name == FLOAT_TO_DECIMAL
        && let Some(Some(input)) = arg_vals.first()
    {
        return eval_float_to_decimal(ctx, function, input);
    }
    if is_single_arg_cast_udf(name)
        && let Some(Some(input)) = arg_vals.first()
        && input.is_null()
    {
        let target = function.func.return_type(&[input.data_type()]).ok();
        return Ok(target.and_then(|data_type| ScalarValue::try_from(&data_type).ok()));
    }

    let lit_args = arg_vals
        .iter()
        .flatten()
        .cloned()
        .map(|scalar| Expr::Literal(scalar, None))
        .collect();
    let rebuilt = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&function.func),
        lit_args,
    ));
    match eval_literal_expr(ctx.options, &rebuilt) {
        Ok(scalar) => Ok(Some(scalar)),
        Err(error) => {
            if !is_cast || !ctx.ansi_enabled() || is_try_cast_udf(name, &function.args) {
                return Ok(None);
            }
            Err(normalize_cast_udf_error(name, function, &arg_vals, error))
        }
    }
}

fn eval_if(ctx: &EvalContext<'_>, args: &[Expr], depth: usize) -> Result<Option<ScalarValue>> {
    if !is_expr_foldable(ctx, &args[0], 0) {
        return Ok(None);
    }
    let Some(cond) = eval_expr_depth(ctx, &args[0], depth)? else {
        return Ok(None);
    };
    if matches!(cond, ScalarValue::Boolean(Some(true))) {
        eval_expr_depth(ctx, &args[1], depth)
    } else {
        eval_expr_depth(ctx, &args[2], depth)
    }
}

fn eval_nvl2(ctx: &EvalContext<'_>, args: &[Expr], depth: usize) -> Result<Option<ScalarValue>> {
    if !is_expr_foldable(ctx, &args[0], 0) {
        return Ok(None);
    }
    let Some(probe) = eval_expr_depth(ctx, &args[0], depth)? else {
        return Ok(None);
    };
    if probe.is_null() {
        eval_expr_depth(ctx, &args[2], depth)
    } else {
        eval_expr_depth(ctx, &args[1], depth)
    }
}

fn eval_coalesce(
    ctx: &EvalContext<'_>,
    args: &[Expr],
    depth: usize,
) -> Result<Option<ScalarValue>> {
    for arg in args {
        if !is_expr_foldable(ctx, arg, 0) {
            return Ok(None);
        }
        let Some(value) = eval_expr_depth(ctx, arg, depth)? else {
            return Ok(None);
        };
        if !value.is_null() {
            return Ok(Some(value));
        }
    }
    Ok(Some(ScalarValue::Null))
}

fn eval_float_to_decimal(
    ctx: &EvalContext<'_>,
    function: &ScalarFunction,
    input: &ScalarValue,
) -> Result<Option<ScalarValue>> {
    let target = function.func.return_type(&[input.data_type()]).ok();
    if input.is_null() {
        return Ok(target.and_then(|data_type| ScalarValue::try_from(&data_type).ok()));
    }
    if is_non_finite_float(input) {
        return Ok(target.and_then(|data_type| ScalarValue::try_from(&data_type).ok()));
    }
    let rebuilt = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&function.func),
        vec![Expr::Literal(input.clone(), None)],
    ));
    match eval_literal_expr(ctx.options, &rebuilt) {
        Ok(scalar) => Ok(Some(scalar)),
        Err(error) => {
            if !ctx.ansi_enabled() {
                return Ok(None);
            }
            if let Some(
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
            ) = target
            {
                let rendered = format_float_value(input);
                return Err(DataFusionError::Execution(format!(
                    "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {rendered} cannot be \
                     represented as Decimal({precision}, {scale}). If necessary set \
                     \"spark.sql.ansi.enabled\" to \"false\" to bypass this error, and return \
                     NULL instead. SQLSTATE: 22003"
                )));
            }
            Err(error)
        }
    }
}

fn format_float_value(input: &ScalarValue) -> String {
    match input {
        ScalarValue::Float64(Some(value)) => value.to_string(),
        ScalarValue::Float32(Some(value)) => value.to_string(),
        other => other.to_string(),
    }
}

fn is_single_arg_cast_udf(name: &str) -> bool {
    matches!(
        name,
        PARSE_JAVA_DOUBLE
            | PARSE_JAVA_FLOAT
            | BOOL_TO_DECIMAL
            | FLOAT_TO_DECIMAL
            | TIMESTAMP_NTZ_CAST_NAME
            | TRY_TIMESTAMP_NTZ_CAST_NAME
            | TIMESTAMP_NS_CAST_NAME
            | TRY_TIMESTAMP_NS_CAST
            | TIMESTAMPTZ_NS_CAST_NAME
            | TRY_TIMESTAMPTZ_NS_CAST
            | NARROW_TIMESTAMP_NS_NAME
            | "to_timestamp"
            | "to_timestamp_ltz"
            | "to_timestamp_ntz"
            | "try_to_timestamp"
    ) || name.starts_with(STRING_INTEGRAL_PREFIX)
        || name.starts_with(NARROW_INTEGRAL_PREFIX)
        || name.starts_with(NARROW_DECIMAL_PREFIX)
}

fn is_spark_cast_udf(name: &str, args: &[Expr]) -> bool {
    if name == "to_timestamp" && args.len() != 1 {
        return false;
    }
    if name == CAST_MAP_NAME || name == EMPTY_MAP_CAST_NAME {
        return true;
    }
    is_single_arg_cast_udf(name)
}

fn is_try_cast_udf(name: &str, args: &[Expr]) -> bool {
    if matches!(
        name,
        TRY_TIMESTAMP_NTZ_CAST_NAME
            | TRY_TIMESTAMP_NS_CAST
            | TRY_TIMESTAMPTZ_NS_CAST
            | "try_to_timestamp"
    ) {
        return true;
    }
    if (name == CAST_MAP_NAME || name == EMPTY_MAP_CAST_NAME)
        && let Some(Expr::Literal(ScalarValue::Boolean(Some(true)), _)) = args.get(2)
    {
        return true;
    }
    false
}

fn is_expr_foldable(ctx: &EvalContext<'_>, expr: &Expr, depth: usize) -> bool {
    if depth > MAX_EXPR_DEPTH {
        return false;
    }
    match expr {
        Expr::Literal(_, _) => true,
        Expr::Column(column) => ctx.lookup_column(column).is_some(),
        Expr::Alias(Alias { expr, .. }) => is_expr_foldable(ctx, expr, depth + 1),
        Expr::Cast(cast) => is_expr_foldable(ctx, &cast.expr, depth + 1),
        Expr::TryCast(try_cast) => is_expr_foldable(ctx, &try_cast.expr, depth + 1),
        Expr::Not(inner)
        | Expr::Negative(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => is_expr_foldable(ctx, inner, depth + 1),
        Expr::BinaryExpr(binary) => {
            is_expr_foldable(ctx, &binary.left, depth + 1)
                && is_expr_foldable(ctx, &binary.right, depth + 1)
        }
        Expr::Between(between) => {
            is_expr_foldable(ctx, &between.expr, depth + 1)
                && is_expr_foldable(ctx, &between.low, depth + 1)
                && is_expr_foldable(ctx, &between.high, depth + 1)
        }
        Expr::InList(in_list) => {
            is_expr_foldable(ctx, &in_list.expr, depth + 1)
                && in_list
                    .list
                    .iter()
                    .all(|item| is_expr_foldable(ctx, item, depth + 1))
        }
        Expr::Like(like) | Expr::SimilarTo(like) => {
            is_expr_foldable(ctx, &like.expr, depth + 1)
                && is_expr_foldable(ctx, &like.pattern, depth + 1)
        }
        Expr::Case(case) => {
            case.expr
                .as_deref()
                .is_none_or(|base| is_expr_foldable(ctx, base, depth + 1))
                && case.when_then_expr.iter().all(|(when_expr, then_expr)| {
                    is_expr_foldable(ctx, when_expr, depth + 1)
                        && is_expr_foldable(ctx, then_expr, depth + 1)
                })
                && case
                    .else_expr
                    .as_deref()
                    .is_none_or(|else_expr| is_expr_foldable(ctx, else_expr, depth + 1))
        }
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            if !is_spark_cast_udf(name, &function.args)
                && function.func.signature().volatility == Volatility::Volatile
            {
                return false;
            }
            function
                .args
                .iter()
                .all(|arg| is_expr_foldable(ctx, arg, depth + 1))
        }
        _ => false,
    }
}

fn eval_literal_expr(options: &ConfigOptions, expr: &Expr) -> Result<ScalarValue> {
    let mut props = ExecutionProps::new();
    props.config_options = Some(Arc::new(options.clone()));
    let physical = create_physical_expr(expr, &DFSchema::empty(), &props)?;
    let batch_options = RecordBatchOptions::new().with_row_count(Some(1));
    let batch = RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::empty()),
        Vec::new(),
        &batch_options,
    )?;
    match physical.evaluate(&batch)? {
        ColumnarValue::Scalar(scalar) => Ok(scalar),
        ColumnarValue::Array(array) if !array.is_empty() => {
            ScalarValue::try_from_array(array.as_ref(), 0)
        }
        ColumnarValue::Array(_) => Err(DataFusionError::Internal(
            "constant evaluation produced an empty array".to_string(),
        )),
    }
}

fn normalize_cast_udf_error(
    name: &str,
    function: &ScalarFunction,
    arg_vals: &[Option<ScalarValue>],
    error: DataFusionError,
) -> DataFusionError {
    let text = error.to_string();
    if name == BOOL_TO_DECIMAL
        && text.contains("[NUMERIC_VALUE_OUT_OF_RANGE]")
        && !text.contains("[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]")
        && let Some(Some(input)) = arg_vals.first()
        && let Ok(DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale)) =
            function.func.return_type(&[input.data_type()])
    {
        return DataFusionError::Execution(format!(
            "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  1 cannot be represented as \
             Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" to \
             \"false\" to bypass this error, and return NULL instead. SQLSTATE: 22003"
        ));
    }
    if has_spark_error_class(&text) {
        return error;
    }
    if let Some(Some(input)) = arg_vals.first()
        && let Ok(target) = function.func.return_type(&[input.data_type()])
    {
        return format_spark_cast_error(input, &target, error);
    }
    error
}

pub(super) fn has_spark_error_class(message: &str) -> bool {
    message.contains("[CAST_INVALID_INPUT]")
        || message.contains("[CAST_OVERFLOW]")
        || message.contains("[NUMERIC_VALUE_OUT_OF_RANGE")
        || message.contains("[INVALID_INTERVAL_FORMAT")
        || message.contains("[DATATYPE_MISMATCH")
}

fn format_spark_cast_error(
    input: &ScalarValue,
    target: &DataType,
    error: DataFusionError,
) -> DataFusionError {
    let message = error.to_string();
    if has_spark_error_class(&message) {
        return error;
    }
    match (input, target) {
        (
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
        ) => {
            if is_valid_decimal_text(text) {
                DataFusionError::Execution(format!(
                    "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {} cannot be represented as \
                     Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" \
                     to \"false\" to bypass this error, and return NULL instead. SQLSTATE: 22003",
                    text.trim()
                ))
            } else {
                invalid_input_error(text, "STRING", &format!("DECIMAL({precision},{scale})"))
            }
        }
        (
            other,
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
        ) => DataFusionError::Execution(format!(
            "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {other} cannot be represented as \
             Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" to \
             \"false\" to bypass this error, and return NULL instead. SQLSTATE: 22003"
        )),
        (
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            DataType::Interval(_),
        ) => DataFusionError::Execution(format!(
            "[INVALID_INTERVAL_FORMAT.UNMATCHED_FORMAT_STRING_WITH_NOTICE] Error parsing '{text}' \
             to interval. SQLSTATE: 22006"
        )),
        (
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            _,
        ) => invalid_input_error(text, "STRING", &spark_type_name(target)),
        (
            ScalarValue::List(_)
            | ScalarValue::LargeList(_)
            | ScalarValue::FixedSizeList(_)
            | ScalarValue::Struct(_)
            | ScalarValue::Map(_),
            _,
        ) => {
            let lower = message.to_ascii_lowercase();
            if lower.contains("overflow") || lower.contains("too large") {
                DataFusionError::Execution(format!(
                    "[CAST_OVERFLOW] The value {input} of the type \"{}\" cannot be cast to \
                     \"{}\" due to an overflow. Use `try_cast` to tolerate overflow and return \
                     NULL instead. SQLSTATE: 22003",
                    spark_type_name(&input.data_type()),
                    spark_type_name(target)
                ))
            } else {
                DataFusionError::Execution(format!(
                    "[CAST_INVALID_INPUT] The value {input} of the type \"{}\" cannot be cast to \
                     \"{}\" because it is malformed. Correct the value as per the syntax, or \
                     change its target type. Use `try_cast` to tolerate malformed input and \
                     return NULL instead. SQLSTATE: 22018",
                    spark_type_name(&input.data_type()),
                    spark_type_name(target)
                ))
            }
        }
        (other, target) if other.data_type().is_numeric() && target.is_integer() => {
            DataFusionError::Execution(format!(
                "[CAST_OVERFLOW] The value {other} of the type \"{}\" cannot be cast to \"{}\" \
                 due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. \
                 SQLSTATE: 22003",
                spark_type_name(&other.data_type()),
                spark_type_name(target)
            ))
        }
        (other, target) => DataFusionError::Execution(format!(
            "[CAST_INVALID_INPUT] The value {other} of the type \"{}\" cannot be cast to \"{}\" \
             because it is malformed. SQLSTATE: 22018",
            spark_type_name(&other.data_type()),
            spark_type_name(target)
        )),
    }
}

fn invalid_input_error(text: &str, from: &str, to: &str) -> DataFusionError {
    let escaped = text.replace('\\', "\\\\").replace('\'', "\\'");
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{escaped}' of the type \"{from}\" cannot be cast to \
         \"{to}\" because it is malformed. Correct the value as per the syntax, or change its \
         target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018"
    ))
}

fn is_valid_decimal_text(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }
    let body = trimmed
        .strip_prefix('+')
        .or_else(|| trimmed.strip_prefix('-'))
        .unwrap_or(trimmed);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(pos) => (&body[..pos], Some(&body[pos + 1..])),
        None => (body, None),
    };
    let mut saw_digit = false;
    let mut saw_dot = false;
    for byte in mantissa.bytes() {
        if byte.is_ascii_digit() {
            saw_digit = true;
        } else if byte == b'.' && !saw_dot {
            saw_dot = true;
        } else {
            return false;
        }
    }
    if !saw_digit {
        return false;
    }
    if let Some(exp) = exponent {
        let exp_digits = exp
            .strip_prefix('+')
            .or_else(|| exp.strip_prefix('-'))
            .unwrap_or(exp);
        if exp_digits.is_empty() || !exp_digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return false;
        }
    }
    true
}

fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Map(_, _) => "MAP".to_string(),
        other => format!("{other}"),
    }
}
