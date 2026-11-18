use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, ExprSchemable, Operator};
use datafusion::prelude::SessionConfig;
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan};

use crate::java_double::SUFFIX_LITERAL_NAME;

use super::{adjust_precision_scale, decimal128_parts};

pub const PICK_MINIMUM_PRECISION_KEY: &str = "spark.sql.legacy.literal.pickMinimumPrecision";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkPickMinimumPrecision {
    pub enabled: bool,
}

impl Default for SparkPickMinimumPrecision {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl ConfigExtension for SparkPickMinimumPrecision {
    const PREFIX: &'static str = "repark.pick_minimum_precision";
}

impl ExtensionOptions for SparkPickMinimumPrecision {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option; set `{PICK_MINIMUM_PRECISION_KEY}` instead",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

pub fn write_pick_minimum_precision(options: &mut ConfigOptions, enabled: bool) {
    options
        .extensions
        .insert(SparkPickMinimumPrecision { enabled });
}

#[allow(clippy::missing_errors_doc)]
pub fn install_pick_minimum_precision<S: BuildHasher>(
    mut config: SessionConfig,
    conf: &HashMap<String, String, S>,
) -> Result<SessionConfig> {
    let Some(raw) = conf.get(PICK_MINIMUM_PRECISION_KEY) else {
        return Ok(config);
    };
    let enabled = crate::decimal_legacy::parse_decimal_truncate_legacy(raw).ok_or_else(|| {
        DataFusionError::Configuration(crate::merge_schema::boolean_type_mismatch_message(
            PICK_MINIMUM_PRECISION_KEY,
            &repark_common::redaction::mask_value_credentials(raw),
        ))
    })?;
    write_pick_minimum_precision(config.options_mut(), enabled);
    Ok(config)
}

pub(super) fn pick_minimum_precision(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<SparkPickMinimumPrecision>()
        .is_none_or(|flag| flag.enabled)
}

#[must_use]
pub fn dataframe_divide_operand(expr: Expr) -> Expr {
    let once = Expr::Cast(Cast::new(Box::new(expr), DataType::Float64));
    Expr::Cast(Cast::new(Box::new(once), DataType::Float64))
}

pub(super) fn rewrite_operators(
    expr: Expr,
    schema: &DFSchema,
    pick: bool,
) -> Result<Transformed<Expr>> {
    expr.transform_up(|node| Ok(operator_node(node, schema, pick)))?
        .transform_data(|lifted| rewrite_remainder_calls(lifted, schema, pick))
}

fn operator_node(node: Expr, schema: &DFSchema, pick: bool) -> Transformed<Expr> {
    let Expr::BinaryExpr(binary) = node else {
        return Transformed::no(node);
    };
    let (binary, unmarked) = match dataframe_divide(&binary, schema) {
        Some(plain) => (plain, true),
        None => (binary, false),
    };
    if !pick || !matches!(binary.op, Operator::Divide | Operator::Modulo) {
        return Transformed::new_transformed(Expr::BinaryExpr(binary), unmarked);
    }
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        return Transformed::new_transformed(Expr::BinaryExpr(binary), unmarked);
    };
    let BinaryExpr { left, op, right } = binary;
    let left = narrow_operand(*left, &right_type);
    let right = narrow_operand(*right, &left_type);
    let changed = unmarked || left.transformed || right.transformed;
    Transformed::new_transformed(
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left.data),
            op,
            Box::new(right.data),
        )),
        changed,
    )
}

fn dataframe_divide(binary: &BinaryExpr, schema: &DFSchema) -> Option<BinaryExpr> {
    if binary.op != Operator::Divide {
        return None;
    }
    let left = marked_operand(&binary.left)?;
    let right = marked_operand(&binary.right)?;
    let left_type = left.get_type(schema).ok()?;
    let right_type = right.get_type(schema).ok()?;
    if decimal_pair(&left_type, &right_type) || decimal_pair(&right_type, &left_type) {
        return Some(BinaryExpr::new(
            Box::new(left.clone()),
            Operator::Divide,
            Box::new(right.clone()),
        ));
    }
    Some(BinaryExpr::new(
        Box::new(Expr::Cast(Cast::new(
            Box::new(left.clone()),
            DataType::Float64,
        ))),
        Operator::Divide,
        Box::new(Expr::Cast(Cast::new(
            Box::new(right.clone()),
            DataType::Float64,
        ))),
    ))
}

fn marked_operand(expr: &Expr) -> Option<&Expr> {
    let Expr::Cast(outer) = expr else {
        return None;
    };
    let Expr::Cast(inner) = outer.expr.as_ref() else {
        return None;
    };
    (outer.field.data_type() == &DataType::Float64 && inner.field.data_type() == &DataType::Float64)
        .then_some(inner.expr.as_ref())
}

fn decimal_pair(decimal: &DataType, other: &DataType) -> bool {
    decimal128_parts(decimal).is_some()
        && (decimal128_parts(other).is_some() || other.is_integer() || other.is_null())
}

fn narrow_operand(operand: Expr, other: &DataType) -> Transformed<Expr> {
    if decimal128_parts(other).is_none() {
        return Transformed::no(operand);
    }
    match integer_literal_value(&operand).or_else(|| suffix_literal_value(&operand)) {
        Some(value) => Transformed::yes(cast_integer_literal_to_min_precision(operand, value)),
        None => Transformed::no(operand),
    }
}

fn rewrite_remainder_calls(expr: Expr, schema: &DFSchema, pick: bool) -> Result<Transformed<Expr>> {
    expr.transform_down(|node| remainder_node(node, schema, pick))
}

fn remainder_node(node: Expr, schema: &DFSchema, pick: bool) -> Result<Transformed<Expr>> {
    if let Expr::Cast(cast) = &node
        && let Expr::ScalarFunction(function) = cast.expr.as_ref()
        && is_remainder_call(function)
        && decimal128_parts(cast.field.data_type()).is_some()
    {
        return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
    }
    let Expr::ScalarFunction(function) = node else {
        return Ok(Transformed::no(node));
    };
    if !is_remainder_call(&function) || function.args.len() != 2 {
        return Ok(Transformed::no(Expr::ScalarFunction(function)));
    }
    let ScalarFunction { func, args } = function;
    let mut changed = false;
    let mut operands = Vec::with_capacity(2);
    for arg in args {
        let rewritten = rewrite_remainder_calls(arg, schema, pick)?;
        changed |= rewritten.transformed;
        operands.push(rewritten.data);
    }
    let types = (operands[0].get_type(schema), operands[1].get_type(schema));
    if pick && let (Ok(left_type), Ok(right_type)) = types {
        let right = narrow_operand(operands.remove(1), &left_type);
        let left = narrow_operand(operands.remove(0), &right_type);
        changed |= left.transformed || right.transformed;
        operands = vec![left.data, right.data];
    }
    let spark = remainder_type(&operands[0], &operands[1], schema);
    let operands = match <[Expr; 2]>::try_from(operands) {
        Ok([left, right]) if spark.is_some() && func.name() == "mod" => {
            return Ok(Transformed::new(
                remainder_operator(left, right, schema),
                true,
                TreeNodeRecursion::Jump,
            ));
        }
        Ok(pair) => Vec::from(pair),
        Err(operands) => operands,
    };
    let call = Expr::ScalarFunction(ScalarFunction::new_udf(func, operands));
    let Some((precision, scale)) = spark else {
        return Ok(Transformed::new(call, changed, TreeNodeRecursion::Jump));
    };
    Ok(Transformed::new(
        Expr::Cast(Cast::new(
            Box::new(call),
            DataType::Decimal128(precision, scale),
        )),
        true,
        TreeNodeRecursion::Jump,
    ))
}

fn remainder_operator(left: Expr, right: Expr, schema: &DFSchema) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(remainder_decimal_operand(left, schema)),
        Operator::Modulo,
        Box::new(remainder_decimal_operand(right, schema)),
    ))
}

fn remainder_decimal_operand(operand: Expr, schema: &DFSchema) -> Expr {
    let Ok(data_type) = operand.get_type(schema) else {
        return operand;
    };
    if decimal128_parts(&data_type).is_some() {
        return operand;
    }
    let precision = remainder_operand(&operand, &data_type)
        .and_then(|(precision, _)| u8::try_from(precision).ok());
    match precision {
        Some(precision) => Expr::Cast(Cast::new(
            Box::new(operand),
            DataType::Decimal128(precision, 0),
        )),
        None => operand,
    }
}

fn is_remainder_call(function: &ScalarFunction) -> bool {
    matches!(function.func.name(), "mod" | "pmod")
}

fn remainder_type(left: &Expr, right: &Expr, schema: &DFSchema) -> Option<(u8, i8)> {
    let left_type = left.get_type(schema).ok()?;
    let right_type = right.get_type(schema).ok()?;
    if decimal128_parts(&left_type).is_none() && decimal128_parts(&right_type).is_none() {
        return None;
    }
    let (left_precision, left_scale) = remainder_operand(left, &left_type)?;
    let (right_precision, right_scale) = remainder_operand(right, &right_type)?;
    let scale = left_scale.max(right_scale);
    let precision = (left_precision - left_scale).min(right_precision - right_scale) + scale;
    Some(adjust_precision_scale(precision, scale))
}

fn remainder_operand(expr: &Expr, data_type: &DataType) -> Option<(i32, i32)> {
    if let Some((precision, scale)) = decimal128_parts(data_type) {
        return Some((i32::from(precision), i32::from(scale)));
    }
    let precision = match data_type {
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64
            if integer_literal_value(expr).is_some_and(|value| i32::try_from(value).is_ok()) =>
        {
            10
        }
        DataType::Int64 | DataType::UInt64 => 20,
        _ => return None,
    };
    Some((precision, 0))
}

pub(super) fn suffix_literal_value(expr: &Expr) -> Option<i128> {
    match expr {
        Expr::Negative(inner) => suffix_literal_value(inner)?.checked_neg(),
        Expr::ScalarFunction(function) if function.func.name() == SUFFIX_LITERAL_NAME => {
            match function.args.as_slice() {
                [Expr::Cast(cast)]
                    if matches!(cast.field.data_type(), DataType::Int16 | DataType::Int64) =>
                {
                    integer_literal_value(&cast.expr)
                }
                [Expr::Literal(ScalarValue::Int16(Some(value)), _)] => Some(i128::from(*value)),
                [Expr::Literal(ScalarValue::Int64(Some(value)), _)] => Some(i128::from(*value)),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(super) fn min_precision_integer_literal(
    operand: Expr,
    other_type: &DataType,
    pick: bool,
) -> Expr {
    if !pick || decimal128_parts(other_type).is_none() {
        return operand;
    }
    if let Some(value) = integer_literal_value(&operand).or_else(|| suffix_literal_value(&operand))
    {
        return cast_integer_literal_to_min_precision(operand, value);
    }
    let Expr::Cast(cast) = &operand else {
        return operand;
    };
    let Some(value) = integer_literal_value(&cast.expr) else {
        return operand;
    };
    let Ok(native_type) = cast.expr.get_type(&DFSchema::empty()) else {
        return operand;
    };
    if !is_default_integer_to_decimal(cast.field.data_type(), &native_type) {
        return operand;
    }
    cast_integer_literal_to_min_precision(*cast.expr.clone(), value)
}

fn cast_integer_literal_to_min_precision(integer_literal: Expr, value: i128) -> Expr {
    let precision = min_precision_digits(value);
    Expr::Cast(Cast::new(
        Box::new(integer_literal),
        DataType::Decimal128(precision, 0),
    ))
}

fn integer_literal_value(expr: &Expr) -> Option<i128> {
    let Expr::Literal(scalar, _) = expr else {
        return None;
    };
    match scalar {
        ScalarValue::Int16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

fn is_default_integer_to_decimal(cast_target: &DataType, native: &DataType) -> bool {
    matches!(
        (cast_target, native),
        (
            DataType::Decimal128(20, 0),
            DataType::Int64 | DataType::UInt64 | DataType::Int32
        ) | (DataType::Decimal128(10, 0), DataType::UInt32)
            | (
                DataType::Decimal128(5, 0),
                DataType::Int16 | DataType::UInt16
            )
    )
}

pub(super) fn min_precision_digits(value: i128) -> u8 {
    let magnitude = value.unsigned_abs();
    if magnitude == 0 {
        return 1;
    }
    let digits = magnitude.ilog10() + 1;
    match u8::try_from(digits) {
        Ok(count) if (1..=38).contains(&count) => count,
        _ => 38,
    }
}

#[must_use]
pub fn spell_integer_suffix(
    tokens: &[TokenWithSpan],
    at: usize,
    last: usize,
    unary: bool,
    sql_type: &str,
    cast: String,
) -> String {
    let first = if unary {
        previous_significant(tokens, at).unwrap_or(at)
    } else {
        at
    };
    let before = previous_significant(tokens, first);
    let after = (last + 1..tokens.len())
        .find(|&cursor| !matches!(tokens[cursor].token, Token::Whitespace(_)))
        .map(|cursor| &tokens[cursor].token);
    let operator = |token: Option<&Token>| {
        matches!(
            token,
            Some(Token::Mul | Token::Div | Token::Mod | Token::Plus | Token::Minus)
        )
    };
    let argument = before.is_some_and(|cursor| {
        matches!(tokens[cursor].token, Token::LParen | Token::Comma)
            && remainder_call_argument(tokens, cursor)
    });
    if matches!(sql_type, "SMALLINT" | "BIGINT")
        && (operator(before.map(|cursor| &tokens[cursor].token)) || operator(after) || argument)
    {
        return format!("{SUFFIX_LITERAL_NAME}({cast})");
    }
    cast
}

fn previous_significant(tokens: &[TokenWithSpan], index: usize) -> Option<usize> {
    (0..index)
        .rev()
        .find(|&cursor| !matches!(tokens[cursor].token, Token::Whitespace(_)))
}

fn remainder_call_argument(tokens: &[TokenWithSpan], from: usize) -> bool {
    let mut depth = 0i32;
    let open = (0..=from).rev().find(|&cursor| {
        depth += match tokens[cursor].token {
            Token::RParen => 1,
            Token::LParen => -1,
            _ => 0,
        };
        depth < 0
    });
    let call = open.and_then(|open| previous_significant(tokens, open));
    call.is_some_and(|word| {
        matches!(&tokens[word].token, Token::Word(name) if name.quote_style.is_none()
            && ["mod", "pmod"].iter().any(|function| name.value.eq_ignore_ascii_case(function)))
    })
}
