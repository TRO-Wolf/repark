use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Decimal128Array, Decimal256Array, Float64Array, PrimitiveArray,
    as_boolean_array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, Decimal128Type, DecimalType, Float32Type, Float64Type, i256,
};
use arrow::error::ArrowError;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::ansi::spark_ansi_enabled_from_options;

pub(crate) const BOOL_TO_DECIMAL_NAME: &str = "__repark_bool_to_decimal__";
pub(crate) const FLOAT_TO_DECIMAL_NAME: &str = "__repark_float_to_decimal__";
pub(crate) const DECIMAL_TO_DOUBLE_NAME: &str = "__repark_decimal_to_double__";
const ARROW_CAST_NAME: &str = "arrow_cast";

pub fn install_bool_decimal_cast(ctx: &SessionContext, spark_door: bool) {
    ctx.add_analyzer_rule(Arc::new(BoolDecimalCast { spark_door }));
}

pub fn install_shared_analyzer_rules(ctx: &SessionContext, spark_door: bool) {
    crate::integer_spark::install_integer_overflow(ctx);
    install_bool_decimal_cast(ctx, spark_door);
}

#[derive(Debug, Default)]
pub struct BoolDecimalCast {
    pub spark_door: bool,
}

impl AnalyzerRule for BoolDecimalCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let null_on_overflow = !spark_ansi_enabled_from_options(config);
        let spark_door = self.spark_door;
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, null_on_overflow, spark_door))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "bool_decimal_cast"
    }
}

fn rewrite_plan(
    plan: LogicalPlan,
    null_on_overflow: bool,
    spark_door: bool,
) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr
            .transform_down(|node| Ok(rewrite_cast(node, &schema, null_on_overflow, spark_door)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_cast(
    expr: Expr,
    schema: &DFSchema,
    null_on_overflow: bool,
    spark_door: bool,
) -> Transformed<Expr> {
    if !spark_door {
        return rewrite_bool_cast(expr, schema, null_on_overflow);
    }
    match float_decimal_cast(&expr, schema).or_else(|| decimal_double_cast(&expr, schema)) {
        Some(rewritten) => Transformed::yes(rewritten),
        None => rewrite_bool_cast(expr, schema, null_on_overflow),
    }
}

fn decimal_double_cast(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    let (Expr::Cast(datafusion::logical_expr::Cast {
        expr: operand,
        field,
    })
    | Expr::TryCast(datafusion::logical_expr::TryCast {
        expr: operand,
        field,
    })) = expr
    else {
        return None;
    };
    if field.data_type() != &DataType::Float64 {
        return None;
    }
    if !matches!(operand.get_type(schema), Ok(DataType::Decimal128(_, _))) {
        return None;
    }
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(DecimalToDouble::new())),
        vec![operand.as_ref().clone()],
    )))
}

fn float_decimal_cast(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    let (operand, target, nullable) = match expr {
        Expr::Cast(cast) => (
            cast.expr.as_ref(),
            cast.field.data_type().clone(),
            crate::decimal_cast::nullable_spark_cast(cast, schema).is_some(),
        ),
        Expr::TryCast(cast) => (cast.expr.as_ref(), cast.field.data_type().clone(), true),
        Expr::ScalarFunction(function)
            if function.func.name() == ARROW_CAST_NAME && function.args.len() == 2 =>
        {
            let (Some(operand), Some(Expr::Literal(ScalarValue::Utf8(Some(text)), _))) =
                (function.args.first(), function.args.get(1))
            else {
                return None;
            };
            (operand, text.parse::<DataType>().ok()?, false)
        }
        _ => return None,
    };
    if !matches!(target, DataType::Decimal128(_, _)) {
        return None;
    }
    if !matches!(
        operand.get_type(schema),
        Ok(DataType::Float32 | DataType::Float64)
    ) {
        return None;
    }
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(FloatToDecimal::new(
            target,
            matches!(expr, Expr::TryCast(_)),
            nullable,
        ))),
        vec![operand.clone()],
    )))
}

fn rewrite_bool_cast(expr: Expr, schema: &DFSchema, null_on_overflow: bool) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let target = cast.field.data_type().clone();
    if !matches!(
        target,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    ) {
        return Transformed::no(expr);
    }
    if !matches!(cast.expr.get_type(schema), Ok(DataType::Boolean)) {
        return Transformed::no(expr);
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(BoolToDecimal::new(
            target,
            null_on_overflow,
        ))),
        vec![cast.expr.as_ref().clone()],
    )))
}

#[derive(Debug)]
pub struct BoolToDecimal {
    signature: Signature,
    target: DataType,
    null_on_overflow: bool,
}

impl BoolToDecimal {
    fn new(target: DataType, null_on_overflow: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target,
            null_on_overflow,
        }
    }

    fn precision_scale(&self) -> (u8, i8) {
        match self.target {
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                (precision, scale)
            }
            _ => (10, 0),
        }
    }

    fn unscaled_of(&self, bit: bool, precision: u8, scale: i8) -> Result<Option<i128>> {
        if !bit {
            return Ok(Some(0));
        }
        if i32::from(precision) - i32::from(scale) < 1 {
            if self.null_on_overflow {
                return Ok(None);
            }
            return Err(crate::decimal_spark::numeric_out_of_range_error(
                precision, scale,
            ));
        }
        Ok(Some(pow10(scale)))
    }

    fn decimal_of(&self, bit: bool, precision: u8, scale: i8) -> Result<ScalarValue> {
        let unscaled = self.unscaled_of(bit, precision, scale)?;
        match self.target {
            DataType::Decimal256(_, _) => Ok(ScalarValue::Decimal256(
                unscaled.map(i256::from_i128),
                precision,
                scale,
            )),
            _ => Ok(ScalarValue::Decimal128(unscaled, precision, scale)),
        }
    }
}

impl PartialEq for BoolToDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.target == other.target
            && self.null_on_overflow == other.null_on_overflow
    }
}

impl Eq for BoolToDecimal {}

impl Hash for BoolToDecimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.target.hash(state);
        self.null_on_overflow.hash(state);
    }
}

impl ScalarUDFImpl for BoolToDecimal {
    crate::shim_udf_boilerplate!("__repark_bool_to_decimal__");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{BOOL_TO_DECIMAL_NAME}' expects one argument"))
        })?;
        let (precision, scale) = self.precision_scale();
        let exposed = i32::from(precision) - i32::from(scale) < 1;
        Ok(Field::new(
            self.name(),
            self.target.clone(),
            first.is_nullable() || exposed,
        )
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (precision, scale) = self.precision_scale();
        match args.args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Boolean(value))) => {
                Ok(ColumnarValue::Scalar(match value {
                    None => match self.target {
                        DataType::Decimal256(_, _) => {
                            ScalarValue::Decimal256(None, precision, scale)
                        }
                        _ => ScalarValue::Decimal128(None, precision, scale),
                    },
                    Some(bit) => self.decimal_of(*bit, precision, scale)?,
                }))
            }
            Some(ColumnarValue::Array(array)) => {
                let bools = as_boolean_array(array.as_ref());
                let mut values = Vec::with_capacity(bools.len());
                for index in 0..bools.len() {
                    if bools.is_null(index) {
                        values.push(None);
                    } else {
                        values.push(self.unscaled_of(bools.value(index), precision, scale)?);
                    }
                }
                if matches!(self.target, DataType::Decimal256(_, _)) {
                    let wide: Vec<Option<i256>> =
                        values.into_iter().map(|v| v.map(i256::from_i128)).collect();
                    Ok(ColumnarValue::Array(Arc::new(
                        Decimal256Array::from(wide).with_precision_and_scale(precision, scale)?,
                    )))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(
                        Decimal128Array::from(values).with_precision_and_scale(precision, scale)?,
                    )))
                }
            }
            _ => Err(DataFusionError::Execution(format!(
                "'{BOOL_TO_DECIMAL_NAME}' expects a boolean argument"
            ))),
        }
    }
}

#[derive(Debug)]
pub struct FloatToDecimal {
    signature: Signature,
    target: DataType,
    safe: bool,
    nullable: bool,
}

impl FloatToDecimal {
    fn new(target: DataType, safe: bool, nullable: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target,
            safe,
            nullable,
        }
    }

    fn precision_scale(&self) -> (u8, i8) {
        match self.target {
            DataType::Decimal128(precision, scale) => (precision, scale),
            _ => (38, 0),
        }
    }

    fn unscaled_of(&self, value: f64, precision: u8, scale: i8) -> Result<Option<i128>> {
        let overflow = || {
            ArrowError::CastError(format!(
                "Cannot cast to Decimal128({precision}, {scale}). Overflowing on {value:?}"
            ))
        };
        let Some(unscaled) = repark_common::java_double::java_double_unscaled(value, scale) else {
            return if self.safe {
                Ok(None)
            } else {
                Err(overflow().into())
            };
        };
        if Decimal128Type::is_valid_decimal_precision(unscaled, precision) {
            return Ok(Some(unscaled));
        }
        if self.safe {
            return Ok(None);
        }
        Decimal128Type::validate_decimal_precision(unscaled, precision, scale)?;
        Err(overflow().into())
    }

    fn convert<T>(&self, values: &PrimitiveArray<T>) -> Result<Decimal128Array>
    where
        T: ArrowPrimitiveType,
        T::Native: Into<f64>,
    {
        let (precision, scale) = self.precision_scale();
        let converted: Decimal128Array = if self.safe {
            values.unary_opt(|value| {
                self.unscaled_of(value.into(), precision, scale)
                    .ok()
                    .flatten()
            })
        } else {
            values.try_unary(|value| {
                self.unscaled_of(value.into(), precision, scale)?
                    .ok_or_else(|| DataFusionError::Internal(String::new()))
            })?
        };
        Ok(converted.with_precision_and_scale(precision, scale)?)
    }
}

impl PartialEq for FloatToDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target && self.safe == other.safe && self.nullable == other.nullable
    }
}

impl Eq for FloatToDecimal {}

impl Hash for FloatToDecimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.target.hash(state);
        self.safe.hash(state);
        self.nullable.hash(state);
    }
}

impl ScalarUDFImpl for FloatToDecimal {
    crate::shim_udf_boilerplate!("__repark_float_to_decimal__");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{FLOAT_TO_DECIMAL_NAME}' expects one argument"))
        })?;
        Ok(Field::new(
            self.name(),
            self.target.clone(),
            first.is_nullable() || self.nullable,
        )
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (precision, scale) = self.precision_scale();
        match args.args.first() {
            Some(ColumnarValue::Scalar(scalar)) => {
                let value = match scalar {
                    ScalarValue::Float64(value) => *value,
                    ScalarValue::Float32(value) => value.map(f64::from),
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "'{FLOAT_TO_DECIMAL_NAME}' expects a FLOAT or DOUBLE argument"
                        )));
                    }
                };
                let unscaled = match value {
                    Some(value) => self.unscaled_of(value, precision, scale)?,
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    unscaled, precision, scale,
                )))
            }
            Some(ColumnarValue::Array(array)) => {
                let converted = match array.data_type() {
                    DataType::Float64 => self.convert(array.as_primitive::<Float64Type>())?,
                    DataType::Float32 => self.convert(array.as_primitive::<Float32Type>())?,
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "'{FLOAT_TO_DECIMAL_NAME}' expects a FLOAT or DOUBLE argument"
                        )));
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(converted)))
            }
            None => Err(DataFusionError::Execution(format!(
                "'{FLOAT_TO_DECIMAL_NAME}' expects one argument"
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalToDouble {
    signature: Signature,
}

impl DecimalToDouble {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DecimalToDouble {
    crate::shim_udf_boilerplate!("__repark_decimal_to_double__");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{DECIMAL_TO_DOUBLE_NAME}' expects one argument"))
        })?;
        Ok(Field::new(self.name(), DataType::Float64, first.is_nullable()).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expects = || {
            DataFusionError::Execution(format!(
                "'{DECIMAL_TO_DOUBLE_NAME}' expects one DECIMAL argument"
            ))
        };
        match args.args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Decimal128(unscaled, _, scale))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(unscaled.map(
                    |unscaled| repark_common::java_double::java_decimal_to_double(unscaled, *scale),
                ))))
            }
            Some(ColumnarValue::Array(array)) => {
                let DataType::Decimal128(_, scale) = *array.data_type() else {
                    return Err(expects());
                };
                let converted: Float64Array =
                    array.as_primitive::<Decimal128Type>().unary(|unscaled| {
                        repark_common::java_double::java_decimal_to_double(unscaled, scale)
                    });
                Ok(ColumnarValue::Array(Arc::new(converted)))
            }
            _ => Err(expects()),
        }
    }
}

fn pow10(scale: i8) -> i128 {
    let mut value = 1i128;
    for _ in 0..scale.max(0) {
        value = value.saturating_mul(10);
    }
    value
}

#[cfg(test)]
mod tests;
