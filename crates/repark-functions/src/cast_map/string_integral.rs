use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Projection, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, WriteOp,
};
use datafusion::optimizer::AnalyzerRule;

use super::leaf::{Mode, narrow_numeric, string_to_integral};
use crate::ansi::spark_ansi_enabled_from_options;
use crate::java_double::SUFFIX_LITERAL_NAME;
use crate::spark_string_timestamp::is_string_type;

pub const ARROW_CAST_OPERAND_NAME: &str = "__repark_arrow_cast_operand__";
pub const NARROW_DECIMAL_PREFIX: &str = "__repark_narrow_to_decimal_";

const TARGETS: [(DataType, &str); 4] = [
    (DataType::Int8, "__repark_string_to_tinyint__"),
    (DataType::Int16, "__repark_string_to_smallint__"),
    (DataType::Int32, "__repark_string_to_int__"),
    (DataType::Int64, "__repark_string_to_bigint__"),
];
const NARROW_TARGETS: [(DataType, &str); 4] = [
    (DataType::Int8, "__repark_narrow_to_tinyint__"),
    (DataType::Int16, "__repark_narrow_to_smallint__"),
    (DataType::Int32, "__repark_narrow_to_int__"),
    (DataType::Int64, "__repark_narrow_to_bigint__"),
];
static STRING_INTEGRAL: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkStringIntegral::new(
            target, name, true,
        )))
    })
});
static STRING_INTEGRAL_KEYS: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkStringIntegral::new(
            target, name, false,
        )))
    })
});
static NARROW_INTEGRAL: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    NARROW_TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkNarrowIntegral::new(
            target, name, true,
        )))
    })
});
static NARROW_INTEGRAL_KEYS: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    NARROW_TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkNarrowIntegral::new(
            target, name, false,
        )))
    })
});
static ARROW_CAST_OPERAND: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(ArrowCastOperand::new())));

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    STRING_INTEGRAL
        .iter()
        .chain(NARROW_INTEGRAL.iter())
        .chain(std::iter::once(&*ARROW_CAST_OPERAND))
        .map(Arc::clone)
        .collect()
}

#[must_use]
pub fn string_integral_cast_parts(expr: &Expr) -> Option<(&Expr, DataType)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    let (target, _) = TARGETS
        .iter()
        .find(|(_, name)| *name == function.func.name())?;
    let [operand] = function.args.as_slice() else {
        return None;
    };
    Some((operand, target.clone()))
}

#[must_use]
pub fn narrow_cast_parts(expr: &Expr) -> Option<(&Expr, DataType)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    let [operand] = function.args.as_slice() else {
        return None;
    };
    if let Some((target, _)) = NARROW_TARGETS
        .iter()
        .find(|(_, name)| *name == function.func.name())
    {
        return Some((operand, target.clone()));
    }
    if function.func.name().starts_with(NARROW_DECIMAL_PREFIX) {
        return function
            .func
            .return_type(&[])
            .ok()
            .map(|target| (operand, target));
    }
    None
}

fn target_index(target: &DataType) -> Option<usize> {
    match target {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}

fn string_integral_udf(target: &DataType, named: bool) -> Option<Arc<ScalarUDF>> {
    let index = target_index(target)?;
    let family = if named {
        &STRING_INTEGRAL
    } else {
        &STRING_INTEGRAL_KEYS
    };
    family.get(index).map(Arc::clone)
}

fn narrow_integral_udf(target: &DataType, named: bool) -> Option<Arc<ScalarUDF>> {
    let index = target_index(target)?;
    let family = if named {
        &NARROW_INTEGRAL
    } else {
        &NARROW_INTEGRAL_KEYS
    };
    family.get(index).map(Arc::clone)
}

fn plain_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => plain_type(value),
        other => other,
    }
}

fn is_narrowing_signed_integer(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Int64,
            DataType::Int32 | DataType::Int16 | DataType::Int8
        ) | (DataType::Int32, DataType::Int16 | DataType::Int8)
            | (DataType::Int16, DataType::Int8)
    )
}

fn is_in_range_int_literal(operand: &Expr, target: &DataType) -> bool {
    let Expr::Literal(scalar, _) = operand else {
        return false;
    };
    let value = match scalar {
        ScalarValue::Int16(Some(inner)) => i64::from(*inner),
        ScalarValue::Int32(Some(inner)) => i64::from(*inner),
        ScalarValue::Int64(Some(inner)) => *inner,
        _ => return false,
    };
    match target {
        DataType::Int8 => i8::try_from(value).is_ok(),
        DataType::Int16 => i16::try_from(value).is_ok(),
        DataType::Int32 => i32::try_from(value).is_ok(),
        _ => false,
    }
}

fn decimal_integer_overflows(precision: u8, scale: i8, target: &DataType) -> bool {
    let max = match target {
        DataType::Int8 => i128::from(i8::MAX),
        DataType::Int16 => i128::from(i16::MAX),
        DataType::Int32 => i128::from(i32::MAX),
        DataType::Int64 => i128::from(i64::MAX),
        _ => return false,
    };
    let integer_digits = i32::from(precision) - i32::from(scale);
    if integer_digits <= 0 {
        return false;
    }
    let Some(exponent) = u32::try_from(integer_digits).ok() else {
        return true;
    };
    10i128
        .checked_pow(exponent)
        .and_then(|power| power.checked_sub(1))
        .is_none_or(|top| top > max)
}

fn decimal_to_decimal_overflows(
    from_precision: u8,
    from_scale: i8,
    to_precision: u8,
    to_scale: i8,
) -> bool {
    let from_integer = i32::from(from_precision) - i32::from(from_scale);
    let to_integer = i32::from(to_precision) - i32::from(to_scale);
    if to_scale < from_scale {
        to_integer <= from_integer
    } else {
        to_integer < from_integer
    }
}

fn is_suffix_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(function) if function.func.name() == SUFFIX_LITERAL_NAME)
}

fn cast_udf_for(
    operand: &Expr,
    source: &DataType,
    target: &DataType,
    named: bool,
) -> Option<Arc<ScalarUDF>> {
    if is_arrow_cast_operand(operand) || is_suffix_literal(operand) {
        return None;
    }
    if is_string_type(source) {
        return string_integral_udf(target, named);
    }
    let plain = plain_type(source);
    if is_narrowing_signed_integer(plain, target) {
        if is_in_range_int_literal(operand, target) {
            return None;
        }
        return narrow_integral_udf(target, named);
    }
    if let DataType::Decimal128(precision, scale) = plain {
        if decimal_integer_overflows(*precision, *scale, target) {
            return narrow_integral_udf(target, named);
        }
        if let DataType::Decimal128(to_precision, to_scale) = target
            && decimal_to_decimal_overflows(*precision, *scale, *to_precision, *to_scale)
        {
            return Some(Arc::new(ScalarUDF::from(SparkNarrowDecimal::new(
                *to_precision,
                *to_scale,
                named,
            ))));
        }
        return None;
    }
    if matches!(plain, DataType::Float32 | DataType::Float64) {
        return narrow_integral_udf(target, named);
    }
    None
}

#[derive(Debug, Default)]
pub struct SparkStringIntegralCast;

impl AnalyzerRule for SparkStringIntegralCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let stores = store_projections(&plan)?;
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, &stores))
            .data()
    }

    fn name(&self) -> &'static str {
        "spark_string_integral_cast"
    }
}

#[derive(Debug, Default)]
pub struct SparkCoercionCastMark;

impl AnalyzerRule for SparkCoercionCastMark {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let stores = store_projections(&plan)?;
        plan.transform_up_with_subqueries(|node| mark_plan(node, &stores))
            .data()
    }

    fn name(&self) -> &'static str {
        "spark_coercion_cast_mark"
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn insert_string_integral_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return plan_err!("the string integral cast rule requires the type_coercion analyzer rule");
    };
    rules.insert(position, Arc::new(SparkStringIntegralCast));
    Ok(rules)
}

fn store_projections(plan: &LogicalPlan) -> Result<Vec<Vec<Expr>>> {
    let mut stores = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Dml(dml) = node
            && matches!(dml.op, WriteOp::Insert(_))
            && let LogicalPlan::Projection(projection) = dml.input.as_ref()
        {
            stores.push(projection.expr.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(stores)
}

fn held(plan: &LogicalPlan, stores: &[Vec<Expr>]) -> bool {
    match plan {
        LogicalPlan::Values(_) => true,
        LogicalPlan::Projection(projection) => stores.contains(&projection.expr),
        _ => false,
    }
}

fn mark_plan(plan: LogicalPlan, stores: &[Vec<Expr>]) -> Result<Transformed<LogicalPlan>> {
    if held(&plan, stores) {
        return Ok(Transformed::no(plan));
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    plan.map_expressions(|expr| expr.transform_up(|node| Ok(mark_cast(node, &schema))))
}

fn mark_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = expr else {
        return Transformed::no(expr);
    };
    let wants_mark = cast.expr.get_type(schema).is_ok_and(|source| {
        cast_udf_for(&cast.expr, &source, cast.field.data_type(), true).is_some()
    });
    if !wants_mark {
        return Transformed::no(Expr::Cast(cast));
    }
    let Cast { expr, field } = cast;
    let marked = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&ARROW_CAST_OPERAND),
        vec![*expr],
    ));
    Transformed::yes(Expr::Cast(Cast::new_from_field(Box::new(marked), field)))
}

fn rewrite_plan(plan: LogicalPlan, stores: &[Vec<Expr>]) -> Result<Transformed<LogicalPlan>> {
    if held(&plan, stores) {
        return Ok(Transformed::no(plan));
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let named = matches!(
        plan,
        LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
    );
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_down(|node| Ok(rewrite_node(node, &schema, named)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    let changed = transformed.transformed;
    transformed.map_data(|plan| refresh_schema(plan, changed))
}

fn refresh_schema(plan: LogicalPlan, changed: bool) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(projection) = plan else {
        if changed {
            return plan.recompute_schema();
        }
        return Ok(plan.clone().recompute_schema().unwrap_or(plan));
    };
    let input_schema = projection.input.schema();
    let mut fields = Vec::with_capacity(projection.expr.len());
    for ((qualifier, field), expr) in projection.schema.iter().zip(&projection.expr) {
        let nullable = field.is_nullable() || expr.nullable(input_schema)?;
        fields.push((
            qualifier.cloned(),
            Arc::new(field.as_ref().clone().with_nullable(nullable)),
        ));
    }
    let schema = DFSchema::new_with_metadata(fields, projection.schema.metadata().clone())?;
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        projection.expr,
        projection.input,
        Arc::new(schema),
    )?))
}

fn is_arrow_cast_operand(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(function) if function.func.name() == ARROW_CAST_OPERAND_NAME)
}

fn rewrite_node(expr: Expr, schema: &DFSchema, named: bool) -> Transformed<Expr> {
    rewrite_cast(expr, schema, named)
}

fn rewrite_cast(expr: Expr, schema: &DFSchema, named: bool) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let Ok(source) = cast.expr.get_type(schema) else {
        return Transformed::no(expr);
    };
    let Some(udf) = cast_udf_for(&cast.expr, &source, cast.field.data_type(), named) else {
        return Transformed::no(expr);
    };
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        udf,
        vec![*cast.expr.clone()],
    )))
}

fn parse(texts: &ArrayRef, target: &DataType, mode: Mode) -> Result<ArrayRef> {
    if matches!(
        texts.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return string_to_integral(texts, target, mode);
    }
    string_to_integral(&cast(texts.as_ref(), &DataType::Utf8)?, target, mode)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkStringIntegral {
    target: DataType,
    name: &'static str,
    named: bool,
    signature: Signature,
}

impl SparkStringIntegral {
    fn new(target: DataType, name: &'static str, named: bool) -> Self {
        Self {
            target,
            name,
            named,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkStringIntegral {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [value] = args else {
            return plan_err!("'{}' takes one argument", self.name);
        };
        if self.named {
            return Ok(value.schema_name().to_string());
        }
        Ok(format!("{}({})", self.name, value.schema_name()))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name, self.target.clone(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mode = if spark_ansi_enabled_from_options(&args.config_options) {
            Mode::Ansi
        } else {
            Mode::Legacy
        };
        let [value] = args.args.as_slice() else {
            return exec_err!("'{}' takes one argument", self.name);
        };
        match value {
            ColumnarValue::Array(texts) => {
                Ok(ColumnarValue::Array(parse(texts, &self.target, mode)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let parsed = parse(&scalar.to_array_of_size(1)?, &self.target, mode)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &parsed, 0,
                )?))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrowCastOperand {
    signature: Signature,
}

impl ArrowCastOperand {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrowCastOperand {
    fn name(&self) -> &str {
        ARROW_CAST_OPERAND_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [value] = args else {
            return plan_err!("'{ARROW_CAST_OPERAND_NAME}' takes one argument");
        };
        Ok(value.schema_name().to_string())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [value] = arg_types else {
            return plan_err!("'{ARROW_CAST_OPERAND_NAME}' takes one argument");
        };
        Ok(value.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [value] = args.arg_fields else {
            return plan_err!("'{ARROW_CAST_OPERAND_NAME}' takes one argument");
        };
        Ok(Arc::clone(value))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("'{ARROW_CAST_OPERAND_NAME}' takes one argument");
        };
        Ok(value.clone())
    }
}

fn invoke_narrow(
    name: &str,
    target: &DataType,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let mode = if spark_ansi_enabled_from_options(&args.config_options) {
        Mode::Ansi
    } else {
        Mode::Legacy
    };
    let [value] = args.args.as_slice() else {
        return exec_err!("'{name}' takes one argument");
    };
    match value {
        ColumnarValue::Array(source) => {
            Ok(ColumnarValue::Array(narrow_numeric(source, target, mode)?))
        }
        ColumnarValue::Scalar(scalar) => {
            let converted = narrow_numeric(&scalar.to_array_of_size(1)?, target, mode)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &converted, 0,
            )?))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkNarrowIntegral {
    target: DataType,
    name: &'static str,
    named: bool,
    signature: Signature,
}

impl SparkNarrowIntegral {
    fn new(target: DataType, name: &'static str, named: bool) -> Self {
        Self {
            target,
            name,
            named,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkNarrowIntegral {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [value] = args else {
            return plan_err!("'{}' takes one argument", self.name);
        };
        if self.named {
            return Ok(value.schema_name().to_string());
        }
        Ok(format!("{}({})", self.name, value.schema_name()))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [source] = args.arg_fields else {
            return plan_err!("'{}' takes one argument", self.name);
        };
        let nullable = match plain_type(source.data_type()) {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                source.is_nullable()
            }
            _ => true,
        };
        Ok(Arc::new(Field::new(
            self.name,
            self.target.clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_narrow(self.name, &self.target, &args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkNarrowDecimal {
    target: DataType,
    name: String,
    named: bool,
    signature: Signature,
}

impl SparkNarrowDecimal {
    fn new(precision: u8, scale: i8, named: bool) -> Self {
        let name = if scale < 0 {
            format!(
                "{NARROW_DECIMAL_PREFIX}{precision}_m{}__",
                scale.unsigned_abs()
            )
        } else {
            format!("{NARROW_DECIMAL_PREFIX}{precision}_{scale}__")
        };
        Self {
            target: DataType::Decimal128(precision, scale),
            name,
            named,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkNarrowDecimal {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [value] = args else {
            return plan_err!("'{}' takes one argument", self.name);
        };
        if self.named {
            return Ok(value.schema_name().to_string());
        }
        Ok(format!("{}({})", self.name, value.schema_name()))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return plan_err!("'{}' takes one argument", self.name);
        }
        Ok(Arc::new(Field::new(&self.name, self.target.clone(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_narrow(&self.name, &self.target, &args)
    }
}
