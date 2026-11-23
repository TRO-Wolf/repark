use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
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
            target, name, true, false,
        )))
    })
});
static NARROW_INTEGRAL_NULLABLE: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    NARROW_TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkNarrowIntegral::new(
            target, name, true, true,
        )))
    })
});
static NARROW_INTEGRAL_KEYS: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    NARROW_TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkNarrowIntegral::new(
            target, name, false, false,
        )))
    })
});
static NARROW_INTEGRAL_KEYS_NULLABLE: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    NARROW_TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkNarrowIntegral::new(
            target, name, false, true,
        )))
    })
});
static ARROW_CAST_OPERAND: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(ArrowCastOperand::new(false))));
static ARROW_CAST_OPERAND_NULLABLE: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(ArrowCastOperand::new(true))));

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    STRING_INTEGRAL
        .iter()
        .chain(NARROW_INTEGRAL_NULLABLE.iter())
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

fn narrow_integral_udf(
    target: &DataType,
    named: bool,
    force_nullable: bool,
) -> Option<Arc<ScalarUDF>> {
    let index = target_index(target)?;
    let family = match (named, force_nullable) {
        (true, false) => &NARROW_INTEGRAL,
        (true, true) => &NARROW_INTEGRAL_NULLABLE,
        (false, false) => &NARROW_INTEGRAL_KEYS,
        (false, true) => &NARROW_INTEGRAL_KEYS_NULLABLE,
    };
    family.get(index).map(Arc::clone)
}

fn arrow_cast_operand_udf(force_nullable: bool) -> Arc<ScalarUDF> {
    if force_nullable {
        Arc::clone(&ARROW_CAST_OPERAND_NULLABLE)
    } else {
        Arc::clone(&ARROW_CAST_OPERAND)
    }
}

fn logical_operand_nullable(operand: &Expr, schema: &DFSchema) -> bool {
    match operand {
        Expr::ScalarSubquery(_) => true,
        Expr::Alias(alias) => logical_operand_nullable(&alias.expr, schema),
        Expr::Negative(inner) => logical_operand_nullable(inner, schema),
        Expr::Cast(cast) => logical_operand_nullable(&cast.expr, schema),
        Expr::BinaryExpr(binary) => {
            logical_operand_nullable(&binary.left, schema)
                || logical_operand_nullable(&binary.right, schema)
        }
        _ => operand.nullable(schema).unwrap_or(true),
    }
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

fn is_synthetic_suffix_literal(expr: &Expr) -> bool {
    let Expr::ScalarFunction(function) = expr else {
        return false;
    };
    if function.func.name() != SUFFIX_LITERAL_NAME {
        return false;
    }
    match function.args.as_slice() {
        [Expr::Literal(_, _)] => true,
        [Expr::Negative(inner)] => matches!(inner.as_ref(), Expr::Literal(_, _)),
        _ => false,
    }
}

fn peel_non_synthetic_suffix_literal(expr: &Expr) -> &Expr {
    if is_synthetic_suffix_literal(expr) {
        return expr;
    }
    if let Expr::ScalarFunction(function) = expr
        && function.func.name() == SUFFIX_LITERAL_NAME
        && let [inner] = function.args.as_slice()
    {
        return inner;
    }
    expr
}

fn cast_udf_for(
    operand: &Expr,
    source: &DataType,
    target: &DataType,
    named: bool,
    force_nullable: bool,
) -> Option<Arc<ScalarUDF>> {
    if is_arrow_cast_operand(operand) || is_synthetic_suffix_literal(operand) {
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
        return narrow_integral_udf(target, named, force_nullable);
    }
    if let DataType::Decimal128(precision, scale) = plain {
        if decimal_integer_overflows(*precision, *scale, target) {
            return narrow_integral_udf(target, named, true);
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
        return narrow_integral_udf(target, named, true);
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

struct DmlProjections {
    inserts: Vec<Vec<Expr>>,
    updates: Vec<Vec<Expr>>,
}

fn store_projections(plan: &LogicalPlan) -> Result<DmlProjections> {
    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Dml(dml) = node
            && let LogicalPlan::Projection(projection) = dml.input.as_ref()
        {
            match dml.op {
                WriteOp::Insert(_) => inserts.push(projection.expr.clone()),
                WriteOp::Update => updates.push(projection.expr.clone()),
                _ => {}
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(DmlProjections { inserts, updates })
}

fn held(plan: &LogicalPlan, stores: &[Vec<Expr>]) -> bool {
    match plan {
        LogicalPlan::Values(_) => true,
        LogicalPlan::Projection(projection) => stores.contains(&projection.expr),
        _ => false,
    }
}

fn is_update_projection(plan: &LogicalPlan, updates: &[Vec<Expr>]) -> bool {
    matches!(plan, LogicalPlan::Projection(projection) if updates.contains(&projection.expr))
}

fn peel_aliases(
    mut expr: Expr,
) -> (
    Expr,
    Vec<(Option<datafusion::common::TableReference>, String)>,
) {
    let mut frames = Vec::new();
    while let Expr::Alias(alias) = expr {
        frames.push((alias.relation, alias.name));
        expr = *alias.expr;
    }
    (expr, frames)
}

fn wrap_aliases(
    mut expr: Expr,
    frames: Vec<(Option<datafusion::common::TableReference>, String)>,
) -> Expr {
    for (relation, name) in frames.into_iter().rev() {
        expr = Expr::Alias(Alias::new(expr, relation, name));
    }
    expr
}

fn mark_plan(plan: LogicalPlan, stores: &DmlProjections) -> Result<Transformed<LogicalPlan>> {
    if held(&plan, &stores.inserts) {
        return Ok(Transformed::no(plan));
    }
    let update_store = is_update_projection(&plan, &stores.updates);
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    plan.map_expressions(|expr| {
        if update_store {
            let (inner, frames) = peel_aliases(expr);
            if let Expr::Cast(cast) = inner {
                let Cast {
                    expr: operand,
                    field,
                } = cast;
                let marked = (*operand).transform_up(|node| Ok(mark_cast(node, &schema)))?;
                return Ok(marked.update_data(|child| {
                    wrap_aliases(
                        Expr::Cast(Cast::new_from_field(Box::new(child), field)),
                        frames,
                    )
                }));
            }
            return wrap_aliases(inner, frames).transform_up(|node| Ok(mark_cast(node, &schema)));
        }
        expr.transform_up(|node| Ok(mark_cast(node, &schema)))
    })
}

fn mark_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = expr else {
        return Transformed::no(expr);
    };
    let wants_mark = cast.expr.get_type(schema).is_ok_and(|source| {
        cast_udf_for(&cast.expr, &source, cast.field.data_type(), true, true).is_some()
    });
    if !wants_mark {
        return Transformed::no(Expr::Cast(cast));
    }
    let force_nullable = logical_operand_nullable(&cast.expr, schema);
    let Cast { expr, field } = cast;
    let marked = Expr::ScalarFunction(ScalarFunction::new_udf(
        arrow_cast_operand_udf(force_nullable),
        vec![*expr],
    ));
    Transformed::yes(Expr::Cast(Cast::new_from_field(Box::new(marked), field)))
}

fn rewrite_plan(plan: LogicalPlan, stores: &DmlProjections) -> Result<Transformed<LogicalPlan>> {
    if held(&plan, &stores.inserts) {
        return Ok(Transformed::no(plan));
    }
    let update_store = is_update_projection(&plan, &stores.updates);
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
        let rewritten = if update_store {
            let (inner, frames) = peel_aliases(expr);
            if let Expr::Cast(cast) = inner {
                let Cast {
                    expr: operand,
                    field,
                } = cast;
                let inner_rewritten =
                    (*operand).transform_up(|node| Ok(rewrite_node(node, &schema, named)))?;
                inner_rewritten.update_data(|child| {
                    wrap_aliases(
                        Expr::Cast(Cast::new_from_field(Box::new(child), field)),
                        frames,
                    )
                })
            } else {
                wrap_aliases(inner, frames)
                    .transform_up(|node| Ok(rewrite_node(node, &schema, named)))?
            }
        } else {
            expr.transform_up(|node| Ok(rewrite_node(node, &schema, named)))?
        };
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
    if let Expr::ScalarFunction(mut function) = expr {
        if function.func.name() == ARROW_CAST_OPERAND_NAME
            && let [arg] = function.args.as_slice()
            && logical_operand_nullable(arg, schema)
            && function.func.as_ref() != ARROW_CAST_OPERAND_NULLABLE.as_ref()
        {
            function.func = Arc::clone(&ARROW_CAST_OPERAND_NULLABLE);
            return Transformed::yes(Expr::ScalarFunction(function));
        }
        return Transformed::no(Expr::ScalarFunction(function));
    }
    rewrite_cast(expr, schema, named)
}

fn rewrite_cast(expr: Expr, schema: &DFSchema, named: bool) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let operand = peel_non_synthetic_suffix_literal(&cast.expr);
    let Ok(source) = operand.get_type(schema) else {
        return Transformed::no(expr);
    };
    let force_nullable = logical_operand_nullable(operand, schema);
    let Some(udf) = cast_udf_for(
        operand,
        &source,
        cast.field.data_type(),
        named,
        force_nullable,
    ) else {
        return Transformed::no(expr);
    };
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        udf,
        vec![operand.clone()],
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
    force_nullable: bool,
    signature: Signature,
}

impl ArrowCastOperand {
    fn new(force_nullable: bool) -> Self {
        Self {
            force_nullable,
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
        if self.force_nullable && !value.is_nullable() {
            return Ok(Arc::new(value.as_ref().clone().with_nullable(true)));
        }
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
    force_nullable: bool,
    signature: Signature,
}

impl SparkNarrowIntegral {
    fn new(target: DataType, name: &'static str, named: bool, force_nullable: bool) -> Self {
        Self {
            target,
            name,
            named,
            force_nullable,
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
        let nullable = self.force_nullable
            || match plain_type(source.data_type()) {
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
