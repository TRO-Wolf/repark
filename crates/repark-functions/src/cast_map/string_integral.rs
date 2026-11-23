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
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, Projection, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use super::leaf::{Mode, string_to_integral};
use crate::ansi::spark_ansi_enabled_from_options;
use crate::spark_string_timestamp::is_string_type;

pub const ARROW_CAST_OPERAND_NAME: &str = "__repark_arrow_cast_operand__";

const TARGETS: [(DataType, &str); 4] = [
    (DataType::Int8, "__repark_string_to_tinyint__"),
    (DataType::Int16, "__repark_string_to_smallint__"),
    (DataType::Int32, "__repark_string_to_int__"),
    (DataType::Int64, "__repark_string_to_bigint__"),
];
static STRING_INTEGRAL: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkStringIntegral::new(target, name, true)))
    })
});
static STRING_INTEGRAL_KEYS: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    TARGETS.map(|(target, name)| {
        Arc::new(ScalarUDF::from(SparkStringIntegral::new(target, name, false)))
    })
});
static ARROW_CAST_OPERAND: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(ArrowCastOperand::new())));

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    STRING_INTEGRAL
        .iter()
        .chain(std::iter::once(&*ARROW_CAST_OPERAND))
        .map(Arc::clone)
        .collect()
}

fn string_integral_udf(target: &DataType, named: bool) -> Option<Arc<ScalarUDF>> {
    let index = match target {
        DataType::Int8 => 0,
        DataType::Int16 => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        _ => return None,
    };
    let family = if named {
        &STRING_INTEGRAL
    } else {
        &STRING_INTEGRAL_KEYS
    };
    family.get(index).map(Arc::clone)
}

#[derive(Debug, Default)]
pub struct SparkStringIntegralCast;

impl AnalyzerRule for SparkStringIntegralCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    fn name(&self) -> &'static str {
        "spark_string_integral_cast"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if matches!(plan, LogicalPlan::Values(_)) {
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
    if matches!(expr, Expr::InList(_)) {
        return Transformed::new(expr, false, TreeNodeRecursion::Jump);
    }
    rewrite_cast(expr, schema, named)
}

fn rewrite_cast(expr: Expr, schema: &DFSchema, named: bool) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let Some(udf) = string_integral_udf(cast.field.data_type(), named) else {
        return Transformed::no(expr);
    };
    if is_arrow_cast_operand(&cast.expr) {
        return Transformed::no(expr);
    }
    let Ok(source) = cast.expr.get_type(schema) else {
        return Transformed::no(expr);
    };
    if !is_string_type(&source) {
        return Transformed::no(expr);
    }
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
