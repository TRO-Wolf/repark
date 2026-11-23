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
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use super::leaf::{Mode, string_to_integral};
use crate::ansi::spark_ansi_enabled_from_options;
use crate::spark_string_timestamp::is_string_type;

pub const ARROW_CAST_OPERAND_NAME: &str = "__repark_arrow_cast_operand__";

static STRING_INTEGRAL: LazyLock<[Arc<ScalarUDF>; 4]> = LazyLock::new(|| {
    [
        (DataType::Int8, "__repark_string_to_tinyint__"),
        (DataType::Int16, "__repark_string_to_smallint__"),
        (DataType::Int32, "__repark_string_to_int__"),
        (DataType::Int64, "__repark_string_to_bigint__"),
    ]
    .map(|(target, name)| Arc::new(ScalarUDF::from(SparkStringIntegral::new(target, name))))
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

fn string_integral_udf(target: &DataType) -> Option<Arc<ScalarUDF>> {
    let index = match target {
        DataType::Int8 => 0,
        DataType::Int16 => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        _ => return None,
    };
    STRING_INTEGRAL.get(index).map(Arc::clone)
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
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_down(|node| Ok(rewrite_node(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn is_arrow_cast_operand(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(function) if function.func.name() == ARROW_CAST_OPERAND_NAME)
}

fn rewrite_node(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    if matches!(expr, Expr::InList(_)) {
        return Transformed::new(expr, false, TreeNodeRecursion::Jump);
    }
    rewrite_cast(expr, schema)
}

fn rewrite_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let Some(udf) = string_integral_udf(cast.field.data_type()) else {
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
    signature: Signature,
}

impl SparkStringIntegral {
    fn new(target: DataType, name: &'static str) -> Self {
        Self {
            target,
            name,
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
        Ok(value.schema_name().to_string())
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
        match arg_types {
            [value] => Ok(value.clone()),
            _ => plan_err!("'{ARROW_CAST_OPERAND_NAME}' takes one argument"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        match args.arg_fields {
            [value] => Ok(Arc::clone(value)),
            _ => plan_err!("'{ARROW_CAST_OPERAND_NAME}' takes one argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.as_slice() {
            [value] => Ok(value.clone()),
            _ => exec_err!("'{ARROW_CAST_OPERAND_NAME}' takes one argument"),
        }
    }
}
