use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, Decimal256Array, as_boolean_array};
use arrow::datatypes::i256;
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

pub fn install_bool_decimal_cast(ctx: &SessionContext) {
    ctx.add_analyzer_rule(Arc::new(BoolDecimalCast));
}

#[derive(Debug, Default)]
pub struct BoolDecimalCast;

impl AnalyzerRule for BoolDecimalCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let null_on_overflow = !spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, null_on_overflow))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "bool_decimal_cast"
    }
}

fn rewrite_plan(plan: LogicalPlan, null_on_overflow: bool) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten =
            expr.transform_down(|node| Ok(rewrite_bool_cast(node, &schema, null_on_overflow)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
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

fn pow10(scale: i8) -> i128 {
    let mut value = 1i128;
    for _ in 0..scale.max(0) {
        value = value.saturating_mul(10);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionConfig;

    fn ctx_with(ansi: Option<bool>) -> SessionContext {
        let config = match ansi {
            Some(enabled) => crate::ansi::with_spark_ansi_config(SessionConfig::new(), enabled),
            None => SessionConfig::new(),
        };
        let ctx = SessionContext::new_with_config(config);
        ctx.add_analyzer_rule(Arc::new(BoolDecimalCast));
        ctx
    }

    async fn flags(ctx: &SessionContext, sql: &str) -> Vec<bool> {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        crate::analyze_eagerly(&ctx.state(), plan)
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|field| field.is_nullable())
            .collect()
    }

    async fn values(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                out.push(array_value_to_string(column, row).unwrap());
            }
        }
        out
    }

    #[tokio::test]
    async fn wide_targets_are_nonnull_with_one_and_zero() {
        let ctx = ctx_with(Some(true));
        assert_eq!(
            flags(&ctx, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
            vec![false]
        );
        assert_eq!(
            values(&ctx, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
            vec!["1.00".to_string()]
        );
        assert_eq!(
            values(&ctx, "SELECT CAST(false AS DECIMAL(10,2)) AS v").await,
            vec!["0.00".to_string()]
        );
        assert_eq!(
            values(&ctx, "SELECT CAST(true AS DECIMAL(1,0)) AS v").await,
            vec!["1".to_string()]
        );
    }

    #[tokio::test]
    async fn narrow_target_nulls_or_raises_by_ansi() {
        let legacy = ctx_with(Some(false));
        assert_eq!(
            flags(&legacy, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
            vec![true]
        );
        assert_eq!(
            values(&legacy, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
            vec![String::new()]
        );
        let ansi = ctx_with(Some(true));
        assert_eq!(
            flags(&ansi, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
            vec![true]
        );
        let error = ansi
            .sql("SELECT CAST(true AS DECIMAL(2,2)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("NUMERIC_VALUE_OUT_OF_RANGE"), "{error}");
    }

    #[tokio::test]
    async fn native_door_defaults_to_ansi() {
        let native = ctx_with(None);
        let error = native
            .sql("SELECT CAST(true AS DECIMAL(2,2)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("NUMERIC_VALUE_OUT_OF_RANGE"), "{error}");
        assert_eq!(
            values(&native, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
            vec!["1.00".to_string()]
        );
    }
}
