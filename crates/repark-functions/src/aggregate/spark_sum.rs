use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Expr, GroupsAccumulator,
    LogicalPlan, Operator, ReversedUDAF, SetMonotonicity, Signature,
};

use super::sum_decimal::{DecimalSum, DecimalSumDistinct, DecimalSumGroups, Place, SumCheck};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SumMode {
    Ansi,
    Legacy,
    Try,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SparkSum {
    name: &'static str,
    mode: SumMode,
    place: Place,
    inner: Arc<AggregateUDF>,
}

pub(crate) fn sum_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkSum {
        name: "sum",
        mode: SumMode::Ansi,
        place: Place::Ungrouped,
        inner: datafusion::functions_aggregate::sum::sum_udaf(),
    }))
}

pub(crate) fn try_sum_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkSum {
        name: "try_sum",
        mode: SumMode::Try,
        place: Place::Ungrouped,
        inner: datafusion_spark::function::aggregate::try_sum(),
    }))
}

pub(crate) fn grouped(node: &LogicalPlan) -> bool {
    matches!(node, LogicalPlan::Aggregate(aggregate) if !aggregate.group_expr.is_empty())
}

pub(crate) fn place_of(grouped: bool) -> Place {
    if grouped {
        Place::Grouped
    } else {
        Place::Ungrouped
    }
}

pub(crate) fn placed(func: &Arc<AggregateUDF>, place: Place) -> Option<Arc<AggregateUDF>> {
    let sum = func.inner().downcast_ref::<SparkSum>()?;
    (sum.mode == SumMode::Ansi && sum.place != place).then(|| {
        Arc::new(AggregateUDF::new_from_impl(SparkSum {
            place,
            ..sum.clone()
        }))
    })
}

pub(crate) fn legacy(func: &Arc<AggregateUDF>) -> Option<Arc<AggregateUDF>> {
    let sum = func.inner().downcast_ref::<SparkSum>()?;
    (sum.mode == SumMode::Ansi).then(|| {
        Arc::new(AggregateUDF::new_from_impl(SparkSum {
            mode: SumMode::Legacy,
            ..sum.clone()
        }))
    })
}

fn decimal_arm(input: &DataType, result: &DataType) -> Option<(u8, u8, i8)> {
    match (input, result) {
        (DataType::Decimal128(input_precision, _), DataType::Decimal128(precision, scale)) => {
            Some((*input_precision, *precision, *scale))
        }
        _ => None,
    }
}

impl SparkSum {
    fn arm(&self, args: &AccumulatorArgs) -> Option<(u8, SumCheck)> {
        let input = args.expr_fields.first()?.data_type();
        let (input_precision, precision, scale) = decimal_arm(input, args.return_type())?;
        Some((
            input_precision,
            SumCheck::new(precision, scale, self.mode == SumMode::Ansi),
        ))
    }

    fn row_accumulator(
        &self,
        args: &AccumulatorArgs,
        place: Place,
    ) -> Option<Box<dyn Accumulator>> {
        let (input_precision, check) = self.arm(args)?;
        Some(if args.is_distinct {
            Box::new(DecimalSumDistinct::new(check, place))
        } else {
            Box::new(DecimalSum::new(input_precision, check, place))
        })
    }
}

impl AggregateUDFImpl for SparkSum {
    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match self.row_accumulator(&args, self.place) {
            Some(accumulator) => Ok(accumulator),
            None => self.inner.accumulator(args),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let Some(input) = args.input_fields.first() else {
            return self.inner.state_fields(args);
        };
        let result = args.return_field.data_type().clone();
        if decimal_arm(input.data_type(), &result).is_none() {
            return self.inner.state_fields(args);
        }
        if args.is_distinct {
            return Ok(vec![
                Field::new_list(
                    format_state_name(args.name, "sum distinct"),
                    Field::new_list_field(result, true),
                    false,
                )
                .into(),
            ]);
        }
        Ok(vec![
            Field::new(format_state_name(args.name, "sum"), result, true).into(),
        ])
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if self.arm(&args).is_some() {
            return !args.is_distinct;
        }
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        match self.arm(&args) {
            Some((_, check)) if !args.is_distinct => Ok(Box::new(DecimalSumGroups::new(check))),
            _ => self.inner.create_groups_accumulator(args),
        }
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return self.inner.create_sliding_accumulator(args);
        }
        match self.row_accumulator(&args, Place::Window) {
            Some(accumulator) => Ok(accumulator),
            None => self.inner.create_sliding_accumulator(args),
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.inner().reverse_expr()
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.inner().order_sensitivity()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(data_type)
    }

    fn simplify_expr_op_literal(
        &self,
        agg_function: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        arg_is_left: bool,
    ) -> Result<Option<Expr>> {
        if let Expr::Literal(value, _) = lit
            && value.data_type().is_decimal()
        {
            return Ok(None);
        }
        self.inner
            .inner()
            .simplify_expr_op_literal(agg_function, arg, op, lit, arg_is_left)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.inner().default_value(data_type)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.inner().documentation()
    }
}
