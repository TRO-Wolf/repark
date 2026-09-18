use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::functions_table::generate_series::{GenSeriesArgs, GenerateSeriesTable};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SparkRangeFunc;

impl TableFunctionImpl for SparkRangeFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.is_empty() || exprs.len() > 4 {
            return plan_err!("range function requires 1 to 4 arguments");
        }
        let mut bounds: Vec<Option<i64>> = Vec::with_capacity(3);
        for (index, expr) in exprs.iter().take(3).enumerate() {
            bounds.push(coerce_range_bound(index, expr)?);
        }
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        if bounds.iter().any(Option::is_none) {
            return Ok(Arc::new(GenerateSeriesTable::new(
                schema,
                GenSeriesArgs::ContainsNull { name: "range" },
            )));
        }
        let flat: Vec<i64> = bounds.into_iter().flatten().collect();
        let (start, end, step) = match flat.as_slice() {
            [end] => (0, *end, 1),
            [start, end] => (*start, *end, 1),
            [start, end, step] => (*start, *end, *step),
            _ => return plan_err!("range function requires 1 to 4 arguments"),
        };
        if step == 0 {
            return plan_err!("Step cannot be zero");
        }
        Ok(Arc::new(GenerateSeriesTable::new(
            schema,
            GenSeriesArgs::Int64Args {
                start,
                end,
                step,
                include_end: false,
                name: "range",
            },
        )))
    }
}

fn coerce_range_bound(index: usize, expr: &Expr) -> Result<Option<i64>> {
    let position = index + 1;
    let Expr::Literal(scalar, _) = expr else {
        return plan_err!("Arguments must be literals");
    };
    match scalar {
        ScalarValue::Int64(Some(value)) => Ok(Some(*value)),
        ScalarValue::Int64(None)
        | ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None) => Ok(None),
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => match text.trim().parse::<i64>() {
            Ok(value) => Ok(Some(value)),
            Err(_) => {
                plan_err!("range function argument #{position} must be an INTEGER, got {text:?}")
            }
        },
        _ => plan_err!(
            "range function argument #{position} must be an INTEGER or NULL, got {:?}",
            scalar.data_type()
        ),
    }
}

pub(crate) fn register_spark_range(context: &SessionContext) {
    context.register_udtf("range", Arc::new(SparkRangeFunc));
}

#[cfg(test)]
mod tests;
