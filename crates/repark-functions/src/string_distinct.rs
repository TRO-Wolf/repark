use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

use crate::max_min_by::unqualified_name;
use crate::percentile::single_row_list;

#[must_use]
pub fn string_distinct_udaf(listagg: bool) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkStringDistinct::new(
        listagg,
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkStringDistinct {
    signature: Signature,
    listagg: bool,
}

impl SparkStringDistinct {
    fn new(listagg: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            listagg,
        }
    }

    fn display_base(&self) -> &str {
        if self.listagg {
            "listagg"
        } else {
            "string_agg"
        }
    }

    fn internal_name(&self) -> String {
        format!("__repark_{}_distinct", self.display_base())
    }
}

fn render_logical_delimiter(expr: &Expr) -> String {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(delimiter)), _) => delimiter.clone(),
        Expr::Literal(ScalarValue::Null, _) => "NULL".to_string(),
        Expr::Column(column) => column.name.clone(),
        other => other.schema_name().to_string(),
    }
}

fn distinct_name(listagg: bool, args: &[Expr]) -> String {
    let base = if listagg { "listagg" } else { "string_agg" };
    let value = args.first().map(unqualified_name).unwrap_or_default();
    let delimiter = args
        .get(1)
        .map(render_logical_delimiter)
        .unwrap_or_default();
    format!("{base}(DISTINCT {value}, {delimiter})")
}

fn physical_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let erased: &dyn std::any::Any = expr.as_ref();
    erased
        .downcast_ref::<Literal>()
        .map(|literal| literal.value().clone())
}

fn resolve_delimiter(expr: &Arc<dyn PhysicalExpr>) -> Result<String> {
    match physical_literal(expr) {
        Some(ScalarValue::Utf8(Some(delimiter))) => Ok(delimiter),
        Some(ScalarValue::Null) => Ok(String::new()),
        _ => Err(DataFusionError::Plan(
            "listagg_distinct delimiter must be a string literal or NULL".to_string(),
        )),
    }
}

impl AggregateUDFImpl for SparkStringDistinct {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        if self.listagg {
            "__repark_listagg_distinct"
        } else {
            "__repark_string_agg_distinct"
        }
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(distinct_name(self.listagg, &params.args))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.schema_name(params)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{} requires 2 parameters but got {}",
                self.internal_name(),
                arg_types.len()
            )));
        }
        Ok(vec![DataType::Utf8, arg_types[1].clone()])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{} requires 2 parameters but got {}",
                self.internal_name(),
                arg_types.len()
            )));
        }
        Ok(DataType::Utf8)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.exprs.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{} requires 2 parameters but got {}",
                self.internal_name(),
                acc_args.exprs.len()
            )));
        }
        let delimiter = resolve_delimiter(&acc_args.exprs[1])?;
        Ok(Box::new(StringDistinctAccumulator::new(delimiter)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let items = Arc::new(Field::new("item", DataType::Utf8, true));
        Ok(vec![Arc::new(Field::new(
            format_state_name(self.name(), "seen"),
            DataType::List(items),
            true,
        ))])
    }
}

#[derive(Debug)]
struct StringDistinctAccumulator {
    seen: Vec<String>,
    members: HashSet<String>,
    delimiter: String,
}

impl StringDistinctAccumulator {
    fn new(delimiter: String) -> Self {
        Self {
            seen: Vec::new(),
            members: HashSet::new(),
            delimiter,
        }
    }

    fn push(&mut self, value: &str) {
        if self.members.insert(value.to_string()) {
            self.seen.push(value.to_string());
        }
    }
}

impl Accumulator for StringDistinctAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let seen = StringArray::from(self.seen.clone());
        Ok(vec![ScalarValue::List(Arc::new(single_row_list(
            Arc::new(seen),
            DataType::Utf8,
        )))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let strings = cast(&values[0], &DataType::Utf8).map_err(|err| {
            DataFusionError::Plan(format!("string distinct value must be castable: {err}"))
        })?;
        let strings = strings
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("string distinct value must be castable".to_string())
            })?;
        for row in (0..strings.len()).rev() {
            if strings.is_null(row) {
                continue;
            }
            self.push(strings.value(row));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let seen = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("string distinct state must hold lists".to_string())
            })?;
        for group in 0..seen.len() {
            if seen.is_null(group) {
                continue;
            }
            let group_seen = seen.value(group);
            let group_seen = group_seen
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan("string distinct state must hold strings".to_string())
                })?;
            for row in 0..group_seen.len() {
                if group_seen.is_null(row) {
                    continue;
                }
                self.push(group_seen.value(row));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.seen.is_empty() {
            return Ok(ScalarValue::Utf8(None));
        }
        Ok(ScalarValue::Utf8(Some(self.seen.join(&self.delimiter))))
    }

    fn size(&self) -> usize {
        self.seen.iter().map(String::len).sum::<usize>() + self.delimiter.len() + 64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(string_distinct_udaf(true).as_ref().clone());
        ctx.register_udaf(string_distinct_udaf(false).as_ref().clone());
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        batches.into_iter().next().expect("one batch")
    }

    fn text(batch: &RecordBatch, row: usize) -> Option<String> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        values.is_valid(row).then(|| values.value(row).to_string())
    }

    async fn failure(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(failure) => failure.to_string(),
            Ok(frame) => frame.collect().await.expect_err("must fail").to_string(),
        }
    }

    #[tokio::test]
    async fn reverse_scan_orders_by_last_occurrence() {
        let batch = run(
            &ctx(),
            "SELECT __repark_listagg_distinct(v, ',') FROM (VALUES ('x'), ('y'), (NULL), ('x')) AS t(v)",
        )
        .await;
        assert_eq!(text(&batch, 0), Some("x,y".to_string()));
        let batch = run(
            &ctx(),
            "SELECT __repark_listagg_distinct(v, '-') FROM (VALUES ('a'), ('b'), ('b'), ('c')) AS t(v)",
        )
        .await;
        assert_eq!(text(&batch, 0), Some("c-b-a".to_string()));
    }

    #[tokio::test]
    async fn null_delimiter_concatenates_bare() {
        let batch = run(
            &ctx(),
            "SELECT __repark_listagg_distinct(v, NULL) FROM (VALUES ('x'), ('y'), (NULL), ('x')) AS t(v)",
        )
        .await;
        assert_eq!(text(&batch, 0), Some("xy".to_string()));
    }

    #[tokio::test]
    async fn all_null_group_answers_null() {
        let batch = run(
            &ctx(),
            "SELECT __repark_listagg_distinct(v, ',') FROM (VALUES (NULL), (NULL)) AS t(v)",
        )
        .await;
        assert_eq!(text(&batch, 0), None);
    }

    #[tokio::test]
    async fn string_agg_spelling_shares_kernel() {
        let batch = run(
            &ctx(),
            "SELECT __repark_string_agg_distinct(v, ',') FROM (VALUES ('x'), ('y'), ('x')) AS t(v)",
        )
        .await;
        assert_eq!(text(&batch, 0), Some("x,y".to_string()));
        assert_eq!(
            string_distinct_udaf(false).name(),
            "__repark_string_agg_distinct"
        );
    }

    #[tokio::test]
    async fn non_literal_delimiter_is_refused() {
        let refused = failure(
            &ctx(),
            "SELECT __repark_listagg_distinct(v, d) FROM (VALUES ('x', ','), ('y', ',')) AS t(v, d)",
        )
        .await;
        assert!(refused.contains("must be a string literal"), "{refused}");
    }

    #[tokio::test]
    async fn reverse_order_merge_matches_single_partition() {
        let values = StringArray::from(vec![Some("a"), Some("b"), Some("b"), Some("c")]);
        let state_of = |slice: StringArray| {
            let mut accumulator = StringDistinctAccumulator::new("-".to_string());
            accumulator
                .update_batch(&[Arc::new(slice) as ArrayRef])
                .expect("update");
            accumulator.state().expect("state")
        };
        let single = {
            let mut accumulator = StringDistinctAccumulator::new("-".to_string());
            accumulator
                .update_batch(&[Arc::new(values.clone()) as ArrayRef])
                .expect("update");
            accumulator.evaluate().expect("evaluate")
        };
        let merged = {
            let first = state_of(
                values
                    .slice(0, 2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("strings")
                    .clone(),
            );
            let second = state_of(
                values
                    .slice(2, 2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("strings")
                    .clone(),
            );
            let mut merged = StringDistinctAccumulator::new("-".to_string());
            for state in [second, first] {
                let arrays = state
                    .iter()
                    .map(|scalar| scalar.to_array_of_size(1).expect("state array"))
                    .collect::<Vec<_>>();
                merged.merge_batch(&arrays).expect("merge");
            }
            merged.evaluate().expect("evaluate")
        };
        assert_eq!(single, merged);
        assert_eq!(single, ScalarValue::Utf8(Some("c-b-a".to_string())));
    }
}
