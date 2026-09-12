mod exec;
mod planner;
mod rewrite;
mod udf;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef, Result as DataFusionResult, plan_err};
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::{DataFrame, SessionContext};

use crate::{Error, Result, engine_err};

pub use planner::StackQueryPlanner;
pub use rewrite::StackRewrite;
pub use udf::stack_udf;

pub(crate) use exec::UnpivotExec;

pub fn register_stack(context: &SessionContext) {
    context.register_udf(stack_udf().as_ref().clone());
}

#[expect(
    clippy::missing_errors_doc,
    reason = "errors are Spark AnalysisException text; the reason lives in stack/map.md"
)]
pub fn apply_stack(
    frame: DataFrame,
    n: i64,
    passthrough_count: usize,
    output_names: Option<&[String]>,
) -> Result<DataFrame> {
    let n = parse_stack_n(n)?;
    let (session_state, plan) = frame.into_parts();
    let node =
        UnpivotNode::try_new(plan, n, passthrough_count, output_names).map_err(engine_err)?;
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    Ok(DataFrame::new(session_state, plan))
}

pub(crate) fn parse_stack_n(n: i64) -> Result<usize> {
    if n <= 0 || n > i64::from(i32::MAX) {
        return Err(Error::Analysis(format!(
            "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve stack due to data type \
             mismatch: The `n` must be between (0, 2147483647] (current value = {n}). \
             SQLSTATE: 42K09"
        )));
    }
    usize::try_from(n).map_err(|_| {
        Error::Analysis(
            "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve stack due to data type \
             mismatch: The `n` must be between (0, 2147483647]. SQLSTATE: 42K09"
                .to_string(),
        )
    })
}

pub(crate) fn stack_column_count(stack_count: usize, n: usize) -> usize {
    if n == 0 { 0 } else { stack_count.div_ceil(n) }
}

pub(crate) fn unify_stack_type(left: &DataType, right: &DataType) -> DataFusionResult<DataType> {
    if left == right {
        return Ok(left.clone());
    }
    if matches!(left, DataType::Null) {
        return Ok(right.clone());
    }
    if matches!(right, DataType::Null) {
        return Ok(left.clone());
    }
    plan_err!(
        "[DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES] Cannot resolve stack due to data type \
         mismatch: The data type of the column do not have the same type: \"{left}\" <> \
         \"{right}\". SQLSTATE: 42K09"
    )
}

#[derive(Debug, Clone, Eq)]
pub(crate) struct UnpivotNode {
    input: LogicalPlan,
    n: usize,
    passthrough_count: usize,
    stack_count: usize,
    schema: DFSchemaRef,
}

impl UnpivotNode {
    pub(crate) fn try_new(
        input: LogicalPlan,
        n: usize,
        passthrough_count: usize,
        output_names: Option<&[String]>,
    ) -> DataFusionResult<Self> {
        if n == 0 {
            return plan_err!(
                "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve stack due to data type \
                 mismatch: The `n` must be between (0, 2147483647] (current value = 0). \
                 SQLSTATE: 42K09"
            );
        }
        let input_fields = input.schema().fields();
        if passthrough_count > input_fields.len() {
            return plan_err!(
                "stack passthrough count {passthrough_count} exceeds input width {}",
                input_fields.len()
            );
        }
        let stack_count = input_fields.len() - passthrough_count;
        let n_cols = stack_column_count(stack_count, n);
        let output_names = output_names.filter(|names| names.len() == n_cols);
        let mut fields: Vec<Field> = Vec::with_capacity(passthrough_count.saturating_add(n_cols));
        for field in input_fields.iter().take(passthrough_count) {
            fields.push(Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        for column_index in 0..n_cols {
            let mut data_type = DataType::Null;
            for row_index in 0..n {
                let source = passthrough_count
                    .saturating_add(row_index.saturating_mul(n_cols))
                    .saturating_add(column_index);
                if source < passthrough_count.saturating_add(stack_count) {
                    data_type = unify_stack_type(&data_type, input_fields[source].data_type())?;
                }
            }
            let name = output_names
                .and_then(|names| names.get(column_index).cloned())
                .unwrap_or_else(|| format!("col{column_index}"));
            fields.push(Field::new(name, data_type, true));
        }
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            fields.into(),
            HashMap::new(),
        )?);
        Ok(Self {
            input,
            n,
            passthrough_count,
            stack_count,
            schema,
        })
    }

    pub(crate) fn n(&self) -> usize {
        self.n
    }

    pub(crate) fn passthrough_count(&self) -> usize {
        self.passthrough_count
    }

    pub(crate) fn stack_count(&self) -> usize {
        self.stack_count
    }

    pub(crate) fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(self.schema.inner())
    }
}

impl PartialEq for UnpivotNode {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n
            && self.passthrough_count == other.passthrough_count
            && self.stack_count == other.stack_count
            && self.schema == other.schema
            && self.input == other.input
    }
}

impl std::hash::Hash for UnpivotNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.n.hash(state);
        self.passthrough_count.hash(state);
        self.stack_count.hash(state);
        self.schema.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for UnpivotNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (
                self.n,
                self.passthrough_count,
                self.stack_count,
                self.schema.field_names(),
            )
                .cmp(&(
                    other.n,
                    other.passthrough_count,
                    other.stack_count,
                    other.schema.field_names(),
                )),
        )
    }
}

impl UserDefinedLogicalNodeCore for UnpivotNode {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "Unpivot"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Unpivot: stack(n={}, columns={})",
            self.n, self.stack_count
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        let input = inputs.pop().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "Unpivot requires one input plan".to_string(),
            )
        })?;
        if !inputs.is_empty() {
            return plan_err!("Unpivot requires exactly one input plan");
        }
        let names = self
            .schema
            .fields()
            .iter()
            .skip(self.passthrough_count)
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        Self::try_new(
            input,
            self.n,
            self.passthrough_count,
            Some(names.as_slice()),
        )
    }
}

#[cfg(test)]
mod tests;
