use std::convert::Infallible;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, Expr as PlanExpr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, ObjectNamePart, Query, SetExpr, Value, VisitMut, VisitorMut,
};

pub(crate) mod consume;

pub use consume::{SparkValuesTyping, insert_values_typing_first};

pub const VALUES_CELL_NAME: &str = "__repark_values_cell";

#[derive(Debug, PartialEq, Eq, Hash)]
struct ValuesCell {
    signature: Signature,
}

impl ScalarUDFImpl for ValuesCell {
    fn name(&self) -> &str {
        VALUES_CELL_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(one_argument)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(one_argument)?;
        Ok(Arc::new(
            Field::new(self.name(), first.data_type().clone(), first.is_nullable())
                .with_metadata(first.metadata().clone()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.into_iter().next().ok_or_else(one_argument)
    }
}

fn one_argument() -> DataFusionError {
    DataFusionError::Plan(format!("'{VALUES_CELL_NAME}' expects one argument"))
}

static VALUES_CELL: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(ValuesCell {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

#[must_use]
pub fn values_cell_udf() -> Arc<ScalarUDF> {
    Arc::clone(&VALUES_CELL)
}

#[must_use]
pub fn values_cell_argument(expr: &PlanExpr) -> Option<&PlanExpr> {
    match expr {
        PlanExpr::ScalarFunction(function)
            if function.func.name() == VALUES_CELL_NAME && function.args.len() == 1 =>
        {
            function.args.first()
        }
        _ => None,
    }
}

#[must_use]
pub fn session_marks_values(state: &datafusion::execution::SessionState) -> bool {
    state.scalar_functions().contains_key(VALUES_CELL_NAME)
}

pub fn mark_values_cells<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut ValuesMarker);
}

pub(crate) fn mark_df_statement(statement: &mut DfStatement) {
    match statement {
        DfStatement::Statement(inner) => mark_values_cells(inner.as_mut()),
        DfStatement::Explain(explain) => mark_df_statement(explain.statement.as_mut()),
        _ => {}
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn plan_marked_sql(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<datafusion::logical_expr::LogicalPlan> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let mut statement = state.sql_to_statement(sql, &dialect)?;
    if session_marks_values(&state) {
        mark_df_statement(&mut statement);
    }
    state.statement_to_plan(statement).await
}

struct ValuesMarker;

impl VisitorMut for ValuesMarker {
    type Break = Infallible;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        mark_set_expr(query.body.as_mut());
        ControlFlow::Continue(())
    }
}

fn mark_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Values(values) if values.rows.len() > 1 => {
            for row in &mut values.rows {
                for cell in &mut row.content {
                    if is_markable(cell) {
                        let inner = std::mem::replace(cell, Expr::value(Value::Null));
                        *cell = marker_call(inner);
                    }
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            mark_set_expr(left.as_mut());
            mark_set_expr(right.as_mut());
        }
        _ => {}
    }
}

fn is_markable(cell: &Expr) -> bool {
    match cell {
        Expr::Value(value) => !matches!(value.value, Value::Placeholder(_)),
        Expr::Identifier(ident) => !ident.value.eq_ignore_ascii_case("default"),
        Expr::Function(function) => !is_marker_name(&function.name),
        _ => true,
    }
}

fn is_marker_name(name: &ObjectName) -> bool {
    matches!(
        name.0.as_slice(),
        [ObjectNamePart::Identifier(ident)] if ident.value.eq_ignore_ascii_case(VALUES_CELL_NAME)
    )
}

fn marker_call(inner: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(VALUES_CELL_NAME)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

#[cfg(test)]
mod tests;
