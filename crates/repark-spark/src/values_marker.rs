use std::convert::Infallible;
use std::hash::{BuildHasher, RandomState};
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
    CastKind, DataType as SqlType, ExactNumberInfo, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, SetExpr,
    UnaryOperator, Value, VisitMut, VisitorMut,
};

pub(crate) mod consume;

pub use consume::{SparkValuesTyping, insert_values_typing_first};

static VALUES_CELL_NAME: LazyLock<String> = LazyLock::new(|| {
    format!(
        "__repark_values_cell_{:016x}",
        RandomState::new().hash_one(std::process::id())
    )
});

#[must_use]
pub fn values_cell_name() -> &'static str {
    VALUES_CELL_NAME.as_str()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ValuesCell {
    signature: Signature,
}

impl ScalarUDFImpl for ValuesCell {
    fn name(&self) -> &str {
        values_cell_name()
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
    DataFusionError::Plan(format!("'{}' expects one argument", values_cell_name()))
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
            if function.args.len() == 1 && function.func.name() == values_cell_name() =>
        {
            function.args.first()
        }
        _ => None,
    }
}

#[must_use]
pub fn session_marks_values(state: &datafusion::execution::SessionState) -> bool {
    state.scalar_functions().contains_key(values_cell_name())
}

pub fn mark_values_cells<T: VisitMut>(node: &mut T) {
    let _ = node.visit(&mut ValuesMarker::new(Seat::Statement));
}

pub fn mark_fragment_cells<T: VisitMut>(node: &mut T) -> bool {
    let mut marker = ValuesMarker::new(Seat::Fragment);
    let _ = node.visit(&mut marker);
    marker.mixed
}

pub(crate) fn mark_fragment_statement(statement: &mut DfStatement) -> bool {
    match statement {
        DfStatement::Statement(inner) => mark_fragment_cells(inner.as_mut()),
        DfStatement::Explain(explain) => mark_fragment_statement(explain.statement.as_mut()),
        _ => false,
    }
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

#[allow(clippy::missing_errors_doc)]
pub async fn plan_marked_fragment(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<Option<datafusion::logical_expr::LogicalPlan>> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let mut statement = state.sql_to_statement(sql, &dialect)?;
    if session_marks_values(&state) && mark_fragment_statement(&mut statement) {
        return Ok(None);
    }
    state.statement_to_plan(statement).await.map(Some)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Seat {
    Statement,
    Fragment,
}

struct ValuesMarker {
    seat: Seat,
    mixed: bool,
}

impl ValuesMarker {
    fn new(seat: Seat) -> Self {
        Self { seat, mixed: false }
    }
}

impl VisitorMut for ValuesMarker {
    type Break = Infallible;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.mixed |= mark_set_expr(query.body.as_mut(), self.seat);
        ControlFlow::Continue(())
    }
}

fn mark_set_expr(body: &mut SetExpr, seat: Seat) -> bool {
    match body {
        SetExpr::Values(values) if values.rows.len() > 1 => {
            let mut rows: Vec<&mut Vec<Expr>> =
                values.rows.iter_mut().map(|row| &mut row.content).collect();
            mark_rows(&mut rows, seat)
        }
        SetExpr::SetOperation { left, right, .. } => {
            let left = mark_set_expr(left.as_mut(), seat);
            let right = mark_set_expr(right.as_mut(), seat);
            left || right
        }
        _ => false,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cell {
    Decimal,
    Double,
    Float,
    Integer,
    Empty,
    Other,
    Unknown,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Column {
    Plain,
    Decided,
    Undecided,
}

fn mark_rows(rows: &mut [&mut Vec<Expr>], seat: Seat) -> bool {
    let width = rows.iter().map(|row| row.len()).max().unwrap_or(0);
    let mut mixed = false;
    let mut marked = rows.iter().any(|row| row.iter().any(is_marker_call));
    for column in 0..width {
        match column_kind(rows, column) {
            Column::Plain if seat == Seat::Statement && spark_decimal_column(rows, column) => {
                for row in rows.iter_mut() {
                    if let Some(cell) = row.get_mut(column)
                        && is_markable(cell)
                    {
                        wrap_in_marker(cell);
                        marked = true;
                    }
                }
            }
            Column::Plain => {}
            Column::Decided if seat == Seat::Statement => {
                mixed = true;
                for row in rows.iter_mut() {
                    if let Some(cell) = row.get_mut(column)
                        && matches!(cell_kind(cell), Cell::Decimal | Cell::Float | Cell::Integer)
                    {
                        let inner = std::mem::replace(cell, Expr::value(Value::Null));
                        *cell = double_cast(inner);
                    }
                }
            }
            Column::Decided | Column::Undecided => {
                mixed = true;
                for row in rows.iter_mut() {
                    if let Some(cell) = row.get_mut(column)
                        && is_markable(cell)
                    {
                        wrap_in_marker(cell);
                        marked = true;
                    }
                }
            }
        }
    }
    if !marked {
        mark_one_witness(rows);
    }
    mixed
}

fn column_kind(rows: &[&mut Vec<Expr>], column: usize) -> Column {
    let (mut decimal, mut float, mut unknown) = (false, false, false);
    for row in rows {
        match row.get(column).map_or(Cell::Empty, cell_kind) {
            Cell::Decimal => decimal = true,
            Cell::Double | Cell::Float => float = true,
            Cell::Unknown => unknown = true,
            Cell::Other => return Column::Plain,
            Cell::Integer | Cell::Empty => {}
        }
    }
    if unknown {
        Column::Undecided
    } else if decimal && float {
        Column::Decided
    } else {
        Column::Plain
    }
}

fn spark_decimal_column(rows: &[&mut Vec<Expr>], column: usize) -> bool {
    let (mut decimal, mut integer, mut wide) = (false, false, false);
    for row in rows {
        let Some(cell) = row.get(column) else {
            continue;
        };
        match cell_kind(cell) {
            Cell::Decimal => {
                decimal = true;
                wide |= decimal_width(cell) >= 20;
            }
            Cell::Integer => integer = true,
            _ => {}
        }
    }
    decimal && (integer || wide)
}

fn decimal_width(cell: &Expr) -> u64 {
    match cell {
        Expr::Value(value) => match &value.value {
            Value::Number(text, false) => text
                .trim_start_matches(['0', '.'])
                .bytes()
                .filter(u8::is_ascii_digit)
                .count()
                .try_into()
                .unwrap_or(u64::MAX),
            _ => 0,
        },
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => decimal_width(inner),
        Expr::Cast {
            data_type: SqlType::Decimal(info) | SqlType::Numeric(info) | SqlType::Dec(info),
            ..
        } => match info {
            ExactNumberInfo::Precision(precision)
            | ExactNumberInfo::PrecisionAndScale(precision, _) => *precision,
            ExactNumberInfo::None => 10,
        },
        Expr::Function(function) if is_marker_call(cell) => {
            marker_argument(function).map_or(0, decimal_width)
        }
        _ => 0,
    }
}

fn cell_kind(cell: &Expr) -> Cell {
    match cell {
        Expr::Value(value) => match &value.value {
            Value::Number(text, false) => number_kind(text),
            Value::Null => Cell::Empty,
            Value::Placeholder(_) | Value::Number(_, true) => Cell::Unknown,
            _ => Cell::Other,
        },
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("default") => Cell::Empty,
        Expr::Nested(inner) => cell_kind(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } if matches!(expr.as_ref(), Expr::Value(_) | Expr::Nested(_)) => match cell_kind(expr) {
            kind @ (Cell::Decimal | Cell::Double | Cell::Float | Cell::Integer) => kind,
            _ => Cell::Unknown,
        },
        Expr::Cast {
            data_type,
            array: false,
            format: None,
            ..
        } => type_kind(data_type),
        Expr::Function(function) if is_marker_call(cell) => {
            marker_argument(function).map_or(Cell::Unknown, cell_kind)
        }
        _ => Cell::Unknown,
    }
}

fn marker_argument(function: &Function) -> Option<&Expr> {
    match &function.args {
        FunctionArguments::List(list) => match list.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] => Some(argument),
            _ => None,
        },
        _ => None,
    }
}

fn number_kind(text: &str) -> Cell {
    if text.bytes().all(|byte| byte.is_ascii_digit()) {
        if text.parse::<i64>().is_ok() {
            Cell::Integer
        } else {
            Cell::Unknown
        }
    } else if text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
        && text.bytes().filter(|&byte| byte == b'.').count() == 1
    {
        Cell::Decimal
    } else {
        Cell::Unknown
    }
}

fn type_kind(data_type: &SqlType) -> Cell {
    match data_type {
        SqlType::Decimal(_) | SqlType::Numeric(_) | SqlType::Dec(_) => Cell::Decimal,
        SqlType::Double(ExactNumberInfo::None) | SqlType::DoublePrecision | SqlType::Float8 => {
            Cell::Double
        }
        SqlType::Float(ExactNumberInfo::None) | SqlType::Real | SqlType::Float4 => Cell::Float,
        SqlType::TinyInt(None)
        | SqlType::SmallInt(None)
        | SqlType::Int(None)
        | SqlType::Integer(None)
        | SqlType::BigInt(None) => Cell::Integer,
        SqlType::String(_)
        | SqlType::Varchar(_)
        | SqlType::Char(_)
        | SqlType::Text
        | SqlType::Boolean
        | SqlType::Bool
        | SqlType::Date
        | SqlType::Timestamp(..)
        | SqlType::TimestampNtz(..)
        | SqlType::Binary(_)
        | SqlType::Interval { .. } => Cell::Other,
        _ => Cell::Unknown,
    }
}

fn double_cast(inner: Expr) -> Expr {
    Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(inner),
        data_type: SqlType::Double(ExactNumberInfo::None),
        array: false,
        format: None,
    }
}

fn mark_one_witness(rows: &mut [&mut Vec<Expr>]) {
    let witness = |cell: &Expr| is_markable(cell) && cell_kind(cell) != Cell::Empty;
    let found = rows
        .iter_mut()
        .flat_map(|row| row.iter_mut())
        .find(|cell| witness(cell));
    if let Some(cell) = found {
        wrap_in_marker(cell);
    }
}

fn wrap_in_marker(cell: &mut Expr) {
    let inner = std::mem::replace(cell, Expr::value(Value::Null));
    *cell = marker_call(inner);
}

fn is_markable(cell: &Expr) -> bool {
    match cell {
        Expr::Value(value) => !matches!(value.value, Value::Placeholder(_)),
        Expr::Identifier(ident) => !ident.value.eq_ignore_ascii_case("default"),
        Expr::Function(_) => !is_marker_call(cell),
        _ => true,
    }
}

fn is_marker_call(cell: &Expr) -> bool {
    matches!(
        cell,
        Expr::Function(Function { name, .. }) if matches!(
            name.0.as_slice(),
            [ObjectNamePart::Identifier(ident)] if ident.value == values_cell_name()
        )
    )
}

fn marker_call(inner: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(values_cell_name())]),
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
