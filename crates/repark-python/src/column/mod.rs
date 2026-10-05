//! Python-facing immutable columns backed by DataFusion logical expressions.
//! Constructors resolve literals and standalone SQL expressions; `DataFrame` methods resolve
//! expressions against their input schema.

use crate::{AnalysisException, fence::fenced};
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::rank::{dense_rank_udwf, percent_rank_udwf, rank_udwf};
use datafusion::functions_window::{cume_dist::cume_dist_udwf, ntile::ntile_udwf};
use datafusion::functions_window::{nth_value::nth_value_udwf, row_number::row_number_udwf};
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda, NullTreatment, WindowFunction};
use datafusion::logical_expr::{
    Case, Cast, Expr, ExprFunctionExt, TryCast, WindowFunctionDefinition, lambda_var, lit,
};
use datafusion::scalar::ScalarValue;
use datafusion::{arrow::datatypes::DataType, functions_aggregate::count::count_udaf};
use pyo3::types::{PyBool, PyFloat, PyInt, PyString};
use pyo3::{exceptions::PyValueError, prelude::*};

pub(crate) mod display;
#[cfg(test)]
mod door_parity_tests;
pub(crate) mod expr_build;
#[cfg(test)]
mod expr_tests;
mod function_dispatch;
mod levels;
pub(crate) mod series;
mod window;

use expr_build::{
    TIMESTAMP_UNIT, collapse_identity_alias_chain, parse_data_type, percentile_approx_list_expr,
    percentile_approx_scalar_expr, refuse_nested_higher_order, written_column,
};
use function_dispatch::{
    call_scalar_expr, cast_unsigned_count_to_signed, nary_aggregate_udaf, unary_aggregate_udaf,
};
pub use levels::PyColumn;
use window::{OverSpec, build_over_expression};

impl PyColumn {
    async fn plan_sql_text(sql: &str, keep_verbatim: bool) -> PyResult<Expr> {
        repark_spark::refuse_sql_fragment(sql).map_err(crate::datafusion_to_py_err)?;
        let context = expr_build::sql_context(sql, true).map_err(crate::datafusion_to_py_err)?;
        let canonical = repark_spark::spark_literals::canonicalize_verbatim(sql, keep_verbatim)
            .map_err(crate::datafusion_to_py_err)?;
        expr_build::plan_expr_column(&context, canonical.as_ref(), sql).await
    }

    /// The held expression, cloned for handoff to a [`crate::dataframe::PyDataFrame`] method.
    pub(crate) fn expr(&self) -> Expr {
        crate::deep_stack::grown_clone_expr(&self.expr, self.expr_levels, self.plan_levels)
    }

    pub(crate) fn expression_depth(&self) -> usize {
        self.expr_levels
    }

    pub(crate) fn df_depth(&self) -> usize {
        self.df_levels
    }

    pub(crate) fn plan_depth(&self) -> usize {
        self.plan_levels
    }
}

// The fence converts Rust panics into catchable PySpark exceptions at the Python boundary.
#[allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::needless_pass_by_value,
    clippy::missing_errors_doc
)]
#[pymethods]
impl PyColumn {
    /// A column reference by name (PySpark `col(name)` / `F.col`).
    #[staticmethod]
    pub fn column(name: &str) -> PyResult<Self> {
        let column = written_column(name);
        fenced!("Column.column", { Ok(Self::from_expr(column)) })
    }

    /// A literal from a Python scalar (PySpark `lit(value)`).
    ///
    /// Supports `None`, `bool`, `int` (Int32 if it fits, else Int64), `float`, and `str`.
    /// Python `bool` is checked before `int` because it is an `int` subclass.
    ///
    /// # Errors
    /// Returns `ValueError` for a Python type with no scalar literal mapping.
    #[staticmethod]
    pub fn literal(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        fenced!("Column.literal", {
            if value.is_none() {
                return Ok(Self::from_expr(lit(ScalarValue::Null)));
            }
            if value.is_instance_of::<PyBool>() {
                let boolean: bool = value.extract()?;
                return Ok(Self::from_expr(lit(boolean)));
            }
            if value.is_instance_of::<PyInt>() {
                let integer: i64 = value.extract()?;
                return Ok(Self::from_expr(
                    i32::try_from(integer).map_or_else(|_| lit(integer), lit),
                ));
            }
            if value.is_instance_of::<PyFloat>() {
                let float: f64 = value.extract()?;
                return Ok(Self::from_expr(lit(float)));
            }
            if value.is_instance_of::<PyString>() {
                let text: String = value.extract()?;
                return Ok(Self::from_expr(lit(text)));
            }
            Err(PyValueError::new_err(format!(
                "lit() supports None, bool, int, float, or str; got {}",
                value.get_type().name()?
            )))
        })
    }

    /// Whether this expression contains a higher-order function.
    ///
    /// # Errors
    /// Propagates tree-walk failures through the engine exception classifier.
    pub fn contains_higher_order(&self) -> PyResult<bool> {
        fenced!("Column.contains_higher_order", {
            self.grown_read(expr_build::contains_higher_order)
        })
    }

    /// A lambda parameter reference resolved when the parent `DataFrame` is planned.
    #[staticmethod]
    pub fn lambda_variable(name: &str) -> PyResult<Self> {
        fenced!("Column.lambda_variable", {
            Ok(Self::from_expr(lambda_var(name)))
        })
    }

    /// Invoke a registered higher-order function with value arguments and lambda bodies.
    ///
    /// # Errors
    /// Returns `ValueError` for an unknown function and `UnsupportedOperationException` for a
    /// nested higher-order function.
    #[staticmethod]
    pub fn call_higher_order(
        name: &str,
        value_args: Vec<PyColumn>,
        lambdas: Vec<(Vec<String>, PyColumn)>,
    ) -> PyResult<Self> {
        fenced!("Column.call_higher_order", {
            let function = repark_functions::higher_order::by_name(name).ok_or_else(|| {
                PyValueError::new_err(format!("unknown higher-order function {name:?}"))
            })?;
            let mut args: Vec<Expr> = Vec::with_capacity(value_args.len() + lambdas.len());
            for value in &value_args {
                let value = value.expr();
                refuse_nested_higher_order(&value, name, "value argument")?;
                args.push(value);
            }
            for (params, body) in &lambdas {
                let body = body.expr();
                refuse_nested_higher_order(&body, name, "lambda")?;
                args.push(Expr::Lambda(Lambda::new(params.clone(), body)));
            }
            Ok(Self::combine_surveyed(
                Expr::HigherOrderFunction(HigherOrderFunction::new(function, args)),
                value_args
                    .iter()
                    .chain(lambdas.iter().map(|(_, body)| body)),
            ))
        })
    }

    /// Pack fields into a struct expression.
    ///
    /// Uses already-built child expressions so the result binds in a parent `DataFrame` projection.
    ///
    /// # Errors
    /// Returns `ValueError` when `fields` is empty.
    #[staticmethod]
    pub fn make_struct(fields: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.make_struct", {
            if fields.is_empty() {
                return Err(PyValueError::new_err(
                    "struct() requires at least one field column",
                ));
            }
            // Preserve aliases because DataFusion's struct return type otherwise uses c0, c1, ….
            let mut args: Vec<Expr> = Vec::with_capacity(fields.len() * 2);
            for (index, column) in fields.iter().enumerate() {
                let expr = column.expr();
                let (field_name, value) = match expr {
                    Expr::Alias(alias) => (alias.name.clone(), *alias.expr),
                    other => {
                        let name = column.grown_read(|expr| expr.schema_name().to_string());
                        let field_name = if name.is_empty() {
                            format!("col{index}")
                        } else {
                            name
                        };
                        (field_name, other)
                    }
                };
                args.push(lit(field_name));
                args.push(value);
            }
            Ok(Self::combine_surveyed(
                datafusion::functions::expr_fn::named_struct(args),
                &fields,
            ))
        })
    }

    /// A SQL-string expression (PySpark `expr(sql)` / `F.expr`).
    ///
    /// Parses `sql` on the shared process-wide expr context provisioned with
    /// `repark_functions::register_all` + `analyzer_rules()` (same function surface as a
    /// repark session), plans `SELECT (<sql>)`, then runs the analyzer **eagerly**
    /// (`repark_functions::analyze_eagerly`) before extracting the projection expression.
    /// The extracted [`Expr`] therefore already carries the Spark rewrites (integer `/` →
    /// both-operands-double, div-by-zero `nullif`, planner-embedded `substr` → shim, …)
    /// *and* their post-analysis types — so both the values and the schema a consumer
    /// `DataFrame` exports over Arrow match `spark.sql`. The rules are idempotent, so the
    /// consumer session's own analysis pass is a no-op on this
    ///
    /// # Errors
    /// Returns `ParseException` for invalid SQL and `AnalysisException` for unresolved columns.
    /// This path bypasses the Spark SQL router, so it applies the parse-altitude valves here.
    #[staticmethod]
    pub fn sql(sql: &str, keep_verbatim: bool) -> PyResult<Self> {
        fenced!("Column.sql", {
            let grown = crate::deep_stack::sql_drive_grown(sql);
            let runtime = crate::session::shared_runtime()?;
            let expr = crate::deep_stack::block_on_grown_if(
                &runtime,
                Self::plan_sql_text(sql, keep_verbatim),
                grown,
            )?;
            Ok(Self::from_sql_text(expr))
        })
    }

    /// `IS NULL` predicate (PySpark `Column.isNull`).
    pub fn is_null(&self) -> PyResult<Self> {
        fenced!("Column.is_null", {
            Ok(Self::combine(self.expr().is_null(), [self], 1))
        })
    }

    /// `IS NOT NULL` predicate (PySpark `Column.isNotNull`).
    pub fn is_not_null(&self) -> PyResult<Self> {
        fenced!("Column.is_not_null", {
            Ok(Self::combine(self.expr().is_not_null(), [self], 1))
        })
    }

    /// Build a searched `CASE WHEN … THEN … [ELSE …] END` (PySpark `F.when` / `otherwise`).
    ///
    /// `when_thens` is an ordered list of `(condition, value)` pairs; `otherwise` is the optional
    /// ELSE arm (NULL when omitted).
    #[staticmethod]
    pub fn case_when(
        when_thens: Vec<(PyColumn, PyColumn)>,
        otherwise: Option<PyColumn>,
    ) -> PyResult<Self> {
        fenced!("Column.case_when", {
            let when_then_expr = when_thens
                .iter()
                .map(|(condition, value)| (Box::new(condition.expr()), Box::new(value.expr())))
                .collect();
            let else_expr = otherwise.as_ref().map(|column| Box::new(column.expr()));
            Ok(Self::combine(
                Expr::Case(Case {
                    expr: None,
                    when_then_expr,
                    else_expr,
                }),
                when_thens
                    .iter()
                    .flat_map(|(condition, value)| [condition, value])
                    .chain(otherwise.iter()),
                1,
            ))
        })
    }

    /// First non-null of the arguments (PySpark `coalesce(*cols)`).
    #[staticmethod]
    pub fn coalesce(columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.coalesce", {
            let exprs = columns.iter().map(PyColumn::expr).collect();
            Ok(Self::combine_surveyed(
                datafusion::functions::expr_fn::coalesce(exprs),
                &columns,
            ))
        })
    }

    /// String concatenation of the arguments (PySpark `concat(*cols)`).
    #[staticmethod]
    pub fn concat(columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.concat", {
            use datafusion::logical_expr::expr::ScalarFunction;
            let exprs: Vec<Expr> = columns.iter().map(PyColumn::expr).collect();
            Ok(Self::combine_surveyed(
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    repark_functions::string::concat_udf(),
                    exprs,
                )),
                &columns,
            ))
        })
    }

    /// The statement's current timestamp with Spark's microsecond UTC type.
    ///
    /// Spark's `current_timestamp()` maps DataFusion's `now()` to a UTC microsecond timestamp.
    /// DataFusion produces nanoseconds, so the binding casts them for Arrow and Iceberg.
    #[staticmethod]
    pub fn current_timestamp() -> PyResult<Self> {
        fenced!("Column.current_timestamp", {
            let now = datafusion::functions::expr_fn::now();
            // Match Spark's Arrow precision and timezone.
            let spark_timestamp =
                DataType::Timestamp(TIMESTAMP_UNIT, Some(std::sync::Arc::<str>::from("UTC")));
            Ok(Self::from_expr(Expr::Cast(Cast::new(
                Box::new(now),
                spark_timestamp,
            ))))
        })
    }

    /// Call a DataFusion scalar function by name.
    ///
    /// Arguments are already-built [`PyColumn`]s. Unknown names fail loudly.
    #[staticmethod]
    pub fn call_scalar(name: &str, args: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.call_scalar", {
            let exprs: Vec<Expr> = args.iter().map(PyColumn::expr).collect();
            Ok(Self::combine_surveyed(
                call_scalar_expr(name, exprs)?,
                &args,
            ))
        })
    }

    // ---- Spark date functions ---------------------------------------------------------------

    /// Spark `year(date)` — the calendar year.
    pub fn year(&self) -> PyResult<Self> {
        fenced!("Column.year", {
            Ok(Self::combine(
                repark_functions::expr_fn::year(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `month(date)` — the month of year, 1..=12.
    pub fn month(&self) -> PyResult<Self> {
        fenced!("Column.month", {
            Ok(Self::combine(
                repark_functions::expr_fn::month(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `quarter(date)` — the quarter of year, 1..=4.
    pub fn quarter(&self) -> PyResult<Self> {
        fenced!("Column.quarter", {
            Ok(Self::combine(
                repark_functions::expr_fn::quarter(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `weekofyear(date)` — the ISO-8601 week number.
    pub fn weekofyear(&self) -> PyResult<Self> {
        fenced!("Column.weekofyear", {
            Ok(Self::combine(
                repark_functions::expr_fn::weekofyear(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `dayofweek(date)` — 1=Sunday .. 7=Saturday.
    pub fn dayofweek(&self) -> PyResult<Self> {
        fenced!("Column.dayofweek", {
            Ok(Self::combine(
                repark_functions::expr_fn::dayofweek(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `weekday(date)` — 0=Monday .. 6=Sunday.
    pub fn weekday(&self) -> PyResult<Self> {
        fenced!("Column.weekday", {
            Ok(Self::combine(
                repark_functions::expr_fn::weekday(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `dayofmonth(date)` — the day of month, 1..=31.
    pub fn dayofmonth(&self) -> PyResult<Self> {
        fenced!("Column.dayofmonth", {
            Ok(Self::combine(
                repark_functions::expr_fn::dayofmonth(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `dayofyear(date)` — the day of year, 1..=366.
    pub fn dayofyear(&self) -> PyResult<Self> {
        fenced!("Column.dayofyear", {
            Ok(Self::combine(
                repark_functions::expr_fn::dayofyear(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `last_day(date)` — the last day of the month containing this date.
    pub fn last_day(&self) -> PyResult<Self> {
        fenced!("Column.last_day", {
            Ok(Self::combine(
                repark_functions::expr_fn::last_day(self.expr()),
                [self],
                1,
            ))
        })
    }

    /// Spark `add_months(start, num_months)` — end-of-month-preserving month arithmetic.
    pub fn add_months(&self, num_months: &PyColumn) -> PyResult<Self> {
        fenced!("Column.add_months", {
            Ok(Self::combine_surveyed(
                repark_functions::expr_fn::add_months(self.expr(), num_months.expr()),
                [self, num_months],
            ))
        })
    }

    /// Spark `date_add(start, num_days)` — the date `num_days` after this date.
    pub fn date_add(&self, num_days: &PyColumn) -> PyResult<Self> {
        fenced!("Column.date_add", {
            Ok(Self::combine_surveyed(
                repark_functions::expr_fn::date_add(self.expr(), num_days.expr()),
                [self, num_days],
            ))
        })
    }

    /// Spark `date_format(timestamp, format)` — format with a Java pattern string (a literal).
    pub fn date_format(&self, format: &str) -> PyResult<Self> {
        fenced!("Column.date_format", {
            Ok(Self::combine(
                repark_functions::expr_fn::date_format(self.expr(), lit(format)),
                [self],
                1,
            ))
        })
    }

    /// Spark `trunc(date, format)` — truncate a DATE to `format` (year/month/week/quarter).
    pub fn trunc(&self, format: &str) -> PyResult<Self> {
        fenced!("Column.trunc", {
            Ok(Self::combine(
                repark_functions::expr_fn::trunc(self.expr(), lit(format)),
                [self],
                1,
            ))
        })
    }

    /// Spark `date_trunc(format, timestamp)` — truncate this TIMESTAMP to `format`. The Spark
    /// argument order (format first) is applied here; the facade passes the format as a literal.
    pub fn date_trunc(&self, format: &str) -> PyResult<Self> {
        fenced!("Column.date_trunc", {
            Ok(Self::combine(
                repark_functions::expr_fn::date_trunc(lit(format), self.expr()),
                [self],
                1,
            ))
        })
    }

    // ---- window functions -----------------------------------------------------------------------

    /// The `row_number()` window function with an empty `OVER` clause (PySpark
    /// `functions.row_number()`). [`PyColumn::over`] fills in the partition/order to complete it.
    ///
    /// PySpark's `row_number()` is `IntegerType`, but DataFusion's is `UInt64`; wrap the window
    /// in `CAST(… AS INT)` so the output type matches Spark. [`PyColumn::over`] unwraps and
    /// reapplies the cast when it re-windows the function.
    #[staticmethod]
    pub fn row_number() -> PyResult<Self> {
        fenced!("Column.row_number", {
            Ok(Self::window_udwf_i32(row_number_udwf(), vec![]))
        })
    }

    /// Spark `rank()` — dense ties leave gaps (`IntegerType`).
    #[staticmethod]
    pub fn rank() -> PyResult<Self> {
        fenced!("Column.rank", {
            Ok(Self::window_udwf_i32(rank_udwf(), vec![]))
        })
    }

    /// Spark `dense_rank()` — ties do not leave gaps (`IntegerType`).
    #[staticmethod]
    pub fn dense_rank() -> PyResult<Self> {
        fenced!("Column.dense_rank", {
            Ok(Self::window_udwf_i32(dense_rank_udwf(), vec![]))
        })
    }

    /// Spark `ntile(n)` — bucket number in `1..=n` (`IntegerType`).
    #[staticmethod]
    pub fn ntile(n: i64) -> PyResult<Self> {
        fenced!("Column.ntile", {
            if n <= 0 {
                return Err(PyValueError::new_err(format!(
                    "ntile requires a positive integer, got {n}"
                )));
            }
            Ok(Self::window_udwf_i32(ntile_udwf(), vec![lit(n)]))
        })
    }

    /// Spark `lag` — preceding row; preserves input type.
    #[staticmethod]
    pub fn lag(args: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.lag", { Ok(Self::window_udwf(lag_udwf(), &args)) })
    }

    /// Spark `lead` — following row; preserves input type.
    #[staticmethod]
    pub fn lead(args: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.lead", { Ok(Self::window_udwf(lead_udwf(), &args)) })
    }

    /// Spark `nth_value` — 1-based; preserves input type.
    #[staticmethod]
    pub fn nth_value(args: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.nth_value", {
            Ok(Self::window_udwf(nth_value_udwf(), &args))
        })
    }

    /// Spark `percent_rank()` — already Float64; no `IntegerType` cast.
    #[staticmethod]
    pub fn percent_rank() -> PyResult<Self> {
        fenced!("Column.percent_rank", {
            Ok(Self::window_udwf(percent_rank_udwf(), &[]))
        })
    }

    /// Spark `cume_dist()` — already Float64; no `IntegerType` cast.
    #[staticmethod]
    pub fn cume_dist() -> PyResult<Self> {
        fenced!("Column.cume_dist", {
            Ok(Self::window_udwf(cume_dist_udwf(), &[]))
        })
    }

    /// A TA window function (`ta_ema`, `ta_adx`, `ta_bbands_upper`, …) as an un-`OVER`ed window
    /// expression: the series column(s) then the scalar literal params, in `args` order.
    ///
    /// The wrapped [`WindowUDF`](datafusion::logical_expr::WindowUDF) is the *same* instance the
    /// session registers for the SQL path (`repark_ta::udf`), so the two surfaces are one kernel.
    ///
    /// # Errors
    /// Returns `ValueError` if `name` is not a known TA window function.
    #[staticmethod]
    #[pyo3(signature = (name, args, null_prefix = 0))]
    pub fn ta_window(name: &str, args: Vec<PyColumn>, null_prefix: usize) -> PyResult<Self> {
        fenced!("Column.ta_window", {
            let udf = repark_ta::udf::window_udf_with_null_prefix(name, null_prefix).ok_or_else(
                || PyValueError::new_err(format!("unknown TA window function {name:?}")),
            )?;
            let arg_exprs: Vec<Expr> = args.iter().map(PyColumn::expr).collect();
            Ok(Self::combine_surveyed(
                Expr::from(WindowFunction::new(
                    WindowFunctionDefinition::WindowUDF(udf),
                    arg_exprs,
                )),
                &args,
            ))
        })
    }

    /// Apply an `OVER (PARTITION BY … ORDER BY … [frame])` window.
    ///
    /// Accepts pure window functions (`row_number`/`rank`/…, optionally CAST-wrapped for Spark
    /// `IntegerType`) and aggregate expressions (`sum`/`max`/…), which become window aggregates.
    ///
    /// `frame_start` and `frame_end` use Spark offsets; `i64::MIN/MAX` mean unbounded.
    ///
    /// # Errors
    /// Returns `ValueError` for mismatched order vectors or a non-window, non-aggregate column.
    /// Unordered window UDFs return `AnalysisException`; other build failures use the engine
    /// exception classifier.
    #[allow(clippy::too_many_arguments)] // PyO3 frame-optional surface mirrors Spark WindowSpec.
    #[pyo3(signature = (
        partition_by,
        order_by,
        order_ascending,
        order_nulls_first,
        frame_units = None,
        frame_start = None,
        frame_end = None,
    ))]
    pub fn over(
        &self,
        partition_by: Vec<PyColumn>,
        order_by: Vec<PyColumn>,
        order_ascending: Vec<bool>,
        order_nulls_first: Vec<bool>,
        frame_units: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        fenced!("Column.over", {
            let levels = Self::input_levels(
                std::iter::once(self)
                    .chain(partition_by.iter())
                    .chain(order_by.iter()),
            );
            let spec = OverSpec {
                partition_by,
                order_by,
                order_ascending,
                order_nulls_first,
                frame_units,
                frame_start,
                frame_end,
            };
            Ok(Self::surveyed_levels(
                build_over_expression(&self.expr, spec)?,
                levels,
            ))
        })
    }

    /// `self + other` (PySpark `Column.__add__`).
    pub fn add(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.add", {
            Ok(Self::combine(self.expr() + other.expr(), [self, other], 1))
        })
    }

    /// `self - other` (PySpark `Column.__sub__`).
    pub fn sub(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.sub", {
            Ok(Self::combine(self.expr() - other.expr(), [self, other], 1))
        })
    }

    /// `self * other` (PySpark `Column.__mul__`).
    pub fn mul(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.mul", {
            Ok(Self::combine(self.expr() * other.expr(), [self, other], 1))
        })
    }

    /// `self / other` (PySpark `Column.__truediv__`).
    ///
    /// PySpark's `/` is **always true (double) division** — `col(7) / col(2)` is `3.5` of
    /// `DoubleType`, and integer division is the separate `div`/`//` operator. DataFusion's `/`
    /// keeps the operand type, so on two integer columns it does integer-truncating division
    /// (`7 / 2 == 3`). We cast both sides to `Float64` first so the result matches Spark; a cast
    /// on operands that are already floating point is a no-op, and NULL casts stay NULL.
    pub fn div(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.div", {
            let numerator = Expr::Cast(Cast::new(Box::new(self.expr()), DataType::Float64));
            let denominator = Expr::Cast(Cast::new(Box::new(other.expr()), DataType::Float64));
            Ok(Self::combine(numerator / denominator, [self, other], 2))
        })
    }

    /// `self % other` (PySpark `Column.__mod__`). Spark's `%` is the modulo (remainder) operator,
    /// which maps to DataFusion's `%`.
    pub fn modulo(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.modulo", {
            Ok(Self::combine(self.expr() % other.expr(), [self, other], 1))
        })
    }

    /// `self == other` (PySpark `Column.__eq__`).
    pub fn eq(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.eq", {
            Ok(Self::combine(
                self.expr().eq(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// `self != other` (PySpark `Column.__ne__`).
    pub fn ne(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.ne", {
            Ok(Self::combine(
                self.expr().not_eq(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// `self < other` (PySpark `Column.__lt__`).
    pub fn lt(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.lt", {
            Ok(Self::combine(
                self.expr().lt(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// `self > other` (PySpark `Column.__gt__`).
    pub fn gt(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.gt", {
            Ok(Self::combine(
                self.expr().gt(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// `self <= other` (PySpark `Column.__le__`).
    pub fn le(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.le", {
            Ok(Self::combine(
                self.expr().lt_eq(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// `self >= other` (PySpark `Column.__ge__`).
    pub fn ge(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.ge", {
            Ok(Self::combine(
                self.expr().gt_eq(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// Logical AND (PySpark `Column.__and__`, spelled `&`). Spark's `&` is boolean AND, not a
    /// bitwise operator, so this maps to the logical `AND`.
    pub fn and_(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.and_", {
            Ok(Self::combine(
                self.expr().and(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// Logical OR (PySpark `Column.__or__`, spelled `|`).
    pub fn or_(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.or_", {
            Ok(Self::combine(
                self.expr().or(other.expr()),
                [self, other],
                1,
            ))
        })
    }

    /// Logical NOT (PySpark `Column.__invert__`, spelled `~`).
    pub fn not_(&self) -> PyResult<Self> {
        fenced!("Column.not_", {
            Ok(Self::combine(!self.expr(), [self], 1))
        })
    }

    /// Rename the column (PySpark `Column.alias`).
    pub fn alias(&self, name: &str) -> PyResult<Self> {
        fenced!("Column.alias", {
            Ok(Self::combine(self.expr().alias(name), [self], 1))
        })
    }

    /// Cast to a target type (PySpark `Column.cast`). `type_spec` is the canonical engine type
    /// string the facade `types` classes emit (`"string"`, `"int"`, `"double"`, `"boolean"`,
    /// `"date"`, `"timestamp"`, `"decimal(p,s)"`, `"float"`, `"byte"`, `"short"`, `"binary"`, …),
    /// plus the PySpark integer-width spellings `"long"` / `"bigint"` (Int64) and short forms
    /// `"tinyint"` / `"smallint"` / `"integer"`.
    ///
    /// # Errors
    /// Returns [`AnalysisException`] if `type_spec` is not a recognized cast type string
    pub fn cast(&self, type_spec: &str) -> PyResult<Self> {
        fenced!("Column.cast", {
            let data_type = parse_data_type(type_spec).map_err(AnalysisException::new_err)?;
            Ok(Self::combine(
                Expr::Cast(Cast::new(Box::new(self.expr()), data_type)),
                [self],
                1,
            ))
        })
    }

    /// Try-cast to a target type (PySpark `Column.try_cast` / SQL ``TRY_CAST``).
    ///
    /// Same type-spec grammar as [`Self::cast`]; on conversion failure the engine yields NULL
    /// instead of raising (DataFusion ``Expr::TryCast``).
    ///
    /// # Errors
    /// Returns [`AnalysisException`] if `type_spec` is not a recognized cast type string.
    pub fn try_cast(&self, type_spec: &str) -> PyResult<Self> {
        fenced!("Column.try_cast", {
            let data_type = parse_data_type(type_spec).map_err(AnalysisException::new_err)?;
            Ok(Self::combine(
                Expr::TryCast(TryCast::new(Box::new(self.expr()), data_type)),
                [self],
                1,
            ))
        })
    }

    // ---- aggregate functions --------------------------------------------------------------------

    /// The column schema name supplies facade aggregate aliases such as `sum(x)`.
    pub fn display_name(&self) -> PyResult<String> {
        fenced!("Column.display_name", {
            Ok(self.grown_read(|expr| expr.schema_name().to_string()))
        })
    }

    /// Collapse nested alias chains to one outer rename.
    ///
    /// ``col("x").alias("x").alias("x")`` and ``col("x").alias("a").alias("b")`` both become a
    /// single ``Alias`` (outermost name) so logical plans no longer show ``… AS x AS x`` or
    /// ``… AS a AS b``. Non-alias expressions are unchanged. Idempotent.
    pub fn collapse_identity_aliases(&self) -> PyResult<Self> {
        fenced!("Column.collapse_identity_aliases", {
            Ok(Self::combine_surveyed(
                collapse_identity_alias_chain(self.expr()),
                [self],
            ))
        })
    }

    /// Build a Spark aggregate for the requested `kind`.
    /// `first` and `last` honor `ignore_nulls`; collection forms always ignore NULL elements.
    /// Other reducers follow their DataFusion kernels. Collection order is nondeterministic.
    ///
    /// # Errors
    /// Returns `ValueError` for an unknown `kind`, or if the aggregate builder fails.
    pub fn aggregate(&self, kind: &str, ignore_nulls: bool) -> PyResult<Self> {
        fenced!("Column.aggregate", {
            if kind == "collect_list" || kind == "collect_set" {
                return Self::collect_aggregate(self, kind == "collect_set");
            }
            let udaf = unary_aggregate_udaf(kind)?;
            let base = udaf.call(vec![self.expr()]);
            // A plain `call` is already a usable aggregate `Expr`; only IGNORE NULLS needs the
            // builder chain (`ExprFunctionExt` on `Expr` → `ExprFuncBuilder` → `build`). The
            // unsigned cast wraps the finished aggregate, since the builder chain only accepts
            // a bare aggregate as its receiver.
            let expr = if ignore_nulls {
                base.null_treatment(NullTreatment::IgnoreNulls)
                    .build()
                    .map_err(|err| {
                        PyValueError::new_err(format!(
                            "could not build aggregate expression: {err}"
                        ))
                    })?
            } else {
                base
            };
            Ok(Self::combine_surveyed(
                cast_unsigned_count_to_signed(&udaf, 1, expr),
                [self],
            ))
        })
    }

    pub fn aggregate_binary(&self, kind: &str, others: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.aggregate_binary", {
            let udaf = nary_aggregate_udaf(kind)?;
            let mut args = vec![self.expr()];
            args.extend(others.iter().map(PyColumn::expr));
            let expr = cast_unsigned_count_to_signed(&udaf, others.len() + 1, udaf.call(args));
            Ok(Self::combine_surveyed(
                expr,
                std::iter::once(self).chain(others.iter()),
            ))
        })
    }

    pub fn approx_percentile_cont(&self, percentile: f64, accuracy: Option<i64>) -> PyResult<Self> {
        fenced!("Column.approx_percentile_cont", {
            if !(0.0..=1.0).contains(&percentile) {
                return Err(PyValueError::new_err(format!(
                    "approx_percentile_cont percentile must be in [0, 1], got {percentile}"
                )));
            }
            let expr = percentile_approx_scalar_expr(self.expr(), percentile, accuracy);
            Ok(Self::combine_surveyed(expr, [self]))
        })
    }
    pub fn approx_percentile_list(
        &self,
        percentages: Vec<f64>,
        accuracy: Option<i64>,
    ) -> PyResult<Self> {
        fenced!("Column.approx_percentile_list", {
            let out_of_range = |percentage| !(0.0..=1.0).contains(percentage);
            if percentages.iter().any(out_of_range) {
                return Err(PyValueError::new_err(
                    "approx_percentile percentages must be in [0, 1]",
                ));
            }
            let expr = percentile_approx_list_expr(self.expr(), percentages, accuracy);
            Ok(Self::combine_surveyed(expr, [self]))
        })
    }

    /// Build a Spark `count` aggregate over `columns` (PySpark `F.count` / `F.countDistinct`).
    ///
    /// # Errors
    /// Returns `ValueError` if `columns` is empty, or if the aggregate builder fails.
    #[staticmethod]
    pub fn count_aggregate(columns: Vec<PyColumn>, distinct: bool) -> PyResult<Self> {
        fenced!("Column.count_aggregate", {
            if columns.is_empty() {
                return Err(PyValueError::new_err(
                    "count() requires at least one argument column",
                ));
            }
            let args: Vec<Expr> = columns.iter().map(PyColumn::expr).collect();
            let expr = if distinct {
                let counted = Self::count_distinct_argument(args)?;
                count_udaf()
                    .call(vec![counted])
                    .distinct()
                    .build()
                    .map_err(|err| {
                        PyValueError::new_err(format!("could not build count(DISTINCT …): {err}"))
                    })?
            } else {
                count_udaf().call(args)
            };
            Ok(Self::combine_surveyed(expr, &columns))
        })
    }
}
