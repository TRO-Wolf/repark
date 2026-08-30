//! Python-facing immutable columns backed by DataFusion logical expressions.
//! Constructors resolve literals and standalone SQL expressions; `DataFrame` methods resolve
//! expressions against their input schema.

use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_window::cume_dist::cume_dist_udwf;
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::nth_value::nth_value_udwf;
use datafusion::functions_window::ntile::ntile_udwf;
use datafusion::functions_window::rank::{dense_rank_udwf, percent_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda, NullTreatment, WindowFunction};
use datafusion::logical_expr::{
    Case, Cast, Expr, ExprFunctionExt, WindowFunctionDefinition, lambda_var, lit,
};
use datafusion::prelude::{SessionContext, col};
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyString};

use crate::AnalysisException;
use crate::fence::fenced;

#[cfg(test)]
mod door_parity_tests;
mod expr_build;
mod function_dispatch;
mod window;

use expr_build::{
    TIMESTAMP_UNIT, collapse_identity_alias_chain, extract_projection_expr, parse_data_type,
    refuse_nested_higher_order, strip_outer_alias,
};
use function_dispatch::{
    binary_aggregate_udaf, call_scalar_expr, cast_unsigned_count_to_signed, unary_aggregate_udaf,
};
use window::{spark_window_frame, unordered_window_frame};

/// A Python-facing immutable DataFusion expression.
#[pyclass(name = "PyColumn", module = "repark._native", from_py_object)]
#[derive(Clone)]
pub struct PyColumn {
    expr: Expr,
}

impl PyColumn {
    /// Wrap a logical [`Expr`] (crate-internal — the facade only ever calls the pyclass
    /// constructors and operators).
    pub(crate) fn from_expr(expr: Expr) -> Self {
        Self { expr }
    }

    /// The held expression, cloned for handoff to a [`crate::dataframe::PyDataFrame`] method.
    pub(crate) fn expr(&self) -> Expr {
        self.expr.clone()
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
        fenced!("Column.column", { Ok(Self::from_expr(col(name))) })
    }

    /// A literal from a Python scalar (PySpark `lit(value)`).
    ///
    /// Supports `None` (SQL NULL), `bool`, `int` (i64), `float` (f64), and `str`. `bool` is checked
    /// **before** `int` because Python's `bool` is a subclass of `int` — `lit(True)` must be a
    /// boolean literal, not the integer `1`.
    ///
    /// # Errors
    /// Returns `ValueError` for a Python type with no scalar literal mapping.
    #[staticmethod]
    pub fn literal(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        fenced!("Column.literal", {
            if value.is_none() {
                return Ok(Self::from_expr(lit(ScalarValue::Null)));
            }
            // Python bool is an int subclass, so test it before extracting integers.
            if value.is_instance_of::<PyBool>() {
                let boolean: bool = value.extract()?;
                return Ok(Self::from_expr(lit(boolean)));
            }
            if value.is_instance_of::<PyInt>() {
                let integer: i64 = value.extract()?;
                return Ok(Self::from_expr(lit(integer)));
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
            expr_build::contains_higher_order(&self.expr)
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
            for (params, body) in lambdas {
                let body = body.expr();
                refuse_nested_higher_order(&body, name, "lambda")?;
                args.push(Expr::Lambda(Lambda::new(params, body)));
            }
            Ok(Self::from_expr(Expr::HigherOrderFunction(
                HigherOrderFunction::new(function, args),
            )))
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
            for (index, column) in fields.into_iter().enumerate() {
                let expr = column.expr();
                let (field_name, value) = match expr {
                    Expr::Alias(alias) => (alias.name.clone(), *alias.expr),
                    other => {
                        let name = other.schema_name().to_string();
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
            Ok(Self::from_expr(
                datafusion::functions::expr_fn::named_struct(args),
            ))
        })
    }

    /// A SQL-string expression (PySpark `expr(sql)` / `F.expr`).
    ///
    /// Parses `sql` on a throwaway context provisioned with `repark_functions::register_all` +
    /// `analyzer_rules()` (same function surface as a repark session), plans `SELECT (<sql>)`,
    /// then runs the analyzer **eagerly** (`repark_functions::analyze_eagerly`) before
    /// extracting the projection expression. The extracted [`Expr`] therefore already carries
    /// the Spark rewrites (integer `/` → both-operands-double, div-by-zero `nullif`,
    /// planner-embedded `substr` → shim, …) *and* their post-analysis types — so both the
    /// values and the schema a consumer `DataFrame` exports over Arrow match `spark.sql`. The
    /// rules are idempotent, so the consumer session's own analysis pass is a no-op on this
    /// subtree. Column-referencing expressions still fail (empty schema / no FROM).
    ///
    /// # Errors
    /// Returns `ParseException` for invalid SQL and `AnalysisException` for unresolved columns.
    #[staticmethod]
    pub fn sql(sql: &str) -> PyResult<Self> {
        fenced!("Column.sql", {
            // This path bypasses the Spark SQL router, so apply its parse-altitude valves here.
            repark_spark::refuse_sql_fragment(sql).map_err(crate::datafusion_to_py_err)?;
            let context = SessionContext::new();
            repark_functions::register_all(&context);
            for rule in repark_functions::analyzer_rules() {
                context.add_analyzer_rule(rule);
            }
            let select_sql = format!("SELECT ({sql}) AS _repark_expr");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| {
                    PyValueError::new_err(format!("could not start expr runtime: {err}"))
                })?;
            let plan = runtime
                .block_on(async {
                    let df = context.sql(&select_sql).await?;
                    repark_functions::analyze_eagerly(&context.state(), df.logical_plan().clone())
                })
                .map_err(crate::datafusion_to_py_err)?;
            let expr = strip_outer_alias(extract_projection_expr(&plan)?);
            // Cast Utf8View for Arrow FFI compatibility; analyzed numeric types remain unchanged.
            let expr = match plan
                .schema()
                .fields()
                .first()
                .map(|field| field.data_type().clone())
            {
                Some(DataType::Utf8View) => Expr::Cast(Cast::new(Box::new(expr), DataType::Utf8)),
                _ => expr,
            };
            Ok(Self::from_expr(expr))
        })
    }

    /// `IS NULL` predicate (PySpark `Column.isNull`).
    pub fn is_null(&self) -> PyResult<Self> {
        fenced!("Column.is_null", {
            Ok(Self::from_expr(self.expr.clone().is_null()))
        })
    }

    /// `IS NOT NULL` predicate (PySpark `Column.isNotNull`).
    pub fn is_not_null(&self) -> PyResult<Self> {
        fenced!("Column.is_not_null", {
            Ok(Self::from_expr(self.expr.clone().is_not_null()))
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
                .into_iter()
                .map(|(condition, value)| (Box::new(condition.expr), Box::new(value.expr)))
                .collect();
            let else_expr = otherwise.map(|column| Box::new(column.expr));
            Ok(Self::from_expr(Expr::Case(Case {
                expr: None,
                when_then_expr,
                else_expr,
            })))
        })
    }

    /// First non-null of the arguments (PySpark `coalesce(*cols)`).
    #[staticmethod]
    pub fn coalesce(columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.coalesce", {
            let exprs = columns.iter().map(PyColumn::expr).collect();
            Ok(Self::from_expr(datafusion::functions::expr_fn::coalesce(
                exprs,
            )))
        })
    }

    /// String concatenation of the arguments (PySpark `concat(*cols)`).
    ///
    /// PySpark's `concat` **propagates NULL**: if *any* argument is NULL the result is NULL.
    /// DataFusion's `concat` instead treats a NULL argument as the empty string and skips it, so
    /// the raw `concat` diverges. We wrap it in a `CASE WHEN (arg1 IS NULL OR … OR argN IS NULL)
    /// THEN NULL ELSE concat(args) END` guard so the any-null → NULL semantics match Spark.
    #[staticmethod]
    pub fn concat(columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.concat", {
            let exprs: Vec<Expr> = columns.iter().map(PyColumn::expr).collect();
            // Match Spark's Utf8 result and keep both CASE arms at one Arrow type.
            let concatenated = Expr::Cast(Cast::new(
                Box::new(datafusion::functions::expr_fn::concat(exprs.clone())),
                DataType::Utf8,
            ));
            // Guard every argument because Spark propagates NULL.
            let any_null = exprs.into_iter().map(Expr::is_null).reduce(Expr::or);
            Ok(match any_null {
                None => Self::from_expr(concatenated),
                Some(any_null) => Self::from_expr(Expr::Case(Case::new(
                    None,
                    // Keep the NULL arm at the same Utf8 type as the concatenated value.
                    vec![(Box::new(any_null), Box::new(lit(ScalarValue::Utf8(None))))],
                    Some(Box::new(concatenated)),
                ))),
            })
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
            Ok(Self::from_expr(call_scalar_expr(name, exprs)?))
        })
    }

    // ---- Spark date functions ---------------------------------------------------------------

    /// Spark `year(date)` — the calendar year.
    pub fn year(&self) -> PyResult<Self> {
        fenced!("Column.year", {
            Ok(Self::from_expr(repark_functions::expr_fn::year(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `month(date)` — the month of year, 1..=12.
    pub fn month(&self) -> PyResult<Self> {
        fenced!("Column.month", {
            Ok(Self::from_expr(repark_functions::expr_fn::month(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `quarter(date)` — the quarter of year, 1..=4.
    pub fn quarter(&self) -> PyResult<Self> {
        fenced!("Column.quarter", {
            Ok(Self::from_expr(repark_functions::expr_fn::quarter(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `weekofyear(date)` — the ISO-8601 week number.
    pub fn weekofyear(&self) -> PyResult<Self> {
        fenced!("Column.weekofyear", {
            Ok(Self::from_expr(repark_functions::expr_fn::weekofyear(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `dayofweek(date)` — 1=Sunday .. 7=Saturday.
    pub fn dayofweek(&self) -> PyResult<Self> {
        fenced!("Column.dayofweek", {
            Ok(Self::from_expr(repark_functions::expr_fn::dayofweek(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `weekday(date)` — 0=Monday .. 6=Sunday.
    pub fn weekday(&self) -> PyResult<Self> {
        fenced!("Column.weekday", {
            Ok(Self::from_expr(repark_functions::expr_fn::weekday(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `dayofmonth(date)` — the day of month, 1..=31.
    pub fn dayofmonth(&self) -> PyResult<Self> {
        fenced!("Column.dayofmonth", {
            Ok(Self::from_expr(repark_functions::expr_fn::dayofmonth(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `dayofyear(date)` — the day of year, 1..=366.
    pub fn dayofyear(&self) -> PyResult<Self> {
        fenced!("Column.dayofyear", {
            Ok(Self::from_expr(repark_functions::expr_fn::dayofyear(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `last_day(date)` — the last day of the month containing this date.
    pub fn last_day(&self) -> PyResult<Self> {
        fenced!("Column.last_day", {
            Ok(Self::from_expr(repark_functions::expr_fn::last_day(
                self.expr.clone(),
            )))
        })
    }

    /// Spark `add_months(start, num_months)` — end-of-month-preserving month arithmetic.
    pub fn add_months(&self, num_months: &PyColumn) -> PyResult<Self> {
        fenced!("Column.add_months", {
            Ok(Self::from_expr(repark_functions::expr_fn::add_months(
                self.expr.clone(),
                num_months.expr.clone(),
            )))
        })
    }

    /// Spark `date_add(start, num_days)` — the date `num_days` after this date.
    pub fn date_add(&self, num_days: &PyColumn) -> PyResult<Self> {
        fenced!("Column.date_add", {
            Ok(Self::from_expr(repark_functions::expr_fn::date_add(
                self.expr.clone(),
                num_days.expr.clone(),
            )))
        })
    }

    /// Spark `date_format(timestamp, format)` — format with a Java pattern string (a literal).
    pub fn date_format(&self, format: &str) -> PyResult<Self> {
        fenced!("Column.date_format", {
            Ok(Self::from_expr(repark_functions::expr_fn::date_format(
                self.expr.clone(),
                lit(format),
            )))
        })
    }

    /// Spark `trunc(date, format)` — truncate a DATE to `format` (year/month/week/quarter).
    pub fn trunc(&self, format: &str) -> PyResult<Self> {
        fenced!("Column.trunc", {
            Ok(Self::from_expr(repark_functions::expr_fn::trunc(
                self.expr.clone(),
                lit(format),
            )))
        })
    }

    /// Spark `date_trunc(format, timestamp)` — truncate this TIMESTAMP to `format`. The Spark
    /// argument order (format first) is applied here; the facade passes the format as a literal.
    pub fn date_trunc(&self, format: &str) -> PyResult<Self> {
        fenced!("Column.date_trunc", {
            Ok(Self::from_expr(repark_functions::expr_fn::date_trunc(
                lit(format),
                self.expr.clone(),
            )))
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
    /// The `repark.ta` facade builds these; [`PyColumn::over`] attaches the `ORDER BY` and
    /// partition.
    ///
    /// The wrapped [`WindowUDF`](datafusion::logical_expr::WindowUDF) is the *same* instance the
    /// session registers for the SQL path (`repark_ta::udf`), so the two surfaces are one kernel.
    ///
    /// # Errors
    /// Returns `ValueError` if `name` is not a known TA window function.
    #[staticmethod]
    pub fn ta_window(name: &str, args: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.ta_window", {
            let udf = repark_ta::udf::window_udf(name).ok_or_else(|| {
                PyValueError::new_err(format!("unknown TA window function {name:?}"))
            })?;
            let arg_exprs: Vec<Expr> = args.iter().map(PyColumn::expr).collect();
            Ok(Self::from_expr(Expr::from(WindowFunction::new(
                WindowFunctionDefinition::WindowUDF(udf),
                arg_exprs,
            ))))
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
            if order_by.len() != order_ascending.len() || order_by.len() != order_nulls_first.len()
            {
                return Err(PyValueError::new_err(
                    "over expects order_by, order_ascending, and order_nulls_first of equal length",
                ));
            }
            // Window UDF / aggregate → WindowFunction, under an optional CAST that is peeled
            // here and re-applied to the window result. The facade casts count-like aggregates
            // to Int64 (Spark has no unsigned type), so `over` must look through a CAST to find
            // the aggregate or the windowed form refuses a call the grouped form accepts.
            let (inner, cast_type) = match &self.expr {
                Expr::Cast(cast) => (&*cast.expr, Some(cast.field.data_type().clone())),
                other => (other, None),
            };
            let window_expr = match inner {
                Expr::WindowFunction(_) => inner.clone(),
                Expr::AggregateFunction(agg) => Expr::from(WindowFunction::new(
                    WindowFunctionDefinition::AggregateUDF(std::sync::Arc::clone(&agg.func)),
                    agg.params.args.clone(),
                )),
                _ => {
                    return Err(PyValueError::new_err(
                        "over() applies only to a window or aggregate function column \
                         (e.g. row_number(), sum(...))",
                    ));
                }
            };
            let partitions: Vec<Expr> = partition_by.iter().map(PyColumn::expr).collect();
            let orderings: Vec<_> = order_by
                .iter()
                .zip(order_ascending)
                .zip(order_nulls_first)
                .map(|((column, is_ascending), nulls_first)| {
                    column.expr().sort(is_ascending, nulls_first)
                })
                .collect();
            // The frame decision reads the built function, so take it before the builder moves it.
            let unordered_frame = order_by
                .is_empty()
                .then(|| unordered_window_frame(&window_expr))
                .transpose()
                .map_err(crate::AnalysisException::new_err)?;
            let mut builder = window_expr.partition_by(partitions).order_by(orderings);
            if let Some(units_text) = frame_units.as_deref() {
                let start = frame_start.ok_or_else(|| {
                    PyValueError::new_err("over frame_units requires frame_start")
                })?;
                let end = frame_end
                    .ok_or_else(|| PyValueError::new_err("over frame_units requires frame_end"))?;
                let frame =
                    spark_window_frame(units_text, start, end).map_err(PyValueError::new_err)?;
                builder = builder.window_frame(frame);
            } else if let Some(frame) = unordered_frame {
                builder = builder.window_frame(frame);
            }
            let windowed = builder.build().map_err(|err| {
                PyValueError::new_err(format!("could not build window expression: {err}"))
            })?;
            let result = match cast_type {
                Some(data_type) => Expr::Cast(Cast::new(Box::new(windowed), data_type)),
                None => windowed,
            };
            Ok(Self::from_expr(result))
        })
    }

    /// `self + other` (PySpark `Column.__add__`).
    pub fn add(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.add", {
            Ok(Self::from_expr(self.expr.clone() + other.expr.clone()))
        })
    }

    /// `self - other` (PySpark `Column.__sub__`).
    pub fn sub(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.sub", {
            Ok(Self::from_expr(self.expr.clone() - other.expr.clone()))
        })
    }

    /// `self * other` (PySpark `Column.__mul__`).
    pub fn mul(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.mul", {
            Ok(Self::from_expr(self.expr.clone() * other.expr.clone()))
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
            let numerator = Expr::Cast(Cast::new(Box::new(self.expr.clone()), DataType::Float64));
            let denominator =
                Expr::Cast(Cast::new(Box::new(other.expr.clone()), DataType::Float64));
            Ok(Self::from_expr(numerator / denominator))
        })
    }

    /// `self % other` (PySpark `Column.__mod__`). Spark's `%` is the modulo (remainder) operator,
    /// which maps to DataFusion's `%`.
    pub fn modulo(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.modulo", {
            Ok(Self::from_expr(self.expr.clone() % other.expr.clone()))
        })
    }

    /// `self == other` (PySpark `Column.__eq__`).
    pub fn eq(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.eq", {
            Ok(Self::from_expr(self.expr.clone().eq(other.expr.clone())))
        })
    }

    /// `self != other` (PySpark `Column.__ne__`).
    pub fn ne(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.ne", {
            Ok(Self::from_expr(
                self.expr.clone().not_eq(other.expr.clone()),
            ))
        })
    }

    /// `self < other` (PySpark `Column.__lt__`).
    pub fn lt(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.lt", {
            Ok(Self::from_expr(self.expr.clone().lt(other.expr.clone())))
        })
    }

    /// `self > other` (PySpark `Column.__gt__`).
    pub fn gt(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.gt", {
            Ok(Self::from_expr(self.expr.clone().gt(other.expr.clone())))
        })
    }

    /// `self <= other` (PySpark `Column.__le__`).
    pub fn le(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.le", {
            Ok(Self::from_expr(self.expr.clone().lt_eq(other.expr.clone())))
        })
    }

    /// `self >= other` (PySpark `Column.__ge__`).
    pub fn ge(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.ge", {
            Ok(Self::from_expr(self.expr.clone().gt_eq(other.expr.clone())))
        })
    }

    /// Logical AND (PySpark `Column.__and__`, spelled `&`). Spark's `&` is boolean AND, not a
    /// bitwise operator, so this maps to the logical `AND`.
    pub fn and_(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.and_", {
            Ok(Self::from_expr(self.expr.clone().and(other.expr.clone())))
        })
    }

    /// Logical OR (PySpark `Column.__or__`, spelled `|`).
    pub fn or_(&self, other: &PyColumn) -> PyResult<Self> {
        fenced!("Column.or_", {
            Ok(Self::from_expr(self.expr.clone().or(other.expr.clone())))
        })
    }

    /// Logical NOT (PySpark `Column.__invert__`, spelled `~`).
    pub fn not_(&self) -> PyResult<Self> {
        fenced!("Column.not_", { Ok(Self::from_expr(!self.expr.clone())) })
    }

    /// Rename the column (PySpark `Column.alias`).
    pub fn alias(&self, name: &str) -> PyResult<Self> {
        fenced!("Column.alias", {
            Ok(Self::from_expr(self.expr.clone().alias(name)))
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
            Ok(Self::from_expr(datafusion::logical_expr::Expr::Cast(
                datafusion::logical_expr::Cast::new(Box::new(self.expr.clone()), data_type),
            )))
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
            Ok(Self::from_expr(datafusion::logical_expr::Expr::TryCast(
                datafusion::logical_expr::TryCast::new(Box::new(self.expr.clone()), data_type),
            )))
        })
    }

    // ---- aggregate functions --------------------------------------------------------------------

    /// The column schema name supplies facade aggregate aliases such as `sum(x)`.
    pub fn display_name(&self) -> PyResult<String> {
        fenced!("Column.display_name", {
            Ok(self.expr.schema_name().to_string())
        })
    }

    /// Collapse nested alias chains to one outer rename.
    ///
    /// ``col("x").alias("x").alias("x")`` and ``col("x").alias("a").alias("b")`` both become a
    /// single ``Alias`` (outermost name) so logical plans no longer show ``… AS x AS x`` or
    /// ``… AS a AS b``. Non-alias expressions are unchanged. Idempotent.
    pub fn collapse_identity_aliases(&self) -> PyResult<Self> {
        fenced!("Column.collapse_identity_aliases", {
            Ok(Self::from_expr(collapse_identity_alias_chain(
                self.expr.clone(),
            )))
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
                return Self::collect_aggregate(self.expr.clone(), kind == "collect_set");
            }
            let udaf = unary_aggregate_udaf(kind)?;
            let base = udaf.call(vec![self.expr.clone()]);
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
            Ok(Self::from_expr(cast_unsigned_count_to_signed(
                &udaf, 1, expr,
            )))
        })
    }

    /// Binary aggregate: `corr`, `covar_pop`, or `covar_samp`.
    pub fn aggregate_binary(&self, kind: &str, other: PyColumn) -> PyResult<Self> {
        fenced!("Column.aggregate_binary", {
            let udaf = binary_aggregate_udaf(kind)?;
            let expr = udaf.call(vec![self.expr.clone(), other.expr.clone()]);
            Ok(Self::from_expr(cast_unsigned_count_to_signed(
                &udaf, 2, expr,
            )))
        })
    }

    /// Approximate continuous percentile.
    ///
    /// Lowers to DataFusion's t-digest `approx_percentile_cont(col, percentile)`. Facade names
    /// `percentile_approx` / `approx_percentile` (Spark) alias the same UDAF after
    /// `repark_functions::register_all`. `percentile` must be in `[0, 1]`.
    ///
    /// # Errors
    /// Returns `ValueError` when `percentile` is outside `[0, 1]`.
    pub fn approx_percentile_cont(&self, percentile: f64) -> PyResult<Self> {
        fenced!("Column.approx_percentile_cont", {
            if !(0.0..=1.0).contains(&percentile) {
                return Err(PyValueError::new_err(format!(
                    "approx_percentile_cont percentile must be in [0, 1], got {percentile}"
                )));
            }
            let expr = approx_percentile_cont_udaf().call(vec![self.expr.clone(), lit(percentile)]);
            Ok(Self::from_expr(expr))
        })
    }

    /// Build a Spark `count` aggregate over `columns` (PySpark `F.count` / `F.countDistinct`).
    ///
    /// One column is `count(col)` (skips NULLs); a literal-`1` column is `count(*)` (counts every
    /// Several columns with `distinct = true` count distinct tuples.
    /// DataFusion rejects multi-argument `COUNT DISTINCT` natively, so the multi-column form packs
    /// the arguments into a `struct` and nulls out any row where *any* field is NULL (Spark
    /// excludes a row when any of the distinct columns is NULL — verified against live PySpark
    /// 4.1.2). The facade sets the Spark output name via `alias`.
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
            Ok(Self::from_expr(expr))
        })
    }
}

#[cfg(test)]
mod expr_tests {
    use super::*;

    #[test]
    fn expr_sql_substr_zero_matches_spark() {
        let column = PyColumn::sql("substr('hello', 0, 3)").expect("parse");
        // Consumer context is a *different* SessionContext (mirrors F.expr → spark DF handoff).
        let context = SessionContext::new();
        repark_functions::register_all(&context);
        for rule in repark_functions::analyzer_rules() {
            context.add_analyzer_rule(rule);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let batches = runtime.block_on(async {
            let df = context.sql("SELECT 1 AS dummy").await.unwrap();
            let df = df.select(vec![column.expr().alias("s")]).unwrap();
            df.collect().await.unwrap()
        });
        let pretty = arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(
            pretty.contains("hel"),
            "expected Spark substr pos0 → hel, got:\n{pretty}\nexpr={:?}",
            column.expr()
        );
    }

    /// Two- and three-argument `substr` calls use the Spark-compatible UDF.
    ///
    /// The analyzer must rewrite the three-argument form as that UDF, so zero-based slicing
    /// matches SQL (`'hello'` pos0 len3 → `'he'`).
    #[test]
    fn call_scalar_substr_zero_matches_spark() {
        use datafusion::arrow::array::StringArray;

        let string_col = PyColumn::from_expr(lit("hello"));
        let start = PyColumn::from_expr(lit(0_i64));
        let length = PyColumn::from_expr(lit(3_i64));
        let column = PyColumn::call_scalar("substr", vec![string_col, start, length])
            .expect("call_scalar substr");
        let context = SessionContext::new();
        repark_functions::register_all(&context);
        for rule in repark_functions::analyzer_rules() {
            context.add_analyzer_rule(rule);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let batches = runtime.block_on(async {
            let df = context.sql("SELECT 1 AS dummy").await.unwrap();
            let df = df.select(vec![column.expr().alias("s")]).unwrap();
            df.collect().await.unwrap()
        });
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        assert_eq!(
            array.value(0),
            "hel",
            "call_scalar substr pos0 len3 must be Spark 'hel' (not DF 'he'); expr={:?}",
            column.expr()
        );
        // Negative start from end: substr('hello', -3, 2) → 'll'
        let neg = PyColumn::call_scalar(
            "substr",
            vec![
                PyColumn::from_expr(lit("hello")),
                PyColumn::from_expr(lit(-3_i64)),
                PyColumn::from_expr(lit(2_i64)),
            ],
        )
        .expect("call_scalar substr neg");
        let batches_neg = runtime.block_on(async {
            let df = context.sql("SELECT 1 AS dummy").await.unwrap();
            let df = df.select(vec![neg.expr().alias("s")]).unwrap();
            df.collect().await.unwrap()
        });
        let array_neg = batches_neg[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        assert_eq!(array_neg.value(0), "ll");
    }

    /// The handoff expression must carry its post-analysis type so the logical schema matches
    /// executed buffers. Integer division `5/2` must remain `Float64` through the handoff.
    #[test]
    fn expr_sql_integer_division_hands_off_float64() {
        use datafusion::arrow::array::Float64Array;

        let column = PyColumn::sql("5/2").expect("parse");
        // Consumer context is a *different* SessionContext (mirrors F.expr → spark DF handoff).
        let context = SessionContext::new();
        repark_functions::register_all(&context);
        for rule in repark_functions::analyzer_rules() {
            context.add_analyzer_rule(rule);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (logical_type, batches) = runtime.block_on(async {
            let df = context.sql("SELECT 1 AS dummy").await.unwrap();
            let df = df.select(vec![column.expr().alias("x")]).unwrap();
            let logical_type = df.schema().field(0).data_type().clone();
            (logical_type, df.collect().await.unwrap())
        });
        assert_eq!(
            logical_type,
            DataType::Float64,
            "F.expr('5/2') must hand off Float64 — an Int64 label over Float64 buffers \
             bit-reinterprets at the Arrow boundary"
        );
        let executed_type = batches[0].schema().field(0).data_type().clone();
        assert_eq!(
            executed_type, logical_type,
            "logical (exported) schema and executed batch schema must agree"
        );
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("executed column must be Float64");
        assert!((values.value(0) - 2.5).abs() < f64::EPSILON);
    }
}
