//! [`PyColumn`] — the Python-facing wrapper over a DataFusion [`Expr`].
//!
//! A [`PyColumn`] holds a single fully-formed logical [`Expr`]. The pure-Python `Column` facade
//! (under `python/repark`) builds these through the constructors here (`column` / `literal` /
//! `sql`) and composes them with the operator methods, then hands the inner [`Expr`] to the
//! [`crate::dataframe::PyDataFrame`] transform methods (`with_column`, `filter`, `select`, `sort`,
//! `join`).
//!
//! Construction covers the three PySpark `Column` origins: a column reference (`col("x")`), a
//! literal (`lit(1)` / `lit(None)`), and a SQL-string expression (`expr("make_date(2020, 1, 1)")`).
//! The SQL path parses eagerly in a throwaway [`SessionContext`] against an empty schema, so
//! `DataFusion`'s built-in functions and literals resolve; an `expr` string that references a
//! *column* fails loudly here (the empty schema has no columns). Resolving a column-referencing
//! `expr` against the `DataFrame` it is applied to is the DataFrame-bound `expr` path a later
//! increment adds — the acceptance kernel that needs it (the date-dimension transform) lands with
//! the date-function group, not here.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::bit_and_or_xor::{bit_and_udaf, bit_or_udaf, bit_xor_udaf};
use datafusion::functions_aggregate::correlation::corr_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::covariance::{covar_pop_udaf, covar_samp_udaf};
use datafusion::functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion::functions_aggregate::median::median_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use datafusion::functions_window::cume_dist::cume_dist_udwf;
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::nth_value::nth_value_udwf;
use datafusion::functions_window::ntile::ntile_udwf;
use datafusion::functions_window::rank::{dense_rank_udwf, percent_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr::{Alias, NullTreatment, ScalarFunction, WindowFunction};
use datafusion::logical_expr::{
    Case, Cast, Expr, ExprFunctionExt, Operator, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition, binary_expr, lit,
};
use datafusion::prelude::{SessionContext, col};
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyString};

use crate::AnalysisException;
use crate::fence::fenced;

/// ===========================================================================================
/// The Python-facing `Column` (`repark._native.PyColumn`).
///
/// Wraps one DataFusion logical [`Expr`]. Cheap to [`Clone`] (an `Expr` is an `Arc`-heavy tree);
/// every operator method returns a *new* [`PyColumn`] rather than mutating in place, matching
/// PySpark's immutable `Column` semantics.
/// ===========================================================================================
// `from_py_object`: opt in to the `FromPyObject` derive so `PyColumn` (and `Vec<PyColumn>`) can be
// extracted by value as a `PyDataFrame` method argument (pyo3 0.29 made this Clone-based derive
// opt-in). The extraction clones the held `Expr`, which is cheap.
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

    /// Window UDF with Spark `IntegerType` cast (`row_number` / `rank` / `dense_rank` / `ntile`).
    fn window_udwf_i32(
        udwf: std::sync::Arc<datafusion::logical_expr::WindowUDF>,
        args: Vec<Expr>,
    ) -> Self {
        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(udwf),
            args,
        ));
        Self::from_expr(Expr::Cast(Cast::new(Box::new(window), DataType::Int32)))
    }

    /// Window UDF with no `IntegerType` cast.
    fn window_udwf(
        udwf: std::sync::Arc<datafusion::logical_expr::WindowUDF>,
        args: &[PyColumn],
    ) -> Self {
        Self::from_expr(Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(udwf),
            args.iter().map(PyColumn::expr).collect(),
        )))
    }
}

/// ===========================================================================================
/// Build a DataFusion [`WindowFrame`] from Spark-relative `rowsBetween` / `rangeBetween` offsets.
///
/// Spark offsets: `i64::MIN` → unbounded preceding; `i64::MAX` → unbounded following;
/// `0` → current row; negative → N preceding; positive → N following.
/// ===========================================================================================
fn spark_window_frame(units_text: &str, start: i64, end: i64) -> Result<WindowFrame, String> {
    let units = match units_text {
        "rows" => WindowFrameUnits::Rows,
        "range" => WindowFrameUnits::Range,
        other => {
            return Err(format!(
                "window frame units must be 'rows' or 'range', got {other:?}"
            ));
        }
    };
    let start_bound = spark_offset_to_bound(start, units)?;
    let end_bound = spark_offset_to_bound(end, units)?;
    Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
}

fn spark_offset_to_bound(offset: i64, units: WindowFrameUnits) -> Result<WindowFrameBound, String> {
    // Mirror PySpark `Window` JVM-long clamping (facade already clamps to `i64` extremes).
    if offset == i64::MIN {
        return Ok(WindowFrameBound::Preceding(unbounded_scalar(units)));
    }
    if offset == i64::MAX {
        return Ok(WindowFrameBound::Following(unbounded_scalar(units)));
    }
    if offset == 0 {
        return Ok(WindowFrameBound::CurrentRow);
    }
    if offset < 0 {
        let n = u64::try_from(-offset)
            .map_err(|_| format!("window frame offset {offset} cannot be converted to a bound"))?;
        return Ok(WindowFrameBound::Preceding(offset_scalar(units, n)));
    }
    let n = u64::try_from(offset)
        .map_err(|_| format!("window frame offset {offset} cannot be converted to a bound"))?;
    Ok(WindowFrameBound::Following(offset_scalar(units, n)))
}

fn unbounded_scalar(units: WindowFrameUnits) -> ScalarValue {
    match units {
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => ScalarValue::UInt64(None),
        // RANGE unbounded uses null of a numeric type; Int64 is the common Spark long path.
        WindowFrameUnits::Range => ScalarValue::Int64(None),
    }
}

fn offset_scalar(units: WindowFrameUnits, n: u64) -> ScalarValue {
    match units {
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => ScalarValue::UInt64(Some(n)),
        // Frame offsets from Spark are non-negative magnitudes; i64::MAX is the only
        // out-of-range input and is handled as unbounded before this path.
        WindowFrameUnits::Range => ScalarValue::Int64(Some(i64::try_from(n).unwrap_or(i64::MAX))),
    }
}

/// ===========================================================================================
/// Drop a single outer `AS name` alias so `F.expr(...).alias("x")` can re-alias cleanly.
/// ===========================================================================================
/// Spark `sec`/`csc` at exact zero divisor → `±Inf` (live 4.1.2), not NULL.
///
/// Global non-ANSI `/` rewrite (`nullif(divisor, 0)`) would turn bare `1/sin(0)` into NULL.
/// Branch on exact zero so the reciprocal-trig surface matches Spark without changing the
/// div-by-zero analyzer rule (F2).
fn reciprocal_trig_or_inf(divisor: Expr) -> Expr {
    Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(binary_expr(divisor.clone(), Operator::Eq, lit(0.0f64))),
            Box::new(lit(f64::INFINITY)),
        )],
        else_expr: Some(Box::new(lit(1.0f64) / divisor)),
    })
}

fn strip_outer_alias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    }
}

/// ===========================================================================================
/// Collapse nested ``Alias`` layers to a single outer rename (r25 T3 plan hygiene).
/// ===========================================================================================
///
/// DataFusion pretty-prints ``col.alias("x").alias("x")`` as ``… AS x AS x`` and
/// ``col.alias("a").alias("b")`` as ``… AS a AS b``. The facade N2 collapse skipped a *further*
/// ``for_select`` re-alias when ``display_name`` already matched, but did not unwrap aliases
/// already stacked on the native [`Expr`]. Any nested Alias chain peels to the core expr plus
/// **one** outer alias (outermost name wins) — matching H2's display-side re-alias collapse.
/// The outer alias's qualifier (``relation``) and Arrow field ``metadata`` are part of the
/// projection identity and survive the peel; a non-nested Alias passes through untouched.
fn collapse_identity_alias_chain(expr: Expr) -> Expr {
    let Expr::Alias(alias) = expr else {
        return expr;
    };
    // A lone Alias must not be rebuilt: `Expr::alias` would null out `relation`/`metadata`
    // that DataFusion's optimizer attaches (e.g. `alias_qualified` in distinct rewrites).
    if !matches!(alias.expr.as_ref(), Expr::Alias(_)) {
        return Expr::Alias(alias);
    }
    // Outermost name/qualifier/metadata are the projection identity; peel every
    // intermediate Alias beneath them.
    let Alias {
        expr: boxed,
        relation,
        name,
        metadata,
    } = alias;
    let mut inner = *boxed;
    while let Expr::Alias(inner_alias) = inner {
        inner = *inner_alias.expr;
    }
    inner.alias_qualified_with_metadata(relation, name, metadata)
}

/// ===========================================================================================
/// Pull the first projection expression out of an analyzed/optimized plan.
/// ===========================================================================================
fn extract_projection_expr(plan: &LogicalPlan) -> PyResult<Expr> {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .first()
            .cloned()
            .ok_or_else(|| PyValueError::new_err("expr plan produced an empty projection")),
        // Optimized plans may wrap the projection (e.g. `SubqueryAlias` / `TableScan` empty).
        other => other
            .inputs()
            .iter()
            .find_map(|input| extract_projection_expr(input).ok())
            .ok_or_else(|| {
                PyValueError::new_err(format!("expr plan had no projection to extract: {other}"))
            }),
    }
}

// These are Python-facing constructors/operators: every return value is consumed by the caller in
// Python, so `must_use` / `return_self_not_must_use` don't apply, and pyclass args arrive by value.
//
// Every method returns `PyResult<Self>` because every method routes its body through the shared
// SAF-007 fence (`fenced!`): a Rust panic anywhere in the body is caught and returned as the base
// `PySparkException` (a `RuntimeError`) rather than escaping to PyO3's trampoline as an uncatchable
// `PanicException`. That is the ONLY error the pure `Expr`-builder methods can return; the parsing /
// planning methods (`literal`, `sql`, `cast`, `ta_window`, `over`) additionally document their own
// analysis/value errors in a `# Errors` section. `missing_errors_doc` is allowed at the block level
// so the uniform fence-error contract is stated once here instead of on every method.
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
            // `bool` before `int`: Python `bool` is an `int` subclass, so an unguarded `int` extract
            // would swallow `True`/`False` as `1`/`0`.
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

    /// Pack fields into a struct expression (PySpark `functions.struct`).
    ///
    /// Uses DataFusion `struct(args…)` over already-built child [`Expr`]s so the result
    /// binds in a parent `DataFrame` projection (unlike free-SQL `named_struct`, which has
    /// no FROM schema at `Column.sql` construction time — X3 census).
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
            // DataFusion's bare `struct(args)` always names fields c0/c1/… — aliases on
            // children are ignored by `StructFunc::return_type`. Spark `functions.struct`
            // keeps argument names, which DF expresses as `named_struct('name', expr, …)`
            // (the SQL planner rewrites `struct(a as a, …)` the same way). Extract outer
            // Alias names the Python facade attaches (octo X3 C4).
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
    /// Returns `ValueError` if `sql` does not parse/plan, or references a column (no schema).
    #[staticmethod]
    pub fn sql(sql: &str) -> PyResult<Self> {
        fenced!("Column.sql", {
            // G15: F.expr / Column.sql bypass the Spark-door router. Refuse COLLATE
            // here so the fragment sees the same actionable message as spark.sql().
            repark_spark::refuse_collation_in_sql(sql).map_err(crate::datafusion_to_py_err)?;
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
                    // `ctx.sql` returns a PRE-analysis plan (its schema can disagree with executed
                    // types — the AR-WG-SQL boundary lesson); analyze before anything reads it.
                    repark_functions::analyze_eagerly(&context.state(), df.logical_plan().clone())
                })
                // Classify the DataFusion error into the taxonomy (WG-3): a syntax error becomes
                // `ParseException`, a column-referencing / otherwise-unresolvable expression becomes
                // `AnalysisException` (both subclass `RuntimeError`). The raw engine diagnostic is
                // preserved in `str(exc)`.
                .map_err(crate::datafusion_to_py_err)?;
            let expr = strip_outer_alias(extract_projection_expr(&plan)?);
            // One handoff cast survives analysis: the Arrow FFI export mishandles `Utf8View`
            // (empty string cells), and DataFusion string built-ins can still type `Utf8View`
            // post-analysis. Numeric types need no pinning — the analyzed expression already
            // carries its true (e.g. Float64) type.
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
            // Cast the `concat` result to `Utf8`: DataFusion's `concat` returns `Utf8View`, but the
            // NULL guard arm below must declare the *same* type or the planner promises `Utf8` and
            // the kernel returns `Utf8View` (a type-mismatch that panics in the array path). Spark's
            // `concat` yields `StringType` (→ `Utf8`, not `Utf8View`) anyway, so this also pins the
            // parity output type.
            let concatenated = Expr::Cast(Cast::new(
                Box::new(datafusion::functions::expr_fn::concat(exprs.clone())),
                DataType::Utf8,
            ));
            // Disjunction of `IS NULL` over every argument; `None` only when `concat()` was called
            // with no arguments. KNOWN DIVERGENCE: DataFusion rejects zero-arg `concat` at plan
            // time, where Spark's `concat()` returns '' — fail-loud on a no-real-caller path
            // (tracked in task/todo.md).
            let any_null = exprs.into_iter().map(Expr::is_null).reduce(Expr::or);
            Ok(match any_null {
                None => Self::from_expr(concatenated),
                Some(any_null) => Self::from_expr(Expr::Case(Case::new(
                    None,
                    // A typed `Utf8` NULL keeps both CASE arms the same type as the cast above.
                    vec![(Box::new(any_null), Box::new(lit(ScalarValue::Utf8(None))))],
                    Some(Box::new(concatenated)),
                ))),
            })
        })
    }

    /// The statement's current timestamp (PySpark `current_timestamp()`).
    ///
    /// DataFusion's `now()` is `timestamp[ns, tz=UTC]`. Spark's `current_timestamp()` is
    /// microsecond precision with UTC (`timestamp[us, tz=UTC]` on the Arrow path). Iceberg v2
    /// also rejects nanosecond timestamps (`timestamp_ns is not supported until v3`), so the
    /// binding casts to microsecond UTC here — recorded from live PySpark 4.1.2
    /// (`spark.range(1).select(F.current_timestamp()).toArrow().schema`).
    #[staticmethod]
    pub fn current_timestamp() -> PyResult<Self> {
        fenced!("Column.current_timestamp", {
            let now = datafusion::functions::expr_fn::now();
            // Spark Arrow: timestamp[us, tz=UTC]. Match precision AND timezone.
            let spark_timestamp =
                DataType::Timestamp(TIMESTAMP_UNIT, Some(std::sync::Arc::<str>::from("UTC")));
            Ok(Self::from_expr(Expr::Cast(Cast::new(
                Box::new(now),
                spark_timestamp,
            ))))
        })
    }

    /// Call a DataFusion scalar function by name (R-FN-BATCH1 facade wrappers).
    ///
    /// Arguments are already-built [`PyColumn`]s (column refs or literals). Unknown names fail loud.
    #[staticmethod]
    #[allow(clippy::too_many_lines)] // large match table of expr_fn bindings
    pub fn call_scalar(name: &str, args: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("Column.call_scalar", {
            use datafusion::functions::expr_fn;
            use datafusion::functions_nested::expr_fn as nested_fn;
            let exprs: Vec<Expr> = args.iter().map(PyColumn::expr).collect();
            let need = |n: usize| -> PyResult<()> {
                if exprs.len() != n {
                    return Err(PyValueError::new_err(format!(
                        "call_scalar({name}) expects {n} args, got {}",
                        exprs.len()
                    )));
                }
                Ok(())
            };
            let need_at_least = |n: usize| -> PyResult<()> {
                if exprs.len() < n {
                    return Err(PyValueError::new_err(format!(
                        "call_scalar({name}) expects at least {n} args, got {}",
                        exprs.len()
                    )));
                }
                Ok(())
            };
            let expr = match name {
                "lower" => {
                    need(1)?;
                    expr_fn::lower(exprs[0].clone())
                }
                "upper" => {
                    need(1)?;
                    expr_fn::upper(exprs[0].clone())
                }
                "length" | "character_length" => {
                    need(1)?;
                    expr_fn::length(exprs[0].clone())
                }
                "trim" | "btrim" => {
                    need_at_least(1)?;
                    expr_fn::trim(exprs.clone())
                }
                "ltrim" => {
                    need_at_least(1)?;
                    expr_fn::ltrim(exprs.clone())
                }
                "rtrim" => {
                    need_at_least(1)?;
                    expr_fn::rtrim(exprs.clone())
                }
                "initcap" => {
                    need(1)?;
                    expr_fn::initcap(exprs[0].clone())
                }
                "lpad" => {
                    need_at_least(2)?;
                    expr_fn::lpad(exprs.clone())
                }
                "rpad" => {
                    need_at_least(2)?;
                    expr_fn::rpad(exprs.clone())
                }
                "instr" | "strpos" => {
                    need(2)?;
                    expr_fn::instr(exprs[0].clone(), exprs[1].clone())
                }
                "concat_ws" => {
                    need_at_least(1)?;
                    let delimiter = exprs[0].clone();
                    let rest = exprs[1..].to_vec();
                    expr_fn::concat_ws(delimiter, rest)
                }
                "regexp_replace" => {
                    need_at_least(3)?;
                    // Spark `regexp_replace` is always global (all matches). DataFusion defaults
                    // to first-match only unless flags include `g` (F2 / Apache test_regexp_replace).
                    // Optional 4th arg is an explicit flags Column (advanced); default is `"g"`.
                    let flags = if exprs.len() >= 4 {
                        Some(exprs[3].clone())
                    } else {
                        Some(lit("g"))
                    };
                    expr_fn::regexp_replace(
                        exprs[0].clone(),
                        exprs[1].clone(),
                        exprs[2].clone(),
                        flags,
                    )
                }
                "sqrt" => {
                    need(1)?;
                    expr_fn::sqrt(exprs[0].clone())
                }
                "floor" => {
                    need(1)?;
                    expr_fn::floor(exprs[0].clone())
                }
                "ceil" | "ceiling" => {
                    need(1)?;
                    expr_fn::ceil(exprs[0].clone())
                }
                "signum" | "sign" => {
                    need(1)?;
                    expr_fn::signum(exprs[0].clone())
                }
                "exp" => {
                    need(1)?;
                    expr_fn::exp(exprs[0].clone())
                }
                "pow" | "power" => {
                    need(2)?;
                    expr_fn::power(exprs[0].clone(), exprs[1].clone())
                }
                // ---- X1 census: trig / hyperbolic / inverse (DataFusion math expr_fn) ----------
                "cos" => {
                    need(1)?;
                    expr_fn::cos(exprs[0].clone())
                }
                "sin" => {
                    need(1)?;
                    expr_fn::sin(exprs[0].clone())
                }
                "tan" => {
                    need(1)?;
                    expr_fn::tan(exprs[0].clone())
                }
                "cosh" => {
                    need(1)?;
                    expr_fn::cosh(exprs[0].clone())
                }
                "sinh" => {
                    need(1)?;
                    expr_fn::sinh(exprs[0].clone())
                }
                "tanh" => {
                    need(1)?;
                    expr_fn::tanh(exprs[0].clone())
                }
                "acos" => {
                    need(1)?;
                    expr_fn::acos(exprs[0].clone())
                }
                "asin" => {
                    need(1)?;
                    expr_fn::asin(exprs[0].clone())
                }
                "atan" => {
                    need(1)?;
                    expr_fn::atan(exprs[0].clone())
                }
                "atan2" => {
                    need(2)?;
                    expr_fn::atan2(exprs[0].clone(), exprs[1].clone())
                }
                "acosh" => {
                    need(1)?;
                    expr_fn::acosh(exprs[0].clone())
                }
                "asinh" => {
                    need(1)?;
                    expr_fn::asinh(exprs[0].clone())
                }
                "atanh" => {
                    need(1)?;
                    expr_fn::atanh(exprs[0].clone())
                }
                "cot" => {
                    need(1)?;
                    // DF cot already yields ±Inf at tan=0; bare 1/tan would hit SparkExprSemantics
                    // nullif-zero → NULL (F2 / Apache test_reciprocal_trig_functions).
                    expr_fn::cot(exprs[0].clone())
                }
                // Spark sec/csc = 1/cos, 1/sin. Bare `/` is rewritten by SparkExprSemantics to
                // `nullif(divisor, 0)` (non-ANSI div-by-zero → NULL). Live Spark 4.1.2 still
                // returns ±Inf from F.sec/F.csc at exact zeros (csc(0)=Inf) — special-case via
                // CASE so the reciprocal trig surface matches without loosening the global
                // div-by-zero rule (F2 FAIL-VALUE harvest).
                "sec" => {
                    need(1)?;
                    reciprocal_trig_or_inf(expr_fn::cos(exprs[0].clone()))
                }
                "csc" => {
                    need(1)?;
                    reciprocal_trig_or_inf(expr_fn::sin(exprs[0].clone()))
                }
                "hypot" => {
                    need(2)?;
                    let xx = expr_fn::power(exprs[0].clone(), lit(2i64));
                    let yy = expr_fn::power(exprs[1].clone(), lit(2i64));
                    expr_fn::sqrt(xx + yy)
                }
                // Bitwise (Spark Column.bitwiseAND / | / ^) via DF Operator.
                "bitwise_and" | "bit_and_scalar" => {
                    need(2)?;
                    datafusion::logical_expr::binary_expr(
                        exprs[0].clone(),
                        datafusion::logical_expr::Operator::BitwiseAnd,
                        exprs[1].clone(),
                    )
                }
                "bitwise_or" | "bit_or_scalar" => {
                    need(2)?;
                    datafusion::logical_expr::binary_expr(
                        exprs[0].clone(),
                        datafusion::logical_expr::Operator::BitwiseOr,
                        exprs[1].clone(),
                    )
                }
                "bitwise_xor" | "bit_xor_scalar" => {
                    need(2)?;
                    datafusion::logical_expr::binary_expr(
                        exprs[0].clone(),
                        datafusion::logical_expr::Operator::BitwiseXor,
                        exprs[1].clone(),
                    )
                }
                "is_not_distinct_from" | "eqnullsafe" | "eq_null_safe" => {
                    need(2)?;
                    datafusion::logical_expr::binary_expr(
                        exprs[0].clone(),
                        datafusion::logical_expr::Operator::IsNotDistinctFrom,
                        exprs[1].clone(),
                    )
                }
                "like" => {
                    need(2)?;
                    exprs[0].clone().like(exprs[1].clone())
                }
                "ilike" => {
                    need(2)?;
                    exprs[0].clone().ilike(exprs[1].clone())
                }
                "rlike" | "regexp_like" => {
                    need(2)?;
                    expr_fn::regexp_like(exprs[0].clone(), exprs[1].clone(), None)
                }
                "round" => {
                    need_at_least(1)?;
                    if exprs.len() == 1 {
                        expr_fn::round(vec![exprs[0].clone()])
                    } else {
                        expr_fn::round(vec![exprs[0].clone(), exprs[1].clone()])
                    }
                }
                // Spark `log` is natural log (ln). Keep `log10` as base-10 for callers who ask.
                "log" | "ln" => {
                    need(1)?;
                    expr_fn::ln(exprs[0].clone())
                }
                "log10" => {
                    need(1)?;
                    expr_fn::log10(exprs[0].clone())
                }
                "md5" => {
                    need(1)?;
                    expr_fn::md5(exprs[0].clone())
                }
                "isnan" => {
                    need(1)?;
                    expr_fn::isnan(exprs[0].clone())
                }
                "nanvl" => {
                    need(2)?;
                    expr_fn::nanvl(exprs[0].clone(), exprs[1].clone())
                }
                "greatest" => {
                    need_at_least(1)?;
                    expr_fn::greatest(exprs.clone())
                }
                "least" => {
                    need_at_least(1)?;
                    expr_fn::least(exprs.clone())
                }
                "current_date" => {
                    need(0)?;
                    expr_fn::current_date()
                }
                "to_date" => {
                    need(1)?;
                    repark_functions::expr_fn::to_date(exprs[0].clone())
                }
                "to_timestamp" => {
                    need_at_least(1)?;
                    expr_fn::to_timestamp(exprs.clone())
                }
                "from_unixtime" => {
                    // Spark returns a STRING formatted timestamp, not a timestamp type.
                    need(1)?;
                    let ts = expr_fn::to_timestamp_seconds(vec![exprs[0].clone()]);
                    expr_fn::to_char(ts, lit("%Y-%m-%d %H:%M:%S"))
                }
                // ---- R-FN-BATCH2: strings / collections (engine expr_fn lowerings) ---------------
                "reverse" => {
                    need(1)?;
                    expr_fn::reverse(exprs[0].clone())
                }
                // === r24 SB1: cardinality ceilings ===
                "repeat" => {
                    need(2)?;
                    repark_functions::cardinality::refuse_facade_literal_expansion(
                        "repeat", &exprs,
                    )
                    .map_err(crate::datafusion_to_py_err)?;
                    expr_fn::repeat(exprs[0].clone(), exprs[1].clone())
                }
                "translate" => {
                    need(3)?;
                    expr_fn::translate(exprs[0].clone(), exprs[1].clone(), exprs[2].clone())
                }
                "substring_index" | "substr_index" => {
                    need(3)?;
                    expr_fn::substr_index(exprs[0].clone(), exprs[1].clone(), exprs[2].clone())
                }
                "levenshtein" => {
                    need(2)?;
                    expr_fn::levenshtein(exprs[0].clone(), exprs[1].clone())
                }
                "ascii" => {
                    need(1)?;
                    expr_fn::ascii(exprs[0].clone())
                }
                "chr" => {
                    need(1)?;
                    expr_fn::chr(exprs[0].clone())
                }
                "overlay" => {
                    // DF overlay(str, replace, pos [, len]) — 3 or 4 args.
                    // Spark default len=-1 means "use length of replace" (same as the
                    // 3-arg form). DataFusion treats len=-1 as "replace to end of string",
                    // so drop a literal -1 4th arg (F2 octo C1-Q-002 / SQL overlay).
                    need_at_least(3)?;
                    let mut overlay_args = exprs.clone();
                    if overlay_args.len() >= 4 {
                        let is_spark_default_len = matches!(
                            &overlay_args[3],
                            Expr::Literal(
                                ScalarValue::Int64(Some(-1))
                                    | ScalarValue::Int32(Some(-1))
                                    | ScalarValue::Int8(Some(-1))
                                    | ScalarValue::Int16(Some(-1)),
                                _
                            )
                        );
                        if is_spark_default_len {
                            overlay_args.truncate(3);
                        }
                    }
                    expr_fn::overlay(overlay_args)
                }
                "find_in_set" => {
                    need(2)?;
                    expr_fn::find_in_set(exprs[0].clone(), exprs[1].clone())
                }
                "locate" | "position" => {
                    // Spark locate(substr, str) / position(substr IN str) → DF strpos(str, substr).
                    need(2)?;
                    expr_fn::strpos(exprs[1].clone(), exprs[0].clone())
                }
                "encode" => {
                    need(2)?;
                    expr_fn::encode(exprs[0].clone(), exprs[1].clone())
                }
                "decode" => {
                    need(2)?;
                    expr_fn::decode(exprs[0].clone(), exprs[1].clone())
                }
                "base64" => {
                    need(1)?;
                    expr_fn::encode(exprs[0].clone(), lit("base64"))
                }
                "unbase64" => {
                    need(1)?;
                    expr_fn::decode(exprs[0].clone(), lit("base64"))
                }
                "size" | "cardinality" => {
                    need(1)?;
                    nested_fn::cardinality(exprs[0].clone())
                }
                "array_distinct" => {
                    need(1)?;
                    nested_fn::array_distinct(exprs[0].clone())
                }
                "array_except" => {
                    need(2)?;
                    nested_fn::array_except(exprs[0].clone(), exprs[1].clone())
                }
                "array_intersect" => {
                    need(2)?;
                    nested_fn::array_intersect(exprs[0].clone(), exprs[1].clone())
                }
                "array_union" => {
                    need(2)?;
                    nested_fn::array_union(exprs[0].clone(), exprs[1].clone())
                }
                "array_join" | "array_to_string" => {
                    // Spark array_join(arr, delim [, null_rep]); DF array_to_string is 2-arg.
                    need_at_least(2)?;
                    nested_fn::array_to_string(exprs[0].clone(), exprs[1].clone())
                }
                "array_max" => {
                    need(1)?;
                    nested_fn::array_max(exprs[0].clone())
                }
                "array_min" => {
                    need(1)?;
                    nested_fn::array_min(exprs[0].clone())
                }
                "array_position" => {
                    // DF array_position(array, element, index) — index defaults to 1 (Spark).
                    need_at_least(2)?;
                    let index = if exprs.len() >= 3 {
                        exprs[2].clone()
                    } else {
                        lit(1i64)
                    };
                    nested_fn::array_position(exprs[0].clone(), exprs[1].clone(), index)
                }
                // r21 T7 census-r6: Spark array_contains → DF array_has
                "array_contains" | "array_has" => {
                    need(2)?;
                    nested_fn::array_has(exprs[0].clone(), exprs[1].clone())
                }
                "array_remove" => {
                    need(2)?;
                    nested_fn::array_remove(exprs[0].clone(), exprs[1].clone())
                }
                // === r24 SB1: cardinality ceilings ===
                "array_repeat" => {
                    need(2)?;
                    repark_functions::cardinality::refuse_facade_literal_expansion(
                        "array_repeat",
                        &exprs,
                    )
                    .map_err(crate::datafusion_to_py_err)?;
                    nested_fn::array_repeat(exprs[0].clone(), exprs[1].clone())
                }
                "array_sort" | "sort_array" => {
                    // DF array_sort(array [, order: 'ASC'|'DESC' [, nulls: 'NULLS FIRST'|…]]).
                    // Spark sort_array(arr [, asc: bool]). Default ascending uses 1-arg form.
                    need_at_least(1)?;
                    if exprs.len() == 1 {
                        nested_fn::array_sort(exprs[0].clone(), lit("ASC"), lit("NULLS FIRST"))
                    } else if exprs.len() == 2 {
                        // Second arg from Python may already be ASC/DESC lit.
                        nested_fn::array_sort(
                            exprs[0].clone(),
                            exprs[1].clone(),
                            lit("NULLS FIRST"),
                        )
                    } else {
                        nested_fn::array_sort(exprs[0].clone(), exprs[1].clone(), exprs[2].clone())
                    }
                }
                "array_slice" => {
                    // DF array_slice(arr, begin, end) — end inclusive 1-indexed.
                    need_at_least(3)?;
                    nested_fn::array_slice(
                        exprs[0].clone(),
                        exprs[1].clone(),
                        exprs[2].clone(),
                        None,
                    )
                }
                "slice" => {
                    // Spark slice(arr, start, length) → DF array_slice(arr, start, start+length-1).
                    need(3)?;
                    let end = exprs[1].clone() + exprs[2].clone() - lit(1i64);
                    nested_fn::array_slice(exprs[0].clone(), exprs[1].clone(), end, None)
                }
                "flatten" => {
                    need(1)?;
                    nested_fn::flatten(exprs[0].clone())
                }
                "map_keys" => {
                    need(1)?;
                    nested_fn::map_keys(exprs[0].clone())
                }
                "map_values" => {
                    need(1)?;
                    nested_fn::map_values(exprs[0].clone())
                }
                "map_entries" => {
                    need(1)?;
                    nested_fn::map_entries(exprs[0].clone())
                }
                // === r24 SB1: cardinality ceilings ===
                "sequence" | "generate_series" | "gen_series" => {
                    // Spark sequence(start, stop [, step]); DF gen_series(start, stop, step).
                    need_at_least(2)?;
                    let step = if exprs.len() >= 3 {
                        exprs[2].clone()
                    } else {
                        lit(1i64)
                    };
                    let check_args = vec![exprs[0].clone(), exprs[1].clone(), step.clone()];
                    repark_functions::cardinality::refuse_facade_literal_expansion(
                        "sequence",
                        &check_args,
                    )
                    .map_err(crate::datafusion_to_py_err)?;
                    nested_fn::gen_series(exprs[0].clone(), exprs[1].clone(), step)
                }
                "elt" => {
                    // Spark elt(n, e1, e2, ...) — 1-indexed pick; DF array_element is 0-indexed.
                    need_at_least(2)?;
                    let index = exprs[0].clone() - lit(1i64);
                    let elements = nested_fn::make_array(exprs[1..].to_vec());
                    nested_fn::array_element(elements, index)
                }
                // Spark ``Column[i]`` (0-based) via owned ``__repark_array_get__`` (octo C1-L-001).
                // Not DF's 1-based ``array_element`` — avoids array_slice 1-element-list residual.
                "array_element" => {
                    need(2)?;
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        repark_functions::collection::spark_array_get_udf(),
                        vec![exprs[0].clone(), exprs[1].clone()],
                    ))
                }
                // Polymorphic Spark GetItem: array 0-based **or** map-by-key (octo C2-L-001).
                // Used for Column / non-int/non-str keys so getitem never fails open to parent.
                "getitem" => {
                    need(2)?;
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        repark_functions::collection::spark_get_item_udf(),
                        vec![exprs[0].clone(), exprs[1].clone()],
                    ))
                }
                // Struct field extract for ``Column['field']`` (octo C1-L-002); free-SQL still
                // quotes the ident on the Python side (octo C1-SEC-001).
                "get_field" => {
                    need(2)?;
                    datafusion::functions::core::get_field().call(exprs.clone())
                }
                // Spark ``array(e1, e2, …)`` / ``lit([…])`` — zero-arg is empty array.
                "array" | "make_array" => nested_fn::make_array(exprs.clone()),
                // ---- R-FN-BATCH3: datetime / extract lowerings --------------------------------
                "next_day" => {
                    need(2)?;
                    repark_functions::expr_fn::next_day(exprs[0].clone(), exprs[1].clone())
                }
                "hour" => {
                    need(1)?;
                    repark_functions::expr_fn::hour(exprs[0].clone())
                }
                "minute" => {
                    need(1)?;
                    repark_functions::expr_fn::minute(exprs[0].clone())
                }
                "second" => {
                    need(1)?;
                    repark_functions::expr_fn::second(exprs[0].clone())
                }
                "date_part" | "datepart" => {
                    need(2)?;
                    // Spark date_part(field, source); DF same order.
                    expr_fn::date_part(exprs[0].clone(), exprs[1].clone())
                }
                "timestamp_seconds" | "to_timestamp_seconds" => {
                    need(1)?;
                    expr_fn::to_timestamp_seconds(vec![exprs[0].clone()])
                }
                "timestamp_millis" | "to_timestamp_millis" => {
                    need(1)?;
                    expr_fn::to_timestamp_millis(vec![exprs[0].clone()])
                }
                "timestamp_micros" | "to_timestamp_micros" => {
                    need(1)?;
                    expr_fn::to_timestamp_micros(vec![exprs[0].clone()])
                }
                "sha256" => {
                    need(1)?;
                    expr_fn::sha256(exprs[0].clone())
                }
                // r20 G2: Spark XORShift `rand`/`randn`/`random` (seeded; overwrites DF random).
                "random" | "rand" => {
                    if exprs.len() > 1 {
                        return Err(PyValueError::new_err(format!(
                            "call_scalar({name}) expects 0 or 1 seed arg, got {}",
                            exprs.len()
                        )));
                    }
                    repark_functions::random::spark_rand_udf().call(exprs.clone())
                }
                "randn" => {
                    if exprs.len() > 1 {
                        return Err(PyValueError::new_err(format!(
                            "call_scalar(randn) expects 0 or 1 seed arg, got {}",
                            exprs.len()
                        )));
                    }
                    repark_functions::random::spark_randn_udf().call(exprs.clone())
                }
                // R-POLARS-NS: string predicates / substr (plan path, no SQL string concat)
                "starts_with" => {
                    need(2)?;
                    expr_fn::starts_with(exprs[0].clone(), exprs[1].clone())
                }
                "ends_with" => {
                    need(2)?;
                    expr_fn::ends_with(exprs[0].clone(), exprs[1].clone())
                }
                "contains" => {
                    need(2)?;
                    expr_fn::contains(exprs[0].clone(), exprs[1].clone())
                }
                "substr" | "substring" => {
                    // Embed owned Spark `substring` UDF (pos 0 ≡ 1, negative from end) —
                    // never DF built-in `expr_fn::substr`/`substring`. Analyzer rewrites
                    // only planner-embedded name=="substr"; call_scalar 3-arg used to
                    // embed DF `substring` (name ≠ "substr") so Column.__getitem__ slices
                    // and F.substr bypassed the shim (octo C7-L-001: 'hello'[0:3] → 'he').
                    need_at_least(2)?;
                    if exprs.len() > 3 {
                        return Err(PyValueError::new_err(format!(
                            "call_scalar({name}) expects 2 or 3 args, got {}",
                            exprs.len()
                        )));
                    }
                    let args = if exprs.len() == 2 {
                        vec![exprs[0].clone(), exprs[1].clone()]
                    } else {
                        vec![exprs[0].clone(), exprs[1].clone(), exprs[2].clone()]
                    };
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        repark_functions::string::substring_udf(),
                        args,
                    ))
                }
                other => {
                    return Err(PyValueError::new_err(format!(
                        "call_scalar: unsupported function {other:?}"
                    )));
                }
            };
            Ok(Self::from_expr(expr))
        })
    }

    // ---- Spark date functions (wired to `repark_functions::expr_fn`) -----------------------------

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
    /// PySpark's `row_number()` is `IntegerType`, but DataFusion's is `UInt64`; the window is wrapped
    /// in a `CAST(… AS INT)` so the output type matches Spark. [`PyColumn::over`] unwraps that cast to
    /// re-window the inner function, then re-applies it.
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
    /// expression: the series column(s) then the scalar literal params, in `args` order. The
    /// `repark.ta` facade builds these; [`PyColumn::over`] then attaches the `ORDER BY` / partition.
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

    /// Apply an `OVER (PARTITION BY … ORDER BY … [frame])` window (PySpark `Column.over`).
    ///
    /// Accepts pure window functions (`row_number`/`rank`/…, optionally CAST-wrapped for Spark
    /// `IntegerType`) **and** aggregate expressions (`sum`/`max`/…), which become window aggregates.
    ///
    /// Frame (r20 G2): when `frame_units` is `"rows"` or `"range"`, `frame_start`/`frame_end` are
    /// Spark-relative offsets (`i64::MIN`/`i64::MAX` = unbounded; `0` = current row; negative =
    /// preceding; positive = following). `None` frame keeps DataFusion's default frame for the
    /// order-by presence.
    ///
    /// # Errors
    /// Returns `ValueError` if this column is not a window/aggregate function, if the order vectors
    /// differ in length, or if the windowed expression cannot be built.
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
            // Window UDF / CAST(window) / Aggregate → WindowFunction.
            let (window_expr, cast_type) = match &self.expr {
                Expr::WindowFunction(_) => (self.expr.clone(), None),
                Expr::Cast(cast) if matches!(*cast.expr, Expr::WindowFunction(_)) => {
                    ((*cast.expr).clone(), Some(cast.field.data_type().clone()))
                }
                Expr::AggregateFunction(agg) => {
                    let window = Expr::from(WindowFunction::new(
                        WindowFunctionDefinition::AggregateUDF(std::sync::Arc::clone(&agg.func)),
                        agg.params.args.clone(),
                    ));
                    (window, None)
                }
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
    /// (r24 QUAL-03 — was a bare `ValueError`).
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

    // ---- aggregate functions (Group E: `GroupedData.agg` + the `F.sum`-family) -------------------

    /// The column's schema display name (`col("x")` → `"x"`) — the facade reads this to compute a
    /// PySpark aggregate output name (`sum(x)`) when the argument is a `Column` rather than a plain
    /// column-name string.
    pub fn display_name(&self) -> PyResult<String> {
        fenced!("Column.display_name", {
            Ok(self.expr.schema_name().to_string())
        })
    }

    /// Collapse nested ``Alias`` chains on the held [`Expr`] to one outer rename (r25 T3 / N2).
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

    /// Build a Spark aggregate over this column for `GroupedData.agg` and the `F.sum`-family
    /// functions.
    ///
    /// `kind` selects the reducer (`"sum"`/`"avg"`/`"min"`/`"max"`/`"first"`/`"last"`/
    /// `"collect_list"`/`"collect_set"`); `ignore_nulls` sets `IGNORE NULLS` for `first`/`last`
    /// (PySpark's `ignorenulls=True`). All of these skip NULLs in the reduction (Spark parity);
    /// `count` is built by [`PyColumn::count_aggregate`] instead (it carries the `*` / `DISTINCT` /
    /// multi-argument forms). The output column name is applied facade-side via `alias`, so the
    /// returned expression is deliberately un-aliased.
    ///
    /// `collect_list` / `collect_set` route through DataFusion `array_agg` (with `DISTINCT` for the
    /// set form). Both force `IGNORE NULLS` — Spark excludes NULL elements — and `coalesce` the
    /// result with `make_array()` so an empty group is an empty array (not NULL), matching live
    /// PySpark 4.1.2. Element order is nondeterministic (Spark parity); callers must compare sorted
    /// contents or use single-element groups.
    ///
    /// # Errors
    /// Returns `ValueError` for an unknown `kind`, or if the aggregate builder fails.
    pub fn aggregate(&self, kind: &str, ignore_nulls: bool) -> PyResult<Self> {
        fenced!("Column.aggregate", {
            if kind == "collect_list" || kind == "collect_set" {
                return Self::collect_aggregate(self.expr.clone(), kind == "collect_set");
            }
            let udaf = match kind {
                "sum" => sum_udaf(),
                "avg" => avg_udaf(),
                "min" => min_udaf(),
                "max" => max_udaf(),
                "first" => first_value_udaf(),
                "last" => last_value_udaf(),
                // R-FN-BATCH4 unary stats / bits
                "stddev" | "stddev_samp" => stddev_udaf(),
                "stddev_pop" => stddev_pop_udaf(),
                "variance" | "var_samp" => var_samp_udaf(),
                "var_pop" => var_pop_udaf(),
                "median" => median_udaf(),
                "bit_and" => bit_and_udaf(),
                "bit_or" => bit_or_udaf(),
                "bit_xor" => bit_xor_udaf(),
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown aggregate function {other:?}"
                    )));
                }
            };
            let base = udaf.call(vec![self.expr.clone()]);
            // A plain `call` is already a usable aggregate `Expr`; only IGNORE NULLS needs the
            // builder chain (`ExprFunctionExt` on `Expr` → `ExprFuncBuilder` → `build`).
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
            Ok(Self::from_expr(expr))
        })
    }

    /// Binary aggregate: `corr` / `covar_pop` / `covar_samp` (R-FN-BATCH4).
    pub fn aggregate_binary(&self, kind: &str, other: PyColumn) -> PyResult<Self> {
        fenced!("Column.aggregate_binary", {
            let udaf = match kind {
                "corr" => corr_udaf(),
                "covar_pop" => covar_pop_udaf(),
                "covar_samp" | "covar" => covar_samp_udaf(),
                other_kind => {
                    return Err(PyValueError::new_err(format!(
                        "unknown binary aggregate {other_kind:?}"
                    )));
                }
            };
            let expr = udaf.call(vec![self.expr.clone(), other.expr.clone()]);
            Ok(Self::from_expr(expr))
        })
    }

    /// Approximate continuous percentile (Q1 / R-ML-QUANTILE).
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
    /// row); several columns with `distinct = true` is `count(DISTINCT a, b, …)` (distinct tuples).
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

impl PyColumn {
    /// ===========================================================================================
    /// `array_agg` (+ optional `DISTINCT`) with Spark `collect_list` / `collect_set` NULL / empty
    /// semantics: exclude NULL elements; empty group → empty array (not NULL).
    /// ===========================================================================================
    fn collect_aggregate(argument: Expr, distinct: bool) -> PyResult<Self> {
        let base = array_agg_udaf().call(vec![argument]);
        // FORCE IgnoreNulls: Spark `collect_list`/`collect_set` drop NULL elements (oracle-verified).
        // `DISTINCT` only for the set form. Builder chain order is free (fields land on one struct).
        let aggregated = if distinct {
            base.distinct()
                .null_treatment(NullTreatment::IgnoreNulls)
                .build()
        } else {
            base.null_treatment(NullTreatment::IgnoreNulls).build()
        }
        .map_err(|err| {
            PyValueError::new_err(format!(
                "could not build collect aggregate expression: {err}"
            ))
        })?;
        // DataFusion's empty `array_agg` evaluates to a NULL list; Spark returns `[]`. Coalesce with
        // zero-arg `make_array()` restores the empty-array value (and keeps the list value type —
        // verified against DataFusion 52.5: `coalesce(array_agg(int), make_array())` → `List(Int64)`).
        let empty = datafusion::functions_nested::expr_fn::make_array(vec![]);
        let expr = datafusion::functions::expr_fn::coalesce(vec![aggregated, empty]);
        Ok(Self::from_expr(expr))
    }

    /// ===========================================================================================
    /// Single-column `count(DISTINCT x)` passes through; multi-column packs into a null-if-any
    /// `struct` so DataFusion's single-arg `COUNT DISTINCT` matches Spark tuple semantics.
    /// ===========================================================================================
    fn count_distinct_argument(args: Vec<Expr>) -> PyResult<Expr> {
        if args.len() == 1 {
            return args.into_iter().next().ok_or_else(|| {
                PyValueError::new_err("count(DISTINCT …) requires at least one argument column")
            });
        }
        // Pack the tuple. A bare `struct(a,b)` would *include* rows with null fields as distinct
        // keys; Spark excludes any row where ANY of the columns is NULL. Null the whole struct
        // when any field is NULL so `count(DISTINCT …)` skips those rows.
        let packed = datafusion::functions::expr_fn::r#struct(args.clone());
        let all_present = args
            .into_iter()
            .map(Expr::is_not_null)
            .reduce(Expr::and)
            .ok_or_else(|| {
                PyValueError::new_err("count(DISTINCT …) requires at least one argument column")
            })?;
        Ok(Expr::Case(Case {
            expr: None,
            when_then_expr: vec![(Box::new(all_present), Box::new(packed))],
            // ELSE NULL: type is inferred from the THEN arm (the struct).
            else_expr: None,
        }))
    }
}

/// Spark's `TimestampType` is microsecond precision; map it to an Arrow microsecond timestamp.
const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

/// ===========================================================================================
/// Parse a canonical engine type string into an Arrow [`DataType`] for `CAST`.
///
/// Vocabulary locksteps with the facade `types` classes that claim a primitive cast mapping and
/// with `column.py` `_spark_cast_type_name` (r24 QUAL-03). Accepted tokens:
/// - width integers: `byte`/`tinyint` → Int8, `short`/`smallint` → Int16, `int`/`integer` → Int32,
///   `long`/`bigint` → Int64
/// - floats: `float` → Float32, `double` → Float64
/// - temporal / other primitives: `string`, `boolean`, `date`, `timestamp`, `binary`
/// - parameterized: `decimal(p,s)` → Decimal128
///
/// Unknown tokens (including `varchar`/`char`/`interval`/`variant` as bare engine tags) return
/// `Err` so the `cast` / `try_cast` boundary can raise [`AnalysisException`].
/// ===========================================================================================
// === r24 A3: parse_data_type cast vocabulary ===
fn parse_data_type(spec: &str) -> Result<DataType, String> {
    match spec.trim() {
        "string" => Ok(DataType::Utf8),
        "byte" | "tinyint" => Ok(DataType::Int8),
        "short" | "smallint" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "long" | "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "boolean" => Ok(DataType::Boolean),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(
            TIMESTAMP_UNIT,
            Some(std::sync::Arc::<str>::from("UTC")),
        )),
        "timestamp_ntz" => Ok(DataType::Timestamp(TIMESTAMP_UNIT, None)),
        "binary" => Ok(DataType::Binary),
        other => parse_decimal_type(other),
    }
}

/// Parse a `decimal(precision,scale)` type string into an Arrow `Decimal128`.
///
/// Precision and scale are parsed as `u8` (Arrow's `Decimal128` widths); anything outside that
/// shape is a descriptive error rather than a silent fallback.
fn parse_decimal_type(spec: &str) -> Result<DataType, String> {
    let inner = spec
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| format!("unknown cast type {spec:?}"))?;
    let (precision_text, scale_text) = inner
        .split_once(',')
        .ok_or_else(|| format!("decimal type needs `decimal(precision,scale)`, got {spec:?}"))?;
    let precision: u8 = precision_text
        .trim()
        .parse()
        .map_err(|_| format!("invalid decimal precision in {spec:?}"))?;
    let scale: i8 = scale_text
        .trim()
        .parse()
        .map_err(|_| format!("invalid decimal scale in {spec:?}"))?;
    Ok(DataType::Decimal128(precision, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical + alias tokens the facade documents for primitive cast (QUAL-03).
    /// Renamed from `parse_data_type_maps_the_seven_spark_types` (rule 11) — the old name
    /// under-claimed the vocabulary after float/byte/short/binary landed.
    #[test]
    fn parse_data_type_maps_facade_primitive_cast_vocabulary() {
        assert_eq!(parse_data_type("string").unwrap(), DataType::Utf8);
        assert_eq!(parse_data_type("byte").unwrap(), DataType::Int8);
        assert_eq!(parse_data_type("tinyint").unwrap(), DataType::Int8);
        assert_eq!(parse_data_type("short").unwrap(), DataType::Int16);
        assert_eq!(parse_data_type("smallint").unwrap(), DataType::Int16);
        assert_eq!(parse_data_type("int").unwrap(), DataType::Int32);
        assert_eq!(parse_data_type("integer").unwrap(), DataType::Int32);
        // `long` / `bigint` (Int64) — the PySpark integer-width spellings; no `types` object emits
        // them, but `Column.cast("long")` and the na-fill width-preserving path both need Int64.
        assert_eq!(parse_data_type("long").unwrap(), DataType::Int64);
        assert_eq!(parse_data_type("bigint").unwrap(), DataType::Int64);
        assert_eq!(parse_data_type("float").unwrap(), DataType::Float32);
        assert_eq!(parse_data_type("double").unwrap(), DataType::Float64);
        assert_eq!(parse_data_type("boolean").unwrap(), DataType::Boolean);
        assert_eq!(parse_data_type("date").unwrap(), DataType::Date32);
        assert_eq!(
            parse_data_type("timestamp").unwrap(),
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(std::sync::Arc::<str>::from("UTC"))
            )
        );
        assert_eq!(
            parse_data_type("timestamp_ntz").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(parse_data_type("binary").unwrap(), DataType::Binary);
        assert_eq!(
            parse_data_type("decimal(10,4)").unwrap(),
            DataType::Decimal128(10, 4)
        );
    }

    #[test]
    fn parse_data_type_rejects_unknown_and_malformed() {
        // Bare varchar/char/interval/variant refuse-loud (Q7) unless types.py claims cast.
        assert!(parse_data_type("varchar").is_err());
        assert!(parse_data_type("char").is_err());
        assert!(parse_data_type("interval").is_err());
        assert!(parse_data_type("variant").is_err());
        assert!(parse_data_type("notatype").is_err());
        assert!(parse_data_type("decimal(10)").is_err());
        assert!(parse_data_type("decimal(x,4)").is_err());
    }

    /// r25 T3: nested Alias chains peel to one outer rename (plan shows one ``AS name``).
    #[test]
    fn collapse_identity_alias_chain_peels_same_name_stack() {
        let stacked = col("close").alias("close").alias("close").alias("close");
        let collapsed = collapse_identity_alias_chain(stacked);
        // Exactly one outer Alias named close over a non-alias leaf.
        match collapsed {
            Expr::Alias(alias) => {
                assert_eq!(alias.name, "close");
                assert!(
                    !matches!(alias.expr.as_ref(), Expr::Alias(_)),
                    "inner must not remain Alias after peel: {:?}",
                    alias.expr
                );
            }
            other => panic!("expected single Alias, got {other:?}"),
        }
        // Same-name rename chain peels to one outer alias.
        let renamed = col("close").alias("c").alias("c");
        match collapse_identity_alias_chain(renamed) {
            Expr::Alias(alias) => {
                assert_eq!(alias.name, "c");
                assert!(!matches!(alias.expr.as_ref(), Expr::Alias(_)));
            }
            other => panic!("expected single Alias rename, got {other:?}"),
        }
        // Distinct intermediate rename also peels (… AS a AS b → … AS b) — octo C1-Q-006.
        let chain = col("close").alias("a").alias("b");
        match collapse_identity_alias_chain(chain) {
            Expr::Alias(alias) => {
                assert_eq!(alias.name, "b");
                assert!(!matches!(alias.expr.as_ref(), Expr::Alias(_)));
            }
            other => panic!("expected single outer Alias b, got {other:?}"),
        }
        // Non-alias expr is a no-op.
        let bare = col("close");
        assert!(matches!(
            collapse_identity_alias_chain(bare.clone()),
            Expr::Column(_)
        ));
        // Idempotent.
        let once = collapse_identity_alias_chain(col("x").alias("x").alias("x"));
        let twice = collapse_identity_alias_chain(once.clone());
        assert_eq!(format!("{once}"), format!("{twice}"));
    }

    /// r25 morning critic pin: the outer Alias's qualifier + Arrow field metadata survive the
    /// peel, and a non-nested Alias round-trips byte-identical (no silent rebuild).
    #[test]
    fn collapse_identity_alias_chain_preserves_qualifier_and_metadata() {
        use datafusion::common::metadata::FieldMetadata;
        use std::collections::HashMap;

        let metadata = FieldMetadata::from(HashMap::from([(
            "repark.origin".to_string(),
            "t3-pin".to_string(),
        )]));
        // Lone qualified+metadata alias is returned unchanged (no rebuild path).
        let lone = col("x").alias_qualified_with_metadata(Some("t"), "y", Some(metadata.clone()));
        assert_eq!(collapse_identity_alias_chain(lone.clone()), lone);
        // Nested stack under a qualified+metadata outer alias: stack peels, identity stays.
        let stacked = col("x")
            .alias("x")
            .alias("x")
            .alias_qualified_with_metadata(Some("t"), "y", Some(metadata.clone()));
        match collapse_identity_alias_chain(stacked) {
            Expr::Alias(alias) => {
                assert_eq!(alias.name, "y");
                assert_eq!(
                    alias.relation.as_ref().map(ToString::to_string).as_deref(),
                    Some("t"),
                    "outer alias qualifier must survive the peel"
                );
                assert_eq!(
                    alias.metadata,
                    Some(metadata),
                    "outer alias field metadata must survive the peel"
                );
                assert!(!matches!(alias.expr.as_ref(), Expr::Alias(_)));
            }
            other => panic!("expected qualified Alias, got {other:?}"),
        }
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

    /// `call_scalar` 2/3-arg `substr` must embed the Spark UDF (octo C7-L-001).
    ///
    /// Pre-fix, the 3-arg arm used DF `expr_fn::substring` (name ≠ `"substr"`), so the
    /// analyzer never rewrote it and `Column.__getitem__` slices / `F.substr` diverged
    /// from SQL (`'hello'` pos0 len3 → `'he'`).
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

    /// The boundary invariant behind the 2026-07-13 F.expr regression: the handoff expression
    /// must carry its POST-analysis type, so the consumer `DataFrame`'s logical schema (what the
    /// PyO3 Arrow export reads) agrees with the executed buffers. Pre-fix, `5/2` handed off as
    /// Int64, executed as Float64, and `collect()` returned 2.5's bit pattern as an int.
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
