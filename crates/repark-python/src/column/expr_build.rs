//! Expression-construction helpers for [`super::PyColumn`].

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, SchemaError, TableReference};
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr::{Alias, Cast, NullTreatment, WindowFunction};
use datafusion::logical_expr::{
    AggregateUDF, Case, Expr, ExprFunctionExt, Operator, WindowFunctionDefinition, binary_expr, lit,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::PyColumn;
use crate::fence::fenced;

/// Preserve Spark's `±Inf` result at an exact zero reciprocal-trig divisor.
pub(super) fn reciprocal_trig_or_inf(divisor: Expr) -> Expr {
    Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(binary_expr(divisor.clone(), Operator::Eq, lit(0.0f64))),
            Box::new(lit(f64::INFINITY)),
        )],
        else_expr: Some(Box::new(lit(1.0f64) / divisor)),
    })
}

pub(crate) fn parse_canonical_predicate(
    frame: &datafusion::prelude::DataFrame,
    predicate: &str,
) -> datafusion::error::Result<Expr> {
    let canonical = repark_spark::spark_literals::canonicalize(predicate)?;
    frame.parse_sql_expr(canonical.as_ref()).map_err(|error| {
        repark_spark::spark_literals::translate_downstream_error(
            predicate,
            canonical.as_ref(),
            error,
        )
    })
}

pub(crate) async fn plan_expr_column(
    context: &SessionContext,
    canonical: &str,
    original: &str,
) -> PyResult<Expr> {
    let rewritten = repark_functions::cast_map::rewrite_map_casts(canonical);
    let canonical = rewritten.as_deref().unwrap_or(canonical);
    let select_sql = format!("SELECT ({canonical}) AS _repark_expr");
    let plan = match context.sql(&select_sql).await {
        Ok(frame) => {
            match repark_functions::analyze_eagerly(&context.state(), frame.logical_plan().clone())
            {
                Ok(analyzed) => analyzed,
                Err(error) => {
                    if missing_column(&error).is_some() {
                        return parse_unresolved_expr(context, canonical)
                            .map_err(|inner| crate::unknown_routine_to_py_err(original, inner));
                    }
                    return Err(crate::unknown_routine_to_py_err(original, error));
                }
            }
        }
        Err(error) => {
            if missing_column(&error).is_some() {
                return parse_unresolved_expr(context, canonical)
                    .map_err(|inner| crate::unknown_routine_to_py_err(original, inner));
            }
            return Err(crate::unknown_routine_to_py_err(original, error));
        }
    };
    let expr = strip_outer_alias(extract_projection_expr(&plan)?);
    Ok(
        match plan
            .schema()
            .fields()
            .first()
            .map(|field| field.data_type().clone())
        {
            Some(DataType::Utf8View) => Expr::Cast(Cast::new(Box::new(expr), DataType::Utf8)),
            _ => expr,
        },
    )
}

fn missing_column(
    error: &datafusion::error::DataFusionError,
) -> Option<(Option<TableReference>, String)> {
    match error {
        datafusion::error::DataFusionError::SchemaError(inner, _) => match inner.as_ref() {
            SchemaError::FieldNotFound { field, .. } => {
                Some((field.relation.clone(), field.name.clone()))
            }
            _ => None,
        },
        datafusion::error::DataFusionError::Diagnostic(_, inner) => missing_column(inner),
        _ => None,
    }
}

fn parse_unresolved_expr(
    context: &SessionContext,
    canonical: &str,
) -> datafusion::error::Result<Expr> {
    let mut qualified: Vec<(Option<TableReference>, Arc<Field>)> = Vec::new();
    loop {
        let schema =
            DFSchema::new_with_metadata(qualified.clone(), HashMap::new()).map_err(|error| {
                datafusion::error::DataFusionError::Plan(format!(
                    "F.expr discovered-column schema failed: {error}"
                ))
            })?;
        match context.state().create_logical_expr(canonical, &schema) {
            Ok(expr) => return Ok(expr),
            Err(error) => {
                let Some((relation, name)) = missing_column(&error) else {
                    return Err(error);
                };
                let next = (relation, Arc::new(Field::new(name, DataType::Utf8, true)));
                if qualified.len() >= 64 || qualified.contains(&next) {
                    return Err(error);
                }
                qualified.push(next);
            }
        }
    }
}

/// Drop one outer alias so a standalone expression can be re-aliased by the facade.
pub(super) fn strip_outer_alias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    }
}

static EXPR_CONTEXT: OnceLock<SessionContext> = OnceLock::new();

pub(super) fn sql_context(
    _sql: &str,
    _normalize_idents: bool,
) -> datafusion::error::Result<SessionContext> {
    if let Some(context) = EXPR_CONTEXT.get() {
        return Ok(context.clone());
    }
    let context = build_expr_context()?;
    let _ = EXPR_CONTEXT.set(context.clone());
    Ok(context)
}

fn build_expr_context() -> datafusion::error::Result<SessionContext> {
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = datafusion::config::Dialect::Databricks;
    config.options_mut().sql_parser.enable_ident_normalization = false;
    let rules = repark_functions::analyzer_rules_with_higher_order_preparation(
        datafusion::optimizer::Analyzer::new().rules,
    )?;
    let mut rules = repark_spark::spark_literal_typing::insert_literal_rule_before_coercion(rules)?;
    rules.push(std::sync::Arc::new(repark_spark::FoldSparkNumericCasts));
    rules.push(std::sync::Arc::new(repark_spark::SparkProjectionDisplay));
    rules.extend(repark_spark::spark_literal_typing::spark_door_post_coercion_rules());
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let context = SessionContext::new_with_state(state);
    context.register_udf(repark_spark::spark_as_udf().as_ref().clone());
    context.register_udf(repark_spark::suffix_literal_udf().as_ref().clone());
    repark_functions::register_all(&context);
    Ok(context)
}

/// Collapse nested `Alias` layers to one outer rename.
pub(super) fn collapse_identity_alias_chain(expr: Expr) -> Expr {
    let Expr::Alias(alias) = expr else {
        return expr;
    };
    // Rebuilding a lone alias would discard DataFusion's qualifier and field metadata.
    if !matches!(alias.expr.as_ref(), Expr::Alias(_)) {
        return Expr::Alias(alias);
    }
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

/// Pull the first projection expression out of an analyzed/optimized plan.
pub(super) fn extract_projection_expr(plan: &LogicalPlan) -> PyResult<Expr> {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .first()
            .cloned()
            .ok_or_else(|| PyValueError::new_err("expr plan produced an empty projection")),
        other => other
            .inputs()
            .iter()
            .find_map(|input| extract_projection_expr(input).ok())
            .ok_or_else(|| {
                PyValueError::new_err(format!("expr plan had no projection to extract: {other}"))
            }),
    }
}

pub(super) fn single_wrapped_aggregate(expr: &Expr) -> Option<Expr> {
    let mut found: Option<Expr> = None;
    let mut seen = 0usize;
    let walked = expr.apply(|node| {
        if matches!(node, Expr::AggregateFunction(_)) {
            seen += 1;
            if found.is_none() {
                found = Some(node.clone());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if walked.is_err() || seen != 1 {
        return None;
    }
    found
}

pub(super) fn replace_wrapped_aggregate(expr: Expr, windowed: &Expr) -> Result<Expr, String> {
    expr.transform(|node| {
        if matches!(node, Expr::AggregateFunction(_)) {
            Ok(Transformed::yes(windowed.clone()))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|transformed| transformed.data)
    .map_err(|error| format!("could not push the window into the aggregate wrapper: {error}"))
}

pub(super) fn inner_null_treatment(expr: &Expr) -> Option<NullTreatment> {
    match expr {
        Expr::AggregateFunction(agg) => agg.params.null_treatment,
        Expr::WindowFunction(window) => window.params.null_treatment,
        _ => None,
    }
}

pub(super) fn percentile_approx_scalar_expr(
    argument: Expr,
    percentile: f64,
    accuracy: Option<i64>,
) -> Expr {
    let udaf = repark_functions::percentile_approx::percentile_approx_udaf();
    match accuracy {
        Some(value) => udaf.call(vec![argument, lit(percentile), lit(value)]),
        None => udaf.call(vec![argument, lit(percentile)]),
    }
}

pub(super) fn percentile_approx_list_expr(
    argument: Expr,
    percentages: Vec<f64>,
    accuracy: Option<i64>,
) -> Expr {
    let values: Vec<ScalarValue> = percentages
        .into_iter()
        .map(|percentage| ScalarValue::Float64(Some(percentage)))
        .collect();
    let list = ScalarValue::List(ScalarValue::new_list_nullable(&values, &DataType::Float64));
    let udaf = repark_functions::percentile_approx::percentile_approx_udaf();
    match accuracy {
        Some(value) => udaf.call(vec![argument, lit(list), lit(value)]),
        None => udaf.call(vec![argument, lit(list)]),
    }
}

pub(super) fn window_from_aggregate(
    agg: &datafusion::logical_expr::expr::AggregateFunction,
) -> Expr {
    let mut window = WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(std::sync::Arc::clone(&agg.func)),
        agg.params.args.clone(),
    );
    window.params.null_treatment = agg.params.null_treatment;
    Expr::from(window)
}

impl PyColumn {
    /// Build Spark `collect_list` / `collect_set` semantics for NULL and empty groups.
    pub(super) fn collect_aggregate(argument: Expr, distinct: bool) -> PyResult<Self> {
        let base = array_agg_udaf().call(vec![argument]);
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
        // DataFusion returns NULL for an empty array_agg; Spark returns an empty array.
        let empty = datafusion::functions_nested::expr_fn::make_array(vec![]);
        let expr = datafusion::functions::expr_fn::coalesce(vec![aggregated, empty]);
        Ok(Self::from_expr(expr))
    }

    pub(super) fn grouping_id_call(args: Vec<Expr>) -> PyResult<Expr> {
        let udaf = super::function_dispatch::nary_aggregate_udaf("grouping_id")?;
        let arity = args.len();
        Ok(cast_unsigned_count_to_signed(&udaf, arity, udaf.call(args)))
    }

    /// Build a single count-distinct argument, nulling multi-column tuples when any field is NULL.
    pub(super) fn count_distinct_argument(args: Vec<Expr>) -> PyResult<Expr> {
        if args.len() == 1 {
            return args.into_iter().next().ok_or_else(|| {
                PyValueError::new_err("count(DISTINCT …) requires at least one argument column")
            });
        }
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
            else_expr: None,
        }))
    }
}

#[pyfunction]
pub(crate) fn grouping_id_column(args: Vec<PyColumn>) -> PyResult<PyColumn> {
    fenced!("grouping_id_column", {
        let exprs = args.iter().map(PyColumn::expr).collect::<Vec<_>>();
        Ok(PyColumn::from_expr(PyColumn::grouping_id_call(exprs)?))
    })
}

pub(super) fn cast_unsigned_count_to_signed(udaf: &AggregateUDF, arity: usize, expr: Expr) -> Expr {
    match udaf.return_type(&vec![DataType::Int64; arity]) {
        Ok(returned) if returned.is_unsigned_integer() => {
            Expr::Cast(Cast::new(Box::new(expr), DataType::Int64))
        }
        _ => expr,
    }
}

/// Spark's `TimestampType` is microsecond precision; map it to an Arrow microsecond timestamp.
pub(super) const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

/// Parse a canonical engine type string into an Arrow [`DataType`] for `CAST`.
pub(super) fn parse_data_type(spec: &str) -> Result<DataType, String> {
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

pub(super) fn cast_to(expr: Expr, spec: &str, try_cast: bool) -> Result<Expr, String> {
    if let Some(target) = repark_functions::cast_map::map_cast_target(spec) {
        return Ok(repark_functions::cast_map::cast_map_expr(
            expr, &target, try_cast,
        ));
    }
    let data_type = parse_data_type(spec)?;
    Ok(if try_cast {
        Expr::TryCast(datafusion::logical_expr::TryCast::new(
            Box::new(expr),
            data_type,
        ))
    } else {
        Expr::Cast(Cast::new(Box::new(expr), data_type))
    })
}

/// Parse a `decimal(precision,scale)` type string into an Arrow `Decimal128`.
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

/// Detect a higher-order function anywhere in an expression tree.
pub(super) fn contains_higher_order(expr: &Expr) -> PyResult<bool> {
    let mut found = false;
    expr.apply(|node| {
        if matches!(node, Expr::HigherOrderFunction(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(crate::datafusion_to_py_err)?;
    Ok(found)
}

/// Refuse nested higher-order functions with `UnsupportedOperationException`.
pub(super) fn refuse_nested_higher_order(
    argument: &Expr,
    name: &str,
    position: &str,
) -> PyResult<()> {
    if contains_higher_order(argument)? {
        return Err(crate::UnsupportedOperationException::new_err(format!(
            "{name}: a higher-order function nested inside another one's {position} is not \
             supported through the Column door yet. The Spark SQL door serves nested lambdas; \
             compute the inner result in a separate column first."
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::col;

    /// Canonical and alias tokens map to the facade's primitive cast types.
    #[test]
    fn parse_data_type_maps_facade_primitive_cast_vocabulary() {
        assert_eq!(parse_data_type("string").unwrap(), DataType::Utf8);
        assert_eq!(parse_data_type("byte").unwrap(), DataType::Int8);
        assert_eq!(parse_data_type("tinyint").unwrap(), DataType::Int8);
        assert_eq!(parse_data_type("short").unwrap(), DataType::Int16);
        assert_eq!(parse_data_type("smallint").unwrap(), DataType::Int16);
        assert_eq!(parse_data_type("int").unwrap(), DataType::Int32);
        assert_eq!(parse_data_type("integer").unwrap(), DataType::Int32);
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
        // Bare varchar, char, interval, and variant require an explicit cast.
        assert!(parse_data_type("varchar").is_err());
        assert!(parse_data_type("char").is_err());
        assert!(parse_data_type("interval").is_err());
        assert!(parse_data_type("variant").is_err());
        assert!(parse_data_type("notatype").is_err());
        assert!(parse_data_type("decimal(10)").is_err());
        assert!(parse_data_type("decimal(x,4)").is_err());
    }

    /// Nested aliases collapse to one outer rename.
    #[test]
    fn collapse_identity_alias_chain_peels_same_name_stack() {
        let stacked = col("close").alias("close").alias("close").alias("close");
        let collapsed = collapse_identity_alias_chain(stacked);
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
        let renamed = col("close").alias("c").alias("c");
        match collapse_identity_alias_chain(renamed) {
            Expr::Alias(alias) => {
                assert_eq!(alias.name, "c");
                assert!(!matches!(alias.expr.as_ref(), Expr::Alias(_)));
            }
            other => panic!("expected single Alias rename, got {other:?}"),
        }
        let chain = col("close").alias("a").alias("b");
        match collapse_identity_alias_chain(chain) {
            Expr::Alias(alias) => {
                assert_eq!(alias.name, "b");
                assert!(!matches!(alias.expr.as_ref(), Expr::Alias(_)));
            }
            other => panic!("expected single outer Alias b, got {other:?}"),
        }
        let bare = col("close");
        assert!(matches!(
            collapse_identity_alias_chain(bare.clone()),
            Expr::Column(_)
        ));
        let once = collapse_identity_alias_chain(col("x").alias("x").alias("x"));
        let twice = collapse_identity_alias_chain(once.clone());
        assert_eq!(format!("{once}"), format!("{twice}"));
    }

    /// Alias qualification and field metadata survive nested-alias collapse.
    #[test]
    fn collapse_identity_alias_chain_preserves_qualifier_and_metadata() {
        use datafusion::common::metadata::FieldMetadata;
        use std::collections::HashMap;

        let metadata = FieldMetadata::from(HashMap::from([(
            "repark.origin".to_string(),
            "t3-pin".to_string(),
        )]));
        let lone = col("x").alias_qualified_with_metadata(Some("t"), "y", Some(metadata.clone()));
        assert_eq!(collapse_identity_alias_chain(lone.clone()), lone);
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
