//! Expression-construction helpers for [`super::PyColumn`].

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr::{Alias, NullTreatment, WindowFunction};
use datafusion::logical_expr::{
    Case, Expr, ExprFunctionExt, Operator, WindowFunctionDefinition, binary_expr, lit,
};
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::PyColumn;

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

/// Drop one outer alias so a standalone expression can be re-aliased by the facade.
pub(super) fn strip_outer_alias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    }
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

pub(super) fn inner_null_treatment(expr: &Expr) -> Option<NullTreatment> {
    match expr {
        Expr::AggregateFunction(agg) => agg.params.null_treatment,
        Expr::WindowFunction(window) => window.params.null_treatment,
        _ => None,
    }
}

pub(super) fn percentile_approx_list_expr(argument: Expr, percentages: Vec<f64>) -> Expr {
    let values: Vec<ScalarValue> = percentages
        .into_iter()
        .map(|percentage| ScalarValue::Float64(Some(percentage)))
        .collect();
    let list = ScalarValue::List(ScalarValue::new_list_nullable(&values, &DataType::Float64));
    repark_functions::percentile_approx::percentile_approx_udaf().call(vec![argument, lit(list)])
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
             supported. Spark evaluates it; repark cannot, because DataFusion 54.1 fails such a \
             plan — at evaluation for a nested lambda body (through its own SQL planner too), and \
             at lambda-variable resolution for a value argument. Compute the inner result in a \
             separate column first."
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
