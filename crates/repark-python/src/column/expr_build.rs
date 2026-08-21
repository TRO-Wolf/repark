//! Expression-construction helpers for [`super::PyColumn`].
//!
//! Cast vocabulary, alias-chain collapse, projection extract, reciprocal-trig Inf CASE,
//! and the collect/count-distinct builders. Not `#[pymethods]` — the Python surface stays
//! in `mod.rs`.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr::{Alias, NullTreatment};
use datafusion::logical_expr::{Case, Expr, ExprFunctionExt, Operator, binary_expr, lit};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::PyColumn;

/// ===========================================================================================
/// Drop a single outer `AS name` alias so `F.expr(...).alias("x")` can re-alias cleanly.
/// ===========================================================================================
/// Spark `sec`/`csc` at exact zero divisor → `±Inf` (live 4.1.2), not NULL.
///
/// Global non-ANSI `/` rewrite (`nullif(divisor, 0)`) would turn bare `1/sin(0)` into NULL.
/// Branch on exact zero so the reciprocal-trig surface matches Spark without changing the
/// div-by-zero analyzer rule (F2).
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

pub(super) fn strip_outer_alias(expr: Expr) -> Expr {
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
pub(super) fn collapse_identity_alias_chain(expr: Expr) -> Expr {
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
pub(super) fn extract_projection_expr(plan: &LogicalPlan) -> PyResult<Expr> {
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

impl PyColumn {
    /// ===========================================================================================
    /// `array_agg` (+ optional `DISTINCT`) with Spark `collect_list` / `collect_set` NULL / empty
    /// semantics: exclude NULL elements; empty group → empty array (not NULL).
    /// ===========================================================================================
    pub(super) fn collect_aggregate(argument: Expr, distinct: bool) -> PyResult<Self> {
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
    pub(super) fn count_distinct_argument(args: Vec<Expr>) -> PyResult<Expr> {
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
pub(super) const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

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
/// `Err` so the `cast` / `try_cast` boundary can raise [`crate::AnalysisException`].
/// ===========================================================================================
// === r24 A3: parse_data_type cast vocabulary ===
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

/// Refuse a higher-order function nested inside another one's lambda body.
///
/// **This is an upstream DataFusion 54.1 limitation, measured, not a repark choice.** A nested
/// lambda whose value argument is a real column fails during evaluation with
/// `Field of physical LambdaVariable with index 0 doesn't match batch field` — and it fails the
/// same way through DataFusion's OWN SQL planner, so no way of building the expression avoids it:
///
/// ```text
/// SELECT array_any_match(a, x -> array_any_match(b, y -> y > 4)) FROM t
///   => Field { "y": Int32 } != Field { "x": Int32 }
/// ```
///
/// Refusing here turns a cryptic execution-time error into a statement of the limit. It also
/// closes the S0 this replaced: before lambda parameters were given unique plan names, two
/// lambdas both minting `x` made the inner body bind to the OUTER variable, and
/// `exists(a, x -> exists(b, y -> y > 4))` returned an exactly INVERTED boolean with no error.
pub(super) fn refuse_nested_higher_order(body: &Expr, name: &str) -> PyResult<()> {
    let mut nested = false;
    body.apply(|expr| {
        if matches!(expr, Expr::HigherOrderFunction(_)) {
            nested = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(crate::datafusion_to_py_err)?;
    if nested {
        return Err(crate::UnsupportedOperationException::new_err(format!(
            "{name}: a higher-order function nested inside another one's lambda is not supported \
             (DataFusion 54.1 fails such a plan at evaluation, through its own SQL planner too). \
             Compute the inner result in a separate column first."
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::col;

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
