//! The facade and SQL door must resolve the same kernel.
//! The facade embeds standalone UDFs; SQL resolves session-registered UDFs.
//! `EXPECTED_DIVERGENCES` lists measured exceptions.

use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{Expr, ScalarUDF, lit};
use datafusion::prelude::SessionContext;

use super::function_dispatch::{call_scalar_expr, unary_aggregate_udaf};

/// Names whose two doors still resolve different kernels, with the measured reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FacadeShape {
    /// The facade embeds one UDF that differs from the door's UDF.
    Kernel(usize),
    /// The facade composes expressions instead of calling a kernel.
    Composed,
}

const EXPECTED_DIVERGENCES: &[(&str, FacadeShape, &str)] = &[
    (
        "ascii",
        FacadeShape::Kernel(1),
        "facade expr_fn::ascii vs datafusion-spark ascii — codepoint vs byte on non-ASCII",
    ),
    (
        "base64",
        FacadeShape::Kernel(1),
        "facade lowers to encode(x, 'base64'); spark kernel chunks at 76 chars",
    ),
    ("unbase64", FacadeShape::Kernel(1), "mirror of base64"),
    (
        "ceil",
        FacadeShape::Kernel(1),
        "DF-core ceil is float-first; spark ceil carries the decimal target-scale arm",
    ),
    ("ceiling", FacadeShape::Kernel(1), "alias of ceil"),
    ("floor", FacadeShape::Kernel(1), "mirror of ceil"),
    (
        "round",
        FacadeShape::Kernel(1),
        "DF-core round vs spark HALF_UP with a decimal target scale",
    ),
    (
        "length",
        FacadeShape::Kernel(1),
        "DF-core length is chars; spark length is chars for string, bytes for binary",
    ),
    (
        "character_length",
        FacadeShape::Kernel(1),
        "alias of length",
    ),
    (
        "like",
        FacadeShape::Composed,
        "DF-core LIKE vs spark like with an explicit escape argument",
    ),
    ("ilike", FacadeShape::Composed, "mirror of like"),
    (
        "elt",
        FacadeShape::Kernel(2),
        "facade lowers to make_array + array_element; spark elt has its own null/bounds rule",
    ),
    (
        "size",
        FacadeShape::Kernel(1),
        "DF-core cardinality vs spark size with spark.sql.legacy.sizeOfNull",
    ),
    (
        "sec",
        FacadeShape::Composed,
        "facade lowers to 1/cos; spark sec is a kernel with its own overflow behaviour",
    ),
    ("csc", FacadeShape::Composed, "mirror of sec"),
    (
        "slice",
        FacadeShape::Kernel(3),
        "DF-core array_slice is 0-based-tolerant; spark slice is 1-based and raises on 0",
    ),
    (
        "array_repeat",
        FacadeShape::Kernel(2),
        "DF-core array_repeat vs spark array_repeat on negative counts",
    ),
    (
        "array_contains",
        FacadeShape::Kernel(2),
        "DF-core array_has vs spark array_contains three-valued null",
    ),
    (
        "date_part",
        FacadeShape::Kernel(2),
        "DF-core date_part vs spark date_part field-name set",
    ),
    ("datepart", FacadeShape::Kernel(2), "alias of date_part"),
    (
        "log",
        FacadeShape::Kernel(1),
        "SQL door `log` is DataFusion's base-10, Spark's is natural — registry row LOG-1. The \
         facade is right (`ln`); the door answers 0.903 for log(8) where Spark answers 2.079",
    ),
    (
        "from_unixtime",
        FacadeShape::Kernel(1),
        "SQL door returns TIMESTAMP, the facade and Spark return STRING — registry row UNIX-1",
    ),
    (
        "array",
        FacadeShape::Kernel(1),
        "facade builds `make_array`, the door resolves the `array` alias; values agree, only the \
         kernel identity differs",
    ),
    (
        "array_element",
        FacadeShape::Kernel(2),
        "a DataFusion name Spark does not have at all (`UNRESOLVED_ROUTINE` there). The facade \
         reaches `element_at`; the door's own `array_element` returns NULL for a valid index, \
         which is an engine defect on a non-Spark spelling",
    ),
];

/// Scalar spellings covered by the explicit guard.
const SCALAR_NAMES: &[(&str, usize)] = &[
    ("to_timestamp", 1),
    ("crc32", 1),
    ("sha1", 1),
    ("sha", 1),
    ("soundex", 1),
    ("xxhash64", 1),
    ("format_string", 2),
    ("map_from_arrays", 2),
    ("datediff", 2),
    ("date_diff", 2),
    ("from_utc_timestamp", 2),
    ("to_utc_timestamp", 2),
    ("to_date", 1),
    ("make_date", 3),
    ("bit_length", 1),
    ("octet_length", 1),
    ("split_part", 3),
    ("regexp_count", 2),
];

fn registered_session() -> SessionContext {
    let ctx = SessionContext::new();
    repark_functions::register_all(&ctx);
    ctx
}

/// Return the UDF embedded by the facade for `name` and `arity`.
fn facade_udf(name: &str, arity: usize) -> Option<Arc<ScalarUDF>> {
    let args = vec![lit("2026-01-01"); arity];
    match call_scalar_expr(name, args).ok()? {
        Expr::ScalarFunction(scalar) => Some(scalar.func),
        _ => None,
    }
}

#[test]
fn every_scalar_spelling_resolves_the_same_kernel_on_both_doors() {
    let ctx = registered_session();
    let mut disagreements = Vec::new();

    for (name, arity) in SCALAR_NAMES {
        let Some(facade) = facade_udf(name, *arity) else {
            panic!("{name}: the facade dispatch table has no arm for arity {arity}");
        };
        let door = ctx
            .udf(name)
            .unwrap_or_else(|_| panic!("{name}: register_all installs no UDF under this name"));
        if *facade != *door {
            disagreements.push(*name);
        }
    }

    assert!(
        disagreements.is_empty(),
        "these spellings resolve a different kernel on the facade than on the SQL door, so the \
         same call returns different answers depending on the door: {disagreements:?}. Close the \
         divergence, or move the name into EXPECTED_DIVERGENCES with its reason."
    );
}

#[test]
fn facade_avg_is_the_repark_retracting_kernel_not_datafusion_core() {
    let ctx = registered_session();
    let facade = unary_aggregate_udaf("avg").expect("avg is a dispatched aggregate");
    let door = ctx.udaf("avg").expect("register_all installs avg");
    assert_eq!(
        *facade, *door,
        "F.avg() bypasses SparkAvgWithRetract and lands on DataFusion-core avg, so the facade \
         loses the Spark i64-count and null-on-empty arms that the SQL door keeps (FLOAT-AGG-2)"
    );
}

/// Confirm every sanctioned divergence remains real.
#[test]
fn expected_divergences_are_all_still_real() {
    assert_eq!(
        EXPECTED_DIVERGENCES.len(),
        24,
        "the sanctioned-out table changed size — ratchet DOWN, or justify the new row"
    );
    let ctx = registered_session();
    let mut already_fixed = Vec::new();

    let mut unbuildable = Vec::new();
    for (name, shape, _reason) in EXPECTED_DIVERGENCES {
        match shape {
            FacadeShape::Kernel(arity) => {
                let (Some(facade), Ok(door)) = (facade_udf(name, *arity), ctx.udf(name)) else {
                    unbuildable.push(*name);
                    continue;
                };
                if *facade == *door {
                    already_fixed.push(*name);
                }
            }
            FacadeShape::Composed => {
                if facade_udf(name, 1).is_some() || facade_udf(name, 2).is_some() {
                    already_fixed.push(*name);
                }
            }
        }
    }

    assert!(
        unbuildable.is_empty(),
        "these rows could not be checked at their declared arity, so the ratchet was not applied \
         to them — fix the arity or remove the row: {unbuildable:?}"
    );

    assert!(
        already_fixed.is_empty(),
        "these names are listed as expected divergences but both doors already agree — the table \
         ratchets DOWN, so remove them: {already_fixed:?}"
    );
}

/// Confirm every registered name reachable by the facade resolves the same kernel.
#[test]
fn every_registered_name_the_facade_reaches_resolves_the_same_kernel() {
    let ctx = registered_session();
    let sanctioned: std::collections::HashSet<&str> = EXPECTED_DIVERGENCES
        .iter()
        .map(|(name, _shape, _reason)| *name)
        .collect();
    let mut disagreements = Vec::new();

    let mut names: Vec<String> = ctx.state().scalar_functions().keys().cloned().collect();
    names.sort();
    for name in &names {
        if sanctioned.contains(name.as_str()) {
            continue;
        }
        let Ok(door) = ctx.udf(name) else { continue };
        let reached = (0..=3).find_map(|arity| facade_udf(name, arity));
        let Some(facade) = reached else { continue };
        if *facade != *door {
            disagreements.push(name.clone());
        }
    }

    assert!(
        disagreements.is_empty(),
        "{} of {} registered names resolve a different kernel on the facade than on the SQL door: \
         {disagreements:?}. Close each, or move it into EXPECTED_DIVERGENCES with its reason.",
        disagreements.len(),
        names.len()
    );
}
