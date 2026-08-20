//! Clause C-012 — the facade and the SQL door must resolve the SAME kernel.
//!
//! The facade builds an [`Expr`] standalone, with no `SessionContext` to resolve names against
//! (a `PyColumn` has none), so `function_dispatch` embeds a UDF instance by hand. The SQL door
//! resolves the same spelling out of the session registry, where `repark_functions::register_all`
//! installed `datafusion-spark` and then overwrote names with the repark shims. Nothing forces
//! the two to agree, and where they disagree the same call returns different answers depending
//! on which door the user came through — silently.
//!
//! FN-GT1/GT2 closed sixteen of these by hand under the policy *"both doors resolve the same
//! UDF"*. This guard makes the policy mechanical: it compares the concrete implementation type
//! behind each spelling on both paths. `EXPECTED_DIVERGENCES` is the sanctioned-out table, one
//! row per name with the reason it is still open; it **ratchets DOWN only** — a name leaves when
//! its unit closes it, and a new row needs the same justification any EXCEPTIONS entry does.
//!
//! Campaign: FNP-1. Charter: `task/fnp-0-charter-ledger.md` C-012.

use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{Expr, ScalarUDF, lit};
use datafusion::prelude::SessionContext;

use super::function_dispatch::{call_scalar_expr, unary_aggregate_udaf};

/// Names whose two doors are still known to resolve different kernels, with the reason.
///
/// RATCHETS DOWN ONLY. Every row is a live two-door divergence; leaving one here is a decision,
/// not an oversight. The seventeen below are the latent set measured 2026-08-20 — the facade
/// lowers each to a DataFusion-core builder while the session resolves the `datafusion-spark`
/// kernel — and each needs its own semantic adjudication before it can be closed or registered.
const EXPECTED_DIVERGENCES: &[(&str, &str)] = &[
    (
        "ascii",
        "facade expr_fn::ascii vs datafusion-spark ascii — codepoint vs byte on non-ASCII",
    ),
    (
        "base64",
        "facade lowers to encode(x, 'base64'); spark kernel chunks at 76 chars",
    ),
    ("unbase64", "mirror of base64"),
    (
        "ceil",
        "DF-core ceil is float-first; spark ceil carries the decimal target-scale arm",
    ),
    ("ceiling", "alias of ceil"),
    ("floor", "mirror of ceil"),
    (
        "round",
        "DF-core round vs spark HALF_UP with a decimal target scale",
    ),
    (
        "length",
        "DF-core length is chars; spark length is chars for string, bytes for binary",
    ),
    ("character_length", "alias of length"),
    (
        "like",
        "DF-core LIKE vs spark like with an explicit escape argument",
    ),
    ("ilike", "mirror of like"),
    (
        "elt",
        "facade lowers to make_array + array_element; spark elt has its own null/bounds rule",
    ),
    (
        "size",
        "DF-core cardinality vs spark size with spark.sql.legacy.sizeOfNull",
    ),
    (
        "sec",
        "facade lowers to 1/cos; spark sec is a kernel with its own overflow behaviour",
    ),
    ("csc", "mirror of sec"),
    (
        "slice",
        "DF-core array_slice is 0-based-tolerant; spark slice is 1-based and raises on 0",
    ),
    (
        "array_repeat",
        "DF-core array_repeat vs spark array_repeat on negative counts",
    ),
    (
        "array_contains",
        "DF-core array_has vs spark array_contains three-valued null",
    ),
    ("array_has", "alias of array_contains"),
    (
        "date_part",
        "DF-core date_part vs spark date_part field-name set",
    ),
    ("datepart", "alias of date_part"),
];

/// Scalar spellings this guard checks. Positive controls (the GT1/GT2-closed names) are included
/// deliberately: if a future change re-opens one, this test goes red rather than staying silent.
const SCALAR_NAMES: &[(&str, usize)] = &[
    // The two live divergences this unit closes.
    ("to_timestamp", 1),
    // FNP-3 — arms added for kernels the session already registered.
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
    // GT1/GT2-closed positive controls — these must agree.
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

/// The UDF the facade embeds in its standalone expression for `name`.
///
/// `ScalarUDF`'s `PartialEq` delegates to the inner impl, so comparing two of these asks exactly
/// the question this guard cares about: is it the same function?
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

/// The sanctioned-out table is only honest if every name in it is really still diverging — a row
/// that has quietly been fixed must be removed, or the table stops meaning anything.
#[test]
fn expected_divergences_are_all_still_real() {
    let ctx = registered_session();
    let mut already_fixed = Vec::new();

    for (name, _reason) in EXPECTED_DIVERGENCES {
        let (Some(facade), Ok(door)) = (facade_udf(name, 1), ctx.udf(name)) else {
            continue; // arity or registration mismatch — not this test's claim
        };
        if *facade == *door {
            already_fixed.push(*name);
        }
    }

    assert!(
        already_fixed.is_empty(),
        "these names are listed as expected divergences but both doors already agree — the table \
         ratchets DOWN, so remove them: {already_fixed:?}"
    );
}
