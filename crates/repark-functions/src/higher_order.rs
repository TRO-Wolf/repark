//! Spark higher-order (lambda) functions — the registry both doors resolve through.
//!
//! DataFusion 54.1 keeps higher-order functions in a registry **separate** from scalar UDFs
//! (`SessionState::higher_order_functions`), reached by
//! [`SessionContext::register_higher_order_function`]. Nothing in `register_all`'s scalar/aggregate
//! /window loops touches it, so before FNP-4 a repark session carried DataFusion's three defaults
//! and no Spark spellings at all.
//!
//! Two callers need the same table and must not drift apart (charter clause C-012):
//!
//! * the **SQL door**, which resolves a spelling out of the session registry — [`register`];
//! * the **facade**, whose `PyColumn` is standalone and has no session to resolve against, so the
//!   binding embeds the UDF instance by hand — [`by_name`].
//!
//! Both read [`functions`], so a name reachable from one door is reachable from the other with the
//! same kernel behind it.
//!
//! **Aliasing is not free.** A Spark spelling is attached to an upstream kernel only where the
//! semantics actually match. `array_any_match` is bit-for-bit Spark `exists` under the default
//! three-valued logic, so `exists` is an alias. `array_transform` and `array_filter` are **not**
//! aliased to Spark `transform` / `filter`: both declare a single lambda parameter, and Spark's
//! `(element, index)` form is a hard plan error against them. A divergence absorbed silently
//! behind an alias is worse than no alias.

use std::sync::Arc;

use datafusion::functions_nested::all_default_higher_order_functions;
use datafusion::logical_expr::HigherOrderUDF;
use datafusion::prelude::SessionContext;

/// Spark spellings attached to an upstream kernel whose semantics already match.
///
/// Kept as a table rather than inline so the "which upstream kernel is Spark-equivalent?" decision
/// is visible in one place and reviewable on its own.
const SPARK_ALIASES: &[(&str, &[&str])] = &[
    // Spark `exists(array, x -> pred)`: three-valued null logic, empty array -> false,
    // null array -> null. `array_any_match` matches all three under DataFusion's default.
    ("array_any_match", &["exists"]),
];

/// ===========================================================================================
/// Every higher-order function a repark session resolves, Spark spellings included.
/// ===========================================================================================
#[must_use]
pub fn functions() -> Vec<Arc<HigherOrderUDF>> {
    all_default_higher_order_functions()
        .into_iter()
        .map(|function| {
            let aliases = SPARK_ALIASES
                .iter()
                .find(|(name, _)| *name == function.name())
                .map(|(_, aliases)| *aliases);
            match aliases {
                None => function,
                Some(aliases) => Arc::new(
                    function
                        .as_ref()
                        .clone()
                        .with_aliases(aliases.iter().copied()),
                ),
            }
        })
        .collect()
}

/// ===========================================================================================
/// Resolve a spelling — canonical name or Spark alias — for a caller with no session.
/// ===========================================================================================
///
/// The facade's expression builder is standalone, so it cannot ask a `SessionContext`. Reading
/// the same [`functions`] table the session is populated from is what keeps the two doors on one
/// kernel.
#[must_use]
pub fn by_name(name: &str) -> Option<Arc<HigherOrderUDF>> {
    let lowered = name.to_ascii_lowercase();
    functions().into_iter().find(|function| {
        function.name().eq_ignore_ascii_case(&lowered)
            || function
                .aliases()
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(&lowered))
    })
}

/// ===========================================================================================
/// Install the table on a session. Called by [`crate::register_all`].
/// ===========================================================================================
pub(crate) fn register(ctx: &SessionContext) {
    for function in functions() {
        ctx.register_higher_order_function(function);
    }
}

#[cfg(test)]
mod tests {
    use super::{by_name, functions};

    #[test]
    fn spark_exists_resolves_to_the_any_match_kernel() {
        let function = by_name("exists").expect("exists is a Spark alias of array_any_match");
        assert_eq!(function.name(), "array_any_match");
    }

    #[test]
    fn transform_and_filter_are_not_aliased_to_the_arity_deficient_kernels() {
        // `array_transform` / `array_filter` declare ONE lambda parameter; Spark's
        // `(element, index)` form is a hard plan error against them, so the Spark spellings must
        // NOT resolve here. RePark kernels own those names instead.
        assert!(by_name("transform").is_none());
        assert!(by_name("filter").is_none());
    }

    #[test]
    fn the_facade_table_and_the_session_table_are_the_same_table() {
        // `by_name` reads `functions()`, which is what `register` installs — the property the
        // two-door contract rests on. A future `by_name` that consults its own list breaks this.
        for function in functions() {
            let resolved = by_name(function.name()).expect("every registered name resolves");
            assert_eq!(resolved.name(), function.name());
        }
    }
}
