//! Spark higher-order function registry for both SQL and facade doors.

use std::sync::Arc;

use datafusion::functions_nested::all_default_higher_order_functions;
use datafusion::logical_expr::HigherOrderUDF;
use datafusion::prelude::SessionContext;

/// Spark spellings attached to an upstream kernel whose semantics already match.
const SPARK_ALIASES: &[(&str, &[&str])] = &[
    // Spark `exists` preserves three-valued null logic, empty-array false, and null-array null.
    ("array_any_match", &["exists"]),
];

/// Every higher-order function a repark session resolves, Spark spellings included.
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

/// Resolve a canonical or aliased spelling from the shared function table.
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

/// Install the table on a session.
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
        assert!(by_name("transform").is_none());
        assert!(by_name("filter").is_none());
    }

    #[test]
    fn the_facade_table_and_the_session_table_are_the_same_table() {
        for function in functions() {
            let resolved = by_name(function.name()).expect("every registered name resolves");
            assert_eq!(resolved.name(), function.name());
        }
    }
}
