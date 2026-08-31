//! Spark higher-order function registry for both SQL and facade doors.

mod aggregate;
mod filter;
mod forall;
mod lambda_utils;
mod map_common;
mod map_filter;
mod map_zip_with;
mod transform;
mod transform_keys;
mod transform_values;
mod zip_with;

#[cfg(test)]
mod kernel_eval;

use std::sync::Arc;

use datafusion::functions_nested::all_default_higher_order_functions;
use datafusion::logical_expr::HigherOrderUDF;
use datafusion::prelude::SessionContext;

const SPARK_ALIASES: &[(&str, &[&str])] = &[("array_any_match", &["exists"])];

/// Every higher-order function a repark session resolves, Spark spellings included.
#[must_use]
pub fn functions() -> Vec<Arc<HigherOrderUDF>> {
    let mut functions: Vec<Arc<HigherOrderUDF>> = all_default_higher_order_functions()
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
        .collect();
    functions.extend([
        transform::transform_udf(),
        filter::filter_udf(),
        forall::forall_udf(),
        aggregate::aggregate_udf(),
        zip_with::zip_with_udf(),
        transform_keys::transform_keys_udf(),
        transform_values::transform_values_udf(),
        map_filter::map_filter_udf(),
        map_zip_with::map_zip_with_udf(),
    ]);
    functions
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

    /// pins: fnp-4c-higher-order-kernels/C-013
    #[test]
    fn transform_and_filter_are_repark_kernels_not_arity_deficient_aliases() {
        let transform = by_name("transform").expect("transform is registered");
        assert_eq!(transform.name(), "transform");
        let filter = by_name("filter").expect("filter is registered");
        assert_eq!(filter.name(), "filter");
    }

    /// pins: fnp-4c-higher-order-kernels/C-004
    #[test]
    fn reduce_is_an_alias_of_aggregate() {
        let reduce = by_name("reduce").expect("reduce is registered");
        assert_eq!(reduce.name(), "aggregate");
        assert!(reduce.aliases().iter().any(|alias| alias == "reduce"));
    }

    /// pins: fnp-4c-higher-order-kernels/C-011
    #[test]
    fn forall_and_the_map_family_resolve() {
        for name in [
            "forall",
            "zip_with",
            "transform_keys",
            "transform_values",
            "map_filter",
            "map_zip_with",
            "aggregate",
        ] {
            assert!(by_name(name).is_some(), "{name} must resolve");
        }
    }

    #[test]
    fn the_facade_table_and_the_session_table_are_the_same_table() {
        for function in functions() {
            let resolved = by_name(function.name()).expect("every registered name resolves");
            assert_eq!(resolved.name(), function.name());
        }
    }
}
