//! Spark `try_*` inversions: NULL instead of raise.

mod arith;
mod convert;

use std::sync::Arc;

use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;

pub use arith::{try_add_udf, try_divide_udf, try_mod_udf, try_multiply_udf, try_subtract_udf};
pub use convert::{try_to_binary_udf, try_to_date_udf, try_to_number_udf, try_to_time_udf};

/// Scalar `try_*` kernels this unit registers (aggregates `try_sum` / `try_avg` register elsewhere).
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        try_divide_udf(),
        try_mod_udf(),
        try_add_udf(),
        try_subtract_udf(),
        try_multiply_udf(),
        try_to_date_udf(),
        try_to_number_udf(),
        try_to_binary_udf(),
        try_to_time_udf(),
    ]
}

/// Install the scalar `try_*` names on a session.
pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}
