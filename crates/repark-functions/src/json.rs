use std::sync::Arc;

use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;

mod ddl;
mod decode;
mod from_json;
mod path;
mod reader;
mod scalars;
mod schema_of;
mod to_json;

pub use from_json::from_json_udf;
pub use scalars::{get_json_object_udf, json_array_length_udf, json_object_keys_udf};
pub use schema_of::schema_of_json_udf;
pub use to_json::to_json_udf;

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        get_json_object_udf(),
        json_array_length_udf(),
        json_object_keys_udf(),
        schema_of_json_udf(),
        to_json_udf(),
        from_json_udf(),
    ]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}
