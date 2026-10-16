use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use repark_core::{ReparkSession, SqlDialect};
use repark_iceberg::write::store_cast::{BY_NAME_STORE_KERNELS, NS_WALL_KERNEL_NAMES};
use repark_sql::AnsiDialect;

fn ansi_door() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("ansi session")
}

#[test]
fn the_ansi_door_resolves_every_store_kernel_at_session_build() {
    let session = ansi_door();
    let ctx = session.context();
    for (name, kernel) in BY_NAME_STORE_KERNELS {
        assert_eq!(kernel().name(), name, "the table names its kernel");
        let resolved = ctx
            .udf(name)
            .unwrap_or_else(|error| panic!("the ANSI door does not resolve {name}: {error}"));
        assert_eq!(resolved.name(), name);
    }
    assert_eq!(
        NS_WALL_KERNEL_NAMES,
        [
            repark_functions::timestamp_ntz_cast::TIMESTAMP_NTZ_CAST_NAME,
            repark_functions::timestamp_ns_cast::TIMESTAMP_NS_CAST_NAME,
            repark_functions::timestamp_ns_cast::TIMESTAMPTZ_NS_CAST_NAME,
        ]
    );
    for name in NS_WALL_KERNEL_NAMES {
        let resolved = ctx
            .udf(name)
            .unwrap_or_else(|error| panic!("the ANSI door does not resolve {name}: {error}"));
        assert_eq!(resolved.name(), name);
    }
}
