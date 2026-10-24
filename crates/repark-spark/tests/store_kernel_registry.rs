use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::execution::FunctionRegistry;
use repark_core::{ReparkSession, SqlDialect};
use repark_iceberg::write::ntz_store::NTZ_WALL_CAST_UDF_NAME;
use repark_iceberg::write::store_cast::{BY_NAME_STORE_KERNELS, NS_WALL_KERNEL_NAMES};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const NEW_YORK_WALL: i64 = 1_767_305_045_123_456;
const MISSING_KERNEL: &str = "the store kernel `__repark_cast_timestamp_ntz__` is not registered \
                              in this session";
const BY_NAME_INSERT: &str = "INSERT INTO ice.ns.t BY NAME SELECT id, x AS ts FROM ice.ns.src";

fn spark_door() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .expect("spark session")
}

fn new_york_door() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config("spark.sql.session.timeZone", "America/New_York")
        .build()
        .expect("spark session")
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<(), String> {
    match session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn instant_source(session: &ReparkSession) -> TempDir {
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    for setup in [
        "CREATE NAMESPACE ice.ns",
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP) USING iceberg",
        "INSERT INTO ice.ns.src VALUES (1, TIMESTAMP '2026-01-02 03:04:05.123456+00:00')",
        "CREATE TABLE ice.ns.t (id INT, ts TIMESTAMP_NTZ) USING iceberg",
    ] {
        assert_eq!(attempt(session, setup).await, Ok(()), "{setup}");
    }
    warehouse
}

async fn stored_walls(session: &ReparkSession) -> Vec<i64> {
    let batches = session
        .sql("SELECT ts FROM ice.ns.t")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let mut walls = Vec::new();
    for batch in batches {
        let ticks = cast(batch.column(0), &DataType::Int64).expect("ticks");
        let ticks = ticks.as_primitive::<Int64Type>();
        walls.extend(
            (0..ticks.len())
                .filter(|row| !ticks.is_null(*row))
                .map(|row| ticks.value(row)),
        );
    }
    walls
}

#[tokio::test]
async fn missing_wall_kernel_refuses_instead_of_storing_utc() {
    let registered = new_york_door();
    let _registered_warehouse = instant_source(&registered).await;
    assert_eq!(attempt(&registered, BY_NAME_INSERT).await, Ok(()));
    assert_eq!(stored_walls(&registered).await, vec![NEW_YORK_WALL]);

    let missing = new_york_door();
    let _missing_warehouse = instant_source(&missing).await;
    missing.context().deregister_udf(NTZ_WALL_CAST_UDF_NAME);
    assert!(missing.context().udf(NTZ_WALL_CAST_UDF_NAME).is_err());
    let refusal = attempt(&missing, BY_NAME_INSERT)
        .await
        .expect_err("a missing wall kernel refuses the write");
    assert!(refusal.contains(MISSING_KERNEL), "{refusal}");
    assert!(
        refusal.contains("instead of storing the value as UTC"),
        "{refusal}"
    );
    assert_eq!(stored_walls(&missing).await, Vec::<i64>::new());
}

#[test]
fn the_spark_door_resolves_every_store_kernel_at_session_build() {
    let session = spark_door();
    let ctx = session.context();
    for (name, kernel) in BY_NAME_STORE_KERNELS {
        assert_eq!(kernel().name(), name, "the table names its kernel");
        let resolved = ctx
            .udf(name)
            .unwrap_or_else(|error| panic!("the Spark door does not resolve {name}: {error}"));
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
            .unwrap_or_else(|error| panic!("the Spark door does not resolve {name}: {error}"));
        assert_eq!(resolved.name(), name);
    }
}
