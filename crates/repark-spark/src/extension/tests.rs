use std::collections::HashMap;

use datafusion::arrow::array::{Float64Array, Int32Array};
use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::SessionExtension;
use repark_functions::cardinality::ReparkSqlConfig;

use super::SparkExtension;

/// `configure` re-homes v1's inline r24 SB1 install: the builder conf map's `repark.sql.*`
/// keys land on the `SessionConfig` as the `ReparkSqlConfig` `ConfigExtension`.
#[test]
fn configure_installs_repark_sql_config_from_conf_map() {
    let mut conf = HashMap::new();
    conf.insert("repark.sql.maxArrayElements".to_string(), "42".to_string());
    let config = SparkExtension
        .configure(&conf, SessionConfig::new())
        .unwrap();
    let installed = config
        .options()
        .extensions
        .get::<ReparkSqlConfig>()
        .expect("configure must install ReparkSqlConfig");
    assert_eq!(installed.max_array_elements, 42);
    assert!(!installed.allow_local_filesystem_ddl);
}

/// v1's fail-loud contract: a present-but-unparsable `repark.sql.*` value errors at build
/// time, never silently falls back to the default.
#[test]
fn configure_refuses_unparsable_conf_value() {
    let mut conf = HashMap::new();
    conf.insert(
        "repark.sql.maxArrayElements".to_string(),
        "not-a-number".to_string(),
    );
    let err = SparkExtension
        .configure(&conf, SessionConfig::new())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("repark.sql.maxArrayElements"),
        "error must name the conf key: {err}"
    );
}

/// `register` installs the Spark function registry: `weekofyear` (a date-shim function stock
/// DataFusion does not ship) is callable from SQL after the hook runs.
#[tokio::test]
async fn register_makes_spark_shim_functions_callable() {
    let ctx = SessionContext::new();
    assert!(
        ctx.sql("SELECT weekofyear(DATE '2024-03-15')")
            .await
            .is_err(),
        "stock context must not know weekofyear"
    );
    SparkExtension.register(&ctx).unwrap();
    let batches = ctx
        .sql("SELECT weekofyear(DATE '2024-03-15') AS w")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let weeks = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(weeks.value(0), 11);
}

/// `register` appends the Spark expression-semantics analyzer rules: integer `/` yields a
/// DOUBLE (Spark), not integer division (DataFusion).
#[tokio::test]
async fn register_installs_spark_integer_division_semantics() {
    let ctx = SessionContext::new();
    SparkExtension.register(&ctx).unwrap();
    let batches = ctx
        .sql("SELECT 5 / 2 AS q")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let quotients = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Spark semantics: integer / must produce Float64");
    assert!((quotients.value(0) - 2.5).abs() < f64::EPSILON);
}
