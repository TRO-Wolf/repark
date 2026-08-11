use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::{SessionBuildConf, SessionExtension, SessionTimeZone};
use repark_functions::cardinality::ReparkSqlConfig;
use repark_functions::session_time_zone::{SessionTimeZoneConfig, session_time_zone_from_options};

use super::SparkExtension;

/// The hook's second argument, spelled once: the resolved zone `build()` hands `configure`.
fn build_conf<'a>(
    conf: &'a HashMap<String, String>,
    zone: &'a SessionTimeZone,
) -> SessionBuildConf<'a> {
    SessionBuildConf {
        conf,
        session_time_zone: zone,
    }
}

/// `configure` re-homes v1's inline r24 SB1 install: the builder conf map's `repark.sql.*`
/// keys land on the `SessionConfig` as the `ReparkSqlConfig` `ConfigExtension`.
#[test]
fn configure_installs_repark_sql_config_from_conf_map() {
    let mut conf = HashMap::new();
    conf.insert("repark.sql.maxArrayElements".to_string(), "42".to_string());
    let zone = SessionTimeZone::default();
    let config = SparkExtension
        .configure(build_conf(&conf, &zone), SessionConfig::new())
        .unwrap();
    let installed = config
        .options()
        .extensions
        .get::<ReparkSqlConfig>()
        .expect("configure must install ReparkSqlConfig");
    assert_eq!(installed.max_array_elements, 42);
    assert!(!installed.allow_local_filesystem_ddl);
}

/// H-1a split B: `configure` is the ONE crossing point where `repark-core`'s resolved session
/// zone reaches `repark-functions`' extractor layer. The hook must install the carrier with the
/// zone it was HANDED — not a re-parse of the conf map, and not the default.
#[test]
fn configure_installs_the_resolved_session_time_zone_carrier() {
    let mut conf = HashMap::new();
    // A conf map whose raw string DISAGREES with the resolved value: if the hook ever re-parsed
    // the map instead of carrying what `build()` resolved, the assertion below would see the
    // padded string (or, with a stricter parse, the default) instead of `Asia/Tokyo`.
    conf.insert(
        "spark.sql.session.timeZone".to_string(),
        "  Asia/Tokyo ".to_string(),
    );
    let zone = SessionTimeZone::parse("Asia/Tokyo").expect("a real zone");
    let config = SparkExtension
        .configure(build_conf(&conf, &zone), SessionConfig::new())
        .unwrap();
    assert_eq!(
        session_time_zone_from_options(config.options()),
        "Asia/Tokyo"
    );
    assert!(
        config
            .options()
            .extensions
            .get::<SessionTimeZoneConfig>()
            .is_some(),
        "the carrier itself must be installed, not merely a matching string somewhere"
    );
}

/// The carrier is installed on EVERY Spark session, including one that never set the key — so
/// the extractor layer never falls back to its own default in a real session.
#[test]
fn configure_installs_the_carrier_even_for_the_default_zone() {
    let conf = HashMap::new();
    let zone = SessionTimeZone::default();
    let config = SparkExtension
        .configure(build_conf(&conf, &zone), SessionConfig::new())
        .unwrap();
    assert_eq!(session_time_zone_from_options(config.options()), "UTC");
    assert_eq!(
        zone.id(),
        repark_core::DEFAULT_SESSION_TIME_ZONE,
        "the door carries the ENGINE's default; it does not invent one"
    );
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
    let zone = SessionTimeZone::default();
    let err = SparkExtension
        .configure(build_conf(&conf, &zone), SessionConfig::new())
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

/// PR-4 rider restoration (p2b rider #1): `register` composes [`repark_ta::TaExtension`] at v1's
/// position, so a Spark-extended session has the TA window UDFs — the v1 `build()` behaviour this
/// door owes. Bit-exact against the kernel the repark-ta goldens gate, not an approximate compare.
#[tokio::test]
async fn register_composes_the_ta_extension_window_udfs() {
    let close: Vec<f64> = vec![
        10.0, 10.5, 11.25, 10.75, 12.0, 12.5, 11.5, 13.0, 13.75, 13.25,
    ];
    let ts: Vec<i64> = (0..close.len())
        .map(|i| i64::try_from(i).expect("ts fits i64"))
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("close", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(Float64Array::from(close.clone())),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("bars", batch).unwrap();
    assert!(
        ctx.sql("SELECT ta_ema(close, 3) OVER (ORDER BY ts) FROM bars")
            .await
            .is_err(),
        "stock context must not know ta_ema"
    );

    SparkExtension.register(&ctx).unwrap();
    let batches = ctx
        .sql("SELECT ta_ema(close, 3) OVER (ORDER BY ts) AS v FROM bars ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut engine = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("v is Float64");
        for i in 0..column.len() {
            engine.push(if column.is_null(i) {
                f64::NAN
            } else {
                column.value(i)
            });
        }
    }
    let kernel = repark_ta::ema(&close, 3).unwrap();
    assert_eq!(engine.len(), kernel.len());
    for (i, (a, b)) in engine.iter().zip(&kernel).enumerate() {
        assert!(
            (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits(),
            "bit mismatch at row {i}: engine {a:?} vs kernel {b:?}"
        );
    }
}
