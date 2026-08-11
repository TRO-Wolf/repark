use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::SessionExtension;

use super::TaExtension;
use crate::{ema, udf};

/// A tiny deterministic bar series — enough rows to clear `ta_ema`'s NaN prefix at period 3.
fn closes() -> Vec<f64> {
    vec![
        10.0, 10.5, 11.25, 10.75, 12.0, 12.5, 11.5, 13.0, 13.75, 13.25,
    ]
}

/// Register `closes()` as table `bars` (`ts` orders the window frame) on a bare context.
fn context_with_bars() -> SessionContext {
    let close = closes();
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
            Arc::new(Float64Array::from(close)),
        ],
    )
    .expect("fixture batch");
    let ctx = SessionContext::new();
    ctx.register_batch("bars", batch).expect("register bars");
    ctx
}

/// `register` is the whole point of the wrapper: after the hook runs, the TA window UDFs are
/// SQL-callable and the SQL route is **bit-exact** against the kernel the goldens gate — the
/// crate's own `to_bits` idiom, not an approximate compare. The name-set assertion pins that the
/// wrapper forwards the *whole* registry, not one function.
#[tokio::test]
async fn ta_extension_register_installs_the_whole_ta_udf_set_bit_exact() {
    let ctx = context_with_bars();
    assert!(
        ctx.sql("SELECT ta_ema(close, 3) OVER (ORDER BY ts) AS v FROM bars")
            .await
            .is_err(),
        "a bare context must not know ta_ema before the hook runs"
    );

    TaExtension.register(&ctx).expect("register must succeed");

    let batches = ctx
        .sql("SELECT ta_ema(close, 3) OVER (ORDER BY ts) AS v FROM bars ORDER BY ts")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
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

    let kernel = ema(&closes(), 3).expect("kernel");
    assert_eq!(engine.len(), kernel.len(), "row count");
    for (i, (a, b)) in engine.iter().zip(&kernel).enumerate() {
        assert!(
            (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits(),
            "bit mismatch at row {i}: engine {a:?} vs kernel {b:?}"
        );
    }

    // The whole set, not just the one exercised above.
    let registered = ctx
        .state()
        .window_functions()
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    for expected in udf::window_udfs() {
        assert!(
            registered.contains(expected.name()),
            "{} must be registered by TaExtension",
            expected.name()
        );
    }
}

/// The trait-wrapping both-sides audit: TA registers no `ConfigExtension` and reads no conf key,
/// so `configure` must stay the trait default — a pass-through that returns the `SessionConfig`
/// untouched even when unrelated `repark.*` keys are present (v1 behaviour: `build()` called only
/// `udf::register_all`, never a TA-side config install).
#[test]
fn ta_extension_configure_is_the_trait_default_pass_through() {
    let mut conf = HashMap::new();
    conf.insert("repark.sql.maxArrayElements".to_string(), "42".to_string());
    let base = SessionConfig::new().with_target_partitions(7);
    let zone = repark_core::SessionTimeZone::default();
    let out = TaExtension
        .configure(
            repark_core::SessionBuildConf {
                conf: &conf,
                session_time_zone: &zone,
            },
            base,
        )
        .expect("the default hook cannot fail");
    assert_eq!(
        out.target_partitions(),
        7,
        "configure must return the config untouched"
    );
}
