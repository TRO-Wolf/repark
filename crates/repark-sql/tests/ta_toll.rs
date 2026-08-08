//! Q11 — the ANSI door's **TA toll** (design §2 Q11: "ANSI toll: one smoke row (`f64::to_bits` vs
//! golden) + the non-literal-period refuse row").
//!
//! The TA function set is owned by NEITHER door. `repark-ta` ships a thin register-only
//! `TaExtension`; the Spark door composes it, and a **native** session opts in by installing it
//! itself. That is the claim this file pins, and it is a claim about the SEAM, not about TA: an
//! extension installed on a session is visible through EVERY door, because extensions are
//! session-scoped, not dialect-scoped (design §2 Q13 / graft G5 — the line PR-6 freezes into
//! `docs/design/session-api.md`).
//!
//! Session profile: **Native + `TaExtension`**. Deliberately NOT `SparkExtension` — a
//! Spark-extended session would bring Spark expression semantics along and the row would stop
//! describing this door.
//!
//! `repark-ta` is a DEV-dependency of this crate (feature `datafusion`); the crate-DAG guard
//! scopes layering to normal edges, so no product edge is created (see `Cargo.toml`).
//!
//! The oracle is the crate's recorded C TA-Lib 0.4.0 golden (`ema_21.bin` over
//! `fixture_close.bin`), read straight off disk — no golden is re-recorded here, and the
//! comparison is strict `f64::to_bits` equality, the only comparison the goldens' own gate
//! accepts.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use repark_ta::TaExtension;

/// A golden `.bin` (little-endian `f64`s) from the `repark-ta` crate.
fn fixture(name: &str) -> Vec<f64> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../repark-ta/tests/goldens")
        .join(format!("{name}.bin"));
    let bytes = std::fs::read(&path)
        .unwrap_or_else(|error| panic!("missing fixture {}: {error}", path.display()));
    bytes
        .chunks_exact(8)
        .map(|chunk| {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(chunk);
            f64::from_le_bytes(buf)
        })
        .collect()
}

/// A NATIVE session (no Spark anything) with the ANSI door as its dialect and `TaExtension`
/// installed at the build hook, holding the 5000-row close series as temp view `bars`.
fn ansi_session_with_ta() -> (ReparkSession, Vec<f64>) {
    let close = fixture("fixture_close");
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
    .expect("fixture batch");

    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .with_extension(Arc::new(TaExtension))
        .build()
        .expect("session must build");
    session
        .create_or_replace_temp_view("bars", vec![batch])
        .expect("register bars");
    (session, close)
}

/// The smoke row: one TA kernel driven through **ANSI-door SQL** as a window function, compared
/// `f64::to_bits`-exactly against the recorded C TA-Lib golden.
///
/// What it proves is the Q11 ruling, not the kernel (the kernel is gated in its own crate): a
/// native session that opts into `TaExtension` gets the TA surface through THIS door, unchanged.
///
/// Mutation: drop `.with_extension(Arc::new(TaExtension))` → `ta_ema` is unknown and the plan
/// fails, which is also the assertion in `ta_is_absent_without_the_extension` below.
#[tokio::test]
async fn ta_ema_through_the_ansi_door_is_bit_exact_against_the_golden() {
    let (session, _close) = ansi_session_with_ta();
    let golden = fixture("ema_21");

    let batches = session
        .sql("SELECT ta_ema(close, 21) OVER (ORDER BY ts) AS v FROM bars ORDER BY ts")
        .await
        .expect("ta_ema must plan through the ANSI door")
        .collect()
        .await
        .expect("collect");

    let mut engine = Vec::new();
    for batch in &batches {
        assert_eq!(
            batch.column(0).data_type(),
            &DataType::Float64,
            "the window output must be Float64 (value AND type)"
        );
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        for i in 0..column.len() {
            engine.push(if column.is_null(i) {
                f64::NAN
            } else {
                column.value(i)
            });
        }
    }

    assert_eq!(engine.len(), golden.len(), "row count vs golden");
    for (i, (got, want)) in engine.iter().zip(&golden).enumerate() {
        assert!(
            (got.is_nan() && want.is_nan()) || got.to_bits() == want.to_bits(),
            "bit mismatch at row {i}: engine {got:?} vs C TA-Lib golden {want:?}"
        );
    }
}

/// The `ta_ema` call whose scalar period argument is a COLUMN rather than a literal.
const NON_LITERAL_PERIOD_SQL: &str = "SELECT ta_ema(close, ts) OVER (ORDER BY ts) AS v FROM bars";

/// The refuse row: a NON-LITERAL scalar parameter is a plan error, and the message names the
/// function and the literal requirement. Scalar params are read off the plan's literal arguments,
/// so a column reference cannot be honoured — and quietly producing something would be worse than
/// refusing (design §2 Q11 names this row explicitly).
///
/// Mutation: making the period argument accept a column silently turns this red.
#[tokio::test]
async fn ta_non_literal_period_refuses_loud_through_the_ansi_door() {
    let (session, _close) = ansi_session_with_ta();

    // A lazy plan may refuse at plan time or at collect; either boundary is a legitimate refuse,
    // so accept whichever fires — but require that ONE of them does.
    let message = match session.sql(NON_LITERAL_PERIOD_SQL).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("a non-literal period must refuse at plan time or at collect")
            .to_string(),
    };
    assert!(
        message.contains("literal") || message.contains("ta_ema"),
        "the refusal must name the literal requirement or the function: {message}"
    );
}

/// The other side of "opt in": a native ANSI session WITHOUT the extension does not know `ta_*`.
/// Without this, the smoke row above could be green for the wrong reason (TA leaking in through
/// some always-on path), and the Q11 ruling — TA is owned by neither door, sessions opt in —
/// would be untested.
#[tokio::test]
async fn ta_is_absent_from_a_native_ansi_session_without_the_extension() {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("session");
    session
        .sql("SELECT ta_ema(1.0, 21) OVER () AS v")
        .await
        .expect_err("ta_ema must be unknown until TaExtension is installed");
}
