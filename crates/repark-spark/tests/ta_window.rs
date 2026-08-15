//! e2e — the TA window UDFs through `spark.sql`.
//!
//! Proves the SQL route (`ta_ema(close, 21) OVER (ORDER BY ts)`) registered at session build
//! produces output that is `f64::to_bits`-**identical** to calling the [`repark_ta`] kernel
//! directly on the ordered column. The input is the same 5000-row OHLC fixture the crate's golden
//! gate uses (`crates/repark-ta/tests/goldens/*.bin`), so the kernel outputs here are the very
//! series proven bit-exact against C TA-Lib — no goldens are re-recorded, and the assertion is
//! engine-vs-kernel on shared data. Every one of the 77 registered UDFs is exercised.
//!
//! Ported at phase-2 PR-4 from v1 `crates/repark-session/tests/ta_window.rs` — deferred rows
//! #8-#14 in `task/port/deferred-tests.md`. The session is now door-installed
//! (`SparkExtension` + `SparkDialect`); the UDFs arrive via the composed
//! [`repark_ta::TaExtension`]. The goldens path is unchanged — `repark-ta` sits at the same
//! sibling position in this workspace as it did in v1.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_ta::{
    adx, adxr, apo, aroon, aroonosc, atr, avgprice, bbands, beta, bop, cci, cmo, correl, dema, dx,
    ema, kama, linearreg, linearreg_angle, linearreg_intercept, linearreg_slope, ma, macd, macdext,
    macdfix, mama, mavp, max, medprice, midpoint, midprice, min, minus_di, minus_dm, mom, natr,
    plus_di, plus_dm, ppo, roc, rocp, rocr, rocr100, rsi, sar, sarext, sma, stddev, stoch, stochf,
    stochrsi, sum, t3, tema, trange, trima, trix, tsf, typprice, ultosc, var, wclprice, willr, wma,
};

/// A golden `.bin` (little-endian `f64`s) from the `repark-ta` crate, read as `Vec<f64>`.
fn fixture(name: &str) -> Vec<f64> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../repark-ta/tests/goldens")
        .join(format!("{name}.bin"));
    let bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()));
    bytes
        .chunks_exact(8)
        .map(|c| {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(c);
            f64::from_le_bytes(buf)
        })
        .collect()
}

/// Build the Spark-doored session the way a v1 session was assembled: extension at the two build
/// hooks, dialect as the session default (v1's `ReparkSession::new()`). `SparkExtension` composes
/// `repark_ta::TaExtension`, which is what registers the `ta_*` window UDFs this file exercises.
fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .expect("session")
}

/// A session with the OHLC fixture registered as temp view `bars` (columns `ts`, `open`, `high`,
/// `low`, `close`), plus the four input series for the direct kernel oracle.
fn session_with_bars() -> (ReparkSession, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let open = fixture("fixture_open");
    let high = fixture("fixture_high");
    let low = fixture("fixture_low");
    let close = fixture("fixture_close");
    // `periods` (the MAVP per-row period series) rides along in the view; most tests ignore it.
    let periods = fixture("fixture_periods");
    let n = close.len();
    let ts: Vec<i64> = (0..n)
        .map(|i| i64::try_from(i).expect("ts fits i64"))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("periods", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(Float64Array::from(open.clone())),
            Arc::new(Float64Array::from(high.clone())),
            Arc::new(Float64Array::from(low.clone())),
            Arc::new(Float64Array::from(close.clone())),
            Arc::new(Float64Array::from(periods)),
        ],
    )
    .expect("batch");

    let session = spark_session();
    session
        .create_or_replace_temp_view("bars", vec![batch])
        .expect("register bars");
    (session, open, high, low, close)
}

/// Run `SELECT {expr} AS v FROM bars ORDER BY ts` and return column `v` as `Vec<f64>`, in ts order
/// (the same index order as the fixture, so it aligns element-for-element with the kernel output).
async fn window_column(session: &ReparkSession, expr: &str) -> Vec<f64> {
    let sql = format!("SELECT {expr} AS v FROM bars ORDER BY ts");
    let batches = session
        .sql(&sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let mut out = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("v is Float64");
        for i in 0..column.len() {
            out.push(if column.is_null(i) {
                f64::NAN
            } else {
                column.value(i)
            });
        }
    }
    out
}

/// Strict `to_bits` equality, `NaN` ↔ `NaN` allowed (any payload) — the crate's own gate idiom.
fn assert_bit_exact(label: &str, engine: &[f64], kernel: &[f64]) {
    assert_eq!(engine.len(), kernel.len(), "{label}: length mismatch");
    for (i, (a, b)) in engine.iter().zip(kernel).enumerate() {
        if a.is_nan() && b.is_nan() {
            continue;
        }
        assert!(
            a.to_bits() == b.to_bits(),
            "{label}: bit mismatch at row {i}: engine {a:?} ({:#018x}) vs kernel {b:?} ({:#018x})",
            a.to_bits(),
            b.to_bits()
        );
    }
}

#[tokio::test]
async fn sql_route_single_series_kernels_match_the_kernel() {
    let (session, _open, _high, _low, close) = session_with_bars();
    let cases: Vec<(&str, Vec<f64>)> = vec![
        ("ta_sma(close, 20)", sma(&close, 20).unwrap()),
        ("ta_ema(close, 21)", ema(&close, 21).unwrap()),
        ("ta_rsi(close, 14)", rsi(&close, 14).unwrap()),
        ("ta_linearreg(close, 5)", linearreg(&close, 5).unwrap()),
        (
            "ta_linearreg_slope(close, 5)",
            linearreg_slope(&close, 5).unwrap(),
        ),
        (
            "ta_linearreg_intercept(close, 5)",
            linearreg_intercept(&close, 5).unwrap(),
        ),
        (
            "ta_linearreg_angle(close, 14)",
            linearreg_angle(&close, 14).unwrap(),
        ),
        ("ta_tsf(close, 5)", tsf(&close, 5).unwrap()),
        ("ta_min(close, 21)", min(&close, 21).unwrap()),
        ("ta_max(close, 21)", max(&close, 21).unwrap()),
        ("ta_sum(close, 21)", sum(&close, 21).unwrap()),
        // WG1 overlap-MA family (single series, one period param).
        ("ta_wma(close, 10)", wma(&close, 10).unwrap()),
        ("ta_dema(close, 10)", dema(&close, 10).unwrap()),
        ("ta_tema(close, 10)", tema(&close, 10).unwrap()),
        ("ta_trima(close, 10)", trima(&close, 10).unwrap()),
        ("ta_trima(close, 5)", trima(&close, 5).unwrap()),
        ("ta_kama(close, 10)", kama(&close, 10).unwrap()),
        ("ta_midpoint(close, 10)", midpoint(&close, 10).unwrap()),
        // WG2 single-series simple-momentum kernels (one period param).
        ("ta_mom(close, 10)", mom(&close, 10).unwrap()),
        ("ta_roc(close, 10)", roc(&close, 10).unwrap()),
        ("ta_rocp(close, 10)", rocp(&close, 10).unwrap()),
        ("ta_rocr(close, 10)", rocr(&close, 10).unwrap()),
        ("ta_rocr100(close, 10)", rocr100(&close, 10).unwrap()),
        ("ta_cmo(close, 14)", cmo(&close, 14).unwrap()),
        ("ta_trix(close, 30)", trix(&close, 30).unwrap()),
        // WG3 MACDFIX split (single series + a lone signal-period scalar; 12/26 pinned).
        ("ta_macdfix(close, 9)", macdfix(&close, 9).unwrap().0),
        ("ta_macdfix_signal(close, 9)", macdfix(&close, 9).unwrap().1),
        ("ta_macdfix_hist(close, 9)", macdfix(&close, 9).unwrap().2),
    ];
    for (expr, kernel) in cases {
        let engine = window_column(&session, &format!("{expr} OVER (ORDER BY ts)")).await;
        assert_bit_exact(expr, &engine, &kernel);
    }
}

#[tokio::test]
async fn sql_route_scalar_param_kernels_match_the_kernel() {
    let (session, _open, _high, _low, close) = session_with_bars();
    let cases: Vec<(&str, Vec<f64>)> = vec![
        ("ta_var(close, 5, 1.0)", var(&close, 5, 1.0).unwrap()),
        ("ta_stddev(close, 5, 2.0)", stddev(&close, 5, 2.0).unwrap()),
        // T3 threads a second scalar (vfactor) through the literal-arg path.
        ("ta_t3(close, 5, 0.7)", t3(&close, 5, 0.7).unwrap()),
        ("ta_t3(close, 5, 0.5)", t3(&close, 5, 0.5).unwrap()),
        (
            "ta_bbands_upper(close, 20, 2.0, 2.0)",
            bbands(&close, 20, 2.0, 2.0).unwrap().0,
        ),
        (
            "ta_bbands_middle(close, 20, 2.0, 2.0)",
            bbands(&close, 20, 2.0, 2.0).unwrap().1,
        ),
        (
            "ta_bbands_lower(close, 20, 2.0, 2.0)",
            bbands(&close, 20, 2.0, 2.0).unwrap().2,
        ),
        // APO/PPO thread three scalars (fast, slow, matype) through the literal-arg path.
        ("ta_apo(close, 12, 26, 0)", apo(&close, 12, 26, 0).unwrap()),
        ("ta_ppo(close, 12, 26, 0)", ppo(&close, 12, 26, 0).unwrap()),
        // Octo C5/C6: matype 7 (MAMA) through the SQL UDF literal-arg path.
        ("ta_apo(close, 12, 26, 7)", apo(&close, 12, 26, 7).unwrap()),
        ("ta_ppo(close, 12, 26, 7)", ppo(&close, 12, 26, 7).unwrap()),
        // WG3 MA selector (period + matype literals).
        ("ta_ma(close, 30, 0)", ma(&close, 30, 0).unwrap()),
        ("ta_ma(close, 20, 1)", ma(&close, 20, 1).unwrap()),
        ("ta_ma(close, 30, 7)", ma(&close, 30, 7).unwrap()),
        // WG3 MACD splits thread three scalars (fast, slow, signal).
        (
            "ta_macd(close, 12, 26, 9)",
            macd(&close, 12, 26, 9).unwrap().0,
        ),
        (
            "ta_macd_signal(close, 12, 26, 9)",
            macd(&close, 12, 26, 9).unwrap().1,
        ),
        (
            "ta_macd_hist(close, 12, 26, 9)",
            macd(&close, 12, 26, 9).unwrap().2,
        ),
        // WG3 MACDEXT splits thread six scalars (fast p/type, slow p/type, signal p/type).
        (
            "ta_macdext(close, 12, 0, 26, 0, 9, 0)",
            macdext(&close, 12, 0, 26, 0, 9, 0).unwrap().0,
        ),
        (
            "ta_macdext_signal(close, 12, 0, 26, 0, 9, 0)",
            macdext(&close, 12, 0, 26, 0, 9, 0).unwrap().1,
        ),
        (
            "ta_macdext_hist(close, 12, 0, 26, 0, 9, 0)",
            macdext(&close, 12, 0, 26, 0, 9, 0).unwrap().2,
        ),
        // WG4 STOCHRSI splits (single close series + four scalars: timeperiod, fastk, fastd, type).
        (
            "ta_stochrsi_fastk(close, 14, 5, 3, 0)",
            stochrsi(&close, 14, 5, 3, 0).unwrap().0,
        ),
        (
            "ta_stochrsi_fastd(close, 14, 5, 3, 0)",
            stochrsi(&close, 14, 5, 3, 0).unwrap().1,
        ),
        // Group G2: matype 7 (MAMA) on STOCHRSI — both split UDF entry points with fastd_matype=7
        // (lookback_total depends on fastd matype even for the %K line).
        (
            "ta_stochrsi_fastk(close, 14, 5, 3, 7)",
            stochrsi(&close, 14, 5, 3, 7).unwrap().0,
        ),
        (
            "ta_stochrsi_fastd(close, 14, 5, 3, 7)",
            stochrsi(&close, 14, 5, 3, 7).unwrap().1,
        ),
    ];
    for (expr, kernel) in cases {
        let engine = window_column(&session, &format!("{expr} OVER (ORDER BY ts)")).await;
        assert_bit_exact(expr, &engine, &kernel);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one flat case per multi-series UDF — a table, not real branching.
async fn sql_route_multi_series_kernels_match_the_kernel() {
    let (session, open, high, low, close) = session_with_bars();
    let cases: Vec<(&str, Vec<f64>)> = vec![
        (
            "ta_trange(high, low, close)",
            trange(&high, &low, &close).unwrap(),
        ),
        (
            "ta_atr(high, low, close, 14)",
            atr(&high, &low, &close, 14).unwrap(),
        ),
        (
            "ta_adx(high, low, close, 14)",
            adx(&high, &low, &close, 14).unwrap(),
        ),
        // CORREL takes two arbitrary series; high vs low mirrors the crate's golden case.
        ("ta_correl(high, low, 14)", correl(&high, &low, 14).unwrap()),
        // MIDPRICE is the two-series (high, low) member of the WG1 overlap family.
        (
            "ta_midprice(high, low, 10)",
            midprice(&high, &low, 10).unwrap(),
        ),
        // WG2 multi-series: WILLR/CCI/ULTOSC are H/L/C, AROON/AROONOSC are H/L, BOP is O/H/L/C.
        (
            "ta_willr(high, low, close, 14)",
            willr(&high, &low, &close, 14).unwrap(),
        ),
        (
            "ta_cci(high, low, close, 14)",
            cci(&high, &low, &close, 14).unwrap(),
        ),
        (
            "ta_aroon_down(high, low, 14)",
            aroon(&high, &low, 14).unwrap().0,
        ),
        (
            "ta_aroon_up(high, low, 14)",
            aroon(&high, &low, 14).unwrap().1,
        ),
        (
            "ta_aroonosc(high, low, 14)",
            aroonosc(&high, &low, 14).unwrap(),
        ),
        (
            "ta_ultosc(high, low, close, 7, 14, 28)",
            ultosc(&high, &low, &close, 7, 14, 28).unwrap(),
        ),
        (
            "ta_bop(open, high, low, close)",
            bop(&open, &high, &low, &close).unwrap(),
        ),
        // WG3 directional family: DX/ADXR/PLUS_DI/MINUS_DI are H/L/C; PLUS_DM/MINUS_DM are H/L.
        (
            "ta_dx(high, low, close, 14)",
            dx(&high, &low, &close, 14).unwrap(),
        ),
        (
            "ta_adxr(high, low, close, 14)",
            adxr(&high, &low, &close, 14).unwrap(),
        ),
        (
            "ta_plus_di(high, low, close, 14)",
            plus_di(&high, &low, &close, 14).unwrap(),
        ),
        (
            "ta_minus_di(high, low, close, 14)",
            minus_di(&high, &low, &close, 14).unwrap(),
        ),
        (
            "ta_plus_dm(high, low, 14)",
            plus_dm(&high, &low, 14).unwrap(),
        ),
        (
            "ta_minus_dm(high, low, 14)",
            minus_dm(&high, &low, 14).unwrap(),
        ),
        // WG4 stochastics: STOCH (H/L/C + 5 scalars) and STOCHF (H/L/C + 3 scalars), split ×2.
        (
            "ta_stoch_slowk(high, low, close, 5, 3, 0, 3, 0)",
            stoch(&high, &low, &close, 5, 3, 0, 3, 0).unwrap().0,
        ),
        (
            "ta_stoch_slowd(high, low, close, 5, 3, 0, 3, 0)",
            stoch(&high, &low, &close, 5, 3, 0, 3, 0).unwrap().1,
        ),
        (
            "ta_stochf_fastk(high, low, close, 5, 3, 0)",
            stochf(&high, &low, &close, 5, 3, 0).unwrap().0,
        ),
        (
            "ta_stochf_fastd(high, low, close, 5, 3, 0)",
            stochf(&high, &low, &close, 5, 3, 0).unwrap().1,
        ),
        // Group G2: matype 7 (MAMA) on STOCH/STOCHF — all-MAMA both legs, mixed 7/0, and
        // STOCHF type7 both split entry points (fastd matype trims the %K dense slice too).
        (
            "ta_stoch_slowk(high, low, close, 5, 3, 7, 3, 7)",
            stoch(&high, &low, &close, 5, 3, 7, 3, 7).unwrap().0,
        ),
        (
            "ta_stoch_slowd(high, low, close, 5, 3, 7, 3, 7)",
            stoch(&high, &low, &close, 5, 3, 7, 3, 7).unwrap().1,
        ),
        (
            "ta_stoch_slowk(high, low, close, 5, 3, 7, 3, 0)",
            stoch(&high, &low, &close, 5, 3, 7, 3, 0).unwrap().0,
        ),
        (
            "ta_stoch_slowd(high, low, close, 5, 3, 7, 3, 0)",
            stoch(&high, &low, &close, 5, 3, 7, 3, 0).unwrap().1,
        ),
        (
            "ta_stochf_fastk(high, low, close, 5, 3, 7)",
            stochf(&high, &low, &close, 5, 3, 7).unwrap().0,
        ),
        (
            "ta_stochf_fastd(high, low, close, 5, 3, 7)",
            stochf(&high, &low, &close, 5, 3, 7).unwrap().1,
        ),
        // WG5 sweep-up: NATR (H/L/C + period), BETA (two-series + period), and the four no-period
        // O/H/L/C price transforms.
        (
            "ta_natr(high, low, close, 14)",
            natr(&high, &low, &close, 14).unwrap(),
        ),
        ("ta_beta(high, low, 5)", beta(&high, &low, 5).unwrap()),
        (
            "ta_avgprice(open, high, low, close)",
            avgprice(&open, &high, &low, &close).unwrap(),
        ),
        ("ta_medprice(high, low)", medprice(&high, &low).unwrap()),
        (
            "ta_typprice(high, low, close)",
            typprice(&high, &low, &close).unwrap(),
        ),
        (
            "ta_wclprice(high, low, close)",
            wclprice(&high, &low, &close).unwrap(),
        ),
    ];
    for (expr, kernel) in cases {
        let engine = window_column(&session, &format!("{expr} OVER (ORDER BY ts)")).await;
        assert_bit_exact(expr, &engine, &kernel);
    }
}

#[tokio::test]
async fn sql_route_parked_four_match_the_kernel() {
    // T3 — the parked four through the SQL OVER route, `to_bits`-identical to the kernels. MAMA is
    // split (ta_mama / ta_fama) and carries real-valued limits; SAR / SAREXT are two-series
    // (high, low) with real-valued scalars; MAVP's second column is the `periods` series (not a
    // scalar), and matype 0 (SMA) + 1 (EMA) exercise both the windowed and the shifted-seed paths.
    let (session, _open, high, low, close) = session_with_bars();
    let periods = fixture("fixture_periods");
    let (mama_out, fama_out) = mama(&close, 0.5, 0.05).unwrap();
    let cases: Vec<(&str, Vec<f64>)> = vec![
        ("ta_mama(close, 0.5, 0.05)", mama_out),
        ("ta_fama(close, 0.5, 0.05)", fama_out),
        (
            "ta_sar(high, low, 0.02, 0.2)",
            sar(&high, &low, 0.02, 0.2).unwrap(),
        ),
        (
            "ta_sarext(high, low, 0.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2)",
            sarext(&high, &low, 0.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2).unwrap(),
        ),
        (
            "ta_mavp(close, periods, 5, 20, 0)",
            mavp(&close, &periods, 5, 20, 0).unwrap(),
        ),
        (
            "ta_mavp(close, periods, 5, 20, 1)",
            mavp(&close, &periods, 5, 20, 1).unwrap(),
        ),
    ];
    for (expr, kernel) in cases {
        let engine = window_column(&session, &format!("{expr} OVER (ORDER BY ts)")).await;
        assert_bit_exact(expr, &engine, &kernel);
    }
}

#[tokio::test]
async fn sql_route_partition_by_scopes_the_series() {
    // Two interleaved symbols in one table; PARTITION BY must run the kernel per symbol, so each
    // symbol's output equals the kernel on that symbol's ordered closes alone.
    let close = fixture("fixture_close");
    let n = close.len().min(200);
    let close = &close[..n];
    // symbol A = even ts, symbol B = odd ts (interleaved), each carrying the same close series.
    let mut sym = Vec::with_capacity(n * 2);
    let mut ts = Vec::with_capacity(n * 2);
    let mut closes = Vec::with_capacity(n * 2);
    for (i, &c) in close.iter().enumerate() {
        let ts_value = i64::try_from(i).expect("ts fits i64");
        for symbol in ["A", "B"] {
            sym.push(symbol);
            ts.push(ts_value);
            closes.push(c);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("close", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(datafusion::arrow::array::StringArray::from(sym)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(Float64Array::from(closes)),
        ],
    )
    .expect("batch");
    let session = spark_session();
    session
        .create_or_replace_temp_view("multi", vec![batch])
        .expect("register");

    let batches = session
        .sql(
            "SELECT ta_ema(close, 10) OVER (PARTITION BY sym ORDER BY ts) AS v \
             FROM multi WHERE sym = 'A' ORDER BY ts",
        )
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
            .expect("Float64");
        for i in 0..column.len() {
            engine.push(if column.is_null(i) {
                f64::NAN
            } else {
                column.value(i)
            });
        }
    }
    let kernel = ema(close, 10).unwrap();
    assert_bit_exact("ta_ema PARTITION BY sym", &engine, &kernel);
}

#[tokio::test]
async fn sql_route_multi_batch_partition_matches_the_kernel() {
    // Residual (d): a partition larger than DataFusion's 8192-row batch size, supplied as several
    // input RecordBatches, so the window operator must assemble the whole ordered partition across
    // batch boundaries before the stateful kernel runs. The engine output must still be
    // `to_bits`-identical to the kernel on the full series — proving no per-batch truncation.
    const N: usize = 12_000; // > 8192, and split into 3 physical batches below.
    // A deterministic positive series (a small drift + bounded oscillation), reproducible with no
    // RNG so the fixture is stable across runs.
    #[allow(clippy::cast_precision_loss)]
    let close: Vec<f64> = (0..N)
        .map(|i| {
            let x = i as f64;
            100.0 + 0.001 * x + (x * 0.01).sin() * 5.0 + f64::from(u32::try_from(i % 13).unwrap())
        })
        .collect();
    let ts: Vec<i64> = (0..N)
        .map(|i| i64::try_from(i).expect("ts fits i64"))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("close", DataType::Float64, false),
    ]));
    // Three physical batches (4000 rows each) → the input is genuinely multi-RecordBatch.
    let mut batches = Vec::new();
    for chunk in 0..3 {
        let lo = chunk * 4000;
        let hi = lo + 4000;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts[lo..hi].to_vec())),
                Arc::new(Float64Array::from(close[lo..hi].to_vec())),
            ],
        )
        .expect("batch");
        batches.push(batch);
    }
    let session = spark_session();
    session
        .create_or_replace_temp_view("big", batches)
        .expect("register big");

    let engine = {
        let sql = "SELECT ta_ema(close, 21) OVER (ORDER BY ts) AS v FROM big ORDER BY ts";
        let collected = session
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let mut out = Vec::with_capacity(N);
        for batch in &collected {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("v is Float64");
            for i in 0..column.len() {
                out.push(if column.is_null(i) {
                    f64::NAN
                } else {
                    column.value(i)
                });
            }
        }
        out
    };
    assert_eq!(
        engine.len(),
        N,
        "engine returned every row of the big partition"
    );
    let kernel = ema(&close, 21).unwrap();
    assert_bit_exact("ta_ema over a 12k multi-batch partition", &engine, &kernel);
}

#[tokio::test]
async fn sql_route_rejects_a_non_literal_period() {
    // The scalar period must be a constant literal — a column reference is a plan error, not a
    // silently-wrong result.
    let (session, _open, _high, _low, _close) = session_with_bars();
    let result = session
        .sql("SELECT ta_ema(close, ts) OVER (ORDER BY ts) AS v FROM bars")
        .await;
    let Err(err) = result else {
        // Some engines defer to collect(); force execution if planning succeeded.
        let collected = result.unwrap().collect().await;
        assert!(collected.is_err(), "expected a non-literal period to error");
        return;
    };
    let message = err.to_string();
    assert!(
        message.contains("literal") || message.contains("ta_ema"),
        "unexpected error: {message}"
    );
}

// =================================================================================================
// TA-1 — SQL same-OVER WindowAggExec fusion (Spark door). Plan-shape only; no kernel edits.
// =================================================================================================

/// Four independent TA windows sharing one named `WINDOW w` (same PARTITION BY / ORDER BY).
const SAME_NAMED_OVER_SQL: &str = "\
SELECT \
  ta_ema(close, 5) OVER w AS ema5, \
  ta_sma(close, 10) OVER w AS sma10, \
  ta_rsi(close, 14) OVER w AS rsi14, \
  ta_mom(close, 10) OVER w AS mom10 \
FROM (SELECT ts, close, CAST(1 AS BIGINT) AS sym FROM bars) bars_part \
WINDOW w AS (PARTITION BY sym ORDER BY ts)";

/// Same four windows with the spec repeated inline (fusion is not named-WINDOW-only).
const SAME_INLINE_OVER_SQL: &str = "\
SELECT \
  ta_ema(close, 5) OVER (PARTITION BY sym ORDER BY ts) AS ema5, \
  ta_sma(close, 10) OVER (PARTITION BY sym ORDER BY ts) AS sma10, \
  ta_rsi(close, 14) OVER (PARTITION BY sym ORDER BY ts) AS rsi14, \
  ta_mom(close, 10) OVER (PARTITION BY sym ORDER BY ts) AS mom10 \
FROM (SELECT ts, close, CAST(1 AS BIGINT) AS sym FROM bars) bars_part";

/// Window → filter on an input column → window. Both outputs stay live so dead-code
/// elimination cannot drop the first `WindowAggExec` and fake a fused count of 1.
const INTERVENING_INPUT_FILTER_SQL: &str = "\
SELECT ema5, \
       ta_sma(close, 10) OVER (PARTITION BY sym ORDER BY ts) AS sma10 \
FROM ( \
  SELECT * FROM ( \
    SELECT ts, close, CAST(1 AS BIGINT) AS sym, \
           ta_ema(close, 5) OVER (PARTITION BY CAST(1 AS BIGINT) ORDER BY ts) AS ema5 \
    FROM bars \
  ) first_window \
  WHERE close > 0 \
) filtered";

/// Window → filter on the first window's output → window. Same live-output rule as above.
const INTERVENING_OUTPUT_FILTER_SQL: &str = "\
SELECT ema5, \
       ta_sma(close, 10) OVER (PARTITION BY sym ORDER BY ts) AS sma10 \
FROM ( \
  SELECT * FROM ( \
    SELECT ts, close, CAST(1 AS BIGINT) AS sym, \
           ta_ema(close, 5) OVER (PARTITION BY CAST(1 AS BIGINT) ORDER BY ts) AS ema5 \
    FROM bars \
  ) first_window \
  WHERE ema5 IS NOT NULL \
) filtered";

/// Indent-display of the physical plan (the N2 `WindowAggExec` token lives here).
async fn physical_plan_text(session: &ReparkSession, sql: &str) -> String {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("must plan `{sql}`: {error}"));
    let plan = frame
        .create_physical_plan()
        .await
        .unwrap_or_else(|error| panic!("physical plan `{sql}`: {error}"));
    datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string()
}

/// `EXPLAIN` physical-plan body (Utf8 `plan` column, `plan_type = physical_plan` only).
async fn explain_physical_plan_text(session: &ReparkSession, sql: &str) -> String {
    let batches = session
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|error| panic!("EXPLAIN must plan `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("EXPLAIN collect `{sql}`: {error}"));
    let mut physical = String::new();
    for batch in &batches {
        let plan_types = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("EXPLAIN plan_type is Utf8");
        let plans = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("EXPLAIN plan is Utf8");
        for row in 0..batch.num_rows() {
            if plan_types.value(row) == "physical_plan" {
                physical.push_str(plans.value(row));
            }
        }
    }
    assert!(
        !physical.is_empty(),
        "EXPLAIN must emit a physical_plan row for `{sql}`"
    );
    physical
}

fn window_agg_exec_count(plan: &str) -> usize {
    plan.matches("WindowAggExec").count()
}

/// N2 mechanic on the SQL door: both `create_physical_plan` and `EXPLAIN` must agree.
async fn assert_window_agg_exec_count(
    session: &ReparkSession,
    sql: &str,
    expected: usize,
    required_tokens: &[&str],
) {
    let physical = physical_plan_text(session, sql).await;
    let explained = explain_physical_plan_text(session, sql).await;
    let from_plan = window_agg_exec_count(&physical);
    let from_explain = window_agg_exec_count(&explained);
    assert_eq!(
        from_plan, expected,
        "create_physical_plan WindowAggExec count {from_plan} != {expected}\n{physical}"
    );
    assert_eq!(
        from_explain, expected,
        "EXPLAIN physical_plan WindowAggExec count {from_explain} != {expected}\n{explained}"
    );
    for token in required_tokens {
        assert!(
            physical.contains(token),
            "physical plan dropped `{token}` (DCE would fake a fused count):\n{physical}"
        );
        assert!(
            explained.contains(token),
            "EXPLAIN physical_plan dropped `{token}`:\n{explained}"
        );
    }
}

#[tokio::test]
async fn sql_same_named_over_window_fuses_to_one_window_agg_exec() {
    // Perf-note idea 11: many `ta_*(…) OVER w` sharing PARTITION BY / ORDER BY must plan
    // one WindowAggExec. DataFrame-door fusion is N2; this is the Spark-door SQL pin.
    let (session, _open, _high, _low, _close) = session_with_bars();
    assert_window_agg_exec_count(
        &session,
        SAME_NAMED_OVER_SQL,
        1,
        &["ta_ema", "ta_sma", "ta_rsi", "ta_mom"],
    )
    .await;
}

#[tokio::test]
async fn sql_same_inline_over_spec_fuses_to_one_window_agg_exec() {
    // Same claim without the named WINDOW clause — fusion is the shared spec, not the spelling.
    let (session, _open, _high, _low, _close) = session_with_bars();
    assert_window_agg_exec_count(
        &session,
        SAME_INLINE_OVER_SQL,
        1,
        &["ta_ema", "ta_sma", "ta_rsi", "ta_mom"],
    )
    .await;
}

#[tokio::test]
async fn sql_intervening_filter_between_windows_stacks_window_agg_exec() {
    // Measured truth (2026-08-15, freeze cd0db4f): an intervening filter between two *live*
    // windows stacks (2 WindowAggExec). Predicate pushdown of `close > 0` does not re-fuse
    // the two logical WindowAggr nodes. A filter that does not keep `ema5` live is not this
    // pin — unused first-window output is eliminated and the count collapses to 1 by DCE.
    let (session, _open, _high, _low, _close) = session_with_bars();
    assert_window_agg_exec_count(
        &session,
        INTERVENING_INPUT_FILTER_SQL,
        2,
        &["ta_ema", "ta_sma"],
    )
    .await;
    assert_window_agg_exec_count(
        &session,
        INTERVENING_OUTPUT_FILTER_SQL,
        2,
        &["ta_ema", "ta_sma"],
    )
    .await;
}
