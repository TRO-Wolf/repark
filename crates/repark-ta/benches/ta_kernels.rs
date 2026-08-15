//! P-1 criterion kernel baseline — MEASURE ONLY (perf-wave-14).
//!
//! Per-kernel ns/row at n=1e6, null-free `f64`, pre-sorted walk. Kernels:
//! `ema`/`sma`/`rsi`/`bbands` plus volume `ad`/`adosc`/`obv`/`mfi`.
//!
//! Multi-output path extends the `p1c_microbench` convention as criterion benches
//! (not `#[test]`): BBANDS cold single call vs three independent sibling runs vs
//! the cache-hit shape (one kernel + clone the three bands). The UDF TLS cache
//! lives in `src/udf.rs` and is not instrumented here — see the unit ledger.
//!
//! Run: `cargo bench -p repark-ta --bench ta_kernels -- --quick`
//! Machine-readable lines: `TA_KERNEL …` and `TA_KERNEL_RATIO …` on stderr.

use std::hint::black_box;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use repark_ta::{ad, adosc, bbands, ema, mfi, obv, rsi, sma};

/// Rows per kernel — large enough that arithmetic dominates call overhead.
const ROW_COUNT: usize = 1_000_000;
/// TA-Lib-default-ish periods (BBANDS matches `p1c_microbench`).
const SMA_PERIOD: usize = 20;
const EMA_PERIOD: usize = 21;
const RSI_PERIOD: usize = 14;
const BBANDS_PERIOD: usize = 20;
const BBANDS_NBDEV: f64 = 2.0;
const ADOSC_FAST: usize = 3;
const ADOSC_SLOW: usize = 10;
const MFI_PERIOD: usize = 14;
/// Warm-up + N-iteration median (same shape as `p1c_microbench`).
const WARMUP: u32 = 3;
const ROUNDS: u32 = 15;

/// Pre-sorted OHLC+V fixture: index is time; no nulls.
struct Fixture {
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    volume: Vec<f64>,
}

impl Fixture {
    fn new(row_count: usize) -> Self {
        let close = walk(row_count);
        let mut high = Vec::with_capacity(row_count);
        let mut low = Vec::with_capacity(row_count);
        let mut volume = Vec::with_capacity(row_count);
        for (index, &price) in close.iter().enumerate() {
            let phase_steps = u32::try_from(index % 10_000).unwrap_or(0);
            let phase = f64::from(phase_steps) / 10_000.0;
            // Always high > close > low for this positive walk.
            let half_range = 0.25 + phase * 0.5;
            high.push(price + half_range);
            low.push(price - half_range);
            volume.push(1_000.0 + phase * 9_000.0);
        }
        Self {
            high,
            low,
            close,
            volume,
        }
    }
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| Fixture::new(ROW_COUNT))
}

fn walk(row_count: usize) -> Vec<f64> {
    // Deterministic lognormal-ish walk; values only, no nulls. Same generator as
    // `tests/p1c_microbench.rs` so the BBANDS floor is comparable.
    let mut out = Vec::with_capacity(row_count);
    let mut price = 100.0_f64;
    for index in 0..row_count {
        let phase_steps = u32::try_from(index % 10_000).unwrap_or(0);
        let phase = f64::from(phase_steps) / 10_000.0;
        price *= 1.0 + (phase - 0.5) * 0.002;
        out.push(price);
    }
    out
}

fn nanos_per_row(median: Duration, row_count: usize) -> f64 {
    let rows = f64::from(u32::try_from(row_count).unwrap_or(u32::MAX));
    if rows <= 0.0 {
        return f64::INFINITY;
    }
    median.as_secs_f64() * 1.0e9 / rows
}

fn median_wall<F>(mut run: F) -> Duration
where
    F: FnMut(),
{
    for _ in 0..WARMUP {
        run();
    }
    let mut samples = Vec::with_capacity(ROUNDS as usize);
    for _ in 0..ROUNDS {
        let start = Instant::now();
        run();
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn report(name: &str, params: &str, row_count: usize, median: Duration) {
    let ns_per_row = nanos_per_row(median, row_count);
    let median_ns = median.as_secs_f64() * 1.0e9;
    eprintln!(
        "TA_KERNEL name={name} n={row_count} params={params} \
         median_ns={median_ns:.0} ns_per_row={ns_per_row:.3}"
    );
}

fn report_ratio(subject: &str, baseline: &str, subject_wall: Duration, baseline_wall: Duration) {
    let denom = baseline_wall.as_secs_f64();
    let ratio = if denom <= f64::EPSILON {
        f64::INFINITY
    } else {
        subject_wall.as_secs_f64() / denom
    };
    eprintln!("TA_KERNEL_RATIO subject={subject} baseline={baseline} ratio={ratio:.3}");
}

/// ===========================================================================================
/// Overlap + momentum kernels (`sma` / `ema` / `rsi`) plus `sma` as the ratio baseline.
/// ===========================================================================================
fn bench_overlap_momentum(criterion: &mut Criterion) {
    let data = fixture();
    let close = data.close.as_slice();

    criterion.bench_function("sma_n1e6", |bencher| {
        bencher.iter(|| {
            let out = sma(black_box(close), black_box(SMA_PERIOD)).expect("sma");
            black_box(out);
        });
    });
    criterion.bench_function("ema_n1e6", |bencher| {
        bencher.iter(|| {
            let out = ema(black_box(close), black_box(EMA_PERIOD)).expect("ema");
            black_box(out);
        });
    });
    criterion.bench_function("rsi_n1e6", |bencher| {
        bencher.iter(|| {
            let out = rsi(black_box(close), black_box(RSI_PERIOD)).expect("rsi");
            black_box(out);
        });
    });

    let sma_wall = median_wall(|| {
        black_box(sma(close, SMA_PERIOD).expect("sma"));
    });
    let ema_wall = median_wall(|| {
        black_box(ema(close, EMA_PERIOD).expect("ema"));
    });
    let rsi_wall = median_wall(|| {
        black_box(rsi(close, RSI_PERIOD).expect("rsi"));
    });
    report("sma", "period=20", ROW_COUNT, sma_wall);
    report("ema", "period=21", ROW_COUNT, ema_wall);
    report("rsi", "period=14", ROW_COUNT, rsi_wall);
    report_ratio("ema", "sma", ema_wall, sma_wall);
    report_ratio("rsi", "sma", rsi_wall, sma_wall);
}

/// ===========================================================================================
/// Volume-family kernels (`ad` / `adosc` / `obv` / `mfi`) on the same pre-sorted walk.
/// ===========================================================================================
fn bench_volume(criterion: &mut Criterion) {
    let data = fixture();
    let high = data.high.as_slice();
    let low = data.low.as_slice();
    let close = data.close.as_slice();
    let volume = data.volume.as_slice();

    criterion.bench_function("ad_n1e6", |bencher| {
        bencher.iter(|| {
            let out = ad(
                black_box(high),
                black_box(low),
                black_box(close),
                black_box(volume),
            )
            .expect("ad");
            black_box(out);
        });
    });
    criterion.bench_function("adosc_n1e6", |bencher| {
        bencher.iter(|| {
            let out = adosc(
                black_box(high),
                black_box(low),
                black_box(close),
                black_box(volume),
                black_box(ADOSC_FAST),
                black_box(ADOSC_SLOW),
            )
            .expect("adosc");
            black_box(out);
        });
    });
    criterion.bench_function("obv_n1e6", |bencher| {
        bencher.iter(|| {
            let out = obv(black_box(close), black_box(volume)).expect("obv");
            black_box(out);
        });
    });
    criterion.bench_function("mfi_n1e6", |bencher| {
        bencher.iter(|| {
            let out = mfi(
                black_box(high),
                black_box(low),
                black_box(close),
                black_box(volume),
                black_box(MFI_PERIOD),
            )
            .expect("mfi");
            black_box(out);
        });
    });

    let sma_wall = median_wall(|| {
        black_box(sma(close, SMA_PERIOD).expect("sma"));
    });
    let ad_wall = median_wall(|| {
        black_box(ad(high, low, close, volume).expect("ad"));
    });
    let adosc_wall = median_wall(|| {
        black_box(adosc(high, low, close, volume, ADOSC_FAST, ADOSC_SLOW).expect("adosc"));
    });
    let obv_wall = median_wall(|| {
        black_box(obv(close, volume).expect("obv"));
    });
    let mfi_wall = median_wall(|| {
        black_box(mfi(high, low, close, volume, MFI_PERIOD).expect("mfi"));
    });
    report("ad", "none", ROW_COUNT, ad_wall);
    report("adosc", "fast=3,slow=10", ROW_COUNT, adosc_wall);
    report("obv", "none", ROW_COUNT, obv_wall);
    report("mfi", "period=14", ROW_COUNT, mfi_wall);
    report_ratio("ad", "sma", ad_wall, sma_wall);
    report_ratio("adosc", "sma", adosc_wall, sma_wall);
    report_ratio("obv", "sma", obv_wall, sma_wall);
    report_ratio("mfi", "sma", mfi_wall, sma_wall);
}

/// ===========================================================================================
/// BBANDS multi-output path — p1c convention as criterion, not a cargo-test `#[test]`.
///
/// * `bbands_cold_n1e6` — one kernel, three live outputs.
/// * `bbands_three_sibling_n1e6` — three independent full runs (pre-#8 / cache-miss shape).
/// * `bbands_cache_hit_shape_n1e6` — one kernel + clone three bands (ideal #8 proxy).
///
/// ===========================================================================================
fn bench_bbands_cache_path(criterion: &mut Criterion) {
    let data = fixture();
    let close = data.close.as_slice();

    criterion.bench_function("bbands_cold_n1e6", |bencher| {
        bencher.iter(|| {
            let bands = bbands(
                black_box(close),
                black_box(BBANDS_PERIOD),
                black_box(BBANDS_NBDEV),
                black_box(BBANDS_NBDEV),
            )
            .expect("bbands");
            black_box(bands);
        });
    });
    criterion.bench_function("bbands_three_sibling_n1e6", |bencher| {
        bencher.iter(|| {
            let (upper, _, _) =
                bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("bbands upper");
            let (_, middle, _) =
                bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("bbands middle");
            let (_, _, lower) =
                bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("bbands lower");
            black_box((upper, middle, lower));
        });
    });
    criterion.bench_function("bbands_cache_hit_shape_n1e6", |bencher| {
        bencher.iter(|| {
            let (upper, middle, lower) =
                bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("bbands");
            let upper_hit = upper.clone();
            let middle_hit = middle.clone();
            let lower_hit = lower.clone();
            black_box((upper_hit, middle_hit, lower_hit));
        });
    });

    let sma_wall = median_wall(|| {
        black_box(sma(close, SMA_PERIOD).expect("sma"));
    });
    let cold = median_wall(|| {
        black_box(bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("bbands"));
    });
    let three = median_wall(|| {
        let (upper, _, _) =
            bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("upper");
        let (_, middle, _) =
            bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("middle");
        let (_, _, lower) =
            bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("lower");
        black_box((upper, middle, lower));
    });
    let cache_hit = median_wall(|| {
        let (upper, middle, lower) =
            bbands(close, BBANDS_PERIOD, BBANDS_NBDEV, BBANDS_NBDEV).expect("bbands");
        black_box((upper.clone(), middle.clone(), lower.clone()));
    });
    report("bbands_cold", "period=20,nbdev=2", ROW_COUNT, cold);
    report(
        "bbands_three_sibling",
        "period=20,nbdev=2",
        ROW_COUNT,
        three,
    );
    report(
        "bbands_cache_hit_shape",
        "period=20,nbdev=2,clone=3",
        ROW_COUNT,
        cache_hit,
    );
    report_ratio("bbands_cold", "sma", cold, sma_wall);
    report_ratio("bbands_three_sibling", "bbands_cold", three, cold);
    report_ratio("bbands_cache_hit_shape", "bbands_cold", cache_hit, cold);
}

criterion_group!(
    name = ta_kernel_benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    targets = bench_overlap_momentum, bench_volume, bench_bbands_cache_path
);
criterion_main!(ta_kernel_benches);
