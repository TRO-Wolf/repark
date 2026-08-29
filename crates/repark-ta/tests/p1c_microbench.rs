//! BBANDS three-sibling cost versus one kernel run.
//!
//! Run: `cargo test -p repark-ta --release --test p1c_microbench -- --nocapture`
//! This is a wall-time measurement, not a correctness gate. It models the kernel-level
//! multi-output cache: one run, three independent runs, and one run plus clones.

use std::time::Instant;

use repark_ta::bbands;

fn walk(n: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(n);
    let mut price = 100.0_f64;
    for index in 0..n {
        let phase_steps = u32::try_from(index % 10_000).unwrap_or(0);
        let phase = f64::from(phase_steps) / 10_000.0;
        price *= 1.0 + (phase - 0.5) * 0.002;
        out.push(price);
    }
    out
}

fn mean_ms(samples: &[f64]) -> f64 {
    let count = u32::try_from(samples.len()).unwrap_or(1);
    samples.iter().sum::<f64>() / f64::from(count)
}

#[test]
fn hour0_bbands_three_vs_one_1e6() {
    let n = 1_000_000_usize;
    let close = walk(n);
    let period = 20_usize;
    let nbdev_up = 2.0;
    let nbdev_dn = 2.0;
    let warmup = 3;
    let rounds = 15;

    for _ in 0..warmup {
        let _ = bbands(&close, period, nbdev_up, nbdev_dn).expect("bbands");
    }

    let mut one = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let start = Instant::now();
        let (upper, middle, lower) = bbands(&close, period, nbdev_up, nbdev_dn).expect("bbands");
        std::hint::black_box((upper.last(), middle.last(), lower.last()));
        one.push(start.elapsed().as_secs_f64() * 1e3);
    }

    let mut three = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let start = Instant::now();
        let (upper, _, _) = bbands(&close, period, nbdev_up, nbdev_dn).expect("upper");
        let (_, middle, _) = bbands(&close, period, nbdev_up, nbdev_dn).expect("middle");
        let (_, _, lower) = bbands(&close, period, nbdev_up, nbdev_dn).expect("lower");
        std::hint::black_box((upper.last(), middle.last(), lower.last()));
        three.push(start.elapsed().as_secs_f64() * 1e3);
    }

    let mut ideal_cached = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let start = Instant::now();
        let (upper, middle, lower) = bbands(&close, period, nbdev_up, nbdev_dn).expect("bbands");
        let upper_hit = upper.clone();
        let middle_hit = middle.clone();
        let lower_hit = lower.clone();
        std::hint::black_box((upper_hit.last(), middle_hit.last(), lower_hit.last()));
        ideal_cached.push(start.elapsed().as_secs_f64() * 1e3);
    }

    let one_ms = mean_ms(&one);
    let three_ms = mean_ms(&three);
    let ideal_ms = mean_ms(&ideal_cached);
    let ratio = three_ms / one_ms;
    let ideal_ratio = ideal_ms / one_ms;
    eprintln!("P1c microbench BBANDS n={n} period={period} rounds={rounds}");
    eprintln!("  one kernel (3 outs):     mean={one_ms:.3} ms");
    eprintln!("  three kernels (pre-#8):  mean={three_ms:.3} ms  ratio={ratio:.3}x");
    eprintln!("  ideal cached (#8):       mean={ideal_ms:.3} ms  ratio={ideal_ratio:.3}x");
    assert!(
        three_ms > one_ms * 1.5,
        "hour-0 expectation: 3x independent bbands >> 1x (got three={three_ms} one={one_ms})"
    );
    assert!(
        ideal_ms < three_ms * 0.6,
        "ideal #8 shape should beat three independent runs (ideal={ideal_ms} three={three_ms})"
    );
}
