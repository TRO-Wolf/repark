# Delta 3's batch-add micro-benchmark (2026-11-19)

How the mechanism was chosen before the build. A standalone `rustc 1.96.0 -O -C codegen-units=1` program (std only,
no crate), one lane-B core (`taskset -c 48`), the cost data's `d10` and `d38` value shapes. Each kernel runs over one
cache-resident 8,192-row batch 1,221 times (10,002,432 rows), as a query's decoded batch would be; three rounds.

| kernel | `d10` values | `d38` values |
|---|---|---|
| Arrow's wrapping `sum` (a plain fold, as `arrow-arith` 58.4.1 does for non-null integers) | 2.91 ms | 2.92 ms |
| four-lane checked sum (**chosen**: `exact_total`) | 3.41 ms | 3.41 ms |
| four-lane overflowing sum with a flag | 4.15 ms | 4.15 ms |
| one-lane overflowing sum with a flag | 5.03 ms | 5.03 ms |
| one-lane checked sum (Arrow's `sum_checked`, used here only for a batch with nulls) | 5.46 ms | 5.46 ms |
| `min` + `max` + `sum` (the old path for `p ≥ 35`; the brief's first option on every batch) | 13.8 ms | — |

Over 160 MB of data that misses the cache (each batch read once) the kernels are memory-bound: wrapping 11.5 ms,
one-lane checked 12.7 ms, `min` + `max` + `sum` 20.2 ms.

```rust
use std::hint::black_box;
use std::time::Instant;

#[inline(never)]
fn wrapping(values: &[i128]) -> i128 {
    values.iter().fold(0_i128, |a, v| a.wrapping_add(*v))
}

#[inline(never)]
fn checked(values: &[i128]) -> Option<i128> {
    values.iter().try_fold(0_i128, |a, v| a.checked_add(*v))
}

#[inline(never)]
fn checked_lanes(values: &[i128]) -> Option<i128> {
    let mut acc = [0_i128; 4];
    let mut chunks = values.chunks_exact(4);
    for chunk in &mut chunks {
        for (a, v) in acc.iter_mut().zip(chunk) {
            *a = a.checked_add(*v)?;
        }
    }
    let mut total = chunks.remainder().iter().try_fold(0_i128, |a, v| a.checked_add(*v))?;
    for a in acc {
        total = total.checked_add(a)?;
    }
    Some(total)
}

#[inline(never)]
fn flagged_lanes(values: &[i128]) -> Option<i128> {
    let mut acc = [0_i128; 4];
    let mut flag = false;
    let mut chunks = values.chunks_exact(4);
    for chunk in &mut chunks {
        for (a, v) in acc.iter_mut().zip(chunk) {
            let (s, o) = a.overflowing_add(*v);
            *a = s;
            flag |= o;
        }
    }
    let mut total = 0_i128;
    for v in chunks.remainder().iter().chain(acc.iter()) {
        let (s, o) = total.overflowing_add(*v);
        total = s;
        flag |= o;
    }
    (!flag).then_some(total)
}

#[inline(never)]
fn flagged(values: &[i128]) -> Option<i128> {
    let mut total = 0_i128;
    let mut flag = false;
    for v in values {
        let (s, o) = total.overflowing_add(*v);
        total = s;
        flag |= o;
    }
    (!flag).then_some(total)
}

fn main() {
    let wide = std::env::args().nth(1).is_some();
    let data: Vec<i128> = (0..8192_i128)
        .map(|id| {
            let v = (id * 7919) % 99_999_999;
            if wide { v * 1_000_000_000_000_000_000 } else { v }
        })
        .collect();
    let batches: Vec<&[i128]> = (0..1221).map(|_| &data[..]).collect();
    for round in 0..3 {
        let mut line = format!("round {round} wide={wide}");
        let t = Instant::now();
        let mut s = 0_i128;
        for b in &batches { s = s.wrapping_add(wrapping(black_box(b))); }
        line += &format!(" wrapping {:?}", t.elapsed());
        let t = Instant::now();
        for b in &batches { s = s.wrapping_add(checked(black_box(b)).unwrap_or(0)); }
        line += &format!(" checked {:?}", t.elapsed());
        let t = Instant::now();
        for b in &batches { s = s.wrapping_add(checked_lanes(black_box(b)).unwrap_or(0)); }
        line += &format!(" checked_lanes {:?}", t.elapsed());
        let t = Instant::now();
        for b in &batches { s = s.wrapping_add(flagged_lanes(black_box(b)).unwrap_or(0)); }
        line += &format!(" flagged_lanes {:?}", t.elapsed());
        let t = Instant::now();
        for b in &batches { s = s.wrapping_add(flagged(black_box(b)).unwrap_or(0)); }
        line += &format!(" flagged {:?} {}", t.elapsed(), s & 1);
        println!("{line}");
    }
}
```

The `min` + `max` + `sum` row comes from the same program's sibling (`hot.rs`), whose kernel is:

```rust
fn min_max(values: &[i128]) -> (i128, i128) {
    let lo = values.iter().fold(i128::MAX, |a, v| a.min(*v));
    let hi = values.iter().fold(i128::MIN, |a, v| a.max(*v));
    (lo, hi)
}
```

followed by the wrapping fold.
