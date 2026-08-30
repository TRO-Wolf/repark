# map — crates/repark-ta/benches

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Criterion **kernel** micro-benches for public `repark_ta` entry points. They are measure-only:
they do not edit `src/` or duplicate cargo tests. The harness prints per-kernel `ns/row` and
subject/`sma` ratios as `TA_KERNEL` / `TA_KERNEL_RATIO` lines; wall time has no committed ceiling.

## Contents

| File | What |
|---|---|
| [ta_kernels.rs](ta_kernels.rs) | `ema`/`sma`/`rsi`/`bbands` plus volume, Wilder, MACD, and statistic representatives at n=1e6; BBANDS one-run, three-run, and one-run-plus-clones shapes |
| [map.md](map.md) | this file |

## I want to…

| I want to… | Go to |
|---|---|
| Run the kernel baseline locally | `cargo bench -p repark-ta --bench ta_kernels -- --quick` |
| Compare BBANDS sibling tax vs one kernel | `bbands_three_sibling_n1e6` / `bbands_cold_n1e6` / `bbands_cache_hit_shape_n1e6` |
| Measure a construction change on one kernel | `cargo bench -p repark-ta --bench ta_kernels -- --save-baseline pre_<kernel> <kernel>_n1e6` then re-run with `--baseline pre_<kernel>` |
| Bench the Wilder / statistic sweep subjects | `trange_n1e6` / `atr_n1e6` / `adx_n1e6` / `macd_n1e6` / `linearreg_n1e6` / `stddev_n1e6` |
| See why the UDF TLS cache is not timed here | The bench measures kernels only; cache behavior is covered by UDF tests |

## Pointers

- Up: [../map.md](../map.md)
- Criterion pin: crate-level `Cargo.toml` dev-dep (never `[workspace.dependencies]`)
- p1c convention (test, not this bench): [../tests/p1c_microbench.rs](../tests/p1c_microbench.rs)
- Numbers stay planning-side; they are not committed here

## Debug

| Symptom | Check |
|---|---|
| `criterion` not found | crate-level `dev-dependencies` in `../Cargo.toml`; never workspace.dependencies |
| clippy disallowed_methods on benches | general gate uses `-A clippy::disallowed_methods`; benches may use expect |
| BBANDS three-sibling ≈ cold | each sibling must be a full independent `bbands` call; do not reuse one result |
| want UDF cache hit/miss | Cache behavior is covered by UDF tests; this bench exposes no cache counters |
