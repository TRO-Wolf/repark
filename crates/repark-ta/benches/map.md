# map — crates/repark-ta/benches

## Purpose

P-1 (perf-wave-14) criterion **kernel** micro-benches for the public `repark_ta`
entry points. Measure-only: no `src/` edits, no `unsafe`, no cargo-test `#[test]`
duplicates of `tests/p1c_microbench.rs`. Also the before/after harness for the
output-construction sweep (`--save-baseline` / `--baseline`): every kernel whose
construction is a candidate for `ema`'s single-write form must have a bench here
BEFORE it is touched. Absolute wall is runner-noise; the
harness prints per-kernel `ns/row` and subject/`sma` ratios as `TA_KERNEL` /
`TA_KERNEL_RATIO` lines. No committed ratio ceiling (this unit records a
baseline; it does not gate wall time).

## Contents

| File | What |
|---|---|
| [ta_kernels.rs](ta_kernels.rs) | `ema`/`sma`/`rsi`/`bbands` + volume `ad`/`adosc`/`obv`/`mfi` + Wilder `trange`/`atr`/`adx` + `macd`/`linearreg`/`stddev` at n=1e6; BBANDS cold vs three-sibling vs cache-hit shape |
| [map.md](map.md) | this file |

## I want to…

| I want to… | Go to |
|---|---|
| Run the kernel baseline locally | `cargo bench -p repark-ta --bench ta_kernels -- --quick` |
| Compare BBANDS sibling tax vs one kernel | `bbands_three_sibling_n1e6` / `bbands_cold_n1e6` / `bbands_cache_hit_shape_n1e6` |
| Measure a construction change on one kernel | `cargo bench -p repark-ta --bench ta_kernels -- --save-baseline pre_<kernel> <kernel>_n1e6` then re-run with `--baseline pre_<kernel>` |
| Bench the Wilder / statistic sweep subjects | `trange_n1e6` / `atr_n1e6` / `adx_n1e6` / `macd_n1e6` / `linearreg_n1e6` / `stddev_n1e6` |
| See why the UDF TLS cache is not timed here | [../../../task/p1-ta-kernel-benches-ledger.md](../../../task/p1-ta-kernel-benches-ledger.md) FINDING F-P1-1 |

## Pointers

- Up: [../map.md](../map.md)
- Criterion pin: crate-level `Cargo.toml` dev-dep (never `[workspace.dependencies]`);
  feature set matches [../../repark-functions/benches/map.md](../../repark-functions/benches/map.md)
- p1c convention (test, not this bench): [../tests/p1c_microbench.rs](../tests/p1c_microbench.rs)
- Ledger: [../../../task/p1-ta-kernel-benches-ledger.md](../../../task/p1-ta-kernel-benches-ledger.md)
- Numbers stay planning-side (`planning/hardening/BENCH-BASELINE-2026-08.md`); not in-repo

## Debug

| Symptom | Check |
|---|---|
| `criterion` not found | crate-level `dev-dependencies` in `../Cargo.toml`; never workspace.dependencies |
| clippy disallowed_methods on benches | general gate uses `-A clippy::disallowed_methods`; benches may use expect |
| BBANDS three-sibling ≈ cold | each sibling must be a full independent `bbands` call; do not reuse one result |
| want UDF cache hit/miss | those counters do not exist — FINDING, not a src/ edit |
