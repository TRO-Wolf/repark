# Unit ledger — P-1 criterion TA kernel baseline

**Unit:** P-1 · conductor-14 Track T1 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-p1` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-p1` · **Branch:** `grok/p1-ta-kernel-benches` ·
**Base (FROZEN):** `a5d2d98a449815891016923594c8b1dcd4ae3b43` (#130 on origin/main).

**Charter:** `planning/grok/BRIEF-perf-wave-14.md` P-1 + conductor-14 Addendum
A3 (crate-local `criterion` matching `repark-functions`; NEW
`crates/repark-ta/benches/**` + map.md; no `src/` edits; Cargo.lock delta
only if criterion features force it). **SEPMO:** acc. Floor S1. Risk tier:
mechanical (measure-only).

CLOSED: `crates/repark-ta/src/**`, STATUS, registry, `.github/`,
`[patch.crates-io]`, board, primary checkout, P-2 Python benches, P-3
flamegraph / unsafe ceiling (orchestrator, already in flight).

## Charge

Record a criterion baseline for the public kernel entry points:

1. `ema` / `sma` / `rsi` / `bbands` + volume `ad` / `adosc` / `obv` / `mfi`
   at n=1e6, null-free `f64`, pre-sorted walk. Per-kernel ns/row.
2. Multi-output cache path: BBANDS upper/middle/lower as three independent
   calls vs a cold single call vs the p1c ideal-cached clone shape.
   Extend `tests/p1c_microbench.rs` as criterion benches — do **not** copy
   those cases into this file as `#[test]`.

Numbers land planning-side
(`planning/hardening/BENCH-BASELINE-2026-08.md`). The repo gets bench CODE
only.

## What landed

| Artifact | Path | Role |
|---|---|---|
| Criterion bench | `crates/repark-ta/benches/ta_kernels.rs` | kernel + BBANDS cache-shape |
| Bench map | `crates/repark-ta/benches/map.md` | new directory map |
| Parent row | `crates/repark-ta/map.md` | P-1 benches row + Cargo.toml criterion |
| Crate pin | `crates/repark-ta/Cargo.toml` | criterion 0.8 + `[[bench]] ta_kernels` |
| This ledger | `task/p1-ta-kernel-benches-ledger.md` | unit record |

No `src/` edit. No engine instrumentation.

## Bench names

| Criterion id | Kernel / shape |
|---|---|
| `sma_n1e6` | `sma(close, 20)` |
| `ema_n1e6` | `ema(close, 21)` |
| `rsi_n1e6` | `rsi(close, 14)` |
| `ad_n1e6` | `ad(high, low, close, volume)` |
| `adosc_n1e6` | `adosc(..., 3, 10)` |
| `obv_n1e6` | `obv(close, volume)` |
| `mfi_n1e6` | `mfi(..., 14)` |
| `bbands_cold_n1e6` | one `bbands` (three live outputs) |
| `bbands_three_sibling_n1e6` | three independent `bbands` (pre-#8) |
| `bbands_cache_hit_shape_n1e6` | one `bbands` + clone three bands |

Machine-readable stderr: `TA_KERNEL name=… n=… params=… median_ns=… ns_per_row=…`
and `TA_KERNEL_RATIO subject=… baseline=… ratio=…`. Warm-up 3 + 15-iteration
median, matching p1c. No wall-time assert (bench runtime is not a gate).

## FINDINGS

### F-P1-1 — UDF multi-output cache has no public hit/miss counters

The thread-local multi-output cache (scout #8) lives in
`crates/repark-ta/src/udf.rs` behind feature `datafusion`. There is no public
hit/miss counter and no kernel-level cache API. Instrumenting that path would
be a `src/` edit, which this unit forbids.

P-1 therefore measures the **p1c proxy** on the public kernel API:

- cold = one `bbands` call
- three-sibling = three independent full-kernel runs (cache-miss / pre-#8)
- cache-hit shape = one kernel + clone the three `Vec<f64>` bands

The live UDF `evaluate_all` path (and any real TLS hit rate) is P-2 / P-3.

## ACC

- Risk tier: mechanical. Measure-only. Floor S1.
- No engine surface. No AWS. No `unsafe`.
- Label: `ACC-CONVERGED`.

## Gates (real exit codes)

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make preflight` | **0** |
| `make py-test-facade` (inside preflight) | **0** (3214 passed, 71 skipped) |
| `make audit` | **0** |
| `make workflows-lint` | **0** |
| pre-commit hook | fires (`check_map_md` / crate-dag / lib-rs / file-size / lib-py / manifest / fmt / taplo / typos) |

`cargo bench -p repark-ta --bench ta_kernels -- --quick` is **not** a gate;
it is the capture invocation for the planning numbers doc. Numbers
transcribed to `planning/hardening/BENCH-BASELINE-2026-08.md` (not in-repo).
