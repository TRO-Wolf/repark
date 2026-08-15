# Unit ledger — P-2 Python TA pipeline benches

**Unit:** P-2 · conductor-14 Track T1 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-p1` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-p1` · **Branch:** `grok/p2-ta-pipeline-benches` ·
**Base (FROZEN):** `a5d2d98a449815891016923594c8b1dcd4ae3b43`.

**Charter:** `BRIEF-perf-wave-14.md` P-2 + conductor-14 Addendum A3/A12.
**SEPMO:** acc. Floor S1. Measure-only.

CLOSED: `crates/**/src`, STATUS, registry, `.github/`, `[patch]`, lockfiles,
board, primary, `python/repark/tests/test_merge_differential_parity.py`
(Opus O-2 fence), P-3 flamegraphs / `unsafe`. P-1 lives on
`grok/p1-ta-kernel-benches` (#132) — this tree does not touch it.

## Intent

Six Python bench scripts under `python/repark-parity/bench/ta/` covering
perf-note §8.1–8.5 and §8.7. §8.6 SQL same-OVER EXPLAIN is **#116** (TA-1)
— cited, not rebuilt. Shared `harness.py`. Deterministic p1c walk; warm-up +
N-iteration median; one-line `TA_PIPELINE` results. `--quick` / n=1e5 path
for the time box. Numbers transcribed to planning-side
`BENCH-BASELINE-2026-08.md` only.

`polars_talib` 0.1.5 is reused from the parity record / PEP-723 env. Not
added to the main package.

## What shipped

| Artifact | Path |
|---|---|
| Shared fixture | `python/repark-parity/bench/ta/harness.py` |
| §8.1 kernel race | `python/repark-parity/bench/ta/bench_kernel_race.py` |
| §8.2 many symbols | `python/repark-parity/bench/ta/bench_many_symbols.py` |
| §8.3 wide serving | `python/repark-parity/bench/ta/bench_wide_serving.py` |
| §8.4 batch_size | `python/repark-parity/bench/ta/bench_batch_size.py` |
| §8.5 null_lookback | `python/repark-parity/bench/ta/bench_null_lookback.py` |
| §8.7 last-row | `python/repark-parity/bench/ta/bench_last_row.py` |
| Bench map | `python/repark-parity/bench/ta/map.md` |
| Parent row | `python/repark-parity/bench/map.md` (this lane only) |
| This ledger | `task/p2-ta-pipeline-benches-ledger.md` + `task/map.md` row |

## FINDING F-P2-1

Raw `repark_ta` is not reachable from Python (no kernel binding). The
`repark_ta_raw` leg prints `reachable=false reason=no_python_module_cite_PR132_criterion`.
P-1 #132 remains the raw-kernel number.

## FINDING F-P2-2

UDF TLS cache hit/miss counters are still unpublished (F-P1-1). §8.3
records `WindowAggExec` + window-fn **token** counts, not cache counters.
Instrumenting the cache is a later engine slate, not this PR.

## Honest cuts

- Default capture on a noisy `schedutil` box; ratios > absolutes.
- `--quick` is shipped (n=1e5 / 200k / 32x1024) and was smoke-checked.
  Charter sizes **were** captured (1e6 / 2M / 256x4096); no size cut.
- §8.6 not rebuilt (#116).
- P-3 not duplicated.
- Raw `repark_ta` Python leg SKIP (F-P2-1).

## Capture

Release wheel (`maturin develop --release`, 149 MiB). Record env
`polars_talib==0.1.5`. Numbers live only in
`planning/hardening/BENCH-BASELINE-2026-08.md` §2. Headline:

- engine/C host tax ~4–9x on one symbol (ema 72.5 vs 9.7 ns/row)
- `target_partitions=64` + `partitionBy` **beats** Polars `.over` (26 vs 52)
- `batch_size` 2M vs 8192: **9.2x**
- full `collect` vs last-row `collect`: **46x**

## Gates (real exit codes)

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make preflight` | **0** |
| `make py-test-facade` | **0** (3214 passed, 71 skipped) |
| `make audit` | **0** |
| `make workflows-lint` | **0** |

## ACC

Mechanical / measure-only. Floor S1. Critic-2: CLEAN (no AWS, no engine
surface, no `unsafe`, no main-package dep add).
**Label: `ACC-CONVERGED`.**
