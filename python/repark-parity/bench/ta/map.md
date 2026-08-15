# map — python/repark-parity/bench/ta

## Purpose

**P-2 (perf-wave-14)** measurement-only Python TA pipeline benches. Charter
`planning/grok/BRIEF-perf-wave-14.md` § tracks P-2; the seven measurements live
in `polars-ta-extension-vs-repark-ta-performance.md` §8. This directory ships
the **code** for §8.1–8.5 and §8.7. **§8.6 SQL same-OVER EXPLAIN already
shipped as #116 (TA-1)** — cite it, do not rebuild. Numbers land in
`planning/hardening/BENCH-BASELINE-2026-08.md` (planning-side, not this repo).

Zero engine edits. No `crates/src`. P-3 flamegraph / `unsafe` is orchestrator-
owned — not duplicated here.

`polars_talib` 0.1.5 is reused from the parity `record` / PEP-723 env
(`record_ta_goldens.py`); it is **not** a main-package dependency.

## Contents

- `harness.py` — shared seeded generators (p1c walk, never wall clock in the
  seed), warm-up + N-iteration median, session / plan-shape helpers, one-line
  `TA_PIPELINE` emitter.
- `bench_kernel_race.py` — §8.1: one symbol, n=1e6 (``--quick`` n=1e5): RePark
  `evaluate_all` vs `polars_talib` vs raw `repark_ta` (SKIP: no Python kernel
  API; cite P-1 #132).
- `bench_many_symbols.py` — §8.2: `partitionBy("symbol").orderBy("ts")` at
  `target_partitions ∈ {1, cores}` vs Polars `.over("symbol")`; includes the
  no-`partitionBy` cliff row.
- `bench_wide_serving.py` — §8.3: 3×BBANDS + 3×MACD + 2×STOCH + EMA/RSI/ATR
  in one `over_columns` / `with_indicators`; stacked `filter` repeat;
  `WindowAggExec` count recorded next to wall time. Cites #116 for §8.6.
- `bench_batch_size.py` — §8.4: `batch_size` sweep, one 2M-bar symbol
  (`--quick` 200k), `target_partitions=1`.
- `bench_null_lookback.py` — §8.5: `null_lookback=True` × 10 columns vs
  default; `WindowAggExec` + window-fn token counts.
- `bench_last_row.py` — §8.7: `with_indicators(last_row=True)` vs full-table
  collect; Arrow (`to_arrow`) and Spark-Row (`collect`) sinks.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Race kernels vs C TA-Lib (one symbol) | `bench_kernel_race.py` |
| Measure the partitionBy / `.over` cliff | `bench_many_symbols.py` |
| Time the wide serving SELECT + stacked filter | `bench_wide_serving.py` |
| Sweep `batch_size` on a fat symbol | `bench_batch_size.py` |
| Cost of `null_lookback=True` × 10 | `bench_null_lookback.py` |
| Last-row collect vs full table | `bench_last_row.py` |
| Short path (n=1e5 / 200k) | add `--quick` to any script |
| SQL same-OVER EXPLAIN pin | **#116** (`ta_window.rs` / `ta_toll.rs`) — not here |
| Read transcribed numbers | planning-side `BENCH-BASELINE-2026-08.md` |

## Debug

| Symptom | Check |
|---|---|
| `ModuleNotFoundError: polars_talib` | record / PEP-723 env, or `uv run --with polars-talib==0.1.5`; never add it to the workspace package |
| Absurd walls / 10–50× too slow | debug wheel — `maturin develop --release` first (the harness prints `native=` size) |
| Session knobs ignored mid-sweep | engine knobs are fixed at `getOrCreate`; each `batch_size` / `target_partitions` cell builds a **new** session |
| `repark_ta_raw reachable=false` | expected — kernels are Rust-only; P-1 #132 is the raw-kernel number |
| Full `collect()` of 1M rows is the time box | `--quick` or `--skip-row-collect`; the CODE still has the full path |

## Constraints

- Measurement only — **no** product / fork / knob-default edits.
- Never add `polars_talib` (or any new dep) to `pyproject.toml` / `uv.lock`.
- Deterministic seeds; `ts` is a bar index, never `time.time()`.
- Do not rebuild §8.6 (already #116). Do not duplicate P-3 flamegraphs.
