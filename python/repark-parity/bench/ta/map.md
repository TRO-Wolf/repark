# map — python/repark-parity/bench/ta

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

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
- `target_partition_contract.py` — BH-1 emit/session tokens (`target_partitions=default`
  vs `target_partitions=1` + `isolation=single_core`). No engine imports.
- `bench_kernel_race.py` — §8.1: one symbol, n=1e6 (``--quick`` n=1e5): RePark
  `evaluate_all` vs `polars_talib` vs raw `repark_ta` (SKIP: no Python kernel
  API; cite P-1 #132). PRIMARY at default conf (tp unset).
- `bench_many_symbols.py` — §8.2: `partitionBy("symbol").orderBy("ts")` PRIMARY
  at default conf (tp unset); explicit `tp=1` isolation cell; no-`partitionBy`
  cliff also at default conf. No explicit-cores cell (unset ≡ `num_cpus`).
- `bench_wide_serving.py` — §8.3: 3×BBANDS + 3×MACD + 2×STOCH + EMA/RSI/ATR
  in one `over_columns` / `with_indicators`; stacked `filter` repeat;
  `WindowAggExec` count recorded next to wall time. Cites #116 for §8.6.
  PRIMARY at default conf.
- `bench_batch_size.py` — §8.4: `batch_size` sweep, one 2M-bar symbol
  (`--quick` 200k), `target_partitions=1` single-core isolation (not a
  default-conf primary). SortExec is the measured lever.
- `bench_null_lookback.py` — §8.5: `null_lookback=True` × 10 columns vs
  default; `WindowAggExec` + window-fn token counts. PRIMARY at default conf.
- `bench_last_row.py` — §8.7: `with_indicators(last_row=True)` vs full-table
  collect; Arrow (`to_arrow`) and Spark-Row (`collect`) sinks. PRIMARY at
  default conf.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Race kernels vs C TA-Lib (one symbol) | `bench_kernel_race.py` |
| Measure the partitionBy / `.over` cliff | `bench_many_symbols.py` |
| Time the wide serving SELECT + stacked filter | `bench_wide_serving.py` |
| Sweep `batch_size` on a fat symbol (tp=1 isolation) | `bench_batch_size.py` |
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
- **BH-1 default-conf primary.** PRIMARY `TA_PIPELINE` lines omit
  `repark.target.partitions` and emit `target_partitions=default`. Explicit
  `target_partitions=1` is isolation-only and must also emit
  `isolation=single_core`. Prose labels live here / in script docstrings —
  no spaces in TA_PIPELINE kv values. The old `{1, cores}` many-symbols sweep
  is retired: default (unset) replaces the explicit-cores cell.

## Allocator verdict — AL-1b (2026-08-16)

mimalloc (feature `allocator-mimalloc`, landed default-off in #159) measured A/B against the
glibc default on the BH-1 default-conf battery: 5 contention-guarded interleaved pairs
(verdict set) + 20 contended pairs both orders (corroboration) + a 30-pair single-core
gate-cell sweep. Median ns_per_row deltas (B vs A): kernel_race sma −15% / ema −13% /
rsi −3% / bbands −4%; many_symbols partitionBy −13% / no-partitionBy −11%; wide_serving
over_columns −48% / with_indicators −37% / stacked_filter −52%. Gate cell (batch_size
n=2e6 bs=8192 tp=1): −9% guarded, ±0 across the 30-pair sweep — that shape is bimodal
per process on BOTH allocators (fast/slow mode lottery, ~205 vs ~500 ns/row); mimalloc's
win concentrates where glibc arena contention bites (multi-threaded default conf).
VERDICT: WIRE (≥5% win on 7/9 primaries, no primary regression). Wired into the wheel via
the facade pyproject `[tool.maturin] features` in the AL-1b wire PR; goldens + facade suite
verified green under mimalloc before wiring. Full protocol + deviations: the wire PR body
and task/c19-al1a-mimalloc-ledger.md.
