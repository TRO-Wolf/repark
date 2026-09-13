# map — docs/perf/eager-budget-1-2026-09-13

## Purpose

EAGER-BUDGET-1 step 0 (2026-09-13): the committed measurement behind the
session cache budget — the
[eager_own harness](../../../python/repark-parity/tests/eager_own/map.md)
worker at `--rows 1000000 --iterations 30` under
`systemd-run --user --scope -p MemoryMax=<cap> -p MemorySwapMax=0` for cap ∈
{2G, 4G, 8G}, on the EAGER-OWN-1 base `8936346a` and on `main` `acc9b550`,
release natives (`__debug_assertions__` False on both trees, Python 3.12.3,
`nproc` 64, `free -g` total 125 GiB, `OPENBLAS_NUM_THREADS=8
OMP_NUM_THREADS=8`, box idle before every cell). Per iteration: wall, VmRSS
after the call and after `gc.collect()`, VmHWM, `ru_majflt` delta, and the
`__repark_cache_*` registration count; each record is fsync-flushed to
`<json>.partial` so a cgroup kill still leaves the iterations it reached.

Three loop shapes: `base-bare` (base venv, bare `eager()` — the leak IS the
retention), `main-bare` (bare `eager()`, released each call), `main-retained`
(`--retain`, results appended to a list — the honest pressure case).

## Outcome matrix

| cap | cell | outcome | last iter | VmHWM at end | regs at end |
|---|---|---|---|---|---|
| 2G | base-bare | OOM kill (rc 137) | 5 | 2,025 MB | 6 |
| 2G | main-bare | completed 30 | 29 | 1,384 MB | 0 |
| 2G | main-retained | OOM kill (rc 137) | 5 | 2,051 MB | 6 |
| 4G | base-bare | OOM kill (rc 137) | 16 | 4,139 MB | 17 |
| 4G | main-bare | completed 30 | 29 | 1,361 MB | 0 |
| 4G | main-retained | OOM kill (rc 137) | 15 | 4,054 MB | 16 |
| 8G | base-bare | completed 30 | 29 | 6,695 MB | 30 (leaked) |
| 8G | main-bare | completed 30 | 29 | 1,369 MB | 0 |
| 8G | main-retained | completed 30 | 29 | 6,767 MB | 30 (held; 0 after gc) |

The four kills are journal-confirmed (`scope: Failed with result
'oom-kill'`). Base-bare and main-retained die at the same iteration under each
cap (5/5 at 2G, 16/15 at 4G) — identical retention (~195 MB of distinct Arrow
buffers plus allocator slack per result), differing only in ownership. `main`
bare `eager()` survives 30 iterations even at 2G (VmHWM 1,384 MB < cap).

## Per-cap iteration bands (wall s; first/median/last third)

| cap | cell | first | median | last | max | majflt |
|---|---|---|---|---|---|---|
| 2G | base-bare | 0.810 | 0.785 | 1.758 (iters 4–5) | 2.394 | 0 |
| 2G | main-bare | 2.025 | 2.344 | 2.362 | 3.249 | 0 |
| 2G | main-retained | 2.713 | 2.429 | 2.781 (iters 4–5) | 2.813 | 0 |
| 4G | base-bare | 2.620 | 2.675 | 2.605 (iters 12–16) | 3.064 | 0 |
| 4G | main-bare | 2.631 | 2.734 | 2.600 | 3.015 | 0 |
| 4G | main-retained | 2.718 | 2.288 | 2.154 (iters 11–15) | 2.997 | 0 |
| 8G | base-bare | 2.289 | 2.452 | 2.263 | 4.508 | 0 |
| 8G | main-bare | 2.307 | 2.232 | 2.762 | 4.069 | 0 |
| 8G | main-retained | 2.406 | 2.219 | 2.916 | 4.026 | 0 |

## Retained-buffer probe (`--retained-probe`, one retained result, 8G scope)

Identical on both trees: `Table.nbytes` **199,008,178** vs the
distinct-`buffer.address` sum **188,196,432** over **385** buffers — ratio
**0.9457**. `nbytes` (the `get_array_memory_size` shape) double-counts ~5.4 %
because sliced/derived columns share buffers inside one result; D-2's
distinct-buffer dedupe is the honest figure for a session total.

## Slowdown verdict

The reported per-call slowdown does **not** reproduce as a function of live
cache views on any cap or tree. Wall is flat within noise in every completed
cell (last/first-third ≤ 1.21 on the 8G retention cells, the same rise on the
no-retention bare cell — box noise, not retention). The one real slowdown is
2G base-bare's last two iterations before the kill (0.80 → 2.39 s): cgroup
reclaim stall at the cap edge, not a view-count effect — `main-retained` at
2G dies at the same iteration with wall flat (2.71 → 2.81 s). `ru_majflt` is
0 in every cell (swap off; deaths are anonymous-memory OOM kills, not
thrashing). The absolute per-iteration level varies cell to cell (0.8–2.9 s)
with box load — only within-cell shape is comparable.

## Contents

- `2G-main-bare.json`, `4G-main-bare.json`, `8G-base-bare.json`,
  `8G-main-bare.json`, `8G-main-retained.json` — completed cells (30
  iteration records plus post-loop / post-gc / post-clearCache snapshots).
- `2G-base-bare.json.partial`, `2G-main-retained.json.partial`,
  `4G-base-bare.json.partial`, `4G-main-retained.json.partial` — the four
  OOM-killed cells; the worker died before writing the full JSON, so the
  flushed per-iteration JSONL is the record (6, 6, 17, 16 iterations).
- `probe-base.json`, `probe-main.json` — the `--retained-probe` runs on each
  tree: `table_nbytes`, `distinct_buffer_bytes`, `distinct_buffer_count`,
  `distinct_over_nbytes` for one retained 1e6×25 result.
- `map.md` — this file.

pins: eager-budget-1/C-001

## Pointers

- Up: [../map.md](../map.md)
- Harness: [../../../python/repark-parity/tests/eager_own/map.md](../../../python/repark-parity/tests/eager_own/map.md)
- Ledger: [../../../task/ledgers/staging/eager-budget-1-ledger.md](../../../task/ledgers/staging/eager-budget-1-ledger.md)
- Prior pair: [../eager-own-1-2026-09-13/map.md](../eager-own-1-2026-09-13/map.md)
