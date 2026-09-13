# map — docs/perf/eager-own-1-2026-09-13

## Purpose

EAGER-OWN-1 steps 0+1 (2026-09-13): the committed before/after measurement
files for the bare-`eager()` retention defect. Each JSON is one
worker-subprocess run of the pin in
[python/repark-parity/tests/eager_own/](../../../python/repark-parity/tests/eager_own/map.md)
— the deterministic 1,000,000-row, 25-column TA `withColumns` fixture, ten bare
`temp_df.eager()` calls with per-iteration wall / VmRSS / VmRSS-after-gc /
VmHWM and `__repark_cache_*` counts post-loop, post-gc, and
post-`clearCache()`.

## Contents

- `base.json` — the base-tree run at `8936346a` on the release native
  (`__debug_assertions__` false, Python 3.12.3, 64 cores, ~126 GiB total,
  under a 24 GiB `systemd-run` scope): ten registrations after the loop and
  after `gc.collect()`, zero after `clearCache()`; RSS 546 MB → 2,966 MB across
  the ten calls (~240 MB per retained result); per-iteration wall flat at
  0.80–0.86 s on this box — the retention is measured, the reported slowdown is
  not reproduced at this size on this host and stays honestly a registration +
  RSS result. `base.json` is never overwritten.
- `after.json` — the step-1 run on the same box and scope after the
  refcounted `CacheViewHandle` landed: **zero registrations at every reading**
  (each bare `eager()`'s view dies with its dropped child inside the
  iteration), RSS plateaus ~0.85–1.2 GB, VmHWM 1,257 MB vs base's 3,041 MB,
  wall flat 0.80–0.92 s. Before/after table:

  | iter | before s | after s | before VmRSS MB | after VmRSS MB | before regs | after regs |
  |---|---|---|---|---|---|---|
  | 0 | 0.827 | 0.802 | 546 | 540 | 1 | 0 |
  | 1 | 0.814 | 0.818 | 1,253 | 1,041 | 2 | 0 |
  | 2 | 0.804 | 0.916 | 1,196 | 1,096 | 3 | 0 |
  | 3 | 0.844 | 0.833 | 1,630 | 1,109 | 4 | 0 |
  | 4 | 0.805 | 0.869 | 1,803 | 917 | 5 | 0 |
  | 5 | 0.809 | 0.820 | 1,877 | 858 | 6 | 0 |
  | 6 | 0.815 | 0.797 | 2,052 | 959 | 7 | 0 |
  | 7 | 0.856 | 0.864 | 2,667 | 1,210 | 8 | 0 |
  | 8 | 0.810 | 0.838 | 2,457 | 1,059 | 9 | 0 |
  | 9 | 0.824 | 0.829 | 2,967 | 1,199 | 10 | 0 |
  | post-gc | — | — | 2,967 | 1,199 | 10 | 0 |

  The spawn-cost comparison (facade `chain` cells vs a `git show 8936346a:`
  base copy, interleaved warmup + 5 reps) is in the ledger's Evidence section —
  every `build_only` cell within ±2.1 % of base.
- `map.md` — this file.

pins: eager-own-1/C-001, C-012

## Pointers

- Up: [../map.md](../map.md)
- Harness: [../../../python/repark-parity/tests/eager_own/map.md](../../../python/repark-parity/tests/eager_own/map.md)
- Ledger: [../../../task/ledgers/staging/eager-own-1-ledger.md](../../../task/ledgers/staging/eager-own-1-ledger.md)
