# map — docs/perf/eager-own-1-2026-09-13

## Purpose

EAGER-OWN-1 step 0 (2026-09-13): the committed before/after measurement files
for the bare-`eager()` retention defect. Each JSON is one worker-subprocess run
of the pin in
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
  RSS result. Step 1 adds `after.json` beside it; the two files are the
  before/after pair the card requires (D-7) — `base.json` is never overwritten.
- `map.md` — this file.

pins: eager-own-1/C-001

## Pointers

- Up: [../map.md](../map.md)
- Harness: [../../../python/repark-parity/tests/eager_own/map.md](../../../python/repark-parity/tests/eager_own/map.md)
- Ledger: [../../../task/ledgers/staging/eager-own-1-ledger.md](../../../task/ledgers/staging/eager-own-1-ledger.md)
