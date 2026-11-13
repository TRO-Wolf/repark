# Card C002-BENCH-MAX-PASSES-1: the C-002 cast bench's attribution mode sets `max_passes = 0`, which part B refuses

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** SIGNED-ZERO-NORMALIZE-1 part B
delta 2 re-check, 2026-11-13 (#1326, finding M4, class (c)); owner, 2026-11-13, about 14:52 UTC, verbatim "Defaults,
and merge #1323 when green".

**Status:** filed 2026-11-13, not dispatched. Severity **S4**: a manual measurement harness, not a gate.

## Measured

`python/repark-parity/bench/cast/attribute.py`, run as `run_cast.py --attribute`, calls
`plan_with_max_passes(…, 0)`, which opens a session with `datafusion.optimizer.max_passes = 0`. Part B refuses that
value since its delta 1, so on part B's head the call raises. No internal session sets either guarded key.

## Close

The attribution mode measures without `max_passes = 0` (or drops that row), and `run_cast.py --attribute` runs on
part B's build.
