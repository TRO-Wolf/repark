# task/wo/sum-dec-i128wrap-1/evidence/built/delta1/spread/ — the raw spreads

One JSON object per query: the count of each distinct answer over its runs (`answers`), with a provenance row (the
RePark package's `_native.abi3.so`, or Spark's version and master). Written by [../spread.py](../map.md) over the fixed
files of [files.json](files.json) (row counts and the first 16 hex digits of each file's SHA-256).

- `before/` — 2026-11-18, before any product file of the delta changed: `repark-pkg-sum-a8016e75-*` (the build's
  head), `repark-pkg-view1-c54f5fc7-*` (main `d6f58d7f`), 401 queries × 20 runs per ANSI mode; `spark-<mode>-split-*`
  and `spark-<mode>-default-*`, Spark 4.1.2 `local[4]`, 172 pass-back queries × 20 runs.
- `after/` — the head package `pkg-sum-6d730107` (`BUILT_FROM` `6d730107`), the same 401 queries × 20 runs per mode.

Up: [../map.md](../map.md).
