# map — python/repark-parity/src/repark_parity

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

The `repark_parity` package — the parity comparison core. See [../../map.md](../../map.md).

## Contents

- `compare.py` — `assert_frames_equal` + `FrameMismatchError` (null-aware, order-insensitive Arrow
  comparison). The schema signature is `(name, type, nullable)` per field — **field nullability is
  part of the parity contract** (Spark's non-null guarantees, e.g. `coalesce` with a non-null
  fallback / `row_number`, are reproduced by the engine; verified empirically to keep every existing
  parity case green — closes the residual that nullability was ignored). **G18:** order-insensitive
  path accepts nested list/struct/map columns via total canonical row keys + map entry
  normalization; flat schemas keep the historical `sort_by` path (no golden re-record).
- `sql.py` — dependency-free, quote-only SQL escaping for standalone parity benchmarks.
- `__init__.py` — public exports. `py.typed` — typed marker.

## Pointers

- Up: [../../map.md](../../map.md)

## Debug

First checks: `PYTHONPATH=python/repark-parity/src pytest python/repark-parity/tests -q`.
Escalate to: [../../map.md#debug](../../map.md).
