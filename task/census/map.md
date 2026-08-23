# map — task/census/

## Purpose
What remains of the recorded census runs in the tree: the one directory a gate still reads.
Everything else — the v2 acceptance run `v2-a5be8a7/` and the rest of the baseline — was evicted
on 2026-08-23 (DL-1, owner ruling) and is reachable whole at `main` `b13b22c`
(`git show b13b22c:task/census/map.md`); the pointer and the reasoning are
[../../docs/port/census.md](../../docs/port/census.md) §7.

## Contents
- [baseline-fc3f48102/](baseline-fc3f48102/map.md) — the facade cohort of the freeze-point pin,
  kept because `python/repark-parity/tests/test_deferred_ledger.py` reads it.

## I want to... → go to
| I want to... | go to |
|---|---|
| Generate a comparable v2 run | [../../docs/port/census.md](../../docs/port/census.md) |
| Read the evicted evidence | `git show b13b22c:task/census/<run>/map.md` |

## Pointers
- Up: [../map.md](../map.md)
