# map — task/census/

## Purpose
Recorded census runs for the acceptance gate (docs/port/PLAN.md; procedure docs/port/census.md). One subdirectory per recorded run; baselines are named by the commit they measure.

## Contents
- [baseline-fc3f48102/](baseline-fc3f48102/map.md) — the freeze-point v1-pin baseline (classic ×2 + stability, expand, expand2, facade pair, environment manifests).

## I want to... → go to
| I want to... | go to |
|---|---|
| Generate a comparable v2 run | docs/port/census.md |
| Compare two runs | `python -m compat.compare_reports` (python/repark-parity) |

## Pointers
- Up: [../map.md](../map.md)

## Debug
- A run whose environment manifest differs from the baseline's is not comparable — the comparator refuses before diffing (design §6.4).
