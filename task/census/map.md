# map — task/census/

## Purpose
Recorded census runs for the acceptance gate (docs/port/PLAN.md; procedure docs/port/census.md). One subdirectory per recorded run; baselines are named by the commit they measure.

## Contents
- [v2-a5be8a7/](v2-a5be8a7/map.md) — the **v2 acceptance run** (phase-3 PR-7 = milestone one): four cohorts, byte-flat vs the baseline through the comparator (exit 0 each). THE milestone-one evidence.
- [baseline-fc3f48102/](baseline-fc3f48102/map.md) — the freeze-point v1-pin baseline (classic ×2 + stability, expand, expand2, facade pair, environment manifests).. Valid gate input (regenerated PR-4; every JSON parses, the JUnit XML is well-formed, freezes non-empty).

## I want to... → go to
| I want to... | go to |
|---|---|
| Generate a comparable v2 run | docs/port/census.md |
| Compare two runs | `python -m compat.compare_reports` (python/repark-parity) |

## Pointers
- Up: [../map.md](../map.md)

## Debug
- A run whose environment manifest differs from the baseline's is not comparable — the comparator refuses before diffing (design §6.4). A run whose environment is not *recorded* is refused for the same reason.
- Redact artifacts with `python -m compat.redact` (format-aware, through each parser). A textual substitution breaks JSON string escaping and XML character data, and a committed artifact that does not parse is not evidence.
