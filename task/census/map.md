# map — task/census/

## Purpose
Recorded census runs for the acceptance gate (docs/port/PLAN.md; procedure docs/port/census.md). One subdirectory per recorded run; baselines are named by the commit they measure.

## Contents
- [baseline-fc3f48102/](baseline-fc3f48102/map.md) — the freeze-point v1-pin baseline (classic ×2 + stability, expand, expand2, facade pair, environment manifests). **Currently DEFECTIVE and pending regeneration** — its JSON reports do not parse, its facade JUnit XML is not well-formed, and its census freeze is empty; see that directory's map.md "Regeneration required" before using it as a gate input.

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
