# map — task/census/baseline-fc3f48102/facade/

## Purpose
Recorded evidence for the full-extras facade cohort at the v1 pin (design §6.3): the pair of multisets.

## Contents
- `collected.txt` — `pytest --collect-only -q` node-id list (2,509) — the relocation-discipline artifact.
- `facade.xml` — JUnit `(nodeid, outcome)` multiset (2,517 testcases: 2,471 passed + 46 skipped; the 8 extra vs collected are module-level skip records for the pyspark/duckdb-gated modules — the environment clauses working as designed).
- `run-tail.txt` — run summary tail.

## I want to... → go to
| I want to... | go to |
|---|---|
| Compare against a v2 run | `compat/compare_reports.py --junit` (docs/port/census.md) |

## Pointers
- Up: [../map.md](../map.md)

## Debug
- Paths inside artifacts are mechanically redacted (`<v1-pin>`, `<baseline>`, `<scratch>`); same transform applies to the v2 side.
