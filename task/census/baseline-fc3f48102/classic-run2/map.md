# map — task/census/baseline-fc3f48102/classic-run2/

## Purpose
Recorded evidence for the `classic` census cohort at the v1 pin (procedure: docs/port/census.md). Never hand-edited; a re-run replaces the whole baseline directory in one commit.

## Contents
- `compat-report.json` — machine-readable census report (comparator input).
- `report.md` — human-readable render with both denominators.

## I want to... → go to
| I want to... | go to |
|---|---|
| Compare against a v2 run | `compat/compare_reports.py` (see docs/port/census.md) |

## Pointers
- Up: [../map.md](../map.md)

## Debug
- Paths inside artifacts are mechanically redacted (`<v1-pin>`, `<baseline>`, `<scratch>`) — see the ledger's redaction note; the same transform applies to v2-side artifacts before comparison.
