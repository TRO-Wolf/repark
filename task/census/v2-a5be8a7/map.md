# map — task/census/v2-<sha>/

## Purpose
THE v2 acceptance census run (design §6.6) — the milestone-one gate. Four cohorts run against the
v2 facade in the SAME environment as the pin baseline, redacted through `compat.redact`. Evidence,
never hand-edited; a re-run replaces the whole directory in one commit.

## Contents
- `classic/` **142/345**, `expand/` **44/171**, `expand2/` **87/167** — byte-identical to
  `../baseline-fc3f48102/` through `compat.compare_reports` (exit 0 each).
- `facade/` — the full-extras cohort: 2,499 collected / 2,459 passed + 46 skipped. Byte-identical
  to the baseline after `(v2 − added:2) ∪ deferred:12 = pin:2,509` (junit-mode comparator, exit 0).
- `census-manifest.json` / `facade/facade-manifest.json` — the external manifest halves the
  comparator gates on; `*-venv-freeze.txt` the full pip freezes (pyspark 4.1.2, pandas<3 census /
  pandas 3 facade — the pin environment).

## I want to... → go to
| I want to... | go to |
|---|---|
| Reproduce the comparison | `compat/compare_reports.py` (see docs/port/census.md §5); the exact invocations are in ../../p3g-close-ledger.md |
| The baseline this is compared against | [../baseline-fc3f48102/map.md](../baseline-fc3f48102/map.md) |

## Debug
- Absolute paths are redacted via `compat.redact` (`<repo>`/`<scratch>`/`<home>`/`<session>`).
- The 8-count collected-vs-junit delta (facade) is module-level skip records for the
  pyspark/duckdb-gated modules — the environment clauses working as designed (same as the baseline).
