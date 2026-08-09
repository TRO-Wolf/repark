# map — task/census/baseline-fc3f48102/

## Purpose
THE recorded freeze-point census baseline at the port pin `fc3f48102` (design §6.2/F2): four cohorts + the stability self-diff + full environment manifests. Evidence, not source — never hand-edited except the recorded path-redaction transform; a re-run replaces this whole directory in one commit.

## Regeneration record

> **Resolved 2026-08-08.** An earlier committed generation of this directory was DEFECTIVE — a
> textual redaction had corrupted the JSON/XML evidence, a freeze file was empty, one cohort
> carried duplicate ids, and the facade cohort violated two clauses of its own definition — and
> per the never-hand-repair rule it was replaced **wholesale by a re-run** under the corrected
> procedure (`docs/port/census.md` §3: parser-based redaction, mandatory validity assertions).
> The artifacts below are that re-run: every JSON loads, every XML parses, both freezes are
> non-empty and carry the pandas major, and `classic-run1` vs `classic-run2` exits 0 **through
> the comparator**. The full defect inventory and the regeneration record live in the archived
> [p3d ledger](../../../docs/history/port-v2/p3d-parity-ledger.md). *(This note replaced the
> pre-regeneration DEFECTIVE warning on 2026-08-09, Front-Door FD-4 — the warning described a
> state the re-run had already resolved.)*

## Contents
- `classic-run1/`, `classic-run2/` — classic cohort ×2 (stability): **142/345 both runs**, self-diff ZERO rows (`stability-self-diff.txt`).
- `expand/` — **44/171**. `expand2/` — **87/167**.
- `facade/` — full-extras facade cohort pair (2,509 collected / 2,517 junit outcomes).
- `census-env-manifest.txt` + `census-venv-freeze.txt`; `facade-env-manifest.txt` + `facade-venv-freeze.txt` — the environments ARE part of the pin (pyspark 4.1.2, pandas<3, debug build; facade venv: full extras, **pandas major 3** — the major v1's own CI is green under; a pandas-2.3.3 run fails `test_to_pandas_with_nulls_values_and_dtypes`, recorded in the ledger — pyspark+duckdb ABSENT, JVM-free PATH verified, all gates unset by name, wheel installed by explicit path).
- `stability-self-diff.txt` — empty (zero unstable rows).
- `quarantine.txt` — two ids: the v1 runner's duplicate emissions in the expand cohort
  (`test_udf.UDFInitializationTests.*` pair, conflicting classes in one run) — excluded from
  the gate by name on both sides, echoed separately by the comparator.
- `census-manifest.json` + `facade/facade-manifest.json` — the external manifest halves the
  comparator gates on (pandas/pyarrow versions; the report JSONs carry the rest).

## I want to... → go to
| I want to... | go to |
|---|---|
| The procedure that generated this | [../../../docs/port/census.md](../../../docs/port/census.md) |
| The acceptance rules it feeds | `docs/design/python-facade.md` §6 |
| The unit ledger (archived) | [p3d-parity-ledger.md](../../../docs/history/port-v2/p3d-parity-ledger.md) |

## Debug
- All absolute scratch paths are mechanically redacted (public-hygiene requirement). The tokens actually present in these artifacts are `<home>`, `<baseline>` and `<v1-pin>` — **not** the `<v1-pin>` / `<baseline>` / `<scratch>` set this file previously claimed, and the most-used token (`<home>`) was undocumented. The regenerated baseline uses the tokens the procedure now fixes: `<scratch>` / `<repo>` / `<home>`, applied by `python -m compat.redact` (docs/port/census.md §3), which is also what makes the artifacts survive the transform. Apply the identical transform to v2-side artifacts before comparison.
- Do not attempt to repair a broken artifact in place. The corruption is not losslessly reversible — the transform collapsed escaped `\"` and genuine string-terminating `"` into the same character — and these files are evidence: the only sanctioned fix is a re-run.
- The PyPI package `repark==0.0.1` is a name reservation — never install `repark` by bare name from an index; the wheel is installed by explicit file path (see `facade-env-manifest.txt`).
