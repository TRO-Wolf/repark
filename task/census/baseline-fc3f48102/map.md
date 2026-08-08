# map — task/census/baseline-fc3f48102/

## Purpose
THE recorded freeze-point census baseline at the port pin `fc3f48102` (design §6.2/F2): four cohorts + the stability self-diff + full environment manifests. Evidence, not source — never hand-edited except the recorded path-redaction transform; a re-run replaces this whole directory in one commit.

## Contents
- `classic-run1/`, `classic-run2/` — classic cohort ×2 (stability): **142/345 both runs**, self-diff ZERO rows (`stability-self-diff.txt`).
- `expand/` — **44/171**. `expand2/` — **87/167**.
- `facade/` — full-extras facade cohort pair (2,509 collected / 2,517 junit outcomes).
- `census-env-manifest.txt` + `census-venv-freeze.txt`; `facade-env-manifest.txt` + `facade-venv-freeze.txt` — the environments ARE part of the pin (pyspark 4.1.2, pandas<3, debug build; facade venv: full extras, pyspark+duckdb ABSENT, all gates unset, wheel installed by explicit path).
- `stability-self-diff.txt` — empty (zero unstable rows; nothing quarantined).

## I want to... → go to
| I want to... | go to |
|---|---|
| The procedure that generated this | [../../../docs/port/census.md](../../../docs/port/census.md) |
| The acceptance rules it feeds | `docs/design/python-facade.md` §6 |
| The unit ledger | [../../p3d-parity-ledger.md](../../p3d-parity-ledger.md) |

## Debug
- All absolute scratch paths are mechanically redacted to `<v1-pin>` / `<baseline>` / `<scratch>` (public-hygiene requirement, recorded in the ledger); apply the identical transform to v2-side artifacts before manifest comparison.
- The PyPI package `repark==0.0.1` is a name reservation — never install `repark` by bare name from an index; the wheel is installed by explicit file path (see `facade-env-manifest.txt`).
