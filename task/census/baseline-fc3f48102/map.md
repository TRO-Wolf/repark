# map — task/census/baseline-fc3f48102/

## Purpose
THE recorded freeze-point census baseline at the port pin `fc3f48102` (design §6.2/F2): four cohorts + the stability self-diff + full environment manifests. Evidence, not source — never hand-edited except the recorded path-redaction transform; a re-run replaces this whole directory in one commit.

## REGENERATION REQUIRED — this directory cannot currently anchor the gate

**Status: DEFECTIVE. Do not treat these artifacts as the phase-close gate input until they are
re-run.** The defects are in the artifacts themselves, and artifacts are evidence: the fix is a
re-run under the corrected procedure (`docs/port/census.md` §3, whose redaction step is now
executable code and whose validity assertions are now mandatory), **never** a hand-repair. Every
item below is mechanically reproducible from the committed bytes:

1. **All four `compat-report.json` files are invalid JSON.** The redaction was applied textually,
   which ate the backslash of the escaped quote closing each traceback path (`File \"<home>"`
   instead of `File \"<home>\"`): 214 / 214 / 137 / 110 sites. `json.load` fails on every one,
   and the comparator built in this same PR exits **2** on all four — the "stability run1 vs run2,
   empty diff, exit 0" claim was therefore never produced through the instrument that gates the
   phase. (Repairing the escapes in a scratch copy does reproduce the empty diff at 142/345 both
   sides, so the underlying stability result stands; the artifact does not.)
2. **`facade/facade.xml` is not well-formed XML.** The same textual redaction injected the literal
   token `<v1-pin>` into character data 46 times, where it parses as an unclosed start tag.
   Junit-mode acceptance cannot run at all — it exits 2 even comparing the file against itself.
3. **`census-venv-freeze.txt` is 0 bytes** and `census-env-manifest.txt` records only
   python/build-mode/rustc/pin. The pandas major — which design §6.1 calls load-bearing and
   non-negotiable — is recorded nowhere for the three Apache cohorts. Design §5 F2: *a baseline
   whose environment is not recorded is not a baseline.*
4. **`expand/compat-report.json` carries duplicate `test_id`s with conflicting statuses** (two
   `UDFInitializationTests` rows appear as both `FAIL-ERROR-CLASS` and `FAIL-MISSING`), and its
   recorded `all_collected` (171) disagrees with its unique-id count (169). Duplicates are a loud
   failure by design, so this cohort would be refused even with the escaping repaired.
5. **The facade cohort violates two clauses of its own definition** (§6.3): it ran under
   **pandas 3.0.5** against the mandated `pandas>=2.1,<3`, and with a **JVM on PATH**
   (`java-11-openjdk`, and not the pinned Temurin 17). `facade-env-manifest.txt` also summarises
   the gate variables as "all REPARK_* + TABLE_BUCKET_ARN" where §6.3 requires each of the
   thirteen names listed individually, and never states the pyspark-ABSENT / duckdb-ABSENT
   clauses. Mitigating and verified: pyspark and duckdb *are* genuinely absent from the freeze and
   the skip messages read `could not import 'pyspark'`, so the `importorskip` sites fired for the
   recorded reason.

The re-run must satisfy the §3 close-out assertions before commit: every JSON loads, every XML
parses, the freeze is non-empty and carries the pandas major, and `classic-run1` vs `classic-run2`
exits 0 **through the comparator**.

## Contents
- `classic-run1/`, `classic-run2/` — classic cohort ×2 (stability): **142/345 both runs**, self-diff ZERO rows (`stability-self-diff.txt`).
- `expand/` — **44/171**. `expand2/` — **87/167**.
- `facade/` — full-extras facade cohort pair (2,509 collected / 2,517 junit outcomes).
- `census-env-manifest.txt` + `census-venv-freeze.txt`; `facade-env-manifest.txt` + `facade-venv-freeze.txt` — the environments ARE part of the pin (pyspark 4.1.2, pandas<3, debug build; facade venv: full extras, pyspark+duckdb ABSENT, all gates unset, wheel installed by explicit path).
- `stability-self-diff.txt` — empty (zero unstable rows; nothing quarantined).
- `quarantine.txt` — the quarantined-unstable ledger, **empty of entries by result**. The
  documented acceptance command passes `--quarantine`, and a missing ledger file is exit 2 by
  design: recording zero quarantined rows requires an empty file, not an absent one. Re-derive it
  from the regenerated stability run rather than carrying it forward.

## I want to... → go to
| I want to... | go to |
|---|---|
| The procedure that generated this | [../../../docs/port/census.md](../../../docs/port/census.md) |
| The acceptance rules it feeds | `docs/design/python-facade.md` §6 |
| The unit ledger | [../../p3d-parity-ledger.md](../../p3d-parity-ledger.md) |

## Debug
- All absolute scratch paths are mechanically redacted (public-hygiene requirement). The tokens actually present in these artifacts are `<home>`, `<baseline>` and `<v1-pin>` — **not** the `<v1-pin>` / `<baseline>` / `<scratch>` set this file previously claimed, and the most-used token (`<home>`) was undocumented. The regenerated baseline uses the tokens the procedure now fixes: `<scratch>` / `<repo>` / `<home>`, applied by `python -m compat.redact` (docs/port/census.md §3), which is also what makes the artifacts survive the transform. Apply the identical transform to v2-side artifacts before comparison.
- Do not attempt to repair a broken artifact in place. The corruption is not losslessly reversible — the transform collapsed escaped `\"` and genuine string-terminating `"` into the same character — and these files are evidence: the only sanctioned fix is a re-run.
- The PyPI package `repark==0.0.1` is a name reservation — never install `repark` by bare name from an index; the wheel is installed by explicit file path (see `facade-env-manifest.txt`).
