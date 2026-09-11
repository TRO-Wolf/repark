# Unit ledger — REVIEW-FIX-6 step 1 · `explain()` refuses the both-set shape (Q-17)

**Unit:** REVIEW-FIX-6 step 1 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-6-11` · **Base:** `2f82df95`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The review-1 sweep found `explain()` silently prefers `extended` when both
`extended` and `mode` are passed (Q-17). PySpark refuses that shape with the
`CANNOT_SET_TOGETHER` error; this card makes the facade refuse it the same way.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** any dependency file, `STATUS.md`, `briefs/next-sequence.md`, `gh`,
pushes, live Spark (`REPARK_PARITY_LIVE` stays unset per the round notes).

## PROPOSITION LEDGER — REVIEW-FIX-6 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: passing both `extended` and `mode` raises `PySparkValueError` with the `CANNOT_SET_TOGETHER` shape this repo already uses for `Row(1, a=2)`. | `test_explain_both_set_raises_cannot_set_together` | **PROVEN** | Red 2026-09-10 on the base tree (new test file laid over stashed `core.py`, `.venv/bin/python -m pytest python/repark/tests/test_df_explain_1.py -q`): `1 failed, 12 passed in 0.62s`, the failure `test_explain_both_set_raises_cannot_set_together` with `E Failed: DID NOT RAISE PySparkValueError` at `test_df_explain_1.py:98` — the base silently rendered the extended plan to stdout instead. Green after the guard at the top of `_explain_text` (`extended is not None and mode is not None`, PySpark's guard shape; message `[CANNOT_SET_TOGETHER] extended and mode should not be set together.` mirroring `row.py:53`): `13 passed in 0.56s`. The pin covers `explain(True, mode="formatted")`, `explain(False, mode="simple")`, and `_explain_text("formatted", mode="formatted")`. |
| C-002 | D-1 compat: a string `extended` with no `mode` keeps working, and `mode` alone keeps working. | `test_explain_string_extended_still_prints_tree` (+ existing `test_explain_formatted_carries_datafusion_tree_glyphs` for `mode="formatted"`) | **PROVEN** | The guard sits above the untouched `isinstance(extended, str) and mode is None` remap. Green 2026-09-10 in the same `13 passed` run: the new string-`extended` pin asserts the physical header plus both tree glyphs, and the pre-existing `mode="formatted"` pin still passes. |
| C-003 | Red-first: the new pins run RED on the base tree before the fix, with the failure output recorded here. | Base-tree pytest run of the RF-6 pins | **PROVEN** | Red run recorded in C-001 (`1 failed, 12 passed`; only the both-set pin fails — the compat pins are green-before-green by design, since D-1 changes refusal, not rendering). |
| C-004 | D-2 (A-2 ruling): the four-line guard is funded without raising the `core.py` exact baseline — five in-region comment lines move to `python/repark/src/repark/spark/dataframe/map.md`, and the `check_lib_py.py` row plus the CAP-1 mirror ratchet DOWN to the new exact count. | `python3 scripts/check_lib_py.py` green at 4486; CAP-1 mirror at 4486 | **PROVEN** | Reason: the owner declined a baseline increase (A-2, same ruling as REVIEW-FIX-5 R5-A), so the +4 guard lines are paid for by deleting five `#` lines in the worked region (two grouping-set generator lines, the unpivot bridge-snapshot line, two `create_temp_view` engine-path lines), each restated on the `core.py` map row. `core.py` 4487 + 4 − 5 = 4486; `check_lib_py.py` row and `test_cap_1_source_file_line_cap.py` tuple both moved 4487 → 4486 in the same commit with no other row touched; `check_lib_py.py` green (see Gates). |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Green

`.venv/bin/python -m pytest python/repark/tests/test_df_explain_1.py -q` 2026-09-10:
`13 passed in 0.56s` (7 existing + 6 new). Neighbor pins:
`test_df_batch2.py -k explain` `1 passed, 5 deselected in 0.12s`.

## Red first

Base-tree run with the new pins over stashed `core.py` (2026-09-10):
`1 failed, 12 passed in 0.62s`; the single failure is the both-set pin
(`Failed: DID NOT RAISE PySparkValueError`, `test_df_explain_1.py:98`).

## Residue

None. No D-2 disagreement surfaced: every RF-11 mode pin passed on the base tree,
confirming the D-2 rows already matched the code.

## Gates

- `.venv/bin/python -m pytest python/repark/tests/test_df_explain_1.py -q`: exit 0, 13 passed.
- `test_df_batch2.py -k explain`: exit 0, 1 passed.
- `python3 scripts/check_lib_py.py`: exit 0 after the 4486 ratchet.
- `make verify`: exit 0 (see handback).
- Comment fence over the staged diff: clean (no added `//` / `#`).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-6
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: D-1 walked pin by pin. The both-set pin reds on the base tree with DID NOT RAISE (the base printed the extended plan) and greens with the guard; the string-extended and mode-only compat pins green with the guard in place. The 13-test file run plus the batch2 neighbor pin re-measured in the same tree.
      artifacts: [python/repark/tests/test_df_explain_1.py, python/repark/src/repark/spark/dataframe/core.py, python/repark/tests/test_df_batch2.py]
    - id: AT-2
      status: ATTACKED
      evidence: The argument domain is the risk surface (extended accepts bool/None/mode-string, mode accepts None/string). Exercised: True+mode, False+mode, str+mode (all refuse), str alone (remaps), mode alone, True alone, default (all render). False+mode refuses per PySpark's is-not-None guard, pinned, not assumed.
      artifacts: [python/repark/tests/test_df_explain_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The guard raises before the string remap, before mode validation, and before _ensure_alive or any scratch-view creation, so no view leaks on the refusal path and a stopped session still gets the argument error first.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no ordering assumption, no async spawn and no lock. The guard is a pure argument check; the moved comment facts are prose in the map.
    - id: AT-5
      status: ATTACKED
      evidence: The refusal reuses the repo's single CANNOT_SET_TOGETHER message shape (row.py precedent), not a second shape; unknown-mode validation below it is untouched.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py, python/repark/src/repark/spark/row.py]
    - id: AT-6
      status: ATTACKED
      evidence: No export-surface change: no new name, no signature change, no CAP-1 freeze-pin movement. The CAP-1 mirror moves only its count (4487 to 4486), verified by the parity-cap test.
      artifacts: [python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-7
      status: N/A
      justification: Argument validation before any plan work; no data scan, no loop, no allocation that scales with input size.
    - id: AT-8
      status: ATTACKED
      evidence: The upstream contract is PySpark's documented explain guard (both-set refuses CANNOT_SET_TOGETHER) plus the repo's existing Row precedent for that error class. Live-Spark measurement is explicitly out of scope for this round per the brief (REPARK_PARITY_LIVE unset); the affected pin surface (13 explain pins + batch2 explain pin) re-measured green in the same tree instead.
      artifacts: [python/repark/tests/test_df_explain_1.py, task/ledgers/staging/review-fix-6-ledger.md]
    - id: AT-9
      status: N/A
      justification: No log or metric surface in the path; the refusal message itself names both arguments.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first proven by the stash run (1 failed on base, the both-set pin, output pasted in C-001); every clause names its pin; the file-size gate held by the sanctioned down-ratchet (4487 to 4486 in check_lib_py.py plus the CAP-1 mirror, no baseline raised); the comment fence clean over the staged diff.
      artifacts: [task/ledgers/staging/review-fix-6-ledger.md, scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
```
