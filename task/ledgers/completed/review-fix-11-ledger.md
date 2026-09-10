# Unit ledger — REVIEW-FIX-11 step 1 · the unpinned `explain()` rows (Q-32, with Q-17)

**Unit:** REVIEW-FIX-11 step 1 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-6-11` · **Base:** `2f82df95`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** DF-EXPLAIN-1's D-2 mode map has rows with no pins (cost, codegen, `ANALYZE`,
simple-mode physical-only, blank-line separation). This card pins every one of them, landed
with REVIEW-FIX-6 so the both-set refusal and the mode rows are one change.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** any dependency file, `STATUS.md`, `briefs/next-sequence.md`, `gh`,
pushes, live Spark (`REPARK_PARITY_LIVE` stays unset per the round notes), any `core.py`
behavior change (none was needed — every D-2 row measured true, see C-006).

## PROPOSITION LEDGER — REVIEW-FIX-11 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-2 cost row: `mode="cost"` runs `EXPLAIN ANALYZE` and prints under `== Physical Plan ==` with no logical header. | `test_explain_cost_prints_physical_without_logical` (cost half) | **PROVEN** | Green on the landed tree in the `13 passed` run. The base-tree red run also passed this pin (green-before-green), confirming the D-2 row already matched the code — measurement wins, no `core.py` change. |
| C-002 | D-2 codegen row: `mode="codegen"` prints `== Physical Plan ==` plus the one trailing `codegen: not applicable (RePark has no generated code)` line. | `test_explain_codegen_appends_not_applicable_note` | **PROVEN** | Green on the landed tree (`13 passed`); green-before-green on base. The pin asserts the physical header present, the logical header absent, and the exact trailing note line. |
| C-003 | D-2 `ANALYZE` row: any mode containing `analyze` behaves as cost. | `test_explain_cost_prints_physical_without_logical` (analyze half, `mode="analyze"`) | **PROVEN** | Green on the landed tree (`13 passed`); green-before-green on base. `lowered="analyze"` misses the section plan, hits the `"analyze" in lowered` branch, resolves to `cost`. |
| C-004 | D-2 simple row: `explain()` / `mode="simple"` prints only `== Physical Plan ==` from `physical_plan`, never the logical header. | `test_explain_simple_mode_prints_physical_only` | **PROVEN** | Green on the landed tree (`13 passed`) for both the default and the explicit `mode="simple"` spellings; green-before-green on base. |
| C-005 | D-1/D-2 separation row: extended mode separates the logical and physical sections with a blank line. | `test_explain_extended_separates_sections_with_blank_line` | **PROVEN** | Green on the landed tree (`13 passed`); green-before-green on base. The pin asserts `"\n\n== Physical Plan =="`, matching the measured missing trailing newline on the `logical_plan` text. |
| C-006 | Red-first: the five new mode pins run on the base tree before any `core.py` change, with the outcome recorded here. | Base-tree pytest run of the five RF-11 pins | **PROVEN** | Base run 2026-09-10 (new pins over stashed `core.py`): `1 failed, 12 passed` — all five mode pins PASSED on base, the only failure being the REVIEW-FIX-6 both-set pin. Per the round notes the measurement wins: the D-2 rows agree with the code, so no `core.py` behavior change was made and no residue row is owed. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Green

`.venv/bin/python -m pytest python/repark/tests/test_df_explain_1.py -q` 2026-09-10:
`13 passed in 0.56s` (7 existing + 6 new). Neighbor pins:
`test_df_batch2.py -k explain` `1 passed, 5 deselected in 0.12s`.

## Red first

Base-tree run with the new pins over stashed `core.py` (2026-09-10):
`1 failed, 12 passed in 0.62s`; every RF-11 mode pin passed on base (green-before-green),
confirming the D-2 rows match the implementation.

## Residue

None. No D-2 row disagreed with the measured code, so no residue row is owed and
`core.py` carries no behavior change beyond the REVIEW-FIX-6 refusal.

## Gates

- `.venv/bin/python -m pytest python/repark/tests/test_df_explain_1.py -q`: exit 0, 13 passed.
- `test_df_batch2.py -k explain`: exit 0, 1 passed.
- `python3 scripts/check_lib_py.py`: exit 0 (4486 ratchet, shared with REVIEW-FIX-6).
- `make verify`: exit 0 (see handback).
- Comment fence over the staged diff: clean.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-11
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each D-2 row walked pin by pin against the measured code. Cost and analyze print metrics under the physical header with no logical header; codegen appends the exact trailing note; simple prints physical-only in both spellings; extended joins sections with a blank line. All green on the landed tree and on base (green-before-green, recorded in C-006).
      artifacts: [python/repark/tests/test_df_explain_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The mode domain is the risk surface. Exercised: default, simple, extended (True and mode string), formatted (mode kwarg and positional-extended), cost, analyze-lowered, codegen, unknown string (still raises naming the five modes, pre-existing pin), and the three both-set refusal shapes from REVIEW-FIX-6.
      artifacts: [python/repark/tests/test_df_explain_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Cost/analyze execute the plan (EXPLAIN ANALYZE) on the three-row VALUES frame; the pins assert header shape only, so no cleanup state exists. Mode validation precedes scratch-view creation (unchanged code), so unknown modes leak no view.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no ordering assumption, no async spawn and no lock. Rendering is a pure function of the returned rows plus the codegen note.
    - id: AT-5
      status: N/A
      justification: No privileged action, no auth surface, no secret, no deserialization and no path handling. No error-contract change in this unit (the refusal is REVIEW-FIX-6's).
    - id: AT-6
      status: ATTACKED
      evidence: No export-surface change and no output change: the pins assert behavior #427 already implemented. The batch2 explain neighbor pin greens unchanged.
      artifacts: [python/repark/tests/test_df_batch2.py]
    - id: AT-7
      status: N/A
      justification: Header-shape assertions over a three-row plan; no data scan, no loop, no allocation that scales with input size.
    - id: AT-8
      status: ATTACKED
      evidence: The upstream contract is DataFusion's EXPLAIN surface as measured in DF-EXPLAIN-1 (FORMAT TREE pass-through, Plan-with-Metrics rows, newline-terminated plan texts). Re-read the DF-EXPLAIN-1 card and its C-002 measurement notes before pinning, per the round notes; every pin matches the recorded measurement. Live-Spark measurement explicitly out of scope per the brief.
      artifacts: [task/ledgers/completed/df-explain-1-ledger.md, python/repark/tests/test_df_explain_1.py]
    - id: AT-9
      status: N/A
      justification: No failure path to diagnose and no logging change; the printed plan is the diagnosis surface and is unchanged.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first run recorded (1 failed on base — the REVIEW-FIX-6 pin, not a mode pin — with output pasted in review-fix-6/C-001); every clause names its pin; citations carried in python/repark/tests/map.md; no baseline movement owed by this unit (the 4486 ratchet is REVIEW-FIX-6's C-004).
      artifacts: [task/ledgers/staging/review-fix-11-ledger.md, python/repark/tests/map.md]
```
