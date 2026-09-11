# Unit ledger — REVIEW-FIX-15B · citations live in map.md, never in code

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when REVIEW-FIX-15B merges, or when the owner closes the slate row.

**Unit:** REVIEW-FIX-15B · **Date:** 2026-09-11 · **Executor:** Devin SWE-2 (swe-2-high), Actor ·
**Branch:** `fix/review-fix-15b` · **Base:** `bf810824`
**Model:** swe-2-high
**Card:** card REVIEW-FIX-15b in `task/roadmap/mid-term/cheap-tier-slate-2-2026-09-09.md` (S2-14)
**Home:** `python/repark-parity/tests/test_dl_2_ledger_grammar.py`, `python/repark-parity/tests/map.md`

Red first per clause below: the base state fails the card's requirement, the
edited tree passes it. Suite form: `.venv/bin/python -m pytest
python/repark-parity/tests/test_dl_2_ledger_grammar.py -q`.

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | D-1 No `_pins` binding remains in tracked code and no replacement construct (constant, decorator, marker, comment) is introduced: `grep -rn '_pins = "' python/ crates/ scripts/` prints nothing. | Red on base `bf810824`: `grep -rn '_pins = ' python/ crates/ scripts/` prints the four bindings in `test_dl_2_ledger_grammar.py` (lines 150, 157, 204, 284) plus two unrelated `lineage_pins` locals (`crates/repark-sql/src/router.rs:37`, `crates/repark-spark/src/router.rs:68`) — the pin is therefore tightened to the exact shape `_pins = "…"`, which on base prints exactly the four bindings. Green after: `grep -rn '_pins = "' python/ crates/ scripts/` prints nothing; the diff is four deleted lines with no replacement construct; the test functions stay otherwise byte-identical; suite `17 passed`. | **PROVEN** |
| C-002 | D-2/D-3 The citations those bindings carried (`ledger-reading-1/C-001, C-002, C-003`; `review-fix-15/C-001, C-002`) and this unit's own (`review-fix-15b/C-001, C-002`) resolve from `python/repark-parity/tests/map.md`, and `python3 scripts/check_ledger_grammar.py` is clean. | Red first — `_pins` lines deleted with the map untouched, gate run verbatim (stderr): task/ledgers/completed/review-fix-15-ledger.md: 2 PROVEN clause(s) with no `pins: review-fix-15/C-NNN` citation (ceiling 0): C-001, C-002 / ledger-grammar: FAIL — 1 finding(s) (exit 1). Green after the map edit: the DL-2 bullet carries `pins: review-fix-15/C-001, C-002` and `pins: review-fix-15b/C-001, C-002` on their own lines beside the existing `pins: ledger-reading-1/C-001, C-002, C-003, C-004` line, and the REVIEW-FIX-15 paragraph now states citations live in map.md per S2-14 rather than describing `_pins` as the convention. `python3 scripts/check_ledger_grammar.py` prints `ledger-grammar: 95 live ledgers clean (588 clauses, 1218 pinned clause ids, 2 exception rows)` (exit 0); suite `17 passed`; full parity suite `719 passed, 11 xfailed`. | **PROVEN** |

## Notes

The `_pins` near-miss: the card's loose grep `_pins = ` also matches the
`lineage_pins` locals in the two SQL routers; those are ordinary variables, not
citation carriers, so C-001's pin names the binding shape `_pins = "…"`.

## Fix

| Name | Layer |
|---|---|
| in-test citations removed | `python/repark-parity/tests/test_dl_2_ledger_grammar.py`: the four `_pins` bindings deleted, no replacement construct |
| navigation record | `python/repark-parity/tests/map.md` DL-2 bullet: REVIEW-FIX-15 paragraph rewritten per S2-14, `pins: review-fix-15/C-001, C-002` and `pins: review-fix-15b/C-001, C-002` on their own lines |
| Ledger | `task/ledgers/staging/review-fix-15b-ledger.md` (this file) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-15b
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Red-first per clause above; deleting the `_pins` lines with the map untouched reds the gate on review-fix-15's two PROVEN clauses; the map edit turns it green; suite 17 passed before and after.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py, python/repark-parity/tests/map.md]
    - id: AT-2
      status: ATTACKED
      evidence: Old and new citations both resolve — grammar gate clean, no dead citation, no unpinned clause; the ledger-reading-1 citations remain pinned through the standing map line.
      artifacts: [python/repark-parity/tests/map.md]
    - id: AT-3
      status: N/A
      justification: Scratch-tree gate tests only; no product execution path is touched.
    - id: AT-4
      status: N/A
      justification: Sequential pytest over tmp trees; no shared mutable state, no concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, push, gh, or dependency-file change.
      artifacts: [task/ledgers/staging/review-fix-15b-ledger.md]
    - id: AT-6
      status: N/A
      justification: No public API change. Test-body locals deleted only.
    - id: AT-7
      status: N/A
      justification: No live oracle exists for ledger-grammar fixtures; the gate itself is the oracle.
    - id: AT-8
      status: ATTACKED
      evidence: No comment added to code — the change deletes four lines and adds none; the fence grep `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P '^\+\s*(//|#(?! noqa))'` prints nothing.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Ledger listed in its directory map in the same commit; own clauses cited from the DL-2 bullet of python/repark-parity/tests/map.md (the gate reads citations only under crates/, python/, scripts/), gate green.
      artifacts: [task/ledgers/staging/review-fix-15b-ledger.md, task/ledgers/staging/map.md, python/repark-parity/tests/map.md]
```
