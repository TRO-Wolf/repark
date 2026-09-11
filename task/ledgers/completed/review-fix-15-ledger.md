# Unit ledger — REVIEW-FIX-15 · a pin cites the clause it holds

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when REVIEW-FIX-15 merges, or when the owner closes the slate row.

**Unit:** REVIEW-FIX-15 · **Date:** 2026-09-11 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `fix/review-fix-13-15` · **Base:** `7681e8be`
**Model:** muse-spark-1.3-contributor
**Card:** card REVIEW-FIX-15 in `task/roadmap/mid-term/review-1-findings-2026-09-10.md` (Q-48, Q-49)
**Home:** `python/repark-parity/tests/test_dl_2_ledger_grammar.py` and its two siblings, `python/repark-parity/tests/map.md`

Red first per clause below: the base state fails the card's requirement, the
edited tree passes it. Suite form: `PYTHONPATH=python/repark-parity/src uv run
--no-project --with pyarrow --with pytest --with "pydantic>=2.10,<3" pytest
python/repark-parity/tests/test_dl_2_ledger_grammar.py -q`.

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | D-1 LEDGER-READING-1's C-001…C-003 citations move into the tests that pin them; the map.md rows stay as navigation. | Red on base: `grep -c "ledger-reading-1" python/repark-parity/tests/test_dl_2_ledger_grammar.py` prints `0`; the only home is `python/repark-parity/tests/map.md:432` (`pins: ledger-reading-1/C-001, C-002, C-003, C-004`). Green after: four `_pins` bindings in the test bodies — the marker-exemption test cites C-001, C-003; the no-marker and no-attestation controls cite C-001; the EXCEPTIONS ratchet test cites C-002 — suite `17 passed`, `python3 scripts/check_ledger_grammar.py` clean (`87 live ledgers clean (556 clauses, 1186 pinned clause ids, 2 exception rows)`), and the gate resolves all four new citations. C-004 has no pinning test (step 2 wrote none; its pin is the real-tree gate run in its evidence cell), so its citation stays map-only; the map line keeps all four rows as navigation. | **PROVEN** |
| C-002 | D-2 The reading fixture's Verdict and Evidence columns are the way round the grammar describes. | Red on base, both orderings for the record. Fixture `_READING_ROWS` header reads `\| Clause \| Proposition \| Verdict \| Evidence \|` but its row reads `\| C-001 \| The document section exists. \| docs: docs/design/session-api.md#the-session \| PROVEN \|` — the `docs:` evidence in the Verdict position, `PROVEN` in the Evidence position. The convention (`docs/testing.md` "Reading units": each clause names the discharging section "in its evidence cell, shaped `docs: <path>#<heading-anchor>`") and every live ledger put Verdict before Evidence. Green after: the row reads `\| C-001 \| The document section exists. \| PROVEN \| docs: docs/design/session-api.md#the-session \|`; suite `17 passed`, grammar gate clean. The swap is behavior-neutral under the gate (rule A checks shape, not column position), which is why note 3 calls this a direction check. | **PROVEN** |

## Notes

Carrier: the binding is `_pins = "pins: ..."` code-as-data in each test body,
not a `# pins:` comment. The gate matches raw `pins:` text either way, but the
comment ban forbids added `#` lines and no `# pins:` line has been added since
the ban (the file's eight predate it; post-ban units cite from map.md). The map
entry records the convention so the next reader meets it before the tests.

Residue, not widened: `pins: review-fix-10/C-001` is map-only for the three
field-parse tests added by #480 — the same Q-48 pattern one card over. It is
outside D-1's named set (LEDGER-READING-1 C-001…C-004), so it stays, named here
for a follow-up.

## Fix

| Name | Layer |
|---|---|
| fixture order | `python/repark-parity/tests/test_dl_2_ledger_grammar.py` `_READING_ROWS`: Verdict `PROVEN` before Evidence `docs:` |
| in-test citations | Same file: `_pins` bindings on four tests (C-001 ×3, C-002, C-003) |
| navigation record | `python/repark-parity/tests/map.md` DL-2 bullet: REVIEW-FIX-15 paragraph, existing pins line kept |
| Ledger | `task/ledgers/staging/review-fix-15-ledger.md` (this file) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-15
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Red-first per clause above; suite 17 passed before and after; swapped fixture keeps every reading test green.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-2
      status: ATTACKED
      evidence: Old and new citations both resolve — grammar gate clean, no dead citation, no unpinned staging clause introduced.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py, python/repark-parity/tests/map.md]
    - id: AT-3
      status: N/A
      justification: Scratch-tree gate tests only; no product execution path is touched.
    - id: AT-4
      status: N/A
      justification: Sequential pytest over tmp trees; no shared mutable state, no concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, push, gh, or dependency-file change; no comments added to code.
      artifacts: [task/ledgers/staging/review-fix-15-ledger.md]
    - id: AT-6
      status: N/A
      justification: No public API change. Test-body locals only.
    - id: AT-7
      status: N/A
      justification: No live oracle exists for ledger-grammar fixtures; the gate itself is the oracle.
    - id: AT-8
      status: ATTACKED
      evidence: No .rs/.toml/.sh/.yml changed; added .py lines carry no `#` and no docstrings; the fence grep prints nothing.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Ledger listed in its directory map in the same commit; own clauses cited from the pinning tests (the gate reads citations only under crates/, python/, scripts/, so a map-only self-citation would leave rule B red — found the hard way, gate red before the self-pins, green after).
      artifacts: [task/ledgers/staging/review-fix-15-ledger.md, task/ledgers/staging/map.md, python/repark-parity/tests/test_dl_2_ledger_grammar.py]
```
