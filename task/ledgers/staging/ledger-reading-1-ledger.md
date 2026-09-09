# Unit ledger — LEDGER-READING-1 · reading units may prove clauses on document evidence (R-10)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when LEDGER-READING-1 merges.

**Unit:** LEDGER-READING-1 step 1 · **Date:** 2026-09-09 · **Model:** glm-5.3-flash · **Branch:** `feat/ledger-reading-1`
**Home:** [scripts/check_ledger_grammar.py](../../../scripts/check_ledger_grammar.py),
[test_dl_2_ledger_grammar.py](../../../python/repark-parity/tests/test_dl_2_ledger_grammar.py),
[docs/testing.md](../../../docs/testing.md) "Pinning a charter clause"

## Clause table

| Clause | Decision | Verdict | Evidence (red-first output, diff, gates) |
|---|---|---|---|
| C-001 | D-1: the READING value of the `Path` header field, anywhere in a staging ledger's first 40 lines, exempts every clause of that ledger from rule B; rules A and C stay armed | PROVEN | Red first: `.venv/bin/python -m pytest python/repark-parity/tests/test_dl_2_ledger_grammar.py -q` on the base script → `1 failed, 13 passed`, `test_reading_marker_exempts_rule_b` red with `task/ledgers/staging/u11-reading-charter-ledger.md: 1 PROVEN clause(s) with no \`pins: u11-reading-charter/C-NNN\` citation (ceiling 0): C-001`. Green after the script change: one header regex (`READING_MARKER` over the first `HEADER_LINES` = 40 lines), one branch guarding the `proven` recording in `run()`. The no-marker control and the no-attestation case pin that rules B and C stay armed on the same shape. |
| C-002 | D-2: the `EXCEPTIONS` table is untouched | PROVEN | The script diff adds no `EXCEPTIONS` hunk; `fnp-0-charter-ledger.md` (12, False) and `v3-0-charter-ledger.md` (0, False) are byte-identical; `test_exceptions_table_ratchets_down_only` stays green against the real tree. |
| C-003 | D-3: a reading clause's evidence cell is `docs: <path>#<heading-anchor>`; the grammar gate reads no further into the cell (the anchor is `make check-docs-links`' job) | PROVEN | The shape is recorded in docs/testing.md "Pinning a charter clause" as a convention; the script diff adds no evidence-cell check (rule A already requires a non-empty evidence cell); the fixture clause row carries the shape and the full file is green. |

## Notes

Step 1 only. Step 2 flips BALLISTA-AUDIT-0's twelve clauses to `PROVEN` on `docs:` cells and
re-points its attestation at the document sections. D-3's "only the prefix" is read as a
convention the gate does not enforce: the card bounds the step-1 script change at one header
regex and one branch, and a cell-prefix check would need a cell-position contract the card does
not give. This ledger does not carry the marker itself: it adds tests, so its clauses are pinned
the ordinary way.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ledger-reading-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each decision was walked as an executable case on a scratch tree — marker exempts rule B, no marker reds rule B with the standing message, marker without an attestation reds rule C — plus the real-tree ratchet test for the untouched EXCEPTIONS table.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py, scripts/check_ledger_grammar.py]
    - id: AT-2
      status: N/A
      justification: No numeric claim is made; the unit's assertions are gate behavior, pinned in both directions rather than measured.
    - id: AT-3
      status: N/A
      justification: No failure path is added; the gate's failure reporting is the subject and is asserted verbatim in the tests.
    - id: AT-4
      status: N/A
      justification: No state, ordering or concurrency is introduced; the script stays a single-pass reader.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, deserialization or path handling is added.
    - id: AT-6
      status: ATTACKED
      evidence: The hole the exemption could open was attacked with a control pair — the identical ledger without the marker still reds rule B, and the marker ledger without an attestation still reds rule C — so the marker alone cannot silence the gate.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-7
      status: N/A
      justification: No execution path or resource behavior changes; the gate still reads each ledger once.
    - id: AT-8
      status: N/A
      justification: No API, dependency or upstream contract is touched; the marker reuses the ledger header field ledgers already carry.
    - id: AT-9
      status: N/A
      justification: Nothing new to diagnose; the findings messages are asserted in the tests, so a wording regression is red.
    - id: AT-10
      status: ATTACKED
      evidence: The three new tests pin C-001 and the two armed-rule halves, the tests map carries the pins line, and the file ran red on the base script then green after — the base script is the mutant for the new branch.
      artifacts: [python/repark-parity/tests/map.md]
  complete: true
```
