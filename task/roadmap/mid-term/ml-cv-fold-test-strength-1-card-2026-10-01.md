# ML-CV-FOLD-TEST-STRENGTH-1 — fold-label test cannot tell use from ignore

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 79;
test strength, finding **VE4-2**). **Severity:** not rated. **Status:**
pre-existing. **Target:** mid-term.

## The gap it records

`test_ml_boost_oracle.py::test_cross_validator_materializes_fold_labels`
cannot tell folds that ignore the materialized view from folds that use it,
because the fold-count query reads the view anyway.

Test file, verified present at base `02c2f1be`:
`python/repark/tests/test_ml_boost_oracle.py` (the named test at line 1231).

## Suggested direction (a suggestion, not a decision)

Strengthen the test so the fold-count query does not read the view (or assert
a signal that only view use produces), so a fold implementation that ignores
the materialized labels fails. This direction is suggested by the
verifier, not a decision.

## Clauses (draft)

- **C-001** A fold implementation that ignores the materialized view fails the test.
- **C-002** A fold implementation that uses the view passes.
- **C-003** No product-code change ships with this card.

## Pointers

- Up: [map.md](map.md)
