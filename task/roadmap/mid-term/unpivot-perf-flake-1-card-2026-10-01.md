# UNPIVOT-PERF-FLAKE-1 — timing-bound unpivot linearity test flakes on shared runners

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 80; CI
flake). **Severity:** not rated. **Status:** pre-existing. **Target:**
mid-term.

## The gap it records

`python/repark/tests/test_perf_unpivot_1.py::test_stack_is_linear_in_columns`
is timing-based and fails on shared CI runners with no product cause. It hit
#891 (exponent 1.224) and #892 (1.120); neither PR changes unpivot. Needs a
noise-robust bound, e.g. median of more reps, or a ratio against a same-run
control.

Test file, verified present at base `02c2f1be`:
`python/repark/tests/test_perf_unpivot_1.py` (the named test at line 131).

## Suggested direction (a suggestion, not a decision)

Replace the wall-clock bound with a noise-robust one (median of more reps, or
a ratio against a same-run control). This direction is suggested by the
verifier/orchestrator, not a decision.

## Clauses (draft)

- **C-001** The test passes on a loaded shared runner with no product change.
- **C-002** A genuine superlinear regression still fails the new bound (validated by a seeded slowdown).
- **C-003** No product-code change ships with this card.

## Pointers

- Up: [map.md](map.md)
