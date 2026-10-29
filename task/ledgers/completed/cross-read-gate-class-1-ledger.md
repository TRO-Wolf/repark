# Unit ledger — CROSS-READ-GATE-CLASS-1 · the cross-read gate accepts only the unsupported-type condition

**Date:** 2026-10-29 · **Branch:** `fix/cross-read-gate-class-1` · **Base:** `3ce17f73`
(`origin/main`) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retired:** this ledger moved to `../completed/` in the post-merge records commit of 2026-10-29, after PR #1211 merged as `7ef6bcf4`.

**Why now.** The independent audit of 2026-10-28 (AUD-005) reproduced a gate
defect: the cross-read helper classifies every Spark exception as
`SPARK-CANNOT-READ` and accepts that outcome on any nanosecond table, so a
connection failure passes as the expected nanosecond limitation. This unit
narrows the accepted outcome to the recorded unsupported-type condition. Card:
[../../roadmap/mid-term/cross-read-gate-class-1-card-2026-10-28.md](../../roadmap/mid-term/cross-read-gate-class-1-card-2026-10-28.md).

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The gate accepts `SPARK-CANNOT-READ` only when the table has nanosecond columns and the Spark first error line is the recorded prefix followed by `timestamp_ns` or `timestamptz_ns`; a first-line match with stack text after stays accepted, while a second-line-only condition and a variant name are rejected. | The narrowed predicate plus its red-first pins, each failing on the pre-fix helper where the brief says red. | PROVEN | 11 passed on the branch; the 4 rejection pins failed on the pre-fix helper (see the red-first record). Pins: `test_cross_read_gate.py::test_timestamp_ns_condition_on_a_nanosecond_table_is_accepted`, `test_cross_read_gate.py::test_timestamptz_ns_condition_on_a_nanosecond_table_is_accepted`, `test_cross_read_gate.py::test_multiline_message_matching_on_its_first_line_is_accepted`, `test_cross_read_gate.py::test_condition_only_on_the_second_line_is_rejected`, `test_cross_read_gate.py::test_variant_condition_on_a_nanosecond_table_is_rejected`. |
| C-002 | The audit's connection-failure probe on a nanosecond table is rejected with the error class in the message, an authorization failure on the same table is rejected, and the microsecond control rejects both the connection failure and the condition text. | The probe pin plus the authorization and microsecond-control pins, red on the pre-fix helper for the nanosecond shapes. | PROVEN | The probe and authorization pins failed on the pre-fix helper; the microsecond controls passed before and after. Pins: `test_cross_read_gate.py::test_connection_failure_on_a_nanosecond_table_is_rejected`, `test_cross_read_gate.py::test_authorization_failure_on_a_nanosecond_table_is_rejected`, `test_cross_read_gate.py::test_connection_failure_on_a_microsecond_table_is_rejected`, `test_cross_read_gate.py::test_unsupported_condition_text_on_a_microsecond_table_is_rejected`. |
| C-003 | `EQUAL` on a matching pair stays accepted and `DIFFERENT` on a row mismatch stays rejected. | The unchanged-behaviour control pins, green before and after. | PROVEN | Both pins passed on the pre-fix and the fixed helper. Pins: `test_cross_read_gate.py::test_equal_pair_is_accepted`, `test_cross_read_gate.py::test_different_pair_is_rejected`. |
| C-004 | The branch merges with its records, and AUD-005 closes. | The merge of `fix/cross-read-gate-class-1`; the registry line, the label-ledger row and the card status land in the same unit. | PROVEN | Merged 2026-10-29 as `7ef6bcf4` (PR #1211); closed by C-005, the merge clause. |
| C-005 | The unit merged on 2026-10-29 as PR #1211, squash `7ef6bcf4`, after a Muse MAX critic PASS at `845c32bd`, with the two S3 observations of C-006 and no change made for them. | `gh pr view 1211 --json mergeCommit`, copied from the brief; the clerk ran no `gh`. | PROVEN | pins: `7ef6bcf4` (the merge commit on main, PR #1211). The citation the grammar gate counts is in the tests map row for `test_cross_read_gate.py` (`python/repark-parity/tests/map.md`) and in this unit's completed map pins line; this clause measures nothing itself. |
| C-006 | OBSERVATION, S3, dated 2026-10-29, from the Muse MAX critic at `845c32bd`; not fixed. (a) Truncation shape: a Spark first error line longer than 300 characters whose non-matching text falls after a run of 246 or more spaces is cut by the helper's pre-existing first-line cap (`_MAX_ERROR_LINE_CHARS`) and would pass the gate. No real Spark message has that shape. (b) Three commit messages on the branch carried a trailing blank line after the trailer; cosmetic. | The critic's two measurements, copied; the clerk re-ran nothing. | OPEN | Closing question: does the owner want the first-line check made independent of line length (a pin for the 300-plus-character shape, red on the current cap), or is the shape declined as not real? |

VERDICT (2026-10-29, merge): 6 clauses. 5 PROVEN (C-001 to C-005), 1 OPEN (C-006, the two S3 observations; nothing changed for them).

## Red-first record (2026-10-29)

With the pre-fix helper checked out (`HEAD~1`), the new file ran 4 failed and
7 passed: the failures were the audit probe, the second-line-only condition,
the authorization failure and the variant condition, exactly the brief's red
set. With the fix restored, the file runs 11 passed. The progress file
(`test_cross_read_progress.py`, 24 passed) predicts with the loaded real
predicate, so its `SPARK-CANNOT-READ` cells follow the narrowed rule.

## Neighbours

The live legs (`test_acceptance_cross_read_local.py`, `test_aws_acceptance_cross_read.py`)
take the narrowed assertion; their recorded nanosecond outcome (the
`timestamp_ns` condition) stays accepted, and no other Spark failure passes.
They need live Spark and AWS, so this lane does not run them. The pure
neighbours (`test_acceptance_cross_read_types.py`, `_acceptance_cross_release.py`)
import only the untouched compare helpers.

## Records

The narrowed helper and its dated map lines, the registry AUD-005 line
([spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)), the appended
rows in [ice-spark-cross-read-1-ledger.md](../staging/ice-spark-cross-read-1-ledger.md) and
[ice-v2-label-1-ledger.md](../staging/ice-v2-label-1-ledger.md), and the card status.
