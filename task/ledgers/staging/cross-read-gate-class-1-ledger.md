# Unit ledger — CROSS-READ-GATE-CLASS-1 · the cross-read gate accepts only the unsupported-type condition

**Date:** 2026-10-28 · **Branch:** `fix/cross-read-gate-class-1` · **Base:** `3ce17f73`
(`origin/main`) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

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
| C-004 | The branch merges with its records, and AUD-005 closes. | The merge of `fix/cross-read-gate-class-1`; the registry line, the label-ledger row and the card status land in the same unit. | OPEN | Built 2026-10-28, awaiting verification and merge. |

## Red-first record (2026-10-28)

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
rows in [ice-spark-cross-read-1-ledger.md](ice-spark-cross-read-1-ledger.md) and
[ice-v2-label-1-ledger.md](ice-v2-label-1-ledger.md), and the card status.
