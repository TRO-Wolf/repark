# CROSS-READ-GATE-CLASS-1: the cross-read gate accepts only the unsupported-type class of Spark failure as an expected nanosecond limitation (RePark card)

**Filed:** 2026-10-28. **Filed by:** the audit clerk round (`claude-haiku-5-5`), from the independent audit ([iceberg-v2-label-independent-audit-2026-10-28.md](../../../docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md)) and the orchestrator's reproduction of 2026-10-28.

**Status:** built 2026-10-29 on branch `fix/cross-read-gate-class-1`, awaiting verification and merge. Registry note: [ICE-COMMIT-UNKNOWN-1](../../../docs/spark-sql-iceberg-parity.md), the dated AUD-005 line.

**Grade:** not graded yet. It changes a test helper, not a product path.

**Severity:** Low under the audit's scale (AUD-005). A reproduced gate defect, not a reproduced product wrong result. It weakens shared cross-read assurance for the nanosecond surface; it is not proof that ordinary V2 cross-read runs failed unnoticed.

## Why

`python/repark/tests/parity/_acceptance_cross_read.py`, lines 298 to 314, classifies every Spark exception as `SPARK-CANNOT-READ`. Lines 376 to 384 accept that outcome whenever the table has nanosecond columns, without checking the exception against a known unsupported-type condition. A connection, authorization, catalog-initialization or malformed-file failure therefore passes verification as the expected nanosecond limitation. The helper's own contract and the label ledger's catalog-init exclusion (C-010) both say it must not.

## What was measured

Audit Appendix C calls the real helper with an in-memory Arrow nanosecond table and a stand-in Spark that raises `ConnectionError`. No connection was attempted. The helper accepts: `gate: accepted`, `status: SPARK-CANNOT-READ`, `error_class: ConnectionError`, `has_ns_columns: true`. The non-nanosecond control with the same connection failure is rejected.

## The fix

Accept `SPARK-CANNOT-READ` only for the specific unsupported-type condition the nanosecond contract names. Every other Spark exception fails verification whatever the timestamp precision. The change is in the test helper only.

## Pins

- The helper rejects the simulated connection failure on a nanosecond table, and accepts the unsupported-type condition on the same table.
- The non-nanosecond control keeps its current result.

## Out of scope

- Any product change to the nanosecond surface (the registry rows own it).
- Whether a past cross-read run met a real connection failure; the audit makes no claim that one did.

Up: [map.md](map.md).
