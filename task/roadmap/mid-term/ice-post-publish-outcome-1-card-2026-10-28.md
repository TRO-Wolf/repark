# ICE-POST-PUBLISH-OUTCOME-1: a CTAS whose post-publish catalog refresh fails reports failure for a committed table (RePark card)

**Filed:** 2026-10-28. **Filed by:** the audit clerk round (`claude-haiku-5-5`), from the independent audit ([iceberg-v2-label-independent-audit-2026-10-28.md](../../../docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md)) and the orchestrator's reproduction of 2026-10-28.

**Status:** dispatched 2026-10-28 on an Opus 5.5 builder lane, the same lane as ICE-DELETE-FORMAT-GUARD-1. Open until the merge. Registry row: [ICE-POST-PUBLISH-OUTCOME-1](../../../docs/spark-sql-iceberg-parity.md).

**Status update, 2026-10-28:** built as PR #1206, awaiting verification.

**Grade:** product unit, Opus 5.5 builder lane.

**Severity:** Medium, the audit's rating (AUD-002). A fail-loud outcome-reporting gap: the table is fully published before the error. No partial snapshot, library panic, resurrected row or data loss was demonstrated.

## Why

`CREATE TABLE … AS SELECT` publishes the staged table through the catalog, then refreshes the provider. In `crates/repark-spark/src/ctas.rs` (around lines 312 to 324) the staged commit finishes and a failure from `reregister` propagates as the statement's error. The refresh in `crates/repark-iceberg/src/catalog/provider.rs` (around lines 241 to 254) makes fallible catalog calls after the publish. A caller that sees the error and retries does not know that the table exists.

## What was measured

Audit Appendix E. A catalog wrapper forwards every operation to the real fork memory catalog and fails the next `list_tables` call, immediately after `publish_create_table` succeeds. The statement `CREATE TABLE … USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT CAST(7 AS INT) AS id` returns `Unexpected => AUDIT: catalog listing unavailable after publication`, while the published table has one row and a current snapshot. The probe's panic is its own assertion; the library returned an error and did not panic.

Only CTAS was reproduced. The audit's section 4 names the other error-after-commit paths (CTAS/RTAS and the other mutation paths) as an unmeasured lead.

## The fix

The audit's expectation: the API communicates the committed outcome accurately, including the refresh problem, and a caller's retry does not depend on the hidden post-publish stage. How the outcome is reported is the builder's choice within that expectation. A change of error class is a question for the orchestrator before it lands.

## Pins

- CTAS with one listing failure after the publish: the returned outcome matches the committed state (readable, one row, current snapshot), and a retry is safe.
- The same schedule for `CREATE OR REPLACE TABLE … AS SELECT` and `REPLACE TABLE … AS SELECT`, which the audit did not reproduce.
- Each pin asserts the returned outcome, the committed rows and the retry result, not only that an error was returned.

## Out of scope

- Unknown commit outcomes, which are ICE-COMMIT-UNKNOWN-1 and pinned elsewhere.
- The delete-format guard, ICE-DELETE-FORMAT-GUARD-1, which shares the lane but changes other code.

Up: [map.md](map.md).
