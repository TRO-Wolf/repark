# Card NATIVE-CONCAT-NULL-1: native `concat` skips a NULL argument where Spark returns NULL

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) verifier 1, 2026-11-13, finding F7.

**Status:** filed 2026-11-13, not dispatched. A native-door card: the native door keeps DataFusion's dialect, and the
owner set its semantics outside the parity claim (2026-11-03). The source gives no severity (class (c),
pre-existing). Filed **S3**: a silent value difference, and it breaks the promise of #1317's refusal text for this
function.

## Measured

- Native `concat(NULL, '_x')` answers `'_x'`; Spark 4.1.2 answers NULL.
- So for a query using `concat` over a NULL, the native two-step workaround that
  [NATIVE-CTAS-ICEBERG-REFUSE-1](native-ctas-iceberg-refuse-1-card-2026-11-12.md)'s refusal names (`CREATE TABLE`, then
  `INSERT … SELECT`) stores rows that differ from Spark's CTAS.

**Builds:** #1317's head `ed9f678c`, recorded as pre-existing against its base, main `a3f8c7d9`.

## Fix direction

None given by the source. The choice is the owner's: Spark's NULL semantics for native `concat`, or a stated
native-door limit, which the refusal text then qualifies.

## Close

The ruling, then either the change with a pinned cell or the stated limit with its pin and the refusal text's
qualifier.
