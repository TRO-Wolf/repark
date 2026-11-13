# Card NATIVE-CREATE-VIEW-ICEBERG-EMPTY-1: a native `CREATE VIEW` over an empty query creates an Iceberg table

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) verifier 1, 2026-11-13, finding F4.

**Status:** filed 2026-11-13, not dispatched. The source gives no severity (class (c), pre-existing). Filed **S3**:
the statement reports success and leaves a durable empty table where a view was asked. Its first read is right
(empty); later reads miss the rows a view would show.

## Measured

- A native `CREATE VIEW <iceberg>.ns.v AS <empty query>` creates an empty Iceberg **table**, not a view.
- The control, a native `CREATE VIEW` over a query with rows on an Iceberg catalog, keeps the fork's refusal (#1317
  verifiers 1 and 2, check 2).
- An in-memory native `CREATE VIEW` answers rows (control).

**Builds:** #1317's head `ed9f678c` and its base, main `a3f8c7d9`, identical.

## Not known yet

Which empty queries reach it (`WHERE false`, `LIMIT 0`, an empty source), and which catalog kinds.

## Fix direction

None given by the source, which did not root-cause it. A neighbouring native-door record:
[NATIVE-CTAS-ICEBERG-REFUSE-1](native-ctas-iceberg-refuse-1-card-2026-11-12.md).

## Close

A native `CREATE VIEW` into an Iceberg catalog refuses for every query shape, with an empty-query cell pinned.
