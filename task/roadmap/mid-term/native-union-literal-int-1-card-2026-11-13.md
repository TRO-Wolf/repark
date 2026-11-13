# Card NATIVE-UNION-LITERAL-INT-1: a native `INSERT` of an `INT` column unioned with a bare integer literal fails

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) delta verifier, 2026-11-13 (finding D1); verifier 1, 2026-11-13 (F3); verifier 2, 2026-11-13 (F3).

**Status:** filed 2026-11-13, not dispatched. Severity **S3**: a loud failure; a `CAST` works around it.

## Measured

- **Fails:** native `INSERT INTO <t> SELECT id FROM <c>.ns.src UNION SELECT 9` into an `INT` column, on the memory
  catalog `ice` and the hadoop catalog `hd`, with `Internal error: cannot convert text Int64 into byte Int32`. The
  table is left unchanged. `… UNION ALL SELECT 9 AS id` fails the same way (verifier 1).
- **Stores:** `SELECT 1 UNION SELECT 9` (rows 1, 9), `SELECT 1 AS id UNION ALL SELECT 9 AS id`,
  `SELECT CAST(1 AS INT) UNION SELECT CAST(9 AS INT)`, and `SELECT id FROM <c>.ns.src UNION SELECT CAST(9 AS INT)`
  (rows 1, 2, 3, 9).
- **Spark 4.1.2** and RePark's Spark-door CTAS both store `(1),(2),(3),(9)`.

So the failing shape is an `INT` column arm unioned with an untyped integer literal arm.

**Builds:** #1317's delta head `dbd86fd6` (delta verifier); #1317's head `ed9f678c` and its base, main `a3f8c7d9`,
identical (verifiers 1 and 2).

## Why it matters

[NATIVE-CTAS-ICEBERG-REFUSE-1](native-ctas-iceberg-refuse-1-card-2026-11-12.md)'s refusal text tells native users to
`CREATE TABLE` and then `INSERT … SELECT`; for this shape the `INSERT` needs the `CAST`.

## Fix direction

None given by the source; the verifiers record it as a pre-existing DataFusion issue.

## Close

The failing shape stores Spark's rows on the native door, or a recorded ruling keeps the `CAST` as the stated
workaround, with the cell pinned either way.
