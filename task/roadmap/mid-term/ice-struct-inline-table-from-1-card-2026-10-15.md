# Card ICE-STRUCT-INLINE-TABLE-FROM-1: an inline table in FROM stores a reordered struct by name

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 107 (a).

**Status:** open. Not this round. In the struct-pairing family (ruling 57: column-list VALUES pairs by position).

**Retires:** when the inline table pairs by position and disagreeing names refuse with Spark's
`INVALID_INLINE_TABLE`, with pins.

**Registry row:** [ICE-STRUCT-INLINE-TABLE-FROM-1](../../../docs/spark-sql-iceberg-parity.md)
(dated row 2026-10-15, owner ruling 107 (a)).

## Risk

An inline table in `FROM` stores a reordered struct by name where Spark stores it by position.

## Sequence

`INSERT INTO t SELECT * FROM VALUES (…), (…)` (an inline table in `FROM`) stores a struct
whose fields are reordered from the table's by field name; Spark pairs by position. Rows whose
field names disagree refuse with a raw `Unsupported CAST` text, not Spark's
`INVALID_INLINE_TABLE`. Plain `INSERT … VALUES`, `SELECT` literals and `UNION ALL` store as
Spark on main since #1062. Main and #1062's head are the same.

## Note

Measured by U-6's re-verifier 3, 2026-10-15. Column-list VALUES pairs by position under ruling 57.
