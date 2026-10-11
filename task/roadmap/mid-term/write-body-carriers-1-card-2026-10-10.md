# Card WRITE-BODY-CARRIERS-1: a write body inside `CREATE TABLE … AS` refuses at parse; `EXPLAIN ANALYZE` and `PREPARE` / `EXECUTE` keep running their write

**Date:** 2026-10-10. **Filed by:** the v1.5.4 docs lane (Claude Haiku 5.5), from the owner's ruling of 2026-10-10.

**Status:** open. Target not assigned.

**Related:** the statement walk in `executed_writes` (`crates/repark-spark/src/router/nested_ns.rs`),
which card ICE-TSNS-NESTED-1 records as the walk the nested unit must keep:
[ice-tsns-nested-1-card-2026-10-09.md](ice-tsns-nested-1-card-2026-10-09.md).

## Why

Owner ruling, 2026-10-10: `CREATE [OR REPLACE] TABLE z AS INSERT INTO t …` (a write body inside
CTAS) must refuse. Spark rejects that statement at parse.

Today RePark commits the inner `INSERT` and then fails with `UInt64 is not supported`, on any
table. The refusal arrives after the write has committed, so a statement that fails has still
written its rows.

`EXPLAIN ANALYZE <write>` and `PREPARE … AS <write>` / `EXECUTE` run the write body. That is what
those verbs mean in RePark. Spark has neither verb, so there is no parity claim for either. This
card documents both behaviours; it does not change them.

## The ask

- Refuse `CREATE [OR REPLACE] TABLE z AS INSERT INTO t …` before any file is written and before
  any commit. The refusal names the statement and not the internal error text.
- Measure Spark 4.1.2 on the CTAS shape before the refusal lands, and record Spark's class and
  text in the ledger. The refusal's text follows Spark's measured answer.
- Document `EXPLAIN ANALYZE <write>` and `PREPARE` / `EXECUTE` as running the write body, in the
  parity document, as rows with no Spark parity claim. The owner's ruling already settles that
  they run; the card records it.
- Keep the walk that finds writes inside wrapper statements (`executed_writes`) for every spelling
  that still runs. Only the CTAS shape changes.

## Recorded, not fixed by this card

- At top level, 36 of 48 `EXPLAIN ANALYZE INSERT` cells into a nanosecond column from another
  timestamp type fail with a DataFusion internal error. The cause is that this path skips the
  INSERT conform. Measured on main, and at head.
- Whether `EXPLAIN ANALYZE` should execute a write at all is the ledger's question Q14, wider than
  this card. The owner's ruling above keeps the execution; Q14 is not closed by this card.

## Measured 2026-10-10 (R-008 verify)

Twenty cells, identical on main and at the R-008 head: `EXPLAIN ANALYZE INSERT`,
`EXPLAIN ANALYZE VERBOSE`, `EXPLAIN ANALYZE INSERT OVERWRITE` and `PREPARE` +
`EXECUTE` (insert and overwrite) store a naive `timestamp_ns` wall into a top-level
`timestamptz_ns` column as if it were UTC, in the four non-UTC zones; INSERT localises
it. From `TIMESTAMP`, `TIMESTAMP_NTZ` and `DATE` sources those routes raise DataFusion's
internal error on both builds. These verbs do not merely run the body, they store a
different instant than INSERT for this shape, so this card's fix must cover them (refuse
by name or route through the same doors) rather than only document them.

## Gates

- The CTAS shape with `CREATE TABLE` and `CREATE OR REPLACE TABLE`, on a new table and on an
  existing one: the refusal fires before the inner `INSERT` commits, and the table holds the
  rows it held before the statement (read back from the table).
- `EXPLAIN ANALYZE INSERT`, `UPDATE`, `DELETE` and `MERGE` and `PREPARE` / `EXECUTE` answer as
  main answers them. Pinned, so the documented behaviour cannot drift.
- Every other wrapper spelling that carries a write keeps its walk.
- An Opus verifier on the product pull request.
