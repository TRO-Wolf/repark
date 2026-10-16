# Card ICE-TSNS-UPDATE-NTZ-CAST-1: a written `CAST(ns AS TIMESTAMP_NTZ)` in `UPDATE … WHERE` stores what the no-`WHERE` form stores

**Date:** 2026-10-15. **Filed by:** the ICE-TSNS-TYPE-PLANNER-1 (U-1) carry-over fix round,
from question Q1 of the fresh build's hand-back and the orchestrator's provisional ruling on
it (2026-10-15, the owner told): a written `CAST(ns AS TIMESTAMP_NTZ)` in `UPDATE … WHERE` is
outside ruling 91, which rules `TIMESTAMP` targets; it gets its own card and four dated rows,
no code in U-1.

**Status:** open. No target yet.

**Related:** parity row ICE-TSNS-SQL-1-R-017 in
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) (the line
"Three written casts on that door disagree"), U-1's ledger
[ice-tsns-type-planner-1-ledger.md](../../ledgers/staging/ice-tsns-type-planner-1-ledger.md)
(Q1), and the dated rows of `python/repark/tests/ice_tsns_type_planner_1_dated.json`.

## Why

On U-1's branch, `UPDATE t SET v = CAST(c AS TIMESTAMP_NTZ)` over a nanosecond `c` into a
`timestamp_ns` or `timestamptz_ns` column floors to microseconds (`-1 ns` stores `-1000`),
while the same statement with `WHERE id >= 0` truncates (`-1 ns` stores `0`); in New York
the zoned target's wall differs too. The identity `UPDATE … WHERE` path re-plans the `SET`
text with plain DataFusion SQL, where a written NTZ cast over nanoseconds is an Arrow cast;
the floor lives in the Spark door's NTZ lowering. Today's main refuses the `WHERE` form
(TZ-6), so the cells are red on main too. Apache Spark has no nanosecond type; the rule is
the floor U-3 rules for every write door.

## Cells (dated on U-1's branch, red on today's main)

`repro|r017_update_nowhere_ntz_cast|{ns,tzns}|{UTC,America/New_York}` of
`python/repark/tests/test_ice_tsns_type_planner_1.py`.

## Done when

The four cells are green, their dated rows are retired, and the `WHERE`, no-`WHERE` and
written-cast forms store one value on every door.
