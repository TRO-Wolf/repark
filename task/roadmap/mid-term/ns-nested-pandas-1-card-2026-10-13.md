# Card NS-NESTED-PANDAS-1: nanosecond values inside struct and map fields returned from `applyInPandas`

**Date:** 2026-10-13. **Filed by:** the docs clerk lane, from verdict 2 of draft PR
#1047 and the ledger of `fix/ns-array-apply-in-pandas-refuse-1`.

**Status:** open, unscheduled. Severity S0 on main.

Parity row: ICE-TSNS-SQL-1-R-020 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

`Table.to_pandas()` gives a `timestamp[ns]` that is a direct field of a struct, or a
direct key or value of a map, to the function as a Python integer (`{'t':
1767323045123456789}`, `[('k', 1767323045123456789)]`); a `timestamp[us]` there is a
`datetime`. No `to_pandas` option changes that except `types_mapper=ArrowDtype`. On the
way back `from_pandas` takes a Python integer under a declared timestamp as a tick of
the declared unit, at a top-level position too. So identity over
`named_struct('t', ns)`, `create_map(lit('k'), ns)`, `array(named_struct('t', ns))` and
`array(create_map(…))` stores `1767323045123456789` as microseconds, with no error.
A struct holding a nanosecond array answers values that raise `OverflowError: date
value out of range` on read.

## The ask

- Decide what the struct and map fields holding nanosecond values answer through the
  grouped and cogrouped `applyInPandas` doors: floor, refuse, or disclose — measured
  against Spark 4.1.2 first, as NS-ARRAY-FLOOR-1 does for the array door.
- Cells from the same verdict and ledger: struct fields, map keys and map values at any
  depth, in every container spelling pyarrow accepts; the zero-dimensional array, the
  bytes-keyed dict, and the integer-subclass spellings the #1047 verdict lists.
- The array door itself is card NS-ARRAY-FLOOR-1, not this card.

## Out of scope

- The top-level nanosecond column (`PySparkException` with `would lose data`): as main.
- Microsecond data, `ArrowDtype` columns, lists of `pd.Timestamp` or `datetime`: as main.
