# Card NS-ARRAY-FLOOR-1: floor a nanosecond datetime array to microseconds under `array<timestamp>`, refuse it under `array<timestamp_ntz>`

**Date:** 2026-10-13. **Filed by:** the docs clerk lane, from verdict 2 of draft PR
#1047 and the ledger of `fix/ns-array-apply-in-pandas-refuse-1`.

**Status:** open. Target v1.5.6. A redesign that replaces draft PR #1047.

Parity row: ICE-TSNS-SQL-1-R-020 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

Grouped `applyInPandas` and `cogroup().applyInPandas` return a `datetime64[ns]` array
under a declared `array<timestamp>` or `array<timestamp_ntz>`; RePark copies the
nanosecond ticks and reads them as microseconds (unit reinterpretation: the stored
instant is wrong by a factor of 1000 in ticks). Spark 4.1.2 measured (verdict 2 of
#1047, 2026-10-13): floors the array to microseconds under `array<timestamp>` (ticks
`[1767323045123456, 1767323045000000]`), refuses it under `array<timestamp_ntz>`
(`ArrowNotImplementedError: Expected np.datetime64 but got: timestamp[ns]`).

## The ask

- Floor a nanosecond datetime array to microseconds under `array<timestamp>`, and refuse
  it under `array<timestamp_ntz>`, as Spark 4.1.2 measured.
- The decision is driven by the dtype (a `datetime64[ns]` array at a declared
  list-of-timestamps position), never by integer comparison.
- Integer cells stay as main: a Python integer under a declared timestamp stores as a
  microsecond tick, on main and on Spark alike.

## Why the refusal design of #1047 was dropped

From verdict 2: its integer rule refused a correct function-made microsecond integer
when it equalled a nanosecond tick of the group frame (a `bigint` column holding
`1767323045123456` returned as `{'t': int(u)}` under `struct<t:timestamp_ntz>` refused,
though main and Spark 4.1.2 store it as microsecond ticks), and following its hint
literally (`value // 1000`) then stored a wrong value (`1767323045123`, and `[0, 0, 0]`
for a row-counter frame). An integer cannot carry a unit.

## Out of scope

- Nanosecond values inside struct and map fields: card NS-NESTED-PANDAS-1.
- The `pandas_udf` doors and the top-level nanosecond column: as main.
