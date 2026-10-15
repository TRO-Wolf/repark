# Card NS-ENCODED-NESTED-FLOOR-1: floor an encoded or nested nanosecond source into a microsecond column and read its wall in the session zone

**Date:** 2026-10-14. **Filed by:** the docs clerk lane, from finding F4 of the scoped
verifier of PR #1059 (ICE-TSNS-FLOOR-DOORS-1, U-3) and the owner's ruling on question 53
of 2026-10-14.

**Status:** open. Target v1.5.7 (nanosecond carve-out).

**Related:** parity row ICE-TSNS-SQL-1-R-024 in
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md), and rows
R-009 (the top-level plain-array floor, PR #1059), R-014 and R-015 (a nested
`timestamp_ns` target leaf refuses).

## Why

ICE-TSNS-FLOOR-DOORS-1 floors a plain `timestamp[ns]` array narrowed into a microsecond
column and reads its naive wall in the session zone, on every door. Its verifier wrote the
same values from sources the unit does not cover: a dictionary-encoded or run-end-encoded
top-level array, and a nanosecond leaf inside a struct, list or map source, into a
microsecond `TIMESTAMP` or `TIMESTAMP_NTZ` leaf. Those still truncate toward zero and read
the naive wall as UTC on every door that stores them, plain `INSERT` included. `-1 ns`
into `STRUCT<t: TIMESTAMP>` in America/New_York stores `0` where the floor rule stores
`17999999999`; into `TIMESTAMP_NTZ` in UTC a dictionary-encoded `-1 ns` stores `0` where
the floor is `-1`. One table can hold two values for one input, by source layout. Apache
Spark has no `timestamp_ns` type, so there is no oracle; the rule is R-009's.

## Measured 2026-10-14 (verifier of PR #1059, F4)

Head `faf0267e` against main's module, UTC and America/New_York, format v2 and v3:

- Nested and run-end-encoded sources into microsecond leaves: 720 cells (five nanosecond
  shapes, two targets, eighteen doors). 656 store; every storing door of a shape stores one
  value list per zone and version, and every one of them differs from the floor. The 64
  others are the four MERGE doors refusing a list or run-end-encoded source with
  ``MERGE INSERT cannot store-assign column `v`: source type … is not
  ANSI-store-assignable …`` (``MERGE UPDATE SET`` on the update arms). 1,080 of 1,080 nested cells, the microsecond `struct_us` control and the
  `timestamp_ns` targets included, are identical on main.
- Dictionary-encoded source: 224 cells into `TIMESTAMP` store on every door; into
  `TIMESTAMP_NTZ` 176 store and the four MERGE doors refuse 48 with
  `[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] … cannot cast "Dictionary(Int32,
  Timestamp(ns))" to "TIMESTAMP_NTZ". SQLSTATE: 42K09` where `INSERT` stores. Every
  dictionary-encoded cell is identical on main.
- In a UTC session, into `TIMESTAMP_NTZ` from a naive source and into `TIMESTAMP` from
  the timezone-aware source, 8 of 26 labelled values differ (the pre-epoch values with
  digits below the microsecond and the three nearest the 64-bit minimum); in
  America/New_York into `TIMESTAMP` from a naive source, or into `TIMESTAMP_NTZ` from the
  timezone-aware one, 25 of 26.

The per-shape table is in R-024.

## The pin, reconciled

`every_door_refuses_a_nested_nanosecond_leaf_and_writes_nothing` holds a TARGET column
whose type holds a `timestamp_ns` leaf: nine shapes through seventeen doors in
America/New_York and Asia/Kolkata, each refused by name with nothing written (R-015).
This card is about a SOURCE whose nanosecond value is encoded or nested, written into a
microsecond target leaf: R-015's gate reads the target's type, never the source, so it
does not fire, and the write stores the truncated UTC-wall value. Both hold at once: the
verifier wrote the same struct, list and map sources into `timestamp_ns` leaves and every
door took R-015's refusal, on head and on main. R-024 names both door lists.

## Scope

- The rule of R-009 extended to encoded and nested sources: a dictionary-encoded or
  run-end-encoded `timestamp[ns]` array, and a `timestamp[ns]` leaf in a struct, list or
  map source, written into a microsecond `TIMESTAMP` or `TIMESTAMP_NTZ` leaf, floors the
  ticks and reads a naive wall in the session zone, on every door that stores, as the
  plain array does through `INSERT`.
- A timezone-aware nanosecond leaf into `TIMESTAMP_NTZ` stores its session-zone wall,
  floored, as the plain `timestamptz_ns` source does.

## Out of scope

- The top-level plain-array shapes: ICE-TSNS-FLOOR-DOORS-1 (R-009, PR #1059).
- A nested `timestamp_ns` target leaf: refused by R-015 until ICE-TSNS-NESTED-1.
- The nested microsecond walls: R-011. The verifier's `struct_us` control into
  `STRUCT<t: TIMESTAMP>` in America/New_York reads the naive wall as UTC where the
  top-level microsecond `INSERT` reads it in the session zone (25 of 26 values), same on
  main; recorded here, not in this card's scope.
- The MERGE refusals of a dictionary-encoded, run-end-encoded or list source where
  `INSERT` stores: recorded in R-024, not decided by this card.
- Shapes not measured: a map key, and the timezone-aware list, map and encoded sources.
- `applyInPandas` (R-020) and nanosecond targets.

## Gates

- Every F4 cell that stores on main stores what head's plain-array `INSERT` stores for the
  same value, target, zone and format version.
- The R-015 refusal of a nested `timestamp_ns` target leaf is unchanged.
- An Opus verifier on the product pull request.
